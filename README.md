# Go Columnar Engine — Native Streaming Execution

> **原生流执行分支，包含破坏性 API 变更。** 当前接口、资源边界和验证范围见
> [当前状态](docs/CURRENT_STATUS.md) 与 [迁移说明](docs/STREAM_MIGRATION.md)。
> 旧 prepared W4 性能目标已接受延期，不再阻塞后续开发；历史未达标结论保持不变。
> [组合诊断](validation/charged-reuse/README.md) 为 13.338 ms，较前一 WIP 的
> 17.309 ms 改善，但不是原 10.341 ms 门槛的通过证据。该决定不表示完整验收或可合并。

Go Columnar Engine 是一个面向 Go 原生服务、CLI 和嵌入式分析场景的 Polars-like 列式分析引擎。核心路线保持：

```text
Arrow / Parquet columnar memory
        ↓
Immutable Lazy LogicalPlan
        ↓
Rule optimizer + runtime estimates
        ↓
Projection / Predicate Pushdown
        ↓
Typed expression program / CSE
        ↓
Prepared physical Source DAG
        ↓
SIMD / hash / radix / scheduler
```

当前只有一套 `source.Source.NewBatchStream` 物理执行协议，`Collect`、批次消费、
文件 Sink 与 terminal Aggregate 共用原生执行图。旧 `source.Reader` 与整分区 fallback
已经删除。表达式的 predicate `expr.Program` 与 projection `expr.ScalarProgram`
目前仍分开，统一表达式架构是后续工作，不是已完成能力。

已有能力包括 **Limit / Sort / Top-K**、**opaque Arrow Take/materialization**、
以及 **runtime feedback → aggregate CostModel**。下文介绍当前用法；Phase 2/3
阶段文档和 benchmark 只记录各自版本、平台与输入上的结果。

当前表达式层也支持编译期正则：`RegexMatch` 用于过滤，
`RegexExtract` 用于提取完整匹配或指定捕获组。pattern 只能是字面量，
使用 Go RE2 引擎并限制为 64 KiB；未匹配和未参与匹配的可选捕获组返回
NULL。

```go
pattern := lazy.Str(`^([^:]+):([0-9]+)$`)
result := lf.
    Filter(lazy.RegexMatch(lazy.Col("event"), pattern)).
    SelectExprs(
        lazy.RegexExtract(lazy.Col("event"), pattern, lazy.Lit(1)).Alias("name"),
        lazy.RegexExtract(lazy.Col("event"), pattern, lazy.Lit(2)).Alias("id"),
    )
```

专用 `List<String>` 列的父列表 offsets/validity 与字符串子向量
分别保存，因此 NULL List、空 List、NULL 元素和空字符串不会混淆。该类型可从
Arrow `List<String>` 零拷贝/轻量规范化接入，并能穿过 Filter、Sort、Limit、
Join、窗口物化、投影和 DataFrame `Collect`。

有界 `Struct` 的父级 validity 与按行对齐的子向量
独立保存，首个切片支持标量字段及 `List<String>` 字段。JSON 解码必须显式
提供固定 schema，不做抽样推断：

```go
target := memory.NewSchema(
    memory.Field{Name: "name", Type: memory.TypeString, Nullable: true},
    memory.ListStringField("tags", true, true),
)

df, err := lf.
    WithColumns(lazy.JSONDecode(lazy.Col("payload"), target).Alias("decoded")).
    Collect(ctx)
```

Struct 能作为 Filter/Take/Sort/Join 的 payload，并通过分块只读 `Series` 和
有界 DataFrame 显示访问；递归 Struct/List、Map 和 schema inference 仍明确
留在后续阶段。

## 结果消费与资源边界

小结果继续用最简单的 DataFrame：

```go
df, err := lf.Collect(ctx) // Memory Sink：完整结果必须适合内存
if err != nil {
    return err
}
defer df.Close()
```

大结果可直接写原生文件，不先物化整张结果表。`ExecutionBounded` 必须配置有限
managed memory limit；需要 Spill 的状态算子还必须配置有限临时盘额度，例如：

```go
opts := engine.DefaultOptions()
opts.MemoryLimitBytes = 256 << 20
opts.Spill = engine.SpillOptions{Directory: "/tmp/golume", MaxBytes: 4 << 30}
session := lazy.NewSession(opts)
defer session.Close()

lf, err := session.ScanParquet("input.parquet")
if err != nil {
    return err
}
result, err := lf.
    WithExecution(lazy.ExecutionOptions{Policy: lazy.ExecutionBounded}).
    SinkParquet(ctx, "events.parquet", lazy.ParquetSinkOptions{})
```

自定义流式消费使用借用式 iterator；`BatchView` 只在下一次 `Next` 前有效：

```go
reader, err := lf.CollectBatches(ctx, lazy.BatchOptions{})
if err != nil {
    return err
}
defer reader.Close()
for reader.Next(ctx) {
    consume(reader.Batch())
}
err = reader.Err()
```

`Collect`、Batch iterator、`SinkBatches`、Parquet 和 IPC Sink 复用同一个 LazyPlan、
optimizer、SIMD kernel dispatch、查询预算和 Spill 生命周期。文件 Sink 使用同目录 staging、
fsync 和原子提交；失败不返回临时路径。

预算约束的是纳入 reservation 的活跃 buffer、workspace、状态与结果，不是整个 Go
进程的 RSS。Arrow decoder pages/dictionaries、codec、固定元数据、runtime 与 GC
保留存在额外开销。默认 memory limit 为 0，Spill 关闭；只选择 bounded 策略不能替代
资源配置。不能安全按预算执行的计划或单个超限值应返回明确错误。

`Prepare` 返回 `PreparedDataFrame`，可重复 `Execute` 或 `ExecuteBatches`；
每个 DataFrame、BatchReader 和 prepared plan 都必须按各自生命周期关闭。
`PrepareResult` / `CollectResult` 只接受根节点为 Aggregate 的计划。

调用者通过 context 提供的 `QueryBudget` 由这些消费入口共同采纳，仍由调用者拥有和
关闭。`ExecutionBounded` 会拒绝无限额的外部 budget，不能用它绕过有限内存要求。

`Analyze` / `ExplainAnalyze` 已改为 drain 同一原生批次入口，不保留完整 DataFrame。
`MaterializedRows` 已删除；`OutputBatches` 只计非空输出批次，`FirstBatchDuration`
从打开 stream 开始计时，没有观察到非空输出时为零。失败仍返回部分报告，调用者必须
检查 error。`Termination`、`FailurePhase`、`Error` 和 `CleanupError` 按公共调用
边界报告，嵌套算子的 Close 错误也可能从 Next 返回，详见
[Analyze 迁移契约](docs/STREAM_MIGRATION.md#streaming-analyze-and-partial-failure-reports)。

诊断中 `peak_memory_scope=query` 表示共享 query budget 高水位，不是算子独占内存，
不能跨算子求和；外部 budget 也可能包含调用者先前或同时的使用。
本阶段实现、独立 review 与本地完整 `scripts/validate.sh` 已通过，覆盖普通/SIMD、
宿主支持的强制宽度、SIMD-disabled fallback、Linux build、race、vet、GoDoc、
结构/生成器检查及 demo。见 [验收记录](validation/analyze-stream-review.md) 和
[完整日志](validation/analyze-stream-full-validation.txt)。仍使用合成替代 fixture，
未核验 GitHub Actions，不表示原始 fixture、旧 W4 门槛或发布已通过。统一表达式阶段
尚未完成，不属于本次验收范围。

## Lazy ordering API

```go
lf, err := session.ScanParquet("events.parquet")
if err != nil {
    return err
}

result := lf.
    Filter(lazy.Col("score").Gt(lazy.Lit(0))).
    Sort(lazy.Desc("score")).
    Limit(100)
```

`Sort(...).Limit(K)` 会在语义允许时优化成：

```text
TOPK k=K
```

也可以直接写：

```go
lf.TopK(100,
    lazy.Desc("score"),
    lazy.AscNullsFirst("timestamp"),
)
```

这些变换只构造 LogicalPlan；`Prepare` 编译并准备可复用物理计划，`Execute` /
`Collect`、批次迭代或 Sink 消费输出。需要 terminal Aggregate 的 record/counter
结果时使用 `CollectResult` / `PrepareResult`。

## Sort 和 Top-K 的执行差异

```text
Sort
  -> resident stable sort，或配置 Spill 后生成稳定 sorted runs
  -> 外排路径以有界 k-way merge 输出
  -> retained state / run buffers 受 managed budget 约束

Top-K
  -> 扫描排序键
  -> bounded worst-first heap
  -> 只保留 K 个候选
  -> 可重放输入二次扫描，按 row ordinal 收集 winner payload
  -> 只 materialize K 行 payload
  -> O(K) retained state, MemoryManager 计费
```

Top-K 的 O(K) 指候选状态，不是对任意输入的总内存承诺；不可重放输入需要受预算
约束的快照。Collect 的完整输出仍另占内存。

历史 Phase 2.8 单核验证机 benchmark 中位数（262,144 行、Top100）：

```text
Global Sort : 671.099 ms/op, 40.09 MB/op
Top-K 100   :  14.129 ms/op, 49.3 KB/op
ratio       : ~47.5x
```

这是历史版本上的算法选择证据，不是当前分支重测或跨机器性能承诺。

## Opaque Arrow payload

未拥有专用算术 kernel 的 Arrow nested/decimal/extension 等列可以作为 opaque payload
保留和筛选。但 bounded 模式下，无法提供可靠 sizing 的非连续 gather 会在分配前
返回资源错误；这不是任意 Arrow 类型/操作都支持有限内存执行的承诺。

选择行通过 Arrow-native slice/concatenate 完成，并使用显式 retained-array ownership：

```text
OpaqueVector.Owned
Batch.ReleaseOwned()
DataFrame.Close()
```

因此“暂时不能计算某类型”和“不能保留/筛选该类型”已经解耦。

## Aggregate → Sort / Top-K

常见查询现在可以继续组成一个 Lazy DAG：

```text
Scan
 -> Filter
 -> GroupBy
 -> Agg
 -> Sort
 -> Limit
```

Aggregate 作为原生 Source 节点继续向下游产出；terminal Aggregate 的
`engine.Result` 也复用同一聚合执行。不存在独立 Reader 聚合后端。

## Runtime feedback

第一次 Prepare 仍使用静态估算。成功 Execute 后记录实际：

```text
rows
selected rows
output groups
```

下一次 Prepare 在没有显式 Hint 时使用 EWMA：

```text
new = old * 0.75 + observed * 0.25
```

feedback cache 最多保留 1024 个 plan signature。用户显式 `WithHints` 永远优先。

原生聚合计划保留执行反馈，下一次 Prepare 使用已观察的 cardinality；旧整行组 radix 聚合后端已删除，实际策略为 direct-fused、bitmap-direct、partitioned-hash、sorted 或预算驱动的 spill-hash。迁移见 [STREAM_MIGRATION.md](docs/STREAM_MIGRATION.md)。

## Existing execution capabilities

当前接口和实现包括（验证范围见下面的历史证据说明）：

- Go 1.27 scalar / portable SIMD / arch hotspot dispatch；
- Projection/Predicate Pushdown 与 Parquet row-group pruning；
- Arrow RecordBatch typed/opaque source；
- typed scalar expressions、Cast、CSE；
- bounded literal-pattern RegexMatch / RegexExtract；
- specialized Arrow-compatible List<String> vectors and Lazy passthrough；
- engine.PreparedQuery / lazy.PreparedDataFrame / lazy.PreparedResult；
- adaptive hash aggregation、nullable/composite/dictionary keys；
- columnar JoinSource；
- benchmark-gated parallel radix large join；
- 流式 operator-level `Analyze` / `ExplainAnalyze` 与失败时部分诊断。

## Reference regression

`testdata/bench-1m.parquet`：1,048,576 rows。

固定结果：

```text
groups   = 219196
selected = 472475
checksum = 236716301
```

## Build and validate

需要 Go 1.27：

以下是复现命令。本阶段本地完整验证已通过，具体范围与日志见上面的验收记录。
本次及历史 native/charged-reuse 验证使用已披露的合成替代 fixture，不能据此声称
原始 fixture 或 GitHub Actions 已通过。后续代码修改必须针对实际变更重新验证；
详见 [验证边界](docs/CURRENT_STATUS.md#验证与历史证据)。

```bash
go mod download
./scripts/check_simd_structure.sh
go test ./...
GOEXPERIMENT=simd go test ./...
go test -race ./...
go vet ./...
go run ./tools/checkgodoc .
```

推荐：

```bash
./scripts/validate.sh
./scripts/bench.sh
```

Parquet Lazy demo：

```bash
GOEXPERIMENT=simd go run ./cmd/lazy-demo \
  -parquet ./testdata/bench-1m.parquet \
  -workers 4 -repeat 3 -analyze
```

## Documentation

- [当前状态、限制与后续阶段](docs/CURRENT_STATUS.md)
- [破坏性 API 与资源迁移](docs/STREAM_MIGRATION.md)
- [近期计划与历史 Phase 3 清单](docs/PHASE3_STREAMING_ENGINE_TODO.md)
- [本阶段流式 Analyze 验收](validation/analyze-stream-review.md)
- [原生执行历史验证](validation/native-stream-unification-review.md)
- [持续计费复用与共享内核历史证据](validation/charged-reuse/README.md)
- `docs/PHASE2_8_ORDERING_TAKE_RUNTIME_FEEDBACK.md`
- `docs/PHASE2_7_OPERATOR_ANALYZE_PARALLEL_RADIX_JOIN.md`
- `docs/ARCHITECTURE.md`
- `docs/JOIN.md`
- `docs/LAZY_OPTIMIZER.md`
- `docs/ROADMAP.md`
- `VALIDATION.md`
