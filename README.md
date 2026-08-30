# Go Columnar Engine — Phase 3.6 Bounded Streaming and Native Sinks

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

Phase 2.8 在 Phase 2.7 的 Lazy/Join/Analyze 基础上补齐三个面向真实分析查询的重要能力：**Limit / Sort / Top-K**、**opaque Arrow Take/materialization**、以及 **runtime feedback → aggregate CostModel**。

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

Phase 2.17 新增专用 `List<String>` 列：父列表 offsets/validity 与字符串子向量
分别保存，因此 NULL List、空 List、NULL 元素和空字符串不会混淆。该类型可从
Arrow `List<String>` 零拷贝/轻量规范化接入，并能穿过 Filter、Sort、Limit、
Join、窗口物化、投影和 DataFrame `Collect`。

Phase 2.21 在此基础上增加有界 `Struct`：父级 validity 与按行对齐的子向量
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

Phase 3.6 已提供端到端有界结果出口。小结果继续用最简单的 DataFrame：

```go
df, err := lf.Collect(ctx) // Memory Sink：完整结果必须适合内存
defer df.Close()
```

大结果可直接写原生文件，不先物化整张表：

```go
result, err := lf.
    WithExecution(lazy.ExecutionOptions{Policy: lazy.ExecutionBounded}).
    SinkParquet(ctx, "events.parquet", lazy.ParquetSinkOptions{})
```

自定义流式消费使用借用式 iterator；`BatchView` 只在下一次 `Next` 前有效：

```go
reader, err := lf.CollectBatches(ctx, lazy.BatchOptions{})
defer reader.Close()
for reader.Next(ctx) {
    consume(reader.Batch())
}
err = reader.Err()
```

`Collect`、Batch iterator、`SinkBatches`、Parquet 和 IPC Sink 复用同一个 LazyPlan、
optimizer、SIMD kernel dispatch、查询预算和 Spill 生命周期。文件 Sink 使用同目录 staging、
fsync 和原子提交；失败不返回临时路径。

## Lazy ordering API

```go
lf := session.ScanParquet("events.parquet")

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

所有这些仍然只构造 LogicalPlan；真正执行发生在返回 DataFrame 的 `Collect` /
`Prepare`。需要专用聚合诊断结果时使用 `CollectResult` / `PrepareResult`。

## Sort 和 Top-K 的执行差异

```text
Sort
  -> 全量 columnar materialize
  -> stable sort row indices
  -> gather output
  -> O(N) memory, MemoryManager 计费

Top-K
  -> 扫描排序键
  -> bounded worst-first heap
  -> 只保留 K 个候选
  -> 重读 winner partitions
  -> 只 materialize K 行 payload
  -> O(K) retained state, MemoryManager 计费
```

262,144 行、Top100、单核验证机 benchmark 中位数：

```text
Global Sort : 671.099 ms/op, 40.09 MB/op
Top-K 100   :  14.129 ms/op, 49.3 KB/op
ratio       : ~47.5x
```

这是当前验证机上的算法选择证据，不是跨机器性能承诺。

## Opaque Arrow payload

未拥有专用算术 kernel 的 Arrow nested/decimal/extension 等列不再因为 Filter/Take/Top-K 而失败。

选择行通过 Arrow-native slice/concatenate 完成，并使用显式 retained-array ownership：

```text
OpaqueVector.Owned
Batch.ReleaseOwned()
Frame.Close()
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

当 Aggregate 后还有行级 operator 时，现有 `PreparedQuery` 会被包装成内部 columnar Source；普通 terminal Aggregate 快路径保持不变。

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

已经有回归测试证明：一个默认估算会选择 radix 的 300K-row/4-partition 聚合，在观察到实际 cardinality=2 后，下一次 Prepare 自动切换到非 radix strategy。

## Existing execution capabilities

Phase 2.8 继续保留并回归：

- Go 1.27 scalar / portable SIMD / arch hotspot dispatch；
- Projection/Predicate Pushdown 与 Parquet row-group pruning；
- Arrow RecordBatch typed/opaque source；
- typed scalar expressions、Cast、CSE；
- bounded literal-pattern RegexMatch / RegexExtract；
- specialized Arrow-compatible List<String> vectors and Lazy passthrough；
- PreparedQuery / PreparedFrame；
- adaptive hash aggregation、nullable/composite/dictionary keys；
- columnar JoinSource；
- benchmark-gated parallel radix large join；
- operator-level `ExplainAnalyze`。

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

- `docs/PHASE2_8_ORDERING_TAKE_RUNTIME_FEEDBACK.md`
- `docs/PHASE2_7_OPERATOR_ANALYZE_PARALLEL_RADIX_JOIN.md`
- `docs/ARCHITECTURE.md`
- `docs/JOIN.md`
- `docs/LAZY_OPTIMIZER.md`
- `docs/ROADMAP.md`
- `VALIDATION.md`
