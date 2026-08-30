# Phase 3：流式物理执行引擎改造 TODO

状态：Phase 3.0–3.6 已完成；下一步为 Phase 3.7 单引擎收口与旧路径删除。

执行策略、默认行为、资源错误和用户 API 已由
[ADR_PHASE3_EXECUTION_UX.md](ADR_PHASE3_EXECUTION_UX.md) 决定；本文件保留实施清单。

本文定义 GoLume 在 Phase 3 的执行引擎改造路线。目标是获得类似
Polars 的批式流处理、有限内存执行和流式 Sink 能力，但不照搬其内部实现，
也不新建一套与现有 LazyPlan、优化器、`memory.Batch`、`kernel.Set` 和
DataFrame 语义分叉的引擎。

## 1. 目标与边界

### 1.1 必须达到的结果

- [x] Scan、Filter、Projection、Limit 等流水算子可在输入尚未读完时持续产出。
- [ ] GroupBy、Join、Sort 等状态型算子受统一内存预算约束，并在允许时 Spill。
- [ ] `ExecutionBounded` 模式下，输入规模增长到内存数倍时，峰值内存保持在配置上限附近。
- [x] `Collect`、批次迭代和文件 Sink 共享同一套物理执行图；不建立 eager 第二引擎。
- [ ] 取消、预算失败、磁盘写满、解码失败和下游提前停止时，释放全部 Batch、reservation、临时文件和 worker。
- [ ] SQL NULL、稳定排序、Join 重复行、聚合溢出、Dictionary domain 和嵌套值语义与当前引擎一致。
- [ ] SIMD 只进入已证明有效的批内热路径，不能侵入 public API、调度协议或 Spill 生命周期。
- [ ] 现有纯内存查询的相关端到端基准回退不超过 3%。

这里的“流式”是**有限内存的增量物理执行**，不是只把已物化的整表切成若干
Batch。`Collect` 最终仍需把完整结果保存在 DataFrame 中，因此只有批次消费 API
和 Sink 才能保证结果本身大于内存时继续执行。

### 1.2 非目标

- 本阶段不做分布式调度、GPU、SQL 前端或远程 Shuffle。
- 不为了流式化改变表达式、NULL、排序稳定性和 Join 结果语义。
- 不把所有算子强行做成单遍算法；真正需要全局状态的算子采用分区、外排或多遍算法。
- 不承诺任意递归嵌套、Map 或运行时 Schema 演化；先保证现有类型可 Spill、可恢复。
- 不以“代码中出现了 SIMD 指令”为验收标准，性能证据才是准入标准。

## 2. 当前基线与缺口

当前 `source.Reader.ReadPartition` 能逐分区读取，Filter、Projection、Limit 也能逐
Batch 处理；Top-K 使用有界候选集合，Probe 侧 Join 可以逐分区读取。这些都是可复用
基础，但还不是端到端流式引擎。

| 能力 | 当前状态 | Phase 3 目标 |
| --- | --- | --- |
| Source 分区读取 | 已有 | 统一成可取消、可重放的 morsel stream |
| Filter / Projection / Limit | 批内增量 | 同一 pipeline 内融合或直接拉取 |
| Top-K | 有界 O(K) | 接入统一 stream、预算和输出分块 |
| Hash GroupBy | 逐批读、全局表常驻 | 分区聚合、可合并状态、按压力 Spill |
| Join | 右侧 Build 常驻，左侧逐批 Probe | Broadcast / 分区 Hash / Merge 三类策略 |
| Sort / Window | 全输入物化 | 外排 run、k-way merge、窗口能力分级 |
| 内存 | 有 reservation，但没有查询级 Spill 协调 | Engine → Query → Operator 分层仲裁 |
| Spill | 只有接口和 `NoSpill` | 查询级临时目录、格式、恢复和清理协议 |
| 背压 | 无统一协议 | pull 自然背压 + 并行边界字节有界 Exchange |
| Sink | `Collect` 保留全部结果 | Collect、Batch、Parquet/IPC Sink 共享图 |
| Explain | 逻辑/物理与 Analyze 已有 | 展示 pipeline、阻塞边界、峰值内存和 Spill |

旧 `source.Source + Reader` 可以作为首批 Source Adapter 使用，但不能长期成为所有
物理算子的共同协议。当前一个输入分区只对应一个输出 Batch 的隐含假设，无法自然表达
Filter 的零输出、Join 的多 Batch 输出、外排 Sort 的合并流和下游提前停止。

## 3. 总体架构

```text
LazyPlan
   │ optimizer / pushdown / CSE
   ▼
Physical lowering + stream capability analysis
   │
   ├── Pipeline: Scan → Filter → Projection → Limit
   ├── Bounded state: TopK / bounded window
   ├── Spillable state: GroupBy / Join / Sort
   └── Sink: Collect / Batch consumer / Parquet / IPC
                │
                ▼
      query memory manager + spill store
                │
                ▼
          kernel.Set batch kernels
```

### 3.1 拉取优先的混合执行

推荐采用 demand-driven pull，而不是在每两个算子之间都创建 goroutine 和 channel：

- 同一 pipeline 内由 Sink 向上游调用 `Next`，下游不请求时上游自然停止，形成背压。
- Filter、Projection 等算子在调用栈内运行，减少 goroutine、channel 和调度开销。
- 只有需要并行 fan-out/fan-in、分支复用或异步 I/O 的边界才插入 `Exchange`。
- Exchange 同时限制 Batch 数和已预留字节；只限制队列长度不能约束宽表或大字符串。
- 每个 pipeline 有明确的 owner、取消树和关闭顺序，不允许后台 worker 脱离查询生命周期。

候选内部协议：

```go
type BatchStream interface {
	Next(ctx context.Context) (BatchLease, bool, error)
	Close() error
}

type BatchLease interface {
	Batch() *memory.Batch
	Release()
}
```

这不是预先冻结的 public API。Phase 3.0 必须通过原型比较以下方案后再定：

- `Next(ctx, dst) (bool, error)`：复用直接、分配少，但跨 Exchange 的 ownership 较难表达；
- 返回 `BatchLease`：ownership 清楚，但要证明接口分派和 lease 对象没有热路径分配；
- 内部泛型/具体类型 cursor：开销最低，但组合和测试复杂度更高。

无论选择哪一种，协议必须支持一个输入产生 0、1 或多个输出 Batch，并明确：

- Batch 的借用/转移/释放规则；
- 下游是否可以跨下一次 `Next` 保留数据；
- 提前停止、错误和 panic 时谁负责释放；
- `Sequence` / `RowOrdinal` 如何维持稳定语义；
- Source 是否可重放，以及重放是否会观察到相同数据版本。

### 3.2 物理算子能力分类

每个物理节点在 lowering 时必须声明能力，不能运行时悄悄全量物化：

```go
type StreamClass uint8

const (
	PipelineOperator StreamClass = iota
	BoundedStateOperator
	SpillableStateOperator
	MemorySinkOperator
)
```

- `PipelineOperator`：状态与 Batch 或少量 look-ahead 有关，可以持续产出。
- `BoundedStateOperator`：状态由显式参数限制，例如 Top-K 的 K。
- `SpillableStateOperator`：可能需要全局状态，但具备外存算法。
- `MemorySinkOperator`：有意将全部结果留在 RAM，例如 DataFrame `Collect`。

新增执行模式：

```go
type ExecutionMode uint8

const (
	ExecutionAuto ExecutionMode = iota
	ExecutionMemoryOnly
	ExecutionBounded
)
```

- `Auto`：默认，在同一物理图内逐节点选择流水、内存状态或 Spill；不在两套引擎间回退。
- `MemoryOnly`：禁用 Spill，为小数据、诊断和基准保留最快路径。
- `Bounded`：要求中间状态具备预算上界或 Spill；Prepare 阶段发现不支持节点就返回带节点路径的错误。

`Bounded` 只保证中间执行有界。`Collect` 是 Memory Sink，完整结果仍须适合内存；
超限错误必须建议用户改用 Batch 或文件 Sink。普通调用仍是 `Collect(ctx)`，执行策略
通过 Session 默认值或不可变 LazyFrame override 提供，不增加常用方法参数。

## 4. 分阶段 TODO

每个子阶段独立提交。前一阶段的完整验收和生成器漂移检查通过后，才进入下一阶段。

### Phase 3.0：执行契约与基线

内部流协议及 ownership/replay/error 决策见
[`ADR_PHASE3_STREAM_CONTRACT.md`](ADR_PHASE3_STREAM_CONTRACT.md)。

状态：已完成（2026-08-30）。机器可读验收记录见
[`validation/phase30-streaming-gate.json`](../validation/phase30-streaming-gate.json)。

- [x] 冻结 execution policy、fallback、资源配置和用户结果 API 语义。
- [x] 补充内部 Batch ownership、replay、cancel 和错误传播 ADR。
- [x] 为物理节点增加 stream capability；Explain 标出 `PIPELINE`、`BOUNDED`、`SPILLABLE`、`MEMORY SINK`。
- [x] 实现两种 `BatchStream` 原型，用基准决定最终内部协议。
- [x] 以 Adapter 包装现有 Source/Reader，保持 Parquet、Arrow RecordBatch 和 InMemory Source 可用。
- [x] 将 Scan → Filter → Projection → Limit → Collect 纵向切片迁移到新协议。
- [x] 增加只消费并释放 Batch 的内部 `DrainSink`，让早期阶段能验证真正的有界执行；public Sink API 留到 Phase 3.6。
- [x] 允许一个输入 morsel 产生 0..N 个输出；删除物理层对“一分区一 Batch”的依赖。
- [x] 建立 pipeline/morsel benchmark 基线和机器可读结果。
- [x] 增加关闭顺序、重复 Close、取消和下游提前停止测试。

验收：

- 新旧协议结果逐 Batch、逐行、逐 NULL 位一致；
- 1K、4K、16K、64K 行 morsel 下无热路径分配增长；
- 单 worker 流水查询相对当前引擎端到端回退不超过 3%；
- race、取消和 fault-injection 测试无 goroutine、reservation、Arrow 引用泄漏。

### Phase 3.1：Pipeline Scheduler 与背压

- [x] 实现 query-owned pipeline graph，不使用全局后台 goroutine。
- [x] 实现字节有界 `Exchange`，reservation 成功后才允许生产者入队。
- [x] 支持 worker 数、目标 morsel 字节数和最大 in-flight 字节配置。
- [x] 目标 morsel 大小由 Schema 宽度和历史 Batch 大小估算，不只按行数固定。
- [x] 处理大字符串或嵌套值导致单个 morsel 超目标的问题；单值超过硬限制时明确失败。
- [x] fan-in 保留稳定 `Sequence`；不要求顺序的算子可声明 relaxed ordering。
- [x] fan-out 禁止无上限缓存；可重放 Source 优先重扫，不可重放 Source 才允许受预算控制的 multiplexer。
- [x] 下游完成或报错时主动取消上游和所有 sibling pipeline。
- [x] Observer 只记录 morsel/stage 级数据，不进入 row hot loop。

验收：

- 人为减慢 Sink 时，Source 的 in-flight bytes 不持续增长；
- worker=1 与 worker=N 结果确定性一致；
- 反复取消和提前 Limit 不遗留 worker；
- 小 Batch 调度开销、宽表峰值内存和吞吐均有独立基准。

状态：完成。验收证据见 `validation/phase31-pipeline-gate.json`。Apple M4 Pro 的
10×1 秒采样中，小 Batch Auto 路径相对 direct 中位数回退 1.294%、分配不变；4 worker
相对 1 worker 提升 39.091%，8 worker 比 4 worker 回退 1.834%，因此默认 pipeline
worker 上限取 4；16 列宽字符串表的 exchange 峰值为 4,523,392 bytes，低于配置的
8,388,608 bytes，Close 后 reservation 回到 0。

### Phase 3.2：查询级内存管理与 Spill 基础设施

- [x] 建立 `EngineBudget → QueryBudget → OperatorBudget` 分层 reservation。
- [x] 区分 retained state、in-flight batch、workspace、output、spill buffer 五类内存。
- [x] 所有扩容按实际差额先 Reserve，再分配；失败路径回滚一次且仅一次。
- [x] 定义 soft limit、hard limit 和 spill request；禁止算子各自猜测系统剩余内存。
- [x] 内存仲裁器可以要求可 Spill 算子释放目标字节数，并防止多个算子同时抖动式 Spill。
- [x] 重做当前 `memory.SpillManager`，统一查询级目录、配额、文件命名、关闭和清理。
- [x] 临时目录权限为 `0700`、文件为 `0600`；文件头包含版本、Schema fingerprint、endianness 和校验信息。
- [x] 写入采用临时文件加原子提交；恢复拒绝截断、版本不符、Schema 不符和损坏文件。
- [x] 支持磁盘配额、磁盘写满、文件删除失败和进程退出后的孤儿目录清理策略。
- [x] 行数据 run 优先使用统一 Arrow-shaped 格式；聚合/Join 内部状态允许专用版本化 codec。
- [x] 初版不默认压缩或加密；只有端到端吞吐、CPU 和临时盘压力基准证明收益后才启用压缩。

Spill 是内存管理器发起、算子执行的协作协议。算子不得在 `Reserve` 失败后私自向任意
路径写文件；radix partition 是 Hash 类算子的首选 Spill 单元。

验收：

- 在固定预算下反复 Spill/Restore 后，active reservation 回到零；
- 覆盖取消、磁盘写满、短写、损坏、重复 Delete、恢复中止和 Close 幂等；
- 临时文件无敏感明文路径泄漏，查询结束后目录可回收；
- 10 倍于内存预算的数据能通过一个最小 Spill 算子测试，峰值内存不随输入线性增长。

状态：完成。设计与文件协议见
[`ADR_PHASE3_MEMORY_SPILL.md`](ADR_PHASE3_MEMORY_SPILL.md)，机器验收见
[`validation/phase32-memory-spill-gate.json`](../validation/phase32-memory-spill-gate.json)。
最小算子在 4 KiB hard limit 下处理 51,360 bytes（12.54 倍预算），真实触发 40 个
spill run，峰值 reservation 为 3,890 bytes，结束后 query/engine/disk 使用均为 0。

### Phase 3.3：流式/Spillable GroupBy

- [x] 抽象可合并聚合状态：`Init`、`UpdateBatch`、`MergeBatch`、`FinalizeBatch`。
- [x] 保留现有类型专用 SoA 聚合布局，不退化成 `map[any]any` 或逐行 interface dispatch。
- [x] 将高基数 Hash 表拆成 radix partitions；只让工作集分区驻留。
- [x] 内存压力下优先 Spill 冷分区，最终按分区 Merge/Finalize。
- [x] 对已按 key 聚集或排序的数据增加 sorted aggregation 物理策略。
- [x] 有限预算/Bounded radix 取消与总输入行数线性相关的常驻辅助数组；无限预算 resident 快路径明确保留该性能权衡。
- [x] 维持 nullable/composite/dictionary key、SUM/COUNT/MIN/MAX/AVG、溢出行和 checksum 语义。
- [x] 输出也按 Batch 分块，单个超大分组结果不能迫使整个结果先物化。
- [x] skew 统计反馈给 planner 和 ExplainAnalyze。

验收：

- 低/高基数、Zipf skew、全唯一、全同 key、极端 NULL 密度、多 Batch 和百万行一致性；
- 预算不足触发一次和多次 Spill，结果与 InMemory 模式完全一致；
- Prepared 重复执行不复用上次查询状态或临时文件；
- 无 Spill 的已有聚合基准回退不超过 3%，Spill 基准报告吞吐和峰值内存。

状态：完成。算法、易用性和 resident/bounded 性能取舍见
[`ADR_PHASE3_AGGREGATION.md`](ADR_PHASE3_AGGREGATION.md)，机器验收见
[`validation/phase33-aggregate-gate.json`](../validation/phase33-aggregate-gate.json)。

### Phase 3.4：流式/Spillable Join

- [x] planner 在 Broadcast Hash、Partitioned Hash 和 Merge Join 中选择策略。
- [x] 小 Build 侧走现有驻留 Hash 快路径，并按实际字节而不是只按行数判断。
- [x] 大 Build/Probe 双侧按兼容 hash/radix 分区；不能驻留的分区 Spill 后逐对处理。
- [x] 已排序输入可选择 Merge Join，避免构建 Hash 表。
- [x] 一个输入行匹配大量结果时分多个输出 Batch，不能形成无界单 Batch。
- [x] 维护 Inner/Left/Right/Full 的未匹配状态和稳定输出契约。
- [x] 递归 repartition 必须设置最大深度；极端单 key skew 进入专用处理或明确失败，不能无限递归。
- [x] 支持现有 scalar、dictionary、fixed composite key 和 Struct/List payload Spill/Restore。
- [x] 将 build bytes、probe bytes、重复倍数和 skew 写入 runtime feedback。

验收：

- 覆盖重复 key、高放大倍数、全 NULL、外连接未匹配、字典 domain、多 Batch 和嵌套 payload；
- 强制 1 次、多次和递归分区 Spill，结果与 InMemory Join 一致；
- 输出放大到远大于 RAM 时可经 Batch Sink 完成；
- 预算、磁盘和取消错误不遗留 build table、Batch lease 或文件。

状态：完成。策略、易用性、外连接生命周期、12× 输入 bounded 验收和 SIMD 候选拒绝依据见
[`ADR_PHASE3_JOIN.md`](ADR_PHASE3_JOIN.md)，机器验收见
[`validation/phase34-join-gate.json`](../validation/phase34-join-gate.json)。

### Phase 3.5：External Sort 与 Window 分级

- [x] 根据预算生成多个稳定 sorted run，每个 run 记录原始 `RowOrdinal` 作为最终 tie-breaker。
- [x] run 可在内存或 Spill store 中，最终使用有界 k-way merge 流式输出。
- [x] merge fan-in 受打开文件数和 buffer 预算限制；run 太多时执行分层 merge。
- [x] 保留现有 Top-K 路径；当 K 的实际状态超预算时给出明确策略或错误。
- [x] Window lowering 将能力分成：可流水、分区有界、需排序 Spill、暂不支持。
- [x] rolling 窗口只保留 frame 所需状态；无界/排序窗口复用 External Sort。
- [x] 在 `ExecutionBounded` 下，暂不支持的 window frame 在 Prepare 阶段失败。

验收：

- 稳定多 key、ASC/DESC、独立 NULL placement、NaN、重复值和表达式 key 与当前结果一致；
- 小预算、多 run、多级 merge、取消、损坏 run 和打开文件上限有测试；
- 输入为预算 10 倍以上时，Sort → Batch Sink 峰值内存受限；
- Top-K 小 K 性能不因 External Sort 抽象而回退超过 3%。

状态：完成。外排稳定性、Window 原序恢复、易用性边界、预算与 SIMD 决策见
[`ADR_PHASE3_EXTERNAL_SORT_WINDOW.md`](ADR_PHASE3_EXTERNAL_SORT_WINDOW.md)，机器验收见
[`validation/phase35-sort-window-gate.json`](../validation/phase35-sort-window-gate.json)。

### Phase 3.6：流式结果 API 与 Sink

- [x] `Collect` 保持 DataFrame memory sink，文档明确结果大小本身必须适合内存。
- [x] 增加借用式 pull Batch iterator；Batch 只在下一次 `Next` 或 `Close` 前有效，提前结束只需 Close。
- [x] 增加 push 型 Batch consumer，callback 返回后 Batch 默认失效。
- [x] 实现 Parquet Sink，再实现 IPC Sink；二者复用 optimizer、物理图、预算和取消。
- [x] Sink 写入临时目标并提交，失败不暴露半成品为成功结果。
- [x] 支持受控 sink multiplex；每个下游有独立背压，慢下游不能导致无界共享缓存。
- [x] 明确文件分片的最大行数、目标字节数、Schema 和顺序语义。

候选 public API 在本阶段单独评审，不能直接暴露内部 lease：

```go
stream, err := lf.CollectBatches(ctx, BatchOptions{})
if err != nil {
	return err
}
defer stream.Close()
for stream.Next(ctx) {
	consume(stream.Batch()) // view 借用到下一次 Next
}
if err := stream.Err(); err != nil {
	return err
}

err := lf.SinkBatches(ctx, BatchOptions{}, func(ctx context.Context, batch BatchView) error {
	// batch 只在 callback 期间有效
	return consume(batch)
})

_, err := lf.SinkParquet(ctx, path, ParquetSinkOptions{})
```

验收：

- 结果大于 RAM 时 Parquet/IPC Sink 可完成且内存稳定；
- callback 慢、报错或 panic 时停止整个查询并释放资源；
- `Collect`、Batch Sink 和文件 Sink 在相同输入上结果一致；
- 不允许格式化或 `String()` 隐式触发任何 Lazy 执行。

状态：完成。API、借用生命周期、文件提交、分片语义、预算 allocator、易用性和 SIMD
决策见 [`ADR_PHASE3_RESULT_SINKS.md`](ADR_PHASE3_RESULT_SINKS.md)，机器验收见
[`validation/phase36-result-sink-gate.json`](../validation/phase36-result-sink-gate.json)。

### Phase 3.7：单引擎收口与旧路径删除

- [ ] Lazy 查询全部 lowering 到新 physical graph；不保留两套优化或表达式语义。
- [ ] 删除物理算子内部的旧 Reader 兼容分支和静默全量物化 fallback。
- [ ] 外部 Source 兼容只允许停留在 Source Adapter 边界，并给出迁移期和删除版本。
- [ ] Prepared 查询复用编译产物，但每次执行创建独立 QueryBudget、pipeline 状态和 Spill 目录。
- [ ] 更新 Extension Points、性能文档、迁移文档、examples 和 CHANGELOG。
- [ ] 将全套流式 fault/race/benchmark gate 纳入 `scripts/validate.sh` 或独立 CI job。

完成标准：同一 LazyPlan 只有一个物理执行语义；`MemoryOnly` 和 `ExecutionBounded`
只改变物理策略和资源边界，不改变结果。

## 5. SIMD 策略

Polars 将 SIMD、列式内存、多线程、lazy 优化和 streaming 组合使用。GoLume 也应组合，
但必须保持分层：streaming 决定**数据何时流动、状态何时释放**，SIMD 决定**一个
连续 Batch 内怎样更快计算**。SIMD 不能解决无界 Hash 表、全量 Sort 或无背压队列。

### 5.1 保持现有 dispatch 契约

- [ ] 所有 SIMD 继续隐藏在 `kernel.Set`，物理算子只调用 KernelID 对应能力。
- [ ] `ModeScalar` 是规范语义；`ModeSIMD` 在无实现时正确回退；`ModeAuto` 只选通过门槛的精确组合。
- [ ] 每个候选按 operation、类型、架构、向量宽度、NULL 密度和选择率分别验证。
- [ ] 不把 darwin/arm64 结果外推到 linux/amd64；没有目标机证据时 Auto 保持 scalar。
- [ ] 热点至少 10 个同构建、每次 1 秒样本，kernel 提升至少 10%、`p < 0.05`，相关端到端回退不超过 3%。
- [ ] scalar、Auto、forced-SIMD、`GODEBUG=simd=0`、无 SIMD 构建和 race 同时通过。
- [ ] 每次 hot loop 变化后重跑证据，不复用旧 benchmark 结论。

### 5.2 Phase 3 的候选优先级

优先候选，先写 scalar 基线再做 SIMD：

1. validity bitmap AND/OR、selection bitmap 合并和 word count；
2. 已有数值比较/算术在新 pipeline 中的融合调用；
3. radix histogram、固定宽 key 的 partition classification 和连续 scatter 辅助；
4. 固定宽 row encoding、批量 Hash64 和连续 key equality；
5. 外排读写中的 checksum 或压缩，仅在 I/O 端到端基准证明 CPU 是瓶颈时。

需要基准后决定：

- 高选择率连续 compaction；
- 固定宽 Join 输出的连续 gather/scatter；
- sorted run 的块级比较；
- 字符串 offset 检查、ASCII 特化和 row encoding。

默认保持 scalar：

- scheduler、Exchange、channel、reservation 和文件系统调用；
- Hash 表随机 probe、Top-K/k-way merge heap 和 branch-heavy window；
- 低密度随机 gather、变长 String/List/Struct materialization；
- Regex、JSON、动态 delimiter 和 Unicode code-point 拆分；
- Spill 文件元数据和错误处理。

这些路径未来不是永久禁止，而是不能在没有数据布局改变和端到端证据时仅靠 SIMD
指令解决。现有 UTF-8 候选曾出现微基准大幅加速、混合 Unicode 端到端却回退的情况；
Phase 3 必须继续执行“失败候选直接删除”的规则。

### 5.3 morsel 大小与 SIMD 的共同调优

过小 morsel 会让函数调用、调度和尾部处理吞掉 SIMD 收益；过大 morsel 会增加延迟、
峰值内存和 Spill 压力。因此不能只设一个固定行数：

- [ ] 基准矩阵至少覆盖 256、1K、4K、16K、64K 行；
- [ ] 覆盖窄数值表、宽表、长字符串、List/Struct、0/1/50/99% NULL；
- [ ] 覆盖 1/10/50/90/100% Filter 选择率和不同 Join/GroupBy 基数；
- [ ] 同时记录 rows/s、bytes/s、ns/row、allocs、峰值 reservation 和 in-flight bytes；
- [ ] Auto morsel sizing 以目标字节为主，并设置最小/最大行数；
- [ ] planner 只有在证据充分时才把 morsel size/selectivity 加入 SIMD 策略桶。

不要假定“更宽 SIMD = 整体更快”。当查询受内存带宽、Hash 随机访问、磁盘或调度限制
时，应优先减少复制、改善布局、融合 kernel 和释放状态。

## 6. Planner 与可观测性

- [ ] `ExplainPhysical` 展示 pipeline id、Exchange、stream class、ordering 和预计常驻字节。
- [ ] 明确标红 Memory Sink、不可 Spill blocking 节点和需要不可重放缓存的 multiplexer。
- [ ] `ExplainAnalyze` 记录输入/输出 rows、morsels、峰值/当前 reservation、in-flight bytes。
- [ ] Spill 指标包含请求次数、写/读 bytes、partition/run 数、耗时和最大临时盘占用。
- [ ] Backpressure 指标包含 producer blocked time、consumer idle time 和队列高水位。
- [ ] 每个 kernel 继续报告实际 backend、vector bits、hardware/emulation 和 Auto 原因。
- [ ] planner 反馈增加 row width、cardinality、skew、Join multiplicity、Spill 和 Source replayability。
- [ ] Observer 事件按 query/stage/morsel 聚合，禁止逐行回调。

## 7. 性能与正确性验收矩阵

### 7.1 正确性

- [ ] 单 Batch、多 Batch、空 Batch、百万行和输入规模大于 RAM。
- [ ] NULL 密度 0%、1%、50%、99%、100%，以及 NaN/Inf/溢出边界。
- [ ] Unicode、宽字符串、List/Struct、Dictionary domain 和 Arrow retained ownership。
- [ ] 稳定 Sort、Join 重复放大、外连接未匹配、高基数和极端 skew。
- [ ] Prepared 并发执行、上下文取消、下游提前结束和重复 Close。
- [ ] 磁盘写满、短写、损坏、权限错误、配额不足和清理失败。

### 7.2 性能

- [ ] 无状态 pipeline 的 in-memory 端到端回退不超过 3%。
- [ ] 已有 SIMD allowlist 不因 stream 抽象失去收益或增加分配。
- [ ] 固定预算下，10 倍预算输入的 GroupBy、Join、Sort → Sink 峰值内存形成平台期。
- [ ] 1、2、4、8 worker 的吞吐扩展有报告；无收益时不默认增加 worker。
- [ ] Spill 1 次、多次、多级 merge 分别记录吞吐、写放大和峰值内存。
- [ ] Collect 与 Sink 分开测量，不能用小结果 Collect 证明大结果流式能力。

### 7.3 每阶段命令门槛

- [ ] `./scripts/validate.sh`
- [ ] 目标包 `go test -race`
- [ ] `go vet ./...`
- [ ] GoDoc 检查和 `go run ./internal/cmd/gensimd -check`
- [ ] scalar、Auto、forced-SIMD、无 SIMD、linux/amd64 交叉构建
- [ ] `git diff --check`
- [ ] 新增可复现 benchmark 脚本和 `validation/streaming-*.json` 决策记录

## 8. 风险清单

- [ ] goroutine/channel 图发生环形等待，取消后无法退出。
- [ ] Batch ownership 不清导致 use-after-release、Arrow ref 泄漏或重复释放。
- [ ] 队列按 Batch 数而非字节限制，宽表仍然 OOM。
- [ ] reservation 与 Go slice capacity 不一致，造成系统内存显著超预算。
- [ ] Spill 时内存峰值出现“旧状态 + 编码缓冲 + 新状态”三份并存。
- [ ] Join 高放大倍数生成单个超大 Batch。
- [ ] stable Sort 在多 run merge 时丢失原始 ordinal。
- [ ] skew 分区递归 Spill 无法收敛。
- [ ] 不可重放 Source 的多分支缓存退化为隐式全量物化。
- [ ] Prepared 执行共享可变 stream/spill 状态导致数据竞争。
- [ ] SIMD 微基准变快但 morsel 端到端因尾部、复制或调度回退。
- [ ] 临时文件错误路径、进程崩溃和测试 panic 留下孤儿数据。

## 9. 已决定的设计选择

1. **调度模型**：pull-first，只有并行/分支边界使用有界 Exchange。
2. **执行策略**：`Auto` 默认，另有 `MemoryOnly` 和 `Bounded`；不存在不受预算的静默回退。
3. **易用 API**：普通查询保持 `Collect(ctx)`；大结果优先原生 Sink，通用 Batch API 使用借用式 iterator。
4. **资源默认值**：嵌入式零配置不隐式占用临时盘；bounded 模式要求显式有限内存和 Spill 配额。
5. **Spill 格式**：行 run 使用 Arrow-shaped 格式，聚合/Join 状态用专用 codec，共享同一存储生命周期。
6. **Source 重放**：显式声明 `Replayable` 和 snapshot identity，不能根据类型名称猜测。
7. **顺序语义**：只维护逻辑计划已经承诺的顺序；无显式 Sort 的 GroupBy/Join 不承诺偶然行序。
8. **首个纵向切片**：Scan → Filter → Projection → Spillable GroupBy → 内部 DrainSink；稳定后再公开 Batch Sink。
9. **兼容策略**：public Lazy/DataFrame API 延续，内部物理接口直接重构；Source Adapter 只设有限迁移期。
10. **默认 morsel**：不照搬固定 chunk size，按 GoLume Schema 字节宽度与目标机基准校准。

## 10. Polars 参考边界

参考 Polars 的是可验证的架构原则，而不是承诺 GoLume 与某个 Polars 版本内部实现一致：

- Polars 官方仓库将 streaming、multi-threading、SIMD、lazy optimization 和 Arrow
  columnar format 列为组合能力：<https://github.com/pola-rs/polars>
- Polars Sources/Sinks 文档说明 scan 可在文件读完前开始批处理，Sink 可避免把完整结果
  放入 RAM，并支持多个 Sink：<https://docs.pola.rs/user-guide/lazy/sources_sinks/>
- 新 streaming engine 跟踪页展示了 Source、Sink、GroupBy、Join、Sort、Window 和
  out-of-core multiplexer 的逐节点推进方式，也明确记录了部分节点可能回退：
  <https://github.com/pola-rs/polars/issues/20947>
- Polars 自身也存在 multiplexer 计划导致内存风险的公开讨论。这说明 GoLume 的 Explain
  必须区分“运行在 streaming engine”与“真正 bounded memory”，不可只标一个
  streaming 标签：<https://github.com/pola-rs/polars/issues/27454>

GoLume 的最终目标是：列式布局让 SIMD 有连续数据，Lazy optimizer 减少必须处理的数据，
streaming/backpressure 限制同时存活的数据，Spill 处理不可避免的全局状态，Sink 避免结果
重新全部进入内存。这五项一起成立，才是可持续的“像 Polars 一样快”。
