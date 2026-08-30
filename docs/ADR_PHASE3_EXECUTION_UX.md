# ADR：Phase 3 执行策略、有限内存与用户 API

状态：已决定，Phase 3 实现时按本文执行。

## 背景

“使用 streaming engine”和“查询具有有限内存上界”不是同一个承诺：

- Scan、Filter、Projection 可以逐 Batch 运行，但 GroupBy、Join、Sort 和 Window
  仍需要与输入规模相关的状态；
- 即使所有中间算子都受限，`Collect` 仍要把完整结果保存为 DataFrame；
- 一个标记为 streaming 的计划仍可能包含 Memory Sink、不可 Spill 状态或需要缓存
  全输入的分支复用；
- SIMD 只会缩短批内计算时间，不会自动限制 Hash 表、队列或最终结果的大小。

本 ADR 决定 GoLume 如何在不增加普通用户认知负担的前提下，提供可证明的资源边界。

## 调研结论

### Polars

Polars 当前稳定文档的 `LazyFrame.collect(engine="auto")` 默认仍解析到 in-memory；
指定 streaming 但无法执行时会回退，并需要 verbose 日志才能看到原因：
<https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.collect.html>

Polars 2.0 RC 计划让 Lazy `auto` 默认解析到 streaming，同时明确警告：未要求顺序的
GroupBy、Join 等操作可能改变偶然行序：
<https://docs.pola.rs/releases/upgrade/2/>

Polars 提供 `collect_batches`、`sink_batches` 和原生文件 Sink。官方文档提醒通用 Batch
回调/迭代比原生 Sink 慢，并提供 `maintain_order` 和 early stop：
<https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.collect_batches.html>
<https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.sink_batches.html>

Polars 新 streaming engine 使用 morsel sequence、consume token 和有界 pipe 做顺序合并与
背压；2026-04 合入的 OOC multiplexer 又为分支缓存接入 SpillFrame：
<https://github.com/pola-rs/polars/blob/main/crates/polars-stream/src/morsel.rs>
<https://github.com/pola-rs/polars/blob/main/crates/polars-stream/src/pipe.rs>
<https://github.com/pola-rs/polars/pull/26774>

但这不能等同于“所有 streaming 计划都有固定 RAM 上界”。Polars 1.40.1 的公开问题
展示了一个两遍依赖计划：multiplexer 的一侧必须等聚合完成，另一侧会缓存完整输入；
对可重放输入，更合理的计划是独立重扫，而不是为了共享扫描保留全表：
<https://github.com/pola-rs/polars/issues/27454>

采用的经验：

- 普通 `Collect` 应保持最短路径；
- 大结果必须有 Batch 和原生文件 Sink；
- 物理计划应显示非流式/内存敏感部分；
- 顺序必须由逻辑契约决定，不能依赖某个执行引擎的偶然行为。
- morsel 必须携带稳定序号、消费完成信号和 source stop 信号，背压应在所有权交接处生效。
- fan-out 先判断输入是否是同一稳定快照；可重放则重扫，不可重放才使用受预算控制、
  可在后续接入 Spill 的 multiplexer。

不采用的经验：

- GoLume 不在两个长期并存的物理引擎间透明回退；
- 不用单一 `streaming=true` 表示有限内存保证；
- 不要求用户为普通查询手动选择 chunk size。
- 不因共享一次 Scan 看起来更快，就允许 multiplexer 在 RAM 中无界积累。

### DuckDB

DuckDB 默认尽量自动完成大于内存的查询，并把 GroupBy、Join、Sort、Window 明确列为
blocking operators；用户主要配置 memory limit、temporary directory 和 temporary
directory size，而不是逐算子选择算法。官方也明确列出暂时无法 Spill 的聚合状态以及
多 blocking operator 仍可能 OOM 的限制：
<https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads>
<https://duckdb.org/docs/stable/configuration/overview>

采用的经验：

- 自动策略应是默认体验；
- 内存和临时盘必须分别设置硬边界；
- 不支持 Spill 的状态必须明确报错，不能暗示“任何查询都一定完成”；
- 资源错误应给出可操作的配置建议。

不直接采用的经验：

- GoLume 是嵌入式 Go 库，不能默认占用宿主机大部分 RAM 或磁盘；
- 不自动把系统剩余磁盘的高比例当作本进程额度。

### Apache DataFusion

DataFusion 的 `ExecutionPlan.execute` 返回 pull-based RecordBatch stream，Exchange
只承担需要的并行边界；官方文档认为这种模型可以获得与 push scheduler 相近的扩展性：
<https://docs.rs/datafusion/latest/datafusion/>

DataFusion 用 MemoryPool/Reservation 管理大内存消费者，并提供 Greedy/FairSpill
策略；官方同时说明并非每个流经算子的 Batch 都受默认 MemoryPool 追踪：
<https://docs.rs/datafusion/latest/datafusion/execution/memory_pool/trait.MemoryPool.html>

采用的经验：

- pull-first + 必要 Exchange 适合 GoLume；
- spillable consumer 之间需要公平仲裁；
- 内存失败信息应列出最大的消费者。

改进点：

- GoLume 的 bounded 模式必须同时统计 retained state、in-flight Batch、workspace、
  materialized output 和 spill buffer，不能把流经 Batch 假设为总是很小。

## 决策

### 1. 只有一套物理执行引擎

LazyPlan、optimizer、physical lowering、SIMD dispatch、memory manager 和 Sink 只保留
一套。`Auto` 在同一物理图内逐节点选择 pipeline、in-memory state 或 spillable state，
不是在“旧内存引擎”和“新流式引擎”之间回退。

因此用户看到的是一套结果语义：

```go
df, err := lf.Collect(ctx)
```

普通用户不需要理解 morsel、Exchange、radix partition 或 Spill run。

### 2. 执行策略分成三个级别

```go
type ExecutionPolicy uint8

const (
	ExecutionAuto ExecutionPolicy = iota
	ExecutionMemoryOnly
	ExecutionBounded
)
```

- `ExecutionAuto`：默认。流水节点增量运行；blocking 节点在内存内走快路径，达到
  压力阈值后在实现支持且已配置 Spill 时转入 Spill。
- `ExecutionMemoryOnly`：禁用 Spill，供小数据、诊断和基准使用；达到预算就失败。
- `ExecutionBounded`：要求所有与输入规模相关的中间状态都有预算上界或 Spill 实现；
  Prepare 时发现不满足就失败。

`ExecutionBounded` 不表示 `Collect` 可以返回大于内存的 DataFrame。它保证中间执行
有界；Memory Sink 仍受结果预算限制。

### 3. 简单 API 与高级控制分开

常用路径不增加参数：

```go
df, err := lf.Collect(ctx)
defer df.Close()

err := lf.SinkParquet(ctx, "result.parquet", lazy.SinkOptions{})
```

资源配置放在 Session/Engine：

```go
session := lazy.NewSession(engine.Options{
	Workers:          8,
	MemoryLimitBytes: 2 << 30,
	Spill: engine.SpillOptions{
		Directory: "/fast-temp/golume",
		MaxBytes:  50 << 30,
	},
})
defer session.Close()
```

高级用户可以在不可变 LazyFrame 上覆盖策略，不修改 `Collect` 签名：

```go
df, err := lf.WithExecution(lazy.ExecutionOptions{
	Policy: lazy.ExecutionBounded,
}).Collect(ctx)
```

策略名和结构最终放在哪个 package 由 Phase 3.0 API review 决定，但以上行为不再变化。

`Workers`、目标 morsel 和 in-flight byte limit 同样是 Engine 级上限，不是要求用户精调
才能获得合理性能的必填参数。Auto planner 会让过小 Batch 走直接 pull，避免 Exchange、
克隆和 goroutine 固定成本；只有分区大小足以摊销调度时才启用并行。Explain 显示最终
采用的 worker 数与降级原因，用户无需通过反复试 chunk size 猜测。

首轮 Apple M4 Pro 的 10×1 秒基准中，4 worker 相对 1 worker 吞吐提高 39.1%，8 worker
反而比 4 worker 低 1.83%。因此 production default 的 pipeline worker 上限暂定为 4；
Engine 的其他并行算子仍可使用完整 `GOMAXPROCS`，显式 `Pipeline.Workers` 也不受该默认
上限约束。后续跨架构和 Join/GroupBy workload 证据可以调整默认值，不能仅按 CPU 数翻倍。

### 4. 默认资源行为

- 为保持嵌入式兼容，`MemoryLimitBytes <= 0` 继续表示无显式硬上限。
- 未配置有限内存时，Explain 显示 `memory_limit=unbounded`；不能宣称 bounded execution。
- `ExecutionBounded` 要求正数内存上限，否则 Prepare 返回配置错误。
- Spill 只有设置 `SpillOptions` 后启用；GoLume 不静默占用系统临时盘。
- Spill 必须同时设置最大字节数；目录留空可以使用进程临时目录中的 query-scoped
  安全子目录，但仍需要 `MaxBytes > 0`。
- `ExecutionBounded` 计划只要包含可能随输入增长的 `SpillableStateOperator`，就要求
  已配置 Spill；不能用当前统计“看起来很小”代替资源保证。纯 pipeline 或 Top-K 这类
  已有显式状态上界的计划不要求 Spill。
- `Auto` 下未配置 Spill 时仍可执行估计适合内存的计划；实际预算不足时明确失败，
  不越过上限，也不创建隐式临时文件。

这个默认比“自动使用 80% RAM/90% 磁盘”略多一次配置，但对嵌入到服务中的 Go 库
更安全、可预测。

### 5. Collect、Batch 与 Sink 的责任边界

#### Collect

`Collect` 是 Memory Sink：结果必须适合配置的内存预算。它不会把 Spill 文件包装成
看似普通、实际按需读取的 DataFrame。

结果物化超过预算时返回 typed `ResourceError`，错误信息至少包含：

- `Operator=Collect`；
- requested、used、limit；
- 当前已物化 rows/bytes；
- 建议改用 `SinkParquet`、`CollectBatches` 或提高预算。

调用者可以稳定检查错误，而不解析字符串：

```go
var resourceErr *memory.ResourceError
if errors.As(err, &resourceErr) {
	log.Printf("operator=%s used=%d limit=%d hint=%s",
		resourceErr.Operator,
		resourceErr.UsedBytes,
		resourceErr.LimitBytes,
		resourceErr.Hint,
	)
}
```

#### CollectBatches

Go API 使用借用式 iterator，避免用户逐 Batch 手动 Release：

```go
batches, err := lf.CollectBatches(ctx, lazy.BatchOptions{})
if err != nil {
	return err
}
defer batches.Close()

for batches.Next(ctx) {
	batch := batches.Batch() // 只在下一次 Next 或 Close 前有效
	consume(batch)
}
if err := batches.Err(); err != nil {
	return err
}
```

需要跨 `Next` 保存数据时，调用者必须显式 Clone/Copy。提前退出只需 `defer Close()`，
不会要求每次循环都写一对 Release。

#### Sink

原生 `SinkParquet` / `SinkIPC` 是大结果首选，因为可以直接使用编码器、字典和 I/O
buffer，不需要每批跨通用 callback 边界。自定义 Batch Sink 是便利接口，不承诺快于
原生 Sink。

### 6. 不使用静默 fallback

`Auto` 的正常逐节点选择不叫 fallback。以下情况必须可见：

- `ExecutionBounded` 遇到不可 bounded 节点：Prepare 返回 `UnsupportedBoundedPlanError`，
  包含节点路径和建议；
- `Auto` 中估计适合内存、运行时却超过预算且不能 Spill：返回 `ResourceError`；
- Spill 配额用尽：返回包含 memory/spill 双重用量的 `ResourceError`；
- Source 不可重放而分支计划需要全量缓存：Explain 标记 `UNBOUNDED BUFFER`，
  bounded 模式拒绝；已配置明确字节上限时才允许 bounded multiplexer，并为每个下游
  保留独立配额，防止后来的分支占满共享额度造成稳定 fan-in 死锁；
- `Collect` 本身可能成为最大内存消费者：Explain 标记 `MEMORY SINK`。

不会因为一个节点不支持 Spill 就转到不受预算管理的旧执行路径。

### 7. 顺序语义保持简单

- 只有显式 `Sort` 或算子文档已经承诺的顺序才是 API 合同。
- Filter、Projection、Limit 保留其输入已声明的 ordering property。
- GroupBy 和 Join 在没有显式 Sort 时继续不承诺物理输出行序。
- SIMD、worker 数、Spill 次数和 MemoryOnly/Bounded 策略不能改变有合同的顺序。
- 不增加一个影响整个查询的 `maintain_order` 模糊开关；确有业务语义的保持顺序能力
  放在对应算子上，并反映在 physical ordering property 中。

这样不会像执行引擎切换那样悄悄改变用户原本有权依赖的顺序，也不会为没有合同的偶然
顺序付出全局排序代价。

### 8. 易用性依赖诊断，而不是更多开关

`ExplainPhysical` 顶部增加执行摘要：

```text
EXECUTION policy=auto memory_limit=2.0GiB spill_limit=50.0GiB workers=8
PIPELINE 0: ParquetScan -> Filter -> Project
EXCHANGE hash(key) in_flight_limit=128MiB
HashAggregate [SPILLABLE] estimated_state=1.3GiB
Collect [MEMORY_SINK] estimated_output=unknown
```

`ExplainAnalyze` 增加 peak memory、最大消费者、spill bytes/runs、backpressure time 和
实际 kernel backend。普通成功查询不打印警告；失败信息直接给出行动建议。

对用户文档使用三个不同词，避免把能力边界混在一起：

- `pipeline`：数据以 Batch/morsel 增量流过；
- `spillable`：随输入增长的状态在预算压力下可落盘；
- `bounded`：本次计划中所有中间状态均有硬上界或可 Spill，且不会静默进入无界路径。

因此 `Collect` 可以运行在 bounded 中间执行上，但它仍是 Memory Sink；若最终结果本身
大于预算，错误必须直接建议 `CollectBatches` 或原生 Sink，而不是让用户误以为
`ExecutionBounded` 失效。

## 被否决的方案

### 两套引擎 + 自动回退

否决原因：测试矩阵翻倍、结果顺序和边界语义容易分叉，用户难以确认实际执行方式。

### `streaming bool`

否决原因：布尔值无法表达 MemoryOnly、Auto 和 bounded guarantee，也容易让用户误以为
`streaming=true` 就能 Collect 任意大结果。

### 每个 Batch 都要求用户 Release

否决原因：Go 调用端容易遗漏，错误路径尤其容易泄漏。借用到下一次 `Next` 的 iterator
更符合现有 DataFrame/FrameView 生命周期设计。

### 默认自动占用宿主机大部分磁盘

否决原因：GoLume 可能嵌入 API 服务、桌面应用或测试进程，隐式大量落盘不可接受。

### 为保持偶然行序而默认全局排序

否决原因：没有逻辑合同的顺序不应成为性能负担；需要稳定业务顺序时显式 `Sort`。

## 验收

- [ ] 零配置小查询仍只需 `Collect(ctx)`。
- [ ] `ExecutionBounded` 在无内存上限、无 Spill 配额或含不支持节点时 Prepare 失败。
- [ ] 所有资源错误可通过 `errors.As` 检查，并包含 operator/used/limit/hint。
- [ ] `CollectBatches` 提前退出、错误和取消时只需一次 Close 即可释放所有资源。
- [ ] 相同 LogicalPlan 在三种策略下结果和已承诺顺序一致。
- [ ] Explain 能让用户在执行前识别 Memory Sink 和非 bounded 节点。
- [ ] 原生 Sink 的结果大于 RAM 时，峰值内存和临时盘均不超过配置边界。
