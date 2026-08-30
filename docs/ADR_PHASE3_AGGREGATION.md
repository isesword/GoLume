# ADR：Phase 3.3 有界聚合、排序聚合与常驻快路径

状态：已决定并完成 Phase 3.3 纵向实现（2026-08-30）。

## 背景与用户目标

`GroupBy` 是第一个能检验“streaming 是否真的等于 bounded memory”的全局状态算子。
仅把输入逐 Batch 喂给一个永不释放的 Hash 表，仍会随分组数增长到 OOM；反过来，把
所有查询都强制落盘又会伤害常见的小表和已有 SIMD/radix 快路径。

本阶段采用一个逻辑计划、三个物理策略：

```text
已证明按完整 group-key prefix 排序 ──► sorted-adjacent（有界，无需 Spill）
有限内存 + 显式 Spill 配额         ──► radix-spill（有界，可递归重分区）
无限预算或显式 MemoryOnly          ──► resident hash/radix（最快，不承诺有界）
```

普通用户仍调用 `GroupBy(...).Agg(...).Collect(ctx)`。选择策略不增加常用 API 参数；
需要硬保证的用户在 Session 或 LazyFrame 上选择 `ExecutionBounded`。`Collect` 仍是内存
Sink，因此完整结果必须适合内存；结果可能大于内存时使用内部 Drain 验证，公开 Batch/
文件 Sink 在 Phase 3.6 提供。

Polars 的 streaming GroupBy、source/sink 和 out-of-core 设计证明了同一 lazy physical graph
可以兼顾吞吐与落盘，但“运行于 streaming engine”本身不能作为有限内存证明。GoLume
因此把 Auto 易用性和 Bounded 可验证合同分开，并在 Explain 中公开每个物理节点的类别：

- <https://github.com/pola-rs/polars/issues/20947>
- <https://github.com/pola-rs/polars/issues/27454>

## 决策

### 1. 聚合状态保持类型专用 SoA

`aggregate.Int64Vector` 提供 `InitResult`、`UpdateBatch`、`MergeResult`、`MergeBatch` 和
`FinalizeBatch`。Hash 表为 I32、U32、U64、fixed pair 和 plain String 分别实现批量合并、
零分配 Visit 和 retained-byte 统计。热路径没有 `map[any]any`、逐行 interface dispatch 或
`[]any` 中间层。

当前纵向切片保持既有 SQL 语义：nullable/composite/dictionary key，以及 `SUM`、
`COUNT(*)`、`COUNT(value)`、`MIN`、`MAX`、`AVG`。状态 run 保存聚合中间状态和 validity，
不能只保存最终值，否则多次 Merge 会破坏 NULL、AVG 分母及 Count 语义。

### 2. Spillable Hash Aggregate 使用版本化状态 run

内存压力先将当前 Hash 状态按 hash 低 4 bit 写成最多 16 个 run；每个 run 使用
`aggregate-int64-state-v1` codec，包含 magic、版本、key shape、聚合 layout、validity 和
长度检查，并复用查询级原子 Spill store、校验和、配额与生命周期。

Merge 时一次只恢复一个 partition。若它仍超过状态预算，就使用后续 hash bit 递归重分区，
最多消费 64 bit；深度耗尽后返回明确 skew/collision 错误，不无限递归。输出状态也写成
分块 run，每次 `Next` 只恢复一个 bounded output Batch。取消、解码失败、配额不足和提前
Close 均删除仍由查询拥有的 run 并释放 reservation。

状态目标取 query hard limit 的 3/8，为“现有状态 + restore/encode buffer + output/exchange”
保留 headroom。实际 slice 扩容必须先按差额 Reserve；`BudgetSnapshot.PeakBytes` 记录查询
峰值用于测试和 ExplainAnalyze。

### 3. 有排序证明时使用 adjacent aggregation

`source.OrderingProvider` 传播确定的排序 prefix；planner 只有在 group key 完整匹配该 prefix
时选择 `StrategySorted`。算法只保留当前 key 和一组 SoA aggregate state，跨 Batch 延续
相邻分组，并按目标 morsel 行数持续输出。字符串当前 key 的复制计入预算。

调用方声明 InMemory ordering 是可信物理合同，而不是运行时猜测。没有 ordering proof 时
绝不选择 sorted aggregation。该策略在 32 KiB、无 Spill 配额下可 Drain 10,000 个唯一组，
证明输出本身大于预算时仍不需要全量物化。

### 4. resident radix 与 bounded radix 明确分流

旧 radix 一次压缩整个输入，可获得最佳吞吐，但 key/value 辅助数组与总输入行数线性相关。
有限 Engine budget 或 `ExecutionBounded` 下已改为 wave workspace：只保留
`workers × max-morsel-rows` 的分类 scratch，partition-owned Hash 状态跨 wave 合并；错误的
cardinality hint 触发可预算的 source replay，而不是保留整输入作 fallback。

无限预算的默认 Auto 和显式 MemoryOnly 保留 resident 一次性 radix。这个选择是公开的
资源/性能权衡，不声称 resident 路径有 bounded guarantee。10×1 秒 Apple M4 Pro 样本中，
新 resident 中位数 4.959 ms，基线提交中位数 5.003 ms（-0.896%，分配不增加），通过
端到端回退不超过 3% 的门槛；有限预算路径则以峰值合同优先。

### 5. Explain/Analyze 必须保留原生 stream

诊断 `ProfileSource` 现在转发 native `BatchStream`，不会因为开启 ExplainAnalyze 而偷偷
退回 legacy Reader/常驻聚合。执行报告附带：

- groups、selected rows；
- spill run/bytes；
- query peak memory；
- max radix depth；
- largest partition group count。

runtime feedback 继续记录实际 selectivity/cardinality；skew 指标进入 Analyze，为后续
planner 的分区位宽和 Join 决策提供数据，但本阶段不凭单次样本自动改变已准备计划。

## SIMD 决策

本阶段没有新增 Auto SIMD allowlist。固定宽单 key 的 partition hash 继续通过
`kernel.Set.Hash64Batch`，在已通过证据的 darwin/arm64 组合使用现有 unrolled backend；
linux/amd64 Auto 在缺少目标机证据时保持 scalar。pair、String、Hash 随机 probe、run codec、
文件 I/O、递归调度和 k-way 状态合并保持 scalar。

这保持清晰分层：SIMD 优化连续 Batch 内 hash/classification；bounded execution 决定同时
存活多少数据以及何时 Spill。强制 SIMD 和无 SIMD 模式都必须正确回退并保持相同结果。

## 验收结论

- resident 与 spillable 覆盖低/高基数、全唯一、nullable key/value、plain String、
  dictionary domain、fixed composite key 和全部现有 aggregate kinds；
- 递归 radix 测试构造低 4 hash bit 冲突，在 32 KiB 下进入至少两级 partition；
- Prepared 重复执行、预取消、codec 损坏、budget failure 和 Close 均无 reservation/run 泄漏；
- 65,536 行、8,192 组的 256 KiB 强制 Spill Drain 样本峰值为 68,989 bytes，平均写盘
  2,355,982 bytes，吞吐约 8.3K rows/s；该数据刻意使用极低预算验证正确性和平台期，
  不作为常驻快路径吞吐代表；
- 机器可读记录见
  [`validation/phase33-aggregate-gate.json`](../validation/phase33-aggregate-gate.json)。

