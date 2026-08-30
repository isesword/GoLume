# ADR：Phase 3.4 有界 Join、策略选择与 SIMD 门槛

状态：已决定并完成 Phase 3.4 纵向实现（2026-08-30）。

## 背景与用户体验

Join 最容易把“逐 Batch 读取”误称为“有限内存”：Broadcast Hash 会让 Build 表随输入增长，
重复 key 会让输出远大于两侧输入，Right/Full Join 还必须保存未匹配状态。GoLume 因此不提供
一个掩盖这些差异的 `streaming=true` 开关，而是在同一 LazyPlan 和物理执行图中选择：

```text
稳定可重放 + 实测小 Build ──► broadcast-hash（最快）
两侧完整且兼容排序证明    ──► merge（不建 Hash 表）
有限预算 + 显式 Spill     ──► partitioned-hash-spill（硬边界）
```

普通用户仍写：

```go
df, err := left.Join(right, opts).Collect(ctx)
```

不需要选择 radix 位数、morsel 大小或哪一侧落盘。需要硬资源合同的宿主只在 Session 设置
`ExecutionBounded`、内存上限和临时盘配额；`ExplainPhysical`/`ExplainAnalyze` 显示实际策略和
原因。`Collect` 仍是 Memory Sink，完整结果必须适合内存；超大结果通过内部 Batch Drain
完成验收，公开 iterator/文件 Sink 在 Phase 3.6 提供。

Polars 的 streaming engine 也组合 pipeline、hash/merge 算法、并行和 SIMD，但公开问题说明
multiplexer 或必须等待另一分支的计划仍可能缓存大量数据。GoLume 因此只把“每项状态都已
预算或可 Spill”称为 bounded，不把执行器名称当作保证：

- <https://github.com/pola-rs/polars/issues/20947>
- <https://github.com/pola-rs/polars/issues/27454>

## 决策

### 1. planner 只依据可证明的字节边界选择 Broadcast

有限预算下，Broadcast 必须同时满足：输入可稳定 replay、Build 实际或保守字节数已知、
最大输出 morsel 已知，且 Build 和输出各不超过 query hard limit 的四分之一。准备阶段可对
稳定 source 做一次有预算的试建；任何统计不确定、实际超限或 reservation 失败都释放试建
状态并选择 Partitioned Hash，不静默越过预算。

无限预算的 Auto/MemoryOnly 保留原有 resident 快路径。相同基线提交的 10×1 秒 resident
基准中，新路径中位数 3.558 ms，旧路径 3.541 ms（+0.489%），通过不超过 3% 的端到端
回退门槛。

### 2. Partitioned Hash 双侧 Spill，逐分区驻留

两侧使用相同 4-bit radix 和完整 64-bit hash 写入版本化 `ColumnarRunCodec`；一次只恢复一个
右侧 Build partition，再逐 run Probe 左侧。Build 仍超预算时消费后续 hash bit 递归分区，
最多 16 层。父 run 在子 run 成功提交后立即删除；所有 handle、codec buffer、gather、Hash
表、matched bitmap、pair/output workspace 和 run metadata 都挂在 query/operator budget 下。

固定 composite key 使用精确 128-bit key；Dictionary 跨 domain 先按实际字符串建立精确
canonical code；不能仅比较 code 或 64-bit hash。payload codec 支持现有 scalar、Dictionary、
`List<String>` 和 Struct，恢复后仍执行 schema fingerprint、长度和 checksum 校验。

单 key skew 不会无限 repartition：达到 radix 极限后进入 run-by-run 专用算法，以两个输入
morsel和一个输出 morsel为上界；若多个不同 key 在全部 hash bit 上碰撞，则返回
`ErrJoinSkewExhausted`，不退回无界常驻表。

### 3. 高放大输出必须分块

Join pair 和物化输出使用从 schema/实际变长数据推导的预留，单批最多 4,096 行且还受预算
动态缩小。输出先写 query-owned run，`Next` 每次仅恢复一个 Batch；下游提前停止、取消、
磁盘配额、损坏 run 和部分物化失败都会删除仍拥有的文件并释放 reservation。

验收覆盖 1×10,000 重复 key，结果明显大于 128 KiB 执行预算并通过多个 Batch 排空；另有
262,144 行、3 MiB 输入在 256 KiB hard limit 下完成，输入为预算 12 倍。这里的 bounded
含义是峰值不越界，临时盘和 metadata 也有显式上限；耗尽任一配额会返回资源错误，而不是
OOM 或偷偷改用 resident 路径。

### 4. Merge Join 依赖完整排序证明

只有两侧 ordering 与全部 equality key 的方向和 NULL placement 兼容时才选择 Merge。
算法跨 Batch 游标推进，只保留当前相等 key group，并把 cross product 切成有界输出 Batch。
Inner/Left/Right/Full/Semi/Anti 与 NULL 不匹配语义均与 Hash Join 一致。

单个超大重复 group 仍可能与输入规模相关，所以 Explain 将 Merge 标为 `BLOCKING`；严格
`ExecutionBounded` 不选它，而使用可递归 Spill 的 Partitioned Hash。无显式 Sort 的 Join
不承诺偶然行序；Merge 的选择不能成为新的公开排序合同。

### 5. 外连接和生命周期是一等语义

Right/Full 在 resident Broadcast 中使用一个 query-wide matched Build bitmap，因此该路径
固定单 pipeline worker；Spill Join 的 bitmap 只覆盖当前 partition。Probe 缺失行使用内部
负索引，由 materializer 为对应 payload 生成 NULL；左右 schema nullable 属性同步修正。
legacy Reader 不支持完整 Right/Full 时明确拒绝，调用 native BatchStream，避免返回看似成功
但缺少未匹配 Build 行的结果。

Prepared 每次执行创建新的 query budget、run registry 和 matched state，不复用上次临时
文件。Batch 仍遵守借用生命周期；Merge 对 List/Struct group 在物化完成前持有底层 Batch。

## SIMD 决策

Join 的连续 fixed-width gather、已有表达式和 Hash table kernel 继续支持 scalar、Auto、
forced-SIMD 与 `GODEBUG=simd=0`。但本阶段提出的“把 Partitioned Join 的 pair-key hash
批量送入 `Hash64Batch`”候选没有进入 Auto：Apple M4 Pro、darwin/arm64 的 10×同负载样本
中，scalar 中位数 773.134 ms，Auto 中位数 799.337 ms，端到端回退 3.389%，超过 3% 门槛。

最终分区器调用标量 `hashagg.Hash128`，避免额外 input/hash/validity scratch。强制 SIMD
模式仍能对已获准的其他 kernel 使用 SIMD，并在这个不适合的步骤正确回退。以后若要重开，
必须按“具体操作 × 架构 × 宽度/基数”重新提交微基准和 Lazy 端到端 10×样本，不能因为
Polars 使用 SIMD 就默认这里也更快。

## 可观测性与反馈

`ExplainAnalyze` 报告 strategy、build/probe rows/bytes、matched/output rows、spill runs/bytes、
query peak memory、最大 radix depth、最大 Build partition 和最大 multiplicity。完成且覆盖完整
partition 集合的执行把 Build/Probe bytes、放大倍数、multiplicity 和 Spill bytes 写入带稳定
source identity 的 EWMA runtime feedback；失败或部分执行不污染后续计划。

机器验收和原始样本见
[`validation/phase34-join-gate.json`](../validation/phase34-join-gate.json)。
