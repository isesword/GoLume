# ADR：Phase 3 查询预算与 Spill 基础设施

状态：已决定并完成 Phase 3.2 纵向基础实现（2026-08-30）。

## 背景与目标

GoLume 的 streaming 不能只表示“按 Batch 调用算子”。真正的 bounded execution 还要求：

- 每个输入相关状态都能归属到查询和算子；
- 并行 Exchange、算子状态、工作区、结果和 Spill I/O 不会分别绕过同一硬上限；
- 内存压力由查询统一仲裁，算子不能自行猜测宿主机剩余内存；
- 需要全局状态的算法可以安全落盘，且取消、损坏和失败不会遗留 reservation 或文件；
- 普通用户继续使用 `Collect(ctx)`，不需要理解 partition、run 或 codec。

Polars 的统一 streaming collect/sink 和增量 physical graph 是有价值的参考；但其官方
tracking issue 仍允许尚未原生支持的节点透明回到内存执行，公开的 multiplexer OOM
讨论也说明“处于 streaming engine”不是有限内存证明。因此 GoLume 将易用的 Auto 策略
与可验证的 Bounded 合同分开，严格模式不允许静默物化或无界缓存：

- <https://github.com/pola-rs/polars/issues/20947>
- <https://github.com/pola-rs/polars/issues/27454>

## 决策

### 1. 分层预算是唯一内存入口

```text
EngineBudget
  └── QueryBudget
        ├── OperatorBudget: HashAggregate / retained-state
        ├── OperatorBudget: Exchange / in-flight-batch
        ├── OperatorBudget: Project / workspace
        ├── OperatorBudget: Collect / output
        └── OperatorBudget: SpillRestore / spill-buffer
```

五类内存分别是 `retained-state`、`in-flight-batch`、`workspace`、`output` 和
`spill-buffer`。一次 reservation 同时计入 Operator、Query 和 Engine，释放使用幂等
对象完成，因此错误路径不会重复扣减。`BudgetSnapshot` 提供当前分类/算子用量、Spill
请求次数和实际释放字节。

soft limit 是提前发起 Spill 的压力阈值，hard limit 是不可跨越的查询上限；多个查询还
共同受 Engine hard limit 约束。软阈值默认取硬上限的 80%，也允许显式配置。

### 2. Spill 是内存管理器发起的协作协议

状态型算子通过 `SpillableConsumer` 注册可释放字节。仲裁器按可释放量排序并串行请求，
以后到达的压力先观察前一次已经释放的字节，避免多个算子同时抖动式落盘。

仲裁器调用 spiller 时使用内部 arbitration context。编码/恢复缓冲仍正常计入
`spill-buffer` 和 hard limit，但不会在持有仲裁锁时再次触发 soft spill；这避免递归
死锁。若“旧状态 + 必需编码缓冲”本身超过 hard limit，操作明确返回 `ResourceError`，
不能绕过预算。后续状态型算子应在 soft/hard 之间保留可完成一次 Spill 的 headroom，
并尽可能按 partition/run 流式编码，避免三份状态同时存活。

### 3. 默认不使用磁盘，首次需要时才创建目录

用户配置保持集中且易发现：

```go
session := lazy.NewSession(engine.Options{
    MemoryLimitBytes: 2 << 30,
    Spill: engine.SpillOptions{
        Directory: "/fast-temp/golume",
        MaxBytes:  50 << 30,
    },
})
```

`SpillOptions{}` 表示禁用，不创建目录。只指定目录而没有正数 `MaxBytes` 是配置错误。
配置有效时，也只在某个 query 第一次调用 `SpillManager` 后创建 query-scoped 目录；
Engine/Session 初始化、Explain 和纯流水查询都没有临时盘副作用。`QueryBudget.Close`
关闭 store 并回收目录，DataFrame output reservation 仍可在之后由 `DataFrame.Close`
释放。

`ExplainPhysical` 顶部显示 `memory_limit` 和 `spill_limit`。`Collect` 仍是 Memory Sink；
结果超过内存预算时，typed error 建议改用后续的 `CollectBatches`/原生 Sink，而不是把
磁盘文件伪装成普通 DataFrame。

### 4. 查询级安全文件协议

- query 目录权限 `0700`，对象权限 `0600`；
- 文件名只含 query-local 递增 ID，不含源路径、列名或值；
- 写入顺序为临时文件、完整写、文件 `fsync`、关闭、原子 rename、目录 `fsync`；
- 磁盘配额在创建临时文件前预留，任一失败只回滚一次；
- `Delete` 幂等，删除失败不提前归还配额并允许重试；
- `Close` 清理整个 query 目录并保留首个清理错误；
- 进程异常退出后的清理由 `CleanupOrphanSpillDirs(base, age, now)` 显式执行，只删除
  名字符合 GoLume 前缀、不是 symlink 且超过年龄阈值的目录。库初始化不擅自扫描宿主机。

固定文件头包含 magic、版本、little-endian 标记、payload kind、codec 长度、Schema
SHA-256 fingerprint、partition、payload 长度和 payload SHA-256。Restore 在分配 payload
前校验 handle、Schema、版本、endianness 和文件长度；读取后再校验 checksum。截断、
尾随、错版本、错 Schema 和坏 checksum 统一满足 `errors.Is(err, ErrSpillCorrupt)`。

### 5. Columnar run codec

首个 `arrow-shaped-columnar-v1` codec 使用版本化 little-endian 列式 buffer：validity、
offsets、连续数据和固定宽 values 分开保存。它支持当前专用 fixed vectors、String/Binary、
dictionary String、`List<String>` 和受 Phase 2.21 限制的 Struct；保留 NULL List、空 List、
NULL element 和 Struct parent validity。Decimal、任意 Arrow opaque 及未支持嵌套明确拒绝，
以后使用单独的 Arrow IPC codec，不能退化成 gob 或 `interface{}` 序列化。

Encode 在分配完整 payload 前按精确大小 Reserve。Restore 在分配 codec/payload 前 Reserve；
Decode 在重建向量前 Reserve 一个不小于所有目标 buffer 的 payload 上界。Reader 在根据
文件中的 row/offset count 创建 slice 前先验证剩余字节，避免损坏头部诱发巨额分配；
解码结束后重新执行 Schema、offset、dictionary index 和 nullability 校验。

初版不压缩、不加密，也不在 codec/scheduler/filesystem 路径增加 SIMD。压缩只有在同一
数据集、目标架构和临时盘上证明端到端吞吐提高且 CPU/峰值预算可接受后才能启用；SIMD
继续只用于 `kernel.Set` 管理的连续 Batch 热路径。

## 错误和生命周期

- memory hard limit：`ResourceError`，包含 scope/operator/class/requested/used/limit/hint；
- spill disk quota：`SpillResourceError`，支持 `errors.Is(ErrSpillQuotaExceeded)`；
- 未配置 spill：`ErrSpillUnsupported`；
- query/store 已关闭：`ErrQueryBudgetClosed` / `ErrSpillClosed`；
- 文件或 codec 损坏：`ErrSpillCorrupt`。

`EncodedColumnarRun`、`RestoredSpill` 和 `DecodedColumnarRun` 都拥有幂等 `Close`。成功、
取消、短写、磁盘满、预算不足、损坏和删除失败路径均由测试核对 memory/disk 使用归零。
PreparedFrame 将 query store 清理错误与主错误合并；若成功结果之后才发现 query cleanup
失败，会先关闭 DataFrame 释放 output reservation，再返回错误。

## 验收结论

- 嵌套 round-trip 覆盖 plain/dictionary String、NULL/空 `List<String>`、NULL element
  和 Struct parent NULL；
- fault tests 覆盖取消、短写、磁盘满、目录同步失败、配额、删除重试、版本/Schema/
  checksum/offset/row-count 损坏、恢复预算失败、孤儿目录和重复 Close；
- 最小 Spillable operator 在 4 KiB hard limit 下累计处理超过 10 倍预算的数据，真实
  触发 soft-limit 仲裁，峰值不超过 hard limit，最后 memory reservation 和 disk quota
  均为 0；
- Phase 3.2 没有新增 SIMD candidate，scalar/Auto/forced/no-SIMD 只验证语义一致，不改
  现有 allowlist。

机器可读验收记录见
[`validation/phase32-memory-spill-gate.json`](../validation/phase32-memory-spill-gate.json)。
