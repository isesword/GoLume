# Extension points

## Expression

新增 predicate operator：扩 `expr.Op`、compile instruction、scalar correctness，再决定是否需要 kernel specialization。不要把 AST type-switch 放进 row hot loop。

## Kernel

`kernel.Set` 是 dispatch table。每个 SIMD kernel 必须同时有 scalar baseline、tail/NULL tests、Emulated fallback policy。`simd` 类型不得泄漏到 public API。

## Key backend

新增 key representation 时，建议步骤：

1. 在 `memory` 定义 physical vector/encoding；
2. compile 阶段得到一次性的 `GroupShape`；
3. 新增专用 native hash backend；
4. materialization 通过 `KeyPart`/source metadata 解码；
5. planner 设置 `KeyWidthBytes` 与适用的 native strategy capability。

不要为了统一写成一个 reflection-based `HashMap[any]`。

## Dictionary source

实现 `source.DictionaryProvider`，并在 schema Field 填稳定 `DictionaryDomain`。若 row-group dictionary 会改变，source adapter 必须 canonicalize codes 或暴露 plain/view encoding；不能谎称 domain 稳定。

## Source

物理执行扩展应实现 `source.Source` 的 metadata 方法与
`source.BatchStreamProvider`（必要时实现 prepared-partition 变体）。BatchStream 是
唯一的物理算子协议：一个输入可以产生 0、1 或多个输出 Batch，调用方借用 Batch 到
下一次 `Next`/`Close`，并由 query-owned budget 约束 Exchange 与状态。

`source.Source` 直接要求 `NewBatchStream`。`Reader`、`LegacyReaderSource` 和旧
Partition adapter 已删除，不存在兼容 fallback。外部 Arrow/IPC/Flight/Iceberg/Delta
扩展必须迁移为真正的增量流。只有能准确处理指定分区顺序/子集的 Source 才能实现
`PreparedBatchStreamProvider`；全局 Sort/Window/Limit 等只提供完整输入流。
具体破坏性变更见 [STREAM_MIGRATION.md](STREAM_MIGRATION.md)。

Source 还应声明 `Replayability()`：只有稳定快照才允许 Top-K/分支重扫，未知重放能力
必须进入有界 multiplex/spill 策略，不能静默缓存整张表。

## Cost model

实现 `planner.CostModel` 可完全替换 bootstrap heuristic。下一步应增加：sampled selectivity/cardinality、CPU calibration、runtime feedback、memory pressure input。

## Query-owned scratch

新的 scratch 必须由当前执行的 stream/operator 明确拥有，并在 allocation 前通过 query budget Reserve；扩容时同时计费旧、新缓冲，完成替换后才释放旧 reservation。Close、失败和取消必须释放引用与预算；不能私下放全局 pool 或恢复已删除的 queryWorkspace。若资源不能估算，应增加 grow-hook/accounting。

例外必须显式使用可回收、持续计费的 owner：当前 prepared resident cache 使用
ReusableAccount 在活跃 query 和 engine-root idle ownership 之间切换；闲置时仍计费，
全局 hard cap、压力驱逐和 Close 必须覆盖交接中的 entry。不得保留旧 query/context，
不得释放预算后继续保存 buffer。详见 [PREPARED_MEMORY_REUSE.md](PREPARED_MEMORY_REUSE.md)。

## Join

`join.KeyCodec` 是后续 dictionary/composite key 的 coarse-grained 扩展边界。Key codec 应在 operator compile 时选定，不能在每次 primitive equality 时走 interface dispatch。

## Spill

以 radix partition 作为 spill unit。Memory reserve failure 应进入 SpillPolicy，而不是任意 operator 自己写临时文件。

## Observability

Observer 只记录 query/stage 级事件，禁止进入 row hot loop。
