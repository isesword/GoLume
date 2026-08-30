# Extension points

## Expression

新增 predicate operator：扩 `expr.Op`、compile instruction、scalar correctness，再决定是否需要 kernel specialization。不要把 AST type-switch 放进 row hot loop。

## Kernel

`kernel.Set` 是 dispatch table。每个 SIMD kernel 必须同时有 scalar baseline、tail/NULL tests、Emulated fallback policy。`simd` 类型不得泄漏到 public API。

## Key backend

新增 key representation 时，建议步骤：

1. 在 `memory` 定义 physical vector/encoding；
2. compile 阶段得到一次性的 `GroupShape`；
3. 新增专用 hash/radix backend；
4. materialization 通过 `KeyPart`/source metadata 解码；
5. planner 设置 `KeyWidthBytes`、`RadixCapable`。

不要为了统一写成一个 reflection-based `HashMap[any]`。

## Dictionary source

实现 `source.DictionaryProvider`，并在 schema Field 填稳定 `DictionaryDomain`。若 row-group dictionary 会改变，source adapter 必须 canonicalize codes 或暴露 plain/view encoding；不能谎称 domain 稳定。

## Source

物理执行扩展应实现 `source.Source` 的 metadata 方法与
`source.BatchStreamProvider`（必要时实现 prepared-partition 变体）。BatchStream 是
唯一的物理算子协议：一个输入可以产生 0、1 或多个输出 Batch，调用方借用 Batch 到
下一次 `Next`/`Close`，并由 query-owned budget 约束 Exchange 与状态。

旧版 Arrow RecordBatch、IPC、Flight、Iceberg/Delta scan 如果仍只有
`NewReader`，可在迁移期实现 `source.LegacyReaderSource`。`source.OpenBatchStream` 只在
这个外部 Source Adapter 边界执行一次 Reader 适配；Filter、Projection、Join、Sort、
Window 和 Aggregate 不得实现或调用 Reader。LegacyReaderSource 计划在 GoLume v0.8 删除，
新扩展应直接提供 BatchStream。`source/inmemory` 可用于 embedding 和 operator tests。

Source 还应声明 `Replayability()`：只有稳定快照才允许 Top-K/分支重扫，未知重放能力
必须进入有界 multiplex/spill 策略，不能静默缓存整张表。

## Cost model

实现 `planner.CostModel` 可完全替换 bootstrap heuristic。下一步应增加：sampled selectivity/cardinality、CPU calibration、runtime feedback、memory pressure input。

## Workspace

新的 retained scratch 必须由 `queryWorkspace` ownership 管理，并在 allocation 前 Reserve budget；不能私下放全局 pool。若资源不能估算，应增加 grow-hook/accounting。

## Join

`join.KeyCodec` 是后续 dictionary/composite key 的 coarse-grained 扩展边界。Key codec 应在 operator compile 时选定，不能在每次 primitive equality 时走 interface dispatch。

## Spill

以 radix partition 作为 spill unit。Memory reserve failure 应进入 SpillPolicy，而不是任意 operator 自己写临时文件。

## Observability

Observer 只记录 query/stage 级事件，禁止进入 row hot loop。
