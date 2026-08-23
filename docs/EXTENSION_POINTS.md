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

实现 `source.Source + Reader` 即可新增 Arrow RecordBatch、IPC、Flight、Iceberg/Delta scan。`source/inmemory` 可用于 embedding 和 operator tests。

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
