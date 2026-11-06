# ADR：Phase 3 内部 BatchStream 契约

状态：已决定。下文历史 Adapter 说明已由 [STREAM_MIGRATION.md](STREAM_MIGRATION.md)
的单流接口收口取代；Reader/LegacyReaderSource 已删除，destination-fill 协议不变。

## 背景

GoLume 现有 `source.Source` 将输入表示为 Partition，并由 `Reader.ReadPartition`
把一个 Partition 填入一个调用方提供的 `memory.Batch`。这个接口保留了批内列式执行和
零拷贝 Source 绑定，但把“输入分区”和“输出 Batch”隐式绑定为 1:1，无法自然表示：

- Filter 消费一个 morsel 后没有输出；
- Join 的一个输入 morsel 产生多个有界输出 Batch；
- Exchange、External Sort merge 或文件 Sink 按下游需求持续拉取；
- 下游提前结束后，不再读取尚未需要的输入。

Phase 3 需要一个仅供物理引擎使用的增量协议。它不是 public DataFrame iterator；
public 借用式 iterator 在 Phase 3.6 基于该协议封装。

## 比较的原型

在同一 Apple M4 Pro、darwin/arm64、同一构建下比较两种单消费者协议。每个样本执行
1024 次 `Next`，复用同一列向量和 Batch，运行 10 个 1 秒样本：

```text
destination fill: 6.309 us/op, 0 B/op, 0 allocs/op
reusable lease:   6.861 us/op, 0 B/op, 0 allocs/op
delta:            +8.74%, p=0.000, n=10
```

可复用 lease 已消除对象分配，仍因额外接口调用和状态转换稳定变慢。它没有给当前
pull pipeline 带来足以抵消开销的能力；跨 Exchange 的 ownership 可以由 Exchange
内部拥有 Batch、`Next` 时再把借用绑定填入 `dst` 表达。

## 决策

采用 destination-fill 协议：

```go
type BatchStream interface {
	Next(ctx context.Context, dst *memory.Batch) (ok bool, err error)
	Close() error
}
```

`BatchStream` 是内部单消费者对象，不允许并发调用 `Next`。同一 prepared plan 的并发
执行必须各自创建独立 stream、reader、预算和算子状态。

### Next 与 0..N 输出

- `ok=true, err=nil`：`dst` 含一个输出 Batch，包括语义上有效的零行 Batch。
- `ok=false, err=nil`：流自然结束；后续 `Next` 仍返回自然结束。
- `err!=nil`：本次失败且流进入 terminal error；后续 `Next` 返回同一错误。
- 一个物理算子的单次 `Next` 可以在内部消费零个或多个输入 morsel，直到获得一个
  输出 Batch、到达 EOF 或失败。因此一个输入可以产生 0、1 或多个输出，而不会把
  `ok=false` 误作“本次暂时无输出”。
- Native stream 不暴露 Partition 数量作为输出 Batch 数量。旧 Reader Adapter 为兼容
  现有空结果 Schema，迁移期仍保留每个 Partition 一个 Batch；这个行为只能存在于
  Source Adapter 边界，Phase 3.7 删除物理层 Reader 兼容分支。

### Batch ownership

- `dst` 由调用方创建并复用；stream 可在当前 borrowed view 的生命周期内保留
  `dst` 清理引用，以便下一次 `Next`/`Close` 释放 slot。调用方不能并发操作该
  destination，或在借用结束后继续使用其中的旧绑定。
- `ok=true` 返回的列绑定是 borrowed view，只保证到下一次 `Next`、调用方清空 `dst`
  或 stream/query `Close` 之前有效。
- 下游要跨越该边界保存值，必须显式 `memory.CloneBatch`/Copy，并承担所得 owned Batch
  的释放与 reservation。
- 下一次 `Next`、EOF 和错误返回前必须清掉旧绑定。`ClearColumns` 会释放 `dst` 中
  标为 Owned 的 source-native 引用，防止复用时泄漏。
- stream/reader 拥有其内部输入 Batch、workspace、Exchange slot 和 Arrow retain；
  下游不得修改借用向量或调用会转移其所有权的操作。

### Close、取消与错误传播

- `Close` 幂等；第一次调用按“下游 → 当前算子 → 上游 Source”顺序释放，后续调用
  返回同一 close result，不再次释放。
- 下游提前停止必须调用 `Close`；pull 模型因此不再读取其余输入，也不需要后台
  goroutine 排空数据。
- `ctx` 取消在每次 `Next` 边界检查，并沿调用栈向上传播。Phase 3.1 的 worker 和
  Exchange 使用 query-owned 派生 context；任一 sibling 失败会取消整个查询树。
- 数据错误是主错误，关闭错误使用 `errors.Join` 保留；不能用清理错误覆盖解码、预算、
  取消或磁盘错误。EOF 触发的关闭错误作为 terminal error 返回。
- 返回错误前清空 `dst`。部分输出不是成功 Batch，不能被 Collect 或 Sink 提交。
- `DrainBatchStream`、Collect 和后续 Sink 在所有成功、失败、取消及提前停止路径上
  调用 `Close`。panic recovery 属于 public callback Sink 边界，不在每个算子 hot loop
  增加 defer/recover。

### Replay 与稳定顺序

- 单个 `BatchStream` 不可 rewind/replay。
- prepared plan 的再次执行通过 `BatchStreamProvider.NewBatchStream` 或
  `Source.NewReader` 创建新流。Source 必须自行声明并保证其快照/重放语义；引擎不能
  假设不可重放 Source 的第二次读取观察同一版本。
- Phase 3.1 在内部 morsel envelope 增加稳定 `Sequence`；需要稳定语义的 fan-in、Sort
  和 Join 使用它或原始 `RowOrdinal`，不依赖 worker 完成顺序。
- fan-out 不通过无界缓存伪造 replay。可重放 Source 优先重扫；不可重放 Source 只能
  使用受 QueryBudget 约束的 multiplexer，否则 Prepare 明确失败。

## SIMD 边界

`BatchStream`、调度、Exchange、Spill codec 和 public iterator 不增加 SIMD 分支。
SIMD 继续只存在于 `kernel.Set` 的批内扫描、比较、bitmap、take 等已基准证明的热路径。
stream/morsel 改造若增加复制、尾部处理或分配，即使 kernel 微基准不变，也必须由端到端
门禁拒绝。

## 验证与后果

- protocol benchmark 的原始样本和机器可读决策保存在 `validation/`；
- pipeline benchmark 覆盖 1K、4K、16K、64K morsel，并比较旧 Reader loop 与
  Adapter stream；单 worker 端到端回退门槛为 3%；
- fault-injection 覆盖 read error、close error、取消、自然 EOF、重复 Close 和下游
  提前停止；race 验证 prepared plan 的独立执行状态；
- destination-fill 是内部冻结协议。若 Phase 3.1 的真实 Exchange 证据证明 lease 更快
  或显著简化 correctness，可以提交新 ADR 和同等 10×1 秒证据替换，不能在 public API
  中同时暴露两套 ownership 模型。

Phase 3.0 最终隔离基准结果如下。每格为 benchstat 中位数；16K stream 的一个
7.17 ms 离群样本原样保留在输入中，未手工删除：

| morsel rows | Reader | Native stream | delta | p | allocation delta |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,024 | 6.246 ms | 6.271 ms | +0.40% | 0.009 | +3/query |
| 4,096 | 6.000 ms | 6.018 ms | +0.30% | 0.218 | +3/query |
| 16,384 | 5.911 ms | 5.918 ms | +0.11% | 0.315 | +3/query |
| 65,536 | 5.893 ms | 5.880 ms | -0.21% | 0.353 | +3/query |

四档几何均值为 +0.15%，低于 3% 回退门槛。额外 3 次分配和约 224 bytes 来自一条
query 的 Filter/Projection/Adapter stream 对象，与 morsel 数无关；不存在每 morsel
新增分配。完整机器记录见
[`validation/phase30-streaming-gate.json`](../validation/phase30-streaming-gate.json)。
