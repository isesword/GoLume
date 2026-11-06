# 当前状态与开发顺序

本文区分当前原生执行架构、历史验证结果和本轮工作。起始基线为
`2c2162cc303429d07e723e9afc94f3e3c457717f`；下文也记录本轮工作树中已经实现的
流式 Analyze 与共同 budget 入口变更。验证状态单独列出，不继承历史 Phase 文档的
通过状态，也不表示这些变更已发布。

## 当前架构

- 一个 LazyPlan 经 optimizer、physical lowering 和共享 kernel dispatch 形成
  原生 Source 图。`source.Source.NewBatchStream` 是必需接口；旧
  `source.Reader`、整分区 fallback 与独立旧聚合执行树已删除。
- `source.BatchStream.Next(ctx, dst)` 向可复用 Batch 绑定借用向量；调用者不能
  跨下一次 `Next` 或 `Close` 保留借用输出。公共 `BatchReader` 同样是单消费者，
  `BatchView` 到下一次 `Next` / `Close` 即失效。
- 聚合、Join、Sort、Window 的物理策略由同一执行图选择。`ExecutionAuto` 是默认；
  `ExecutionMemoryOnly` 禁止 Spill；`ExecutionBounded` 要求有限预算和受支持的
  有界/可 Spill 中间状态，不是另一套引擎或静默回退开关。
- 聚合诊断使用实际策略：direct-fused、bitmap-direct、partitioned-hash、sorted
  或 spill-hash。独立的 radix 工具和 Join radix 算法仍存在，旧 aggregate
  `ForceStrategy="radix"` 不再支持。
- 谓词 `expr.Program` 与投影 `expr.ScalarProgram` 仍是不同表达式程序。
  共享 `expr.PredicateWorkspace` 和已复用的批内 kernel 不代表表达式架构已统一。

## 当前支持的结果 API

| 用途 | 入口 | 所有权与限制 |
| --- | --- | --- |
| 小结果 DataFrame | `Collect(ctx)`；`Prepare(ctx)` 后 `Execute(ctx)` | 完整结果驻留，必须 `DataFrame.Close()`；prepared plan 也要关闭 |
| 借用批次 | `CollectBatches(ctx, BatchOptions)`；prepared `ExecuteBatches` | 每批消费，不保留整表；提前结束必须关闭 reader |
| 回调消费 | `SinkBatches` | 回调内消费借用 BatchView，不能在后续迭代中继续使用 |
| 原生文件 | `SinkParquet` / `SinkIPC` | 流式编码、同目录 staging 和原子提交；结果文件与临时盘额度不是同一个概念 |
| terminal Aggregate 记录/计数 | `PrepareResult` / `CollectResult` | 根节点必须是 Aggregate；聚合后还有 Sort/Filter/Select/Limit 时使用 DataFrame/Batch/Sink API |
| 执行诊断 | `Analyze` / `ExplainAnalyze` | 单次原生流 drain，不保留 DataFrame；失败返回已采集的部分报告与 error |

`PreparedResult.Execute(..., engine.ExecuteOptions{Materialize: false})` 返回计数、
checksum、策略等诊断，不保留结果行。`Materialize: true` 和 `CollectResult`
返回的 `Result.Rows` 在收集时计费，成功返回后为调用者拥有的 Go 值，生命周期不再
计入已结束查询。DataFrame 的显式 `Close` 语义与此不同。

自定义 Source 必须实现原生协议。只有真正按顺序精确处理传入 partition 序列
（包括重复项和空序列）的 Source 才能实现 `PreparedBatchStreamProvider`。
具体迁移列表见 [STREAM_MIGRATION.md](STREAM_MIGRATION.md)。

## 流式 Analyze 契约

- `MaterializedRows` 已删除，不保留兼容字段。`OutputRows` 只统计成功观察的输出；
  `OutputBatches` 只计非空批次。失败时这些计数可能只覆盖部分执行。
- `FirstBatchDuration` 从打开 stream 开始，到第一个非空输出为止；未观察到非空
  输出时为零。`ExecuteDuration` 包括打开、消费和 stream/budget/plan 清理。
- `Termination` 为 completed、failed、canceled、budget-exceeded 或 cleanup-failed。
  `Error` 记录 prepare/open/Next 错误，`FailurePhase` 在此时为 prepare 或 execute；
  `CleanupError` 记录顶层 Close 错误。成功或仅顶层清理失败时 FailurePhase 为空。
  二类错误共同保留在返回 error 中，支持 `errors.Is/As`。
- 这是公共调用边界，不是全局根因分类：嵌套 Close 可能在 prepare/Next 内执行，
  其错误因此出现在 `Error`；有 profiling 时可另看 operator 的 `close_error`。
  仅在没有主错误而顶层 Close 失败时，Termination 才是 cleanup-failed。
- `ExplainAnalyze` 失败时同样返回部分文本，不能忽略 error。终端 Aggregate 的
  summary 与嵌套 Aggregate 的 operator diagnostics 分开；blocking state 仍须
  遵守普通执行的预算和 Spill 契约。

完整字段与计时说明见 [Analyze 迁移契约](STREAM_MIGRATION.md#streaming-analyze-and-partial-failure-reports)。

## Managed memory 的保证与边界

- `engine.DefaultOptions()` 默认 `MemoryLimitBytes=0`，不设硬内存额度，且 Spill
  关闭。严格 bounded 执行要求显式有限内存；需要 Spill 的状态策略还要配置正的
  `Spill.MaxBytes`。不支持的节点、超限值或不能安全计费的操作应明确报错。
- DataFrame、批次迭代、Analyze 与文件 Sink 共同采纳 context 中的外部
  `QueryBudget`，使用它的 root 和额度；它仍由调用者拥有并关闭。执行只释放自己
  的资源，不关闭外部 budget 或释放调用者 reservation。bounded 执行拒绝无限额
  外部 budget，不能借此绕过有限内存要求；实际执行 budget 的 Spill 要求也仍适用。
- reservation 管理纳入计费的 owned buffers、workspace、状态、in-flight payload
  和结果；增长先预留旧/新容量重叠。借用的 immutable Arrow / in-memory 输入
  不作为查询新分配重复计费。
- `ReadOptions.BatchRows` / `BatchBytes` 管理 decoded buffers 与 scratch；
  `BatchPayloadTargetBytes` 是软输出目标，`BatchPayloadBytes` 是单独的硬输出
  上限。软目标不是硬限，单个值超硬限不能靠继续切批解决。
- Parquet compressed/uncompressed page guards 各默认 8 MiB，在 page-body
  分配前检查 header。它们不是所有列、字典与 codec 合计的内存额度。
- Arrow decoder pages/dictionaries、codec 内部、固定元数据、Go 对象开销、
  runtime 与 GC 保留不等于 managed reservation 数值。这里**不承诺全进程 RSS
  硬上限**，也不保证任意大查询都能完成。
- `Collect` 仍需容纳完整结果。Top-K 的 O(K) 描述候选状态；不可重放输入可能需要
  受预算控制的快照。opaque payload 的 bounded gathering 和 Window frame
  有支持边界，不能把 “运行在 streaming engine” 等同于所有路径均能外排。
- `DefaultOptions.PreparedCacheBytes` 为 64 MiB，零关闭复用；只缓存受支持的
  prepared resident 固定宽状态和有界 scratch。idle cache 仍在 engine root
  计费，不能要求每次成功执行后 root 必为零；关闭 prepared plan/engine 会释放
  其缓存。见 [PREPARED_MEMORY_REUSE.md](PREPARED_MEMORY_REUSE.md)。
- Analyze 的 allocation delta 是进程级 Go runtime 计数，不是查询/算子独占值。
  `peak_memory_scope=query` 标记共享 query budget 高水位，可能包含外部调用者
  先前或同时的使用，不等于各算子或本次 Analyze 的独占峰值，不能跨算子求和。

## 验证与历史证据

本阶段流式 Analyze 与分支文档收口的实现、独立最终 review、本地完整
`scripts/validate.sh` 已通过。覆盖普通/SIMD、宿主支持的强制宽度、SIMD-disabled
fallback、Linux build、race、vet、GoDoc、结构/生成器检查及 demo，另有独立定向
检查。见 [本阶段验收记录](../validation/analyze-stream-review.md) 和
[完整日志](../validation/analyze-stream-full-validation.txt)。该结果使用已披露的合成
替代 fixture，不证明原始 fixture、GitHub Actions 或旧 W4 性能门槛通过；统一表达式
独立阶段未完成，不在本次验收范围内。

历史报告与原始数据保留不变，结论只适用于各报告记载的版本、输入和环境：

- [原生执行统一报告](../validation/native-stream-unification-review.md) 保留了
  失败的初始 engine/source 性能门槛、后续 source-only 通过结果及正确性检查。
  source-only 通过不代表 prepared W4 通过。
- [持续计费复用与共享内核证据](../validation/charged-reuse/README.md) 记录组合
  fixed-count 诊断中位数 13.338 ms，对照 WIP 为 17.309 ms；旧正式参考为
  10.04 ms，3% 门槛为 10.341 ms。诊断改善不是正式性能门槛通过。
- 上述报告中的本地验证使用已披露的合成替代 fixture。它们不证明与原始 fixture
  的数据分布或性能一致，也不构成原始 fixture 或 GitHub Actions 通过声明。
- [Phase 3 实施清单](PHASE3_STREAMING_ENGINE_TODO.md) 的阶段状态和
  `validation/phase3*-gate.json` 是历史记录，不是当前提交的总验收状态。
  当前提交必须有自己的检查结果；未运行、失败与通过必须分别标明。

### W4 优先级决定

旧 prepared W4 差距已接受延期，不再作为继续开发的前置条件。保留原始样本、阈值
和未达标事实，不把历史 FAIL 改成 PASS，也不继续为达到该阈值进行无证据的布局试验。
以后重新开展性能工作时，要在相同输入、预算、并发和目标平台下提供新的可归因证据。
这项范围决定本身不批准合并、不替代代码 review，也不免除后续功能的正确性验证。

## 近期分阶段工作

1. **分支文档收口，本阶段本地检查通过。** 已对齐当前文档与接口，保留历史证据，
   区分验证结果、延期目标和后续阶段；不据此声明发布或可合并。
2. **流式 Analyze 与失败诊断，已实现，本阶段本地完整验收通过。** 使用共同 stream
   入口 drain，删除 `MaterializedRows`，保留首批/批次数和部分失败诊断；统一采纳
   caller-owned external budget。独立 review、定向/race 检查及完整 validate 通过；
   具体覆盖与合成 fixture/未核 CI 边界见上面的验收记录。
3. **统一表达式架构，后续独立阶段，尚未完成。** 先明确共享 typed IR、类型与
   NULL/溢出语义、CSE、workspace ownership 和 budget 契约，再逐步迁移 predicate
   与 projection。用独立语义 oracle、预算/取消/生命周期测试和同条件基准验证每个
   切片，删除替代后的重复实现；不另建一套长期并存的表达式执行器。

统一表达式 IR 与后续性能工作不属于本次已验收能力。代码实现、定向检查、
全仓本地验收与发布是不同状态。

本阶段最终验证状态：本地完整 `scripts/validate.sh` exit 0；未声明发布、CI 通过或可合并。
