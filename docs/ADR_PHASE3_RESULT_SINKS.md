# ADR：Phase 3.6 流式结果 API 与原生文件 Sink

状态：已决定并完成 Phase 3.6 纵向实现（2026-08-30）。

## 结论

GoLume 保留一个物理执行图，并把结果出口明确分成三类：

1. `Collect(ctx)` 是易用的 DataFrame Memory Sink，结果本身必须适合内存；
2. `CollectBatches` / `SinkBatches` 是借用式通用批次接口；
3. `SinkParquet` / `SinkIPC` 是大结果的首选原生文件出口。

这与 Polars 值得借鉴的部分一致：lazy optimizer、批式 streaming、列式 kernel 和文件
Sink 共同工作，文件结果无需先形成整张 DataFrame。但 GoLume 不把“使用 streaming
engine”当作有限内存证明。每个 Batch、编码 workspace、Exchange 和状态算子仍必须经过
查询预算，`ExecutionBounded` 也不会让 `Collect` 自动变成磁盘 DataFrame。

## 用户 API

### 小结果：DataFrame

```go
df, err := query.Collect(ctx)
if err != nil {
	return err
}
defer df.Close()
```

`Collect` 的价值是最简单的随机访问、Series 和显示体验。它有意保留全部结果；结果超过
预算时返回 `memory.ResourceError`，并建议 Batch 或文件 Sink，不做静默落盘。

### 自定义增量消费：借用式 iterator

```go
reader, err := query.CollectBatches(ctx, lazy.BatchOptions{})
if err != nil {
	return err
}
defer reader.Close()

for reader.Next(ctx) {
	batch := reader.Batch()
	series, err := batch.Column("value")
	if err != nil {
		return err
	}
	consume(series)
}
return reader.Err()
```

`BatchView` 只借用当前 generation：下一次 `Next`、自然结束或 `Close` 会使旧 view、Series
和 typed chunk 失效。要跨批次保存值，调用者必须通过 Series 的 `Copy*` 或自己的编码器
显式复制。iterator 是单消费者 pull cursor；完整读完会自动释放，提前停止只需一次幂等
`Close`。

Prepared 计划可重复或并发打开独立 iterator：

```go
prepared, err := query.Prepare(ctx)
reader, err := prepared.ExecuteBatches(ctx, lazy.BatchOptions{})
```

每次执行拥有独立 QueryBudget、Batch、stream 和错误状态，不复用上次执行的可变数据。

### Push consumer 与有界 multiplex

```go
err := query.SinkBatches(ctx, lazy.BatchOptions{},
	func(ctx context.Context, batch lazy.BatchView) error { return writeA(batch) },
	func(ctx context.Context, batch lazy.BatchView) error { return writeB(batch) },
)
```

同一批次的 consumers 并发执行，上游只在全部 consumer 返回后才请求下一批。每个下游
最多持有一个借用 view；慢下游会背压整个分支，但不会形成无界共享队列。任一 consumer
报错或 panic 会取消 siblings、关闭查询并返回组合错误。

## Parquet 与 IPC Sink

```go
result, err := query.SinkParquet(ctx, "events.parquet", lazy.ParquetSinkOptions{
	Compression:  lazy.ParquetCompressionZstd,
	RowGroupRows: 64 << 10,
})

result, err = query.SinkIPC(ctx, "events-parts", lazy.IPCSinkOptions{
	SinkOptions: lazy.SinkOptions{
		MaxRowsPerFile: 1_000_000,
	},
	Compression: lazy.IPCCompressionLZ4,
})
```

- 没有分片阈值时，path 是单文件；设置 `MaxRowsPerFile` 或
  `TargetBytesPerFile` 后，path 是目录，文件名为稳定的
  `part-00000.parquet` / `part-00000.arrow`。
- `MaxRowsPerFile` 是严格上限。`TargetBytesPerFile` 是按未压缩列式数据估算的规划目标；
  单行可以超过目标，且压缩后的实际文件字节不要求等于目标。
- 文件与分片按物理输出流顺序写入。只有逻辑计划已承诺的顺序才是 API 合同；无显式
  `Sort` 的 GroupBy/Join 不因此获得新顺序承诺。
- Dictionary String 以普通 Arrow UTF-8 写出，使不同 Batch 的局部字典不会破坏单文件
  schema。List<String> 与首层 Struct 保留父/子 validity。
- `SinkResult` 只返回已提交的最终路径、总行数和实际文件字节。

空结果仍创建一个带 schema 的有效文件。单文件 `Overwrite` 使用同目录原子 rename；已存在
的分片目录不做伪原子覆盖，调用者应写入新目录后自行切换引用。

## 原子性与资源生命周期

Sink 在最终目标的同一父目录创建 staging 文件或目录：

```text
physical stream -> Arrow record -> native writer -> fsync -> rename -> parent fsync
```

格式 writer 看不到底层 `os.File.Close`，因此不能打乱 `writer.Close → file.Sync →
file.Close → rename` 的所有权顺序。提交前的取消、预算失败、编码失败和写入失败都会删除
staging；不会把临时路径放进 `SinkResult`。

Arrow allocator 的每次 Allocate/Reallocate 在分配前向当前 `OperatorBudget` 申请 workspace，
Free 时释放。Arrow 的 allocator 接口不能返回 error，因此预算失败用私有 panic 穿过第三方
编码器，并只在 native sink 边界恢复成带 `memory.ErrBudgetExceeded` 的普通 error。这个 panic
不会暴露给用户。编码器结束后会兜底释放失败路径仍持有的 reservation。

原生 codec 的进程级固定缓存不随输入增长；查询相关 Arrow buffer、RecordBatch 和列式编码
workspace 都受预算限制。Parquet 使用逐 RecordBatch row group 的非 buffered 写法，避免将
多个输入 Batch 聚成常驻 row group。

## 易用性边界

- 日常小结果继续只写 `Collect(ctx)`，不要求理解 streaming。
- 大结果写文件优先用 `SinkParquet` / `SinkIPC`；只有自定义协议才使用 Batch API。
- `DataFrame.String()` 和格式化只读取已物化 DataFrame，永远不会触发 LazyFrame 执行。
- `BatchOptions` 的零值继承 Session 的 morsel/in-flight 默认；错误的负数或不一致上限会在
  打开执行前返回。
- `BatchReader.Close`、writer Close、预算释放和 staging 清理都幂等。

## SIMD 决策

Phase 3.6 没有新增 Auto SIMD candidate。iterator 生命周期、文件系统、压缩调度、变长
List/Struct 转换和 writer metadata 都不是合适的 SIMD 边界。Sink 上游的 Filter、表达式、
hash/radix 等仍调用现有 `kernel.Set`，因此 scalar、Auto、forced-SIMD 和 no-SIMD 共用同一
结果 API。若未来优化 Arrow 转换，必须针对固定类型、架构和 morsel 宽度证明 kernel 至少
提升 10%、`p < 0.05`，并保证 Sink 端到端不回退超过 3%。

## 验收

- 多 Batch、旧 view 失效、提前 Close、Prepared 重复执行；
- 多 consumer 同批并发、慢 consumer 背压、error/panic 取消；
- Parquet/IPC 的 NULL、空字符串、NULL/空 List、NULL element 和 Struct 往返；
- 最大行数分片、目标字节规划、稳定 part 命名和输出顺序；
- target exists、覆盖写、空结果、取消、unsupported type 和低预算原子失败；
- 输出文件超过内存 hard limit 十倍仍可在 `ExecutionBounded` 完成；
- scalar、Auto、forced-SIMD、no-SIMD、race、vet、GoDoc 和 linux/amd64 构建。

机器可读证据见
[`validation/phase36-result-sink-gate.json`](../validation/phase36-result-sink-gate.json)。
Apple M4 Pro 的 10×1 秒样本中，百万行输入、50% 选择率的借用式 Batch 出口中位数为
5.701 ms/op、约 0.438 MB/op；DataFrame Collect 为 5.956 ms/op、约 4.678 MB/op；
未压缩 IPC Sink 为 26.991 ms/op，固定宽结果值吞吐约 155.4 MB/s。已有 16K morsel
batch-stream 相对 `e90606a` 的中位数变化为 -0.702%，`p=0.075`，分配数不变，未发现
相关纯内存路径回退。
