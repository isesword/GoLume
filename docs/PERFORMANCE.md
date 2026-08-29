# Performance evidence

数字用于架构判断，不是产品 SLA。

当前 SIMD kernel 的逐能力门控、forced 实际实现路径和 arm64 10-sample 结果见
[`SIMD_KERNELS.md`](SIMD_KERNELS.md)。`ModeAuto` 不再使用全局 SIMD 开关；
每个具体 KernelID 独立选择 backend。失败候选已从生产代码删除，不能通过
`ModeSIMD` 重新启用。

2026-08-29 M4 Pro 重构门禁的正式结果：Int32/Uint32 比较 kernel 提升
36.22%～72.38%，Float64 Sub/Mul/Div 提升 19.30%～19.61%，四路展开
Hash64 提升 31.91%；Float64 完整表达式提升 17.57%。最终 Auto allowlist
下 1M Parquet、Prepared Lazy 和 Radix Join 均未回退。UTF-8 ASCII-prefix
候选因混合中文表达式显著回退 4.12% 而被彻底删除。原始 1s x 10 样本和
benchstat CSV 由 `validation/simd-auto-gates.json` 索引。


## Phase 2.3 Lazy/pushdown regression

Lazy optimization and source pushdown happen during `Prepare`, before the
`Prepared.Execute` hot loop. A same-process comparison of direct PreparedQuery
and Lazy PreparedQuery on the shared validation host shows materially identical
allocation profiles (~36.86 MB/op, ~31.9K allocs/op). Short-query latency on the
shared host is noisy enough that the samples are treated only as a regression
check, not a stable speed claim.

More importantly, pushdown eliminates work rather than merely accelerating a
kernel. The Phase 2.3 fixture proves:

```text
4 source columns -> 3 physically decoded columns
3 row groups      -> 2 selected row groups for pred > 150
300 source rows   -> 200 rows entering residual scan/filter
```

The residual predicate remains active, so selected row groups still produce the
correct 149 matching rows. Projection and partition pruning therefore reduce
I/O/decode work without weakening SQL correctness.

## Phase 2.2 Prepared workspace regression

当前隔离 x86 host，真实 1,048,576-row Parquet，radix W4，Go 1.27 SIMD512，5 iterations × 5 benchmark repetitions：

```text
BenchmarkRadixW4 median:
  31.302 ms/op
  ~118.0 MB/op
  ~41.0K allocs/op

BenchmarkPreparedRadixW4 median:
  18.069 ms/op
  ~36.9 MB/op
  ~31.9K allocs/op
```

本轮结果中 Prepared steady-state latency 约为 one-shot 的 0.58x（约 1.73x throughput-side speedup），每次 allocation bytes 下降约 69%。

运行噪声会明显影响短 query，所以不能把单机数字当 SLA；更重要的是 allocation 结构和 Prepared lifecycle 的方向稳定。

## Real Parquet correctness

1M-row fixture 在 scalar、SIMD auto、SIMD emulation、强制 128/256/512 构建下均保持：

```text
groups   = 219196
selected = 472475
checksum = 236716301
```

## Apple M4 Pro Phase-0 external comparison

同一 Parquet/query，用户 ARM M4 Pro warm-cache median：

| Engine | Threads | Median ms |
|---|---:|---:|
| Go Arrow decode only | 1 | 6.929 |
| Go fused scalar | 1 | 16.229 |
| Go portable SIMD auto | 1 | 18.223 |
| Go radix pipeline | 4 | 5.451 |
| DuckDB 1.5.5 | 1 | 17.767 |
| DuckDB 1.5.5 | 4 | 8.491 |
| Polars 1.43.2 streaming | 1 | 10.493 |
| Polars 1.43.2 streaming | 4 | 3.686 |

M4 结果说明 narrow SIMD bitmap 并不天然优于 fused scalar，因此当前 heuristic 会同时考虑 predicate complexity 和 vector width。

## Prior kernel observations

- AVX-512 nullable filter -> packed bitmap：在测试 workload 中相对逐行 bitmap 构造超过 10x；
- byte structural scan：约 17x scalar；
- SIMD hash 单独约 2.5x，但完整 group-by 几乎无收益，说明 probe/cache 才是主导；
- radix ownership 在高 cardinality group-by/join PoC 中产生约 2x 级收益。

## Benchmark discipline

后续对比 Polars/DuckDB 必须保持：same file、same query semantics、correctness checksum、warmup、线程数固定、CPU/版本记录。

当前 Go query 路径仍有物理特化，因此结果首先证明 execution ceiling，而不是宣称 generic query engine 已经全面快于 DuckDB/Polars。
