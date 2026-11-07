# Generic grouping validation

`matrix-amd64.txt`: Go 1.27.3 linux/amd64, shared cloud host, GOMAXPROCS=1,
scalar default build, 32,768 input rows, 64/4,096 groups, one UInt64 key or three
keys (UInt64, string, UInt64), Int64 SUM and COUNT, summary-only consumption.
Three samples of 20 iterations, preparation outside timing, allocation reporting.

Command: `go test ./engine -run '^$' -bench '^BenchmarkGenericGroup$' -benchmem -benchtime=20x -count=3`

This is a throughput/allocation baseline for newly supported query shapes. There
was no equivalent supported query before this change. No speedup or cross-machine
performance gate is claimed. Canonical encoding is scalar; preallocated row
encoding, hashing and visiting have zero-allocation regression tests. Legacy
specialized paths remain selected for their existing key shapes.

## Legacy hot-path investigation

Compared the frozen native stage with generic+fixed-statistics integration on
CPU 8 with GOMAXPROCS=1. Eight alternating/reversed pairs of 300 iterations showed
median differences of −1.4% for global, +13.2% for 64 groups, and +0.8% for 4,096 groups. Exact two-sided
rank-sum permutation p-values were .878, .505, and .798; the low-cardinality samples
ranged 335–468 µs before and 341–443 µs after. Allocation counts stayed 109/127/128.
Global fixed metadata increased 128 bytes; grouped allocation bytes were unchanged.

Because these samples do not establish a meaningful regression or a speedup,
we also profiled longer 3-second low-cardinality runs. Their timing reversed:
435,535 ns/op before versus364,877 ns/op after, both 31,841 B/op and 127 allocs/op.
Profiles show the same existing specialized aggregateSelectedSumCountNoGrow
kernel consuming 92.5%/91.2% cumulative CPU. Its 257 disassembled instructions
match after normalizing address operands. Hash/state inner helpers remain inlined.
No generic key encoding or statistics state was added to that legacy row loop.

This shared-host evidence does not justify tuning unchanged kernels or claiming
an improvement. There is no reproducible hot-loop regression in these checks;
there is also no universal latency gate. Preserve the raw samples and profiles
and repeat architecture-specific measurements on a stable benchmark host.
