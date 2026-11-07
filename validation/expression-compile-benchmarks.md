# Compile benchmark comparison

Baseline: a8cb86b33003390aa64ce0dc3c652b725edd3812. Final implementation includes
bottom-up structural interning and dense physical owner IDs. Same benchmark
source, Go 1.27.3 linux/amd64, AMD EPYC 9V74, default GOMAXPROCS=9.

Expressions are constructed outside timing. Each timed iteration calls the
public CompileExpressions API, including Bind, identity/CSE, and storage planning.
The baseline copy was verified against remote blob IDs for all tracked Go sources.
The final before and after runs were sequential, with no other agent benchmarks
running. Five samples per case, 200ms target per sample; deep baseline cases may
perform only one iteration because each compile exceeds the target duration.

Command in each checkout:

```
go test ./expr -run '^$' -bench '^BenchmarkExpressionCompile$' -benchmem -count=5 -benchtime=200ms
```

Medians below; complete samples are in expression-compile-before.txt and
expression-compile-after.txt. These are local microbenchmarks on a shared host,
not end-to-end query speed guarantees or proof of a universal speedup. Allocation
reductions and the compact-key invariant support the architectural change.

| Case | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| chain/depth=8 | 16,177 | 13,053 | 25,568 | 17,720 | 329 | 182 |
| outputs/depth=8 | 32,089 | 28,062 | 47,728 | 31,624 | 671 | 508 |
| regions/depth=8 | 45,664 | 29,382 | 68,064 | 42,352 | 900 | 431 |
| chain/depth=32 | 216,238 | 43,356 | 368,035 | 51,480 | 3,714 | 627 |
| outputs/depth=32 | 293,982 | 105,935 | 517,700 | 118,520 | 4,947 | 1,770 |
| regions/depth=32 | 486,304 | 95,071 | 822,919 | 106,384 | 8,123 | 1,311 |
| chain/depth=128 | 6,117,064 | 219,592 | 13,466,516 | 208,041 | 56,685 | 2,399 |
| outputs/depth=128 | 7,382,084 | 456,634 | 14,749,420 | 391,531 | 61,767 | 6,869 |
| regions/depth=128 | 13,147,659 | 442,204 | 27,837,277 | 408,683 | 116,088 | 4,854 |
| chain/depth=512 | 289,853,938 | 1,029,535 | 740,733,160 | 832,798 | 915,031 | 9,712 |
| outputs/depth=512 | 331,737,167 | 2,490,367 | 758,274,552 | 1,562,979 | 935,482 | 28,013 |
| regions/depth=512 | 543,436,400 | 1,861,771 | 1,494,371,096 | 1,617,317 | 1,840,861 | 19,476 |
| diamond/depth=4 | 16,722 | 17,326 | 20,040 | 17,712 | 365 | 305 |
| diamond/depth=8 | 376,163 | 243,901 | 409,202 | 214,241 | 6,510 | 4,644 |
| diamond/depth=12 | 6,903,011 | 3,843,213 | 8,870,981 | 3,256,894 | 105,518 | 73,832 |

The source diamond is expanded by Bind before interning. It therefore still
scales with bound-tree size, not unique source DAG nodes. The condition-region
case is a nested If with shared chain syntax in separately guarded branches;
register CSE stays local to each region even though structural IDs are shared.
