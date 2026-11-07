# Ordered-transform validation

Go 1.27.3, linux/amd64, AMD EPYC 9V74 shared host, 2026-11-07.

Passed full affected package scalar tests:
`go test ./physical ./lazy ./logical ./expr`

Passed targeted scalar, SIMD and SIMD race tests covering Offset, ShiftDiff,
Fill and FutureWindow cases. New lookahead methods initially lacked GoDoc;
comments were added and `go run ./tools/checkgodoc .` then passed. The raw
combined test log preserves that initial documentation-only failure honestly.

The tests exercise all ten numeric widths with past/current/future offsets,
small input batches, nulls, partition reset and original-order restoration;
lazy resident/ordered/external paths; private-name collisions and alias
replacement; EOF tails; zero/one/65/513 rows; UInt64 diff endpoint checks;
reservation admission, cancellation and injected input failures; and value-fill
NULL/NaN separation, branch type promotion and unused replacement evaluation.

The initial benchmark command was:

```
GOMAXPROCS=1 go test ./physical -run '^$' -bench '^BenchmarkOffsetWindowStream$' -benchmem -benchtime=1s -count=3
```

A 65,536-row Int64 Shift with offset magnitude 3, BatchRows=1024, one worker:
past median 16.23 ms, 560,376 B/op, 388 allocs; future median 20.37 ms,
1,701,032 B/op, 2,017 allocs. Construction/preparation is outside the measured
loop; opening, consuming and closing each stream are inside. Bytes/op are total
allocations, not peak retained memory. Future execution copies bounded owned
batches; it does not retain the entire input. These are baseline observations,
not a SIMD speedup or cross-architecture performance gate.

## Post-review budget hardening

Independent adversarial review added wide nested-struct and 2,048-hidden-output
admission regressions. The final implementation pre-admits recursive clone
metadata, append capacity/temporary overlap, hidden-vector descriptors and
per-execution preparation metadata. Closed streams drop owned descriptors and
rolling state when their leases are released. Added recursive-estimator overflow,
depth/cycle, zero-allocation traversal, preparation-before-source-open, and closed
reference/reopen tests. Past-only preparation now performs no allocations.

`review-budget-tests.txt` records passing full physical/expr tests, focused
SIMD and SIMD race tests, and GoDoc. Full live-tree validation during concurrent
unrelated aggregate editing was not usable because the aggregate source was
mid-edit; integration validation uses the isolated ordered-stage snapshot.

The same benchmark after hardening is in `review-budget-benchmark.txt`: past
median 15.60 ms, 559,864 B/op, 384 allocs; future median 19.75 ms, approximately
1,701,054 B/op, 2,018 allocs. Shared-host variance prevents treating these small
differences as a performance claim. Conservative admission can reject a budget
that exceeds actual payload but does not cover metadata and transient capacity.

## Final isolated integration gate

The final source snapshot layers ordered transforms over all reviewed native
aggregation and expression stages, excluding concurrent generic-key/statistics
work. Full scalar and SIMD `go test ./...`, SIMD race tests for physical/lazy/
logical/expr/engine, vet and GoDoc passed. See full-stage-tests.txt. Independent
review cleared all three future-buffer accounting findings (recursive metadata,
hidden outputs, and preparation descriptors) and reference cleanup regressions.
