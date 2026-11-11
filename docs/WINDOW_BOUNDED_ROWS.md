# Finite ROWS windows

`RollingCountRows`, `RollingCount`, `RollingMin`, and `RollingMax` support
inclusive partition-local frames with finite PRECEDING, CURRENT ROW, and
FOLLOWING endpoints in resident, ordered-state, and external-sort execution.
The start must not be after the end; zero PRECEDING/FOLLOWING equals CURRENT
ROW. This includes frames entirely before or entirely after the current row.

Frames clip to the partition. An empty frame returns zero for either count and
NULL for extrema. Count uses parent validity, including valid false, empty
lists, and valid structs with NULL children. Extrema preserve native numeric
types, compare 64-bit integers exactly, sort NaN above non-NaN values, and keep
the first surviving equal value, including the sign of zero.

## Execution and ownership

Previously supported trailing frames keep their existing path. Other finite
frames use an owned delayed-input stage and private result columns, then a
zero-offset copy into the public output. Private names avoid both input and
output aliases. Every expression reads the original input, even when another
expression replaces its column, partition key, or order key.

A finite frame of width W uses a ring of valid row positions for count or a
monotonic deque for extrema. Missing partition rows advance state in one step;
short partitions do not scan the unused frame width. A past right endpoint additionally needs a delay
ring of scalar copies. A future right endpoint uses the existing lookahead
queue, which clones each retained input batch before requesting the next one.
At a partition boundary, state resets; missing future rows expire old values
without borrowing rows from the next partition. No retained scalar references
a child vector. Input order is restored by the external-sort pipeline.

Processing is linear in input rows per expression, with frame initialization
at partition boundaries and amortized constant-time deque updates. Retained
numeric state is proportional to frame width plus any past-endpoint delay.
Lookahead retains whole input batches spanning the largest following endpoint,
not merely the selected numeric column. Large string or nested passthrough
columns therefore also consume the budget. It may read into later partitions
to satisfy a batch-level lookahead target, but cannot use those rows in a frame.

Ring capacity and byte arithmetic are checked before allocating the rings.
Descriptors, input clones, partition key copies, private results, and public
results are admitted through the existing memory manager. Impossible platform
capacities or byte estimates fail even on empty input. A valid int64 offset is
not a promise that its state fits a particular memory budget. There is no
silent fallback to an unlimited resident partition. External mode also needs
finite memory and spill quotas. Close releases retained clones and all leases;
lookahead errors close that stage immediately.

## Scope and checks

This extension does not change Sum/Avg arithmetic or add their frame support.
It does not add UNBOUNDED FOLLOWING or UNBOUNDED PRECEDING-to-FOLLOWING support.
Existing unbounded-preceding-to-current aggregates are unchanged.

`go test ./lazy -run '^TestBoundedRows'` exercises a standalone clipped-frame
oracle through actual Prepare/Collect and ExecuteBatches calls, all three
strategies, all ten numeric types, nullable Bool/List/Struct counts, aliases,
chunk/partition edges, repeated execution, borrowed-buffer poisoning,
cancellation, errors, and early close. Extremely large frames separately check
capacity rejection and cleanup; resident results retain full int64 clipping.

Run the repository's `scripts/validate.sh` gates for broader validation. If the
original Parquet fixture is absent, fixture-dependent tests and the demo remain
unverified; generated data is not a substitute for that validation.
