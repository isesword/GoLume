# Explicit bounded grouped Decimal average

N33 extends only the explicit `ExecuteDecimalBoundedGrouped` entry point. Native fixed typed input, one nullable signed Int64 key, finite memory/disk requirements, resident-table flushes, N29 partial state encoding and N31 multi-pass scheduling are unchanged. It does not enable PreparedQuery, Auto, Lazy, general Source adapters, multiple keys, windows or another scheduler.

## Selection and configuration

`DecimalBoundedGroupedOptions.Mode` selects:

- `DecimalBoundedSumCounts` (zero): original SUM, COUNT(value), COUNT(rows).
- `DecimalBoundedAverageCounts`: AVG plus both counts. SUM is not finalized or emitted.
- `DecimalBoundedSumAverageCounts`: SUM and AVG plus both counts from the same exact state.

AVG modes require explicit `AveragePrecision`, `AverageScale` and `AverageRounding` (`aggregate.DecimalAverageRoundingMode`). Precision is 1..38, scale 0..precision. All six existing rounding modes are supported: toward zero, away from zero, floor, ceil, half-even and half-away from zero. Zero or unknown rounding, unknown aggregate mode and invalid metadata fail before input Next or memory reservation, including empty input. SUM-only mode rejects any nonzero AVG configuration rather than silently ignoring it. Input precision/scale retain their existing native validation.

Configuration is passed by value and copied into private result state. There is no retained pointer to caller configuration. Every nonnil input is still taken over immediately and closed exactly once, including invalid options. Source Close errors and Close-triggered cancellation prevent success.

## Exact arithmetic and finalization

Each input group retains precisely the existing signed-256 coefficient sum and valid-value count. Flush and merge never narrow or round that state. Input NULL values contribute to COUNT(rows), but not SUM state count or COUNT(value). COUNT(value) equals the state's divisor; NULL keys form an ordinary group.

A shared final-output helper is used both for full-result preflight and for subsequent output-page construction. AVG calls the existing `DecimalSumState.FinalizeAverage` with immutable input scale and explicit output metadata. It divides/rescales the exact merged state and rounds once per final value, with no intermediate rounding. Output construction may recompute the same final value from the unchanged exact state; it does not round an already rounded result. Final result precision is checked after rounding.

AVG-only never calls SUM Finalize. Thus two input precision-38 maxima can have a sum exceeding precision 38 while their representable average succeeds. Combined mode retains the SUM contract: that same input fails with `aggregate.ErrDecimalSumOverflow`, even if AVG itself would fit. SUM-only remains the default and retains its original result semantics. AVG overflow uses `aggregate.ErrDecimalAverageOverflow`, compatible with errors.Is. Invalid AVG configuration uses `aggregate.ErrDecimalAverageOptions`.

Zero valid-value count yields NULL AVG. A nonempty zero average is valid. All final pages are preflighted before Execute publishes a result, so a late-page rounded overflow prevents any earlier page from being exposed as a successful execution.

## Output and lifecycle

`DecimalBoundedOutputRow` adds `Average` and `AverageValid` alongside existing Sum/SumValid and counts. `DecimalBoundedOutputBatch.Mode` identifies selected outputs. Its existing Precision/Scale continue to describe SUM as 38/inputScale; new AveragePrecision/AverageScale describe AVG in AVG modes and are zero in SUM-only mode. Unselected numeric outputs are zero and invalid. In AVG-only mode invalid Sum means SUM was not requested, not that AVG is NULL.

The result's fixed output buffer and added option fields are included in the existing unsafe.Sizeof-based pre-allocation reservation. AVG arithmetic uses the existing fixed-size finalizer scratch, not a new retained lane, heap-backed accumulator or cache. File owners, page codecs, restore buffers and cleanup retain their existing accounting. Larger fixed output records may change measured memory thresholds; resource evidence must be measured on this version, not copied from N32.

Next returns a borrowed fixed batch, invalidated and cleared by the next Next call (including EOF/error) or Close. Before successful Next publication, the restored typed page is explicitly released and then context cancellation is checked; the N32 Release-triggered cancellation fix is preserved. A deferred idempotent close remains as the error-path safeguard. All cleanup-only owner, deletion-retry and input-close contracts remain unchanged.

## Limits and scope

The executor remains capped at 256 rows per input batch, 65,536 total input rows, 4,096 delivered batches and 64 initial runs. Each run has at most TableCapacity<=256 partial groups, so at most 16,384 distinct groups are representable and lower capacities reduce that bound. Capacity zero chooses conservative merge headroom, not a guarantee that all future reservations fit. Reserve failure is terminal with cleanup, not an automatic budget-failure flush policy. The same store quota covers initial, intermediate and final files.

The original SUM/count documentation remains the baseline for mode zero; this document specifies the optional AVG modes. No change is made to N29/N30/N31 files, old query dispatch or aggregate finalizer mathematics.
