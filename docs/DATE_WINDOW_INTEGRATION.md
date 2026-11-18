# Date/time and distribution/moment windows integration

This change is a selective integration on commit
`3313770e0af1c24e89e2670ac3b0d40302365cdf`, not a replacement by the older
cumulative development snapshot. Existing published features, unrelated files,
workflow definitions and repository history are retained.

## Scope

- Captured timezone and civil-time resolution; date-at-midnight, zoned calendar
  and ISO-week fields, UTC offset, civil formatting/composition, time-of-day,
  date-from-ISO-week and date-from-year/month/day expressions.
- PercentRank, CumeDist, NTILE, RollingVarPop/Samp and RollingStdDevPop/Samp,
  including resident, ordered and external execution paths.
- Necessary finite ROWS SUM/AVG shared pipeline, native borrowed-range
  cancellation checks and Binary dictionary Arrow export correctness.
- Allocation-free internal diagnostic sampling with fixed typed counters and a
  scalar budget snapshot. Public explicitly requested diagnostic maps remain
  a cold formatting path; this is not a claim that all process allocations or
  all application diagnostics fall within the query budget.

Existing window enum values 0–12 are preserved; the added values are 13–19 in
both logical and physical layers. Existing scalar opcodes are preserved, with
new date opcodes appended. Unrelated Decimal extensions, generic nested-container
extensions, FILTER, GROUPS and First/Last extensions are not included.

## Validation interpretation

The feature documents retain historical stage descriptions. Their local pass
counts and unpublished-status language are not this integration's acceptance
record. This integration is validated against the selected current-branch base.
The local validation summary below applies to this selective integration.

The original whole-repository gate remains unavailable because an existing
protected test is outside the permitted validation inputs. It is neither read,
reconstructed, deleted nor modified by this integration. Explicitly selected
Lazy tests are not a whole-Lazy or whole-repository pass.

Historical full-race and combined-Lazy-race 600-second timeouts, interrupted
SIMD/runtime work, Join 512 KiB pressure failure and List<TIME> 16 MiB resident /
64 MiB ordered-readback failures are not cleared by this change. ARM64 native,
hosted CI and reliable performance conclusions remain unverified. Deep spill
metadata paging remains deferred.

The transplanted DateInZone pipeline test retains date, grouping and IPC
assertions but uses scalar projection instead of an unpublished nested-struct
intermediate; it does not validate that nested composition. Civil-time and finite
SUM test helpers are self-contained copies/adaptations of accessible test code.
Their budgets, data sizes and supported-feature assertions are preserved.

## Local verification on linux/amd64, Go 1.27.3

- Scalar and SIMD: memory, expr, optimizer, logical, physical and all source
  packages passed, 17,569 tests/subtests per mode across 10 packages.
- Race: moments 249; window/memory 1,272; full expr+optimizer 5,512 passed.
- Vet passed for those 10 packages.
- Explicit safe Lazy selection (16 test files): scalar/SIMD each 1,136 passed.
- Lazy race groups: StdDev 123 (282.427 s), variance 112 (332.778 s),
  NTILE 105 (269.009 s), remaining selected tests 796 (408.931 s). Their
  disjoint union exactly matches the scalar/SIMD test set. This is not a new
  combined race pass and does not erase the earlier combined timeout.
- Original fixed 4 MiB physical and 8 MiB Lazy resource tests passed without
  increasing budgets or reducing data. Actual spill and cleanup assertions,
  1,024-operator diagnostic EOF/Close zero-allocation tests, scalar snapshot
  concurrency, and canceled borrow/Arrow retain cleanup are included.
- All frozen implementation/test hashes were rechecked after testing.

Initial transplant compile failures (a missing date context adapter and test
helper dependencies) were retained as evidence and corrected before the final
applicable checks. No full-repository, hosted CI, ARM64 or performance pass is
claimed. Raw logs are intentionally not committed.
