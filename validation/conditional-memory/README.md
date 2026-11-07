# Conditional workspace validation

Base: `95d216c11b98d118bef323fcaec5f366f253de07`. Recursive remote tree
verification found every source, test, module and script blob identical to the
starting snapshot. Historical validation logs and a generated Parquet fixture
differed or were absent locally and are not republished by this stage.

## Benchmark method

Identical benchmark source runs against the unchanged base and new code.
Linux/amd64, Go 1.27.0, AMD EPYC 9V74 shared host; GOMAXPROCS=1, five 200ms
samples per case. 4,096 rows, every numbered arm demanded; CASE ELSE is empty.
Cold includes fresh workspace allocation and Close per iteration. Warm setup is
excluded. Peak/retained figures count managed capacities, not RSS, Go heap, or
allocator overhead. Compiled program and source batch storage are excluded.
The measured cold fixture has no growth after its initial branch layout;
replacement overlap is separately tested rather than implied by these peaks.

## Medians

| Shape | Before ns/op | After ns/op | Before peak/retained B | After peak/retained B | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| case/arms=2/warm=false | 139666 | 119318 | 227896 | 145056 | 246192 | 164168 | 133 | 80 |
| case/arms=2/warm=true | 112074 | 104467 | 227896 | 145056 | 13368 | 13368 | 7 | 7 |
| case/arms=8/warm=false | 384812 | 278955 | 702808 | 145056 | 731328 | 164168 | 421 | 80 |
| case/arms=8/warm=true | 279618 | 257439 | 702808 | 145056 | 13368 | 13368 | 7 | 7 |
| case/arms=32/warm=false | 1374035 | 959868 | 2602456 | 145056 | 2671920 | 164168 | 1573 | 80 |
| case/arms=32/warm=true | 949007 | 937252 | 2602456 | 145056 | 13368 | 13368 | 7 | 7 |
| case/arms=64/warm=false | 3049751 | 2043247 | 5135320 | 145056 | 5259376 | 164168 | 3109 | 80 |
| case/arms=64/warm=true | 1949250 | 1862756 | 5135320 | 145056 | 13368 | 13368 | 7 | 7 |
| coalesce/arms=2/warm=false | 112153 | 91097 | 219760 | 144544 | 236768 | 160624 | 104 | 71 |
| coalesce/arms=2/warm=true | 81868 | 71648 | 219760 | 144544 | 13368 | 13368 | 7 | 7 |
| coalesce/arms=8/warm=false | 267303 | 210480 | 671104 | 144544 | 694544 | 160624 | 308 | 71 |
| coalesce/arms=8/warm=true | 189291 | 189894 | 671104 | 144544 | 13368 | 13368 | 7 | 7 |
| coalesce/arms=32/warm=false | 973506 | 652067 | 2476480 | 144544 | 2525648 | 160624 | 1124 | 71 |
| coalesce/arms=32/warm=true | 631191 | 610990 | 2476480 | 144544 | 13368 | 13368 | 7 | 7 |
| coalesce/arms=64/warm=false | 2148312 | 1272721 | 4883648 | 144544 | 4967120 | 160624 | 2212 | 71 |
| coalesce/arms=64/warm=true | 1285593 | 1223023 | 4883648 | 144544 | 13368 | 13368 | 7 | 7 |

Managed peak/retention is flat from 2 through 64 arms in this fixture. At 64
arms CASE saves 97.18% and COALESCE saves 97.04%. Warm allocations remain seven
per evaluation in both versions (existing export/result wrappers), with no new
managed admissions. Timing is descriptive shared-host evidence, not a universal
speedup claim. Flat, cache-free fixed-width arms are the optimized scope; nested
parent, variable-width and cache-bearing fallback behavior remains unchanged.

Raw samples: `before.txt` and `after.txt`.

## Correctness and checks

`conditional_memory_test.go` checks branch-count independence, explicit expected
row values, exact managed accounting, no warm admissions and release on Close.
The independent `conditional_review_test.go` covers mixed-width numeric owner
patterns and promotion, selected NULL versus fallthrough, duplicate shared roots,
independent workspaces, grow/shrink/empty batches and non-word-aligned tails,
Boolean true/false/NULL results, borrowed-input immutability, guarded overflow and
nested fallback. It rejects each successive reservation and checks retained
accounting and retry, and cancels during each admission as well as before a call.

`final-validation.txt` records the final scalar/SIMD full suites, affected
SIMD race suites (expr/lazy/physical/engine), vet, GoDoc, generated SIMD and
registry checks, forced 0/128/256/512-bit execution, and Linux/arm64 SIMD
cross-build. ARM64 is compilation only, not ARM execution validation.

Independent production review found no correctness issue. Existing guarded
region CSE, error, string and nested-region tests also remain in the full suite.
The new tests supplement those with independent expected-value oracles rather
than treating another use of the same conditional evaluator as a reference.

Local checks do not demonstrate remote CI execution or imply merge readiness.
