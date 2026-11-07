# Expression buffer lifetime review

## Result

No blocking correctness issue was found in the fixed-width storage-reuse change.
The review checked the planner, evaluator, exports, guarded regions, reservation
accounting, retry behavior, cancellation, and close. This is a bounded review and
test result, not a proof that every possible expression is correct.

## Ownership and lifetime checks

- `expr/liveness.go`: dependencies cover every current register-consuming opcode.
  Last use is computed across the complete compiled instruction stream, so a
  common subexpression remains live for its final consumer. Root and named-output
  registers are pinned through evaluation, including duplicate and early roots.
  A dead input is released only after its consuming instruction, avoiding
  unsupported in-place kernels.
- `expr/scalar.go` and `expr/workspace.go`: logical register views stay separate
  from physical owners. The evaluator and fixed-capacity admission use the same
  ownership mapping. Only owning slots are allocated and counted, while register
  metadata remains proportional to instruction count. Exact result types preserve
  native integer and floating-point widths. Validity is copied or computed into
  owned storage instead of extending a dead input's bitmap lifetime implicitly.
- `expr/regions.go`: guarded regions have independent plans and pinned roots.
  Parent condition/needle inputs remain live for the conditional instruction.
  Partial-row validity writes use the mapped owner. Borrowed columns and region
  control state retain dedicated ownership.
- `expr/boolean.go` and `expr/value_view.go`: deferred BOOL-to-uint32 projection
  resolves the physical owner even when it differs from the root register.
  Exported roots cannot be recycled during the same evaluation. Borrowed
  variable-width exports remain dedicated, so duplicate-output view sharing does
  not acquire a new lifetime hazard.
- `expr/unified.go` and the workspace lifecycle: retained allocations have one
  owner and one release path. Fixed growth is admitted before replacement.
  Budget failures remain retryable, while cancellation closes the workspace.
  Dynamic payloads retain their existing separate admission and release paths.
- Instruction-local dictionary/literal caches, conditional state, JSON, borrowed
  inputs, and variable-width outputs are excluded from the pool. This conservative
  boundary avoids recycling state whose lifetime is not just its result vector.

The improvement applies to fixed-width payload capacity, not all workspace
memory: logical/owner metadata still grows with the compiled DAG, and dedicated
dynamic/region state retains its previous behavior.

## Reproducible differential validation

`expr/liveness_differential_test.go` preserves the stress harness in the repository.
The reference program is a deep clone of the compiled program and every nested
region with storage ownership assignments cleared. It therefore executes the same
DAG with one physical owner per logical register, without requiring a second
checkout or mutating the planned program.

`TestLivenessSeededDAGDifferential` uses seed `91823712`, 128 generated DAGs, both
scalar and SIMD modes, and repeated row counts `65, 1, 0, 129, 3, 255, 64, 130`.
It compares valid values and null masks for nullable arithmetic, casts, Boolean
operations, shared/duplicate outputs, and nested If/Case/Coalesce/In regions.
Both planned and reference workspaces must release their budgets to zero. This
is 2,048 planned/reference comparisons, or 4,096 workspace evaluations.

`TestLivenessPooledBooleanExportAdmission` ensures the exported Boolean root uses
another register's physical owner. It rejects each of the three allocation
stages (fixed storage, result headers, expanded Boolean values), checks budget
retry results, checks cancellation cleanup, and verifies idempotent close.

Before retaining the repository harness, the same seeded generator was run
against the pristine baseline checkout with 750 DAGs and eight repeated batch
sizes in scalar mode. All 6,000 result comparisons matched and all workspaces
released their budgets. This separate-checkout check complements the retained
same-executor comparison; the latter intentionally isolates storage planning
rather than independently validating the expression kernels.

## Commands and outcome

Environment: Linux/amd64, Go 1.27.3, unchanged module dependencies.

Passed during this review:

```sh
go test ./expr -run TestLivenessSeededDAGDifferential -count=1
go test ./expr -count=1
```

The second command includes the retained differential test, pooled Boolean
export failure sweep, and the other liveness/native-width/partial-region/lifecycle
regressions. For this materialized environment the commands used
`/tmp/golume-toolchain/go/bin/go`, `GOPATH=/tmp/golume-gopath`, and
`GOCACHE=/tmp/golume-gocache`.

A review-specific targeted race rerun was interrupted to avoid contention with
the paired performance measurements. The commands above report only completed
checks; repository-wide gates and performance results are recorded separately.

Final follow-up: after paired benchmarks completed, the complete expression
suite including these retained tests passed uncached under SIMD and SIMD race
(`GOEXPERIMENT=simd go test -race ./expr -count=1`). See the checks log.
