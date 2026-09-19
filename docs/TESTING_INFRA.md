# Testing Infrastructure

This document describes GoLume's referee system: the automated checks that
decide whether a change is correct, fast, and safe to merge. It complements
[ARCHITECTURE.md](ARCHITECTURE.md), which describes the engine itself.

## 1. Three layers

The referee system has three layers, from fast feedback to deep assurance:

```text
  +-----------------------------------------------------------+
  | Layer 3: Correctness oracle & property tests              |
  |   internal/oracle    row-based reference evaluator        |
  |   internal/proptest  seeded generators + invariants       |
  |   internal/fixturegen + tools/genfixture  golden datasets |
  +-----------------------------------------------------------+
  | Layer 2: Benchmark regression tracking                    |
  |   tools/benchtrack + internal/benchtrack                  |
  |   history: validation/bench-history.jsonl                 |
  +-----------------------------------------------------------+
  | Layer 1: CI matrix                                        |
  |   .github/workflows/ci.yml  (ubuntu-latest/macos-latest)  |
  +-----------------------------------------------------------+
```

- **CI matrix** mirrors `scripts/validate.sh` step by step on both
  ubuntu-latest and macos-latest: GoDoc check, `gensimd -check`,
  `check_simd_structure.sh`, scalar and `GOEXPERIMENT=simd` test passes,
  the `GODEBUG=simd=0` variant, race detector, `go vet`, and — on amd64 only —
  a conditional `simd=+256` step. It triggers on `push` (main/master),
  `pull_request`, and `workflow_dispatch`.
- **Benchmark regression tracking** records benchmark results as append-only
  JSONL history and compares a candidate run against a baseline with explicit
  per-metric thresholds, so "did this get slower?" is a mechanical question.
- **Correctness oracle and property tests** verify semantics, not speed:
  engine output is checked against a small row-based reference evaluator and
  against invariants that must hold for any input, on seeded pseudo-random
  tables and queries.

## 2. Benchmark tracking: benchtrack

`benchtrack` never runs benchmarks itself; it ingests `go test -bench` output
you already produced:

```text
go test -bench=. -benchmem ./kernel | go run ./tools/benchtrack record
go run ./tools/benchtrack record -input bench.txt -format gotest \
    -history validation/bench-history.jsonl -commit $(git rev-parse HEAD)
go run ./tools/benchtrack compare -baseline 3f9a1c2 -threshold 5 \
    -bytes-threshold 5 -allocs-threshold 5 \
    -history validation/bench-history.jsonl
go run ./tools/benchtrack report -history validation/bench-history.jsonl
```

- `record` appends one JSON object per benchmark result to the history file.
  `-input` is a file or `-` (stdin, the default); `-format` is `auto`,
  `gotest`, or `csv`; `-commit` defaults to `git rev-parse HEAD`.
- `compare` matches benchmarks by name between the candidate (latest run) and
  a `-baseline` given as a commit (exact or prefix), a 1-based history line
  number, or empty for the newest run with a different commit. A metric is a
  regression only when it is strictly greater than its threshold (percent,
  default 5 for ns/op, B/op and allocs/op). Exit codes: 0 = no regression,
  1 = regression found, 2 = usage or I/O error.
- `report` groups history by benchmark and shows first/latest/min/max plus
  the relative change since the first record.

## 3. Fixture generation: genfixture

```text
go run ./tools/genfixture -seed 42 -spec spec.json \
    -out testdata/fixture-42.parquet \
    -compression zstd -row-group-rows 65536 -overwrite
```

- `-spec` (required) is the path to a JSON dataset spec with a `version`
  field; `-out` (required) is the Parquet output path. `-rows` overrides the
  row count declared by the spec; `-compression` selects `zstd` (default),
  `snappy`, `gzip`, or `none`; `-overwrite` replaces an existing file.
- The column library (`internal/fixturegen`) covers int64 (uniform/zipf
  distribution, cardinality, NULL rate), float64 (uniform/normal, NaN rate),
  string (length range, cardinality) and bool (true ratio, NULL rate).
- `-seed` fixes the pseudo-random stream: identical flags and spec produce
  identical data on every machine, so large fixtures never need to be
  committed — only their small JSON specs.

## 4. Correctness oracle and property tests

`internal/oracle` is a deliberately simple row-based reference evaluator for
the engine's semantics: in-memory tables (int64/float64/string/bool + NULL),
Filter with strict SQL three-valued logic, Project (arithmetic, concat,
aliases), GroupBy (SUM/COUNT/COUNT(*)/MIN/MAX/AVG), stable multi-key Sort
with explicit nulls placement, Limit and TopK. It also provides a seeded
random table generator and a seeded random query-DAG generator.

The oracle test suites exploit both:

- `diff_test.go` runs 14 fixed regression seeds x 3 kernel modes
  (scalar/auto/forced-SIMD) x 3 batch shapes, requiring cell-for-cell
  identical output between engine and oracle.
- `sweep_test.go` runs an 800-seed wide sweep; it is skipped under
  `go test -short`.

The oracle intentionally covers a bounded subset that matches existing engine
constraints: aggregate value columns are int64 only, group keys are int64 or
string only, and bool columns never participate in sorting or grouping. The
query generator actively avoids constructs outside this subset.

`internal/proptest` is the lower-level toolkit: a seeded generator
(`NewGen`), invariant checks (`CheckEqual`, `CheckRowPermutation`,
`CheckSorted`, `CheckTopKPrefix`), and a multi-backend `Harness` that runs the
same operation under scalar, auto and forced-SIMD backends and compares
results cell by cell.

## 5. Design principles

### Determinism

Every randomized component takes an explicit seed. A failing oracle or
property test prints its seed, and re-running with that seed reproduces the
exact table and query DAG. Fixtures are regenerable from `(seed, spec, rows)`
alone.

### Self-containment

The correctness oracle does not shell out to DuckDB, SQLite, or any external
engine. It is a row-based evaluator that trades speed for auditability and
runs only on small inputs. This keeps the dependency policy intact —
production code stays on stdlib plus the existing allowed modules, and the
test infrastructure adds no new module dependencies at all.

### Dual-path consistency

Every kernel exists in scalar and SIMD form. Oracle diff tests and the
proptest harness require identical results across scalar, auto and
forced-SIMD modes; the CI matrix additionally runs the `GODEBUG=simd=0`
variant (plus `+256` on amd64), mirroring `scripts/validate.sh`. A
discrepancy between paths is a bug in the kernel, never in the test.

## 6. Extension guide

When you add new engine functionality, add the matching referee coverage:

| You add...                    | Add these tests                                                        |
|-------------------------------|------------------------------------------------------------------------|
| a kernel (`kernel/`)          | proptest harness run across all three backends (random slices incl.    |
|                               | tails), oracle coverage if reachable from queries, a `Benchmark*` for  |
|                               | hot loops, and the generated-SIMD counterpart via                      |
|                               | `internal/cmd/gensimd`                                                 |
| an expression (`expr/`)       | oracle diff coverage (nulls, three-valued logic), seeded random-query  |
|                               | seeds in `diff_test.go` if the expression enters the oracle subset,    |
|                               | dual-mode run                                                          |
| an operator (`physical/`)     | oracle diff test if the operator is in the oracle subset (or extend    |
|                               | the oracle deliberately), invariant checks via proptest                |
|                               | (`CheckRowPermutation`/`CheckSorted`/`CheckTopKPrefix` where they      |
|                               | apply), budget/ownership test if it retains memory                     |
| a source or sink (`source/`)  | round-trip test through `genfixture` output, pushdown equivalence      |
|                               | test (filtered result == full scan + filter)                           |

Placement rules: unit and property tests live in the package they test;
shared helpers live in `internal/oracle`, `internal/proptest` and
`internal/fixturegen`; anything runnable as a command goes under `tools/`.
New benchmarks that should be regression-gated must follow the
`Benchmark<Name>` convention so `benchtrack record` can ingest them.

## 7. Relationship to validate.sh and validation/

`scripts/validate.sh` remains the single entry point for the local quality
gate; the CI workflow replicates its steps on the OS matrix rather than
calling the script, so both evolve together — when one gains a check, mirror
it in the other.

The `validation/` directory keeps its evidence-driven convention:

- Raw command output is archived, not summarized away. Benchmark history
  lives in `validation/bench-history.jsonl`, an append-only ledger; never
  rewrite or compact old entries, since regressions are judged against them.
- History is currently written by developers running `benchtrack record`
  locally; automating periodic baseline recording in CI is a deliberate
  future extension, not an existing mechanism.
- Performance claims in docs and commit messages must cite a
  `benchtrack compare` run or an archived validation log, exactly as today
  they cite `validation/phase*.txt` outputs.
- When a benchmark is deliberately changed (algorithmic trade-off), record
  the new baseline with `benchtrack record` in the same commit and note the
  commit label in the changelog.

## 8. Kernel registry and the approval gate: kernelgate

`validation/kernel-registry.json` (schema 1) is the single source of
truth for every kernel's gate lifecycle. Each entry records the kernel
ID, its family/type/operation (derived from the ID and re-checked on
load), whether the scalar implementation is `generated` (by
`internal/cmd/gensimd`) or `handwritten`, and a per-GOOS/GOARCH state:
`candidate`, `approved`, or `rejected`. Approved and rejected entries
must point at evidence files in `validation/`. Legal transitions are
`candidate -> approved|rejected`, `approved -> candidate`, and
`rejected -> candidate`; anything else is rejected by the registry
validator. The gates file `validation/simd-auto-gates.json` remains the
approval list consumed by dispatch documentation; the registry mirrors
it and adds the lifecycle states the gates file does not carry.

`go run ./tools/kernelgate` provides four subcommands:

- `status` prints the per-kernel state table.
- `check` verifies three-way consistency — registry versus
  `simd-auto-gates.json` versus the kernels that exist in code (via
  `cmd/kernel-info` and `gensimd -list`) — plus evidence-file
  existence. It is the CI-friendly invariant check.
- `init` bootstraps or rebuilds the registry from those same sources.
- `gate -kernel <id> [-kernel ...]` runs the full approval pipeline for
  a candidate in one command: it benchmarks the kernel's `mode=scalar`
  and `mode=simd` sub-benchmarks at the samples/benchtime criteria from
  the gates file, decides with the Mann-Whitney U test
  (`golang.org/x/perf/benchmath`) against `minimum_improvement_percent`
  and `maximum_p_value`, archives the raw output and benchstat CSV as
  `validation/simd-gate-<id>-<goarch>.{txt,csv}`, appends every
  measurement to `validation/bench-history.jsonl`, and on a pass adds
  the kernel to the gates file's `approved_kernel_ids` and marks it
  approved in the registry. A failing kernel is marked `rejected` with
  the reason and its evidence kept. Approved or rejected kernels refuse
  re-gating unless `-force` is given. Note that the isolated kernel
  gate does not run end-to-end workloads; the
  `maximum_end_to_end_regression_percent` criterion is still enforced
  by the separate end-to-end benchmark evidence documented in
  `docs/SIMD_*.md`.

`validation/bench-history.jsonl` is deliberately tracked in git rather
than ignored: it is the append-only evidence ledger that
`benchtrack compare` judges regressions against, and at one JSON object
per benchmark measurement its growth rate is modest. Do not add it to
`.gitignore`.
