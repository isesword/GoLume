# Manual two-workload regression audit

`python3 scripts/item27_manual_regression.py --kind AA --a A.json --b B.json --out NEW_DIRECTORY`

This is a manual frozen-executable runner, not CI, a compiler, a benchmark-suite replacement or a baseline-promotion tool. Use `AB` for future genuinely different baseline/candidate binaries. A/A requires the identical binary SHA for both labels and only demonstrates repeatability. Existing production, tests and broad benchmark scripts are unchanged.

## Preparation contract

A coordinator must first compile the existing engine test binary with a pinned official Go toolchain, `GOEXPERIMENT=simd`, and freeze explicit source manifests. No directory discovery is performed. The runner executes and records the independent resource control before timing; external correctness evidence may additionally accompany the build. A self-reported gates_ok boolean is not accepted as proof.

Each JSON input has these required fields:

- `binary`: absolute executable path; `binary_sha256`.
- `source_root`: absolute frozen checkout path.
- `source_manifest`: absolute JSON file mapping relative allowed file paths to SHA256; `source_manifest_sha256`.
- `harness_sha256`: mapping of all relevant test-harness paths to SHA256, anchored in the manifest. Include the unchanged `engine/global_binding_benchmark_test.go` and relevant resource-test harness. The full manifest also captures every compilation input.
- `toolchain`: pinned version/build identity, identically represented in both inputs; retain actual `go version` and `go version -m` evidence outside this JSON.
- `build_env`: object with all build settings, including `GOEXPERIMENT: "simd"`, `mode: "auto"`, GOOS/GOARCH/GOAMD64/CGO, compiler flags and GODEBUG. Both objects must match exactly.
- `host`: frozen CPU/OS/architecture/affinity fingerprint object; both must match. This fingerprint is checked against actual host observation; neither observation nor equality proves hardware isolation.
- `go_path`, `go_sha256`, `build_receipt`, `build_receipt_sha256`: required measured-provenance linkage described below.

For this first demonstration use the same binary twice. Real A/B permits production/source-manifest differences, but rejects harness/compiler/build-environment/host differences as INCONCLUSIVE. No source manifest may include protected or escaping paths. Paths are resolved and checked within the supplied root. The runner checks all listed bytes and binary hashes before each process and after all samples. Empty or incomplete manifests must not be supplied; reviewer approval of the full compilation manifest is a prerequisite.

Existing independent budget test: `TestItem7AvgBatchBudget/nonavg`, with 257 rows/65-row batches, Float64 SUM+COUNT+pred COUNT. Its exact/minus-one coverage is separate; it is **not** an exact memory-boundary claim for the four-type timing cells. No new Go adapter is added.

## Fixed measurement

Exactly two existing `BenchmarkGlobalBindingWholeQuery` leaves: four lanes, nullable, global, rows/batch63/63 and262147/262147, modeAuto. Types are U64/F64/I32/F32, SUM+nonnull COUNT for each plus COUNT rows. Six AB/BA alternating pairs,150ms, GOMAXPROCS1: 24 subprocess samples. Workload, parameters and counts are not configurable. Engine prepared `ExecuteTyped(Materialize:true)` lifecycle is timed; fixture/Prepare/full oracle/charged peak audit are outside. The large source fixture batch is not the delivered view size; the existing pipeline splits at65536.

Output directory creation is exclusive. Raw logs, exit codes, exact commands, frozen input metadata and script SHA, parsed samples and final JSON are retained. No sample deletion, trimming, rerun or automatic rescue. Errors/timeouts/parse mismatch close INCONCLUSIVE. Exit codes: PASS0, REGRESSION1, INCONCLUSIVE2. For known host interference invoke `--noisy`; this flag cannot certify absence of noise when omitted. Do not run beside deliberate heavy work. Document unpinned/nonexclusive-host limitations even when PASS.

Per-cell B/A paired ratios, median/range and bootstrap median95%CI: seed20261119,100000 resamples of six pairs, percentile indices2500/97500. True comparable A/B lowerCI>1.03 is REGRESSION. Otherwise PASS requires upperCI<=1.03 for both cells, complete comparable evidence, and no observed B/op/alloc/op/charged-peak increase. CI crossing threshold, unexplained resource growth or known noise is INCONCLUSIVE. A/A significant bias in either direction is INCONCLUSIVE, never source regression or optimization success. Do not tune thresholds to obtain an A/A pass.

Go B/op, alloc/op and query-charged peak are separately retained in raw samples. Peak is the existing untimed preflight metric, not RSS or timed process maximum. Immutable fixture memory is excluded. No improvement or broad project acceptance follows from these two cells.

Run Python contracts only with `python3 scripts/item27_manual_regression_test.py`. These use synthetic measurements and temporary identity files, not Go benchmarks.

## Measured provenance and preflight

`gates_ok` in an input JSON is ignored. Before timing, the runner actually executes each binary with `-test.run '^TestItem7AvgBatchBudget$/^nonavg$' -test.v -test.count=1`; exit0 plus genuine PASS lines for parent, nonavg, exact and minusone are required, with no SKIP/FAIL. Raw commands/logs/status are retained. The two benchmark leaves also execute their existing full oracle preflight. No new Go tests are introduced.

Additional mandatory input fields: `go_path` (absolute pinned executable), `go_sha256`, `build_receipt` (absolute JSON), `build_receipt_sha256`. Receipt must link the exact `binary_sha256`, `source_manifest_sha256`, `go_sha256`, `toolchain`, `build_env`, `harness_sha256` and actual compile `command` array starting with go_path and containing `test`, `-c`. Coordinator creates this receipt during the real build; it is not a cryptographic proof that a command ran. Runner verifies all linked identities.

Runner invokes that verified Go executable with local/offline settings to read `go version -m BINARY`, saves raw output and checks actual compiler version plus GOEXPERIMENT, GOAMD64, GOOS, GOARCH and CGO_ENABLED against the fingerprint. `host` must exactly equal the current `observe_host()` output: platform.uname fields, CPU model from proc when available, and process affinity list. It rechecks current host and immutable identities before samples and afterward. Observation does not establish host exclusivity or CPU frequency stability.

A/A additionally requires identical source-manifest SHA, not only identical binary. A/A PASS requires the entire CI inside[0.97,1.03]; crossing either edge is INCONCLUSIVE. True A/B regression threshold remains unchanged. Known-noise declaration remains conservative and cannot prove a quiet host when absent.

Go output with no processor suffix or `-1` is accepted; any other suffix fails closed. Malformed/missing input JSON creates an INCONCLUSIVE result in the exclusive output directory. Subprocess output streams directly to raw log, so timeout keeps partial output and TIMEOUT status. Unsafe/aliased manifest names are rejected before their target is read. Contract tests cover these paths using synthetic files and outputs only.

## Actual build-flag comparison revision

The runner now parses every actual `go version -m` build entry, splitting only at the first `=` so keys such as `-race` and `-gcflags` and values containing `=` or spaces are preserved. It saves each full actual settings map. A/B maps must match exactly except `vcs.revision`, `vcs.time`, and `vcs.modified`; new, missing or different other keys close INCONCLUSIVE. The five declared target/compiler setting checks remain mandatory. VCS backend itself is not ignored.

Evidence boundary: the existing aa-r2 demonstration's actual24 samples were produced by the original runner SHA `a88bc0a4b33c731ad294839721dc7039ee4afd9b25909b3d6c9253bc00d2d001`. Those same-binary results remain historical evidence; this final revision has new Python contract validation only, with no Go build, new benchmark or A/A repeat. Old three-file bytes/hashes and the previous frozen manifest are retained separately before this edit.
