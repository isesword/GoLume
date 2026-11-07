# Unified expression architecture validation

Base: `5dddf50587affba10fb9e39e3134e360a85f9d03`.

## Architecture and API

A single Bind derives complete result fields, nullability, dependencies and
conservative row-value error effects. Logical inference and physical consumers
use it instead of independently reconstructing schemas. Compile and
CompileExpressions emit one Program; one Workspace executes the same typed
instructions for projected values and filter Boolean roots. Instruction storage,
roots and conditional regions are private. Public schema metadata accessors
return deep copies. Former scalar/projection program and separate expression
workspace types are removed, not kept as compatibility executors.

Filter uses an owned selection mask rather than modifying shared truth values.
Boolean output remains TypeBool/Uint32 physically and exports bool/nil through
Series. Lazy and expr constructors expose Bool, typed Null, If, Case, Coalesce
and In, with Alias support. All branches bind statically; only demanded rows
execute conditional branches. Region-scoped CSE does not hoist fallible work
across guards. CASE is ordered, a missing ELSE yields typed NULL, COALESCE stops
at the first non-NULL value, and IN preserves SQL three-valued equality.

Retained expression error domains constrain filter merging and source pruning.
Unused projection outputs remain prunable under the existing demand contract.
Integer/float comparisons preserve exact ordering without first rounding integer
operands; this does not change the documented arithmetic/result-promotion rules.

## Correctness fixes included

- Native Float32-to-integer access and NaN/overflow integer cast boundaries.
- Mixed signed/unsigned/integer/float comparison and literal fast-path precision.
- Guarded Boolean reuse when shrinking logical row counts.
- Cancellation during output production and budget-failure cleanup/retry.
- Nested string/list/JSON retained-memory reporting and schema ownership.
- Referenced input-prefix views and non-owning opaque/Arrow output aliases.
- NaN and nonrepresentable integer metadata pruning, including NOT predicates.
- Sequential filter, retained projection and sort-key error reachability.

## Validation

- Full scripts/validate.sh: PASS on the final code, with Go 1.27.3 linux/amd64.
  Ordinary/SIMD tests, disabled fallback, supported forced widths, Linux build,
  broad race tests, vet, GoDoc, generated-kernel/structure checks and demo pass.
- Full consumer suites and focused race tests: PASS for physical, engine, lazy,
  optimizer, logical validation, Parquet and ArrowRecord consumers.
- Independent canonical API suite: scalar, SIMD and race PASS. Its 23 cases are
  retained as expr/unified_external_test.go rather than only external evidence.
- Exact mixed-number comparisons were checked against an independent big.Rat
  oracle across 8,186 rows and 90 comparison configurations.
- Fault injection covers 80 reservation failure points, retry and cleanup;
  lifetime checks cover 0/1/63/64/65/130-row reuse, nested payload growth,
  Arrow ownership, cancellation and immutable program/schema metadata.
- Source regressions compare complete scan plus residual filtering against
  pushed-down execution in scalar and SIMD modes. They retain NaN matches and
  exact numeric-boundary matches previously lost by unsafe metadata inference.

Raw final check output:
[unified-expression-full-validation.txt](unified-expression-full-validation.txt).
Migration and detailed semantics:
[STREAM_MIGRATION.md](../docs/STREAM_MIGRATION.md) and
[EXPRESSION_EVALUATION.md](../docs/EXPRESSION_EVALUATION.md).

## Explicit limits

Conditional result types currently cover numeric, Boolean, string and supported
physical temporal scalars. List, Struct, opaque Arrow and Binary conditional
results are rejected during Bind, not accepted and later silently mis-evaluated.
Typed NULL uses the same scalar family. CASE requires a WHEN arm; COALESCE and IN
require nonempty argument/candidate lists. Regex patterns retain their existing
compile-time constant restriction. General expressions and aliases still support
their separately documented nested-value capabilities.

Ordinary AND/OR are not a substitute for guarded conditional evaluation when an
operand may fail. Use IF/CASE for row-dependent error guards. Budget failure is
retryable where specified; cancellation and semantic failure close a workspace.
Results remain read-only borrowed views until the next evaluation or Close.

Validation uses the disclosed synthetic Parquet fixture, SHA256
`115f46c063da52e2b3b8c883522e8d29a4559b410772bc08681a87235d783876`.
It is not original-fixture or GitHub Actions acceptance. Workflows and the
repository's original fixture are unchanged. No benchmark/RSS/merge-readiness
claim is made, and the previously deferred W4 target is not reopened or relabeled.
