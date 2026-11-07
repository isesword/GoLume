# Expression evaluation boundaries

## One schema authority

`expr.Bind(schema, expression)` validates every branch and returns a `BoundExpr`
with the complete output `Field`, sorted input `Columns`, and conservative
`MayError` effect. `OutputField(name)` clones the field and assigns its output
name. Logical schema inference and physical projection construction use this
result directly, including list children, struct children, encoding, dictionary
identity, and nullability. They do not infer nested shapes independently.

A filter accepts only a bound Boolean expression. Boolean expressions are also
ordinary projected values; comparison, null-test, Boolean-column, conditional,
and membership results can be projected and then filtered again. The batch
representation remains `TypeBool` with a `Uint32Vector`; predicate execution
exposes immutable truth and validity bitmaps. Filtering owns a separate
selection bitmap equal to truth AND validity, with unused tail bits cleared.
`Series.ToAnySlice` exposes Boolean values as Go `bool` and SQL NULL as `nil`.

## Fluent lazy construction

The lazy DSL exposes `Bool`, `Null`, `If`, `Case`, `Coalesce`, and `In` as thin
constructors for the same expression nodes. Each supports `.Alias(name)`, for
example `If(Col("keep"), Col("x"), Lit(0)).Alias("chosen")`. `Case` accepts
`[]expr.WhenThen`; a nil ELSE produces a typed NULL. These constructors do not
add another binder or evaluation path.

Use `expr.Not(In(value, candidates...))` for NOT IN. It is ordinary SQL
three-valued negation: TRUE becomes FALSE, FALSE becomes TRUE, and NULL stays
NULL. In particular, a non-match with a NULL candidate is still NULL after NOT.

## Canonical execution API

`Compile(schema, expression)` and `CompileExpressions(schema, named)` both
produce one immutable `expr.Program`. Instructions, root registers, and branch
regions are private. Read-only access uses `Type()`, `Nullable()`,
`InstructionCount()`, `ResultField()`, `OutputCount()`, `OutputField(i)`,
`OutputName(i)`, and `OutputFields()`; returned field descriptions are deep
copies. `NewWorkspace(ctx, program, manager,
operatorName)` creates its execution-local `expr.Workspace`.
`Workspace.Evaluate(ctx, batch, kernels)` returns typed outputs;
`EvaluateBool` borrows a Boolean root without expanding its bitmap to a
`Uint32Vector`. Physical projections, filters, and aggregate predicates all
use these same entry points. There are no separate scalar/predicate program
or workspace contracts.

The workspace retains its opening query owner when a later context omits a
`QueryBudget` and rejects a different explicit owner. Admission failures are
retryable; semantic errors and cancellation close the workspace. `Close` is
idempotent, and `MemoryBytes` reports retained owned capacities. Program and
input storage are borrowed. `Program.WorkspaceBytes(rows)` describes fresh
fixed register/bitmap/vector backing; dynamic payloads, region workspaces,
and output metadata are admitted separately.

Comparisons preserve exact mixed integer/float ordering rather than converting
integers to rounded floating-point values. Float32 values are compared against
the original literal precision. NaNs never match ordinary comparisons; signed
zeroes compare equal. Parquet pruning rejects rounded integer metadata bounds
and retains possible FALSE results from NaNs omitted by min/max statistics,
so negation cannot lose matching NaN rows.

## Demanded outputs and demanded rows

The existing optimizer contract prunes unused projection outputs before
execution. A removed output is not demanded, even if evaluating it would have
raised an error. Its dependencies may also be removed. `MayError` does not
change this dead-output pruning contract.

For retained expressions, rewrites must preserve which rows demand computation
and the relevant expression order. In particular:

- Sequential filters evaluate the second predicate only on rows for which the
  first is TRUE. SQL AND also evaluates its right operand for NULL left values.
  A fallible second filter therefore cannot be freely merged into SQL AND.
- An outer safe filter cannot prune source rows before a fallible inner filter
  or retained computed projection. That could suppress a demanded cast,
  arithmetic, string, or JSON error.
- A filter cannot cross fallible sort-key evaluation for the same reason.
- Source metadata showing that a safe conjunct is always FALSE does not prove
  that an entire fallible predicate can be skipped. Parquet row-group pruning
  and Arrow RecordBatch pruning conservatively retain all partitions whenever
  the bound predicate `MayError` is true. Safe predicates retain normal pruning.

`MayError` describes row-value computation errors. It does not include
cancellation, memory admission failures, or malformed source storage. Binding
still validates all branches and dependencies before execution, including
branches that will never be selected for particular input rows.

## Regression coverage

- `optimizer/expression_error_domain_test.go` checks sequential-filter,
  projection, ordering, and unused-output rewrite boundaries.
- `physical/filter_selection_test.go` checks immutable Boolean results,
  owned selection masks, tail bits, and reserve-before-allocation replacement.
- `physical/unified_expressions_test.go` exercises projected predicates,
  conditionals and membership through compacting filters for plain/dictionary
  strings, exact UInt64 boundaries, Float32 literals, scalar and SIMD modes,
  plus logical/physical nested schema agreement.
- The Parquet and ArrowRecord `expression_error_domain_test.go` suites verify
  that direct source pushdown calls cannot bypass the error-domain guard.
- `physical/opaque_projection_test.go` verifies that projected opaque aliases
  and include-input values never transfer source ownership to borrowed output.
- `lazy/expression_error_domain_test.go` exercises sequential filters through
  Collect, Analyze, aggregate collection, and repeated prepared execution.
- `source/parquet/exact_float_pushdown_test.go` compares full and pruned residual
  results for NaN negation and exact mixed integer/float boundaries in scalar
  and SIMD modes.
- `lazy/unified_expression_dsl_test.go` validates the six fluent constructors,
  `.Alias`, nullable membership negation, and filtering projected aliases in
  scalar and SIMD modes.
