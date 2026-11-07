# Selection-aware Filter → Project boundary

## Scope and decision

An adjacent native `FilterSource` and `ProjectSource` can now pass borrowed
input plus ascending selected row indices internally instead of compacting every
filter output column first. Public `source.BatchStream` batches remain dense.
The immutable sources negotiate this capability once; every execution owns its
own predicate workspace, selection and expression workspace.

The initial boundary is deliberately conservative:

- The immediate producer must be the concrete native filter.
- All visible filter input fields must be native fixed-width types.
- The projection must be a supported row-local fixed-width expression program.
  The opcode whitelist includes numeric arithmetic, comparison, casts, Boolean
  expressions and guarded conditionals, recursively checking guarded regions.
- `includeInput` keeps the established materializing path. Benchmarks showed that
  expanding retained fields into explicit expression outputs increases metadata
  and allocations even when the filter passes every row.
- At least one input column must be omitted from projection dependencies. A
  full-width gather does not eliminate work compared with existing compaction.

Strings, dictionaries, binary, nested and opaque schemas, unknown producers,
non-whitelisted expressions and full-input projections retain the old boundary.
Profiling wrappers also retain the old boundary so their per-operator observations
are not bypassed. Ordinary untraced lazy lowering creates the native adjacent
operators and can use the optimization. Optimizer column pruning may already
reduce a wide source to a narrow filter input; the wide physical benchmark is an
upper-opportunity case, not a claim that every end-to-end query gets its speedup.
This is an internal execution optimization, not a new public sparse batch type.

## One executor and a dense selected domain

`Workspace.EvaluateSelected` gathers only the program's referenced fixed-width
columns, once per column, into reusable admitted storage. Specialized Boolean
instructions and dependencies inside guarded regions are included. The existing
typed executor then evaluates the compact selected domain. There is no second
interpreter and no full-input masked projection evaluation.

This first step does **not** eliminate every input copy. Referenced leaf vectors
are still gathered. It eliminates copies of columns the projection never reads
and avoids repeatedly scanning unselected rows in expression instructions. Direct
selected loads inside each kernel and selection transport through further
operators are separate future work, not claimed here.

Nil selection means ordinary all-row evaluation; a nonnil empty selection means
zero rows. Indices must be strictly increasing and in range. A public filter still
returns its original compacted output, and an all-pass private filter uses the
existing borrowed input range path without allocating selected gather buffers.

## Semantics and ownership

The predicate owns its truth-and-validity intersection and ordered indices. It
never modifies borrowed predicate buffers. The private input and indices remain
valid until the next filter call or close. The projection consumes all selected
prefixes before advancing the filter. Gathered values are owned by the expression
workspace, with original nullness and encoding preserved. Outputs remain borrowed
until the next projection call or close; the prior destination is cleared even
when the next call uses a different destination.

Rejected rows never enter projection evaluation. A rejected row's invalid cast
does not fail the query, while the same error on a selected row does. Conditional
regions keep their existing dense-domain short-circuit semantics and CSE scope.
Null predicates reject rows; row order and visible schema are unchanged.

## Admission and retries

Selection, dependency metadata, gathered values and validity capacities are
reserved before allocation. Growth admits a replacement while the previous
reservation is still live. Retained capacity is accounted until close; map/vector
headers follow the existing batch accounting convention. Query ownership cannot
change between calls.

Projection payload and budget retries shrink the **selected** prefix, discard the
candidate workspace and retry without advancing the source. Semantic errors are
never converted into resource retries. Cancellation, errors, EOF and close release
both operators' reservations. All-pass batches still honor ordinary range and
payload admission. A tighter budget may now succeed because unused filter output
buffers no longer exist; identical historical resource-failure thresholds are not
part of the contract.

## Validation

Differential tests compare the automatic path with an intentionally hidden filter
that forces the public materializing boundary. They cover selectivities from
0–100%, bitmap tails, nullable inputs, aliases, guarded expressions, multiple
batches, reused sources, changed destinations, rejected/selected cast errors,
budget retries, cancellation, close, and mixed/nested fallback. Expression tests
cover every native fixed-width physical/logical type, repeated cardinality changes,
nil versus empty selections, input preservation and reservation accounting.

Reproducible measurements and the final validation matrix are in
`validation/selection-project/`. Benchmark source/projected row metrics are logical
row counts, not instrumented kernel visits. Analytical copy-work comparisons must
be interpreted separately from measured time, allocations and peak reservations.
