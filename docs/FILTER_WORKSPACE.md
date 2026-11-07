# Filter workspace ownership and bounded output

`physical.FilterSource` is the canonical batch-local predicate/compaction
operator. A prepared source is immutable. Every execution and producer worker
creates its own `expr.Workspace`, selection bitmap and indices, and reusable
compacted output. The aggregate predicate owner uses the same workspace rather
than duplicating expression execution or reservation logic.

`NewFilterProjectSource(input, predicate, columns)` evaluates the predicate
against the full input but only emits designated output columns. Nil columns
preserves the input schema. A nonnil empty slice emits row counts with no
columns. Names are validated, duplicates rejected, and the requested schema
order retained. A nil predicate selects every row. The ordinary
`NewFilterSource` constructor uses the same implementation with every column.

## Accounting and lifetime

- `Workspace` binds one immutable program and one execution owner.
  An external context `QueryBudget` takes precedence over a supplied manager;
  its optional diagnostic operator name defaults to `Expression` and the class
  is `MemoryWorkspace`. Without an external query the supplied manager is
  preserved. An absent manager uses an unlimited local accounting owner.
- Fixed expression capacity is reserved using `Program.WorkspaceBytes` before
  allocation. Dynamic dictionary caches, string/list/JSON payloads, branch
  regions, and exported output metadata own separate reservation tokens.
  `MemoryBytes` reports actual retained fixed and dynamic capacities.
  A context without a query keeps the opening owner and is bound to it before
  child work or reservations. The same explicit owner is accepted; a different
  explicit owner is rejected. A workspace opened without a query cannot adopt
  one later. `Close` is idempotent and terminal; a later evaluation needs a
  new workspace. Admission failures can retry; cancellation and semantic
  errors close the expression workspace.
- The selection bitmap owns the intersection of predicate truth and validity,
  including a masked final word. Predicate results are immutable and can borrow
  input or common-expression storage. Neither truth nor validity is modified.
  The bitmap and selection indices reserve their exact new backing capacities.
  Compaction preflights numeric values, validity, string bytes/offsets, dictionary indices,
  list children, and recursive struct buffers before allocating any payload.
  Buffer growth reserves the full replacement while old buffers remain charged.
  Failed reservation leaves the old workspace intact. Successful replacement
  drops old buffers before releasing the old token. Smaller batches reuse
  storage and retain its capacity charge, not just the current logical length.
- As with `memory.BatchBytes`, Go batch/map/vector headers are outside payload
  accounting. The index backing array and predicate metadata arrays are charged.
  Dictionary bytes/offsets and struct field schemas are borrowed from the
  child. Borrowed dictionary payload references are cleared before advancing
  the child so old input allocations are not kept alive without their owner.
- A successful output is borrowed until the next `Next` or `Close`. The stream
  clears the previous destination even when the next call uses a different
  destination. Opaque/struct ownership headers are independent non-owning
  views: clearing an output never changes child ownership or releases its
  Arrow references. EOF, cancellation, failure, and early/idempotent Close
  release every predicate, selection, index, and output reservation.
- Prepared partition order, duplicates, and empty subsets are forwarded
  exactly. Replayability, partition parallelism, visible ordering prefixes,
  and visible dictionary metadata are preserved.
- Native list NULL parents emit no hidden children, NULL string elements emit
  empty byte spans, and NULL struct parents normalize child values/validity.
  Preflight and copying follow the same rule so invisible large payloads are
  not charged or copied. Existing validity backing is retained as all-valid
  bits across nullable/all-valid batches and remains charged at its capacity.

## Opaque compatibility boundary

Partial opaque row gathering currently uses Arrow slice/concatenate routines
whose allocator cannot return admission errors and whose arbitrary nested
allocation sizes have no reserve-before-allocation contract. Therefore a
filter with an external `QueryBudget` or any explicit `ReadOptions.MemoryManager`
rejects discontiguous opaque output with `*memory.ResourceError` before gathering.
A contiguous selection of an Arrow array uses a zero-copy Arrow slice: only
its independently sized validity bitmap is reserved, and its retained reference
is released before the child advances. Non-Arrow opaque values still need an
explicit bounded gather contract.
This is an intentional restriction of bounded filtering, including for an
unlimited manager: the presence of an owner does not waive truthful accounting.
Projecting opaque columns out avoids this restriction while the predicate can
still read its necessary typed input columns. All-selected opaque output can
be borrowed and remains supported in bounded streams. Nested struct Arrow
slices follow the same source-owned payload and independent slice lifetime.

Direct streams with neither a context query budget nor an explicit memory
manager remain explicitly unbounded and preserve prior arbitrary Arrow gather
semantics. This is not a fallback when bounded admission fails. They must still
be closed, and their opaque results are borrowed from the filter's owned output.
Not every opaque type supports gathering: the existing Arrow gather must itself
support that type. Future bounded support needs a truthful allocation contract
or an error-returning budget-aware gather implementation.
