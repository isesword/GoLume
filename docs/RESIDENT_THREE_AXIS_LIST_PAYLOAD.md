# Resident Sort: top-level three-axis List payload

The existing resident Sort constructor admits checked, canonical top-level
List<List<List<Int64>>> and List<List<List<String>>> payloads. Keys remain
existing scalar keys; original row ordinals retain stable ties and existing
NULL ordering. Existing supported native siblings are retained. No new API,
comparison semantics, quota increase, or nullable-selection owner is introduced.

## Explicit limits

The exception is local to resident Sort (Spillable=false). TopK, its separate
key input, spill, depth-three Lists beneath Struct parents, new leaf types, and
mixed native TIME Lists remain rejected by their existing or local guards.
The new profile rejects opaque runtime storage, including siblings and zero-row
batches, before any materialization. Legacy schemas retain their prior behavior. In particular, an existing opaque
Arrow List payload is not identified as the new native profile merely by its
logical shape: new-profile detection also requires canonical checked metadata.
This preserves the old opaque path, not new opaque support. A canonical native
three-axis payload combined with opaque siblings remains rejected.

## Strict ownership and validation

Each delivered batch is validated before the zero-row shortcut. The existing
full nested storage validator checks declared metadata, unexpected containers,
all offsets including hidden NULL spans, and required values in visible domains.
The recursive accumulator copies rows before the next upstream read. A strict
second accumulator applies sorted row ordinals. N39's root-optional nullable
selection helper is not used. Required roots are not silently made nullable.

Source batch storage and the growing retained input coexist under the existing
source/query accounting contract. Sorting additionally reserves ordinal and
comparator/worker storage. The full sorted output is allocated while input and
permutation remain charged; only then are old owners released. This is the
existing two-materialization resident algorithm, not an extra third Take copy.
Capacity growth preserves old-plus-new reservations until successful replacement.

The retained accumulator metadata now counts each actual List axis plus its
terminal node: four nodes for three axes rather than the old fixed three.
Borrowed output already uses BorrowedRangeMetadataBytesWithCheck, which charges
1024 bytes per ArrayNode recursively, plus necessary rebased offsets and
misaligned bitmaps. Its additional 4096-byte per-array map/header allowance is
unchanged. It was not the missing fourth-node charge and is not a reason to
increase quotas. Payload is borrowed from the sorted owner, not copied again.

The previous output is invalidated on Next or Close before view reservations
are released. Source poison after reading cannot alter retained or sorted
values. Errors and cancellation follow the existing terminal cleanup path.
Caller-retained output beyond this borrowed lifetime needs an explicit owned
copy and its own simultaneous reservation. This contract concerns accounted
owned buffers and borrowed descriptors, not whole-process RSS or arbitrary
source-private allocations.

## Evidence scope

The new independent fixture exercises native Parquet scanning, scalar stable
resident sorting, borrowed typed output, and actual IPC/Parquet round trips.
Budget and lifecycle tests cover full source/retained/permutation/output
coexistence, exact and minus-one limits, failures/cancellation, poison, and
unchanged unsupported consumers. Acceptance depends on those tests and the
cumulative gates; constructor admission alone is not an end-to-end result.
