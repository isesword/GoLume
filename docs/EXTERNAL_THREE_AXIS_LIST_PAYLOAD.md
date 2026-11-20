# External Sort with top-level three-axis List payload

This slice admits canonical top-level List³<Int64/String> payloads to the
existing finite-query, finite-disk external stable Sort with existing scalar
keys. It does not add List keys, depth-three Lists under Struct, mixed TIME,
opaque codec storage, new leaves, or a new TopK/replay policy.

## Codec prerequisite

The outer spill envelope remains version 1. Columnar payload versions remain
2 for existing scalar profiles, 3 for StructList, and 4 for repeated Lists.
Depth-three transport extends checked payload v4, not a new wire version.
Rejected obsolete internal columnar v1 is not read. Encoder and decoder both
check bounded schema before recursive planning/allocation. Only canonical
root-level depth three is added; all other existing profile checks remain.

The decoder metadata estimator follows the actual List axes and terminal type.
The root ArrayNode is already charged by map classification; each additional
node and the terminal Int64/String vector descriptor is separately rounded and
charged. Restored wire payload and decoded typed owners have distinct live
reservations. Nested offsets, validity and visible required domains remain
strict; this is not N39's root-optional nullable selection.

## Bounded execution preflight

Opening an external stream captures and validates its current input schema,
not just the schema seen by the constructor. A context-checked bounded walk
runs before recursive metadata estimation, SpillManager side effects, schema
cloning, row-size planning and fingerprinting. The metadata counting helper retains its counting-only contract (including
context, nesting and overflow limits); it does not require fields to be a
complete codec schema. The actual stream entry independently applies full
codec admission to the captured schema before store/reserve/clone side effects. The execution keeps a cloned input schema view alongside its
internal stable-ordinal schema, rather than re-reading a mutable Source.Schema
for later initial-run plans. This does not promise safety for concurrent data
races mutating caller objects during a call.

The existing initial selected-row gather, recursive accumulator, spill/run
owners and four-way hierarchical merge are reused. Runs contain bounded
physical segments; logical run merging, not raw segment-count reduction,
determines progress. Source, gather, wire encode, restore, decoded cursor and
merge-output reservations overlap as required. No extra whole-result owner,
nullable Take, quota increase or unaccounted cache is added. Existing finite
disk accounting, synchronization and cleanup remain the execution contract.

## Evidence and limitations

Stage 1 independently tests Encode and hand-built valid v4 Decode, including
actual-node metadata and malformed/truncated payloads. Stage 2 must establish
real initial runs and multiple merge levels, stable equal-key ordering, native
borrowed output and IPC/Parquet round trips. Resource tests must cover observed
memory/disk peaks, exact/minus-one limits, failures, cancellation and deletion.
The codec prerequisite alone is not proof that external sorting is complete.
No claim concerns whole-process RSS or unlimited caller-owned source storage.
