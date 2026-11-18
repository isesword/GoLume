# Resident stable Sort with existing nested payloads (N8 candidate)

Resident Sort carries the existing bounded recursive Struct, flat Struct-item
List and two-level Int64/String List profiles under scalar ordering keys. The
container limits and NULL/empty distinctions are unchanged. Nested values are
payload, not comparable keys. TopK support for these same payloads is introduced separately by N9; see
NESTED_TOPK.md for replay, one-pass ownership and replacement-peak limits.
Native Decimal ordering also remains rejected.

Resident Sort still materializes the input and preserves stable input order for
equal keys, including parallel sorting. A finite RAM budget does not imply an
automatic spill fallback: no spill directory is required for resident execution.
Insufficient budget is an error. Original input ownership, ordinal scratch and
replacement output can overlap and must all remain admitted during sorting.
The external bounded execution policy remains a separate configured choice.

Owned Collect survives source/session closure; reader output is a revocable
borrow and expires on the next read or Close. Existing opaque Arrow sidecars,
Binary transport and temporal logical metadata retain their original contracts.
Mixed opaque output can emit one-row views instead of concatenating arbitrary
Arrow arrays. Generic IPC export of opaque TypeArrow still requires an explicit
Arrow adapter and is rejected without one. Binary/native-only projection can
use the existing exporter. IPC/Parquet transport does not expand native profiles.

The N8 candidate tests cover mixed maximum-depth payloads, NULL/empty values,
stable scalar ties across source batches, serial/parallel configurations, finite
RAM without spill, Collect and sink readback, source-close ownership, expired
borrows, cancellation/budget denial, and preserved TopK/nested-key/Decimal
boundaries. These are assertions pending current-candidate validation, not a
performance claim or full-repository gate claim. Prior failures and protected
full-repository restrictions remain in force.
