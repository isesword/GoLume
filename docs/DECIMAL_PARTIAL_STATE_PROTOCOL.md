# Decimal partial-state protocol (N29 infrastructure)

N29 adds checked partial-state transport infrastructure. It does **not** enable
Decimal grouped aggregation spill execution. Existing engine admission guards,
ordinary aggregation codecs and public execution capabilities remain unchanged.

## Fixed state API and wire representation

```go
const aggregate.DecimalSumStateEncodedBytes = 40
func (s aggregate.DecimalSumState) EncodeChecked(dst []byte, inputPrecision int32) error
func (s *aggregate.DecimalSumState) DecodeChecked(src []byte, inputPrecision int32) error
```

Both buffers must have exactly 40 bytes. Offsets 0..31 encode four uint64 limbs
in little-endian order, least-significant limb first, representing a signed256
two's-complement sum. Offsets 32..39 encode the uint64 non-NULL SUM count in
little-endian order. No Go padding, native struct layout or machine endianness
is part of the protocol. The fixed state has no internal version tag; its
containing run protocol must identify the codec/version and immutable metadata.

Every encode and decode validates:

- input precision is in 1..38;
- count is no greater than MaxInt64;
- `abs(sum) <= count * (10^inputPrecision - 1)`.

Consequently count zero requires sum zero. The absolute-value calculation is
unsigned fixed-limb arithmetic, so the signed256 minimum cannot overflow a
signed absolute-value operation or accidentally pass validation. For admitted
states the bound is strictly less than 2^190.

The count here belongs to SUM's valid values. An independent COUNT aggregate
may have a uint64/MaxUint64 contract; callers must not substitute that limit for
SUM's MaxInt64 invariant.

## Atomicity, precision and scale

Encode checks the complete candidate before writing any destination byte.
Decode checks a local candidate before assigning the receiver. Invalid length,
precision, count or sum, and a nil decode receiver, return
`aggregate.ErrDecimalSumStateEncoding`; the destination remains unchanged.
The sentinel supports `errors.Is`.

Both operations are allocation-free fixed-limb calculations. They do not call
`Finalize`, convert to float, rescale coefficients, or truncate to Decimal128.
A valid partial sum may exceed signed128 and precision38, then cancel against
another partial during exact `Merge`. Only final aggregation output may perform
the precision38 finalization. Merge retains its checked count limit.

Scale is deliberately **not** in the 40-byte state. The containing run must bind
input precision, scale, lane kind and identity and reject incompatible metadata
before merging. These bytes alone are not permission to merge different units.
Structural validation also is not a checksum: bit changes that produce another
admissible state require containing-storage integrity checks to detect.

## Engine run envelope

The internal engine codec is `decimal-partial-state-v1`, stored as
`memory.SpillAggregateState`. All integers below are little endian. It carries
one nullable Int64 key, one Decimal SUM partial state, COUNT(valid), and
COUNT(rows). It is not a dispatchable aggregation strategy.

The fixed 32-byte header is:

| Offset | Bytes | Meaning |
| --- | --- | --- |
| 0 | 8 | ASCII magic `GLDPART1` |
| 8 | 2 | version, exactly 1 |
| 10 | 2 | flags, exactly zero |
| 12 | 4 | lane/key kind, exactly 1 |
| 16 | 4 | input precision, 1..38 |
| 20 | 4 | input scale, 0..precision |
| 24 | 8 | record count |

Each fixed 72-byte record is:

| Relative offset | Bytes | Meaning |
| --- | --- | --- |
| 0 | 8 | signed Int64 key payload |
| 8 | 1 | key validity, exactly 0 or 1 |
| 9 | 7 | reserved, all zero |
| 16 | 40 | checked DecimalSumState representation |
| 56 | 8 | COUNT(valid) |
| 64 | 8 | COUNT(rows) |

Encoding a NULL key writes key payload zero regardless of the caller's hidden
payload. Decoding rejects a nonzero NULL payload. Non-NULL key zero is distinct
from NULL. Every record requires `SUM.Count == COUNT(valid) <= COUNT(rows)`.
SUM.Count/COUNT(valid) is at most MaxInt64; COUNT(rows) may reach MaxUint64.
A count-zero, sum-zero partial is accepted, including a record with zero rows;
it is an identity for its same key/unit, not proof that a group occurred.

Decode validates exact total length, record-count arithmetic, magic, version,
kind, flags, reserved bytes, metadata equality, count relationships and the
precision-dependent partial bound. Unknown versions and trailing bytes fail.
The immutable metadata fingerprint is SHA256 of magic[8], precision[4] and
scale[4]. Restore additionally checks the outer object kind and codec name.
The existing query store supplies storage integrity checks; the typed codec
alone is not a cryptographic integrity check.

`mergeDecimalPartialRecord` requires matching key validity/value and matching
precision/scale. NULL hidden payloads are ignored and the committed key is
canonical zero. It checks both input states and uint64 row-count addition,
uses checked SUM Merge, and commits only after all checks and final cancellation
inspection succeed. Failure leaves the destination unchanged. It never calls
Finalize. Final precision is checked only after all partials have been merged by
a future executor.

## Ownership, admission and failure handling

The internal APIs are `encodeDecimalPartialRun`, `decodeDecimalPartialRun`,
`spillDecimalPartialRun` and `restoreDecimalPartialRun`. Every owner-producing
entry requires a non-nil manager with a positive finite limit. Spill additionally
requires a finite QuerySpillStore quota.

The input record slice and its source/group-table state are caller-owned and
must already be accounted by that caller. Encoding reserves for the new exact
wire payload plus bookkeeping before allocation. It does not claim to account
the caller's records. Decoding reserves for a new typed record slice plus
bookkeeping; it copies states and never borrows wire payload memory.

During SpillWithMetadata the encoded owner remains live, and the existing store
separately admits handle/map/path metadata through the supplied manager. During
restore, the store's restored payload owner and the new decoded typed owner
remain concurrently charged until decoding and restored-owner Close complete.
There are no additional uncharged run maps or caches.

Encoded and decoded owners release their buffers and reservations idempotently.
A file owner represents exactly one committed handle and has a separate
bookkeeping lease. Its close deletes through the query store. Delete failure
keeps the handle and owner lease so the caller can retry; cancellation or a nil
close context is an error from the store and is not treated as successful
cleanup. After successful deletion close is idempotent.

Operations check cancellation before work, periodically during record loops
and before publishing an owner. If cancellation is observed after a successful
spill commit, cleanup is attempted with a background context. Successful
cleanup returns no owner. Failed cleanup returns both the live owner and a
joined error; callers must retain and close that owner rather than discard it.
Restore errors release the temporary restored owner and any decoded owner;
the committed file remains available for retry or explicit deletion.

## Deliberate scope boundary

This is a single-run codec and lifecycle foundation, not Decimal grouped spill
execution. It has no group table, partition/repartition planner, merge scheduler,
end-of-group detection, or public execution admission. It does not use legacy
Int64Result materialization. The old grouped-spill rejection and codec dispatch
remain unchanged. AVG is not added to this run kind. Future executor work must
provide those components and validate complete bounded execution before claiming
spill aggregation support.
