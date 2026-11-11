# Native owned-copy admission in fan-out

This change closes a native Go-vector copy-admission gap in the bounded
multiplexer. It is **not** a complete fan-out or Opaque Arrow memory-budget fix.
The public ByteSizer still uses logical payload sizing. Borrowed partition fan-in
and stable-source rescans retain their existing behavior.

## Two explicitly separate accounting components

`EstimateNativeOwnedBatchRangeBytes` returns `NativeOwnedRangeEstimate`:

- `NativeBytes`: native Go-vector payload copy capacity, live old/new append
  growth, native vector/list descriptors, Batch/maps containers, and native
  Struct's actually copied top-level Fields and directly referenced name/domain
  strings under the documented model.
- `LegacyOpaqueBytes`: exactly the Opaque contribution of the pre-existing
  logical estimator. This includes Arrow `Data.SizeInBytes` and the Opaque Go
  validity bitmap's logical word length. It is not a retained-size upper bound.
- `Bytes`: the checked sum used for admission.
- `OpaquePresent`: also true for a zero-logical-byte Opaque array. A zero-byte
  item continues to consume the legacy minimum of one exchange token.

Mixed batches always keep the native component; they do not fall back wholesale
to old logical sizing. A pure-Opaque root keeps its old charge. A native Struct's
child Batch/maps remain native container metadata even when that child's runtime
entries happen to be Opaque. Opaque descriptors, bitmap spare capacity, retained
Arrow capacity, extension/private datatype graphs and lazy caches remain outside
this new native guarantee, including in mixed batches.

For independent owned copies, the exchange independently recomputes admission instead of trusting
`MorselRange.Bytes`, then acquires the same total from the group semaphore, global
semaphore and active query budget (or fallback manager). No allocation is admitted
by a post-copy top-up. Opaque sizing and cloning continue through the original
logical estimator and `CloneBatchRange`; there is no new Arrow type whitelist,
serialization, cache inspection or type conversion. Timestamp, official Bool8,
custom Array wrappers and custom extension types keep that legacy behavior.

`ExchangeStats.LegacyOpaqueItems` and `LegacyOpaqueBytes` count successfully
published owned items and their old Opaque logical charges. Multiplexer events
include `owned_admission_model=native_capacity_plus_legacy_opaque_logical`, plus
those Opaque counters. Total in-flight bytes are accounting charges, not a claim
that Arrow/private retention is fully bounded.

## Native model and lifecycle

Direct bulk native payload uses checked `n + ceil(n/4)`, plus the existing
64-byte per-backing rounding allowance. Numeric/Decimal, String, Dictionary and
List payload backings are pointer-free. `append(nil, slice...)` allocates once
with requested capacity equal to length; offsets/bitmaps use one `make`.
For Go 1.27.3, the noscan size-class table exceeds 1.25 times the requested size
by at most 6.75 bytes (33 to 48 bytes). Large allocations round to 8192-byte pages;
above 32768 bytes the extra page fraction is below one quarter of the request.
The 32761-through-32768-byte transition rounds to 32768 and also meets the bound.
Numeric/Decimal have at most two backings, String three, Dictionary four, and
List five across its two descriptor allowances. Summing logical sizes before
rounding up the quarter is safe with that existing per-backing slack. Every
simultaneously live output backing is included; bulk copies have no old target
backing to coexist with a replacement. The quarter is computed before checked
addition, avoiding an overflowing multiply-by-five intermediate.

Positive-length Struct row append retains four copies for live old/new growth,
including parent validity, conservative child payload, repeated Fields and
synthesized missing/NULL children. Empty Structs recursively use the bulk mode.
Pointer-bearing Fields remain in the separate metadata model, not this noscan
proof. This bound must be re-audited if the Go runtime or clone strategy changes.
Each native Batch includes a 4096-byte container allowance;
native map entries/vector descriptors use 1024 bytes, with a separate list-child
allowance. Copied top-level Fields and their directly visible names/domains are
charged separately. Caller-owned shared Field.Children graphs retain their old
accounting scope and are not traversed or newly bounded; deep and cyclic shared
schema metadata remains compatible. See the estimator's source comments for the
derivation. Runtime Struct.Values graphs must satisfy the same finite, acyclic
input invariant as the existing logical estimator and clone; this API is not a
general data-structure validator. Root/empty-Struct bulk String and List admission
checks endpoints, lengths and validity bounds without rescanning interior offsets:
the clone first allocates the already-budgeted fixed-length offset backing and
validates every interior before copying data. Invalid interiors still return an
error, and all reservations are released, but rejection occurs during cloning.
Non-empty Struct children retain full pre-copy offset validation, including NULL
parents, because their row-at-a-time copies could otherwise allocate data beyond
an endpoint-only bound. Valid input charges and splitting are unchanged.
It adds no fixed width/depth cutoff or
traversal/duplicate-name maps. Repeated Struct.Fields are charged per append
occurrence instead of rejected or checked by a quadratic duplicate scan.
These constants must be reconsidered if Go's allocation strategy changes.

Reservations and semaphore capacity are returned on failed reservation, cloning
errors, canceled/closed publication, consumption and branch draining. A delivered
item swaps its maps into the consumer destination. Release restores the caller's
original empty maps and drops the native clone maps with their charge, keeping
the destination reusable without leaving those clone maps attached to it.

This is an engine admission model, not an RSS ceiling. It excludes GC-delayed
garbage, allocator arenas, hidden backing of Go substrings, source/caller memory,
queue/setup allocations, producer ranges and the existing recursive estimation
call-stack scope. No new heap scratch maps are used for admission traversal.
Consumer activity after delivery has its own ownership scope. Shared metadata
can be conservatively counted more than once.

## Native clone-once sharing with guard-sized ranges

The bounded multiplexer first uses the same native-owned ByteSizer and guard
range boundaries described above. It may then clone one native range once and
publish independently clearable binding graphs that share immutable payload.
There is no whole-source-batch shortcut, logical-target reinterpretation,
cross-source coalescing, sparse caller-map borrowing, or cross-Next sizing cache.
The public ByteSizer remains logical; owned ranges retain their existing native
charge target, minimum-row exceptions, maximum rows and hard maximum bytes.

The existing stream contract forbids downstream mutation of borrowed vectors or
operations transferring their ownership (ADR_PHASE3_STREAM_CONTRACT.md).
Sharing relies on that contract, not on a new promise that writable Go fields
are mechanically immutable. Each branch has independent Batch/maps/vector and
List-child headers and independent recursive Struct.Values binding graphs.
Payload and Fields remain read-only. BorrowNativeWholeBatch copies the completed
clone's actual header lengths, including duplicate-Field append results; it does
not re-slice children to Rows. Consumer ClearColumns, root projection/drop and
binding replacement do not mutate another branch's binding graph. Destination
release restores the caller's original empty maps, as in the guard path.

Let C be the recomputed native clone charge, M its metadata allowance, and N the
number of prepared live bindings. The stable owner charge is
P = C - M + 256; each independent binding charge is H = M - 256.
Thus P + H = C and P + N*H <= N*C. The 256-byte transfer funds the stable owner;
the remaining per-Batch metadata allowance covers private bindings and the
per-binding pending/reference node. Runtime structure sizes and metadata
allowances are tested. Shared Fields are conservatively included in each
binding's metadata allowance. The owner stores only a reservation and reference
count, never the first branch's Batch or maps. After all but one binding have
been acknowledged, the remaining charge is C, not an uncharged retained payload.

The active context query has priority over the configured manager. Sharing uses
only the supported built-in immediate-reservation route; an unknown/custom
manager takes the independent guard path. TryReserveImmediate checks cancellation,
closed state and query soft/hard limits under the query reservation lock, then
attempts root admission without spill arbitration or reclaim callbacks. Lock
contention or insufficient soft/hard/root capacity is an unchanged-state miss.
Cancellation/closed/invalid-input errors are errors, not capacity misses. A miss
rolls back all unpublished reservations and credits before the same guard ranges
use ordinary admission, which retains its normal spill/reclaim semantics.

The first binding's owner and metadata are reserved before cloning. Each further
binding is separately reserved before its graph is allocated. A later admission
miss can therefore discard a previously constructed, unpublished clone; it does
not duplicate publication or leave live allocations uncharged. All views are
constructed before any publication. Producer references protect the owner and
pending node against an early consumer ACK. Publication detaches pending graph
references before sending; failed/closed sends and cancellation release their
unpublished bindings. No branch send lock is held while waiting for another
branch's credits. Release drops the binding graph before its ACK, and the final
binding plus construction ACK releases the shared owner reservation. No idle
pool, detached query accounting, or retained source borrow is used.

Any Opaque runtime entry at any Struct depth, including undeclared or zero-row
entries, makes sharing inapplicable. The entire range then uses the existing
native-plus-legacy-Opaque guard accounting and clone path, not all-logical
accounting. Timestamp, Bool8, custom wrappers and private extensions therefore
keep their original compatibility. One active branch also takes the guard path.

Each branch's group/global byte semaphores and ExchangeStats current/peak bytes
still debit C: the conservative footprint that branch can pin. Query/manager
reservations instead charge the unique shared owner once plus each live H.
Per-branch peaks must not be added and described as simultaneous unique memory,
and neither measure is RSS. Multiplexer shared/guard counters report selected
paths, not a claim that every input range can share. Source/caller storage,
consumer workspace, setup/queues, GC-delayed garbage and the previously stated
Opaque exclusions remain outside this native per-copy guarantee.

## Evidence and verification

The prior 257-row Struct containing List<String> reserved 2321 bytes and reported
2321 through `BatchBytes`, while its cloned slice capacities alone (including
Fields, excluding Batch/maps/descriptors/growth overlap) occupied 4784 bytes.
The one-row example reserved 17 bytes for at least 200 bytes of slice capacities.

Tests cover native splitting, exact/one-byte-short admission, mixed native/Opaque
accounting, Opaque compatibility, cancellation/close, query-budget authority and
consumer map reuse. The native model validates more metadata/offsets and often
splits more morsels; this has CPU cost and is not a performance-improvement claim.
Measure the final source with:

`go test ./pipeline -run '^$' -bench '^BenchmarkOwnedAdmissionSplit$' -benchmem -benchtime=300ms -count=3`

Performance and resource acceptance must use the final shared candidate, not
older independent-clone or whole-batch experiments. Compare both six drain
fixtures and three real Filter/Project/Drop workloads against the published
baseline, with interleaved samples, rows/checksums, allocations, source/output
item counts and charged budget peaks. Short local samples are not a throughput
guarantee. The ten-sample comparison and reproducible commands are recorded in
[FANOUT_REPRODUCIBILITY.md](FANOUT_REPRODUCIBILITY.md). The final local validation
matrix completed with no unexpected failed gates. The two original-fixture lazy
tests and fixture-dependent demo remain unverified, as do native ARM and remote
CI; this is not an unqualified full validation pass. The measurements do not
establish a strict per-case 3% regression threshold: physical Scalar's median is
higher, with no statistically significant difference in the ten-sample result.
Neither validation nor measurement implies release acceptance or publication.

Independent tests cover size-class/page boundaries and all direct native maps,
zero-allocation estimates, duplicate fields and deep runtime graphs, malformed
bulk rollback, sharing's last ACK, early Clear, branch Close/cancellation,
unknown-manager and Opaque fallback, and immediate-admission soft/root misses.
Local phase1 resource runs at 512 KiB succeeded for Scalar, String and StructList
in both fast and slow drain scenarios. That evidence does not prove arbitrary
consumer workspaces always fit, nor replace end-to-end operator resource tests.
The separate whole-source optimization was not adopted: successful immediate
admission could pin enough additional data to make a later consumer reservation
fail. It is not part of this candidate.

Validation remains local linux/amd64 Go 1.27.3. Native ARM and CI are not
established. The original parquet fixture is unavailable; its two dependent
end-to-end tests must be reported as excluded, never passed or replaced with a
substitute fixture.

## Separate unfinished Opaque contract

Complete Opaque admission remains future design work. Arrow's extension
Serialize/Deserialize protocol does not provide an allocation-peak or retained
private-state bound. Storage-only conversion loses extension semantics, and
rejecting previously supported extensions is not a compatibility-preserving fix.
A future change needs an explicit, reviewed extension memory/clone contract or
another separately agreed boundary. This patch does not complete that work.
