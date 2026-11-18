# Cumulative distribution windows

> Integration note: scope and semantics below are retained from the reviewed
> feature candidate. Stage-local test results and unpublished-state wording
> are historical; see [integration validation](DATE_WINDOW_INTEGRATION.md) for
> this branch update and its remaining limits.

`lazy.CumeDist(alias)` returns a non-null Float64 equal to the inclusive end
position of the current ordering peer divided by the partition row count.
Positions are one-based. A singleton or an all-peer partition returns 1 for
every row. NULL rows count toward the denominator. Empty input preserves schema
without inventing a data row.

Peer equality is the existing ordering equality, including NULL, NaN, signed
zero, multicolumn keys and descending order. Explicit ORDER BY remains mandatory.
Column must be empty and Frame is ignored as for Rank and PercentRank.
Float64 rounding applies to division; no exact rational representation is claimed.

Logical and physical WindowCumeDist are appended as enum value 14, leaving
WindowPercentRank at 13 and all old values intact. No fields are added to the
public window expression/options structs.

## One execution, three paths

Resident reuses the distribution-window captured input and independent result
batch. It scans each sorted partition by peer, writes peerEnd/N, and allocates
no peer array. Results are merged only after every mixed expression has finished
reading original inputs. Its separately charged result vector/bitmap/map metadata
uses the same per-output plan as PercentRank. Output pages preserve original
order, respect BatchRows, and retain the full owner until borrow invalidation.
Resident still materializes all input and is not input-bounded.

Ordered and external execution share one partition spool for CD-only or PR+CD.
A monotone lookahead cursor finds peer ends while the output cursor replays rows.
Each cursor has independent admitted decode ownership. A page cannot retire its
encoded backing until both cursors no longer need it for decoding; decoding ahead
is not permission to delete data the output cursor has not read.
No peer-end array, summary stream, source reopen or metadata pagination is added.

The additional lookahead restore/decode buffers, key lifetime and output vector
are real simultaneous resource costs. The original upstream tail-batch owner,
output decode and external restore sort may also remain live. Live-page metadata
still grows with partition size and can hit the hard budget. No arbitrary-size
O(1) memory claim is made.

External execution retains both actual sorts: partition/order sort before the
shared window and ordinal sort afterward to restore input order.
Diagnostics distinguish those sorts and the partition writes from actual
`peer_lookahead_pages` and `partition_replay_pages`. The new cursor counters
are emitted for CumeDist, not invented from aggregate query spill bytes.
Cold closed observers retain only integer counters, not source/schema graphs.

PR-only queries retain their single replay cursor and allocate no lookahead or
second ratio vector. Multiple aliases of either function share one private
vector for that function. PR+CD uses at most two private vectors and one spool.
Private names avoid original fields, all output aliases and each other.
Copied lowered expressions use the appropriate private column as zero-offset
Shift. All old mixed windows still read original input even when user aliases
replace partition, order or value fields.

The flat native profile and finite-memory requirements of the validated
PercentRank streaming implementation remain. This does not add Decimal, opaque,
nested transport, unsupported frames or the deferred deep spill metadata work.

## Frozen verification scope

Small cases cover all three paths, empty/singleton input, all peers, unique peers,
peers crossing pages and original-order restoration, both distributions together.
Independent review covers NULL/NaN/descending/mixed aliases and cursor failures.

The physical resource cases remain 262144 rows with four Int64 inputs,
BatchRows=1024, 4MiB hard memory and 256MiB disk. Both CD-only and PR+CD are tested
in ordered and external modes, plus the same ordered cases without disk expecting
a resource error. No budget or data size is relaxed after a failure.
Actual cursor counters and each sort's diagnostics are required as phase evidence.

Source and tests do not establish a passing result. Final validation is recorded
separately. This increment neither recovers the missing later candidate nor
completes item 18 or the blocked original repository gate; prior failures and
unpublished-state restrictions remain.
