# Root native Decimal128 TopK (N12 candidate)

Existing TopK and Sort(...).Limit(k) accept root native Decimal keys and payloads.
Each key column has fixed precision 1..38 and scale 0..precision. Ordering compares
signed high and unsigned low coefficient words exactly, never through float.
Multiple keys are lexicographic; direction, NULL placement and stable input ties
retain their existing meanings. Narrow replay key schemas must match payload
Decimal key precision/scale, not merely share a column name.

Replay-stable inputs use key selection then payload replay. Other inputs use
one pass and independently owned winner snapshots; no key source is opened and
no full-input Sort fallback is introduced. Native Decimal snapshots use the
existing typed container-owner path, not the legacy fixed snapshot layout.
A replacement owns the complete new winner before releasing the old one, so
K+1 temporary winners plus input/index/output overlap require admission.
K is not a fixed byte bound. Large values or K can still exceed finite RAM.
The one-pass snapshot path emits typed pages bounded by 64 rows (or a smaller
read limit); mixed opaque payload retains its existing one-row path. Replay
collects admitted winners in its existing output owner and may expose all K
typed rows in one batch; the snapshot page bound is not a replay guarantee.
This does not alter legacy flat fees.

Validation applies to each returned batch in both replay passes and snapshot
input, not only selected rows: vector metadata, length, bitmap, nonnullable NULL
and valid-coefficient precision matter. NULL coefficient bits are not values.
Decimal descendants inside typed Struct/List remain unsupported. Default opaque
transport is not native Decimal execution and descriptive opaque children must
not be rejected merely for containing Decimal metadata.

Native Arrow input remains opt-in. N11 NativeNested Parquet supports root
Decimal alongside the existing bounded container profiles, allowing arithmetic,
TopK, owned Collect, IPC and Parquet roundtrips. N13 separately adds bounded root Decimal external Sort (DECIMAL_EXTERNAL_SORT.md).
Join, window, aggregate and nested Decimal execution remain separate work; no global rejection
guard is removed. Existing historical failures and full-repository restrictions
remain. Query-budget accounting does not cover every third-party codec/page
allocation or RSS. No performance improvement or roadmap completion is claimed.

Acceptance uses an independent big.Int stable oracle, observed replay/one-pass
sources, differing batch boundaries, K near output page edges, source-close
ownership, and existing low-budget regressions. Test specifications do not
assert publication or that all current gates have passed.

N14 separately adds global all-native-Decimal SUM queries only; see
[exact global SUM](DECIMAL_GLOBAL_SUM.md). Other aggregate/group/window
boundaries described above remain, rather than a general Decimal aggregate opening.

N15 additionally allows COUNT(*) and COUNT(root native Decimal) beside at least
one global native Decimal SUM; see [SUM with COUNT](DECIMAL_SUM_COUNT.md).
This does not open grouped or arbitrary mixed aggregates.
