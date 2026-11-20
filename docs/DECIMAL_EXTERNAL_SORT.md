# Root Decimal external stable Sort (N13 candidate)

Explicit bounded execution with finite query RAM and a finite spill quota can
sort root native Decimal128 keys and payloads. This is the external Sort consumer,
not a new Decimal codec or an expansion of native container child types. Existing
TopK and resident Sort remain separate paths. Sort+Limit may use TopK, so it is
not evidence that external Sort ran.

Precision/scale are retained, coefficients use exact signed128 ordering, and
stable ties use the global input ordinal across runs and merge levels. Multiple
Decimal keys have their own fixed scales; comparisons do not convert to float.
NULL coefficient poison is ignored numerically. Root Decimal can accompany
existing supported native payloads; Decimal inside typed Struct/List, unsupported
opaque spill shapes, Join, windows and value/group aggregates remain outside
this slice. No shared consumer rejection is globally disabled.

Columnar runs encode 16-byte coefficients and explicit precision/scale; schema
fingerprints include that metadata. Decoding checks counts, array bounds,
precision/scale agreement, valid coefficient range and trailing bytes. The real
spill store also verifies exact file size, handle/schema/codec and SHA-256, not
CRC. Restore bytes and decoded ownership coexist and require separate admission.
Initial gather, encoded buffers, merge cursors/output and ordinal metadata also
have overlapping lifetimes; disk quota is not a RAM budget substitute.

Tests must prove nonzero actual spill and hierarchical merges through stream
diagnostics, not just options. The N13 lazy fixture keeps 65 mixed rows and64MiB
RAM, forcing 65 initial runs with one-row input batches; it checks exact output
against an independent big.Int stable oracle. NativeNested Parquet, IPC and owned
Collect roundtrips retain root Decimal metadata. Existing16MiB downstream budget
cases are not relaxed. Gate results and failures are recorded separately from
these acceptance specifications.

Cancellation is cooperative; some full-vector codec validation and third-party
I/O/hash operations are not interruptible at every instruction. Query accounting
does not prove all filesystem cache, codec temporary, allocator or RSS usage.
Cleanup must release query-owned run files and reservations on error/Close;
this is not a promise of arbitrary filesystem durability or universal panic
recovery. Historical failures and protected full-repository restrictions remain.

N14 separately adds global all-native-Decimal SUM queries only; see
[exact global SUM](DECIMAL_GLOBAL_SUM.md). Other aggregate/group/window
boundaries described above remain, rather than a general Decimal aggregate opening.

N15 additionally allows COUNT(*) and COUNT(root native Decimal) beside at least
one global native Decimal SUM; see [SUM with COUNT](DECIMAL_SUM_COUNT.md).
This does not open grouped or arbitrary mixed aggregates.
