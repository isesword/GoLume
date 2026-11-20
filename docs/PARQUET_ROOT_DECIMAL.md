# Root Decimal with NativeNested Parquet (N11 candidate)

Explicit `parquet.Options{NativeNested: true}` now composes existing bounded
container forests with top-level native Decimal128 columns. The lazy equivalent
is `ParquetScanOptions{NativeNested: true}`. Default flat reads are unchanged.
This is a candidate scope description, not a publication or full-gate claim.

Precision is 1..38 and scale is 0..precision. INT32 (precision <=9), INT64
(<=18), BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY retain signed coefficients exactly.
Byte coefficients are signed big endian; valid redundant sign extension is
accepted, overflow and invalid precision are rejected. NULL coefficients are not
values. No floating-point conversion, rescaling or Decimal-in-container support
is introduced. Existing depth/shape restrictions and unsupported consumers stay
in force.

Root Decimal occupies row coordinates alongside every complete forest row.
List element/repetition coordinates never advance the root column. Successful
short batches crop Decimal value and validity slices to the actual row count;
budget prefix returns must neither repeat nor skip a coefficient. Projection
can select only Decimal or only containers. Precision/scale survive native
re-entry, arithmetic, resident stable Sort, owned Collect and IPC/Parquet output.
N12 adds root Decimal TopK and N13 adds bounded external Sort as separate
consumer slices; see DECIMAL_EXTERNAL_SORT.md. See [Decimal TopK](DECIMAL_TOPK.md).

Output coefficients cost 16 bytes each plus validity; existing metadata and
simultaneous list staging/replacement reservations remain charged. Conversion
uses one page-borrowed coefficient at a time, not row-sized byte-header arrays.
Footer encoding checks are supplemented by actual-page preflight before decode.
Unsupported fixed DELTA_BYTE_ARRAY and numeric BYTE_STREAM_SPLIT combinations
remain rejected rather than exposed to unsupported third-party decoders.

Page streaming and compressed/uncompressed page caps remain enabled. They are
not a claim that every third-party page/decompression temporary is charged to
the query MemoryManager. Query reservation traces and Arrow allocator cleanup
checks are distinct evidence. Cancellation is cooperative, not instantaneous
inside every third-party page operation. Error/Close paths release owned output,
staging, reader and metadata state; returned source batches are borrowed.

N11 acceptance combines physical-format/NULL/row-group/projection coverage with
reservation fault injection and mixed Arrow-to-Parquet-to-native integration.
Earlier failed runs and limitations are retained in validation evidence; this
document does not supersede those records or claim unrestricted generic nesting.
