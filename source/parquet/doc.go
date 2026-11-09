// Package parquet exposes flat Parquet files through native, bounded batch
// streams. Prepared partition lists preserve their exact order and subset.
// Signed and unsigned integer annotations retain their native widths and exact
// values. UTF-8 STRING, DATE, and UTC TIMESTAMP annotations are supported;
// DECIMAL precision 1..38 with nonnegative scale is decoded exactly to native
// decimal128 vectors. Wider decimals, TIME, UNKNOWN, and other unsupported
// annotations fail at OpenFile. Decimal arithmetic/expression execution is not
// implemented yet. Fixed-length DELTA_BYTE_ARRAY and INT32/INT64
// BYTE_STREAM_SPLIT are rejected because the upstream decoders are unavailable
// or unsafe on scalar fallback. Fixed-length BYTE_STREAM_SPLIT is supported.
// Selected decimal columns receive a page-bounded encoding-validation pass
// before decoding, so inaccurate footer lists cannot bypass these restrictions.
// This pass reads their complete selected row-group chunks even for an early
// Limit and may decompress pages again during the subsequent decoding pass.
// Decimal and unsigned value statistics are not used for row-group range
// pruning; null-count pruning remains available.
//
// Strings use charged, page-bounded view headers and copy only an admitted
// row span into owned output. No borrowed page is advanced until its views
// have been consumed. Near-budget outliers may trigger a string-column-only
// replay with inline cursors; subsequent batches restore normal capacity.
// Replay can reread preceding pages, but never buffers a whole row group.
// Owned vector and scratch capacity is reused across equal or smaller row
// groups within one execution; borrowed page state never crosses a group.
//
// BatchBytes and MemoryManager account for output capacities, typed scratch,
// borrowed-view arrays, and replacement-allocation overlap. O(columns) stream
// metadata, Go object overhead, Arrow page/dictionary/codec/metadata buffers,
// and process RSS are outside that accounting. Separate compressed and
// uncompressed page-body caps default to 8 MiB each and return recoverable
// errors before oversized bodies are allocated. They also cover dictionary
// page bodies, but do not cap aggregate pages across columns, decoded
// dictionary indexes, codec workspace, or all allocations in the process.
// BatchPayloadBytes separately caps the output's logical payload lengths;
// BatchPayloadTargetBytes is a soft multi-row target. A single row can exceed
// the target while fitting the hard payload and owned decode-buffer limits.
// Output caps do not charge typed scratch or borrowed-view header storage.
// Arrow's allocator has no error return, so this adapter does not fake an
// aggregate Arrow memory quota with allocator panics.
package parquet
