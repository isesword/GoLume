// Package arrowrecord exposes Apache Arrow RecordBatch values through the
// engine's partitioned Source contract.
//
// The adapter accepts every Arrow field type. Types that already have a native
// engine vector use a typed fast path, while other Arrow arrays are preserved
// as memory.OpaqueVector values so lazy projection and schema planning do not
// reject nested, extension, decimal, union, or other Arrow types.
//
// Dictionary-encoded UTF-8 string columns are normalized once per dictionary
// value into a source-wide domain. Execution therefore keeps row values as
// integer codes even when individual input RecordBatches use different local
// dictionary orderings.
package arrowrecord
