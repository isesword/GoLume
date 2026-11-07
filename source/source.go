package source

import (
	"context"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
)

// Partition identifies an independently readable source fragment such as a Parquet row group.
type Partition struct {
	ID   int
	Rows int64
}

// Stats summarizes source size for planning and memory decisions.
type Stats struct {
	Rows           int64
	Partitions     int
	Bytes          int64
	RowsUpperBound bool
}

// ReadOptions carries immutable execution services into a complete source
// stream chain. Kernels is mandatory so wrappers cannot silently rebuild or
// substitute a dispatch table.
type ReadOptions struct {
	Kernels kernel.Set
	// BatchRows caps the rows decoded by a native scan at once. Zero selects
	// DefaultBatchRows.
	BatchRows int
	// BatchBytes caps engine-owned decoded buffers and explicit scan scratch.
	// Zero selects DefaultBatchBytes. An oversized row/value returns a
	// resource error. Third-party decoder pages, codecs, and runtime overhead
	// and immutable source-owned buffers are excluded, so this is not a hard
	// heap/RSS cap.
	BatchBytes int64
	// BatchPayloadBytes optionally bounds the logical payload exposed by each
	// native output batch, including borrowed immutable values/dictionaries.
	// Zero adds no payload bound. This is distinct from owned allocation
	// accounting through BatchBytes and MemoryManager; it is not an RSS cap.
	BatchPayloadBytes int64
	// BatchPayloadTargetBytes is a soft payload target. A single row may
	// exceed this target up to BatchPayloadBytes; zero adds no soft target.
	BatchPayloadTargetBytes int64
	// MaxCompressedPageBytes and MaxUncompressedPageBytes bound decoder page
	// allocations independently of engine-owned BatchBytes. Zero selects the
	// corresponding default. These are per-page limits, not a heap/RSS cap;
	// sources without compressed pages do not use them.
	MaxCompressedPageBytes   int64
	MaxUncompressedPageBytes int64
	// MemoryManager charges live scan buffers to the owning query. A nil
	// manager leaves direct scans uncharged; OpenBatchStream binds the query
	// budget from its context when available.
	MemoryManager memory.Manager
}

const (
	// DefaultBatchRows is the native scan row limit when BatchRows is zero.
	DefaultBatchRows = 8192
	// DefaultBatchBytes is the native scan byte limit when BatchBytes is zero.
	DefaultBatchBytes = int64(8 << 20)
	// DefaultMaxCompressedPageBytes limits an encoded decoder page to 8 MiB.
	DefaultMaxCompressedPageBytes = int64(8 << 20)
	// DefaultMaxUncompressedPageBytes limits a decoded decoder page to 8 MiB.
	DefaultMaxUncompressedPageBytes = int64(8 << 20)
)

// Normalize fills zero batch limits without replacing the kernel set or
// memory manager. Negative limits remain invalid and are rejected by Validate.
func (o ReadOptions) Normalize() ReadOptions {
	if o.BatchRows == 0 {
		o.BatchRows = DefaultBatchRows
	}
	if o.BatchBytes == 0 {
		o.BatchBytes = DefaultBatchBytes
	}
	if o.MaxCompressedPageBytes == 0 {
		o.MaxCompressedPageBytes = DefaultMaxCompressedPageBytes
	}
	if o.MaxUncompressedPageBytes == 0 {
		o.MaxUncompressedPageBytes = DefaultMaxUncompressedPageBytes
	}
	return o
}

// Validate rejects the zero value instead of installing a hidden default.
func (o ReadOptions) Validate() error {
	if !o.Kernels.Valid() {
		return fmt.Errorf("source read options require a valid kernel set")
	}
	if o.BatchRows < 0 || o.BatchBytes < 0 || o.BatchPayloadBytes < 0 || o.BatchPayloadTargetBytes < 0 || o.MaxCompressedPageBytes < 0 || o.MaxUncompressedPageBytes < 0 {
		return fmt.Errorf("source batch and decoder page limits must be non-negative")
	}
	if o.BatchPayloadBytes > 0 && o.BatchPayloadTargetBytes > o.BatchPayloadBytes {
		return fmt.Errorf("source payload target exceeds hard limit")
	}
	return nil
}

// Source describes a partitioned columnar input with native streaming execution.
// Every source must implement NewBatchStream; there is no eager partition API.
type Source interface {
	BatchStreamProvider
	Schema() memory.Schema
	Stats(ctx context.Context) (Stats, error)
	Partitions(ctx context.Context) ([]Partition, error)
}

// Dictionary is source-level dictionary metadata. A source may implement
// DictionaryProvider when dictionary codes are stable across all partitions.
// Engine code must not assume code identity across batches without this
// contract (or an equivalent non-empty Field.DictionaryDomain contract).
type Dictionary struct {
	Domain  string
	Offsets []uint32
	Data    []byte
}

// Value returns the dictionary bytes for code or nil when code is out of range.
func (d Dictionary) Value(code uint32) []byte {
	i := int(code)
	if i < 0 || i+1 >= len(d.Offsets) {
		return nil
	}
	lo, hi := d.Offsets[i], d.Offsets[i+1]
	if lo > hi || int(hi) > len(d.Data) {
		return nil
	}
	return d.Data[lo:hi]
}

// DictionaryProvider exposes a stable source-level dictionary for a named column.
type DictionaryProvider interface {
	Dictionary(ctx context.Context, column string) (Dictionary, error)
}
