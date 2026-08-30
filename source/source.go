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

// Reader reads source partitions into a caller-owned reusable Batch.
type Reader interface {
	ReadPartition(ctx context.Context, p Partition, dst *memory.Batch) error
	Close() error
}

// ReadOptions carries immutable execution services into a complete source
// reader chain. Kernels is mandatory so wrappers cannot silently rebuild or
// substitute a dispatch table.
type ReadOptions struct{ Kernels kernel.Set }

// Validate rejects the zero value instead of installing a hidden default.
func (o ReadOptions) Validate() error {
	if !o.Kernels.Valid() {
		return fmt.Errorf("source read options require a valid kernel set")
	}
	return nil
}

// Source describes the metadata required to plan a partitioned columnar input.
// Execution is provided by BatchStreamProvider. During the Phase 3 migration,
// scan implementations may instead implement LegacyReaderSource and are
// adapted exactly once by OpenBatchStream at the external source boundary.
type Source interface {
	Schema() memory.Schema
	Stats(ctx context.Context) (Stats, error)
	Partitions(ctx context.Context) ([]Partition, error)
}

// LegacyReaderSource is the temporary compatibility contract for external
// scan extensions written against the pre-Phase-3 partition Reader API.
//
// Deprecated: implement BatchStreamProvider. This adapter is scheduled for
// removal in GoLume v0.8. Physical operators must never implement this
// interface; it exists only at the external source boundary.
type LegacyReaderSource interface {
	Source
	NewReader(ctx context.Context, opts ReadOptions) (Reader, error)
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
