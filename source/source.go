package source

import (
	"context"
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

// Source describes a partitioned columnar input and creates independent readers.
type Source interface {
	Schema() memory.Schema
	Stats(ctx context.Context) (Stats, error)
	Partitions(ctx context.Context) ([]Partition, error)
	NewReader(ctx context.Context) (Reader, error)
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
