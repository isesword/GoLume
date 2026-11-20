package parquet

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"sort"

	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/thrift/lib/go/thrift"
	"gocolumn/expr"
	"gocolumn/memory"
)

// One optional filter is read per row group. These caps bound encoded input and
// Arrow's additional decode copy, not whole-query memory or retained pool RSS.
const maxPruningBloomBytes int64 = 64 << 10

// bloomGroupExcluded uses only a negative membership result as evidence. It
// never treats a positive result as a matching row, nor negates membership to
// implement NOT. The residual predicate remains required.
func bloomGroupExcluded(ctx context.Context, r *file.Reader, group int, bindings []columnBinding, predicate expr.Expr) bool {
	var ranges bloomRangeValidation
	return bloomGroupExcludedCached(ctx, r, group, bindings, predicate, &ranges)
}

func bloomGroupExcludedCached(ctx context.Context, r *file.Reader, group int, bindings []columnBinding, predicate expr.Expr, ranges *bloomRangeValidation) (excluded bool) {
	defer func() {
		if recover() != nil {
			excluded = false
		}
	}()
	if predicate == nil || ctx.Err() != nil {
		return false
	}
	meta := r.MetaData()
	if meta.FileDecryptor != nil {
		return false
	}
	rg := meta.RowGroup(group)
	if rg.NumRows() < 1024 {
		return false
	}
	byName := make(map[string]columnBinding, len(bindings))
	for _, b := range bindings {
		if ctx.Err() != nil {
			return false
		}
		// Decoding these projected types cannot introduce conversion errors that
		// would disappear when the entire group is skipped.
		switch b.Type {
		case memory.TypeInt32, memory.TypeInt64, memory.TypeUint32, memory.TypeUint64,
			memory.TypeDate32, memory.TypeBool, memory.TypeFloat32, memory.TypeFloat64:
		default:
			return false
		}
		byName[b.Name] = b
	}
	anchor, ok := bloomAnchor(ctx, predicate, byName, 0)
	if !ok {
		return false
	}
	descr := meta.Schema.Column(anchor.Index)
	if descr.MaxRepetitionLevel() != 0 || descr.MaxDefinitionLevel() > 1 ||
		((anchor.Type == memory.TypeInt32 || anchor.Type == memory.TypeUint32) && descr.PhysicalType() != pq.Types.Int32) ||
		(anchor.Type == memory.TypeInt64 && descr.PhysicalType() != pq.Types.Int64) {
		return false
	}
	chunk, err := rg.ColumnChunk(anchor.Index)
	if err != nil || chunk.FilePath() != "" || chunk.CryptoMetadata() != nil {
		return false
	}
	off, size := chunk.BloomFilterOffset(), int64(chunk.BloomFilterLength())
	footer := meta.GetSourceFileSize() - 8 - int64(meta.Size())
	if off < 4 || size <= 0 || size > maxPruningBloomBytes || off > footer || size > footer-off || size >= chunk.TotalCompressedSize() {
		return false
	}
	if !ranges.validate(ctx, meta) {
		return false
	}
	encoded := make([]byte, int(size))
	reader := r.GetBloomFilterReader()
	if _, err = reader.Input.ReadAt(encoded, off); err != nil || ctx.Err() != nil || !bloomHeaderValid(ctx, encoded) {
		return false
	}
	// Decode exactly the bytes validated above. Arrow cannot fall back to the
	// original file if a malformed header advertises another read.
	bounded := *reader
	bounded.Input = bloomCachedReader{data: encoded, offset: off}
	gr, err := bounded.RowGroup(group)
	if err != nil {
		return false
	}
	err = gr.VisitColumnBloomFilter(anchor.Index, func(filter metadata.BloomFilter) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		truth := bloomTruth(ctx, predicate, anchor, filter, 0)
		excluded = truth&truthTrue == 0
		return ctx.Err()
	})
	return err == nil && ctx.Err() == nil && excluded
}

func bloomAnchor(ctx context.Context, e expr.Expr, bindings map[string]columnBinding, depth int) (columnBinding, bool) {
	if depth > 256 || ctx.Err() != nil {
		return columnBinding{}, false
	}
	var col expr.Column
	var ok bool
	switch n := e.(type) {
	case expr.Binary:
		if n.Op == expr.OpAnd || n.Op == expr.OpOr {
			if b, found := bloomAnchor(ctx, n.Left, bindings, depth+1); found {
				return b, true
			}
			return bloomAnchor(ctx, n.Right, bindings, depth+1)
		}
		if n.Op != expr.OpEQ {
			return columnBinding{}, false
		}
		col, _, _, ok = normalizeColumnLiteralComparison(n)
	case expr.InExpr:
		col, ok = n.Value.(expr.Column)
	case expr.Unary:
		if n.Op == expr.OpNot {
			return bloomAnchor(ctx, n.Value, bindings, depth+1)
		}
	}
	if !ok {
		return columnBinding{}, false
	}
	b, ok := bindings[col.Name]
	return b, ok && (b.Type == memory.TypeInt32 || b.Type == memory.TypeInt64 || b.Type == memory.TypeUint32)
}

func bloomTruth(ctx context.Context, e expr.Expr, anchor columnBinding, filter metadata.BloomFilter, depth int) truthSet {
	if depth > 256 || ctx.Err() != nil {
		return truthAny
	}
	switch n := e.(type) {
	case expr.Binary:
		if n.Op == expr.OpAnd || n.Op == expr.OpOr {
			return combineTruth(n.Op, bloomTruth(ctx, n.Left, anchor, filter, depth+1), bloomTruth(ctx, n.Right, anchor, filter, depth+1))
		}
		col, lit, op, ok := normalizeColumnLiteralComparison(n)
		if !ok || op != expr.OpEQ || col.Name != anchor.Name {
			return truthAny
		}
		return bloomEquality(lit, anchor, filter)
	case expr.InExpr:
		col, ok := n.Value.(expr.Column)
		if !ok || col.Name != anchor.Name || len(n.Values) == 0 {
			return truthAny
		}
		out := truthFalse
		for _, v := range n.Values {
			if ctx.Err() != nil {
				return truthAny
			}
			out = combineTruth(expr.OpOr, out, bloomEquality(v, anchor, filter))
		}
		return out
	case expr.Unary:
		if n.Op == expr.OpNot {
			return notTruth(bloomTruth(ctx, n.Value, anchor, filter, depth+1))
		}
	}
	return truthAny
}

func bloomEquality(e expr.Expr, b columnBinding, f metadata.BloomFilter) truthSet {
	if _, ok := e.(expr.NullLiteral); ok {
		return truthNull
	}
	lit, ok := e.(expr.Int64Literal)
	if !ok {
		return truthAny
	}
	out := truthFalse
	if b.Nullable {
		out |= truthNull
	}
	var present bool
	if b.Type == memory.TypeUint32 {
		if lit.Value < 0 || lit.Value > math.MaxUint32 {
			return out
		}
		// UINT32 uses physical INT32: retain all 32 bits, including the
		// high bit, and hash the same four-byte representation as the writer.
		typed := metadata.TypedBloomFilter[int32]{BloomFilter: f}
		present = typed.Check(int32(uint32(lit.Value)))
	} else if b.Type == memory.TypeInt32 {
		if lit.Value < math.MinInt32 || lit.Value > math.MaxInt32 {
			return out
		}
		typed := metadata.TypedBloomFilter[int32]{BloomFilter: f}
		present = typed.Check(int32(lit.Value))
	} else {
		typed := metadata.TypedBloomFilter[int64]{BloomFilter: f}
		present = typed.Check(lit.Value)
	}
	if present {
		out |= truthTrue
	}
	return out
}

// Preprocessing is lazy and happens at most once per stream. The reader owns
// immutable footer metadata for the lifetime of the stream. A failed attempt is
// cached too; optional pruning must not repeat expensive work on every group.
// No metadata pointer or temporary interval storage is retained by the cache.
type bloomRangeValidation struct{ attempted, valid bool }

func (v *bloomRangeValidation) validate(ctx context.Context, meta *metadata.FileMetaData) bool {
	if ctx.Err() != nil {
		return false
	}
	if !v.attempted {
		v.attempted = true
		v.valid = bloomFileRangesValid(ctx, meta)
	}
	return v.valid && ctx.Err() == nil
}

// 16,384 intervals use at most 256 KiB of temporary range storage. This cap
// does not include footer metadata, the optional encoded/decode buffers, Go
// object overhead or retained pools; it is not a process RSS guarantee.
const maxPruningBloomRanges = 16384

type bloomInterval struct{ start, end int64 }

// Bound metadata work before allocating, then enumerate each chunk once and
// sort disjoint half-open intervals. O(N log N) work replaces per-group O(N)
// scans; later groups reuse a boolean. Rejecting overlap among ANY intervals is
// deliberately conservative, including shared indexes or duplicated chunks.
func bloomFileRangesValid(ctx context.Context, meta *metadata.FileMetaData) (valid bool) {
	defer func() {
		if recover() != nil {
			valid = false
		}
	}()
	if meta == nil || ctx.Err() != nil || meta.FileDecryptor != nil || len(meta.RowGroups) > maxPruningBloomRanges {
		return false
	}
	fileSize := meta.GetSourceFileSize()
	if fileSize < 8 || meta.Size() < 0 || int64(meta.Size()) > fileSize-8 {
		return false
	}
	footer := fileSize - 8 - int64(meta.Size())
	if footer < 4 {
		return false
	}
	// Each chunk contributes at most four intervals: data, two indexes, Bloom.
	// Count raw slice lengths first; no ColumnChunk decoding or array allocation.
	chunks := 0
	for _, rg := range meta.RowGroups {
		if ctx.Err() != nil || rg == nil || len(rg.Columns) > maxPruningBloomRanges/4-chunks {
			return false
		}
		chunks += len(rg.Columns)
	}
	if chunks == 0 {
		return false
	}
	intervals := make([]bloomInterval, 0, 4*chunks)
	appendRange := func(start, length int64) bool {
		if start < 4 || length <= 0 || start > footer || length > footer-start {
			return false
		}
		intervals = append(intervals, bloomInterval{start, start + length})
		return true
	}
	for g := range meta.RowGroups {
		if ctx.Err() != nil {
			return false
		}
		rg := meta.RowGroup(g)
		for i := 0; i < rg.NumColumns(); i++ {
			if ctx.Err() != nil {
				return false
			}
			c, err := rg.ColumnChunk(i)
			if err != nil || c.FilePath() != "" || c.CryptoMetadata() != nil {
				return false
			}
			start := c.DataPageOffset()
			if c.HasDictionaryPage() {
				start = c.DictionaryPageOffset()
				if start >= c.DataPageOffset() {
					return false
				}
			}
			if !appendRange(start, c.TotalCompressedSize()) {
				return false
			}
			for _, get := range []func(int) (metadata.IndexLocation, bool){rg.ColumnIndexLocation, rg.OffsetIndexLocation} {
				if loc, ok := get(i); ok && !appendRange(loc.Offset, int64(loc.Length)) {
					return false
				}
			}
			// A nonzero offset with missing/negative length cannot establish an
			// interval. A length without an offset is inconsistent as well.
			if c.BloomFilterOffset() != 0 || c.BloomFilterLength() != 0 {
				if !appendRange(c.BloomFilterOffset(), int64(c.BloomFilterLength())) {
					return false
				}
			}
		}
	}
	return bloomIntervalsDisjoint(ctx, intervals)
}

func bloomIntervalsDisjoint(ctx context.Context, intervals []bloomInterval) (valid bool) {
	// Cancellation in sorting unwinds without caching a partially checked array.
	defer func() {
		if recover() != nil {
			valid = false
		}
	}()
	if ctx.Err() != nil || len(intervals) > maxPruningBloomRanges {
		return false
	}
	sort.Slice(intervals, func(i, j int) bool {
		if ctx.Err() != nil {
			panic("canceled optional Bloom interval sort")
		}
		return intervals[i].start < intervals[j].start
	})
	for i, v := range intervals {
		if ctx.Err() != nil || v.start < 4 || v.end <= v.start || (i > 0 && intervals[i-1].end > v.start) {
			return false
		}
	}
	return ctx.Err() == nil
}

type bloomCachedReader struct {
	data   []byte
	offset int64
}

// ReadAt reads only within the immutable, prevalidated Bloom extent.
func (r bloomCachedReader) ReadAt(p []byte, off int64) (int, error) {
	if off < r.offset || off-r.offset > int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off-r.offset:])
	if n != len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Read rejects sequential access outside the ReadAt-only contract.
func (r bloomCachedReader) Read([]byte) (int, error) {
	return 0, errors.New("bloom cached reader requires ReadAt")
}

// Seek rejects positioning outside the ReadAt-only contract.
func (r bloomCachedReader) Seek(int64, int) (int64, error) {
	return 0, errors.New("bloom cached reader requires ReadAt")
}

// Parse only the supported fixed-shape Compact Thrift header. Unknown fields,
// duplicate fields, nonempty marker structs and unsupported unions fail open.
// No attacker-provided list/string/container size is allocated or skipped.
func bloomHeaderValid(ctx context.Context, data []byte) bool {
	if len(data) == 0 || int64(len(data)) > maxPruningBloomBytes || ctx.Err() != nil {
		return false
	}
	transport := &thrift.TMemoryBuffer{Buffer: bytes.NewBuffer(data)}
	p := thrift.NewTCompactProtocolConf(transport, &thrift.TConfiguration{MaxMessageSize: int32(len(data))})
	if _, err := p.ReadStructBegin(ctx); err != nil {
		return false
	}
	seen := uint8(0)
	var size int32
	for {
		if ctx.Err() != nil {
			return false
		}
		_, kind, id, err := p.ReadFieldBegin(ctx)
		if err != nil {
			return false
		}
		if kind == thrift.STOP {
			if p.ReadStructEnd(ctx) != nil {
				return false
			}
			break
		}
		if id < 1 || id > 4 || seen&(1<<uint(id)) != 0 {
			return false
		}
		seen |= 1 << uint(id)
		if id == 1 {
			if kind != thrift.I32 {
				return false
			}
			size, err = p.ReadI32(ctx)
			if err != nil {
				return false
			}
		} else {
			if kind != thrift.STRUCT {
				return false
			}
			if _, err = p.ReadStructBegin(ctx); err != nil {
				return false
			}
			_, k, field, err := p.ReadFieldBegin(ctx)
			if err != nil || k != thrift.STRUCT || field != 1 {
				return false
			}
			if _, err = p.ReadStructBegin(ctx); err != nil {
				return false
			}
			_, k, _, err = p.ReadFieldBegin(ctx)
			if err != nil || k != thrift.STOP || p.ReadStructEnd(ctx) != nil || p.ReadFieldEnd(ctx) != nil {
				return false
			}
			_, k, _, err = p.ReadFieldBegin(ctx)
			if err != nil || k != thrift.STOP || p.ReadStructEnd(ctx) != nil {
				return false
			}
		}
		if p.ReadFieldEnd(ctx) != nil {
			return false
		}
	}
	return seen == 30 && size >= 32 && size%32 == 0 && int64(size) == int64(transport.RemainingBytes()) && ctx.Err() == nil
}
