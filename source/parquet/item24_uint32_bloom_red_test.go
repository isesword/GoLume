package parquet

import (
	"context"
	"encoding/binary"
	"fmt"
	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/cespare/xxhash/v2"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

const item24Rows = 16384

func item24Fixture(t *testing.T, bloom bool, boundary ...bool) *Source {
	t.Helper()
	n, e := schema.NewPrimitiveNodeLogical("x", pq.Repetitions.Optional, schema.NewIntLogicalType(32, false), pq.Types.Int32, -1, -1)
	if e != nil {
		t.Fatal(e)
	}
	root, e := schema.NewGroupNode("schema", pq.Repetitions.Required, schema.FieldList{n, schema.NewInt64Node("id", pq.Repetitions.Required, -1)}, -1)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "uint32.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	w := file.NewParquetWriter(f, root, file.WithWriterProps(pq.NewWriterProperties(pq.WithDictionaryDefault(false), pq.WithPageIndexEnabled(false), pq.WithCompression(compress.Codecs.Uncompressed), pq.WithBloomFilterEnabled(bloom), pq.WithBloomFilterNDV(2), pq.WithDataPageSize(8192))))
	rg := w.AppendRowGroup()
	c, e := rg.NextColumn()
	if e != nil {
		t.Fatal(e)
	}
	vals := []int32{}
	defs := make([]int16, item24Rows)
	ids := make([]int64, item24Rows)
	for i := range ids {
		ids[i] = int64(i)
		if i%7 != 0 {
			defs[i] = 1
			v := int32(0)
			if i%2 != 0 {
				v = -1
			}
			if len(boundary) > 0 && boundary[0] && i%4 == 1 {
				v = -2147483648
			}
			vals = append(vals, v)
		}
	}
	if _, e = c.(*file.Int32ColumnChunkWriter).WriteBatch(vals, defs, nil); e != nil {
		t.Fatal(e)
	}
	if e = c.Close(); e != nil {
		t.Fatal(e)
	}
	c, e = rg.NextColumn()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.(*file.Int64ColumnChunkWriter).WriteBatch(ids, nil, nil); e != nil {
		t.Fatal(e)
	}
	if e = c.Close(); e != nil {
		t.Fatal(e)
	}
	if e = rg.Close(); e != nil {
		t.Fatal(e)
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	s := openBatchStreamFixture(t, path)
	if s.Schema().Fields[0].Type != memory.TypeUint32 {
		t.Fatalf("schema=%v", s.Schema())
	}
	cdesc := s.meta.Schema.Column(0)
	if cdesc.PhysicalType() != pq.Types.Int32 || !cdesc.LogicalType().Equals(schema.NewIntLogicalType(32, false)) {
		t.Fatalf("not physical INT32 unsigned logical32: %v", cdesc)
	}
	cc, err := s.meta.RowGroup(0).ColumnChunk(0)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := cc.Statistics()
	if err != nil || stats == nil || !stats.HasMinMax() {
		t.Fatalf("missing min/max: %v", err)
	}
	lo, hi := stats.EncodeMin(), stats.EncodeMax()
	if len(lo) != 4 || len(hi) != 4 || binary.LittleEndian.Uint32(lo) != 0 || binary.LittleEndian.Uint32(hi) != ^uint32(0) {
		t.Fatalf("unsigned raw bounds min=%x max=%x", lo, hi)
	}
	for _, probe := range []uint32{2147483647, 2147483648, 2147483649} {
		if probe <= binary.LittleEndian.Uint32(lo) || probe >= binary.LittleEndian.Uint32(hi) {
			t.Fatal("probe not enclosed")
		}
	}
	return s
}

type item24IO struct{ Data, Bloom, Footer, Other, Total int64 }

func item24Read(t *testing.T, s *Source, disable bool) (int, item24IO, item24IO) {
	t.Helper()
	ctx := context.Background()
	f, e := os.Open(s.path)
	if e != nil {
		t.Fatal(e)
	}
	counted := &pagePruningIOReader{f: f}
	props := pq.NewReaderProperties(nil)
	props.BufferedStreamEnabled = true
	props.PageStreamingEnabled = true
	r, e := file.NewParquetReader(counted, file.WithReadProps(props))
	if e != nil {
		t.Fatal(e)
	}
	type region struct {
		lo, hi int64
		kind   int
	}
	var regions []region
	for j := 0; j < 2; j++ {
		cc, err := r.MetaData().RowGroup(0).ColumnChunk(j)
		if err != nil {
			t.Fatal(err)
		}
		regions = append(regions, region{cc.DataPageOffset(), cc.DataPageOffset() + cc.TotalCompressedSize(), 1})
		if cc.BloomFilterOffset() > 0 {
			regions = append(regions, region{cc.BloomFilterOffset(), cc.BloomFilterOffset() + int64(cc.BloomFilterLength()), 2})
		}
	}
	regions = append(regions, region{r.MetaData().GetSourceFileSize() - 8 - int64(r.MetaData().Size()), r.MetaData().GetSourceFileSize(), 3})
	classify := func() item24IO {
		var out item24IO
		for _, v := range counted.reads {
			out.Total += v.end - v.first
			for _, r := range regions {
				n := min(v.end, r.hi) - max(v.first, r.lo)
				if n <= 0 {
					continue
				}
				switch r.kind {
				case 1:
					out.Data += n
				case 2:
					out.Bloom += n
				case 3:
					out.Footer += n
				}
			}
		}
		out.Other = out.Total - out.Data - out.Bloom - out.Footer
		if out.Other < 0 {
			t.Fatal("overlapping IO categories")
		}
		return out
	}
	setup := classify()
	counted.reads = nil
	st := s.batchStreamFromReader(ctx, testReadOptions().Normalize(), s.parts, r)
	st.disableBloomPruning = disable
	b := memory.NewBatch()
	rows := 0
	for {
		ok, err := st.Next(ctx, b)
		if err != nil {
			st.Close()
			t.Fatal(err)
		}
		if !ok {
			break
		}
		rows += b.Rows
	}
	io := classify()
	if e = st.Close(); e != nil {
		t.Fatal(e)
	}
	return rows, setup, io
}
func item24Push(t *testing.T, s *Source, p expr.Expr) *Source {
	t.Helper()
	out, report, e := s.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: p})
	if e != nil {
		t.Fatal(e)
	}
	if report.SelectedPartitions != 1 || report.PrunedPartitions != 0 {
		t.Fatalf("footer pruned: %+v", report)
	}
	return out.(*Source)
}
func item24Absent(t *testing.T, s *Source, v uint32) {
	t.Helper()
	f, e := os.Open(s.path)
	if e != nil {
		t.Fatal(e)
	}
	r, e := file.NewParquetReader(f)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	g, e := r.GetBloomFilterReader().RowGroup(0)
	if e != nil {
		t.Fatal(e)
	}
	seen := false
	e = g.VisitColumnBloomFilter(0, func(b metadata.BloomFilter) error {
		seen = true
		typed := metadata.TypedBloomFilter[int32]{BloomFilter: b}
		if typed.Check(int32(v)) {
			t.Fatalf("fixture false positive for %d", v)
		}
		return nil
	})
	if e != nil || !seen {
		t.Fatalf("independent Bloom missing: %v", e)
	}
}

func TestItem24Uint32BloomCorrectness(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		s := item24Fixture(t, enabled)
		for _, v := range []int64{2147483647, 2147483648, 2147483649, 4294967295} {
			for _, in := range []bool{false, true} {
				t.Run(fmt.Sprintf("bloom%v/value%d/in%v", enabled, v, in), func(t *testing.T) {
					var p expr.Expr = expr.EQ(expr.Col("x"), expr.I64(v))
					if in {
						p = expr.In(expr.Col("x"), expr.I64(v), expr.Null(memory.TypeInt64))
					}
					ps := item24Push(t, s, p)
					var want []int64
					if v == 4294967295 {
						for i := 0; i < item24Rows; i++ {
							if i%7 != 0 && i%2 != 0 {
								want = append(want, int64(i))
							}
						}
					}
					for _, src := range []source.Source{s, ps} {
						got := literalInMatchedIDs(t, src, p, kernel.ModeScalar, 257)
						if !slices.Equal(got, want) {
							t.Fatalf("oracle got=%d want=%d", len(got), len(want))
						}
					}
					rows, setup, io := item24Read(t, ps, false)
					t.Logf("decoded=%d setup=%+v execution=%+v", rows, setup, io)
					if (!enabled || v == 4294967295) && (rows != item24Rows || io.Data == 0) {
						t.Fatal("fallback/hit unexpectedly skipped data")
					}
				})
			}
		}
	}
}
func TestItem24Uint32BloomExpectedPruning(t *testing.T) {
	s := item24Fixture(t, true)
	for _, v := range []int64{2147483647, 2147483648, 2147483649} {
		item24Absent(t, s, uint32(v))
		for _, in := range []bool{false, true} {
			t.Run(fmt.Sprintf("value%d/in%v", v, in), func(t *testing.T) {
				var p expr.Expr = expr.EQ(expr.Col("x"), expr.I64(v))
				if in {
					p = expr.In(expr.Col("x"), expr.I64(v), expr.Null(memory.TypeInt64))
				}
				ps := item24Push(t, s, p)
				base, bs, bi := item24Read(t, ps, true)
				got, cs, ci := item24Read(t, ps, false)
				t.Logf("baseline decoded=%d setup=%+v execution=%+v; candidate decoded=%d setup=%+v execution=%+v", base, bs, bi, got, cs, ci)
				if base != item24Rows || bi.Data == 0 {
					t.Fatal("bad baseline")
				}
				if got != 0 || ci.Data != 0 || ci.Bloom == 0 {
					t.Fatalf("MISSING UINT32 BLOOM OPTIMIZATION: decoded=%d data=%d bloom=%d", got, ci.Data, ci.Bloom)
				}
			})
		}
	}
}

// Independent fixed-width byte oracle, not production literal conversion.
func TestItem24Uint32BloomFourByteProof(t *testing.T) {
	for _, boundary := range []bool{false, true} {
		t.Run(fmt.Sprintf("boundaryHit%v", boundary), func(t *testing.T) { item24FourByteProof(t, boundary) })
	}
}
func item24FourByteProof(t *testing.T, boundary bool) {
	s := item24Fixture(t, true, boundary)
	f, e := os.Open(s.path)
	if e != nil {
		t.Fatal(e)
	}
	r, e := file.NewParquetReader(f)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	g, e := r.GetBloomFilterReader().RowGroup(0)
	if e != nil {
		t.Fatal(e)
	}
	seen := false
	e = g.VisitColumnBloomFilter(0, func(b metadata.BloomFilter) error {
		seen = true
		typed := metadata.TypedBloomFilter[int32]{BloomFilter: b}
		for _, v := range []uint32{0, 2147483647, 2147483648, 2147483649, 4294967295} {
			four := []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)}
			eight := []byte{four[0], four[1], four[2], four[3], 0, 0, 0, 0}
			h4, h8 := xxhash.Sum64(four), xxhash.Sum64(eight)
			ht := metadata.GetHash(b.Hasher(), int32(v))
			if h4 != ht || h4 == h8 {
				t.Fatalf("width hash v=%d four=%x typed=%x eight=%x", v, h4, ht, h8)
			}
			want := v == 0 || v == 4294967295 || (boundary && v == 2147483648)
			if b.CheckHash(h4) != want || typed.Check(int32(v)) != want {
				t.Fatalf("membership v=%d want=%v", v, want)
			}
			t.Logf("v=%d bytes=%x hash4=%016x hash8=%016x present=%v", v, four, h4, h8, want)
		}
		return nil
	})
	if e != nil || !seen {
		t.Fatalf("Bloom missing %v", e)
	}
}
