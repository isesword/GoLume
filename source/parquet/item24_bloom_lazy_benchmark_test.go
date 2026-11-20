package parquet_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/cespare/xxhash/v2"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/lazy"
	"gocolumn/memory"
	"gocolumn/pipeline"
)

type item24LazyCell struct {
	name          string
	rows, groups  int
	lo, hi, query uint32
	bloom         bool
}

var item24LazyCells = []item24LazyCell{
	{"primary-high-miss", 16384, 1, 0x80000001, 0xfffffffe, 0xc0000033, true},
	{"high-hit", 16384, 1, 0x80000001, 0xfffffffe, 0x80000001, true},
	{"noBloom-miss", 16384, 1, 0x80000001, 0xfffffffe, 0xc0000033, false},
	{"held-cross-sign-miss", 16384, 1, 0x7ffffffe, 0x80000002, 0x80000000, true},
	{"held-many-miss", 4096, 16, 0x80000001, 0xfffffffe, 0xc0000033, true},
	{"held-small-group", 1023, 1, 0x80000001, 0xfffffffe, 0xc0000033, true},
}

type item24LazyWant struct {
	id int64
	x  uint32
}

func item24LazyFixture(tb testing.TB, c item24LazyCell) (string, []item24LazyWant) {
	tb.Helper()
	must := func(err error) {
		if err != nil {
			tb.Fatal(err)
		}
	}
	n, err := schema.NewPrimitiveNodeLogical("x", pq.Repetitions.Optional, schema.NewIntLogicalType(32, false), pq.Types.Int32, -1, -1)
	must(err)
	root, err := schema.NewGroupNode("schema", pq.Repetitions.Required, schema.FieldList{n, schema.NewInt64Node("id", pq.Repetitions.Required, -1)}, -1)
	must(err)
	path := filepath.Join(tb.TempDir(), "uint32.parquet")
	f, err := os.Create(path)
	must(err)
	w := file.NewParquetWriter(f, root, file.WithWriterProps(pq.NewWriterProperties(pq.WithDictionaryDefault(false), pq.WithPageIndexEnabled(false), pq.WithCompression(compress.Codecs.Uncompressed), pq.WithBloomFilterEnabled(c.bloom), pq.WithBloomFilterNDV(2), pq.WithDataPageSize(8192))))
	var want []item24LazyWant
	for g := 0; g < c.groups; g++ {
		rg := w.AppendRowGroup()
		col, err := rg.NextColumn()
		must(err)
		defs := make([]int16, c.rows)
		ids := make([]int64, c.rows)
		vals := make([]int32, 0, c.rows)
		for i := range ids {
			ids[i] = int64(g*c.rows + i)
			if i%7 == 0 {
				continue
			}
			defs[i] = 1
			x := c.lo
			if i%2 != 0 {
				x = c.hi
			}
			vals = append(vals, int32(x))
			if x == c.query {
				want = append(want, item24LazyWant{ids[i], x})
			}
		}
		_, err = col.(*file.Int32ColumnChunkWriter).WriteBatch(vals, defs, nil)
		must(err)
		must(col.Close())
		col, err = rg.NextColumn()
		must(err)
		_, err = col.(*file.Int64ColumnChunkWriter).WriteBatch(ids, nil, nil)
		must(err)
		must(col.Close())
		must(rg.Close())
	}
	must(w.Close())
	// Actual unsigned encoded min/max enclosure, with statistics enabled.
	rf, err := os.Open(path)
	must(err)
	r, err := file.NewParquetReader(rf)
	must(err)
	for g := 0; g < c.groups; g++ {
		rgmeta := r.MetaData().RowGroup(g)
		for col := 0; col < 2; col++ {
			if _, ok := rgmeta.ColumnIndexLocation(col); ok {
				tb.Fatalf("group %d column %d unexpected column index", g, col)
			}
			if _, ok := rgmeta.OffsetIndexLocation(col); ok {
				tb.Fatalf("group %d column %d unexpected offset index", g, col)
			}
			cm, err := rgmeta.ColumnChunk(col)
			must(err)
			if cm.HasDictionaryPage() {
				tb.Fatal("unexpected dictionary page")
			}
			if !c.bloom && (cm.BloomFilterOffset() != 0 || cm.BloomFilterLength() != 0) {
				tb.Fatalf("noBloom group %d column %d has Bloom metadata", g, col)
			}
		}
		cc, err := rgmeta.ColumnChunk(0)
		must(err)
		st, err := cc.Statistics()
		must(err)
		if st == nil || !st.HasMinMax() {
			tb.Fatal("missing statistics")
		}
		if c.bloom {
			if cc.BloomFilterOffset() <= 0 || cc.BloomFilterLength() <= 0 {
				tb.Fatalf("group %d missing Bloom metadata", g)
			}
			if cc.BloomFilterLength() > 64<<10 || int64(cc.BloomFilterLength()) >= cc.TotalCompressedSize() {
				tb.Fatalf("group %d Bloom size outside existing pruning guard", g)
			}
			br, err := r.GetBloomFilterReader().RowGroup(g)
			must(err)
			seen := false
			err = br.VisitColumnBloomFilter(0, func(filter metadata.BloomFilter) error {
				seen = true
				typed := metadata.TypedBloomFilter[int32]{BloomFilter: filter}
				for _, v := range []uint32{c.lo, c.hi, c.query} {
					var raw [4]byte
					binary.LittleEndian.PutUint32(raw[:], v)
					h := xxhash.Sum64(raw[:])
					if h != metadata.GetHash(filter.Hasher(), int32(v)) {
						tb.Fatal("four-byte physical hash mismatch")
					}
					wantMember := v == c.lo || v == c.hi
					got := filter.CheckHash(h)
					if got != wantMember || typed.Check(int32(v)) != wantMember {
						tb.Fatalf("fixed Bloom fixture blocked: group=%d value=%x present=%v want=%v; do not retune values", g, v, got, wantMember)
					}
				}
				return nil
			})
			must(err)
			if !seen {
				tb.Fatal("Bloom metadata present but filter not delivered")
			}
		}
		lo, hi := st.EncodeMin(), st.EncodeMax()
		if len(lo) != 4 || len(hi) != 4 || binary.LittleEndian.Uint32(lo) != c.lo || binary.LittleEndian.Uint32(hi) != c.hi || c.query < c.lo || c.query > c.hi {
			tb.Fatalf("unsigned bounds min=%x max=%x query=%x", lo, hi, c.query)
		}
	}
	must(r.Close())
	raw, err := os.ReadFile(path)
	must(err)
	tb.Logf("fixture=%s sha256=%x rows=%d groups=%d stats=ON pageindex=OFF dictionary=OFF nullEvery=7", c.name, sha256.Sum256(raw), c.rows*c.groups, c.groups)
	tb.Logf("actual proof: Bloom=%v queryPresent=%v allGroupsMinMaxEnclosing=true bothColumnIndexesAbsent=true; tiny membership checked independently of row-count pruning guard", c.bloom, c.query == c.lo || c.query == c.hi)
	return path, want
}

// Includes engine/session/source open, plan construction, Prepare, execution,
// typed consumption and all Close operations. No prepared cache is hoisted.
func item24LazyRun(tb testing.TB, path string, c item24LazyCell, want []item24LazyWant, full bool) uint64 {
	tb.Helper()
	must := func(err error) {
		if err != nil {
			tb.Fatal(err)
		}
	}
	s := lazy.NewSession(engine.Options{Workers: 1, KernelMode: kernel.ModeAuto, MemoryLimitBytes: 64 << 20, ExecutionPolicy: engine.ExecutionMemoryOnly, Pipeline: pipeline.Options{Workers: 1, MinMorselRows: 1024, MaxMorselRows: 1024}})
	defer s.Close()
	q, err := s.Engine().BeginQueryMemory()
	must(err)
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	f, err := s.ScanParquet(path)
	must(err)
	p, err := f.Filter(expr.EQ(expr.Col("x"), expr.I64(int64(c.query)))).Select("x", "id").Prepare(ctx)
	must(err)
	defer p.Close()
	rd, err := p.ExecuteBatches(ctx, lazy.BatchOptions{})
	must(err)
	defer rd.Close()
	count := 0
	var checksum uint64
	for rd.Next(ctx) {
		batch := rd.Batch()
		xs, err := batch.Column("x")
		must(err)
		ids, err := batch.Column("id")
		must(err)
		if xs.Type() != memory.TypeUint32 || ids.Type() != memory.TypeInt64 {
			tb.Fatalf("wrong types %v/%v", xs.Type(), ids.Type())
		}
		xc, err := xs.Uint32Chunks()
		must(err)
		ic, err := ids.Int64Chunks()
		must(err)
		if len(xc) != len(ic) {
			tb.Fatal("chunk alignment")
		}
		for j := range xc {
			if xc[j].Len() != ic[j].Len() {
				tb.Fatal("chunk length")
			}
			for i := 0; i < xc[j].Len(); i++ {
				x, valid, err := xc[j].Value(i)
				must(err)
				id, iv, err := ic[j].Value(i)
				must(err)
				if !valid || !iv {
					tb.Fatal("unexpected null selected row")
				}
				if full && (count >= len(want) || want[count] != (item24LazyWant{id, x})) {
					tb.Fatalf("row %d id=%d x=%d", count, id, x)
				}
				count++
				checksum += uint64(id) ^ uint64(x)
			}
		}
	}
	must(rd.Err())
	must(rd.Close())
	must(p.Close())
	must(s.Close())
	if count != len(want) {
		tb.Fatalf("count=%d want=%d", count, len(want))
	}
	if full {
		tb.Logf("cell=%s managed_budget=%+v; Arrow heap and initial source metadata are not fully charged by this counter", c.name, q.ScalarSnapshot())
	}
	if q.Used() != 0 {
		tb.Fatalf("query leaked %d", q.Used())
	}
	must(q.Close())
	return checksum
}

var item24LazyChecksum uint64

func TestItem24BloomLazyLifecycleOracle(t *testing.T) {
	for _, c := range item24LazyCells {
		t.Run(c.name, func(t *testing.T) { path, want := item24LazyFixture(t, c); item24LazyRun(t, path, c, want, true) })
	}
}
func BenchmarkItem24BloomLazyLifecycle(b *testing.B) {
	for _, c := range item24LazyCells {
		b.Run(c.name, func(b *testing.B) {
			path, want := item24LazyFixture(b, c)
			item24LazyRun(b, path, c, want, true)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				item24LazyChecksum = item24LazyRun(b, path, c, want, false)
			}
			b.StopTimer()
			item24LazyRun(b, path, c, want, true)
		})
	}
}
