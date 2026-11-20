package parquet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/memory"
)

const n25Day = int64(86400) * 1000000000

func n25WallClockFile(t *testing.T, logical schema.LogicalType, nested bool, groups [][]int64, defs [][]int16, options ...pq.WriterProperty) string {
	t.Helper()
	rep := pq.Repetitions.Optional
	if defs == nil {
		rep = pq.Repetitions.Required
	}
	node, e := schema.NewPrimitiveNodeLogical("clock", rep, logical, pq.Types.Int64, -1, -1)
	if e != nil {
		t.Fatal(e)
	}
	child := schema.Node(node)
	if nested {
		child, e = schema.NewGroupNode("inner", pq.Repetitions.Required, schema.FieldList{node}, -1)
		if e != nil {
			t.Fatal(e)
		}
	}
	root, e := schema.NewGroupNode("schema", pq.Repetitions.Required, schema.FieldList{child}, -1)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "clock.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	w := file.NewParquetWriter(f, root, file.WithWriterProps(pq.NewWriterProperties(options...)))
	for i, g := range groups {
		rg := w.AppendRowGroup()
		c, e := rg.NextColumn()
		if e != nil {
			t.Fatal(e)
		}
		var level []int16
		if defs != nil {
			level = defs[i]
		}
		n, e := c.(*file.Int64ColumnChunkWriter).WriteBatch(g, level, nil)
		if e != nil || n != int64(len(g)) {
			t.Fatal(n, e)
		}
		if e = c.Close(); e != nil {
			t.Fatal(e)
		}
		if e = rg.Close(); e != nil {
			t.Fatal(e)
		}
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	return path
}

func TestN25WallClockParquetNative(t *testing.T) {
	for _, dict := range []bool{false, true} {
		for _, v := range []pq.DataPageVersion{pq.DataPageV1, pq.DataPageV2} {
			path := n25WallClockFile(t, schema.NewTimeLogicalType(false, schema.TimeUnitNanos), false, [][]int64{{0, n25Day - 1}, {65, 999999999}}, [][]int16{{1, 0, 1}, {0, 1, 1}}, pq.WithDictionaryDefault(dict), pq.WithDataPageVersion(v))
			src, e := OpenFile(path)
			if e != nil {
				t.Fatal(e)
			}
			f, ok := src.Schema().Field("clock")
			if !ok || f.Type != memory.TypeTime64 || !f.Nullable || f.Encoding != memory.EncodingPlain {
				t.Fatal(f, ok)
			}
			o := testReadOptions()
			o.BatchRows = 2
			o.BatchBytes = 1024
			m := newBatchStreamPeakManager(1 << 20)
			o.MemoryManager = m
			ctx := context.Background()
			st, e := src.NewBatchStream(ctx, o)
			if e != nil {
				t.Fatal(e)
			}
			defer st.Close()
			b := memory.NewBatch()
			var actual []int64
			for {
				ok, e = st.Next(ctx, b)
				if e != nil {
					t.Fatal(e)
				}
				if !ok {
					break
				}
				z := b.Int64s["clock"]
				if z == nil || len(z.Values) != b.Rows {
					t.Fatal(b.Rows, z)
				}
				for i, x := range z.Values {
					if !z.IsValid(i) {
						x = -1
					}
					actual = append(actual, x)
				}
			}
			if e = st.Close(); e != nil {
				t.Fatal(e)
			}
			if !reflect.DeepEqual(actual, []int64{0, -1, n25Day - 1, -1, 65, 999999999}) || m.Used() != 0 || m.peak.Load() != 44 {
				t.Fatal(actual, m.Used(), m.peak.Load())
			}
		}
	}
}

func TestN25WallClockParquetRejects(t *testing.T) {
	for _, tc := range []struct {
		unit             schema.TimeUnitType
		adjusted, nested bool
	}{
		{schema.TimeUnitMicros, false, false}, {schema.TimeUnitNanos, true, false}, {schema.TimeUnitNanos, false, true},
	} {
		path := n25WallClockFile(t, schema.NewTimeLogicalType(tc.adjusted, tc.unit), tc.nested, [][]int64{{1}}, [][]int16{{1}})
		if _, e := OpenFile(path); e == nil {
			t.Fatal(tc)
		}
		if _, e := OpenFileWithOptions(path, Options{NativeNested: true}); e == nil {
			t.Fatal("native nested", tc)
		}
	}
	for _, bad := range []int64{-1, n25Day} {
		path := n25WallClockFile(t, schema.NewTimeLogicalType(false, schema.TimeUnitNanos), false, [][]int64{{bad}}, [][]int16{{0, 1}})
		src, e := OpenFile(path)
		if e != nil {
			t.Fatal(e)
		}
		o := testReadOptions()
		o.BatchRows = 2
		o.MemoryManager = memory.NewBudgetManager(1 << 20)
		st, e := src.NewBatchStream(context.Background(), o)
		if e != nil {
			t.Fatal(e)
		}
		defer st.Close()
		b := memory.NewBatch()
		ok, e := st.Next(context.Background(), b)
		if ok || e == nil || !strings.Contains(e.Error(), "out of range") || b.Rows != 0 || o.MemoryManager.Used() != 0 {
			t.Fatal(bad, ok, e, b.Rows, o.MemoryManager.Used())
		}
	}
}

func TestN25WallClockParquetAdmissionAndCancel(t *testing.T) {
	g := make([]int64, 65)
	ds := make([]int16, 65)
	for i := range g {
		g[i] = int64(i)
		ds[i] = 1
	}
	path := n25WallClockFile(t, schema.NewTimeLogicalType(false, schema.TimeUnitNanos), false, [][]int64{g}, [][]int16{ds})
	src, e := OpenFile(path)
	if e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"cancel", "reject"} {
		o := testReadOptions()
		o.BatchRows = 16
		o.BatchBytes = 1024
		o.MemoryManager = memory.NewBudgetManager(1 << 20)
		mgr := &rejectBatchStreamManager{}
		if mode == "reject" {
			o.MemoryManager = mgr
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		st, e := src.NewBatchStream(ctx, o)
		if e != nil {
			t.Fatal(e)
		}
		defer st.Close()
		b := memory.NewBatch()
		if mode == "cancel" {
			if ok, e := st.Next(ctx, b); !ok || e != nil {
				t.Fatal(ok, e)
			}
			cancel()
		}
		ok, e := st.Next(ctx, b)
		want := context.Canceled
		if mode == "reject" {
			want = memory.ErrBudgetExceeded
		}
		if ok || !errors.Is(e, want) || o.MemoryManager.Used() != 0 {
			t.Fatal(mode, ok, e, o.MemoryManager.Used())
		}
	}
}

func TestN25WallClockParquetRejectsUnsupportedEncoding(t *testing.T) {
	// Advertise an unsupported INT64 value encoding in metadata. The production
	// guard must fail before calling Arrow's typed value decoder.
	path := n25WallClockFile(t, schema.NewTimeLogicalType(false, schema.TimeUnitNanos), false, [][]int64{{1}}, [][]int16{{1}}, pq.WithDictionaryDefault(false))
	rewriteDecimalFooter(t, path, func(m *metadata.FileMetaData) {
		c := m.FileMetaData.RowGroups[0].Columns[0].MetaData
		for i := range c.Encodings {
			if int64(c.Encodings[i]) == int64(pq.Encodings.Plain) {
				reflect.ValueOf(&c.Encodings[i]).Elem().SetInt(int64(pq.Encodings.ByteStreamSplit))
			}
		}
	})
	_, e := OpenFile(path)
	if e == nil || !strings.Contains(strings.ToLower(e.Error()), "encoding") {
		t.Fatal(e)
	}
}

// Replace only the advisory footer encoding, leaving an actual unsupported
// data page. Opening must succeed, then page preflight must reject before any
// typed decoder or result page can use the data.
func TestN25WallClockParquetRejectsUnsupportedActualPage(t *testing.T) {
	path := n25WallClockFile(t, schema.NewTimeLogicalType(false, schema.TimeUnitNanos), false, [][]int64{{1, 2, 3}}, [][]int16{{1, 0, 1, 1}}, pq.WithDictionaryDefault(false), pq.WithEncoding(pq.Encodings.DeltaBinaryPacked))
	rewriteDecimalFooter(t, path, func(m *metadata.FileMetaData) {
		c := m.FileMetaData.RowGroups[0].Columns[0].MetaData
		for i := range c.Encodings {
			if int64(c.Encodings[i]) == int64(pq.Encodings.DeltaBinaryPacked) {
				reflect.ValueOf(&c.Encodings[i]).Elem().SetInt(int64(pq.Encodings.Plain))
			}
		}
	})
	src, e := OpenFile(path)
	if e != nil {
		t.Fatal(e)
	}
	o := testReadOptions()
	o.MemoryManager = memory.NewBudgetManager(1 << 20)
	ctx := context.Background()
	st, e := src.NewBatchStream(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	b := memory.NewBatch()
	ok, e := st.Next(ctx, b)
	if ok || e == nil || !strings.Contains(strings.ToLower(e.Error()), "page") || !strings.Contains(strings.ToLower(e.Error()), "encoding") || b.Rows != 0 || o.MemoryManager.Used() != 0 {
		t.Fatal(ok, e, b.Rows, o.MemoryManager.Used())
	}
}

func TestN25WallClockRequired(t *testing.T) {
	for _, value := range []int64{n25Day - 1, -1, n25Day} {
		path := n25WallClockFile(t, schema.NewTimeLogicalType(false, schema.TimeUnitNanos), false, [][]int64{{value}}, nil)
		src, e := OpenFile(path)
		if e != nil {
			t.Fatal(e)
		}
		if f, ok := src.Schema().Field("clock"); !ok || f.Nullable || f.Type != memory.TypeTime64 {
			t.Fatal(f, ok)
		}
		o := testReadOptions()
		o.BatchRows = 1
		o.MemoryManager = memory.NewBudgetManager(1 << 20)
		ctx := context.Background()
		st, e := src.NewBatchStream(ctx, o)
		if e != nil {
			t.Fatal(e)
		}
		defer st.Close()
		b := memory.NewBatch()
		got, e := st.Next(ctx, b)
		if value == n25Day-1 {
			z := b.Int64s["clock"]
			if !got || e != nil || z == nil || !z.IsValid(0) || z.Values[0] != value {
				t.Fatal(got, e, b.Rows, z)
			}
		} else if got || e == nil || !strings.Contains(e.Error(), "out of range") || b.Rows != 0 {
			t.Fatal(got, e, b.Rows)
		}
		_ = st.Close()
		if o.MemoryManager.Used() != 0 {
			t.Fatal(o.MemoryManager.Used())
		}
	}
}

func TestN25WallClockSingleRowBudgetBoundary(t *testing.T) {
	// A one-row optional TIME batch admits eight output bytes, an eight-byte
	// validity word, eight dense decode bytes and two definition-level bytes.
	path := n25WallClockFile(t, schema.NewTimeLogicalType(false, schema.TimeUnitNanos), false, [][]int64{{17, 83}}, [][]int16{{1, 0, 1}})
	src, e := OpenFile(path)
	if e != nil {
		t.Fatal(e)
	}
	for _, limit := range []int64{26, 25} {
		o := testReadOptions()
		o.BatchRows = 3
		o.MemoryManager = newBatchStreamPeakManager(limit)
		m := o.MemoryManager.(*batchStreamPeakManager)
		ctx := context.Background()
		st, e := src.NewBatchStream(ctx, o)
		if e != nil {
			t.Fatal(e)
		}
		b := memory.NewBatch()
		got, e := st.Next(ctx, b)
		if limit == 25 {
			if got || !errors.Is(e, memory.ErrBudgetExceeded) || b.Rows != 0 {
				t.Fatal(got, e, b.Rows)
			}
		} else {
			for i, want := range []int64{17, -1, 83} {
				if i != 0 {
					got, e = st.Next(ctx, b)
				}
				if !got || e != nil || b.Rows != 1 {
					t.Fatal(i, got, e, b.Rows)
				}
				z := b.Int64s["clock"]
				if z.IsValid(0) != (want != -1) || want != -1 && z.Values[0] != want {
					t.Fatal(i, z)
				}
			}
			if m.peak.Load() != limit {
				t.Fatal(m.peak.Load())
			}
		}
		if e = st.Close(); e != nil && !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal(e)
		}
		if m.Used() != 0 {
			t.Fatal(m.Used())
		}
	}
}
