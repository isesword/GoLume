package parquet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	ps "github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/memory"
)

func n26TimeListBatch() (*memory.Batch, memory.Schema) {
	b := memory.NewBatch()
	b.Rows = 5
	b.Int64Lists = map[string]*memory.Int64ListVector{"clock": {
		Offsets:  []uint32{0, 1, 2, 2, 5, 6},
		Validity: memory.Bitmap{0b11101},
		Values:   &memory.Int64Vector{Values: []int64{-1, n25Day, 0, -1, n25Day - 1, 123456789}, Validity: memory.Bitmap{0b110111}, Encoding: memory.EncodingPlain},
		Encoding: memory.EncodingView,
	}}
	return b, memory.NewSchema(memory.ListTime64Field("clock", true, true))
}

type n26ListTimeGateManager struct {
	memory.Manager
	reject bool
}

func (m *n26ListTimeGateManager) Reserve(ctx context.Context, size int64) (memory.Reservation, error) {
	if m.reject {
		return nil, memory.ErrBudgetExceeded
	}
	return m.Manager.Reserve(ctx, size)
}

func TestN26ListTimeParquetRoundTrip(t *testing.T) {
	ctx := context.Background()
	for _, dict := range []bool{false, true} {
		for _, version := range []pq.DataPageVersion{pq.DataPageV1, pq.DataPageV2} {
			path := filepath.Join(t.TempDir(), "roundtrip.parquet")
			checked := am.NewCheckedAllocator(am.NewGoAllocator())
			batch, schema := n26TimeListBatch()
			// Row 0's out-of-range value is outside the export range; row 1 hides another
			// invalid value behind a NULL list and row 3 hides one behind a NULL child.
			rec, err := memory.ArrowRecordRange(checked, schema, batch, 1, 4)
			if err != nil {
				t.Fatal(err)
			}
			a, ok := rec.Column(0).(*array.List)
			if !ok || !a.IsNull(0) || a.IsNull(1) || a.ListValues().Len() != 4 {
				t.Fatal(rec)
			}
			vals, ok := a.ListValues().(*array.Time64)
			if !ok || vals.Value(0) != 0 || !vals.IsNull(1) || int64(vals.Value(2)) != n25Day-1 || vals.Value(3) != 123456789 {
				t.Fatal(a)
			}
			f, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			props := pq.NewWriterProperties(pq.WithDictionaryDefault(dict), pq.WithDataPageVersion(version), pq.WithMaxRowGroupLength(2), pq.WithAllocator(checked))
			w, err := pqarrow.NewFileWriter(rec.Schema(), f, props, pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(checked)))
			if err != nil {
				t.Fatal(err)
			}
			if err = w.Write(rec); err != nil {
				t.Fatal(err)
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			rec.Release()
			checked.AssertSize(t, 0)

			r, err := file.OpenParquetFile(path, false)
			if err != nil {
				t.Fatal(err)
			}
			d := r.MetaData().Schema.Column(0)
			tm, ok := d.LogicalType().(ps.TimeLogicalType)
			if !ok || tm.IsAdjustedToUTC() || tm.TimeUnit() != ps.TimeUnitNanos || d.PhysicalType() != pq.Types.Int64 || d.MaxRepetitionLevel() != 1 || d.MaxDefinitionLevel() != 3 {
				t.Fatal(d)
			}
			if r.NumRowGroups() != 2 {
				t.Fatal(r.NumRowGroups())
			}
			for j, expected := range [][]int16{{0, 1}, {3, 2, 3, 3}} {
				rg := r.RowGroup(j)
				c, e := rg.Column(0)
				if e != nil {
					t.Fatal(e)
				}
				vals, def, rep := make([]int64, 10), make([]int16, 10), make([]int16, 10)
				n, _, e := c.(*file.Int64ColumnChunkReader).ReadBatch(10, vals, def, rep)
				if e != nil || int(n) != len(expected) || !reflect.DeepEqual(def[:n], expected) {
					t.Fatalf("bad fixture dictionary=%v version=%v group=%d levels=%d defs=%v err=%v", dict, version, j, n, def[:n], e)
				}
				if e = c.Close(); e != nil {
					t.Fatal(e)
				}
			}
			if err = r.Close(); err != nil {
				t.Fatal(err)
			}

			src, err := OpenFileWithOptions(path, Options{NativeNested: true})
			if err != nil {
				t.Fatal(err)
			}
			if f, _ := src.Schema().Field("clock"); !reflect.DeepEqual(f, schema.Fields[0]) {
				t.Fatal(f)
			}
			o := testReadOptions()
			o.BatchRows = 1
			manager := newBatchStreamPeakManager(1 << 20)
			o.MemoryManager = manager
			st, err := src.NewBatchStream(ctx, o)
			if err != nil {
				t.Fatal(err)
			}
			var actual []any
			dst := memory.NewBatch()
			for {
				ok, err := st.Next(ctx, dst)
				if err != nil {
					t.Fatalf("dictionary=%v version=%v after %d rows: %v", dict, version, len(actual), err)
				}
				if !ok {
					break
				}
				v := dst.Int64Lists["clock"]
				if err = v.Validate(dst.Rows); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < dst.Rows; i++ {
					lo, hi, valid := v.ValueRange(i)
					if !valid {
						actual = append(actual, nil)
						continue
					}
					row := make([]any, 0, hi-lo)
					for j := lo; j < hi; j++ {
						if !v.Values.IsValid(j) {
							row = append(row, nil)
						} else {
							row = append(row, v.Values.Values[j])
						}
					}
					actual = append(actual, row)
				}
			}
			expected := []any{nil, []any{}, []any{int64(0), nil, n25Day - 1}, []any{int64(123456789)}}
			if !reflect.DeepEqual(actual, expected) {
				t.Fatalf("actual %#v want %#v", actual, expected)
			}
			if err = st.Close(); err != nil || manager.Used() != 0 || manager.peak.Load() == 0 {
				t.Fatal(err, manager.Used(), manager.peak.Load())
			}

			for _, mode := range []string{"allocation", "hard-row", "cancel"} {
				options := o
				manager := &n26ListTimeGateManager{Manager: memory.NewBudgetManager(1 << 20)}
				options.MemoryManager = manager
				if mode == "hard-row" {
					options.BatchPayloadBytes = 40
				}
				run, stop := context.WithCancel(ctx)
				stream, e := src.NewBatchStream(run, options)
				if e != nil {
					stop()
					t.Fatal(e)
				}
				out := memory.NewBatch()
				manager.reject = mode == "allocation"
				if mode == "cancel" {
					if ok, e := stream.Next(run, out); !ok || e != nil {
						stop()
						t.Fatal(ok, e)
					}
					stop()
				}
				for {
					ok, e := stream.Next(run, out)
					if ok && mode == "hard-row" {
						continue
					}
					want := memory.ErrBudgetExceeded
					if mode == "cancel" {
						want = context.Canceled
					}
					if ok || !errors.Is(e, want) || options.MemoryManager.Used() != 0 {
						t.Fatal(mode, ok, e, options.MemoryManager.Used())
					}
					break
				}
				stop()
				if e = stream.Close(); e != nil {
					t.Fatal(e)
				}
			}
		}
	}
}

func n26ListTimeFile(t *testing.T, logical ps.LogicalType, depth int, value int64, encoding pq.Encoding) string {
	t.Helper()
	req, opt := pq.Repetitions.Required, pq.Repetitions.Optional
	leaf, e := ps.NewPrimitiveNodeLogical("element", opt, logical, pq.Types.Int64, -1, -1)
	if e != nil {
		t.Fatal(e)
	}
	var rootChild ps.Node = leaf
	makeList := func(name string) {
		g, e := ps.NewGroupNode("list", pq.Repetitions.Repeated, ps.FieldList{rootChild}, -1)
		if e != nil {
			t.Fatal(e)
		}
		rootChild, e = ps.NewGroupNodeLogical(name, opt, ps.FieldList{g}, ps.ListLogicalType{}, -1)
		if e != nil {
			t.Fatal(e)
		}
	}
	makeList("clock")
	switch depth {
	case 1:
		rootChild, e = ps.NewGroupNode("outer", req, ps.FieldList{rootChild}, -1)
		if e != nil {
			t.Fatal(e)
		}
	case 2:
		makeList("outer")
	}
	root, e := ps.NewGroupNode("schema", req, ps.FieldList{rootChild}, -1)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "native.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	w := file.NewParquetWriter(f, root, file.WithWriterProps(pq.NewWriterProperties(pq.WithDictionaryDefault(false), pq.WithEncoding(encoding))))
	rg := w.AppendRowGroup()
	c, e := rg.NextColumn()
	if e != nil {
		t.Fatal(e)
	}
	d := int16(3)
	if depth == 2 {
		d = 5
	}
	n, e := c.(*file.Int64ColumnChunkWriter).WriteBatch([]int64{value}, []int16{d}, []int16{0})
	if e != nil || n != 1 {
		t.Fatal(n, e)
	}
	if e = rg.Close(); e != nil {
		t.Fatal(e)
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	return path
}

func TestN26ListTimeParquetRejections(t *testing.T) {
	ctx := context.Background()
	plain := pq.Encodings.Plain
	local := ps.NewTimeLogicalType(false, ps.TimeUnitNanos)
	open := func(p string) (*Source, error) { return OpenFileWithOptions(p, Options{NativeNested: true}) }
	for _, tc := range []struct {
		name     string
		logical  ps.LogicalType
		depth    int
		encoding pq.Encoding
	}{
		{"micros", ps.NewTimeLogicalType(false, ps.TimeUnitMicros), 0, plain},
		{"utc", ps.NewTimeLogicalType(true, ps.TimeUnitNanos), 0, plain},
		{"struct-list", local, 1, plain},
		{"nested-list", local, 2, plain},
		{"encoding", local, 0, pq.Encodings.DeltaBinaryPacked},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := n26ListTimeFile(t, tc.logical, tc.depth, 1, tc.encoding)
			if _, err := open(path); err == nil {
				t.Fatal("unexpected native support")
			}
		})
	}
	for _, bad := range []int64{-1, n25Day} {
		path := n26ListTimeFile(t, local, 0, bad, plain)
		src, err := open(path)
		if err != nil {
			t.Fatal(err)
		}
		n26AssertListTimeReadError(t, ctx, src, "outside")
		b, s := n26TimeListBatch()
		b.Int64Lists["clock"].Values.Values[0] = bad
		alloc := am.NewCheckedAllocator(am.NewGoAllocator())
		if rec, err := memory.ArrowRecordRange(alloc, s, b, 0, 1); err == nil || rec != nil || !strings.Contains(err.Error(), "outside") {
			if rec != nil {
				rec.Release()
			}
			t.Fatal(err)
		}
		alloc.AssertSize(t, 0)
	}
	path := n26ListTimeFile(t, local, 0, 1, pq.Encodings.DeltaBinaryPacked)
	rewriteDecimalFooter(t, path, func(m *metadata.FileMetaData) {
		c := m.FileMetaData.RowGroups[0].Columns[0].MetaData
		for i := range c.Encodings {
			if int64(c.Encodings[i]) == int64(pq.Encodings.DeltaBinaryPacked) {
				reflect.ValueOf(&c.Encodings[i]).Elem().SetInt(int64(pq.Encodings.Plain))
			}
		}
	})
	src, err := open(path)
	if err != nil {
		t.Fatal(err)
	}
	n26AssertListTimeReadError(t, ctx, src, "encoding")
}

func n26AssertListTimeReadError(t *testing.T, ctx context.Context, src *Source, want string) {
	t.Helper()
	o := testReadOptions()
	o.MemoryManager = memory.NewBudgetManager(1 << 20)
	st, err := src.NewBatchStream(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	b := memory.NewBatch()
	ok, err := st.Next(ctx, b)
	if ok || err == nil || !strings.Contains(err.Error(), want) || b.Rows != 0 || o.MemoryManager.Used() != 0 {
		t.Fatal(ok, err, b.Rows, o.MemoryManager.Used())
	}
	if err = st.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestN26ListTimeArrowRejectsMalformedOrRequiredNull(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*memory.Batch, *memory.Schema)
	}{
		{"malformed-offset", func(b *memory.Batch, _ *memory.Schema) { b.Int64Lists["clock"].Offsets[2] = 0 }},
		{"required-list", func(_ *memory.Batch, s *memory.Schema) { s.Fields[0].Nullable = false }},
		{"required-child", func(_ *memory.Batch, s *memory.Schema) { s.Fields[0].Children[0].Nullable = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, s := n26TimeListBatch()
			tc.edit(b, &s)
			alloc := am.NewCheckedAllocator(am.NewGoAllocator())
			if rec, e := memory.ArrowRecordRange(alloc, s, b, 1, 4); e == nil || rec != nil {
				if rec != nil {
					rec.Release()
				}
				t.Fatal(e)
			}
			alloc.AssertSize(t, 0)
		})
	}
}
