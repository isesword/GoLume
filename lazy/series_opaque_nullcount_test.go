package lazy

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/pipeline"

	"gocolumn/memory"
)

// This oracle does not derive its answer from an Arrow array or bitmap helper.
func TestSeriesNullCountOpaqueIndependent129(t *testing.T) {
	const rows = 129
	validity := make(memory.Bitmap, (rows+63)/64)
	var want int64
	for i := 0; i < rows; i++ {
		if i%3 == 0 {
			want++
		} else {
			validity[i/64] |= uint64(1) << uint(i%64)
		}
	}
	if want != 43 {
		t.Fatalf("fixture oracle=%d, want 43", want)
	}
	batch := memory.NewBatch()
	batch.Rows = rows
	batch.Opaque["opaque"] = &memory.OpaqueVector{Rows: rows, Type: memory.TypeArrow, Validity: validity, Value: struct{}{}}
	df := newDataFrame(memory.NewSchema(memory.Field{Name: "opaque", Type: memory.TypeArrow, Nullable: true}))
	df.state.batches = []*memory.Batch{batch}
	df.state.rows = rows
	defer df.Close()
	series, err := df.Column("opaque")
	if err != nil {
		t.Fatal(err)
	}
	if got := series.NullCount(); got != want {
		t.Fatalf("opaque parent nulls=%d, want %d", got, want)
	}
}

// The nil embedded interface makes every Arrow payload method panic. NullCount
// must neither inspect the payload nor change its retain/release ownership.
type nullCountUnreadableArrow struct{ arrow.Array }

func TestSeriesNullCountOpaqueBitmapAndPayloadMatrix(t *testing.T) {
	payloads := []struct {
		name  string
		value any
	}{
		{"nil", nil},
		{"non-arrow", struct{}{}},
		{"unreadable-arrow", &nullCountUnreadableArrow{}},
	}
	for _, typ := range []memory.DataType{memory.TypeArrow, memory.TypeDecimal} {
		for _, rows := range []int{0, 1, 63, 64, 65, 129} {
			for _, mode := range []string{"nil", "valid", "null", "mixed"} {
				for _, paddingSet := range []bool{false, true} {
					for _, payload := range payloads {
						name := fmt.Sprintf("%s/rows%d/%s/padding%t/%s", typ, rows, mode, paddingSet, payload.name)
						t.Run(name, func(t *testing.T) {
							// Extra vector rows, bitmap words, and final-word padding are
							// caller-owned and outside the Series logical row range.
							var bitmap memory.Bitmap
							if mode != "nil" {
								bitmap = make(memory.Bitmap, (rows+63)/64+2)
								for i := range bitmap {
									if paddingSet {
										bitmap[i] = ^uint64(0)
									}
								}
							}
							var want int64
							for i := 0; i < rows; i++ {
								valid := mode == "nil" || mode == "valid" || mode == "mixed" && i%3 != 0
								if !valid {
									want++
								}
								if bitmap != nil {
									bit := uint64(1) << uint(i%64)
									if valid {
										bitmap[i/64] |= bit
									} else {
										bitmap[i/64] &^= bit
									}
								}
							}
							before := append(memory.Bitmap(nil), bitmap...)
							b := memory.NewBatch()
							b.Rows = rows
							v := &memory.OpaqueVector{Rows: rows + 17, Type: typ, Validity: bitmap, Value: payload.value}
							b.Opaque["opaque"] = v
							df := nullCountOpaqueFrame(typ, b)
							defer df.Close()
							nullCountCheck(t, df, "opaque", int64(rows), want)
							if !reflect.DeepEqual(bitmap, before) || v.Rows != rows+17 || v.Owned || v.Value != payload.value {
								t.Fatal("NullCount changed caller-owned storage or ownership")
							}
						})
					}
				}
			}
		}
	}
}

func TestSeriesNullCountOpaqueIndependentArrowMasksAndViews(t *testing.T) {
	const rows, prefix = 129, 3
	alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	builder := array.NewLargeStringBuilder(alloc)
	for i := 0; i < rows+prefix; i++ {
		if i%5 == 0 {
			builder.AppendNull()
		} else {
			builder.Append(fmt.Sprint(i))
		}
	}
	full := builder.NewArray()
	builder.Release()
	defer full.Release()
	// Arrow parent nulls are (i+3)%5 == 0, while Go parent nulls are
	// i%3 == 0. These are independent masks (26 versus 43 nulls).
	for _, widths := range [][]int{{129}, {33, 64, 32}, {64, 65}} {
		t.Run(fmt.Sprint(widths), func(t *testing.T) {
			var batches []*memory.Batch
			var snapshots []memory.Bitmap
			offset := 0
			for _, width := range widths {
				payload := array.NewSlice(full, int64(prefix+offset), int64(prefix+offset+width))
				defer payload.Release()
				bitmap := make(memory.Bitmap, (width+63)/64)
				for i := 0; i < width; i++ {
					if (offset+i)%3 != 0 {
						bitmap[i/64] |= uint64(1) << uint(i%64)
					}
				}
				b := memory.NewBatch()
				b.Rows = width
				b.Opaque["opaque"] = &memory.OpaqueVector{Rows: width, Type: memory.TypeArrow, Validity: bitmap, Value: payload}
				batches = append(batches, b)
				snapshots = append(snapshots, append(memory.Bitmap(nil), bitmap...))
				offset += width
			}
			df := nullCountOpaqueFrame(memory.TypeArrow, batches...)
			defer df.Close()
			nullCountCheck(t, df, "opaque", rows, 43)
			oracle := func(offset, length int) int64 {
				var want int64
				for i := offset; i < offset+length; i++ {
					if i%3 == 0 {
						want++
					}
				}
				return want
			}
			for start := 0; start <= rows+1; start++ {
				for _, length := range []int{0, 1, 3, 63, 64, 65, 129, 130} {
					v, err := df.Slice(int64(start), int64(length))
					if err != nil {
						t.Fatal(err)
					}
					actualStart := min(start, rows)
					actualLength := min(length, rows-actualStart)
					nullCountCheck(t, v, "opaque", int64(actualLength), oracle(actualStart, actualLength))
				}
			}
			for _, n := range []int{-1, 0, 1, 3, 63, 64, 65, 129, 130} {
				length := min(max(n, 0), rows)
				nullCountCheck(t, df.Head(int64(n)), "opaque", int64(length), oracle(0, length))
				nullCountCheck(t, df.Tail(int64(n)), "opaque", int64(length), oracle(rows-length, length))
			}
			for i, b := range df.batches() {
				if b != batches[i] || b.Rows != widths[i] || !reflect.DeepEqual(b.Opaque["opaque"].Validity, snapshots[i]) {
					t.Fatal("NullCount changed batch boundaries or bitmap storage")
				}
				// Nil Go validity remains all valid even with nullable Arrow.
				b.Opaque["opaque"].Validity = nil
			}
			nullCountCheck(t, df, "opaque", rows, 0)
		})
	}
}

func TestSeriesNullCountOpaqueArrowAllValidGoAllNull(t *testing.T) {
	builder := array.NewLargeStringBuilder(arrowmem.DefaultAllocator)
	builder.AppendValues([]string{"one", "two", "three"}, nil)
	payload := builder.NewArray()
	builder.Release()
	defer payload.Release()
	b := memory.NewBatch()
	b.Rows = 3
	b.Opaque["opaque"] = &memory.OpaqueVector{Rows: 3, Type: memory.TypeArrow, Validity: memory.Bitmap{0}, Value: payload}
	df := nullCountOpaqueFrame(memory.TypeArrow, b)
	defer df.Close()
	nullCountCheck(t, df, "opaque", 3, 3)
}

func TestSeriesNullCountOpaqueScanArrowOwnedAfterClose(t *testing.T) {
	for _, mode := range []string{"valid", "null", "mixed"} {
		t.Run(mode, func(t *testing.T) {
			alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
			defer alloc.AssertSize(t, 0)
			arrowSchema := arrow.NewSchema([]arrow.Field{
				{Name: "opaque", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
				{Name: "decimal", Type: &arrow.Decimal128Type{Precision: 18, Scale: 2}, Nullable: true},
			}, nil)
			builder := array.NewRecordBuilder(alloc, arrowSchema)
			list := builder.Field(0).(*array.ListBuilder)
			child := list.ValueBuilder().(*array.Int32Builder)
			decimal := builder.Field(1).(*array.Decimal128Builder)
			for i := 0; i < 132; i++ {
				valid := mode == "valid" || mode == "mixed" && (i-3)%3 != 0
				list.Append(valid)
				if valid {
					// Child nullness must not be counted as a null parent.
					child.AppendNull()
					decimal.Append(decimal128.FromI64(int64(i)))
				} else {
					decimal.AppendNull()
				}
			}
			whole := builder.NewRecordBatch()
			builder.Release()
			first, second := whole.NewSlice(3, 36), whole.NewSlice(36, 132)
			whole.Release()
			opts := engine.DefaultOptions()
			opts.Pipeline = pipeline.Options{Workers: 1, TargetMorselBytes: 4096, MaxMorselBytes: 16384, MaxInFlightBytes: 32768, MinMorselRows: 1, MaxMorselRows: 17}
			session := NewSession(opts)
			defer session.Close()
			query, err := session.ScanArrow(first, second)
			first.Release()
			second.Release()
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := query.Prepare(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			df, err := prepared.Execute(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer df.Close()
			if err := prepared.Close(); err != nil {
				t.Fatal(err)
			}
			// Session.Close also closes the ScanArrow source. Only the owned
			// DataFrame keeps the original Arrow buffers alive now.
			if err := session.Close(); err != nil {
				t.Fatal(err)
			}
			var want int64
			if mode == "null" {
				want = 129
			} else if mode == "mixed" {
				want = 43
			}
			for _, name := range []string{"opaque", "decimal"} {
				nullCountCheck(t, df, name, 129, want)
				nullCountCheck(t, df.Head(3), name, 3, want/43)
				nullCountCheck(t, df.Tail(3), name, 3, want/43)
				view, err := df.Slice(32, 3)
				if err != nil {
					t.Fatal(err)
				}
				nullCountCheck(t, view, name, 3, want/43)
			}
			field, _ := df.Schema().Field("decimal")
			if field.Type != memory.TypeDecimal || field.DecimalPrecision != 0 {
				t.Fatalf("not a legacy opaque decimal: %+v", field)
			}
			if len(df.batches()) <= 2 {
				t.Fatal("fixture did not exercise multiple collected chunks")
			}
			for _, b := range df.batches() {
				for _, name := range []string{"opaque", "decimal"} {
					v := b.Opaque[name]
					if v == nil || !v.Owned {
						t.Fatalf("%s is not owned opaque storage", name)
					}
					if payload := v.Value.(arrow.Array); payload.Len() != b.Rows {
						t.Fatal("owned Arrow payload lost its row range")
					}
				}
			}
		})
	}
}

func TestSeriesNullCountOpaqueNativePrecedenceAndParentMasks(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 3
	b.Int64s["native"] = &memory.Int64Vector{Values: []int64{1, 2, 3}, Validity: memory.Bitmap{5}}
	b.Opaque["native"] = &memory.OpaqueVector{Rows: 3, Type: memory.TypeArrow, Validity: memory.Bitmap{0}, Value: &nullCountUnreadableArrow{}}
	b.StringLists["list"] = &memory.StringListVector{Offsets: []uint32{0, 1, 2, 3}, Validity: memory.Bitmap{5}, Values: &memory.StringViewVector{Offsets: []uint32{0, 0, 0, 0}, Validity: memory.Bitmap{0}}}
	child := memory.NewBatch()
	child.Rows = 3
	child.Int64s["child"] = &memory.Int64Vector{Values: []int64{1, 2, 3}, Validity: memory.Bitmap{0}}
	childField := memory.Field{Name: "child", Type: memory.TypeInt64, Nullable: true}
	b.Structs["struct"] = &memory.StructVector{Rows: 3, Fields: []memory.Field{childField}, Values: child, Validity: memory.Bitmap{3}}
	df := newDataFrame(memory.NewSchema(memory.Field{Name: "native", Type: memory.TypeInt64, Nullable: true}, memory.ListStringField("list", true, true), memory.StructField("struct", true, childField)))
	df.state.batches, df.state.rows = []*memory.Batch{b}, 3
	defer df.Close()
	for _, name := range []string{"native", "list", "struct"} {
		nullCountCheck(t, df, name, 3, 1)
	}
}

func TestSeriesNullCountOpaqueClosedAndMissingColumn(t *testing.T) {
	if got := (Series{}).NullCount(); got != 0 {
		t.Fatalf("zero Series=%d", got)
	}
	b := memory.NewBatch()
	b.Rows = 3
	b.Opaque["opaque"] = &memory.OpaqueVector{Rows: 3, Type: memory.TypeArrow, Validity: memory.Bitmap{5}, Value: &nullCountUnreadableArrow{}}
	df := nullCountOpaqueFrame(memory.TypeArrow, b)
	defer df.Close()
	series, err := df.Column("opaque")
	if err != nil {
		t.Fatal(err)
	}
	view := df.Head(2)
	viewSeries, err := view.Column("opaque")
	if err != nil {
		t.Fatal(err)
	}
	for _, owner := range []interface{ Column(string) (Series, error) }{df, view} {
		missing, err := owner.Column("missing")
		if err == nil || missing.NullCount() != 0 {
			t.Fatalf("missing column=(%v,%d)", err, missing.NullCount())
		}
	}
	if err := df.Close(); err != nil {
		t.Fatal(err)
	}
	if err := df.Close(); err != nil {
		t.Fatal(err)
	}
	for _, s := range []Series{series, viewSeries} {
		if s.Len() != 0 || s.NullCount() != 0 {
			t.Fatal("closed Series remained usable")
		}
	}
	for _, owner := range []interface{ Column(string) (Series, error) }{df, view} {
		_, err := owner.Column("opaque")
		if !errors.Is(err, ErrDataFrameClosed) {
			t.Fatalf("closed Column=%v", err)
		}
	}
}

func nullCountOpaqueFrame(typ memory.DataType, batches ...*memory.Batch) DataFrame {
	df := newDataFrame(memory.NewSchema(memory.Field{Name: "opaque", Type: typ, Nullable: true}))
	df.state.batches = batches
	for _, b := range batches {
		df.state.rows += int64(b.Rows)
	}
	return df
}

func nullCountCheck(t *testing.T, owner interface{ Column(string) (Series, error) }, name string, rows, nulls int64) {
	t.Helper()
	s, err := owner.Column(name)
	if err != nil {
		t.Fatal(err)
	}
	if s.Len() != rows || s.NullCount() != nulls {
		t.Fatalf("column %s: rows=%d nulls=%d, want rows=%d nulls=%d", name, s.Len(), s.NullCount(), rows, nulls)
	}
}
