package arrowrecord

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
	"math"
	"strings"
	"testing"
)

func timeUnitsArray(alloc arrowmem.Allocator, dt arrow.DataType, values []int64, valid []bool) arrow.Array {
	if typ, ok := dt.(*arrow.Time32Type); ok {
		b := array.NewTime32Builder(alloc, typ)
		defer b.Release()
		v := make([]arrow.Time32, len(values))
		for i, x := range values {
			v[i] = arrow.Time32(x)
		}
		b.AppendValues(v, valid)
		return b.NewArray()
	}
	b := array.NewTime64Builder(alloc, dt.(*arrow.Time64Type))
	defer b.Release()
	v := make([]arrow.Time64, len(values))
	for i, x := range values {
		v[i] = arrow.Time64(x)
	}
	b.AppendValues(v, valid)
	return b.NewArray()
}
func TestArrowTimeUnitNormalization(t *testing.T) {
	for _, tc := range []struct {
		dt   arrow.DataType
		want int64
	}{{arrow.FixedWidthTypes.Time32s, 2000}, {arrow.FixedWidthTypes.Time64us, 2000}} {
		t.Run(tc.dt.String(), func(t *testing.T) {
			alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
			defer alloc.AssertSize(t, 0)
			a := timeUnitsArray(alloc, tc.dt, []int64{2}, nil)
			rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "time", Type: tc.dt}}, nil), []arrow.Array{a}, 1)
			a.Release()
			src, err := NewSource(rec)
			rec.Release()
			if err != nil {
				t.Fatal(err)
			}
			defer src.Close()
			stream, err := src.NewBatchStream(context.Background(), testReadOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			batch := memory.NewBatch()
			if ok, err := stream.Next(context.Background(), batch); !ok || err != nil {
				t.Fatal(ok, err)
			}
			var got int64
			if tc.dt.ID() == arrow.TIME32 {
				got = int64(batch.Int32s["time"].Values[0])
			} else {
				got = batch.Int64s["time"].Values[0]
			}
			if got != tc.want {
				t.Fatalf("raw 2 %s converted=%d want canonical=%d", tc.dt, got, tc.want)
			}
		})
	}
}

func TestArrowTimeUnitLogicalDomain(t *testing.T) {
	for _, dt := range []arrow.DataType{arrow.FixedWidthTypes.Time32s, arrow.FixedWidthTypes.Time32ms, arrow.FixedWidthTypes.Time64us, arrow.FixedWidthTypes.Time64ns} {
		scale, max, min := int64(1), int64(math.MaxInt64), int64(math.MinInt64)
		if dt.ID() == arrow.TIME32 {
			max, min = math.MaxInt32, math.MinInt32
		}
		if dt == arrow.FixedWidthTypes.Time32s || dt == arrow.FixedWidthTypes.Time64us {
			scale = 1000
		}
		cases := []struct {
			name          string
			xs            []int64
			valid, parent []bool
			rows          int
			bad           bool
		}{
			{"signed_extremes", []int64{min / scale, -7, 0, 7, max / scale}, nil, nil, 5, false},
			{"child_null_extreme", []int64{max, 2}, []bool{false, true}, nil, 2, false},
			{"parent_null_extreme", []int64{min, 2}, nil, []bool{false, true}, 2, false},
			{"logical_tail", []int64{2, max}, nil, nil, 1, false},
			{"zero_rows", []int64{max}, nil, nil, 0, false},
		}
		if scale > 1 {
			cases = append(cases, struct {
				name          string
				xs            []int64
				valid, parent []bool
				rows          int
				bad           bool
			}{"positive_overflow", []int64{max/scale + 1}, nil, nil, 1, true}, struct {
				name          string
				xs            []int64
				valid, parent []bool
				rows          int
				bad           bool
			}{"negative_overflow", []int64{min/scale - 1}, nil, nil, 1, true})
		}
		for _, nested := range []bool{false, true} {
			for _, offset := range []int{0, 65} {
				for _, tc := range cases {
					if tc.parent != nil && !nested {
						continue
					}
					t.Run(fmt.Sprintf("%s/struct%v/offset%d/%s", dt, nested, offset, tc.name), func(t *testing.T) {
						alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
						defer alloc.AssertSize(t, 0)
						xs := make([]int64, offset)
						valid := make([]bool, offset)
						parents := make([]bool, offset)
						for i := range xs {
							xs[i] = max
							valid[i] = true
							parents[i] = true
						}
						for i, x := range tc.xs {
							xs = append(xs, x)
							valid = append(valid, tc.valid == nil || tc.valid[i])
							parents = append(parents, tc.parent == nil || tc.parent[i])
						}
						a := timeUnitsArray(alloc, dt, xs, valid)
						if nested {
							st := temporalDomainStruct(t, a, parents)
							a.Release()
							a = st
						}
						sliced := array.NewSlice(a, int64(offset), int64(len(xs)))
						a.Release()
						rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "time", Type: sliced.DataType(), Nullable: true}}, nil), []arrow.Array{sliced}, int64(tc.rows))
						sliced.Release()
						src, err := NewSource(rec)
						rec.Release()
						if tc.bad {
							if src != nil {
								src.Close()
							}
							if err == nil || !strings.Contains(err.Error(), "overflow") || !strings.Contains(err.Error(), "row 0") {
								t.Fatalf("constructor overflow expected: %v", err)
							}
							return
						}
						if err != nil {
							t.Fatal(err)
						}
						defer src.Close()
						schema := src.Schema()
						f := schema.Fields[0]
						if nested {
							f = f.Children[0]
						}
						expected := memory.TypeTime64
						canonical := arrow.FixedWidthTypes.Time64ns.String()
						if dt.ID() == arrow.TIME32 {
							expected = memory.TypeTime32
							canonical = arrow.FixedWidthTypes.Time32ms.String()
						}
						if f.Type != expected || f.ArrowType != dt.String() {
							t.Fatalf("source metadata %v", f)
						}
						outSchema, err := memory.ArrowSchema(memory.NewSchema(f))
						if err != nil || outSchema.Field(0).Type.String() != canonical {
							t.Fatalf("canonical output metadata %v %v", outSchema, err)
						}
						opts := testReadOptions()
						opts.BatchRows = 1
						stream, err := src.NewBatchStream(context.Background(), opts)
						if err != nil {
							t.Fatal(err)
						}
						defer stream.Close()
						src.Close()
						b := memory.NewBatch()
						for row := 0; row < tc.rows; row++ {
							ok, err := stream.Next(context.Background(), b)
							if !ok || err != nil || b.Rows != 1 {
								t.Fatal(ok, err, b.Rows)
							}
							vals := b
							name := "time"
							if nested {
								st := b.Structs[name]
								if st.IsValid(0) != (tc.parent == nil || tc.parent[row]) {
									t.Fatal("parent validity")
								}
								vals = st.Values
								name = "value"
							}
							var got int64
							var isValid bool
							if dt.ID() == arrow.TIME32 {
								v := vals.Int32s[name]
								got = int64(v.Values[0])
								isValid = v.IsValid(0)
							} else {
								v := vals.Int64s[name]
								got = v.Values[0]
								isValid = v.IsValid(0)
							}
							wantValid := tc.valid == nil || tc.valid[row]
							if isValid != wantValid {
								t.Fatal("child validity")
							}
							if wantValid && (tc.parent == nil || tc.parent[row]) && got != tc.xs[row]*scale {
								t.Fatalf("row%d got%d want%d", row, got, tc.xs[row]*scale)
							}
						}
						if tc.rows == 0 {
							if ok, err := stream.Next(context.Background(), b); err != nil || !ok || b.Rows != 0 {
								t.Fatalf("zero record batch: %v %v rows%d", ok, err, b.Rows)
							}
						}
						if ok, err := stream.Next(context.Background(), b); ok || err != nil {
							t.Fatal("EOF", ok, err)
						}
					})
				}
			}
		}
	}
}

func TestArrowTimeUnitStreamCancel(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	a := timeUnitsArray(alloc, arrow.FixedWidthTypes.Time64us, []int64{1, 2}, nil)
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "t", Type: a.DataType()}}, nil), []arrow.Array{a}, 2)
	a.Release()
	src, err := NewSource(rec)
	rec.Release()
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	opts := testReadOptions()
	opts.BatchRows = 1
	stream, err := src.NewBatchStream(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	src.Close()
	b := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), b); !ok || err != nil || b.Int64s["t"].Values[0] != 1000 {
		t.Fatal(ok, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if ok, err := stream.Next(ctx, b); ok || !errors.Is(err, context.Canceled) || b.Rows != 0 {
		t.Fatal(ok, err, b.Rows)
	}
}

func TestArrowTimeUnitRequiredAndMetadata(t *testing.T) {
	for _, dt := range []arrow.DataType{arrow.FixedWidthTypes.Time32s, arrow.FixedWidthTypes.Time64us} {
		for _, parentValid := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/parent%v", dt, parentValid), func(t *testing.T) {
				alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
				defer alloc.AssertSize(t, 0)
				a := timeUnitsArray(alloc, dt, []int64{0}, []bool{false})
				bitmap := arrowmem.NewBufferBytes([]byte{0})
				nulls := 1
				if parentValid {
					bitmap.Release()
					bitmap = arrowmem.NewBufferBytes([]byte{1})
					nulls = 0
				}
				st, err := array.NewStructArrayWithFieldsAndNulls([]arrow.Array{a}, []arrow.Field{{Name: "required", Type: dt}}, bitmap, nulls, 0)
				bitmap.Release()
				a.Release()
				if err != nil {
					t.Fatal(err)
				}
				rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "s", Type: st.DataType(), Nullable: true}}, nil), []arrow.Array{st}, 1)
				st.Release()
				src, err := NewSource(rec)
				rec.Release()
				if src != nil {
					src.Close()
				}
				if parentValid {
					if err == nil || !strings.Contains(err.Error(), "null") && !strings.Contains(err.Error(), "NULL") {
						t.Fatalf("visible required NULL not rejected: %v", err)
					}
				} else if err != nil {
					t.Fatalf("parent NULL must hide required child NULL: %v", err)
				}
			})
		}
	}
	for _, dt := range []arrow.DataType{&arrow.Time32Type{Unit: arrow.Microsecond}, &arrow.Time64Type{Unit: arrow.Millisecond}} {
		for _, rows := range []int64{0, 1} {
			t.Run(fmt.Sprintf("invalid%s/rows%d", dt, rows), func(t *testing.T) {
				alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
				defer alloc.AssertSize(t, 0)
				a := timeUnitsArray(alloc, dt, []int64{0}, []bool{false})
				rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "t", Type: dt, Nullable: true}}, nil), []arrow.Array{a}, rows)
				a.Release()
				src, err := NewSource(rec)
				rec.Release()
				if src != nil {
					src.Close()
				}
				if err == nil || !strings.Contains(err.Error(), "unit") {
					t.Fatalf("invalid unit accepted: %v", err)
				}
			})
		}
	}
}

func TestArrowTimeUnitDictionaryStaysOpaque(t *testing.T) {
	for _, dt := range []arrow.DataType{arrow.FixedWidthTypes.Time32s, arrow.FixedWidthTypes.Time64us} {
		t.Run(dt.String(), func(t *testing.T) {
			alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
			defer alloc.AssertSize(t, 0)
			vals := timeUnitsArray(alloc, dt, []int64{2, -3}, nil)
			ib := array.NewInt8Builder(alloc)
			ib.AppendValues([]int8{1, 0}, nil)
			indices := ib.NewArray()
			ib.Release()
			dtype := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: dt}
			dict := array.NewDictionaryArray(dtype, indices, vals)
			indices.Release()
			vals.Release()
			rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "d", Type: dtype}}, nil), []arrow.Array{dict}, 2)
			dict.Release()
			src, err := NewSource(rec)
			rec.Release()
			if err != nil {
				t.Fatal(err)
			}
			defer src.Close()
			if src.Schema().Fields[0].Type != memory.TypeArrow {
				t.Fatal("dictionary unexpectedly native")
			}
			stream, err := src.NewBatchStream(context.Background(), testReadOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			b := memory.NewBatch()
			if ok, err := stream.Next(context.Background(), b); !ok || err != nil {
				t.Fatal(ok, err)
			}
			d := b.Opaque["d"].Value.(*array.Dictionary)
			if d.GetValueIndex(0) != 1 || d.Dictionary().DataType() != dt {
				t.Fatal("opaque dictionary changed")
			}
			if dt.ID() == arrow.TIME32 {
				if d.Dictionary().(*array.Time32).Value(1) != -3 {
					t.Fatal("opaque raw value changed")
				}
			} else if d.Dictionary().(*array.Time64).Value(1) != -3 {
				t.Fatal("opaque raw value changed")
			}
		})
	}
}

func TestArrowTimeUnitBudget(t *testing.T) {
	for _, limit := range []int64{1, 1 << 20} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
			defer alloc.AssertSize(t, 0)
			xs := make([]int64, 65)
			valid := make([]bool, 65)
			for i := range xs {
				xs[i] = int64(i)
				valid[i] = i%2 == 0
			}
			a := timeUnitsArray(alloc, arrow.FixedWidthTypes.Time64us, xs, valid)
			rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "t", Type: a.DataType(), Nullable: true}}, nil), []arrow.Array{a}, 65)
			a.Release()
			src, err := NewSource(rec)
			rec.Release()
			if err != nil {
				t.Fatal(err)
			}
			defer src.Close()
			m := memory.NewBudgetManager(limit)
			opts := testReadOptions()
			opts.MemoryManager = m
			opts.BatchRows = 1
			stream, err := src.NewBatchStream(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			b := memory.NewBatch()
			if ok, err := stream.Next(context.Background(), b); !ok || err != nil || b.Int64s["t"].Values[0] != 0 {
				t.Fatal("aligned first row", ok, err)
			}
			ok, err := stream.Next(context.Background(), b)
			if limit == 1 {
				if ok || !errors.Is(err, memory.ErrBudgetExceeded) {
					t.Fatalf("budget expected: %v %v", ok, err)
				}
			} else {
				if !ok || err != nil || b.Int64s["t"].IsValid(0) {
					t.Fatal(ok, err)
				}
				if m.Used() == 0 {
					t.Fatal("nullable view not charged")
				}
			}
			stream.Close()
			if m.Used() != 0 {
				t.Fatalf("budget leak %d", m.Used())
			}
		})
	}
}
