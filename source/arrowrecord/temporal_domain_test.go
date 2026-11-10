package arrowrecord

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/expr"
	"gocolumn/memory"
)

func temporalDomainArray(alloc arrowmem.Allocator, kind string, unit arrow.TimeUnit, zone string, values []int64, valid []bool) arrow.Array {
	if kind == "timestamp" {
		b := array.NewTimestampBuilder(alloc, &arrow.TimestampType{Unit: unit, TimeZone: zone})
		defer b.Release()
		v := make([]arrow.Timestamp, len(values))
		for i := range values {
			v[i] = arrow.Timestamp(values[i])
		}
		b.AppendValues(v, valid)
		return b.NewArray()
	}
	b := array.NewDurationBuilder(alloc, &arrow.DurationType{Unit: unit})
	defer b.Release()
	v := make([]arrow.Duration, len(values))
	for i := range values {
		v[i] = arrow.Duration(values[i])
	}
	b.AppendValues(v, valid)
	return b.NewArray()
}

func temporalDomainStruct(t *testing.T, child arrow.Array, parentValid []bool) arrow.Array {
	t.Helper()
	bits := make([]byte, (child.Len()+7)/8)
	nulls := 0
	for i, valid := range parentValid {
		if valid {
			bits[i/8] |= 1 << uint(i%8)
		} else {
			nulls++
		}
	}
	bitmap := arrowmem.NewBufferBytes(bits)
	defer bitmap.Release()
	a, err := array.NewStructArrayWithFieldsAndNulls([]arrow.Array{child}, []arrow.Field{{Name: "value", Type: child.DataType(), Nullable: true}}, bitmap, nulls, 0)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestTemporalLogicalDomain(t *testing.T) {
	for _, kind := range []string{"timestamp", "duration"} {
		for _, unit := range []arrow.TimeUnit{arrow.Second, arrow.Millisecond, arrow.Microsecond, arrow.Nanosecond} {
			scale, _ := arrowTimeUnitScale(unit, kind)
			cases := []struct {
				name          string
				values        []int64
				valid, parent []bool
				rows          int
				wantError     bool
			}{
				{"child_null", []int64{math.MaxInt64, 2}, []bool{false, true}, []bool{true, true}, 2, false},
				{"parent_null", []int64{math.MinInt64, 2}, []bool{true, true}, []bool{false, true}, 2, false},
				{"logical_tail", []int64{2, math.MaxInt64}, []bool{true, true}, []bool{true, true}, 1, false},
				{"zero_rows", []int64{math.MinInt64}, []bool{true}, []bool{true}, 0, false},
				{"positive_boundary", []int64{math.MaxInt64 / scale}, []bool{true}, []bool{true}, 1, false},
				{"negative_boundary", []int64{math.MinInt64 / scale}, []bool{true}, []bool{true}, 1, false},
			}
			if scale > 1 {
				cases = append(cases,
					struct {
						name          string
						values        []int64
						valid, parent []bool
						rows          int
						wantError     bool
					}{"visible_positive_overflow", []int64{math.MaxInt64/scale + 1}, []bool{true}, []bool{true}, 1, true},
					struct {
						name          string
						values        []int64
						valid, parent []bool
						rows          int
						wantError     bool
					}{"visible_negative_overflow", []int64{math.MinInt64/scale - 1}, []bool{true}, []bool{true}, 1, true})
			}
			for _, nested := range []bool{false, true} {
				for _, offset := range []int{0, 65} {
					for _, tc := range cases {
						if tc.name == "parent_null" && !nested {
							continue
						}
						t.Run(fmt.Sprintf("%s/%s/struct_%t/offset_%d/%s", kind, unit, nested, offset, tc.name), func(t *testing.T) {
							alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
							defer alloc.AssertSize(t, 0)
							values := make([]int64, offset)
							valid := make([]bool, offset)
							parent := make([]bool, offset)
							for i := range values {
								values[i], valid[i], parent[i] = math.MaxInt64, true, true
							}
							values = append(values, tc.values...)
							valid = append(valid, tc.valid...)
							parent = append(parent, tc.parent...)
							arr := temporalDomainArray(alloc, kind, unit, "", values, valid)
							if nested {
								st := temporalDomainStruct(t, arr, parent)
								arr.Release()
								arr = st
							}
							sliced := array.NewSlice(arr, int64(offset), int64(len(values)))
							arr.Release()
							field := arrow.Field{Name: "col", Type: sliced.DataType(), Nullable: true}
							rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{field}, nil), []arrow.Array{sliced}, int64(tc.rows))
							sliced.Release()
							src, err := NewSource(rec)
							rec.Release()
							if tc.wantError {
								if src != nil {
									src.Close()
								}
								if err == nil || !strings.Contains(err.Error(), "overflows") || !strings.Contains(err.Error(), `column "col"`) || !strings.Contains(err.Error(), "row 0") {
									t.Fatalf("expected contextual overflow, got %v", err)
								}
								return
							}
							if err != nil {
								t.Fatal(err)
							}
							defer src.Close()
							reader, err := src.NewBatchStream(context.Background(), testReadOptions())
							if err != nil {
								t.Fatal(err)
							}
							defer reader.Close()
							// The active stream owns the bounded record after both
							// the caller's record and the source have been closed.
							if err := src.Close(); err != nil {
								t.Fatal(err)
							}
							batch := memory.NewBatch()
							ok, err := reader.Next(context.Background(), batch)
							if err != nil {
								t.Fatal(err)
							}
							if tc.rows == 0 {
								if ok && batch.Rows != 0 {
									t.Fatalf("empty record produced %d rows", batch.Rows)
								}
								return
							}
							if !ok || batch.Rows != tc.rows {
								t.Fatalf("ok=%t rows=%d want %d", ok, batch.Rows, tc.rows)
							}
							vec := batch.Int64s["col"]
							if nested {
								st := batch.Structs["col"]
								if st.Len() != tc.rows {
									t.Fatalf("struct rows=%d", st.Len())
								}
								vec = st.Values.Int64s["value"]
								for row := 0; row < tc.rows; row++ {
									if st.IsValid(row) != tc.parent[row] {
										t.Fatalf("parent validity at %d", row)
									}
								}
							}
							for row := 0; row < tc.rows; row++ {
								if vec.IsValid(row) != tc.valid[row] {
									t.Fatalf("child validity at %d", row)
								}
								if tc.valid[row] && (!nested || tc.parent[row]) && vec.Values[row] != tc.values[row]*scale {
									t.Fatalf("row %d = %d want %d", row, vec.Values[row], tc.values[row]*scale)
								}
							}
							if ok, err := reader.Next(context.Background(), batch); ok || err != nil {
								t.Fatalf("end ok=%t err=%v", ok, err)
							}
						})
					}
				}
			}
		}
	}
}

func TestTemporalDomainBoundsMixedStruct(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	st := arrow.StructOf(
		arrow.Field{Name: "time", Type: &arrow.TimestampType{Unit: arrow.Second}},
		arrow.Field{Name: "enabled", Type: arrow.FixedWidthTypes.Boolean},
		arrow.Field{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
	)
	b := array.NewStructBuilder(alloc, st)
	times := b.FieldBuilder(0).(*array.TimestampBuilder)
	bools := b.FieldBuilder(1).(*array.BooleanBuilder)
	lists := b.FieldBuilder(2).(*array.ListBuilder)
	strings := lists.ValueBuilder().(*array.StringBuilder)
	b.Append(true)
	times.Append(2)
	bools.Append(true)
	lists.Append(true)
	strings.Append("kept")
	// Neither the tail's invalid time payload nor its non-nullable child
	// NULLs belong to the one-row record.
	b.Append(true)
	times.Append(arrow.Timestamp(math.MaxInt64))
	bools.AppendNull()
	lists.AppendNull()
	a := b.NewArray()
	b.Release()
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "s", Type: st}}, nil), []arrow.Array{a}, 1)
	a.Release()
	src, err := NewSource(rec)
	rec.Release()
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	r, err := src.NewBatchStream(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	batch := memory.NewBatch()
	if ok, err := r.Next(context.Background(), batch); !ok || err != nil {
		t.Fatalf("next=%t %v", ok, err)
	}
	v := batch.Structs["s"]
	if batch.Rows != 1 || v.Rows != 1 || v.Values.Int64s["time"].Values[0] != 2e9 || v.Values.Uint32s["enabled"].Values[0] != 1 {
		t.Fatalf("invalid bounded struct: %+v", v)
	}
	tags := v.Values.StringLists["tags"]
	if err := tags.Validate(1); err != nil {
		t.Fatal(err)
	}
	lo, hi, valid := tags.ValueRange(0)
	if !valid || hi-lo != 1 || string(tags.Values.ValueBytes(lo)) != "kept" {
		t.Fatalf("tags=%+v", tags)
	}
}

func TestTemporalDomainRecordPrefixStatistics(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	b := array.NewInt64Builder(alloc)
	b.AppendValues([]int64{1, 1_000_000}, nil)
	a := b.NewArray()
	b.Release()
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{a}, 1)
	a.Release()
	src, err := NewSource(rec)
	rec.Release()
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	_, report, err := src.ApplyPushdown(context.Background(), sourceRequest([]string{"n"}, expr.GT(expr.Col("n"), expr.Lit(100))))
	if err != nil {
		t.Fatal(err)
	}
	if report.PrunedPartitions != 1 || report.SelectedRows != 0 {
		t.Fatalf("hidden tail affected statistics: %+v", report)
	}
}

func TestTemporalDomainAlwaysValidatesMetadata(t *testing.T) {
	for _, kind := range []string{"timestamp", "duration"} {
		for _, rows := range []int{0, 1} {
			for _, nested := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/rows_%d/struct_%t", kind, rows, nested), func(t *testing.T) {
					alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
					defer alloc.AssertSize(t, 0)
					arr := temporalDomainArray(alloc, kind, arrow.TimeUnit(4), "", []int64{math.MaxInt64}, []bool{false})
					if nested {
						st := temporalDomainStruct(t, arr, []bool{false})
						arr.Release()
						arr = st
					}
					rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "col", Type: arr.DataType(), Nullable: true}}, nil), []arrow.Array{arr}, int64(rows))
					arr.Release()
					src, err := NewSource(rec)
					rec.Release()
					if src != nil {
						src.Close()
					}
					if err == nil || !strings.Contains(err.Error(), "not supported") {
						t.Fatalf("expected unit rejection, got %v", err)
					}
				})
			}
		}
	}
	for _, rows := range []int{0, 1} {
		t.Run(fmt.Sprintf("timezone/rows_%d", rows), func(t *testing.T) {
			alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
			defer alloc.AssertSize(t, 0)
			arr := temporalDomainArray(alloc, "timestamp", arrow.Second, "Asia/Shanghai", []int64{math.MaxInt64}, []bool{false})
			rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "col", Type: arr.DataType(), Nullable: true}}, nil), []arrow.Array{arr}, int64(rows))
			arr.Release()
			src, err := NewSource(rec)
			rec.Release()
			if src != nil {
				src.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "timezone") {
				t.Fatalf("expected timezone rejection, got %v", err)
			}
		})
	}
}

func TestTemporalDomainBoundedStreamLifetime(t *testing.T) {
	for _, nested := range []bool{false, true} {
		t.Run(fmt.Sprintf("struct_%t", nested), func(t *testing.T) {
			alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
			defer alloc.AssertSize(t, 0)
			valid := []bool{true, false, true, true}
			if nested {
				valid[1] = true
			}
			a := temporalDomainArray(alloc, "duration", arrow.Second, "", []int64{1, math.MaxInt64, 3, math.MaxInt64}, valid)
			if nested {
				st := temporalDomainStruct(t, a, []bool{true, false, true, true})
				a.Release()
				a = st
			}
			rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "col", Type: a.DataType(), Nullable: true}}, nil), []arrow.Array{a}, 3)
			a.Release()
			src, err := NewSource(rec)
			rec.Release()
			if err != nil {
				t.Fatal(err)
			}
			defer src.Close()
			opts := testReadOptions()
			opts.BatchRows = 1
			r, err := src.NewBatchStream(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			if err := src.Close(); err != nil {
				t.Fatal(err)
			}
			batch := memory.NewBatch()
			for row := 0; row < 3; row++ {
				if ok, err := r.Next(context.Background(), batch); !ok || err != nil || batch.Rows != 1 {
					t.Fatalf("row %d: ok=%t rows=%d err=%v", row, ok, batch.Rows, err)
				}
				v := batch.Int64s["col"]
				if nested {
					st := batch.Structs["col"]
					if st.IsValid(0) != (row != 1) {
						t.Fatalf("parent validity at row %d", row)
					}
					v = st.Values.Int64s["value"]
				}
				if !nested && v.IsValid(0) != (row != 1) {
					t.Fatalf("validity at row %d", row)
				}
				if row != 1 && v.Values[0] != int64(row+1)*1e9 {
					t.Fatalf("row %d=%d", row, v.Values[0])
				}
			}
			if ok, err := r.Next(context.Background(), batch); ok || err != nil {
				t.Fatalf("EOF=%t %v", ok, err)
			}
		})
	}
}

func TestTemporalDomainNullParentRejectsTimezone(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	a := temporalDomainArray(alloc, "timestamp", arrow.Second, "Asia/Shanghai", []int64{math.MaxInt64}, []bool{true})
	st := temporalDomainStruct(t, a, []bool{false})
	a.Release()
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "col", Type: st.DataType(), Nullable: true}}, nil), []arrow.Array{st}, 1)
	st.Release()
	src, err := NewSource(rec)
	rec.Release()
	if src != nil {
		src.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "timezone") {
		t.Fatalf("expected timezone rejection, got %v", err)
	}
}
