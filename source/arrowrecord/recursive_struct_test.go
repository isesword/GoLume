package arrowrecord

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
	"testing"
)

func recursiveArrowRecord(t *testing.T, alloc arrowmem.Allocator, depth int, dict bool) arrow.RecordBatch {
	t.Helper()
	ib := array.NewInt64Builder(alloc)
	ib.AppendValues([]int64{11, 22, 33, 44}, []bool{true, false, true, true})
	ints := ib.NewArray()
	ib.Release()
	defer ints.Release()
	sb := array.NewStringBuilder(alloc)
	var text arrow.Array
	if dict {
		sb.AppendValues([]string{"a", "b"}, nil)
		values := sb.NewArray()
		defer values.Release()
		indices := array.NewInt8Builder(alloc)
		indices.AppendValues([]int8{0, 0, 1, 0}, []bool{true, false, true, true})
		idx := indices.NewArray()
		indices.Release()
		defer idx.Release()
		text = array.NewDictionaryArray(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}, idx, values)
	} else {
		sb.AppendValues([]string{"a", "hidden", "b", "a"}, []bool{true, false, true, true})
		text = sb.NewArray()
	}
	sb.Release()
	defer text.Release()
	bitmap := arrowmem.NewBufferBytes([]byte{13})
	defer bitmap.Release()
	current, e := array.NewStructArrayWithFieldsAndNulls([]arrow.Array{ints, text}, []arrow.Field{{Name: "n", Type: ints.DataType()}, {Name: "text", Type: text.DataType()}}, bitmap, 1, 0)
	if e != nil {
		t.Fatal(e)
	}
	for i := 1; i < depth; i++ {
		next, e := array.NewStructArrayWithFieldsAndNulls([]arrow.Array{current}, []arrow.Field{{Name: "inner", Type: current.DataType()}}, bitmap, 1, 0)
		current.Release()
		if e != nil {
			t.Fatal(e)
		}
		current = next
	}
	defer current.Release()
	return array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "root", Type: current.DataType(), Nullable: true}}, nil), []arrow.Array{current}, 4)
}
func recursiveArrowLeaf(v *memory.StructVector) *memory.StructVector {
	for len(v.Values.Structs) > 0 {
		v = v.Values.Structs["inner"]
	}
	return v
}
func TestRecursiveStructArrowExplicitSlicedOwnership(t *testing.T) {
	for _, depth := range []int{2, 8} {
		for _, dict := range []bool{false, true} {
			t.Run(fmt.Sprintf("depth%d/dict%v", depth, dict), func(t *testing.T) {
				alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
				defer alloc.AssertSize(t, 0)
				rec := recursiveArrowRecord(t, alloc, depth, dict)
				for _, opts := range []Options{{}, {NativeDecimal128: true}} {
					s, e := NewSourceWithOptions(opts, rec)
					if e == nil {
						s.Close()
						t.Fatal("default recursive admission broadened")
					}
				}
				slice := rec.NewSlice(1, 4)
				rec.Release()
				src, e := NewSourceWithOptions(Options{NativeRecursiveStruct: true}, slice, slice)
				slice.Release()
				if e != nil {
					t.Fatal(e)
				}
				schema := src.Schema()
				field := schema.Fields[0]
				if e = field.ValidateRecursiveStruct(); e != nil {
					t.Fatal(e)
				}
				if field.Name != "root" || !field.Nullable {
					t.Fatal("root metadata")
				}
				opts := testReadOptions()
				opts.BatchRows = 2
				m := memory.NewBudgetManager(0)
				opts.MemoryManager = m
				st, e := src.NewBatchStream(context.Background(), opts)
				if e != nil {
					src.Close()
					t.Fatal(e)
				}
				src.Close()
				cursor := 0
				for {
					b := memory.NewBatch()
					ok, e := st.Next(context.Background(), b)
					if e != nil {
						st.Close()
						t.Fatal(e)
					}
					if !ok {
						break
					}
					v := b.Structs["root"]
					if v == nil || v.Rows != b.Rows {
						t.Fatal("recursive native shape")
					}
					if e = v.ValidateRecursiveWithContext(field, b.Rows, nil); e != nil {
						t.Fatal(e)
					}
					leaf := recursiveArrowLeaf(v)
					for i := 0; i < b.Rows; i++ {
						row := cursor%3 + 1
						valid := row != 1
						if v.IsValid(i) != valid {
							t.Fatal("root NULL")
						}
						if valid {
							want := int64(33)
							text := "b"
							if row == 3 {
								want = 44
								text = "a"
							}
							if leaf.Values.Int64s["n"].Values[i] != want {
								t.Fatal("numeric oracle")
							}
							if s := leaf.Values.Strings["text"]; s != nil {
								if string(s.ValueBytes(i)) != text {
									t.Fatal("String oracle")
								}
							} else {
								d := leaf.Values.DictStrings["text"]
								if d == nil || string(d.DictionaryValue(d.Indices[i])) != text {
									t.Fatal("dictionary oracle")
								}
							}
						}
						cursor++
					}
				}
				st.Close()
				if cursor != 6 || m.Used() != 0 {
					t.Fatal("rows/lifetime budget", cursor, m.Used())
				}
			})
		}
	}
}
func TestRecursiveStructArrowDepthRejection(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	rec := recursiveArrowRecord(t, alloc, 9, false)
	defer rec.Release()
	s, e := NewSourceWithOptions(Options{NativeRecursiveStruct: true}, rec)
	if e == nil {
		s.Close()
		t.Fatal("depth9 admitted")
	}
}

func TestRecursiveStructArrowSchemaSnapshotAndDecimalOptions(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	rec := recursiveArrowRecord(t, alloc, 2, false)
	defer rec.Release()
	drec := decimalRecord(t, alloc, 9, 2, []decimal128.Num{decimal128.FromI64(123), decimal128.FromI64(234), decimal128.FromI64(345), decimal128.FromI64(456)}, nil, true)
	defer drec.Release()
	both := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{rec.Schema().Field(0), drec.Schema().Field(0)}, nil), []arrow.Array{rec.Column(0), drec.Column(0)}, 4)
	defer both.Release()
	for _, nativeDecimal := range []bool{false, true} {
		src, e := NewSourceWithOptions(Options{NativeRecursiveStruct: true, NativeDecimal128: nativeDecimal}, both)
		if e != nil {
			t.Fatal(e)
		}
		snap := src.Schema()
		snap.Fields[0].Children[0].Children[0].Name = "poison"
		if src.Schema().Fields[0].Children[0].Children[0].Name != "n" {
			src.Close()
			t.Fatal("recursive source schema exposed alias")
		}
		opts := testReadOptions()
		st, e := src.NewBatchStream(context.Background(), opts)
		if e != nil {
			src.Close()
			t.Fatal(e)
		}
		b := memory.NewBatch()
		ok, e := st.Next(context.Background(), b)
		if !ok || e != nil {
			t.Fatal(e)
		}
		if nativeDecimal {
			v := b.Decimals["d"]
			if v == nil || v.Precision != 9 || v.Scale != 2 || v.Values[0] != decimal128.FromI64(123) {
				t.Fatal("top Decimal optin interaction")
			}
		} else if b.Opaque["d"] == nil || b.Decimals["d"] != nil {
			t.Fatal("recursive flag changed Decimal default")
		}
		st.Close()
		src.Close()
	}
	inner, e := array.NewStructArrayWithFields([]arrow.Array{drec.Column(0)}, []arrow.Field{drec.Schema().Field(0)})
	if e != nil {
		t.Fatal(e)
	}
	defer inner.Release()
	outer, e := array.NewStructArrayWithFields([]arrow.Array{inner}, []arrow.Field{{Name: "inner", Type: inner.DataType()}})
	if e != nil {
		t.Fatal(e)
	}
	defer outer.Release()
	nested := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "root", Type: outer.DataType()}}, nil), []arrow.Array{outer}, 4)
	defer nested.Release()
	for _, o := range []Options{{NativeRecursiveStruct: true}, {NativeRecursiveStruct: true, NativeDecimal128: true}} {
		src, e := NewSourceWithOptions(o, nested)
		if e == nil {
			src.Close()
			t.Fatal("nested Decimal admitted")
		}
	}
}

func TestRecursiveStructArrowMalformedHiddenStorage(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	t.Run("hidden_offsets", func(t *testing.T) {
		rec := recursiveArrowRecord(t, alloc, 2, false)
		defer rec.Release()
		root := rec.Column(0).(*array.Struct)
		root.NullBitmapBytes()[0] = 0
		leaf := root.Field(0).(*array.Struct)
		text := leaf.Field(1).(*array.String)
		text.ValueOffsets()[1] = 8
		if text.ValueOffsets()[1] <= text.ValueOffsets()[2] {
			t.Fatal("fixture must contain decreasing offsets")
		}
		src, e := NewSourceWithOptions(Options{NativeRecursiveStruct: true}, rec)
		if e == nil {
			defer src.Close()
			st, err := src.NewBatchStream(context.Background(), testReadOptions())
			if err != nil {
				return
			}
			defer st.Close()
			_, err = st.Next(context.Background(), memory.NewBatch())
			if err == nil {
				t.Fatal("NULL ancestor hid malformed String offsets")
			}
		}
	})
	t.Run("missing_child", func(t *testing.T) {
		typ := arrow.StructOf(arrow.Field{Name: "inner", Type: arrow.StructOf(arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int64})})
		data := array.NewData(typ, 1, []*arrowmem.Buffer{nil}, nil, 0, 0)
		a := array.NewStructData(data)
		data.Release()
		defer a.Release()
		rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "root", Type: typ}}, nil), []arrow.Array{a}, 1)
		defer rec.Release()
		src, e := NewSourceWithOptions(Options{NativeRecursiveStruct: true}, rec)
		if e == nil {
			src.Close()
			t.Fatal("missing child accepted")
		}
	})
}
func TestRecursiveStructArrowCancelAndBudgetCleanup(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	rec := recursiveArrowRecord(t, alloc, 8, false)
	src, e := NewSourceWithOptions(Options{NativeRecursiveStruct: true}, rec)
	rec.Release()
	if e != nil {
		t.Fatal(e)
	}
	defer src.Close()
	for _, limit := range []int64{1, 1 << 20} {
		opts := testReadOptions()
		m := memory.NewBudgetManager(limit)
		opts.MemoryManager = m
		st, e := src.NewBatchStream(context.Background(), opts)
		if e != nil {
			if !errors.Is(e, memory.ErrBudgetExceeded) || m.Used() != 0 {
				t.Fatal("constructor cleanup", e)
			}
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		if limit > 1 {
			cancel()
		}
		ok, err := st.Next(ctx, memory.NewBatch())
		cancel()
		st.Close()
		wantErr := error(memory.ErrBudgetExceeded)
		if limit > 1 {
			wantErr = context.Canceled
		}
		if ok || !errors.Is(err, wantErr) || m.Used() != 0 {
			t.Fatal("cancel/budget cleanup", limit, ok, err, m.Used())
		}
	}
}

type recursiveArrowPeakManager struct {
	memory.Manager
	peak int64
}

func (m *recursiveArrowPeakManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	r, e := m.Manager.Reserve(ctx, n)
	if e == nil && m.Used() > m.peak {
		m.peak = m.Used()
	}
	return r, e
}
func TestRecursiveStructArrowDeepViewExactBudget(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	var shallow int64
	for _, depth := range []int{1, 2, 8} {
		rec := recursiveArrowRecord(t, alloc, depth, false)
		slice := rec.NewSlice(2, 3)
		rec.Release()
		src, e := NewSourceWithOptions(Options{NativeRecursiveStruct: true}, slice)
		slice.Release()
		if e != nil {
			t.Fatal(e)
		}
		run := func(limit int64) (int64, error) {
			m := &recursiveArrowPeakManager{Manager: memory.NewBudgetManager(limit)}
			opts := testReadOptions()
			opts.BatchRows = 1
			opts.MemoryManager = m
			st, e := src.NewBatchStream(context.Background(), opts)
			if e != nil {
				return m.peak, e
			}
			ok, e := st.Next(context.Background(), memory.NewBatch())
			st.Close()
			if m.Used() != 0 {
				t.Fatal("view leaked", depth, m.Used())
			}
			if !ok && e == nil {
				t.Fatal("missing row")
			}
			return m.peak, e
		}
		peak, e := run(0)
		if e != nil || (depth > 1 && peak <= 0) {
			src.Close()
			t.Fatal("peak probe", depth, peak, e)
		}
		if depth == 1 {
			shallow = peak
			if peak == 0 {
				src.Close()
				continue
			}
		} else if peak <= shallow {
			t.Fatal("deep nodes not charged", depth, peak, shallow)
		}
		if exact, e := run(peak); e != nil || exact != peak {
			t.Fatal("exact view budget", depth, exact, peak, e)
		}
		if _, e = run(peak - 1); !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal("view -1 budget", depth, peak, e)
		}
		src.Close()
	}
}

func TestRecursiveStructArrowOwnedAfterAllSourcesReleased(t *testing.T) {
	for _, dict := range []bool{false, true} {
		t.Run(fmt.Sprint(dict), func(t *testing.T) {
			alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
			defer alloc.AssertSize(t, 0)
			rec := recursiveArrowRecord(t, alloc, 8, dict)
			src, e := NewSourceWithOptions(Options{NativeRecursiveStruct: true}, rec)
			rec.Release()
			if e != nil {
				t.Fatal(e)
			}
			schema := src.Schema()
			opts := testReadOptions()
			opts.BatchRows = 4
			streamBudget := memory.NewBudgetManager(0)
			opts.MemoryManager = streamBudget
			st, e := src.NewBatchStream(context.Background(), opts)
			if e != nil {
				src.Close()
				t.Fatal(e)
			}
			b := memory.NewBatch()
			ok, e := st.Next(context.Background(), b)
			if !ok || e != nil {
				t.Fatal(e)
			}
			ownedBudget := memory.NewBudgetManager(1 << 20)
			owned, lease, e := memory.CloneCollectBatchRecursive(context.Background(), ownedBudget, b, schema)
			if e != nil {
				st.Close()
				src.Close()
				t.Fatal(e)
			}
			st.Close()
			src.Close()
			alloc.AssertSize(t, 0)
			leaf := recursiveArrowLeaf(owned.Structs["root"])
			if !owned.Structs["root"].IsValid(0) || owned.Structs["root"].IsValid(1) || leaf.Values.Int64s["n"].Values[2] != 33 || string(leaf.Values.Strings["text"].ValueBytes(2)) != "b" {
				t.Fatal("owned reentry oracle")
			}
			if streamBudget.Used() != 0 || ownedBudget.Used() != lease.Bytes() {
				t.Fatal("owned budget/lifetime")
			}
			owned.ReleaseOwned()
			lease.Release()
			if ownedBudget.Used() != 0 {
				t.Fatal("owned final release")
			}
		})
	}
}

func TestRecursiveStructArrowDictionaryDemandedCodes(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	for _, visible := range []bool{false, true} {
		rec := recursiveArrowRecord(t, alloc, 2, true)
		root := rec.Column(0).(*array.Struct)
		inner := root.Field(0).(*array.Struct)
		d := inner.Field(1).(*array.Dictionary)
		idx := d.Indices().(*array.Int8)
		idx.Int8Values()[1] = 99
		idx.NullBitmapBytes()[0] |= 2
		if visible {
			root.NullBitmapBytes()[0] |= 2
			inner.NullBitmapBytes()[0] |= 2
			inner.Field(0).(*array.Int64).NullBitmapBytes()[0] |= 2
		}
		src, e := NewSourceWithOptions(Options{NativeRecursiveStruct: true}, rec)
		rec.Release()
		if visible {
			if e == nil {
				src.Close()
				t.Fatal("visible invalid dictionary code accepted")
			}
		} else {
			if e != nil {
				t.Fatal("hidden dictionary code read", e)
			}
			src.Close()
		}
	}
}

func TestRecursiveStructArrowRejectDictionaryNullTable(t *testing.T) {
	for _, referenced := range []bool{false, true} {
		t.Run(fmt.Sprint(referenced), func(t *testing.T) {
			alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
			defer alloc.AssertSize(t, 0)
			sb := array.NewStringBuilder(alloc)
			sb.AppendValues([]string{"ok", "hidden"}, []bool{true, false})
			table := sb.NewArray()
			sb.Release()
			defer table.Release()
			ib := array.NewInt8Builder(alloc)
			code := int8(0)
			if referenced {
				code = 1
			}
			ib.Append(code)
			indices := ib.NewArray()
			ib.Release()
			defer indices.Release()
			dict := array.NewDictionaryArray(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}, indices, table)
			defer dict.Release()
			inner, e := array.NewStructArrayWithFields([]arrow.Array{dict}, []arrow.Field{{Name: "text", Type: dict.DataType(), Nullable: true}})
			if e != nil {
				t.Fatal(e)
			}
			defer inner.Release()
			outer, e := array.NewStructArrayWithFields([]arrow.Array{inner}, []arrow.Field{{Name: "inner", Type: inner.DataType()}})
			if e != nil {
				t.Fatal(e)
			}
			defer outer.Release()
			rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "root", Type: outer.DataType()}}, nil), []arrow.Array{outer}, 1)
			defer rec.Release()
			if table.NullN() != 1 {
				t.Fatal("fixture lacks table NULL")
			}
			src, e := NewSourceWithOptions(Options{NativeRecursiveStruct: true}, rec)
			if e == nil {
				src.Close()
				t.Fatal("recursive dictionary table NULL accepted", referenced)
			}
		})
	}
}
