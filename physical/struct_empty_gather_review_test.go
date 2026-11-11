package physical

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"unsafe"

	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
)

func reviewEmptyStructFixture(dictionary, precedence bool) (*memory.Batch, memory.Schema) {
	children := memory.NewBatch()
	children.Rows = 1
	children.Int8s["n"] = &memory.Int8Vector{Values: []int8{7}}
	children.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 1}, Data: []byte("x")}
	children.StringLists["l"] = &memory.StringListVector{Offsets: []uint32{0, 0}, Values: &memory.StringViewVector{}}
	children.Int64s["undeclared"] = &memory.Int64Vector{Values: []int64{99}}
	if dictionary {
		children.DictStrings["s"] = &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{0, 3}, Data: []byte("abc"), DictionaryDomain: "review-domain", Encoding: memory.EncodingDictionary}
		if !precedence {
			delete(children.Strings, "s")
		}
	}
	f := memory.StructField("record", true, memory.Field{Name: "n", Type: memory.TypeInt8, Encoding: memory.EncodingPlain}, memory.Field{Name: "s", Type: memory.TypeString, Encoding: memory.EncodingView}, memory.ListStringField("l", true, true))
	b := memory.NewBatch()
	b.Rows = 1
	b.Structs["record"] = &memory.StructVector{Rows: 1, Fields: f.Children, Values: children}
	return b, memory.NewSchema(f)
}

func TestReviewEmptyStructGatherConsumers(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		for _, rows := range [][]int{nil, {}} {
			src, schema := reviewEmptyStructFixture(false, false)
			dst := memory.NewBatch()
			var err error
			if bounded {
				err = takeJoinRowsBounded(context.Background(), dst, src, rows)
			} else {
				err = takeRows(context.Background(), dst, src, rows)
			}
			if err != nil {
				t.Fatal(err)
			}
			v := dst.Structs["record"]
			if err = v.Validate(schema.Fields[0], 0); err != nil {
				t.Fatalf("bounded=%v: %v", bounded, err)
			}
			if dst.Rows != 0 || v.Values.Rows != 0 || len(v.Values.Int64s) != 0 {
				t.Fatal("row count or undeclared child")
			}
			if !reflect.DeepEqual(v.Values.Strings["s"].Offsets, []uint32{0}) || !reflect.DeepEqual(v.Values.StringLists["l"].Offsets, []uint32{0}) || !reflect.DeepEqual(v.Values.StringLists["l"].Values.Offsets, []uint32{0}) {
				t.Fatal("missing canonical sentinel")
			}
			manager := memory.NewBudgetManager(0)
			encoded, err := memory.EncodeColumnarRun(context.Background(), schema, dst, manager)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := memory.DecodeColumnarRun(context.Background(), schema, &memory.RestoredSpill{Object: memory.SpillObject{Codec: memory.ColumnarRunCodec, Payload: encoded.Payload}}, manager)
			if err != nil {
				encoded.Close()
				t.Fatal(err)
			}
			if err = decoded.Batch.Structs["record"].Validate(schema.Fields[0], 0); err != nil {
				t.Fatal(err)
			}
			decoded.Close()
			encoded.Close()
			if manager.Used() != 0 {
				t.Fatal("codec reservation leak")
			}
			alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
			record, err := memory.ArrowRecord(alloc, schema, dst)
			if err != nil {
				t.Fatal(err)
			}
			if record.NumRows() != 0 {
				t.Fatal("Arrow rows")
			}
			record.Release()
			alloc.AssertSize(t, 0)
			dst.ReleaseOwned()
		}
	}
}

func TestReviewEmptyStructGatherRuntimeAndPlan(t *testing.T) {
	for _, dictionary := range []bool{false, true} {
		for _, precedence := range []bool{false, true} {
			src, schema := reviewEmptyStructFixture(dictionary, precedence)
			_, owned, metadata, err := planJoinGather(src, nil)
			if err != nil {
				t.Fatal(err)
			}
			wantOwned := int64(12)
			if dictionary && !precedence {
				wantOwned = 19
			}
			wantMetadata := int64(4096+512+4096+4*512) + 3*int64(unsafe.Sizeof(memory.Field{}))
			if owned != wantOwned || metadata != wantMetadata {
				t.Fatalf("dictionary=%v precedence=%v owned=%d want=%d metadata=%d want=%d", dictionary, precedence, owned, wantOwned, metadata, wantMetadata)
			}
			for _, bounded := range []bool{false, true} {
				dst := memory.NewBatch()
				if bounded {
					err = takeJoinRowsBounded(context.Background(), dst, src, nil)
				} else {
					err = takeRows(context.Background(), dst, src, nil)
				}
				if err != nil {
					t.Fatal(err)
				}
				v := dst.Structs["record"]
				if err = v.Validate(schema.Fields[0], 0); err != nil {
					t.Fatal(err)
				}
				if len(v.Values.Int64s) != 0 {
					t.Fatal("gathered undeclared runtime map")
				}
				if bounded && joinGatherTestOwnedCapacity(dst, false) != owned {
					t.Fatal("capacity differs from plan")
				}
				if dictionary && !precedence {
					d := v.Values.DictStrings["s"]
					original := src.Structs["record"].Values.DictStrings["s"]
					if d == nil || d.DictionaryDomain != "review-domain" || d.Encoding != memory.EncodingDictionary || string(d.Data) != "abc" || !reflect.DeepEqual(d.Offsets, []uint32{0, 3}) {
						t.Fatal("lost dictionary metadata/table")
					}
					if &d.Data[0] == &original.Data[0] || &d.Offsets[0] == &original.Offsets[0] {
						t.Fatal("dictionary table aliases source")
					}
				} else if v.Values.Strings["s"] == nil || v.Values.DictStrings["s"] != nil {
					t.Fatal("Strings precedence changed")
				}
				alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
				record, err := memory.ArrowRecord(alloc, schema, dst)
				if err != nil {
					t.Fatal(err)
				}
				record.Release()
				alloc.AssertSize(t, 0)
				dst.ReleaseOwned()
			}
		}
	}
}

func TestReviewEmptyStructGatherFailureAtomicity(t *testing.T) {
	src, schema := reviewEmptyStructFixture(false, false)
	cost, err := joinGatherAdmissionBytes(schema, src, nil)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := memory.EstimateColumnarRunSelectionBytes(schema, src, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"admission", "encoder", "cancel", "cancel ordinary"} {
		t.Run(mode, func(t *testing.T) {
			dst := memory.NewBatch()
			sentinel := &memory.Int64Vector{Values: []int64{42}}
			dst.Int64s["sentinel"] = sentinel
			dst.Rows = 1
			vectors := memory.NewBudgetManager(cost)
			encoder := memory.NewBudgetManager(encoded)
			if mode == "admission" {
				vectors = memory.NewBudgetManager(cost - 1)
			}
			if mode == "encoder" {
				encoder = memory.NewBudgetManager(encoded - 1)
			}
			ctx := context.Background()
			var result error
			if mode == "cancel" || mode == "cancel ordinary" {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				if mode == "cancel ordinary" {
					result = takeRows(cancelled, dst, src, nil)
				} else {
					result = takeJoinRowsBounded(cancelled, dst, src, nil)
				}
				if !errors.Is(result, context.Canceled) {
					t.Fatalf("cancel: %v", result)
				}
			} else {
				gather, encoding, e := reserveJoinEncoding(ctx, vectors, encoder, cost, encoded)
				result = e
				if gather != nil || encoding != nil {
					if gather != nil {
						gather.Release()
					}
					if encoding != nil {
						encoding.Release()
					}
					t.Fatal("failed admission returned reservations")
				}
				if !errors.Is(result, memory.ErrBudgetExceeded) {
					t.Fatalf("reserve: %v", result)
				}
			}
			if vectors.Used() != 0 || encoder.Used() != 0 || dst.Rows != 1 || len(dst.Int64s) != 1 || dst.Int64s["sentinel"] != sentinel || len(dst.Structs) != 0 {
				t.Fatal("failure leaked or changed destination")
			}
		})
	}
}
