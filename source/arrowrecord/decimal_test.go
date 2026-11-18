package arrowrecord

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func decimalRecord(t *testing.T, alloc arrowmem.Allocator, p, s int32, values []decimal128.Num, valid []bool, nullable bool) arrow.RecordBatch {
	t.Helper()
	dt := &arrow.Decimal128Type{Precision: p, Scale: s}
	b := array.NewDecimal128Builder(alloc, dt)
	b.AppendValues(values, valid)
	a := b.NewArray()
	b.Release()
	defer a.Release()
	return array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "d", Type: dt, Nullable: nullable}}, nil), []arrow.Array{a}, int64(len(values)))
}
func TestDecimalArrowLegacyBoundary(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	rec := decimalRecord(t, alloc, 9, 2, []decimal128.Num{decimal128.FromI64(123)}, nil, true)
	defer rec.Release()
	src, err := NewSource(rec)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	f := src.Schema().Fields[0]
	if f.DecimalPrecision != 0 {
		t.Fatal(f)
	}
	_, err = expr.Compile(src.Schema(), expr.CastDecimal128(expr.Col("d"), 9, 2))
	if err == nil {
		t.Fatal("legacy unexpectedly native")
	}
	t.Log("legacy re-entry boundary:", err)
	stream, err := src.NewBatchStream(context.Background(), decimalTestReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	b := memory.NewBatch()
	ok, err := stream.Next(context.Background(), b)
	if !ok || err != nil || b.Opaque["d"] == nil || b.Decimals["d"] != nil {
		t.Fatalf("legacy binding ok=%v err=%v", ok, err)
	}
}

func TestDecimalArrowNativeSlicedLifetime(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
			defer alloc.AssertSize(t, 0)
			vals := make([]decimal128.Num, 133)
			valid := make([]bool, 133)
			for i := range vals {
				vals[i] = decimal128.FromI64(int64(i - 70))
				valid[i] = i%7 != 0
			}
			rec := decimalRecord(t, alloc, 38, 38, vals, valid, true)
			slice := rec.NewSlice(1, 131)
			rec.Release()
			src, err := NewSourceWithOptions(Options{NativeDecimal128: true}, slice, slice)
			slice.Release()
			if err != nil {
				t.Fatal(err)
			}
			f := src.Schema().Fields[0]
			if f.DecimalPrecision != 38 || f.DecimalScale != 38 || !f.Nullable {
				t.Fatal(f)
			}
			opts := decimalTestReadOptions()
			opts.Kernels = kernel.New(mode)
			opts.BatchRows = 63
			manager := memory.NewBudgetManager(0)
			opts.MemoryManager = manager
			st, err := src.NewBatchStream(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}
			src.Close()
			if _, err = src.NewBatchStream(context.Background(), opts); err == nil {
				t.Fatal("opened closed source")
			}
			b := memory.NewBatch()
			n := 0
			for {
				ok, err := st.Next(context.Background(), b)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				v := b.Decimals["d"]
				if v == nil || b.Opaque["d"] != nil || v.Precision != 38 || v.Scale != 38 {
					t.Fatal("wrong native binding")
				}
				if err := v.Validate(b.Rows); err != nil {
					t.Fatal(err)
				}
				for j := 0; j < b.Rows; j++ {
					original := n%130 + 1
					if v.IsValid(j) != valid[original] || v.IsValid(j) && v.Values[j] != vals[original] {
						t.Fatal("value/NULL", n)
					}
					n++
				}
			}
			st.Close()
			st.Close()
			if n != 260 || manager.Used() != 0 {
				t.Fatalf("rows %d budget %d", n, manager.Used())
			}
		})
	}
}
func TestDecimalArrowMetadataAndCompatibility(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	for _, ps := range [][2]int32{{1, 0}, {9, 2}, {38, 0}, {38, 38}} {
		rec := decimalRecord(t, alloc, ps[0], ps[1], nil, nil, true)
		for _, opts := range []Options{{}, {NativeDecimal128: true}} {
			src, err := NewSourceWithOptions(opts, rec)
			if err != nil {
				t.Fatal(err)
			}
			f := src.Schema().Fields[0]
			mapped, err := sourceMemoryField(rec.Schema().Field(0), opts)
			if err != nil || f.DecimalPrecision != mapped.DecimalPrecision || f.DecimalScale != mapped.DecimalScale || f.ArrowType != mapped.ArrowType {
				t.Fatal("schema mapper mismatch")
			}
			if opts.NativeDecimal128 {
				as, err := memory.ArrowSchema(src.Schema())
				if err != nil || !arrow.TypeEqual(as.Field(0).Type, rec.Schema().Field(0).Type) {
					t.Fatal("export schema", err)
				}
			} else if f.DecimalPrecision != 0 {
				t.Fatal("zero options changed default")
			}
			src.Close()
		}
		rec.Release()
	}
	for _, ps := range [][2]int32{{0, 0}, {39, 0}, {9, -1}, {9, 10}} {
		t.Run(fmt.Sprint(ps), func(t *testing.T) {
			rec := decimalRecord(t, alloc, ps[0], ps[1], nil, nil, true)
			defer rec.Release()
			if src, err := NewSourceWithOptions(Options{NativeDecimal128: true}, rec); err == nil {
				src.Close()
				t.Fatal("accepted native metadata")
			} else if !strings.Contains(err.Error(), "precision 1..38") {
				t.Fatal(err)
			}
			legacy, err := NewSource(rec)
			if err != nil {
				t.Fatal("legacy changed", err)
			}
			legacy.Close()
		})
	}
	dt := &arrow.Decimal256Type{Precision: 50, Scale: 4}
	builder := array.NewDecimal256Builder(alloc, dt)
	builder.Append(decimal256.FromI64(42))
	a := builder.NewArray()
	builder.Release()
	r := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "d", Type: dt}}, nil), []arrow.Array{a}, 1)
	a.Release()
	defer r.Release()
	src, err := NewSourceWithOptions(Options{NativeDecimal128: true}, r)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if src.Schema().Fields[0].DecimalPrecision != 0 {
		t.Fatal("Decimal256 narrowed")
	}
	if _, err := expr.Compile(src.Schema(), expr.CastDecimal128(expr.Col("d"), 38, 4)); err == nil {
		t.Fatal("Decimal256 native cast accepted")
	}
	st, _ := src.NewBatchStream(context.Background(), decimalTestReadOptions())
	defer st.Close()
	b := memory.NewBatch()
	ok, err := st.Next(context.Background(), b)
	if !ok || err != nil || b.Opaque["d"] == nil {
		t.Fatal("Decimal256 transport", err)
	}
	nested := arrow.Field{Name: "parent", Type: arrow.StructOf(arrow.Field{Name: "d", Type: &arrow.Decimal128Type{Precision: 9, Scale: 2}})}
	mapped, err := sourceMemoryField(nested, Options{NativeDecimal128: true})
	if err != nil || mapped.Children[0].DecimalPrecision != 0 {
		t.Fatal("nested schema broadened")
	}
}
func TestDecimalArrowValidationAndRollback(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	for _, tc := range []struct {
		name     string
		values   []decimal128.Num
		valid    []bool
		nullable bool
		want     string
	}{
		{"overflow", []decimal128.Num{decimal128.FromI64(1000)}, nil, true, "exceeds precision"},
		{"nonnull", []decimal128.Num{decimal128.FromI64(0)}, []bool{false}, false, "contains NULL"},
		{"null_payload", []decimal128.Num{decimal128.New(-9223372036854775808, 0)}, []bool{false}, true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := decimalRecord(t, alloc, 3, 0, tc.values, tc.valid, tc.nullable)
			defer r.Release()
			src, err := NewSourceWithOptions(Options{NativeDecimal128: true}, r)
			if tc.want != "" {
				if err == nil {
					src.Close()
					t.Fatal("accepted invalid data")
				}
				if !strings.Contains(err.Error(), tc.want) {
					t.Fatal(err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				src.Close()
			}
		})
	}
	good := decimalRecord(t, alloc, 3, 0, []decimal128.Num{decimal128.FromI64(1)}, nil, true)
	defer good.Release()
	bad := decimalRecord(t, alloc, 3, 0, []decimal128.Num{decimal128.FromI64(1000)}, nil, true)
	defer bad.Release()
	if src, err := NewSourceWithOptions(Options{NativeDecimal128: true}, good, bad); err == nil {
		src.Close()
		t.Fatal("later invalid record accepted")
	}
	other := decimalRecord(t, alloc, 4, 0, nil, nil, true)
	defer other.Release()
	if src, err := NewSourceWithOptions(Options{NativeDecimal128: true}, good, other); err == nil {
		src.Close()
		t.Fatal("schema mismatch accepted")
	}
	// A corrupt physical tail must not enter logical record validation.
	tail := decimalRecord(t, alloc, 3, 0, []decimal128.Num{decimal128.FromI64(7), decimal128.FromI64(1000)}, nil, true)
	defer tail.Release()
	prefix := array.NewRecordBatch(tail.Schema(), tail.Columns(), 1)
	defer prefix.Release()
	src, err := NewSourceWithOptions(Options{NativeDecimal128: true}, prefix)
	if err != nil {
		t.Fatal("validated hidden tail", err)
	}
	src.Close()
	// The binding helper also checks actual array type, not only advertised field.
	f, _ := memory.Decimal128Field("d", true, 3, 0)
	if _, err := nativeDecimalVector(other.Column(0), f, nil, 0); err == nil {
		t.Fatal("accepted actual array metadata mismatch")
	}
}
func TestDecimalArrowBudgetCancelConcurrent(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	vals := make([]decimal128.Num, 130)
	valid := make([]bool, 130)
	for i := range valid {
		valid[i] = i%2 == 0
		vals[i] = decimal128.FromI64(int64(i))
	}
	rec := decimalRecord(t, alloc, 9, 0, vals, valid, true)
	src, err := NewSourceWithOptions(Options{NativeDecimal128: true}, rec)
	rec.Release()
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	for _, cancelled := range []bool{false, true} {
		manager := memory.NewBudgetManager(1)
		opts := decimalTestReadOptions()
		opts.BatchRows = 65
		opts.MemoryManager = manager
		st, err := src.NewBatchStream(context.Background(), opts)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		if cancelled {
			cancel()
		}
		_, err = st.Next(ctx, memory.NewBatch())
		if !cancelled && err == nil {
			// The first aligned bitmap slice is borrowed. Offset 65 on the
			// next batch requires admitted normalization storage.
			_, err = st.Next(ctx, memory.NewBatch())
		}
		cancel()
		st.Close()
		want := error(memory.ErrBudgetExceeded)
		if cancelled {
			want = context.Canceled
		}
		if !errors.Is(err, want) {
			t.Fatalf("want%v got%v", want, err)
		}
		st.Close()
		if manager.Used() != 0 {
			t.Fatal("failed stream leaked")
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			manager := memory.NewBudgetManager(0)
			opts := decimalTestReadOptions()
			opts.BatchRows = 1
			opts.MemoryManager = manager
			st, err := src.NewBatchStream(context.Background(), opts)
			if err != nil {
				errs <- err
				return
			}
			b := memory.NewBatch()
			n := 0
			for {
				ok, e := st.Next(context.Background(), b)
				if e != nil {
					err = e
					break
				}
				if !ok {
					break
				}
				v := b.Decimals["d"]
				if v.Precision != 9 || v.Scale != 0 || v.IsValid(0) != (n%2 == 0) || v.IsValid(0) && v.Values[0] != vals[n] {
					err = fmt.Errorf("NULL at row%d", n)
					break
				}
				n++
			}
			st.Close()
			if err == nil && (n != 130 || manager.Used() != 0) {
				err = fmt.Errorf("rows%d budget%d", n, manager.Used())
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestDecimalArrowNilNativeGuards(t *testing.T) {
	f, _ := memory.Decimal128Field("d", true, 9, 2)
	var a *array.Decimal128
	if _, err := nativeDecimalVector(a, f, nil, 0); err == nil {
		t.Fatal("typed nil array accepted")
	}
	var dt *arrow.Decimal128Type
	if _, err := sourceMemoryField(arrow.Field{Name: "d", Type: dt}, Options{NativeDecimal128: true}); err == nil {
		t.Fatal("typed nil type accepted")
	}
}
func TestDecimalArrowExplicitZeroAndDictionary(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	rec := decimalRecord(t, alloc, 9, 2, []decimal128.Num{decimal128.FromI64(123)}, nil, true)
	defer rec.Release()
	legacy, err := NewSource(rec)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	zero, err := NewSourceWithOptions(Options{}, rec)
	if err != nil {
		t.Fatal(err)
	}
	defer zero.Close()
	if !reflect.DeepEqual(legacy.Schema(), zero.Schema()) {
		t.Fatal("zero options schema differs")
	}
	for _, src := range []*Source{legacy, zero} {
		f := src.Schema().Fields[0]
		if f.DecimalPrecision != 0 || f.ArrowType != rec.Column(0).DataType().String() {
			t.Fatal(f)
		}
		st, err := src.NewBatchStream(context.Background(), decimalTestReadOptions())
		if err != nil {
			t.Fatal(err)
		}
		b := memory.NewBatch()
		ok, err := st.Next(context.Background(), b)
		if !ok || err != nil || b.Opaque["d"] == nil || b.Decimals["d"] != nil {
			t.Fatal("zero/default binding", err)
		}
		st.Close()
	}
	ib := array.NewInt8Builder(alloc)
	ib.Append(0)
	indices := ib.NewArray()
	ib.Release()
	defer indices.Release()
	dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: rec.Column(0).DataType()}
	dict := array.NewDictionaryArray(dt, indices, rec.Column(0))
	defer dict.Release()
	dr := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "d", Type: dt}}, nil), []arrow.Array{dict}, 1)
	defer dr.Release()
	src, err := NewSourceWithOptions(Options{NativeDecimal128: true}, dr)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if src.Schema().Fields[0].DecimalPrecision != 0 {
		t.Fatal("dictionary native expansion")
	}
	st, err := src.NewBatchStream(context.Background(), decimalTestReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	b := memory.NewBatch()
	ok, err := st.Next(context.Background(), b)
	if !ok || err != nil || b.Opaque["d"] == nil || b.Decimals["d"] != nil {
		t.Fatal("dictionary transport", err)
	}
}

// A malformed third-party RecordBatch implementation can expose a typed-nil
// array through an interface even though Arrow's standard constructor does not.
type decimalMalformedRecord struct {
	arrow.RecordBatch
	columns []arrow.Array
}

func (r decimalMalformedRecord) Columns() []arrow.Array   { return r.columns }
func (r decimalMalformedRecord) Column(i int) arrow.Array { return r.columns[i] }
func TestDecimalArrowMalformedRecordNilArray(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	rec := decimalRecord(t, alloc, 9, 2, []decimal128.Num{decimal128.FromI64(1)}, nil, true)
	defer rec.Release()
	var a *array.Decimal128
	malformed := decimalMalformedRecord{RecordBatch: rec, columns: []arrow.Array{a}}
	if src, err := NewSourceWithOptions(Options{NativeDecimal128: true}, malformed); err == nil {
		src.Close()
		t.Fatal("typed nil record accepted")
	} else if !strings.Contains(err.Error(), "nil or incompatible") {
		t.Fatal(err)
	}
}

// decimalTestReadOptions is the unchanged fixture from read_options_test.go.
func decimalTestReadOptions() source.ReadOptions {
	return source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar)}
}
