package physical

import (
	"context"
	"errors"
	"math"
	"reflect"
	"sort"
	"testing"

	"gocolumn/memory"
	"gocolumn/scheduler"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

func TestBoundSortComparatorAllTypes(t *testing.T) {
	validity := memory.EnsureBitmap(nil, 7)
	for _, row := range []int{0, 1, 2, 3, 5, 6} {
		memory.Set(validity, row)
	}
	batch := memory.NewBatch()
	batch.Rows = 7
	batch.Int8s["Int8"] = &memory.Int8Vector{Values: []int8{127, -128, 1, 0, 0, 1, -1}, Validity: validity}
	batch.Int16s["Int16"] = &memory.Int16Vector{Values: []int16{32767, -32768, 1, 0, 0, 1, -1}, Validity: validity}
	batch.Int32s["Int32"] = &memory.Int32Vector{Values: []int32{math.MaxInt32, math.MinInt32, 1, 0, 0, 1, -1}, Validity: validity}
	batch.Int64s["Int64"] = &memory.Int64Vector{Values: []int64{math.MaxInt64, math.MinInt64, 1<<53 + 1, 1 << 53, 0, 1<<53 + 1, -1}, Validity: validity}
	batch.Uint8s["Uint8"] = &memory.Uint8Vector{Values: []uint8{255, 0, 1, 2, 0, 1, 254}, Validity: validity}
	batch.Uint16s["Uint16"] = &memory.Uint16Vector{Values: []uint16{65535, 0, 1, 2, 0, 1, 65534}, Validity: validity}
	batch.Uint32s["Uint32"] = &memory.Uint32Vector{Values: []uint32{math.MaxUint32, 0, 1, 2, 0, 1, math.MaxUint32 - 1}, Validity: validity}
	batch.Uint64s["Uint64"] = &memory.Uint64Vector{Values: []uint64{math.MaxUint64, 0, 1<<53 + 1, 1 << 53, 0, 1<<53 + 1, math.MaxUint64 - 1}, Validity: validity}
	batch.Float32s["Float32"] = &memory.Float32Vector{Values: []float32{float32(math.NaN()), float32(math.Inf(-1)), 1, 0, 0, 1, float32(math.Inf(1))}, Validity: validity}
	batch.Float64s["Float64"] = &memory.Float64Vector{Values: []float64{math.NaN(), math.Inf(-1), 1, math.Copysign(0, -1), 0, 1, math.Inf(1)}, Validity: validity}
	batch.Uint32s["Bool"] = &memory.Uint32Vector{Values: []uint32{1, 0, 1, 0, 1, 1, 0}, Validity: validity}
	batch.Strings["String"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 1, 3, 4, 4, 6, 7}, Data: []byte{'z', 'a', 0, 'b', 'a', 0, 255}, Validity: validity}
	batch.DictStrings["Dictionary"] = &memory.DictionaryStringVector{Indices: []uint32{0, 1, 2, 3, 99, 5, 4}, Offsets: []uint32{0, 1, 1, 3, 4, 5, 7}, Data: []byte{'z', 'a', 0, 'b', 255, 'a', 0}, Validity: validity, DictionaryDomain: "nonlexical"}
	fixtures := []struct {
		name     string
		typ      memory.DataType
		encoding memory.Encoding
	}{
		{"Int8", memory.TypeInt8, memory.EncodingPlain},
		{"Int16", memory.TypeInt16, memory.EncodingPlain},
		{"Int32", memory.TypeInt32, memory.EncodingPlain},
		{"Int64", memory.TypeInt64, memory.EncodingPlain},
		{"Uint8", memory.TypeUint8, memory.EncodingPlain},
		{"Uint16", memory.TypeUint16, memory.EncodingPlain},
		{"Uint32", memory.TypeUint32, memory.EncodingPlain},
		{"Uint64", memory.TypeUint64, memory.EncodingPlain},
		{"Float32", memory.TypeFloat32, memory.EncodingPlain},
		{"Float64", memory.TypeFloat64, memory.EncodingPlain},
		{"String", memory.TypeString, memory.EncodingPlain},
		{"Int32", memory.TypeDate32, memory.EncodingPlain},
		{"Int32", memory.TypeTime32, memory.EncodingPlain},
		{"Int64", memory.TypeDate64, memory.EncodingPlain},
		{"Int64", memory.TypeTime64, memory.EncodingPlain},
		{"Int64", memory.TypeTimestamp, memory.EncodingPlain},
		{"Int64", memory.TypeDuration, memory.EncodingPlain},
		{"Bool", memory.TypeBool, memory.EncodingPlain},
		{"String", memory.TypeBinary, memory.EncodingPlain},
		{"Dictionary", memory.TypeString, memory.EncodingDictionary},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.typ.String()+"/"+fixture.name, func(t *testing.T) {
			schema := memory.NewSchema(memory.Field{Name: fixture.name, Type: fixture.typ, Encoding: fixture.encoding, Nullable: true})
			for _, descending := range []bool{false, true} {
				for _, nullsLast := range []bool{false, true} {
					key := SortKey{Column: fixture.name, Descending: descending, NullsLast: nullsLast}
					bound, err := newBatchComparator(batch, schema, []SortKey{key})
					if err != nil {
						t.Fatal(err)
					}
					for a := 0; a < batch.Rows; a++ {
						for b := 0; b < batch.Rows; b++ {
							want := compareBatchRows(batch, schema, key, a, b)
							got := bound.keys[0].compareRows(a, b)
							if got != want {
								t.Fatalf("direction=%v nullsLast=%v rows=(%d,%d) got=%d want=%d", descending, nullsLast, a, b, got, want)
							}
						}
					}
				}
			}
		})
	}
}

func TestBoundSortComparatorFloatKnownOrder(t *testing.T) {
	batch := memory.NewBatch()
	batch.Rows = 9
	validity := memory.EnsureBitmap(nil, batch.Rows)
	for row := range batch.Rows {
		if row != 7 {
			memory.Set(validity, row)
		}
	}
	batch.Float64s["v"] = &memory.Float64Vector{Values: []float64{math.NaN(), 1, math.Inf(-1), math.Copysign(0, -1), 0, math.Inf(1), math.NaN(), 99, -1}, Validity: validity}
	schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat64, Nullable: true})
	for _, fixture := range []struct {
		descending, nullsLast bool
		want                  []int
	}{
		{false, false, []int{7, 2, 8, 3, 4, 1, 5, 0, 6}},
		{false, true, []int{2, 8, 3, 4, 1, 5, 0, 6, 7}},
		{true, false, []int{7, 0, 6, 5, 1, 3, 4, 8, 2}},
		{true, true, []int{0, 6, 5, 1, 3, 4, 8, 2, 7}},
	} {
		cmp, err := newBatchComparator(batch, schema, []SortKey{{Column: "v", Descending: fixture.descending, NullsLast: fixture.nullsLast}})
		if err != nil {
			t.Fatal(err)
		}
		pool := scheduler.NewPool(3)
		for _, workers := range []int{1, 3} {
			got, err := sortRows(context.Background(), batch.Rows, cmp, workers, pool)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, fixture.want) {
				t.Fatalf("workers=%d got=%v want=%v", workers, got, fixture.want)
			}
		}
		_ = pool.Close()
	}
}

func TestBoundSortComparatorBorrowsOnlySelectedVectors(t *testing.T) {
	batch := memory.NewBatch()
	batch.Rows = 3
	batch.Int64s["v"] = &memory.Int64Vector{Values: []int64{2, 0, 1}}
	schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64})
	cmp, err := newBatchComparator(batch, schema, []SortKey{{Column: "v"}})
	if err != nil {
		t.Fatal(err)
	}
	// Rebinding a batch map does not alter an already bound comparator. It
	// retains neither the map nor unrelated payload columns.
	batch.Int64s["v"] = &memory.Int64Vector{Values: []int64{0, 1, 2}}
	rows, err := sortRows(context.Background(), 3, cmp, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rows, []int{1, 2, 0}) {
		t.Fatalf("bound rows=%v", rows)
	}
	// A new binding sees the new source batch, not a cached previous vector.
	fresh, err := newBatchComparator(batch, schema, []SortKey{{Column: "v"}})
	if err != nil {
		t.Fatal(err)
	}
	if !fresh.less(0, 1) {
		t.Fatal("binding reused stale vector")
	}
}

func TestBoundSortComparatorMultipleKeysAndBitmapBoundary(t *testing.T) {
	const n = 131
	batch := memory.NewBatch()
	batch.Rows = n
	a, b := make([]int16, n), make([]uint64, n)
	valid := memory.EnsureBitmap(nil, n)
	for row := range n {
		a[row] = int16(row % 5)
		b[row] = uint64(row % 7)
		if row%4 != 0 {
			memory.Set(valid, row)
		}
	}
	batch.Int16s["a"] = &memory.Int16Vector{Values: a, Validity: valid}
	batch.Uint64s["b"] = &memory.Uint64Vector{Values: b}
	keys := []SortKey{{Column: "a", Descending: true, NullsLast: true}, {Column: "b"}, {Column: "a"}}
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt16, Nullable: true}, memory.Field{Name: "b", Type: memory.TypeUint64})
	cmp, err := newBatchComparator(batch, schema, keys)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]int, n)
	for row := range want {
		want[row] = row
	}
	sort.SliceStable(want, func(i, j int) bool {
		x, y := want[i], want[j]
		xNull, yNull := x%4 == 0, y%4 == 0
		if xNull != yNull {
			return !xNull
		}
		if !xNull && x%5 != y%5 {
			return x%5 > y%5
		}
		return x%7 < y%7
	})
	pool := scheduler.NewPool(4)
	defer pool.Close()
	for _, workers := range []int{1, 4} {
		got, err := sortRows(context.Background(), n, cmp, workers, pool)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("workers=%d got=%v want=%v", workers, got, want)
		}
	}
}

func TestBoundSortComparatorBudgetAndEmpty(t *testing.T) {
	for _, rows := range []int{0, 1} {
		batch := memory.NewBatch()
		batch.Rows = rows
		cmp, err := newBatchComparator(batch, memory.Schema{}, []SortKey{{Column: "missing"}})
		if err != nil || len(cmp.keys) != 0 {
			t.Fatalf("rows=%d cmp=%v err=%v", rows, cmp, err)
		}
	}
	tiny := memory.NewBudgetManager(1)
	for _, rows := range []int{0, 1} {
		reservation, err := reserveSortBinding(context.Background(), tiny, rows, 1000)
		if err != nil || reservation != nil || tiny.Used() != 0 {
			t.Fatalf("rows=%d retained binding without comparisons", rows)
		}
	}
	if reservation, err := reserveSortBinding(context.Background(), tiny, 1000, 0); err != nil || reservation != nil {
		t.Fatalf("no-key binding reservation=%v err=%v", reservation, err)
	}
	batch := memory.NewBatch()
	batch.Rows = 2
	batch.Int64s["v"] = &memory.Int64Vector{Values: []int64{1, 0}}
	schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64})
	if _, err := newBatchComparator(batch, schema, []SortKey{{Column: "missing"}}); err == nil {
		t.Fatal("missing column accepted")
	}
	keys := make([]SortKey, 100)
	for i := range keys {
		keys[i] = SortKey{Column: "v"}
	}
	input, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	bindingBytes, err := sortBindingBytes(len(keys))
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(estimateOrderBytes(schema, 2) + bindingBytes - 1)
	ordered, err := NewSortSource(input, keys, manager)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := source.OpenBatchStream(context.Background(), ordered, testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Next(context.Background(), memory.NewBatch())
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("budget error=%v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("leaked binding reservation=%d", manager.Used())
	}
}
