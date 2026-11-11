package memory

import (
	"math"
	"reflect"
	"testing"
)

// Capacity is an observable lower bound, not an RSS/allocator-arena measurement.
// Every root backing here follows the actual bulk CloneBatchRange path.
func assertBulkBackingCapacity(t *testing.T, v reflect.Value) int64 {
	t.Helper()
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	var total int64
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if field.Kind() == reflect.Slice {
			bytes := int64(field.Len()) * int64(field.Type().Elem().Size())
			capacity := int64(field.Cap()) * int64(field.Type().Elem().Size())
			charge, err := nativeBulkAdmission(bytes)
			if err != nil {
				t.Fatal(err)
			}
			if capacity > charge+64 {
				t.Fatalf("%s len bytes=%d cap bytes=%d exceeds 5/4+64=%d", v.Type().Field(i).Name, bytes, capacity, charge+64)
			}
			total += capacity
		} else if field.Kind() == reflect.Ptr && field.Type() == reflect.TypeOf((*StringViewVector)(nil)) && !field.IsNil() {
			total += assertBulkBackingCapacity(t, field)
		}
	}
	return total
}

func TestBulkOwnedActualRootCapacityOracle(t *testing.T) {
	for _, rows := range []int{0, 1, 31, 255, 256, 257, 4095, 4096, 4097, 32767, 32768, 32769} {
		for _, nullable := range []bool{false, true} {
			n := rows + 2
			b := NewBatch()
			b.Rows = n
			var validity Bitmap
			if nullable {
				validity = make(Bitmap, WordsFor(n))
				for i := 0; i < n; i++ {
					Set(validity, i)
				}
				validity[0] &^= 2
			}
			b.Int8s["Int8s"] = &Int8Vector{Values: make([]int8, n), Validity: validity}
			b.Int16s["Int16s"] = &Int16Vector{Values: make([]int16, n), Validity: validity}
			b.Int32s["Int32s"] = &Int32Vector{Values: make([]int32, n), Validity: validity}
			b.Int64s["Int64s"] = &Int64Vector{Values: make([]int64, n), Validity: validity}
			b.Uint8s["Uint8s"] = &Uint8Vector{Values: make([]uint8, n), Validity: validity}
			b.Uint16s["Uint16s"] = &Uint16Vector{Values: make([]uint16, n), Validity: validity}
			b.Uint32s["Uint32s"] = &Uint32Vector{Values: make([]uint32, n), Validity: validity}
			b.Uint64s["Uint64s"] = &Uint64Vector{Values: make([]uint64, n), Validity: validity}
			b.Float32s["Float32s"] = &Float32Vector{Values: make([]float32, n), Validity: validity}
			b.Float64s["Float64s"] = &Float64Vector{Values: make([]float64, n), Validity: validity}

			b.Decimals["dec"] = &Decimal128Vector{Values: make([]Decimal128, n), Precision: 10, Validity: validity}
			offsets := make([]uint32, n+1)
			for i := range offsets {
				offsets[i] = uint32(i)
			}
			data := make([]byte, n)
			b.Strings["str"] = &StringViewVector{Offsets: offsets, Data: data, Validity: validity}
			b.DictStrings["dict"] = &DictionaryStringVector{Indices: make([]uint32, n), Offsets: []uint32{0, uint32(n)}, Data: data, Validity: validity}
			b.StringLists["list"] = &StringListVector{Offsets: offsets, Validity: validity, Values: &StringViewVector{Offsets: offsets, Data: data, Validity: validity}}
			estimate, err := EstimateNativeOwnedBatchRangeBytes(b, 1, rows)
			if err != nil {
				t.Fatal(err)
			}
			clone, err := CloneBatchRange(b, 1, rows)
			if err != nil {
				t.Fatal(err)
			}
			var capacity int64
			batch := reflect.ValueOf(clone).Elem()
			for i := 0; i < batch.NumField(); i++ {
				field := batch.Field(i)
				if field.Kind() != reflect.Map {
					continue
				}
				iter := field.MapRange()
				for iter.Next() {
					capacity += assertBulkBackingCapacity(t, iter.Value())
				}
			}
			clone.ReleaseOwned()
			if estimate.NativeBytes < capacity {
				t.Fatalf("rows=%d nullable=%v native=%d actual cap=%d", rows, nullable, estimate.NativeBytes, capacity)
			}
		}
	}
}

func TestBulkOwnedKeepsStructAppendRate(t *testing.T) {
	const rows = 257
	child := NewBatch()
	child.Rows = rows
	child.Int8s["x"] = &Int8Vector{Values: make([]int8, rows)}
	field := Field{Name: "x", Type: TypeInt8}
	b := NewBatch()
	b.Rows = rows
	b.Structs["s"] = &StructVector{Rows: rows, Fields: []Field{field, field}, Values: child, Validity: make(Bitmap, WordsFor(rows))}
	before, err := EstimateNativeOwnedBatchRangeBytes(b, 0, rows)
	if err != nil {
		t.Fatal(err)
	}
	previous, err := previousNativeOwnedFormula(b, 0, rows)
	if err != nil {
		t.Fatal(err)
	}
	if before != previous {
		t.Fatalf("Struct-only row append changed: new=%+v old=%+v", before, previous)
	}
	b.Int8s["bulk"] = &Int8Vector{Values: make([]int8, rows)}
	after, err := EstimateNativeOwnedBatchRangeBytes(b, 0, rows)
	if err != nil {
		t.Fatal(err)
	}
	bulkCharge, err := nativeBulkAdmission(rows)
	if err != nil {
		t.Fatal(err)
	}
	if after.NativeBytes-before.NativeBytes != 1024+int64(len("bulk"))+bulkCharge {
		t.Fatalf("mixed classification delta=%d", after.NativeBytes-before.NativeBytes)
	}
	var opaque, bulk int64
	if _, err := estimateBatchRangeBytesAccounting(b, 0, rows, false, true, &opaque, &bulk, true); err != nil || bulk != rows || opaque != 0 {
		t.Fatalf("mode bulk=%d opaque=%d err=%v", bulk, opaque, err)
	}
}

func TestBulkOwnedZeroStructRecursesBulk(t *testing.T) {
	child := NewBatch()
	child.DictStrings["d"] = &DictionaryStringVector{Offsets: []uint32{0, 32769}, Data: make([]byte, 32769)}
	root := NewBatch()
	root.Structs["s"] = &StructVector{Values: child}
	var opaque, bulk int64
	logical, err := estimateBatchRangeBytesAccounting(root, 0, 0, false, true, &opaque, &bulk, true)
	if err != nil || opaque != 0 || bulk != logical || bulk < 32769 {
		t.Fatalf("zero Struct logical=%d bulk=%d opaque=%d err=%v", logical, bulk, opaque, err)
	}
	clone, err := CloneBatchRange(root, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer clone.ReleaseOwned()
	capacity := assertBulkBackingCapacity(t, reflect.ValueOf(clone.Structs["s"].Values.DictStrings["d"]))
	estimate, err := EstimateNativeOwnedBatchRangeBytes(root, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if estimate.NativeBytes < capacity {
		t.Fatal("zero Struct dictionary cap uncharged")
	}
}

func TestBulkOwnedCheckedRounding(t *testing.T) {
	for _, n := range []int64{0, 1, 2, 3, 4, 31, 32, 33, 255, 256, 257, 32761, 32768, 32769} {
		got, err := nativeBulkAdmission(n)
		if err != nil || got != n+(n+3)/4 {
			t.Fatalf("n=%d got=%d err=%v", n, got, err)
		}
	}
	largest := int64(math.MaxInt64/5)*4 + int64(math.MaxInt64%5)*4/5
	if got, err := nativeBulkAdmission(largest); err != nil || got != math.MaxInt64 {
		t.Fatalf("boundary=%d err=%v", got, err)
	}
	for _, n := range []int64{-1, largest + 1, math.MaxInt64} {
		if _, err := nativeBulkAdmission(n); err == nil {
			t.Fatalf("overflow accepted: %d", n)
		}
	}
}
