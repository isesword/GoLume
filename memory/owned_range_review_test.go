package memory

import (
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

// An Opaque column must not route a mixed batch around native clone admission.
// The Opaque contribution intentionally keeps the legacy logical-size contract.
func TestOwnedReviewMixedOpaqueKeepsNativeAdmission(t *testing.T) {
	const rows = 257
	offsets := make([]uint32, rows+1)
	for i := range offsets {
		offsets[i] = uint32(i)
	}
	child := NewBatch()
	child.Rows = rows
	child.StringLists["list"] = &StringListVector{Offsets: offsets, Values: &StringViewVector{Offsets: offsets, Data: make([]byte, rows)}}
	src := NewBatch()
	src.Rows = rows
	src.Structs["s"] = &StructVector{Rows: rows, Fields: []Field{ListStringField("list", true, true)}, Values: child}
	native, err := EstimateNativeOwnedBatchRangeBytes(src, 0, rows)
	if err != nil {
		t.Fatal(err)
	}
	nativeLogical, err := EstimateBatchRangeBytes(src, 0, rows)
	if err != nil {
		t.Fatal(err)
	}
	clone, err := CloneBatchRange(src, 0, rows)
	if err != nil {
		t.Fatal(err)
	}
	defer clone.ReleaseOwned()
	s := clone.Structs["s"]
	l := s.Values.StringLists["list"]
	backing := int64(cap(l.Offsets)*4+cap(l.Values.Offsets)*4+cap(l.Values.Data)) + int64(cap(s.Fields))*int64(unsafe.Sizeof(Field{}))
	if native.Bytes <= backing || native.Bytes <= nativeLogical {
		t.Fatalf("native admission=%d logical=%d retained=%d", native.Bytes, nativeLogical, backing)
	}
	builder := extensions.NewBool8Builder(arrowmem.DefaultAllocator)
	for i := 0; i < rows; i++ {
		builder.Append(true)
	}
	a := builder.NewArray()
	builder.Release()
	defer a.Release()
	src.Opaque["extension"] = &OpaqueVector{Rows: rows, Value: a}
	mixed, err := EstimateNativeOwnedBatchRangeBytes(src, 0, rows)
	if err != nil {
		t.Fatal(err)
	}
	mixedLogical, err := EstimateBatchRangeBytes(src, 0, rows)
	if err != nil {
		t.Fatal(err)
	}
	if !mixed.OpaquePresent || mixed.NativeBytes < native.NativeBytes || mixed.Bytes != mixed.NativeBytes+mixed.LegacyOpaqueBytes {
		t.Fatal("mixed component accounting is inconsistent")
	}
	if mixed.Bytes-mixedLogical < native.Bytes-nativeLogical {
		t.Fatalf("Opaque removed native allowance: native=%d/%d mixed=%d/%d", native.Bytes, nativeLogical, mixed.Bytes, mixedLogical)
	}
	c, err := CloneBatchRange(src, 0, rows)
	if err != nil {
		t.Fatal(err)
	}
	defer c.ReleaseOwned()
	if c.Opaque["extension"].Value.(arrow.Array).ValueStr(256) != "true" {
		t.Fatal("extension changed")
	}
}

// Empty Struct cloning historically carries undeclared runtime Opaque columns;
// native metadata validation must not introduce an Arrow implementation gate.
func TestOwnedReviewEmptyStructPreservesRuntimeExtension(t *testing.T) {
	allocator := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer allocator.AssertSize(t, 0)
	builder := extensions.NewBool8Builder(allocator)
	a := builder.NewArray()
	builder.Release()
	defer a.Release()
	child := NewBatch()
	child.Opaque["extra"] = &OpaqueVector{Value: a}
	src := NewBatch()
	src.Structs["s"] = &StructVector{Values: child}
	if _, err := EstimateNativeOwnedBatchRangeBytes(src, 0, 0); err != nil {
		t.Fatal(err)
	}
	clone, err := CloneBatchRange(src, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer clone.ReleaseOwned()
	got := clone.Structs["s"].Values.Opaque["extra"].Value.(arrow.Array)
	if got.Len() != 0 || got.DataType() != a.DataType() {
		t.Fatal("zero-row Struct extension semantics changed")
	}
}

func TestOwnedReviewEmptySchemaContainerAdmission(t *testing.T) {
	for _, rows := range []int{0, 1, 257} {
		src := NewBatch()
		src.Rows = rows
		estimate, err := EstimateNativeOwnedBatchRangeBytes(src, 0, rows)
		if err != nil {
			t.Fatal(err)
		}
		if estimate.OpaquePresent || estimate.LegacyOpaqueBytes != 0 || estimate.NativeBytes < 4096 || estimate.Bytes != estimate.NativeBytes {
			t.Fatalf("rows=%d empty native container was exempted: %+v", rows, estimate)
		}
		clone, err := CloneBatchRange(src, 0, rows)
		if err != nil {
			t.Fatal(err)
		}
		if clone.Int8s == nil || clone.Opaque == nil || clone.Rows != rows {
			t.Fatal("clone lacks expected native maps")
		}
		clone.ReleaseOwned()
	}
	src := NewBatch()
	src.Structs["empty"] = &StructVector{Values: NewBatch()}
	estimate, err := EstimateNativeOwnedBatchRangeBytes(src, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if estimate.NativeBytes < 8192 {
		t.Fatalf("zero-row Struct omitted parent or child container: %+v", estimate)
	}
}

// Children metadata is shallow-shared by the existing clone, not recursively
// copied. Deep and cyclic caller schema graphs must not introduce admission-only
// rejection; the native guarantee concerns the copied Fields slice itself.
func TestOwnedReviewSharedSchemaCompatibility(t *testing.T) {
	for _, cycle := range []bool{false, true} {
		fields := []Field{{Name: "v", Type: TypeInt8}}
		if cycle {
			fields[0].Children = fields
		} else {
			cursor := &fields[0]
			for i := 0; i < 128; i++ {
				cursor.Children = []Field{{Name: "metadata", Type: TypeInt8}}
				cursor = &cursor.Children[0]
			}
		}
		values := NewBatch()
		values.Rows = 1
		values.Int8s["v"] = &Int8Vector{Values: []int8{7}}
		src := NewBatch()
		src.Rows = 1
		src.Structs["s"] = &StructVector{Rows: 1, Fields: fields, Values: values}
		estimate, err := EstimateNativeOwnedBatchRangeBytes(src, 0, 1)
		if err != nil {
			t.Fatalf("cycle=%v: %v", cycle, err)
		}
		if estimate.NativeBytes < 8192 {
			t.Fatal("native metadata omitted")
		}
		clone, err := CloneBatchRange(src, 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		if clone.Structs["s"].Values.Int8s["v"].Values[0] != 7 {
			t.Fatal("clone value changed")
		}
		clone.ReleaseOwned()
	}
}

func TestOwnedReviewDeepEmptyRuntimeStructCompatibility(t *testing.T) {
	const depth = 128
	src := NewBatch()
	for i := 0; i < depth; i++ {
		parent := NewBatch()
		parent.Structs["s"] = &StructVector{Values: src}
		src = parent
	}
	estimate, err := EstimateNativeOwnedBatchRangeBytes(src, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if estimate.NativeBytes < int64((depth+1)*4096) {
		t.Fatalf("runtime containers omitted: %+v", estimate)
	}
	clone, err := CloneBatchRange(src, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer clone.ReleaseOwned()
	cursor := clone
	for i := 0; i < depth; i++ {
		s := cursor.Structs["s"]
		if s == nil {
			t.Fatalf("runtime depth %d lost", i)
		}
		cursor = s.Values
	}
}

// The legacy Struct append visits every declared field occurrence. Repeated
// names append repeatedly into one runtime vector and must be charged as such,
// not rejected or estimated solely from the unique source runtime map entry.
func TestOwnedReviewDuplicateFieldsChargeActualAppend(t *testing.T) {
	const rows, repeats = 1024, 128
	child := NewBatch()
	child.Rows = rows
	child.Int8s["v"] = &Int8Vector{Values: make([]int8, rows)}
	fields := make([]Field, repeats)
	for i := range fields {
		fields[i] = Field{Name: "v", Type: TypeInt8}
	}
	src := NewBatch()
	src.Rows = rows
	src.Structs["s"] = &StructVector{Rows: rows, Fields: fields, Values: child}
	clone, err := CloneBatchRange(src, 0, rows)
	if err != nil {
		t.Fatal(err)
	}
	defer clone.ReleaseOwned()
	out := clone.Structs["s"]
	if len(out.Values.Int8s["v"].Values) != rows*repeats {
		t.Fatal("fixture no longer appends duplicate fields")
	}
	estimate, err := EstimateNativeOwnedBatchRangeBytes(src, 0, rows)
	if err != nil {
		t.Fatal(err)
	}
	retained := int64(cap(out.Values.Int8s["v"].Values)) + int64(cap(out.Fields))*int64(unsafe.Sizeof(Field{}))
	if estimate.NativeBytes < retained {
		t.Fatalf("duplicate field append undercounted: charge=%d retained=%d", estimate.NativeBytes, retained)
	}
}

// This is a correctness assertion, not a timing benchmark. Fixtures and the
// warm-up call are outside measurement; preflight must not allocate per-probe
// maps/slices for supported native ranges, duplicates or deep runtime trees.
func TestOwnedReviewNativePreflightZeroAllocations(t *testing.T) {
	scalar := NewBatch()
	scalar.Rows = 1
	scalar.Int8s["x"] = &Int8Vector{Values: []int8{1}}
	child := NewBatch()
	child.Rows = 3
	child.Int8s["v"] = &Int8Vector{Values: []int8{1, 2, 3}}
	duplicate := NewBatch()
	duplicate.Rows = 3
	duplicate.Structs["s"] = &StructVector{Rows: 3, Fields: []Field{{Name: "v", Type: TypeInt8}, {Name: "v", Type: TypeInt8}}, Values: child}
	deep := NewBatch()
	for i := 0; i < 128; i++ {
		p := NewBatch()
		p.Structs["s"] = &StructVector{Values: deep}
		deep = p
	}
	declared := NewBatch()
	declared.Rows = 1
	declared.Int8s["v"] = &Int8Vector{Values: []int8{7}}
	for i := 0; i < 128; i++ {
		p := NewBatch()
		p.Rows = 1
		p.Int8s["v"] = &Int8Vector{Values: []int8{7}}
		p.Structs["s"] = &StructVector{Rows: 1, Fields: []Field{{Name: "v", Type: TypeInt8}}, Values: declared}
		declared = p
	}
	for name, src := range map[string]*Batch{"scalar": scalar, "duplicate": duplicate, "deep_empty": deep, "deep_with_declared_fields": declared} {
		t.Run(name, func(t *testing.T) {
			allocations := testing.AllocsPerRun(10, func() {
				if _, err := EstimateNativeOwnedBatchRangeBytes(src, 0, src.Rows); err != nil {
					t.Fatal(err)
				}
			})
			if allocations != 0 {
				t.Fatalf("preflight heap allocations per probe=%g; want 0", allocations)
			}
		})
	}
}

type reviewCountingArray struct {
	arrow.Array
	calls *int
}

func (a *reviewCountingArray) Data() arrow.ArrayData { *a.calls++; return a.Array.Data() }

// The owned logical walk accumulates Opaque exactly once per occurrence, and
// includes inherited NULL bitmap words in its legacy component. Native charges
// must remain identical to the same Struct without the Opaque runtime extra.
func TestOwnedReviewSingleWalkOpaqueComponents(t *testing.T) {
	const rows = 65
	builder := extensions.NewBool8Builder(arrowmem.DefaultAllocator)
	for i := 0; i < rows; i++ {
		builder.Append(true)
	}
	a := builder.NewArray()
	builder.Release()
	defer a.Release()
	calls := 0
	wrapped := &reviewCountingArray{Array: a, calls: &calls}
	child := NewBatch()
	child.Rows = rows
	child.Int8s["v"] = &Int8Vector{Values: make([]int8, rows)}
	src := NewBatch()
	src.Rows = rows
	src.Structs["s"] = &StructVector{Rows: rows, Fields: []Field{{Name: "v", Type: TypeInt8}}, Values: child, Validity: Bitmap{0, 0}}
	nativeOnly, err := EstimateNativeOwnedBatchRangeBytes(src, 0, rows)
	if err != nil {
		t.Fatal(err)
	}
	perOpaque := int64(a.Data().SizeInBytes()) + int64(WordsFor(rows))*8
	for count := 1; count <= 2; count++ {
		key := "x"
		if count == 2 {
			key = "y"
		}
		child.Opaque[key] = &OpaqueVector{Rows: rows, Value: wrapped}
		calls = 0
		got, err := EstimateNativeOwnedBatchRangeBytes(src, 0, rows)
		if err != nil {
			t.Fatal(err)
		}
		if calls != count {
			t.Fatalf("Opaque Data calls=%d; want one per %d runtime occurrences", calls, count)
		}
		if got.NativeBytes != nativeOnly.NativeBytes || got.LegacyOpaqueBytes != int64(count)*perOpaque || got.Bytes != got.NativeBytes+got.LegacyOpaqueBytes || !got.OpaquePresent {
			t.Fatalf("inherited NULL/component mismatch: %+v native=%+v opaqueEach=%d", got, nativeOnly, perOpaque)
		}
	}
}

// Independent retained-capacity oracle for pointer-free root bulk copies. The
// source deliberately has a nonzero row offset; every destination backing is
// inspected directly rather than comparing two estimator implementations.
func TestOwnedReviewBulkBackingCapacityBound(t *testing.T) {
	for _, rows := range []int{0, 1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257, 511, 512, 513, 1023, 1024, 1025, 2047, 2048, 2049, 4095, 4096, 4097, 8191, 8192, 8193, 16383, 16384, 16385, 32767, 32768, 32769} {
		n := rows + 2
		valid := make(Bitmap, WordsFor(n), WordsFor(n)+17)
		for i := range valid {
			valid[i] = ^uint64(0)
		}
		offsets := make([]uint32, n+1)
		for i := range offsets {
			offsets[i] = uint32(i)
		}
		src := NewBatch()
		src.Rows = n
		src.Int8s["i8"] = &Int8Vector{Values: make([]int8, n), Validity: valid}
		src.Int16s["i16"] = &Int16Vector{Values: make([]int16, n), Validity: valid}
		src.Int32s["i32"] = &Int32Vector{Values: make([]int32, n), Validity: valid}
		src.Int64s["i64"] = &Int64Vector{Values: make([]int64, n), Validity: valid}
		src.Uint8s["u8"] = &Uint8Vector{Values: make([]uint8, n), Validity: valid}
		src.Uint16s["u16"] = &Uint16Vector{Values: make([]uint16, n), Validity: valid}
		src.Uint32s["u32"] = &Uint32Vector{Values: make([]uint32, n), Validity: valid}
		src.Uint64s["u64"] = &Uint64Vector{Values: make([]uint64, n), Validity: valid}
		src.Float32s["f32"] = &Float32Vector{Values: make([]float32, n), Validity: valid}
		src.Float64s["f64"] = &Float64Vector{Values: make([]float64, n), Validity: valid}
		src.Decimals["decimal"] = &Decimal128Vector{Values: make([]Decimal128, n), Validity: valid, Precision: 38}
		src.Strings["string"] = &StringViewVector{Offsets: offsets, Data: make([]byte, n), Validity: valid}
		src.DictStrings["dict"] = &DictionaryStringVector{Indices: make([]uint32, n), Offsets: offsets, Data: make([]byte, n), Validity: valid}
		src.StringLists["list"] = &StringListVector{Offsets: offsets, Validity: valid, Values: &StringViewVector{Offsets: offsets, Data: make([]byte, n), Validity: valid}}
		clone, err := CloneBatchRange(src, 1, rows)
		if err != nil {
			t.Fatal(err)
		}
		retained, logical := int64(0), int64(0)
		backing := func(name string, length, capacity, width int) {
			used, held := int64(length)*int64(width), int64(capacity)*int64(width)
			// ceil(5*used/4)+64 without multiplying used before division.
			bound := used + used/4 + 64
			if used%4 != 0 {
				bound++
			}
			if held > bound {
				t.Fatalf("rows=%d backing=%s logical=%d actual capacity bytes=%d exceeds independent bulk bound=%d", rows, name, used, held, bound)
			}
			logical += used
			retained += held
		}
		bitmap := func(name string, v Bitmap) { backing(name, len(v), cap(v), 8) }
		for name, v := range clone.Int8s {
			backing(name, len(v.Values), cap(v.Values), 1)
			bitmap(name, v.Validity)
		}
		for name, v := range clone.Int16s {
			backing(name, len(v.Values), cap(v.Values), 2)
			bitmap(name, v.Validity)
		}
		for name, v := range clone.Int32s {
			backing(name, len(v.Values), cap(v.Values), 4)
			bitmap(name, v.Validity)
		}
		for name, v := range clone.Int64s {
			backing(name, len(v.Values), cap(v.Values), 8)
			bitmap(name, v.Validity)
		}
		for name, v := range clone.Uint8s {
			backing(name, len(v.Values), cap(v.Values), 1)
			bitmap(name, v.Validity)
		}
		for name, v := range clone.Uint16s {
			backing(name, len(v.Values), cap(v.Values), 2)
			bitmap(name, v.Validity)
		}
		for name, v := range clone.Uint32s {
			backing(name, len(v.Values), cap(v.Values), 4)
			bitmap(name, v.Validity)
		}
		for name, v := range clone.Uint64s {
			backing(name, len(v.Values), cap(v.Values), 8)
			bitmap(name, v.Validity)
		}
		for name, v := range clone.Float32s {
			backing(name, len(v.Values), cap(v.Values), 4)
			bitmap(name, v.Validity)
		}
		for name, v := range clone.Float64s {
			backing(name, len(v.Values), cap(v.Values), 8)
			bitmap(name, v.Validity)
		}
		for name, v := range clone.Decimals {
			backing(name, len(v.Values), cap(v.Values), 16)
			bitmap(name, v.Validity)
		}
		for name, v := range clone.Strings {
			backing(name, len(v.Offsets), cap(v.Offsets), 4)
			backing(name, len(v.Data), cap(v.Data), 1)
			bitmap(name, v.Validity)
		}
		for name, v := range clone.DictStrings {
			backing(name, len(v.Indices), cap(v.Indices), 4)
			backing(name, len(v.Offsets), cap(v.Offsets), 4)
			backing(name, len(v.Data), cap(v.Data), 1)
			bitmap(name, v.Validity)
		}
		for name, v := range clone.StringLists {
			backing(name, len(v.Offsets), cap(v.Offsets), 4)
			bitmap(name, v.Validity)
			backing(name, len(v.Values.Offsets), cap(v.Values.Offsets), 4)
			backing(name, len(v.Values.Data), cap(v.Values.Data), 1)
			bitmap(name, v.Values.Validity)
		}
		estimate, err := EstimateNativeOwnedBatchRangeBytes(src, 1, rows)
		if err != nil {
			t.Fatal(err)
		}
		if estimate.NativeBytes < retained {
			t.Fatalf("rows=%d aggregate admission=%d below actual backing capacities=%d", rows, estimate.NativeBytes, retained)
		}
		if rows >= 32767 && estimate.NativeBytes > 2*logical {
			t.Fatalf("root bulk still charged over twice payload: rows=%d charge=%d logical=%d", rows, estimate.NativeBytes, logical)
		}
		clone.ReleaseOwned()
	}
}

func TestOwnedReviewZeroRowStructUsesBulkDictionaryCopy(t *testing.T) {
	const bytes = 32769
	child := NewBatch()
	child.DictStrings["d"] = &DictionaryStringVector{Offsets: []uint32{0, bytes}, Data: make([]byte, bytes)}
	src := NewBatch()
	src.Structs["s"] = &StructVector{Fields: []Field{{Name: "d", Type: TypeString}}, Values: child}
	clone, err := CloneBatchRange(src, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer clone.ReleaseOwned()
	out := clone.Structs["s"]
	retained := int64(cap(out.Values.DictStrings["d"].Data)) + int64(cap(out.Values.DictStrings["d"].Offsets))*4 + int64(cap(out.Fields))*int64(unsafe.Sizeof(Field{}))
	estimate, err := EstimateNativeOwnedBatchRangeBytes(src, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if estimate.NativeBytes < retained {
		t.Fatalf("zero-row bulk retained=%d charge=%d", retained, estimate.NativeBytes)
	}
	if estimate.NativeBytes > 2*bytes {
		t.Fatalf("zero-row recursive dictionary incorrectly kept row-append multiplier: %d", estimate.NativeBytes)
	}
}
