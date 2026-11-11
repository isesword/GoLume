package memory

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

type nativeOwnedLegacyWrapper struct{ arrow.Array }

func TestNativeOwnedOpaqueRetainsLegacyChargeAndClone(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	numericBuilder := array.NewInt8Builder(alloc)
	numericBuilder.Append(7)
	numeric := numericBuilder.NewArray()
	numericBuilder.Release()
	defer numeric.Release()
	timestampType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	timestampBuilder := array.NewTimestampBuilder(alloc, timestampType)
	timestampBuilder.Append(17)
	timestamp := timestampBuilder.NewArray()
	timestampBuilder.Release()
	defer timestamp.Release()
	extension := array.NewExtensionArrayWithStorage(extensions.NewBool8Type(), numeric)
	defer extension.Release()
	for name, a := range map[string]arrow.Array{"standard": numeric, "wrapper": &nativeOwnedLegacyWrapper{numeric}, "timestamp": timestamp, "extension": extension} {
		t.Run(name, func(t *testing.T) {
			b := NewBatch()
			b.Rows = 1
			b.Opaque["opaque"] = &OpaqueVector{Rows: 1, Value: a, Validity: Bitmap{1}}
			legacy, err := EstimateBatchRangeBytes(b, 0, 1)
			if err != nil {
				t.Fatal(err)
			}
			got, err := EstimateNativeOwnedBatchRangeBytes(b, 0, 1)
			if err != nil {
				t.Fatal(err)
			}
			if got.Bytes != legacy || got.LegacyOpaqueBytes != legacy || got.NativeBytes != 0 || !got.OpaquePresent {
				t.Fatalf("legacy scope changed: %+v want %d", got, legacy)
			}
			clone, err := CloneBatchRange(b, 0, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer clone.ReleaseOwned()
			out := clone.Opaque["opaque"].Value.(arrow.Array)
			if out.Len() != 1 || out.DataType() != a.DataType() {
				t.Fatal("legacy type sharing changed")
			}
			if name == "extension" {
				if _, ok := out.(*extensions.Bool8Array); !ok {
					t.Fatal("extension semantics lost")
				}
			}
		})
	}
}

func TestNativeOwnedMixedSeparatesNativeAndLegacyOpaque(t *testing.T) {
	b := NewBatch()
	b.Rows = 1
	b.Int8s["native"] = &Int8Vector{Values: []int8{5}}
	native, err := EstimateNativeOwnedBatchRangeBytes(b, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Legacy Arrow sizing deliberately reports 8 logical bytes, not the retained
	// megabyte root capacity. Native correctness must not silently reprice it.
	root := arrowmem.NewBufferBytes(make([]byte, 8, 1<<20))
	defer root.Release()
	data := array.NewData(arrow.PrimitiveTypes.Int64, 1, []*arrowmem.Buffer{nil, root}, nil, 0, 0)
	defer data.Release()
	a := array.NewInt64Data(data)
	defer a.Release()
	b.Opaque["legacy"] = &OpaqueVector{Rows: 1, Value: a, Validity: Bitmap{1}}
	mixed, err := EstimateNativeOwnedBatchRangeBytes(b, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if mixed.NativeBytes != native.NativeBytes || mixed.LegacyOpaqueBytes != 16 || mixed.Bytes != native.Bytes+16 || !mixed.OpaquePresent {
		t.Fatalf("mixed scope mismatch native=%+v mixed=%+v", native, mixed)
	}
}

func TestNativeOwnedZeroStructChildContainerAndZeroOpaquePresence(t *testing.T) {
	a := array.NewNull(0)
	defer a.Release()
	child := NewBatch()
	child.Opaque["legacy"] = &OpaqueVector{Value: a}
	pure, err := EstimateNativeOwnedBatchRangeBytes(child, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if pure.Bytes != 0 || pure.NativeBytes != 0 || pure.LegacyOpaqueBytes != 0 || !pure.OpaquePresent {
		t.Fatalf("zero Opaque scope %+v", pure)
	}
	b := NewBatch()
	b.Structs["s"] = &StructVector{Values: child}
	got, err := EstimateNativeOwnedBatchRangeBytes(b, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.NativeBytes < 2*4096+1024 || got.LegacyOpaqueBytes != 0 || !got.OpaquePresent {
		t.Fatalf("native child container missing: %+v", got)
	}
	clone, err := CloneBatchRange(b, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer clone.ReleaseOwned()
	if clone.Structs["s"].Values.Opaque["legacy"] == nil {
		t.Fatal("zero-row runtime Opaque lost")
	}
	empty := NewBatch()
	empty.Rows = 1
	noColumns, err := EstimateNativeOwnedBatchRangeBytes(empty, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if noColumns.Bytes < 4096 || noColumns.OpaquePresent {
		t.Fatalf("empty native container uncharged: %+v", noColumns)
	}
}

func TestNativeOwnedDeepZeroStructChainHasNoArtificialLimit(t *testing.T) {
	root := NewBatch()
	for i := 0; i < 128; i++ {
		next := NewBatch()
		next.Structs["s"] = &StructVector{Values: root}
		root = next
	}
	got, err := EstimateNativeOwnedBatchRangeBytes(root, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.NativeBytes < 129*4096 {
		t.Fatal("deep child containers missing")
	}
	clone, err := CloneBatchRange(root, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	clone.ReleaseOwned()
}

func TestNativeOwnedRepeatedFieldsAndSynthesizedNulls(t *testing.T) {
	for _, kind := range []DataType{TypeInt8, TypeList} {
		child := NewBatch()
		child.Rows = 2
		f := Field{Name: "absent", Type: kind}
		b := NewBatch()
		b.Rows = 2
		b.Structs["s"] = &StructVector{Rows: 2, Values: child, Fields: []Field{f, f}}
		charge, err := EstimateNativeOwnedBatchRangeBytes(b, 0, 2)
		if err != nil {
			t.Fatal(err)
		}
		c, err := CloneBatchRange(b, 0, 2)
		if err != nil {
			t.Fatal(err)
		}
		defer c.ReleaseOwned()
		if charge.NativeBytes <= BatchBytes(c) {
			t.Fatal("synthesized duplicate payload not charged")
		}
		if kind == TypeInt8 {
			v := c.Structs["s"].Values.Int8s["absent"]
			if len(v.Values) != 4 || v.IsValid(0) {
				t.Fatal("numeric duplicate/NULL behavior changed")
			}
		} else {
			v := c.Structs["s"].Values.StringLists["absent"]
			if v.Len() != 4 || v.IsValid(0) {
				t.Fatal("list duplicate/NULL behavior changed")
			}
		}
	}
}

func TestNativeOwnedDoesNotValidateUnclonedRuntimeStructFields(t *testing.T) {
	hidden := NewBatch()
	hidden.Rows = 1
	child := NewBatch()
	child.Rows = 1
	child.Int8s["x"] = &Int8Vector{Values: []int8{7}}
	child.Structs["ignored"] = &StructVector{Rows: 1, Values: hidden, Fields: []Field{{Name: "unsupported", Type: TypeStruct}}}
	b := NewBatch()
	b.Rows = 1
	b.Structs["outer"] = &StructVector{Rows: 1, Values: child, Fields: []Field{{Name: "x", Type: TypeInt8}}}
	if _, err := EstimateNativeOwnedBatchRangeBytes(b, 0, 1); err != nil {
		t.Fatal(err)
	}
	c, err := CloneBatchRange(b, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.ReleaseOwned()
	if len(c.Structs["outer"].Values.Structs) != 0 {
		t.Fatal("undeclared runtime Struct should not be copied")
	}
}

func TestNativeOwnedEstimationHasNoHeapWorkspace(t *testing.T) {
	flat := NewBatch()
	flat.Rows = 2
	flat.Int8s["x"] = &Int8Vector{Values: []int8{1, 2}}
	f := Field{Name: "x", Type: TypeInt8}
	nested := NewBatch()
	nested.Rows = 2
	nested.Structs["s"] = &StructVector{Rows: 2, Values: flat, Fields: []Field{f, f}}
	for _, b := range []*Batch{flat, nested} {
		var result NativeOwnedRangeEstimate
		var err error
		allocations := testing.AllocsPerRun(100, func() { result, err = EstimateNativeOwnedBatchRangeBytes(b, 0, 2) })
		if err != nil || result.Bytes <= 0 {
			t.Fatalf("estimate=%+v err=%v", result, err)
		}
		if allocations != 0 {
			t.Fatalf("preflight allocated %g objects", allocations)
		}
	}
}
