package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

func TestJoinGatherAdmissionCountsEveryRuntimeBinding(t *testing.T) {
	src := memory.NewBatch()
	src.Rows = 1
	src.Int64s["key"] = &memory.Int64Vector{Values: []int64{7}}
	alias := &memory.Int8Vector{Values: []int8{3}}
	const aliases = 4096
	for i := 0; i < aliases; i++ {
		src.Int8s[fmt.Sprintf("extra_%d", i)] = alias
	}
	// Same name in another typed map is another owned gather, too.
	src.Uint8s["extra_0"] = &memory.Uint8Vector{Values: []uint8{9}}
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64})
	selected := []int{0}
	payload, owned, metadata, err := planJoinGather(src, selected)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(aliases + 9); payload != want || owned != want {
		t.Fatalf("payload=%d owned=%d want=%d", payload, owned, want)
	}
	if want := int64(4096 + (aliases+2)*512); metadata != want {
		t.Fatalf("metadata=%d want=%d", metadata, want)
	}
	got, err := joinGatherAdmissionBytes(schema, src, selected)
	if err != nil || got != owned+metadata {
		t.Fatalf("admission=%d want=%d err=%v", got, owned+metadata, err)
	}
	schemaMetadata, err := joinBatchMetadataBytes(schema.Fields)
	if err != nil {
		t.Fatal(err)
	}
	old := 2*memory.BatchBytes(src) + schemaMetadata
	manager := memory.NewBudgetManager(old + 1)
	reservation, err := manager.Reserve(context.Background(), old)
	if err != nil {
		t.Fatalf("old estimate should pass fixed budget: %v", err)
	}
	reservation.Release()
	dst := memory.NewBatch()
	sentinel := &memory.Int64Vector{Values: []int64{123}}
	dst.Int64s["sentinel"] = sentinel
	dst.Rows = 1
	writes := 0
	reserveAndGather := func() error {
		cost, err := joinGatherAdmissionBytes(schema, src, selected)
		if err != nil {
			return err
		}
		reservation, err := manager.Reserve(context.Background(), cost)
		if err != nil {
			return err
		}
		defer reservation.Release()
		writes++
		return takeJoinRowsBounded(context.Background(), dst, src, selected)
	}
	if err := reserveAndGather(); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("want pre-admission budget error, got %v", err)
	}
	if writes != 0 || manager.Used() != 0 || dst.Rows != 1 || dst.Int64s["sentinel"] != sentinel {
		t.Fatalf("preflight wrote or leaked: writes=%d used=%d dst=%+v", writes, manager.Used(), dst)
	}
	if allocs := testing.AllocsPerRun(20, func() {
		if _, err := joinGatherAdmissionBytes(schema, src, selected); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("successful preflight allocated %v times", allocs)
	}
	if err := takeJoinRowsBounded(context.Background(), dst, src, selected); err != nil {
		t.Fatal(err)
	}
	if len(dst.Int8s) != aliases || dst.Int8s["extra_0"] == dst.Int8s["extra_1"] || dst.Int8s["extra_0"] == alias || dst.Uint8s["extra_0"].Values[0] != 9 {
		t.Fatal("gather lost or coalesced runtime bindings")
	}
	if actual := joinGatherTestOwnedCapacity(dst, false); actual != owned {
		t.Fatalf("owned cap bytes=%d planned=%d", actual, owned)
	}
}

func joinGatherTestFixture() *memory.Batch {
	b := memory.NewBatch()
	b.Rows = 3
	b.Int8s["Int8s"] = &memory.Int8Vector{Values: []int8{1, 2, 3}, Validity: memory.Bitmap{5}, Encoding: memory.EncodingPlain}
	b.Int16s["Int16s"] = &memory.Int16Vector{Values: []int16{1, 2, 3}, Validity: memory.Bitmap{5}, Encoding: memory.EncodingPlain}
	b.Int32s["Int32s"] = &memory.Int32Vector{Values: []int32{1, 2, 3}, Validity: memory.Bitmap{5}, Encoding: memory.EncodingPlain}
	b.Int64s["Int64s"] = &memory.Int64Vector{Values: []int64{1, 2, 3}, Validity: memory.Bitmap{5}, Encoding: memory.EncodingPlain}
	b.Uint8s["Uint8s"] = &memory.Uint8Vector{Values: []uint8{1, 2, 3}, Validity: memory.Bitmap{5}, Encoding: memory.EncodingPlain}
	b.Uint16s["Uint16s"] = &memory.Uint16Vector{Values: []uint16{1, 2, 3}, Validity: memory.Bitmap{5}, Encoding: memory.EncodingPlain}
	b.Uint32s["Uint32s"] = &memory.Uint32Vector{Values: []uint32{1, 2, 3}, Validity: memory.Bitmap{5}, Encoding: memory.EncodingPlain}
	b.Uint64s["Uint64s"] = &memory.Uint64Vector{Values: []uint64{1, 2, 3}, Validity: memory.Bitmap{5}, Encoding: memory.EncodingPlain}
	b.Float32s["Float32s"] = &memory.Float32Vector{Values: []float32{1, 2, 3}, Validity: memory.Bitmap{5}, Encoding: memory.EncodingPlain}
	b.Float64s["Float64s"] = &memory.Float64Vector{Values: []float64{1, 2, 3}, Validity: memory.Bitmap{5}, Encoding: memory.EncodingPlain}
	b.Decimals["decimal"] = &memory.Decimal128Vector{Values: []memory.Decimal128{{}, {}, {}}, Validity: memory.Bitmap{5}, Precision: 18, Scale: 4}
	b.Strings["text"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 7, 10}, Data: []byte("aHIDDENbbb"), Validity: memory.Bitmap{5}}
	b.DictStrings["dict"] = &memory.DictionaryStringVector{Indices: []uint32{1, 0, 1}, Offsets: []uint32{0, 3, 6}, Data: []byte("onetwo"), Validity: memory.Bitmap{5}, DictionaryDomain: "words", Encoding: memory.EncodingDictionary}
	list := &memory.StringListVector{Offsets: []uint32{0, 2, 3, 5}, Validity: memory.Bitmap{5}, Values: &memory.StringViewVector{Offsets: []uint32{0, 1, 7, 10, 10, 11}, Data: []byte("aHIDDENxyzq"), Validity: memory.Bitmap{29}}}
	b.StringLists["list"] = list
	children := memory.NewBatch()
	children.Rows = 3
	children.Int8s["number"] = &memory.Int8Vector{Values: []int8{4, 5, 6}, Validity: memory.Bitmap{5}}
	children.Strings["text"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 7, 10}, Data: []byte("aHIDDENbbb"), Validity: memory.Bitmap{3}}
	children.DictStrings["dict"] = b.DictStrings["dict"]
	children.StringLists["list"] = list
	// Fields select which child maps gather. Extra Values maps still belong to
	// the historic full-payload lower bound, and must be checked for safety.
	children.Int64s["ungathered"] = &memory.Int64Vector{Values: []int64{91, 92, 93}}
	b.Structs["record"] = &memory.StructVector{Rows: 3, Validity: memory.Bitmap{5}, Fields: []memory.Field{
		{Name: "number", Type: memory.TypeInt8, Nullable: true},
		{Name: "text", Type: memory.TypeString, Nullable: true},
		{Name: "dict", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingDictionary},
		memory.ListStringField("list", true, true),
	}, Values: children}
	return b
}

func TestJoinGatherExactCapacitiesAndNestedSemantics(t *testing.T) {
	src := joinGatherTestFixture()
	rows := make([]int, 197)
	for i := range rows {
		rows[i] = []int{2, 0, 1}[i%3]
	}
	payload, owned, _, err := planJoinGather(src, rows)
	if err != nil {
		t.Fatal(err)
	}
	if payload != memory.BatchBytes(src) {
		t.Fatalf("checked full payload=%d legacy=%d", payload, memory.BatchBytes(src))
	}
	dst := memory.NewBatch()
	if err := takeJoinRowsBounded(context.Background(), dst, src, rows); err != nil {
		t.Fatal(err)
	}
	if actual := joinGatherTestOwnedCapacity(dst, false); actual != owned {
		t.Fatalf("owned cap bytes=%d planned=%d", actual, owned)
	}
	if dst.Rows != len(rows) || len(dst.Structs["record"].Values.Int64s) != 0 {
		t.Fatal("incorrect selection or gathered undeclared struct child")
	}
	if dst.Decimals["decimal"].Precision != 18 || dst.Decimals["decimal"].Scale != 4 || dst.Decimals["decimal"].IsValid(2) {
		t.Fatal("decimal metadata or NULL lost")
	}
	if &dst.DictStrings["dict"].Data[0] != &src.DictStrings["dict"].Data[0] || &dst.DictStrings["dict"].Offsets[0] != &src.DictStrings["dict"].Offsets[0] {
		t.Fatal("top-level dictionary table should be borrowed")
	}
	nested := dst.Structs["record"]
	if &nested.Values.DictStrings["dict"].Data[0] == &src.Structs["record"].Values.DictStrings["dict"].Data[0] || &nested.Values.DictStrings["dict"].Offsets[0] == &src.Structs["record"].Values.DictStrings["dict"].Offsets[0] {
		t.Fatal("struct dictionary table should be copied")
	}
	if cap(nested.Fields) != len(nested.Fields) || cap(nested.Validity) != joinGatherWords(len(rows)) || len(nested.Validity) != cap(nested.Validity) {
		t.Fatal("struct fields or bitmap was not exactly allocated")
	}
	for i, row := range rows {
		if string(dst.Strings["text"].ValueBytes(i)) != string(src.Strings["text"].ValueBytes(row)) {
			t.Fatal("top-level gather discarded hidden NULL string span")
		}
		if nested.IsValid(i) != src.Structs["record"].IsValid(row) {
			t.Fatalf("struct validity row=%d", i)
		}
		child := nested.Values.Strings["text"]
		valid := src.Structs["record"].IsValid(row) && src.Structs["record"].Values.Strings["text"].IsValid(row)
		if child.IsValid(i) != valid || (!valid && len(child.ValueBytes(i)) != 0) {
			t.Fatalf("struct child NULL handling row=%d", i)
		}
	}
	// Compare against ordinary take for logical nested values, allowing the
	// bounded path to represent all-valid bitmap words explicitly.
	ordinary := memory.NewBatch()
	if err := takeRows(context.Background(), ordinary, src, rows); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(dst.StringLists["list"].Offsets, ordinary.StringLists["list"].Offsets) || !reflect.DeepEqual(dst.StringLists["list"].Values.Offsets, ordinary.StringLists["list"].Values.Offsets) || !reflect.DeepEqual(dst.StringLists["list"].Values.Data, ordinary.StringLists["list"].Values.Data) {
		t.Fatal("list gather differs from ordinary take")
	}
	if !reflect.DeepEqual(nested.Values.Int8s["number"].Values, ordinary.Structs["record"].Values.Int8s["number"].Values) || !reflect.DeepEqual(nested.Values.DictStrings["dict"].Indices, ordinary.Structs["record"].Values.DictStrings["dict"].Indices) {
		t.Fatal("nested values differ from ordinary take")
	}
	if allocs := testing.AllocsPerRun(20, func() {
		if _, err := joinGatherAdmissionBytes(memory.Schema{}, src, rows); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("nested successful preflight allocated %v times", allocs)
	}
}

func TestJoinGatherFullPayloadIncludesUnselectedStructMaps(t *testing.T) {
	src := joinGatherTestFixture()
	rows := []int{0}
	before, ownedBefore, metadataBefore, err := planJoinGather(src, rows)
	if err != nil {
		t.Fatal(err)
	}
	src.Structs["record"].Values.Int64s["other"] = src.Structs["record"].Values.Int64s["ungathered"]
	after, ownedAfter, metadataAfter, err := planJoinGather(src, rows)
	if err != nil {
		t.Fatal(err)
	}
	if after-before != 24 || ownedAfter != ownedBefore || metadataAfter != metadataBefore {
		t.Fatalf("full-payload delta=%d owned delta=%d metadata delta=%d", after-before, ownedAfter-ownedBefore, metadataAfter-metadataBefore)
	}
	got, err := joinGatherAdmissionBytes(memory.Schema{}, src, rows)
	if err != nil {
		t.Fatal(err)
	}
	if got < 2*after+4096 {
		t.Fatalf("historic complete payload lower bound lost: got=%d payload=%d", got, after)
	}
}

func TestJoinGatherRejectsMalformedBeforeDestinationWrites(t *testing.T) {
	cases := []struct {
		name     string
		mutate   func(*memory.Batch)
		contains string
	}{
		{"nil fixed", func(b *memory.Batch) { b.Int8s["extra"] = nil }, "nil"},
		{"short fixed", func(b *memory.Batch) { b.Int8s["extra"] = &memory.Int8Vector{Values: []int8{1}} }, "rows"},
		{"short validity", func(b *memory.Batch) { b.Int8s["Int8s"].Validity = memory.Bitmap{} }, "validity"},
		{"nil string", func(b *memory.Batch) { b.Strings["extra"] = nil }, "nil"},
		{"bad string offsets", func(b *memory.Batch) { b.Strings["text"].Offsets[2] = 99 }, "offsets"},
		{"nil dictionary", func(b *memory.Batch) { b.DictStrings["extra"] = nil }, "nil"},
		{"bad dictionary code", func(b *memory.Batch) { b.DictStrings["dict"].Indices[0] = math.MaxUint32 }, "code"},
		{"nil list", func(b *memory.Batch) { b.StringLists["extra"] = nil }, "nil"},
		{"nil list child", func(b *memory.Batch) { b.StringLists["list"].Values = nil }, "nil"},
		{"bad list offsets", func(b *memory.Batch) { b.StringLists["list"].Offsets[2] = math.MaxUint32 }, "offsets"},
		{"nil struct", func(b *memory.Batch) { b.Structs["extra"] = nil }, "struct"},
		{"missing struct child", func(b *memory.Batch) { delete(b.Structs["record"].Values.Int8s, "number") }, "missing"},
		{"unsupported struct field", func(b *memory.Batch) { b.Structs["record"].Fields[0].Type = memory.TypeBinary }, "unsupported"},
		{"duplicate struct field", func(b *memory.Batch) { b.Structs["record"].Fields[1].Name = "number" }, "duplicate"},
		{"nil undeclared struct map", func(b *memory.Batch) { b.Structs["record"].Values.Int8s["undeclared"] = nil }, "nil"},
		{"cyclic struct", func(b *memory.Batch) { b.Structs["cycle"] = &memory.StructVector{Rows: b.Rows, Values: b} }, "cyclic"},
		{"opaque extra", func(b *memory.Batch) { b.Opaque["extra"] = &memory.OpaqueVector{Rows: b.Rows} }, "opaque"},
		{"opaque undeclared child", func(b *memory.Batch) { b.Structs["record"].Values.Opaque["extra"] = &memory.OpaqueVector{Rows: b.Rows} }, "opaque"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := joinGatherTestFixture()
			tc.mutate(src)
			dst := memory.NewBatch()
			sentinel := &memory.Int64Vector{Values: []int64{42}}
			dst.Int64s["sentinel"] = sentinel
			dst.Rows = 1
			for _, rows := range [][]int{{0}, nil} {
				if _, err := joinGatherAdmissionBytes(memory.Schema{}, src, rows); err == nil || !strings.Contains(err.Error(), tc.contains) {
					t.Fatalf("preflight error=%v want containing %q", err, tc.contains)
				}
				if err := takeJoinRowsBounded(context.Background(), dst, src, rows); err == nil {
					t.Fatal("malformed gather accepted")
				}
				if dst.Rows != 1 || len(dst.Int64s) != 1 || dst.Int64s["sentinel"] != sentinel {
					t.Fatal("rejected gather modified destination")
				}
			}
		})
	}
	for _, rows := range [][]int{{-1}, {3}} {
		if _, err := joinGatherAdmissionBytes(memory.Schema{}, joinGatherTestFixture(), rows); err == nil {
			t.Fatal("out-of-range selection accepted")
		}
	}
	if _, err := joinGatherAdmissionBytes(memory.Schema{}, nil, nil); err == nil {
		t.Fatal("nil source accepted")
	}
}

func TestJoinGatherEmptySelectionExactCapacities(t *testing.T) {
	src := joinGatherTestFixture()
	_, owned, _, err := planJoinGather(src, nil)
	if err != nil {
		t.Fatal(err)
	}
	dst := memory.NewBatch()
	if err := takeJoinRowsBounded(context.Background(), dst, src, nil); err != nil {
		t.Fatal(err)
	}
	if actual := joinGatherTestOwnedCapacity(dst, false); actual != owned {
		t.Fatalf("empty cap bytes=%d planned=%d", actual, owned)
	}
	nested := dst.Structs["record"]
	if dst.Rows != 0 || len(nested.Fields) != 4 || nested.Values.Strings["text"] == nil || nested.Values.DictStrings["dict"] == nil || nested.Values.StringLists["list"] == nil || nested.Values.Int8s["number"] == nil || len(nested.Values.Int64s) != 0 {
		t.Fatal("empty selection lost declared children or copied undeclared children")
	}
}

func TestJoinGatherDictionaryStringFallbackUsesActualMap(t *testing.T) {
	src := joinGatherTestFixture()
	parent := src.Structs["record"]
	parent.Values.Strings["dict"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2, 3}, Data: []byte("abc")}
	_, owned, _, err := planJoinGather(src, []int{2, 0})
	if err != nil {
		t.Fatal(err)
	}
	dst := memory.NewBatch()
	if err := takeJoinRowsBounded(context.Background(), dst, src, []int{2, 0}); err != nil {
		t.Fatal(err)
	}
	child := dst.Structs["record"].Values
	if child.DictStrings["dict"] != nil || child.Strings["dict"] == nil || string(child.Strings["dict"].Data) != "ca" {
		t.Fatal("Strings did not take precedence over dictionary fallback")
	}
	if actual := joinGatherTestOwnedCapacity(dst, false); actual != owned {
		t.Fatalf("capacity=%d plan=%d", actual, owned)
	}
}

func TestJoinGatherNativeDecimalPreservesCoefficients(t *testing.T) {
	src := memory.NewBatch()
	src.Rows = 3
	src.Decimals["extra"] = &memory.Decimal128Vector{Values: []memory.Decimal128{decimal128.FromI64(123), decimal128.FromI64(-456), decimal128.FromI64(789)}, Validity: memory.Bitmap{5}, Precision: 9, Scale: 2}
	// Both bounded and public gather must preserve native decimal columns.
	for _, gather := range []func(context.Context, *memory.Batch, *memory.Batch, []int) error{takeJoinRowsBounded, takeRows} {
		dst := memory.NewBatch()
		if err := gather(context.Background(), dst, src, []int{2, 1, 0, 2}); err != nil {
			t.Fatal(err)
		}
		v := dst.Decimals["extra"]
		if v == nil || v.Precision != 9 || v.Scale != 2 || v.Values[0] != src.Decimals["extra"].Values[2] || v.Values[2] != src.Decimals["extra"].Values[0] || v.IsValid(1) {
			t.Fatal("decimal gather lost values, NULL, or metadata")
		}
		if cap(v.Values) != 4 || cap(v.Validity) != 1 {
			t.Fatal("decimal gather capacity not exact")
		}
	}
}

func TestJoinGatherCheckedUnsignedArithmetic(t *testing.T) {
	tests := []struct {
		total        int64
		count, width uint64
		want         int64
		fail         bool
	}{
		{0, math.MaxInt64, 1, math.MaxInt64, false},
		{1, math.MaxInt64, 1, 1, true},
		{0, math.MaxUint64, 1, 0, true},
		{0, math.MaxUint64, 0, 0, false},
		{0, 1, math.MaxUint64, 0, true},
		{0, uint64(math.MaxInt64)/16 + 1, 16, 0, true},
		{-1, 0, 1, -1, true},
	}
	for _, tc := range tests {
		total := tc.total
		err := addJoinGatherProduct(&total, tc.count, tc.width)
		if (err != nil) != tc.fail || total != tc.want {
			t.Fatalf("total=%d count=%d width=%d got=%d err=%v want=%d fail=%v", tc.total, tc.count, tc.width, total, err, tc.want, tc.fail)
		}
	}
	if got := joinGatherWords(math.MaxInt); got != math.MaxInt/64+1 {
		t.Fatalf("bitmap word overflow: %d", got)
	}
}

// Count backing-array capacities, not logical lengths. Top-level dictionary
// tables are borrowed; only struct fallback dictionary tables are newly owned.
func joinGatherTestOwnedCapacity(b *memory.Batch, nested bool) int64 {
	var total int64
	for _, v := range b.Int8s {
		total += int64(cap(v.Values))*1 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Int16s {
		total += int64(cap(v.Values))*2 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Int32s {
		total += int64(cap(v.Values))*4 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Int64s {
		total += int64(cap(v.Values))*8 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Uint8s {
		total += int64(cap(v.Values))*1 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Uint16s {
		total += int64(cap(v.Values))*2 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Uint32s {
		total += int64(cap(v.Values))*4 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Uint64s {
		total += int64(cap(v.Values))*8 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Float32s {
		total += int64(cap(v.Values))*4 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Float64s {
		total += int64(cap(v.Values))*8 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Decimals {
		total += int64(cap(v.Values))*16 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Strings {
		total += int64(cap(v.Offsets))*4 + int64(cap(v.Data)) + int64(cap(v.Validity))*8
	}
	for _, v := range b.DictStrings {
		total += int64(cap(v.Indices))*4 + int64(cap(v.Validity))*8
		if nested {
			total += int64(cap(v.Offsets))*4 + int64(cap(v.Data))
		}
	}
	for _, v := range b.StringLists {
		total += int64(cap(v.Offsets))*4 + int64(cap(v.Validity))*8
		total += int64(cap(v.Values.Offsets))*4 + int64(cap(v.Values.Data)) + int64(cap(v.Values.Validity))*8
	}
	for _, v := range b.Structs {
		total += int64(cap(v.Validity))*8 + joinGatherTestOwnedCapacity(v.Values, true)
	}
	return total
}

func TestJoinGatherRejectsRealOpaqueWithoutChangingOrdinaryTake(t *testing.T) {
	allocator := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	builder := array.NewInt64Builder(allocator)
	builder.AppendValues([]int64{11, 22, 33}, []bool{true, false, true})
	arr := builder.NewArray()
	builder.Release()
	defer func() { arr.Release(); allocator.AssertSize(t, 0) }()
	src := memory.NewBatch()
	src.Rows = 3
	src.Opaque["unadvertised"] = &memory.OpaqueVector{Rows: 3, Type: memory.TypeInt64, Value: arr}
	dst := memory.NewBatch()
	sentinel := &memory.Int64Vector{Values: []int64{99}}
	dst.Int64s["sentinel"] = sentinel
	dst.Rows = 1
	if _, err := joinGatherAdmissionBytes(memory.Schema{}, src, []int{2, 1, 0, 2}); err == nil || !strings.Contains(err.Error(), "opaque") {
		t.Fatalf("opaque preflight error=%v", err)
	}
	if err := takeJoinRowsBounded(context.Background(), dst, src, []int{2, 1, 0, 2}); err == nil || !strings.Contains(err.Error(), "opaque") {
		t.Fatalf("opaque gather error=%v", err)
	}
	if dst.Rows != 1 || dst.Int64s["sentinel"] != sentinel || len(dst.Opaque) != 0 {
		t.Fatal("rejected opaque gather wrote destination")
	}
	if err := takeRows(context.Background(), dst, src, []int{2, 1, 0, 2}); err != nil {
		t.Fatal(err)
	}
	defer dst.ReleaseOwned()
	out, ok := dst.Opaque["unadvertised"].Value.(*array.Int64)
	if !ok || out.Len() != 4 || out.Value(0) != 33 || !out.IsNull(1) || out.Value(2) != 11 || out.Value(3) != 33 {
		t.Fatal("ordinary opaque gather changed")
	}
}

func TestJoinGatherBoundsUndeclaredStructDepthBeforeAdmission(t *testing.T) {
	makeChain := func(depth int) *memory.Batch {
		root := memory.NewBatch()
		root.Rows = 1
		root.Int64s["key"] = &memory.Int64Vector{Values: []int64{7}}
		current := root
		for i := 0; i < depth; i++ {
			child := memory.NewBatch()
			child.Rows = 1
			// No Fields entry selects the deeper maps for gathering. The
			// complete payload lower bound must still inspect them safely.
			current.Structs["extra"] = &memory.StructVector{Rows: 1, Values: child}
			current = child
		}
		return root
	}
	rows := []int{0}
	supported := makeChain(maxJoinGatherStructDepth)
	if _, err := joinGatherAdmissionBytes(memory.Schema{}, supported, rows); err != nil {
		t.Fatalf("64 Struct.Values levels should be supported: %v", err)
	}
	if allocs := testing.AllocsPerRun(10, func() {
		if _, err := joinGatherAdmissionBytes(memory.Schema{}, supported, rows); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("maximum supported depth allocated %v times", allocs)
	}
	for _, depth := range []int{maxJoinGatherStructDepth + 1, 4096} {
		src := makeChain(depth)
		dst := memory.NewBatch()
		sentinel := &memory.Int64Vector{Values: []int64{99}}
		dst.Int64s["sentinel"] = sentinel
		dst.Rows = 1
		manager := memory.NewBudgetManager(1 << 20)
		reservations, writes := 0, 0
		admitAndGather := func() error {
			cost, err := joinGatherAdmissionBytes(memory.Schema{}, src, rows)
			if err != nil {
				return err
			}
			reservations++
			reservation, err := manager.Reserve(context.Background(), cost)
			if err != nil {
				return err
			}
			defer reservation.Release()
			writes++
			return takeJoinRowsBounded(context.Background(), dst, src, rows)
		}
		if err := admitAndGather(); err == nil || !strings.Contains(err.Error(), "nesting exceeds 64 levels") {
			t.Fatalf("depth=%d error=%v", depth, err)
		}
		if reservations != 0 || writes != 0 || manager.Used() != 0 {
			t.Fatalf("depth=%d reservations=%d writes=%d used=%d", depth, reservations, writes, manager.Used())
		}
		if err := takeJoinRowsBounded(context.Background(), dst, src, rows); err == nil || !strings.Contains(err.Error(), "nesting exceeds 64 levels") {
			t.Fatalf("unchecked gather depth=%d error=%v", depth, err)
		}
		if dst.Rows != 1 || dst.Int64s["sentinel"] != sentinel || len(dst.Structs) != 0 {
			t.Fatalf("depth=%d rejected gather modified destination", depth)
		}
	}
}

func TestJoinGatherAcceptsZeroRowStringsWithoutOffsets(t *testing.T) {
	for _, offsets := range [][]uint32{nil, {}} {
		src := memory.NewBatch()
		src.Strings["empty"] = &memory.StringViewVector{Offsets: offsets}
		src.StringLists["empty_list"] = &memory.StringListVector{Offsets: []uint32{0}, Values: &memory.StringViewVector{Offsets: offsets}}
		if err := src.Strings["empty"].Validate(0); err != nil {
			t.Fatal(err)
		}
		if err := src.StringLists["empty_list"].Validate(0); err != nil {
			t.Fatal(err)
		}
		payload, owned, _, err := planJoinGather(src, nil)
		if err != nil {
			t.Fatal(err)
		}
		if payload != memory.BatchBytes(src) {
			t.Fatalf("empty payload=%d legacy=%d", payload, memory.BatchBytes(src))
		}
		bounded, ordinary := memory.NewBatch(), memory.NewBatch()
		if err := takeJoinRowsBounded(context.Background(), bounded, src, nil); err != nil {
			t.Fatal(err)
		}
		if err := takeRows(context.Background(), ordinary, src, nil); err != nil {
			t.Fatal(err)
		}
		if bounded.Rows != ordinary.Rows || !reflect.DeepEqual(bounded.Strings["empty"].Offsets, ordinary.Strings["empty"].Offsets) || !reflect.DeepEqual(bounded.StringLists["empty_list"].Offsets, ordinary.StringLists["empty_list"].Offsets) || !reflect.DeepEqual(bounded.StringLists["empty_list"].Values.Offsets, ordinary.StringLists["empty_list"].Values.Offsets) {
			t.Fatal("empty gather differs from ordinary take")
		}
		if bounded.Strings["empty"].Encoding != ordinary.Strings["empty"].Encoding || bounded.StringLists["empty_list"].Encoding != ordinary.StringLists["empty_list"].Encoding || bounded.StringLists["empty_list"].Values.Encoding != ordinary.StringLists["empty_list"].Values.Encoding {
			t.Fatal("empty gather encoding differs from ordinary take")
		}
		if actual := joinGatherTestOwnedCapacity(bounded, false); actual != owned {
			t.Fatalf("empty capacity=%d plan=%d", actual, owned)
		}
		if allocs := testing.AllocsPerRun(10, func() {
			if _, err := joinGatherAdmissionBytes(memory.Schema{}, src, nil); err != nil {
				panic(err)
			}
		}); allocs != 0 {
			t.Fatalf("empty preflight allocated %v times", allocs)
		}
	}
}

func TestJoinGatherAcceptsEmptyListChildWithoutOffsets(t *testing.T) {
	src := memory.NewBatch()
	src.Rows = 3
	src.StringLists["empty_children"] = &memory.StringListVector{Offsets: []uint32{0, 0, 0, 0}, Validity: memory.Bitmap{5}, Values: &memory.StringViewVector{}}
	if err := src.StringLists["empty_children"].Validate(src.Rows); err != nil {
		t.Fatal(err)
	}
	rows := []int{2, 1, 0, 2}
	_, owned, _, err := planJoinGather(src, rows)
	if err != nil {
		t.Fatal(err)
	}
	bounded, ordinary := memory.NewBatch(), memory.NewBatch()
	if err := takeJoinRowsBounded(context.Background(), bounded, src, rows); err != nil {
		t.Fatal(err)
	}
	if err := takeRows(context.Background(), ordinary, src, rows); err != nil {
		t.Fatal(err)
	}
	got, want := bounded.StringLists["empty_children"], ordinary.StringLists["empty_children"]
	if !reflect.DeepEqual(got.Offsets, want.Offsets) || !reflect.DeepEqual(got.Values.Offsets, want.Values.Offsets) || got.Values.Len() != 0 || len(got.Values.Data) != 0 {
		t.Fatal("empty-child gather differs from ordinary take")
	}
	for i := range rows {
		if got.IsValid(i) != want.IsValid(i) {
			t.Fatalf("NULL/empty-list semantics changed at row %d", i)
		}
	}
	if got.Encoding != want.Encoding || got.Values.Encoding != want.Values.Encoding {
		t.Fatal("empty-child encoding changed")
	}
	if actual := joinGatherTestOwnedCapacity(bounded, false); actual != owned {
		t.Fatalf("empty-child capacity=%d plan=%d", actual, owned)
	}
}
