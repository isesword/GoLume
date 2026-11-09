package memory

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"unsafe"
)

func TestColumnarRunDecodeMetadataExactAdmission(t *testing.T) {
	schema, batch := nestedSpillFixture()
	encoded, err := EncodeColumnarRun(context.Background(), schema, batch, NewBudgetManager(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.Close()
	metadata, err := ColumnarRunDecodeMetadataBytes(schema)
	if err != nil {
		t.Fatal(err)
	}
	charge := int64(len(encoded.Payload)) + metadata
	restored := &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: encoded.Payload}}
	for _, limit := range []int64{charge - 1, charge} {
		manager := &decodeMetadataManager{BudgetManager: NewBudgetManager(limit)}
		decoded, err := DecodeColumnarRun(context.Background(), schema, restored, manager)
		if limit < charge {
			if !errors.Is(err, ErrBudgetExceeded) || decoded != nil || manager.Used() != 0 || manager.reserves != 1 || manager.releases != 0 {
				t.Fatalf("one-byte-short result=%v err=%v manager=%+v", decoded, err, manager)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if manager.Used() != charge || manager.reserves != 1 || manager.charged != charge {
			t.Fatalf("admission=%+v want=%d", manager, charge)
		}
		assertNestedSpillFixture(t, decoded.Batch)
		_ = decoded.Close()
		_ = decoded.Close()
		if manager.Used() != 0 || manager.releases != 1 {
			t.Fatalf("close=%+v", manager)
		}
	}
}

func TestColumnarRunDecodeMetadataFormulaAndMapBoundaries(t *testing.T) {
	base := 2*int64(unsafe.Sizeof(Batch{})) + 256
	for _, count := range []int{0, 1, 7, 8, 9, 13, 14, 15, 16, 17, 64, 128, 257} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			fields := make([]Field, count)
			batch := NewBatch()
			for i := range fields {
				fields[i] = Field{Name: fmt.Sprint("s", i), Type: TypeString}
				batch.Strings[fields[i].Name] = &StringViewVector{Offsets: []uint32{0}}
			}
			schema := Schema{Fields: fields}
			metadata, err := ColumnarRunDecodeMetadataBytes(schema)
			if err != nil {
				t.Fatal(err)
			}
			want := base
			if count > 0 {
				want += 128 + int64(count)*(512+(int64(unsafe.Sizeof(StringViewVector{}))+15)&^15)
			}
			if metadata != want {
				t.Fatalf("metadata=%d want=%d", metadata, want)
			}
			encoded, err := EncodeColumnarRun(context.Background(), schema, batch, NewBudgetManager(0))
			if err != nil {
				t.Fatal(err)
			}
			defer encoded.Close()
			manager := NewBudgetManager(int64(len(encoded.Payload)) + metadata)
			decoded, err := DecodeColumnarRun(context.Background(), schema, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: encoded.Payload}}, manager)
			if err != nil {
				t.Fatal(err)
			}
			if len(decoded.Batch.Strings) != count {
				t.Fatalf("entries=%d want=%d", len(decoded.Batch.Strings), count)
			}
			assertDecodeMapCount(t, decoded.Batch, boolInt(count != 0))
			_ = decoded.Close()
			if manager.Used() != 0 {
				t.Fatal("reservation leak")
			}
		})
	}
}

func TestColumnarRunDecodeMetadataNumericAliasesShareMaps(t *testing.T) {
	schema := NewSchema(
		Field{Name: "i", Type: TypeInt32}, Field{Name: "date", Type: TypeDate32}, Field{Name: "time", Type: TypeTime32},
		Field{Name: "long", Type: TypeInt64}, Field{Name: "stamp", Type: TypeTimestamp}, Field{Name: "duration", Type: TypeDuration},
		Field{Name: "bool", Type: TypeBool}, Field{Name: "u", Type: TypeUint32},
	)
	got, err := ColumnarRunDecodeMetadataBytes(schema)
	if err != nil {
		t.Fatal(err)
	}
	want := columnarDecodeBaseBytes + 3*128
	for _, field := range schema.Fields {
		_, descriptor := columnarDecodeFieldMap(field)
		want += 512 + (int64(descriptor)+15)&^15
	}
	if got != want {
		t.Fatalf("aliases got=%d want=%d", got, want)
	}
	batch := newColumnarDecodeBatch(schema.Fields, 1)
	assertDecodeMapCount(t, batch, 3)
	if batch.Int32s == nil || batch.Int64s == nil || batch.Uint32s == nil {
		t.Fatal("missing shared physical map")
	}
}

func TestColumnarRunDecodeMetadataAllSupportedTypes(t *testing.T) {
	schema, batch := decodeMetadataAllTypesFixture()
	encoded, err := EncodeColumnarRun(context.Background(), schema, batch, NewBudgetManager(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.Close()
	metadata, err := ColumnarRunDecodeMetadataBytes(schema)
	if err != nil {
		t.Fatal(err)
	}
	manager := NewBudgetManager(int64(len(encoded.Payload)) + metadata)
	decoded, err := DecodeColumnarRun(context.Background(), schema, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: encoded.Payload}}, manager)
	if err != nil {
		t.Fatal(err)
	}
	assertDecodeMapCount(t, decoded.Batch, 15)
	if decoded.Batch.Opaque != nil {
		t.Fatal("unsupported map must remain nil")
	}
	if got := decoded.Batch.DictStrings["dict"]; got.DictionaryDomain != batch.DictStrings["dict"].DictionaryDomain || len(got.DictionaryDomain) < 4096 {
		t.Fatal("dictionary domain not preserved")
	}
	if cap(decoded.Batch.Strings["string"].Data) != len(decoded.Batch.Strings["string"].Data) || cap(decoded.Batch.DictStrings["dict"].Data) != len(decoded.Batch.DictStrings["dict"].Data) {
		t.Fatal("decoded data has spare capacity")
	}
	assertDecodeMapCount(t, decoded.Batch.Structs["record"].Values, 2)
	reencoded, err := EncodeColumnarRun(context.Background(), schema, decoded.Batch, NewBudgetManager(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(reencoded.Payload, encoded.Payload) {
		t.Fatal("supported type round trip changed payload")
	}
	_ = reencoded.Close()
	_ = decoded.Close()
	if manager.Used() != 0 {
		t.Fatal("reservation leak")
	}
}

func TestColumnarRunDecodeMetadataEstimationAndValidationAllocateZero(t *testing.T) {
	schema, batch := decodeMetadataAllTypesFixture()
	wideChildren := make([]Field, 256)
	for i := range wideChildren {
		wideChildren[i] = Field{Name: fmt.Sprint("child", i), Type: TypeInt64}
	}
	wide := Schema{Fields: []Field{StructField("wide", true, wideChildren...)}}
	for _, schema := range []Schema{schema, wide, {}} {
		if _, err := ColumnarRunDecodeMetadataBytes(schema); err != nil {
			t.Fatal(err)
		}
		if allocations := testing.AllocsPerRun(100, func() {
			if _, err := ColumnarRunDecodeMetadataBytes(schema); err != nil {
				panic(err)
			}
		}); allocations != 0 {
			t.Fatalf("metadata allocations=%g", allocations)
		}
	}
	encoded, err := EncodeColumnarRun(context.Background(), schema, batch, NewBudgetManager(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.Close()
	decoded, err := DecodeColumnarRun(context.Background(), schema, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: encoded.Payload}}, NewBudgetManager(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer decoded.Close()
	if allocations := testing.AllocsPerRun(100, func() {
		for _, field := range schema.Fields {
			if err := validateColumnarDecodedField(decoded.Batch, field, batch.Rows); err != nil {
				panic(err)
			}
		}
	}); allocations != 0 {
		t.Fatalf("decoded validation allocations=%g", allocations)
	}
}

func TestColumnarRunDecodeMetadataWideStructShallowFields(t *testing.T) {
	children := make([]Field, 257)
	values := NewBatch()
	for i := range children {
		children[i] = Field{Name: fmt.Sprint("v", i), Type: TypeInt64}
		values.Int64s[children[i].Name] = &Int64Vector{}
	}
	children[0] = ListStringField("v0", true, true)
	delete(values.Int64s, "v0")
	values.StringLists["v0"] = &StringListVector{Offsets: []uint32{0}, Values: &StringViewVector{Offsets: []uint32{0}}}
	schema := Schema{Fields: []Field{StructField("record", true, children...)}}
	batch := NewBatch()
	batch.Structs["record"] = &StructVector{Fields: schema.Fields[0].Children, Values: values, Encoding: EncodingView}
	metadata, err := ColumnarRunDecodeMetadataBytes(schema)
	if err != nil {
		t.Fatal(err)
	}
	childMetadata, err := columnarDecodeBatchBytes(children, true)
	if err != nil {
		t.Fatal(err)
	}
	fieldsBytes := (int64(len(children))*int64(unsafe.Sizeof(Field{})) + 15) &^ 15
	structBytes := (int64(unsafe.Sizeof(StructVector{})) + 15) &^ 15
	want := columnarDecodeBaseBytes + 128 + 512 + structBytes + childMetadata + fieldsBytes
	if metadata != want {
		t.Fatalf("wide metadata=%d want=%d", metadata, want)
	}
	encoded, err := EncodeColumnarRun(context.Background(), schema, batch, NewBudgetManager(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.Close()
	manager := NewBudgetManager(int64(len(encoded.Payload)) + metadata)
	decoded, err := DecodeColumnarRun(context.Background(), schema, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: encoded.Payload}}, manager)
	if err != nil {
		t.Fatal(err)
	}
	fieldsAlias := decoded.Batch.Structs["record"].Fields
	if &fieldsAlias[0] == &schema.Fields[0].Children[0] {
		t.Fatal("Struct Fields outer array is borrowed")
	}
	if &fieldsAlias[0].Children[0] != &schema.Fields[0].Children[0].Children[0] {
		t.Fatal("list child schema should be borrowed")
	}
	_ = decoded.Close()
	for _, field := range fieldsAlias {
		if !reflect.DeepEqual(field, Field{}) {
			t.Fatal("owned Fields not cleared")
		}
	}
	if schema.Fields[0].Children[0].Name != "v0" || schema.Fields[0].Children[0].Children[0].Name != "item" || schema.Fields[0].Children[256].Name != "v256" {
		t.Fatal("Close mutated borrowed schema")
	}
}

func TestColumnarRunDecodeMetadataRejectsUnsupportedBeforeReserve(t *testing.T) {
	cyclic := make([]Field, 1)
	cyclic[0] = Field{Name: "cycle", Type: TypeStruct, Children: cyclic}
	deep := Field{Name: "leaf", Type: TypeInt64}
	for range 1000 {
		deep = StructField("deep", true, deep)
	}
	tests := []Schema{
		{Fields: []Field{{Name: "opaque", Type: TypeArrow}}},
		{Fields: []Field{{Name: "invalid", Type: TypeInvalid}}},
		{Fields: []Field{{Name: "dup", Type: TypeInt64}, {Name: "dup", Type: TypeString}}},
		{Fields: []Field{StructField("empty", true)}},
		{Fields: []Field{StructField("dup", true, Field{Name: "x", Type: TypeInt64}, Field{Name: "x", Type: TypeInt64})}},
		{Fields: []Field{StructField("bad", true, Field{Name: "x", Type: TypeBinary})}},
		{Fields: cyclic}, {Fields: []Field{deep}},
		{Fields: []Field{{Name: "scalar", Type: TypeString, Children: cyclic}}},
		{Fields: []Field{{Name: "list", Type: TypeList, Children: []Field{{Name: "item", Type: TypeString, Children: cyclic}}}}},
		{Fields: []Field{{Name: "decimal", Type: TypeDecimal, DecimalPrecision: 3, DecimalScale: 1, ArrowType: "decimal(3, 2)"}}},
	}
	for i, schema := range tests {
		if _, err := ColumnarRunDecodeMetadataBytes(schema); err == nil {
			t.Fatalf("schema %d accepted", i)
		}
		manager := &decodeMetadataManager{BudgetManager: NewBudgetManager(1 << 20)}
		_, err := DecodeColumnarRun(context.Background(), schema, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec}}, manager)
		if !errors.Is(err, ErrSpillCorrupt) || manager.reserves != 0 {
			t.Fatalf("schema %d err=%v reserves=%d", i, err, manager.reserves)
		}
	}
	for _, pair := range [][2]int64{{math.MaxInt64, 1}, {-1, 0}} {
		if _, err := columnarDecodeAdd(pair[0], pair[1]); err == nil {
			t.Fatal("unchecked add overflow")
		}
	}
	for _, pair := range [][2]int64{{math.MaxInt64, 2}, {-1, 0}} {
		if _, err := columnarDecodeMul(pair[0], pair[1]); err == nil {
			t.Fatal("unchecked multiply overflow")
		}
	}
	if _, err := columnarDecodeRounded(math.MaxInt64); err == nil {
		t.Fatal("unchecked rounding overflow")
	}
}

func TestColumnarRunDecodeMetadataCloseClearsAliasesBeforeRelease(t *testing.T) {
	schema, batch := nestedSpillFixture()
	encoded, err := EncodeColumnarRun(context.Background(), schema, batch, NewBudgetManager(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.Close()
	manager := &decodeMetadataManager{BudgetManager: NewBudgetManager(1 << 20)}
	decoded, err := DecodeColumnarRun(context.Background(), schema, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: encoded.Payload}}, manager)
	if err != nil {
		t.Fatal(err)
	}
	rootAlias := decoded.Batch
	stringMap := rootAlias.Strings
	structMap := rootAlias.Structs
	stringAlias := stringMap["name"]
	structAlias := structMap["profile"]
	childBatch := structAlias.Values
	childMap := childBatch.StringLists
	listAlias := childMap["labels"]
	listChild := listAlias.Values
	fieldsAlias := structAlias.Fields
	manager.onRelease = func() {
		if !reflect.DeepEqual(*rootAlias, Batch{}) || !reflect.DeepEqual(*childBatch, Batch{}) {
			t.Error("Batch aliases retain graph during Release")
		}
		if len(stringMap) != 0 || len(structMap) != 0 || len(childMap) != 0 {
			t.Error("map aliases retain entries during Release")
		}
		if !reflect.DeepEqual(*stringAlias, StringViewVector{}) || !reflect.DeepEqual(*structAlias, StructVector{}) || !reflect.DeepEqual(*listAlias, StringListVector{}) || !reflect.DeepEqual(*listChild, StringViewVector{}) {
			t.Error("vector aliases retain buffers during Release")
		}
		for _, f := range fieldsAlias {
			if !reflect.DeepEqual(f, Field{}) {
				t.Error("Fields alias retains schema during Release")
			}
		}
	}
	_ = decoded.Close()
	_ = decoded.Close()
	if decoded.Batch != nil || manager.Used() != 0 || manager.releases != 1 {
		t.Fatal("Close not idempotent")
	}
	if schema.Fields[4].Children[2].Children[0].Name != "item" {
		t.Fatal("borrowed Children changed")
	}
	// The source and encoded payload are independent of decoded ownership.
	assertNestedSpillFixture(t, batch)
	if len(encoded.Payload) == 0 {
		t.Fatal("encoded payload was cleared")
	}
}

func TestColumnarRunDecodeMetadataMalformedPartialChildAndTrailing(t *testing.T) {
	schema, batch := nestedSpillFixture()
	encoded, err := EncodeColumnarRun(context.Background(), schema, batch, NewBudgetManager(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.Close()
	for _, payload := range [][]byte{encoded.Payload[:len(encoded.Payload)-1], append(append([]byte(nil), encoded.Payload...), 0xff)} {
		manager := &decodeMetadataManager{BudgetManager: NewBudgetManager(1 << 20)}
		decoded, err := DecodeColumnarRun(context.Background(), schema, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: payload}}, manager)
		if !errors.Is(err, ErrSpillCorrupt) || decoded != nil || manager.Used() != 0 || manager.reserves != 1 || manager.releases != 1 {
			t.Fatalf("malformed result=%v err=%v manager=%+v", decoded, err, manager)
		}
	}
	// Exercise the partially populated tree directly so the cleanup assertion
	// observes the nested descriptor that existed when a child failed.
	root := newColumnarDecodeBatch(schema.Fields, batch.Rows)
	reader := runReader{buf: encoded.Payload[:len(encoded.Payload)-1], pos: columnarRunHeader}
	var decodeErr error
	for _, field := range schema.Fields {
		if decodeErr = decodeRunField(&reader, root, field, batch.Rows); decodeErr != nil {
			break
		}
	}
	if !errors.Is(decodeErr, ErrSpillCorrupt) || root.Structs["profile"] == nil || root.Structs["profile"].Values.StringLists["labels"] == nil {
		t.Fatal("failed child was not attached to owned parent")
	}
	child := root.Structs["profile"].Values
	list := child.StringLists["labels"]
	manager := &decodeMetadataManager{BudgetManager: NewBudgetManager(1 << 20)}
	reservation, err := manager.Reserve(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	manager.onRelease = func() {
		if !reflect.DeepEqual(*root, Batch{}) || !reflect.DeepEqual(*child, Batch{}) || !reflect.DeepEqual(*list, StringListVector{}) {
			t.Error("partial graph survived reservation release")
		}
	}
	owned := &DecodedColumnarRun{Batch: root, reservation: reservation}
	_ = owned.Close()
}

func TestColumnarRunDecodeMetadataPayloadOwnershipOverlap(t *testing.T) {
	schema := NewSchema(Field{Name: "s", Type: TypeString})
	batch := NewBatch()
	batch.Rows = 1
	batch.Strings["s"] = &StringViewVector{Offsets: []uint32{0, 3}, Data: []byte("abc")}
	encoded, err := EncodeColumnarRun(context.Background(), schema, batch, NewBudgetManager(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.Close()
	metadata, err := ColumnarRunDecodeMetadataBytes(schema)
	if err != nil {
		t.Fatal(err)
	}
	payloadBytes := int64(len(encoded.Payload))
	for _, releaseRestoreFirst := range []bool{false, true} {
		manager := NewBudgetManager(2*payloadBytes + metadata)
		reservation, err := manager.Reserve(context.Background(), payloadBytes)
		if err != nil {
			t.Fatal(err)
		}
		restored := &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: append([]byte(nil), encoded.Payload...)}, reservation: reservation}
		decoded, err := DecodeColumnarRun(context.Background(), schema, restored, manager)
		if err != nil {
			t.Fatal(err)
		}
		if manager.Used() != 2*payloadBytes+metadata {
			t.Fatal("restored/decoded overlap not charged")
		}
		if releaseRestoreFirst {
			clear(restored.Object.Payload)
			_ = restored.Close()
			if manager.Used() != payloadBytes+metadata || string(decoded.Batch.Strings["s"].Data) != "abc" {
				t.Fatal("decoded payload aliases restored payload")
			}
			_ = decoded.Close()
		} else {
			_ = decoded.Close()
			if manager.Used() != payloadBytes || !reflect.DeepEqual(restored.Object.Payload, encoded.Payload) {
				t.Fatal("decoded Close changed restored ownership")
			}
			_ = restored.Close()
		}
		if manager.Used() != 0 {
			t.Fatal("overlap cleanup leak")
		}
	}
}

func assertDecodeMapCount(t *testing.T, batch *Batch, want int) {
	t.Helper()
	got := 0
	value := reflect.ValueOf(batch).Elem()
	for i := 0; i < value.NumField(); i++ {
		if value.Field(i).Kind() == reflect.Map && !value.Field(i).IsNil() {
			got++
		}
	}
	if got != want {
		t.Fatalf("allocated maps=%d want=%d", got, want)
	}
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func decodeMetadataAllTypesFixture() (Schema, *Batch) {
	batch := NewBatch()
	batch.Rows = 1
	types := []DataType{TypeInt8, TypeInt16, TypeInt32, TypeInt64, TypeUint8, TypeUint16, TypeUint32, TypeUint64, TypeFloat32, TypeFloat64, TypeDate32, TypeDate64, TypeTime32, TypeTime64, TypeTimestamp, TypeDuration, TypeBool}
	fields := make([]Field, 0, len(types)+6)
	for _, typ := range types {
		f := Field{Name: typ.String(), Type: typ}
		fields = append(fields, f)
		switch typ {
		case TypeInt8:
			batch.Int8s[f.Name] = &Int8Vector{Values: []int8{1}}
		case TypeInt16:
			batch.Int16s[f.Name] = &Int16Vector{Values: []int16{1}}
		case TypeInt32, TypeDate32, TypeTime32:
			batch.Int32s[f.Name] = &Int32Vector{Values: []int32{1}}
		case TypeInt64, TypeDate64, TypeTime64, TypeTimestamp, TypeDuration:
			batch.Int64s[f.Name] = &Int64Vector{Values: []int64{1}}
		case TypeUint8:
			batch.Uint8s[f.Name] = &Uint8Vector{Values: []uint8{1}}
		case TypeUint16:
			batch.Uint16s[f.Name] = &Uint16Vector{Values: []uint16{1}}
		case TypeUint32, TypeBool:
			batch.Uint32s[f.Name] = &Uint32Vector{Values: []uint32{1}}
		case TypeUint64:
			batch.Uint64s[f.Name] = &Uint64Vector{Values: []uint64{1}}
		case TypeFloat32:
			batch.Float32s[f.Name] = &Float32Vector{Values: []float32{1}}
		case TypeFloat64:
			batch.Float64s[f.Name] = &Float64Vector{Values: []float64{1}}
		}
	}
	fields = append(fields, Field{Name: "decimal", Type: TypeDecimal, DecimalPrecision: 38, DecimalScale: 7, ArrowType: "decimal(38, 7)"}, Field{Name: "string", Type: TypeString}, Field{Name: "binary", Type: TypeBinary}, Field{Name: "dict", Type: TypeString, Encoding: EncodingDictionary}, Field{Name: "dictbinary", Type: TypeBinary, Encoding: EncodingDictionary}, ListStringField("list", true, true), StructField("record", true, Field{Name: "value", Type: TypeInt64}, ListStringField("list", true, true)))
	batch.Decimals["decimal"] = &Decimal128Vector{Values: []Decimal128{{}}, Precision: 38, Scale: 7}
	batch.Strings["string"] = &StringViewVector{Offsets: []uint32{0, 3}, Data: []byte("abc")}
	batch.Strings["binary"] = &StringViewVector{Offsets: []uint32{0, 1}, Data: []byte{0xff}}
	for _, name := range []string{"dict", "dictbinary"} {
		batch.DictStrings[name] = &DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{0, 3}, Data: []byte("xyz"), DictionaryDomain: strings.Repeat("domain", 1024), Encoding: EncodingDictionary}
	}
	batch.StringLists["list"] = &StringListVector{Offsets: []uint32{0, 1}, Values: &StringViewVector{Offsets: []uint32{0, 1}, Data: []byte("a")}, Encoding: EncodingView}
	children := NewBatch()
	children.Rows = 1
	children.Int64s["value"] = &Int64Vector{Values: []int64{1}}
	children.StringLists["list"] = &StringListVector{Offsets: []uint32{0, 0}, Values: &StringViewVector{Offsets: []uint32{0}}, Encoding: EncodingView}
	batch.Structs["record"] = &StructVector{Rows: 1, Fields: fields[len(fields)-1].Children, Values: children, Encoding: EncodingView}
	return Schema{Fields: fields}, batch
}

type decodeMetadataManager struct {
	*BudgetManager
	reserves, releases int
	charged            int64
	onRelease          func()
}

func (m *decodeMetadataManager) Reserve(ctx context.Context, bytes int64) (Reservation, error) {
	m.reserves++
	m.charged = bytes
	r, err := m.BudgetManager.Reserve(ctx, bytes)
	if err != nil {
		return nil, err
	}
	return &decodeMetadataReservation{Reservation: r, manager: m}, nil
}

type decodeMetadataReservation struct {
	Reservation
	manager *decodeMetadataManager
}

func (r *decodeMetadataReservation) Release() {
	r.manager.releases++
	if r.manager.onRelease != nil {
		r.manager.onRelease()
	}
	r.Reservation.Release()
}

func TestColumnarRunDecodeMetadataCallerClearEndsBorrow(t *testing.T) {
	schema, batch := nestedSpillFixture()
	encoded, err := EncodeColumnarRun(context.Background(), schema, batch, NewBudgetManager(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.Close()
	manager := NewBudgetManager(1 << 20)
	decoded, err := DecodeColumnarRun(context.Background(), schema, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: encoded.Payload}}, manager)
	if err != nil {
		t.Fatal(err)
	}
	root := decoded.Batch
	root.ClearColumns() // Voluntarily relinquishes every vector binding.
	if manager.Used() == 0 {
		t.Fatal("caller ClearColumns released the owner's reservation")
	}
	if err := decoded.Close(); err != nil {
		t.Fatal(err)
	}
	if decoded.Batch != nil || root.Rows != 0 || manager.Used() != 0 {
		t.Fatal("owner/charge survived Close")
	}
	assertDecodeMapCount(t, root, 0)
	// Retaining an extracted vector or slice after ClearColumns/Close is outside
	// the borrow contract. Close need not erase aliases removed by the caller.
}
