package memory

import (
	"context"
	"math"
	"strconv"
	"strings"
	"testing"
)

func TestColumnarRunSelectionEstimateFixedWidths(t *testing.T) {
	types := []struct {
		typ   DataType
		width int64
	}{
		{TypeInt8, 1}, {TypeInt16, 2}, {TypeInt32, 4}, {TypeInt64, 8},
		{TypeUint8, 1}, {TypeUint16, 2}, {TypeUint32, 4}, {TypeUint64, 8},
		{TypeFloat32, 4}, {TypeFloat64, 8}, {TypeBool, 4},
		{TypeDate32, 4}, {TypeDate64, 8}, {TypeTimestamp, 8},
		{TypeTime32, 4}, {TypeTime64, 8}, {TypeDuration, 8}, {TypeDecimal, 16},
	}
	rows := make([]int, 65)
	for i := range rows {
		rows[i] = (2*i + 1) % 3
	}
	for _, test := range types {
		t.Run(test.typ.String(), func(t *testing.T) {
			field := Field{Name: "v", Type: test.typ, Nullable: true}
			batch := selectionFixedFixture(field)
			if test.typ == TypeDecimal {
				field.DecimalPrecision, field.DecimalScale = 10, 2
				field.ArrowType = "decimal(10, 2)"
			}
			schema := NewSchema(field)
			estimate := assertSelectionPayload(t, schema, batch, rows)
			want := int64(columnarRunHeader+4+16) + int64(len(rows))*test.width
			if test.typ == TypeDecimal {
				want += 8
			}
			if estimate != want {
				t.Fatalf("estimate=%d want native-width=%d", estimate, want)
			}
			if allocations := testing.AllocsPerRun(20, func() {
				if _, err := EstimateColumnarRunSelectionBytes(schema, batch, rows); err != nil {
					panic(err)
				}
			}); allocations != 0 {
				t.Fatalf("estimate allocated %g times", allocations)
			}
		})
	}
}

func TestColumnarRunSelectionEstimateNestedPayload(t *testing.T) {
	schema, batch := nestedSpillFixture()
	batch.DictStrings["kind"].DictionaryDomain = strings.Repeat("actual-domain", 10000)
	selections := [][]int{nil, {1}, {3, 0, 1, 3, 2}, {2, 2, 2}, {0, 3}}
	for _, rows := range selections {
		assertSelectionPayload(t, schema, batch, rows)
		if allocations := testing.AllocsPerRun(20, func() {
			if _, err := EstimateColumnarRunSelectionBytes(schema, batch, rows); err != nil {
				panic(err)
			}
		}); allocations != 0 {
			t.Fatalf("selection %v allocated %g times", rows, allocations)
		}
	}
}

func TestColumnarRunSelectionEstimateDictionaryRetainedOnce(t *testing.T) {
	for _, typ := range []DataType{TypeString, TypeBinary} {
		t.Run(typ.String(), func(t *testing.T) {
			batch := NewBatch()
			batch.Rows = 3
			domain := strings.Repeat("domain", 20000)
			data := []byte("__a" + strings.Repeat("b", 1<<16) + "ignored suffix")
			batch.DictStrings["v"] = &DictionaryStringVector{
				Indices: []uint32{0, 1, 0}, Offsets: []uint32{2, 3, 3 + 1<<16},
				Data: data, DictionaryDomain: domain, Validity: Bitmap{0b111}, Encoding: EncodingDictionary,
			}
			schema := NewSchema(Field{Name: "v", Type: typ, Encoding: EncodingDictionary, DictionaryDomain: "short-schema-domain"})
			for _, rows := range [][]int{nil, {0}, {0, 0, 0, 0, 0}, {2, 0, 1}} {
				estimate := assertSelectionPayload(t, schema, batch, rows)
				bitmap := int64(WordsFor(len(rows)) * 8)
				want := int64(columnarRunHeader+4+4+3*4+8+1+(1<<16)+4+len(domain)+len(rows)*4) + bitmap
				if estimate != want {
					t.Fatalf("selection %v estimate=%d want=%d", rows, estimate, want)
				}
			}
		})
	}
}

func TestColumnarRunSelectionEstimateOnlySelectedStringBytes(t *testing.T) {
	for _, typ := range []DataType{TypeString, TypeBinary} {
		t.Run(typ.String(), func(t *testing.T) {
			batch := NewBatch()
			batch.Rows = 3
			batch.Strings["v"] = &StringViewVector{
				Offsets:  []uint32{2, 3, 3 + 1<<20, 6 + 1<<20},
				Data:     []byte("__a" + strings.Repeat("b", 1<<20) + "xyz_suffix"),
				Validity: Bitmap{0b011},
			}
			// NULL row 2 retains its hidden "xyz" payload in top-level takeRows.
			rows := []int{2, 0, 2}
			schema := NewSchema(Field{Name: "v", Type: typ, Nullable: true})
			estimate := assertSelectionPayload(t, schema, batch, rows)
			const want = columnarRunHeader + 4 + 8 + 4 + 4*4 + 8 + 3 + 1 + 3
			if estimate != want {
				t.Fatalf("estimate=%d want selected byte spans=%d", estimate, want)
			}
		})
	}
}

func TestColumnarRunSelectionEstimateListCanonicalNulls(t *testing.T) {
	batch := NewBatch()
	batch.Rows = 3
	batch.StringLists["v"] = &StringListVector{
		Offsets: []uint32{0, 2, 3, 4}, Validity: Bitmap{0b101},
		Values: &StringViewVector{
			Offsets:  []uint32{2, 3, 9, 9 + 1<<20, 11 + 1<<20},
			Data:     []byte("__aHIDDEN" + strings.Repeat("b", 1<<20) + "xy_suffix"),
			Validity: Bitmap{0b1101},
		},
	}
	rows := []int{2, 0, 1, 0}
	schema := NewSchema(ListStringField("v", true, true))
	estimate := assertSelectionPayload(t, schema, batch, rows)
	// Five child rows survive, but NULL parent/element hidden bytes do not.
	const want = columnarRunHeader + (4 + 8) + (4 + 5*4) + (4 + 8) + (4 + 6*4) + 8 + 4
	if estimate != want {
		t.Fatalf("estimate=%d want canonical selected list=%d", estimate, want)
	}
}

func TestColumnarRunSelectionEstimateStructIntroducesChildNulls(t *testing.T) {
	children := NewBatch()
	children.Rows = 3
	children.Int8s["n"] = &Int8Vector{Values: []int8{1, 2, 3}}
	children.Strings["s"] = &StringViewVector{Offsets: []uint32{2, 3, 3 + 1<<20, 5 + 1<<20}, Data: []byte("__a" + strings.Repeat("h", 1<<20) + "xy")}
	children.StringLists["l"] = &StringListVector{
		Offsets: []uint32{0, 1, 2, 3},
		Values:  &StringViewVector{Offsets: []uint32{2, 3, 3 + 1<<20, 5 + 1<<20}, Data: children.Strings["s"].Data},
	}
	field := StructField("v", true,
		Field{Name: "n", Type: TypeInt8}, Field{Name: "s", Type: TypeString}, ListStringField("l", false, false))
	batch := NewBatch()
	batch.Rows = 3
	batch.Structs["v"] = &StructVector{Rows: 3, Values: children, Fields: field.Children, Validity: Bitmap{0b101}}
	rows := []int{1, 0, 1, 2}
	schema := NewSchema(field)
	estimate := assertSelectionPayload(t, schema, batch, rows)
	// Child source bitmaps are nil, but gathered child NULLs require bitmaps.
	const want = columnarRunHeader + (4 + 8) + (4 + 8 + 4) + (4 + 8 + 4 + 5*4 + 8 + 3) + (4 + 8 + 4 + 5*4 + 4 + 8 + 4 + 3*4 + 8 + 3)
	if estimate != want {
		t.Fatalf("estimate=%d want struct bitmap and selected payload=%d", estimate, want)
	}
	if allocations := testing.AllocsPerRun(20, func() {
		if _, err := EstimateColumnarRunSelectionBytes(schema, batch, rows); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("struct estimate allocated %g times", allocations)
	}
}

func TestColumnarRunSelectionEstimateNullabilityOnlySelectedRows(t *testing.T) {
	batch := NewBatch()
	batch.Rows = 2
	batch.Int8s["n"] = &Int8Vector{Values: []int8{1, 0}, Validity: Bitmap{0b01}}
	batch.Strings["s"] = &StringViewVector{Offsets: []uint32{0, 1, 1}, Data: []byte("a"), Validity: Bitmap{0b01}}
	batch.DictStrings["d"] = &DictionaryStringVector{Indices: []uint32{0, math.MaxUint32}, Offsets: []uint32{0, 1}, Data: []byte("a"), Validity: Bitmap{0b01}, DictionaryDomain: "d"}
	batch.Decimals["x"] = &Decimal128Vector{Values: []Decimal128{{}, {}}, Validity: Bitmap{0b01}, Precision: 10, Scale: 2}
	batch.StringLists["l"] = &StringListVector{Offsets: []uint32{0, 1, 2}, Values: &StringViewVector{Offsets: []uint32{0, 1, 1}, Data: []byte("a"), Validity: Bitmap{0b01}}}
	fields := []Field{
		{Name: "n", Type: TypeInt8}, {Name: "s", Type: TypeString},
		{Name: "d", Type: TypeString, Encoding: EncodingDictionary},
		{Name: "x", Type: TypeDecimal, DecimalPrecision: 10, DecimalScale: 2},
		ListStringField("l", false, false),
	}
	for _, field := range fields {
		schema := NewSchema(field)
		assertSelectionPayload(t, schema, batch, []int{0, 0})
		if _, err := EstimateColumnarRunSelectionBytes(schema, batch, []int{1}); err == nil {
			t.Fatalf("selected invalid value accepted for %q", field.Name)
		}
	}
	// Child NULLs in an unselected valid struct must not reject row 0.
	field := StructField("st", true, fields[0], fields[1], fields[4])
	parent := NewBatch()
	parent.Rows = 2
	parent.Structs["st"] = &StructVector{Rows: 2, Fields: field.Children, Values: batch}
	assertSelectionPayload(t, NewSchema(field), parent, []int{0})
	if _, err := EstimateColumnarRunSelectionBytes(NewSchema(field), parent, []int{1}); err == nil {
		t.Fatal("selected invalid struct child accepted")
	}
}

func TestColumnarRunSelectionEstimateWideStructDoesNotAllocate(t *testing.T) {
	children := NewBatch()
	children.Rows = 3
	fields := make([]Field, 24)
	for i := range fields {
		name := "child" + strconv.Itoa(i)
		fields[i] = Field{Name: name, Type: TypeInt8}
		children.Int8s[name] = &Int8Vector{Values: []int8{1, 2, 3}}
	}
	field := StructField("v", true, fields...)
	schema := NewSchema(field)
	batch := NewBatch()
	batch.Rows = 3
	batch.Structs["v"] = &StructVector{Rows: 3, Fields: fields, Values: children, Validity: Bitmap{0b101}}
	rows := []int{1, 2, 0, 1}
	assertSelectionPayload(t, schema, batch, rows)
	if allocations := testing.AllocsPerRun(20, func() {
		if _, err := EstimateColumnarRunSelectionBytes(schema, batch, rows); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("wide struct estimate allocated %g times", allocations)
	}
}

func TestColumnarRunSelectionEstimateProjectionAndEmpty(t *testing.T) {
	batch := NewBatch()
	batch.Rows = 3
	batch.Int8s["v"] = &Int8Vector{Values: []int8{1, 2, 3}}
	batch.Strings["unreferenced"] = nil
	schema := NewSchema(Field{Name: "v", Type: TypeInt8})
	if got := assertSelectionPayload(t, schema, batch, []int{2}); got != columnarRunHeader+4+8+1 {
		t.Fatal(got)
	}
	if got := assertSelectionPayload(t, schema, batch, nil); got != columnarRunHeader+4 {
		t.Fatal(got)
	}
	if got := assertSelectionPayload(t, NewSchema(), NewBatch(), nil); got != columnarRunHeader {
		t.Fatal(got)
	}
}

func TestColumnarRunSelectionEstimateRejectsInvalidInputs(t *testing.T) {
	valid := func() (Schema, *Batch, []int) {
		schema, batch := nestedSpillFixture()
		return schema, batch, []int{0}
	}
	tests := []struct {
		name   string
		mutate func(*Schema, **Batch, *[]int)
	}{
		{"nil batch", func(_ *Schema, b **Batch, _ *[]int) { *b = nil }},
		{"negative rows", func(_ *Schema, b **Batch, _ *[]int) { (*b).Rows = -1 }},
		{"overflow rows", func(_ *Schema, b **Batch, _ *[]int) { (*b).Rows = math.MaxInt }},
		{"negative index", func(_ *Schema, _ **Batch, r *[]int) { *r = []int{-1} }},
		{"large index", func(_ *Schema, _ **Batch, r *[]int) { *r = []int{4} }},
		{"missing fixed", func(_ *Schema, b **Batch, _ *[]int) { delete((*b).Int64s, "id") }},
		{"short fixed", func(_ *Schema, b **Batch, _ *[]int) { (*b).Int64s["id"].Values = nil }},
		{"short fixed bitmap", func(_ *Schema, b **Batch, _ *[]int) { (*b).Int64s["id"].Validity = Bitmap{} }},
		{"short string bitmap", func(_ *Schema, b **Batch, _ *[]int) { (*b).Strings["name"].Validity = Bitmap{} }},
		{"decreasing string offsets", func(_ *Schema, b **Batch, _ *[]int) { (*b).Strings["name"].Offsets[2] = 0 }},
		{"out of bounds string", func(_ *Schema, b **Batch, _ *[]int) { (*b).Strings["name"].Data = nil }},
		{"missing dictionary", func(_ *Schema, b **Batch, _ *[]int) { delete((*b).DictStrings, "kind") }},
		{"empty domain", func(_ *Schema, b **Batch, _ *[]int) { (*b).DictStrings["kind"].DictionaryDomain = "" }},
		{"empty dictionary offsets", func(_ *Schema, b **Batch, _ *[]int) { (*b).DictStrings["kind"].Offsets = nil }},
		{"decreasing dictionary offsets", func(_ *Schema, b **Batch, _ *[]int) { (*b).DictStrings["kind"].Offsets[1] = 0 }},
		{"bad selected dictionary code", func(_ *Schema, b **Batch, _ *[]int) { (*b).DictStrings["kind"].Indices[0] = 2 }},
		{"short dictionary bitmap", func(_ *Schema, b **Batch, _ *[]int) { (*b).DictStrings["kind"].Validity = Bitmap{} }},
		{"missing list", func(_ *Schema, b **Batch, _ *[]int) { delete((*b).StringLists, "tags") }},
		{"bad list parent offsets", func(_ *Schema, b **Batch, _ *[]int) { (*b).StringLists["tags"].Offsets[0] = 1 }},
		{"short list child bitmap", func(_ *Schema, b **Batch, _ *[]int) { (*b).StringLists["tags"].Values.Validity = Bitmap{} }},
		{"missing struct", func(_ *Schema, b **Batch, _ *[]int) { delete((*b).Structs, "profile") }},
		{"short struct bitmap", func(_ *Schema, b **Batch, _ *[]int) { (*b).Structs["profile"].Validity = Bitmap{} }},
		{"missing struct child", func(_ *Schema, b **Batch, _ *[]int) { delete((*b).Structs["profile"].Values.Float64s, "score") }},
		{"unsupported opaque", func(s *Schema, _ **Batch, _ *[]int) { s.Fields[0].Type = TypeArrow }},
		{"unsupported recursive struct", func(s *Schema, b **Batch, _ *[]int) {
			s.Fields[4].Children[0].Type = TypeStruct
			(*b).Structs["profile"].Fields[0].Type = TypeStruct
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema, batch, rows := valid()
			test.mutate(&schema, &batch, &rows)
			if _, err := EstimateColumnarRunSelectionBytes(schema, batch, rows); err == nil {
				t.Fatal("invalid input accepted")
			}
		})
	}
}

func TestColumnarRunSelectionEstimateArithmeticGuards(t *testing.T) {
	for _, n := range []int64{-1, math.MaxUint32, math.MaxInt64} {
		if _, err := selectionOffsetBytes(n); err == nil {
			t.Fatalf("offset row count accepted: %d", n)
		}
	}
	if int64(math.MaxInt) > math.MaxUint32 {
		got, err := selectionOffsetBytes(math.MaxUint32 - 1)
		if err != nil || got != 4+int64(math.MaxUint32)*4 {
			t.Fatal(got, err)
		}
		if _, err := selectionValidityBytes(int64(math.MaxUint32)*64 + 1); err == nil {
			t.Fatal("overflowing validity count accepted")
		}
	}
	for _, n := range []int64{-1, math.MaxInt64} {
		if _, err := selectionValidityBytes(n); err == nil {
			t.Fatalf("invalid validity count accepted: %d", n)
		}
	}
	for _, test := range [][2]int64{{-1, 1}, {1, 0}, {1, -1}, {math.MaxInt64, 16}} {
		if _, err := selectionFixedBytes(test[0], test[1]); err == nil {
			t.Fatal("invalid fixed-width size accepted", test)
		}
	}
	if got, err := selectionAddData(math.MaxUint32-1, 1); err != nil || got != math.MaxUint32 {
		t.Fatal(got, err)
	}
	for _, n := range []int64{-1, math.MaxUint32, math.MaxInt64} {
		if _, err := selectionAddData(n, 1); err == nil {
			t.Fatal("invalid string size accepted", n)
		}
	}
	if _, err := addRunSizes(math.MaxInt64, 1); err == nil {
		t.Fatal("total overflow accepted")
	}
}

// The test gather deliberately follows physical.takeRows rather than deriving
// codec sizes from the estimator. List and struct paths use production gathers.
func gatherSelectionForCodec(t *testing.T, schema Schema, src *Batch, rows []int) *Batch {
	t.Helper()
	dst := NewBatch()
	dst.Rows = len(rows)
	for _, field := range schema.Fields {
		name := field.Name
		switch field.Type {
		case TypeStruct:
			v := src.Structs[name]
			if len(rows) == 0 {
				dst.Structs[name] = &StructVector{Fields: v.Fields, Values: gatherSelectionForCodec(t, Schema{Fields: field.Children}, v.Values, rows)}
			} else {
				out, err := TakeStructRows(v, rows)
				if err != nil {
					t.Fatal(err)
				}
				dst.Structs[name] = out
			}
		case TypeList:
			v := &StringListVector{Offsets: []uint32{0}, Values: &StringViewVector{Offsets: []uint32{0}}}
			for _, row := range rows {
				if err := AppendStringListRow(v, src.StringLists[name], row); err != nil {
					t.Fatal(err)
				}
			}
			dst.StringLists[name] = v
		case TypeString, TypeBinary:
			if field.Encoding == EncodingDictionary {
				v := *src.DictStrings[name]
				v.Indices = selectionGatherValues(v.Indices, rows)
				v.Validity = selectionGatherValidity(v.Validity, rows)
				dst.DictStrings[name] = &v
			} else {
				source := src.Strings[name]
				v := &StringViewVector{Offsets: []uint32{0}, Validity: selectionGatherValidity(source.Validity, rows)}
				for _, row := range rows {
					v.Data = append(v.Data, source.ValueBytes(row)...)
					v.Offsets = append(v.Offsets, uint32(len(v.Data)))
				}
				dst.Strings[name] = v
			}
		case TypeInt8:
			v := *src.Int8s[name]
			v.Values, v.Validity = selectionGatherValues(v.Values, rows), selectionGatherValidity(v.Validity, rows)
			dst.Int8s[name] = &v
		case TypeInt16:
			v := *src.Int16s[name]
			v.Values, v.Validity = selectionGatherValues(v.Values, rows), selectionGatherValidity(v.Validity, rows)
			dst.Int16s[name] = &v
		case TypeInt32, TypeDate32, TypeTime32:
			v := *src.Int32s[name]
			v.Values, v.Validity = selectionGatherValues(v.Values, rows), selectionGatherValidity(v.Validity, rows)
			dst.Int32s[name] = &v
		case TypeInt64, TypeDate64, TypeTimestamp, TypeTime64, TypeDuration:
			v := *src.Int64s[name]
			v.Values, v.Validity = selectionGatherValues(v.Values, rows), selectionGatherValidity(v.Validity, rows)
			dst.Int64s[name] = &v
		case TypeUint8:
			v := *src.Uint8s[name]
			v.Values, v.Validity = selectionGatherValues(v.Values, rows), selectionGatherValidity(v.Validity, rows)
			dst.Uint8s[name] = &v
		case TypeUint16:
			v := *src.Uint16s[name]
			v.Values, v.Validity = selectionGatherValues(v.Values, rows), selectionGatherValidity(v.Validity, rows)
			dst.Uint16s[name] = &v
		case TypeUint32, TypeBool:
			v := *src.Uint32s[name]
			v.Values, v.Validity = selectionGatherValues(v.Values, rows), selectionGatherValidity(v.Validity, rows)
			dst.Uint32s[name] = &v
		case TypeUint64:
			v := *src.Uint64s[name]
			v.Values, v.Validity = selectionGatherValues(v.Values, rows), selectionGatherValidity(v.Validity, rows)
			dst.Uint64s[name] = &v
		case TypeFloat32:
			v := *src.Float32s[name]
			v.Values, v.Validity = selectionGatherValues(v.Values, rows), selectionGatherValidity(v.Validity, rows)
			dst.Float32s[name] = &v
		case TypeFloat64:
			v := *src.Float64s[name]
			v.Values, v.Validity = selectionGatherValues(v.Values, rows), selectionGatherValidity(v.Validity, rows)
			dst.Float64s[name] = &v
		case TypeDecimal:
			v := *src.Decimals[name]
			v.Values, v.Validity = selectionGatherValues(v.Values, rows), selectionGatherValidity(v.Validity, rows)
			dst.Decimals[name] = &v
		default:
			t.Fatalf("test gather does not support %s", field.Type)
		}
	}
	return dst
}

func selectionGatherValues[T any](values []T, rows []int) []T {
	result := make([]T, len(rows))
	for i, row := range rows {
		result[i] = values[row]
	}
	return result
}

func selectionGatherValidity(validity Bitmap, rows []int) Bitmap {
	if validity == nil {
		return nil
	}
	result := EnsureBitmap(nil, len(rows))
	for i, row := range rows {
		if IsSet(validity, row) {
			Set(result, i)
		}
	}
	return result
}

func assertSelectionPayload(t *testing.T, schema Schema, src *Batch, rows []int) int64 {
	t.Helper()
	estimate, err := EstimateColumnarRunSelectionBytes(schema, src, rows)
	if err != nil {
		t.Fatal(err)
	}
	gathered := gatherSelectionForCodec(t, schema, src, rows)
	defer gathered.ReleaseOwned()
	// Reserve exactly the estimate: an underestimated payload must fail here.
	manager := NewBudgetManager(estimate)
	encoded, err := EncodeColumnarRun(context.Background(), schema, gathered, manager)
	if err != nil {
		t.Fatalf("selection %v estimate=%d encode failed: %v", rows, estimate, err)
	}
	if estimate < int64(len(encoded.Payload)) {
		t.Fatalf("selection %v estimate=%d payload=%d", rows, estimate, len(encoded.Payload))
	}
	if err := encoded.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("encode reservation leaked %d bytes", manager.Used())
	}
	return estimate
}

func selectionFixedFixture(field Field) *Batch {
	b := NewBatch()
	b.Rows = 3
	validity := Bitmap{0b101}
	switch field.Type {
	case TypeInt8:
		b.Int8s[field.Name] = &Int8Vector{Values: []int8{-1, 2, 3}, Validity: validity}
	case TypeInt16:
		b.Int16s[field.Name] = &Int16Vector{Values: []int16{-1, 2, 3}, Validity: validity}
	case TypeInt32, TypeDate32, TypeTime32:
		b.Int32s[field.Name] = &Int32Vector{Values: []int32{-1, 2, 3}, Validity: validity}
	case TypeInt64, TypeDate64, TypeTimestamp, TypeTime64, TypeDuration:
		b.Int64s[field.Name] = &Int64Vector{Values: []int64{-1, 2, 3}, Validity: validity}
	case TypeUint8:
		b.Uint8s[field.Name] = &Uint8Vector{Values: []uint8{1, 2, 3}, Validity: validity}
	case TypeUint16:
		b.Uint16s[field.Name] = &Uint16Vector{Values: []uint16{1, 2, 3}, Validity: validity}
	case TypeUint32, TypeBool:
		b.Uint32s[field.Name] = &Uint32Vector{Values: []uint32{1, 0, 1}, Validity: validity}
	case TypeUint64:
		b.Uint64s[field.Name] = &Uint64Vector{Values: []uint64{1, 2, 3}, Validity: validity}
	case TypeFloat32:
		b.Float32s[field.Name] = &Float32Vector{Values: []float32{1, 2, 3}, Validity: validity}
	case TypeFloat64:
		b.Float64s[field.Name] = &Float64Vector{Values: []float64{1, 2, 3}, Validity: validity}
	case TypeDecimal:
		b.Decimals[field.Name] = &Decimal128Vector{Values: []Decimal128{{}, {}, {}}, Validity: validity, Precision: 10, Scale: 2}
	}
	return b
}
