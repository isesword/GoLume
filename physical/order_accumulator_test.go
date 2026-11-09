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
	"unsafe"
)

type externalAccumulatorTestManager struct {
	memory.Manager
	deny          bool
	beforeRelease func()
}
type externalAccumulatorTestReservation struct {
	memory.Reservation
	owner *externalAccumulatorTestManager
}

func (m *externalAccumulatorTestManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	if m.deny {
		return nil, memory.ErrBudgetExceeded
	}
	r, err := m.Manager.Reserve(ctx, n)
	if err != nil {
		return nil, err
	}
	return &externalAccumulatorTestReservation{Reservation: r, owner: m}, nil
}
func (r *externalAccumulatorTestReservation) Release() {
	if r.owner.beforeRelease != nil {
		r.owner.beforeRelease()
	}
	r.Reservation.Release()
}

func externalAccumulatorTestWorkspace(t *testing.T, capacity int64) (*sortWorkspace, *externalAccumulatorTestManager) {
	t.Helper()
	parent := &externalAccumulatorTestManager{Manager: memory.NewBudgetManager(0)}
	w := &sortWorkspace{parent: parent}
	if err := w.ensure(context.Background(), capacity); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	return w, parent
}

func externalAccumulatorTestSchema() memory.Schema {
	fields := []memory.Field{}
	for _, typ := range []memory.DataType{
		memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeDate32, memory.TypeTime32,
		memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration,
		memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeBool, memory.TypeUint64,
		memory.TypeFloat32, memory.TypeFloat64,
	} {
		fields = append(fields, memory.Field{Name: typ.String(), Type: typ, Nullable: true})
	}
	fields = append(fields,
		memory.Field{Name: "decimal", Type: memory.TypeDecimal, Nullable: true, DecimalPrecision: 10, DecimalScale: 2},
		memory.Field{Name: "s", Type: memory.TypeString, Nullable: true},
		memory.Field{Name: "b", Type: memory.TypeBinary, Nullable: true},
		memory.Field{Name: "d", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingDictionary},
		memory.Field{Name: "db", Type: memory.TypeBinary, Nullable: true, Encoding: memory.EncodingDictionary},
		memory.ListStringField("l", true, true),
		memory.StructField("r", true, memory.Field{Name: "n", Type: memory.TypeInt8, Nullable: true}, memory.Field{Name: "s", Type: memory.TypeString, Nullable: true}, memory.ListStringField("l", true, true)),
	)
	return memory.NewSchema(fields...)
}

func TestExternalSortAccumulatorMetadataAdmission(t *testing.T) {
	ctx := context.Background()
	schema := externalAccumulatorTestSchema()
	metadata, err := externalSortAccumulatorMetadataBytes(schema)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := memory.ColumnarRunDecodeMetadataBytes(schema)
	if err != nil || metadata <= decoder {
		t.Fatal(metadata, decoder, err)
	}
	for _, delta := range []int64{-1, 0} {
		t.Run(fmt.Sprint(delta), func(t *testing.T) {
			w, parent := externalAccumulatorTestWorkspace(t, metadata+delta)
			parent.deny = true
			before := parent.Used()
			a, err := newExternalSortAccumulator(ctx, schema, w, 0)
			if delta == -1 {
				if !errors.Is(err, memory.ErrBudgetExceeded) || a != nil || w.Used() != 0 || parent.Used() != before {
					t.Fatal("metadata refusal allocated/charged owner", a, err, w.Used())
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer a.close()
			if w.Used() != metadata || a.reservation.Bytes() != metadata || a.capacityBytes != 0 {
				t.Fatal("incorrect exact metadata admission")
			}
			if a.batch.Opaque != nil || a.batch.StringLists["l"].Values == nil || a.batch.Structs["r"].Values == nil {
				t.Fatal("incorrect sparse graph")
			}
			if &a.batch.Structs["r"].Fields[0] == &schema.Fields[len(schema.Fields)-1].Children[0] {
				t.Fatal("struct field array borrowed")
			}
			if &a.schema.Fields[0] != &schema.Fields[0] {
				t.Fatal("schema was copied")
			}
		})
	}
	if allocations := testing.AllocsPerRun(100, func() {
		if _, err := externalSortAccumulatorMetadataBytes(schema); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatal("metadata estimator allocates", allocations)
	}
}

func TestExternalSortAccumulatorMetadataRejectsUnsupportedBeforeReserve(t *testing.T) {
	schemas := []memory.Schema{
		{Fields: []memory.Field{{Name: "x", Type: memory.TypeArrow}}},
		{Fields: []memory.Field{{Name: "x", Type: memory.TypeInt8}, {Name: "x", Type: memory.TypeInt16}}},
		{Fields: []memory.Field{memory.StructField("r", true, memory.StructField("nested", true, memory.Field{Name: "x", Type: memory.TypeInt8}))}},
		{Fields: []memory.Field{memory.StructField("r", true, memory.Field{Name: "d", Type: memory.TypeString, Encoding: memory.EncodingDictionary})}},
		{Fields: []memory.Field{{Name: "d", Type: memory.TypeDecimal, DecimalPrecision: 39}}},
	}
	for _, schema := range schemas {
		w, parent := externalAccumulatorTestWorkspace(t, 0)
		if a, err := newExternalSortAccumulator(context.Background(), schema, w, 0); err == nil || a != nil || parent.Used() != 0 {
			t.Fatal("invalid schema allocated owner", a, err, parent.Used())
		}
	}
}

func externalAccumulatorTestCapacity(v reflect.Value) int64 {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return 0
		}
		return externalAccumulatorTestCapacity(v.Elem())
	}
	switch v.Kind() {
	case reflect.Map:
		var n int64
		it := v.MapRange()
		for it.Next() {
			n += externalAccumulatorTestCapacity(it.Value())
		}
		return n
	case reflect.Struct:
		var n int64
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).Name != "Fields" {
				n += externalAccumulatorTestCapacity(v.Field(i))
			}
		}
		return n
	case reflect.Slice:
		return int64(v.Cap()) * int64(v.Type().Elem().Size())
	}
	return 0
}

func TestExternalSortAccumulatorWideInt8ExactCapacity(t *testing.T) {
	const columns = 2048
	fields := make([]memory.Field, columns)
	src := memory.NewBatch()
	src.Rows = 1
	for i := range fields {
		name := fmt.Sprintf("c%d", i)
		fields[i] = memory.Field{Name: name, Type: memory.TypeInt8}
		src.Int8s[name] = &memory.Int8Vector{Values: []int8{7}}
	}
	schema := memory.NewSchema(fields...)
	metadata, err := externalSortAccumulatorMetadataBytes(schema)
	if err != nil {
		t.Fatal(err)
	}
	if metadata < int64(columns)*(512+int64(unsafe.Sizeof(memory.Int8Vector{}))) {
		t.Fatal("wide metadata undercharged", metadata)
	}
	w, _ := externalAccumulatorTestWorkspace(t, 0)
	a, err := newExternalSortAccumulator(context.Background(), schema, w, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	for rows := 1; rows <= 9; rows++ {
		if err := a.appendRow(context.Background(), src, 0); err != nil {
			t.Fatal(err)
		}
		expected := 1
		for expected < rows {
			expected *= 2
		}
		for _, v := range a.batch.Int8s {
			if cap(v.Values) != expected {
				t.Fatal("implicit small-slice capacity", rows, cap(v.Values), expected)
			}
		}
		if a.capacityBytes != int64(columns*expected) || a.capacityBytes != externalAccumulatorTestCapacity(reflect.ValueOf(a.batch)) {
			t.Fatal("capacity accounting mismatch", a.capacityBytes)
		}
		if a.reservation.Bytes() < metadata+a.capacityBytes {
			t.Fatal("live capacity exceeds token")
		}
	}
	if a.batch.Int16s != nil || a.batch.Uint32s != nil || a.batch.DictStrings != nil || a.batch.Structs != nil || a.batch.Opaque != nil {
		t.Fatal("unused maps allocated")
	}
}

func externalAccumulatorTestSource(schema memory.Schema) *memory.Batch {
	b := memory.NewBatch()
	b.Rows = 3
	for _, f := range schema.Fields {
		switch f.Type {
		case memory.TypeInt8:
			b.Int8s[f.Name] = &memory.Int8Vector{Values: []int8{7, 8, 9}, Validity: memory.Bitmap{5}}
		case memory.TypeInt16:
			b.Int16s[f.Name] = &memory.Int16Vector{Values: []int16{7, 8, 9}, Validity: memory.Bitmap{5}}
		case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
			b.Int32s[f.Name] = &memory.Int32Vector{Values: []int32{7, 8, 9}, Validity: memory.Bitmap{5}}
		case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
			b.Int64s[f.Name] = &memory.Int64Vector{Values: []int64{7, 8, 9}, Validity: memory.Bitmap{5}}
		case memory.TypeUint8:
			b.Uint8s[f.Name] = &memory.Uint8Vector{Values: []uint8{7, 8, 9}, Validity: memory.Bitmap{5}}
		case memory.TypeUint16:
			b.Uint16s[f.Name] = &memory.Uint16Vector{Values: []uint16{7, 8, 9}, Validity: memory.Bitmap{5}}
		case memory.TypeUint32:
			b.Uint32s[f.Name] = &memory.Uint32Vector{Values: []uint32{7, 8, 9}, Validity: memory.Bitmap{5}}
		case memory.TypeUint64:
			b.Uint64s[f.Name] = &memory.Uint64Vector{Values: []uint64{7, 8, 9}, Validity: memory.Bitmap{5}}
		case memory.TypeFloat32:
			b.Float32s[f.Name] = &memory.Float32Vector{Values: []float32{7, 8, 9}, Validity: memory.Bitmap{5}}
		case memory.TypeFloat64:
			b.Float64s[f.Name] = &memory.Float64Vector{Values: []float64{7, 8, 9}, Validity: memory.Bitmap{5}}
		case memory.TypeBool:
			b.Uint32s[f.Name] = &memory.Uint32Vector{Values: []uint32{1, 0, 1}, Validity: memory.Bitmap{5}}
		case memory.TypeDecimal:
			b.Decimals[f.Name] = &memory.Decimal128Vector{Values: make([]memory.Decimal128, 3), Validity: memory.Bitmap{5}, Precision: f.DecimalPrecision, Scale: f.DecimalScale}
		case memory.TypeString, memory.TypeBinary:
			if f.Encoding == memory.EncodingDictionary {
				b.DictStrings[f.Name] = &memory.DictionaryStringVector{Indices: []uint32{0, 0, 1}, Offsets: []uint32{1, 2, 3}, Data: []byte("!ab?"), Validity: memory.Bitmap{5}, DictionaryDomain: "first", Encoding: memory.EncodingDictionary}
			} else {
				b.Strings[f.Name] = &memory.StringViewVector{Offsets: []uint32{0, 1, 7, 8}, Data: []byte("aHIDDENb"), Validity: memory.Bitmap{5}, Encoding: memory.EncodingView}
			}
		case memory.TypeList:
			b.StringLists[f.Name] = &memory.StringListVector{Offsets: []uint32{0, 2, 3, 4}, Validity: memory.Bitmap{5}, Encoding: memory.EncodingView,
				Values: &memory.StringViewVector{Offsets: []uint32{0, 1, 7, 13, 14}, Data: []byte("aHIDDENPARENTb"), Validity: memory.Bitmap{13}, Encoding: memory.EncodingView}}
		case memory.TypeStruct:
			child := memory.NewBatch()
			child.Rows = 3
			child.Int8s["n"] = &memory.Int8Vector{Values: []int8{7, 99, 9}}
			child.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 7, 8}, Data: []byte("aHIDDENb"), Encoding: memory.EncodingView}
			child.StringLists["l"] = &memory.StringListVector{Offsets: []uint32{0, 1, 2, 3}, Encoding: memory.EncodingView,
				Values: &memory.StringViewVector{Offsets: []uint32{0, 1, 7, 8}, Data: []byte("aHIDDENb"), Encoding: memory.EncodingView}}
			b.Structs[f.Name] = &memory.StructVector{Rows: 3, Fields: f.Children, Values: child, Validity: memory.Bitmap{5}, Encoding: memory.EncodingView}
		}
	}
	return b
}

func TestExternalSortAccumulatorAllPhysicalTypesAndNulls(t *testing.T) {
	ctx := context.Background()
	schema := externalAccumulatorTestSchema()
	src := externalAccumulatorTestSource(schema)
	w, _ := externalAccumulatorTestWorkspace(t, 0)
	a, err := newExternalSortAccumulator(ctx, schema, w, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	for _, row := range []int{2, 1, 0} {
		if err := a.appendRow(ctx, src, row); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.finish(ctx); err != nil {
		t.Fatal(err)
	}
	if a.batch.Rows != 3 || a.capacityBytes != externalAccumulatorTestCapacity(reflect.ValueOf(a.batch)) {
		t.Fatal("rows/capacity mismatch")
	}
	for name, v := range a.batch.Int8s {
		if !reflect.DeepEqual(v.Values, []int8{9, 8, 7}) || v.Validity[0] != 5 {
			t.Fatal(name, v)
		}
	}
	for _, f := range schema.Fields {
		switch f.Type {
		case memory.TypeInt16:
			if v := a.batch.Int16s[f.Name]; !reflect.DeepEqual(v.Values, []int16{9, 8, 7}) || v.Validity[0] != 5 {
				t.Fatal(f.Name, v)
			}
		case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
			if v := a.batch.Int32s[f.Name]; !reflect.DeepEqual(v.Values, []int32{9, 8, 7}) || v.Validity[0] != 5 {
				t.Fatal(f.Name, v)
			}
		case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
			if v := a.batch.Int64s[f.Name]; !reflect.DeepEqual(v.Values, []int64{9, 8, 7}) || v.Validity[0] != 5 {
				t.Fatal(f.Name, v)
			}
		case memory.TypeUint8:
			if v := a.batch.Uint8s[f.Name]; !reflect.DeepEqual(v.Values, []uint8{9, 8, 7}) || v.Validity[0] != 5 {
				t.Fatal(f.Name, v)
			}
		case memory.TypeUint16:
			if v := a.batch.Uint16s[f.Name]; !reflect.DeepEqual(v.Values, []uint16{9, 8, 7}) || v.Validity[0] != 5 {
				t.Fatal(f.Name, v)
			}
		case memory.TypeUint32:
			if v := a.batch.Uint32s[f.Name]; !reflect.DeepEqual(v.Values, []uint32{9, 8, 7}) || v.Validity[0] != 5 {
				t.Fatal(f.Name, v)
			}
		case memory.TypeUint64:
			if v := a.batch.Uint64s[f.Name]; !reflect.DeepEqual(v.Values, []uint64{9, 8, 7}) || v.Validity[0] != 5 {
				t.Fatal(f.Name, v)
			}
		case memory.TypeFloat32:
			if v := a.batch.Float32s[f.Name]; !reflect.DeepEqual(v.Values, []float32{9, 8, 7}) || v.Validity[0] != 5 {
				t.Fatal(f.Name, v)
			}
		case memory.TypeFloat64:
			if v := a.batch.Float64s[f.Name]; !reflect.DeepEqual(v.Values, []float64{9, 8, 7}) || v.Validity[0] != 5 {
				t.Fatal(f.Name, v)
			}
		}
	}
	for _, name := range []string{"s", "b"} {
		v := a.batch.Strings[name]
		if string(v.Data) != "bHIDDENa" || v.IsValid(1) {
			t.Fatal("top-level hidden bytes lost", name, v)
		}
	}
	for _, name := range []string{"d", "db"} {
		v := a.batch.DictStrings[name]
		if string(v.Data) != "ab" || !reflect.DeepEqual(v.Offsets, []uint32{0, 1, 2}) || !reflect.DeepEqual(v.Indices, []uint32{1, 0, 0}) || v.DictionaryDomain != "first" {
			t.Fatal("dictionary window not copied", name, v)
		}
	}
	l := a.batch.StringLists["l"]
	if err := l.Validate(3); err != nil {
		t.Fatal(err)
	}
	if string(l.Values.Data) != "ba" || !reflect.DeepEqual(l.Offsets, []uint32{0, 1, 1, 3}) || !reflect.DeepEqual(l.Values.Offsets, []uint32{0, 1, 2, 2}) || l.IsValid(1) || l.Values.IsValid(2) {
		t.Fatal("list canonical NULLs lost", l, l.Values)
	}
	r := a.batch.Structs["r"]
	if err := r.Validate(schema.Fields[len(schema.Fields)-1], 3); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.Values.Int8s["n"].Values, []int8{9, 0, 7}) || string(r.Values.Strings["s"].Data) != "ba" || string(r.Values.StringLists["l"].Values.Data) != "ba" {
		t.Fatal("struct canonical NULLs lost", r.Values)
	}
	// The internal builder supports decimal coefficients without changing the
	// public Sort's separate eligibility rules.
	if a.batch.Decimals["decimal"].Precision != 10 || a.batch.Decimals["decimal"].Scale != 2 || a.batch.Decimals["decimal"].IsValid(1) {
		t.Fatal("decimal metadata/NULL lost")
	}
	if a.batch.Uint32s["bool"].Values[0] != 1 || a.batch.Uint32s["bool"].Validity[0] != 5 {
		t.Fatal("bool not preserved")
	}
	encoded, err := memory.EncodeColumnarRun(ctx, schema, a.batch, memory.NewBudgetManager(0))
	if err != nil {
		t.Fatal("accumulator output rejected by codec", err)
	}
	encoded.Close()
}

func TestExternalSortAccumulatorDictionaryRemapAndIdentity(t *testing.T) {
	ctx := context.Background()
	schema := memory.NewSchema(memory.Field{Name: "d", Type: memory.TypeString, Encoding: memory.EncodingDictionary})
	w, _ := externalAccumulatorTestWorkspace(t, 0)
	a, err := newExternalSortAccumulator(ctx, schema, w, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	batch := memory.NewBatch()
	batch.Rows = 1
	first := &memory.DictionaryStringVector{Indices: []uint32{1}, Offsets: []uint32{2, 3, 4, 5}, Data: []byte("!!aba??"), DictionaryDomain: "same", Encoding: memory.EncodingDictionary}
	batch.DictStrings["d"] = first
	if err := a.appendRow(ctx, batch, 0); err != nil {
		t.Fatal(err)
	}
	out := a.batch.DictStrings["d"]
	if string(out.Data) != "aba" || !reflect.DeepEqual(out.Offsets, []uint32{0, 1, 2, 3}) || a.domainBytes != 4 {
		t.Fatal("first table window/identity", out, a.domainBytes)
	}
	// A bogus identical domain and changed code ordering must use actual bytes.
	batch.DictStrings["d"] = &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{0, 1, 2}, Data: []byte("ba"), DictionaryDomain: "same"}
	if err := a.appendRow(ctx, batch, 0); err != nil {
		t.Fatal(err)
	}
	if out.Indices[1] != 1 || string(out.Data) != "aba" {
		t.Fatal("trusted false identity")
	}
	// New domains repeating one value must not grow the union dictionary.
	for i := 0; i < 20; i++ {
		batch.DictStrings["d"] = &memory.DictionaryStringVector{Indices: []uint32{1}, Offsets: []uint32{0, 1, 2}, Data: []byte("ca"), DictionaryDomain: fmt.Sprint(i)}
		if err := a.appendRow(ctx, batch, 0); err != nil {
			t.Fatal(err)
		}
	}
	if string(out.Data) != "aba" || len(out.Offsets) != 4 {
		t.Fatal("duplicate codes amplified table")
	}
	batch.DictStrings["d"] = &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{0, 1}, Data: []byte("c"), DictionaryDomain: "new"}
	if err := a.appendRow(ctx, batch, 0); err != nil {
		t.Fatal(err)
	}
	if out.DictionaryDomain != "" || a.domainBytes != 0 || string(out.Data) != "abac" {
		t.Fatal("mutation retained stale identity", out, a.domainBytes)
	}
	if err := a.finish(ctx); err != nil {
		t.Fatal(err)
	}
	expected := &memory.DictionaryStringVector{Offsets: out.Offsets, Data: out.Data}
	finishSortDictionary(expected)
	if out.DictionaryDomain != expected.DictionaryDomain || len(out.DictionaryDomain) != 64 || a.domainBytes != 64 {
		t.Fatal("content identity mismatch")
	}
	if a.reservation.Bytes() < a.metadataBytes+a.capacityBytes+64+externalSortDictionaryFinishScratch {
		t.Fatal("hash/domain scratch not charged")
	}
	if first.DictionaryDomain != "same" || string(first.Data) != "!!aba??" || first.Offsets[0] != 2 {
		t.Fatal("source dictionary mutated")
	}
}

func TestExternalSortAccumulatorRefusesBeforeBufferAllocation(t *testing.T) {
	ctx := context.Background()
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt8})
	metadata, err := externalSortAccumulatorMetadataBytes(schema)
	if err != nil {
		t.Fatal(err)
	}
	// The final two-byte buffer would fit, but old(1)+new(2) must coexist.
	// Admission must reject this two-byte allowance before replacing the pointer.
	w, parent := externalAccumulatorTestWorkspace(t, metadata+2)
	parent.deny = true
	a, err := newExternalSortAccumulator(ctx, schema, w, 1)
	if err != nil {
		t.Fatal(err)
	}
	src := memory.NewBatch()
	src.Rows = 1
	src.Int8s["x"] = &memory.Int8Vector{Values: []int8{42}}
	if err := a.appendRow(ctx, src, 0); err != nil {
		t.Fatal(err)
	}
	v, batch, maps, token := a.batch.Int8s["x"], a.batch, a.batch.Int8s, a.reservation
	ptr := &v.Values[0]
	before := parent.Used()
	if err := a.appendRow(ctx, src, 0); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if cap(v.Values) != 1 || len(v.Values) != 1 || &v.Values[0] != ptr || a.batch.Rows != 1 || a.capacityBytes != 1 || token.Bytes() != metadata+1 || parent.Used() != before {
		t.Fatal("refused growth allocated or dropped live storage")
	}
	w.close()
	if parent.Used() == 0 {
		t.Fatal("failure released live graph")
	}
	parent.beforeRelease = func() {
		if a.batch != nil || a.schema.Fields != nil || a.reservation != nil || batch.Int8s != nil || len(maps) != 0 || v.Values != nil || v.Validity != nil {
			t.Error("released reservation before clearing graph")
		}
	}
	a.close()
	a.close()
	if parent.Used() != 0 || w.Used() != 0 {
		t.Fatal("failed owner leaked accounting")
	}
}

func TestExternalSortAccumulatorLateNullValidityAndGrowthRefusal(t *testing.T) {
	ctx := context.Background()
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt8, Nullable: true})
	metadata, err := externalSortAccumulatorMetadataBytes(schema)
	if err != nil {
		t.Fatal(err)
	}
	w, parent := externalAccumulatorTestWorkspace(t, metadata+1)
	parent.deny = true
	a, err := newExternalSortAccumulator(ctx, schema, w, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	src := memory.NewBatch()
	src.Rows = 1
	src.Int8s["x"] = &memory.Int8Vector{Values: []int8{1}}
	if err := a.appendRow(ctx, src, 0); err != nil {
		t.Fatal(err)
	}
	src.Int8s["x"].Validity = memory.Bitmap{0}
	before := a.capacityBytes
	if err := a.appendRow(ctx, src, 0); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if a.batch.Int8s["x"].Validity != nil || a.capacityBytes != before || len(a.batch.Int8s["x"].Values) != 1 {
		t.Fatal("late bitmap allocated before admission")
	}
	a.close()
	parent.deny = false
	a, err = newExternalSortAccumulator(ctx, schema, w, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	src.Int8s["x"].Validity = nil
	for i := 0; i < 130; i++ {
		if err := a.appendRow(ctx, src, 0); err != nil {
			t.Fatal(err)
		}
	}
	src.Int8s["x"].Validity = memory.Bitmap{0}
	if err := a.appendRow(ctx, src, 0); err != nil {
		t.Fatal(err)
	}
	v := a.batch.Int8s["x"]
	if len(v.Validity) != 3 || cap(v.Validity) != 3 || v.IsValid(130) {
		t.Fatal("late bitmap wrong shape")
	}
	for i := 0; i < 130; i++ {
		if !v.IsValid(i) {
			t.Fatal("historical validity lost", i)
		}
	}
	for i := 131; i < 257; i++ {
		if err := a.appendRow(ctx, src, 0); err != nil {
			t.Fatal(err)
		}
	}
	if cap(v.Validity) != 6 || len(v.Validity) != 5 {
		t.Fatal("validity failed to reuse capacity", len(v.Validity), cap(v.Validity))
	}
}

func TestExternalSortAccumulatorDomainAdmissionAndFinishRefusal(t *testing.T) {
	ctx := context.Background()
	schema := memory.NewSchema(memory.Field{Name: "d", Type: memory.TypeString, Encoding: memory.EncodingDictionary})
	metadata, err := externalSortAccumulatorMetadataBytes(schema)
	if err != nil {
		t.Fatal(err)
	}
	w, parent := externalAccumulatorTestWorkspace(t, metadata)
	parent.deny = true
	a, err := newExternalSortAccumulator(ctx, schema, w, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	src := memory.NewBatch()
	src.Rows = 1
	src.DictStrings["d"] = &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{0, 1}, Data: []byte("a"), DictionaryDomain: strings.Repeat("x", 1024)}
	if err := a.appendRow(ctx, src, 0); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	v := a.batch.DictStrings["d"]
	if v.DictionaryDomain != "" || v.Offsets != nil || v.Data != nil || a.domainBytes != 0 || a.capacityBytes != 0 {
		t.Fatal("borrowed domain before payment")
	}
	if err := a.finish(ctx); !errors.Is(err, errExternalSortAccumulatorFailed) {
		t.Fatal("failed owner reused", err)
	}
	a.close()
	parent.deny = false
	a, err = newExternalSortAccumulator(ctx, schema, w, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	v = a.batch.DictStrings["d"]
	src.DictStrings["d"].DictionaryDomain = ""
	if err := a.appendRow(ctx, src, 0); err != nil {
		t.Fatal(err)
	}
	parent.deny = true
	before, capacity := a.reservation.Bytes(), a.capacityBytes
	if err := a.finish(ctx); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if v.DictionaryDomain != "" || a.domainBytes != 0 || a.reservation.Bytes() != before || a.capacityBytes != capacity {
		t.Fatal("finish allocated identity before admission")
	}
	if err := a.appendRow(ctx, src, 0); !errors.Is(err, errExternalSortAccumulatorFailed) {
		t.Fatal("failed owner reused", err)
	}
	if err := a.finish(ctx); !errors.Is(err, errExternalSortAccumulatorFailed) {
		t.Fatal("failed finish retried", err)
	}
	a.close()
	parent.deny = false
	a, err = newExternalSortAccumulator(ctx, schema, w, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	if err := a.appendRow(ctx, src, 0); err != nil {
		t.Fatal(err)
	}
	if err := a.finish(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestExternalSortAccumulatorCloseClearsNestedAliasesOnly(t *testing.T) {
	ctx := context.Background()
	schema := externalAccumulatorTestSchema()
	src := externalAccumulatorTestSource(schema)
	w, parent := externalAccumulatorTestWorkspace(t, 0)
	a, err := newExternalSortAccumulator(ctx, schema, w, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.appendRow(ctx, src, 0); err != nil {
		t.Fatal(err)
	}
	if err := a.finish(ctx); err != nil {
		t.Fatal(err)
	}
	batch, topMaps := a.batch, a.batch.Structs
	r := batch.Structs["r"]
	fields, children, childMaps, scalar := r.Fields, r.Values, r.Values.Int8s, r.Values.Int8s["n"]
	list, listChild := batch.StringLists["l"], batch.StringLists["l"].Values
	dictionary := batch.DictStrings["d"]
	sourceFields := schema.Fields[len(schema.Fields)-1].Children
	w.close()
	released := false
	parent.beforeRelease = func() {
		released = true
		if a.batch != nil || a.schema.Fields != nil || batch.Structs != nil || len(topMaps) != 0 || r.Values != nil || r.Fields != nil || children.Int8s != nil || len(childMaps) != 0 || scalar.Values != nil || list.Values != nil || listChild.Data != nil || dictionary.DictionaryDomain != "" || dictionary.Data != nil {
			t.Error("nested graph remained reachable at release")
		}
		for _, f := range fields {
			if !reflect.DeepEqual(f, memory.Field{}) {
				t.Error("owned outer Fields not cleared")
			}
		}
	}
	a.close()
	if !released || w.Used() != 0 || parent.Used() != 0 {
		t.Fatal("close did not return accounting")
	}
	if sourceFields[0].Name != "n" || len(sourceFields[2].Children) != 1 || sourceFields[2].Children[0].Name != "item" || src.Structs["r"].Values.Int8s["n"].Values[0] != 7 || src.DictStrings["d"].DictionaryDomain != "first" {
		t.Fatal("close destroyed borrowed source state")
	}
}

func TestExternalSortAccumulatorCheckedArithmeticAndFloor(t *testing.T) {
	for _, tc := range [][2]int{{-1, 0}, {0, -1}, {int(^uint(0) >> 1), int(^uint(0) >> 1)}} {
		if _, err := externalSortBufferCapacity(tc[0], tc[1]); err == nil {
			t.Fatal("unchecked capacity", tc)
		}
	}
	if _, err := externalSortBufferBytes(2, uintptr(math.MaxInt64)); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if _, err := externalSortBufferBytes(-1, 1); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	limit := uint64(math.MaxUint32)
	if uint64(int(^uint(0)>>1)) < limit {
		limit = uint64(int(^uint(0) >> 1))
	}
	if got, err := externalSortBufferCapacity(int(limit/2+1), int(limit/2+2)); err != nil || got != int(limit/2+2) {
		t.Fatal("representable growth rejected", got, err)
	}
	for _, tc := range []struct{ old, need, want int }{{0, 1, 1}, {1, 2, 2}, {2, 3, 4}, {4, 4, 4}, {4, 10, 10}} {
		got, err := externalSortBufferCapacity(tc.old, tc.need)
		if err != nil || got != tc.want {
			t.Fatal(tc, got, err)
		}
	}
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt8})
	w, parent := externalAccumulatorTestWorkspace(t, 0)
	for _, floor := range []int64{-1, math.MaxInt64} {
		if a, err := newExternalSortAccumulator(context.Background(), schema, w, floor); !errors.Is(err, memory.ErrBudgetExceeded) || a != nil || parent.Used() != 0 {
			t.Fatal("unchecked floor", a, err)
		}
	}
	metadata, _ := externalSortAccumulatorMetadataBytes(schema)
	a, err := newExternalSortAccumulator(context.Background(), schema, w, 7777)
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	if a.reservation.Bytes() != metadata+7777 {
		t.Fatal("payload lower bound lost")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := a.finish(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestExternalSortAccumulatorEmptyOffsetsArePrepaid(t *testing.T) {
	ctx := context.Background()
	schema := memory.NewSchema(
		memory.Field{Name: "s", Type: memory.TypeString, Nullable: true},
		memory.Field{Name: "d", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingDictionary},
		memory.ListStringField("l", true, true),
		memory.StructField("r", true, memory.ListStringField("nested", true, true)),
	)
	metadata, err := externalSortAccumulatorMetadataBytes(schema)
	if err != nil {
		t.Fatal(err)
	}
	w, parent := externalAccumulatorTestWorkspace(t, metadata)
	parent.deny = true
	a, err := newExternalSortAccumulator(ctx, schema, w, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.finish(ctx); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if a.capacityBytes != 0 || a.batch.DictStrings["d"].Offsets != nil {
		t.Fatal("empty dictionary sentinel was not prepaid")
	}
	a.close()
	parent.deny = false
	a, err = newExternalSortAccumulator(ctx, schema, w, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	if err := a.finish(ctx); err != nil {
		t.Fatal(err)
	}
	buffers := [][]uint32{a.batch.Strings["s"].Offsets, a.batch.DictStrings["d"].Offsets,
		a.batch.StringLists["l"].Offsets, a.batch.StringLists["l"].Values.Offsets,
		a.batch.Structs["r"].Values.StringLists["nested"].Offsets,
		a.batch.Structs["r"].Values.StringLists["nested"].Values.Offsets}
	for _, offsets := range buffers {
		if len(offsets) != 1 || cap(offsets) != 1 || offsets[0] != 0 {
			t.Fatal("empty offset shape", offsets)
		}
	}
	if a.capacityBytes != int64(4*len(buffers)) || a.capacityBytes != externalAccumulatorTestCapacity(reflect.ValueOf(a.batch)) {
		t.Fatal("empty offset charge", a.capacityBytes)
	}
	if a.batch.DictStrings["d"].DictionaryDomain == "" || a.domainBytes != 64 {
		t.Fatal("empty dictionary identity missing")
	}
}

func TestExternalSortAccumulatorFirstNullDictionaryHasOwnedSentinel(t *testing.T) {
	ctx := context.Background()
	schema := memory.NewSchema(memory.Field{Name: "d", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingDictionary})
	w, _ := externalAccumulatorTestWorkspace(t, 0)
	a, err := newExternalSortAccumulator(ctx, schema, w, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	src := memory.NewBatch()
	src.Rows = 1
	src.DictStrings["d"] = &memory.DictionaryStringVector{Indices: []uint32{math.MaxUint32}, Validity: memory.Bitmap{0}}
	if err := a.appendRow(ctx, src, 0); err != nil {
		t.Fatal(err)
	}
	v := a.batch.DictStrings["d"]
	if len(v.Offsets) != 1 || cap(v.Offsets) != 1 || v.Offsets[0] != 0 || len(v.Data) != 0 || v.IsValid(0) {
		t.Fatal("empty NULL dictionary shape", v)
	}
	if err := a.finish(ctx); err != nil {
		t.Fatal(err)
	}
	if err := v.Validate(1); err != nil {
		t.Fatal(err)
	}
}

func TestExternalSortAccumulatorMalformedRowsFailTerminal(t *testing.T) {
	cases := []struct {
		name   string
		field  memory.Field
		source func(*memory.Batch)
	}{
		{"missing scalar", memory.Field{Name: "x", Type: memory.TypeInt8}, func(b *memory.Batch) {}},
		{"short scalar", memory.Field{Name: "x", Type: memory.TypeInt8}, func(b *memory.Batch) { b.Int8s["x"] = &memory.Int8Vector{} }},
		{"short validity", memory.Field{Name: "x", Type: memory.TypeInt8}, func(b *memory.Batch) { b.Int8s["x"] = &memory.Int8Vector{Values: []int8{1}, Validity: memory.Bitmap{}} }},
		{"string offsets", memory.Field{Name: "x", Type: memory.TypeString}, func(b *memory.Batch) {
			b.Strings["x"] = &memory.StringViewVector{Offsets: []uint32{2, 1}, Data: []byte("a")}
		}},
		{"list offsets", memory.ListStringField("x", true, true), func(b *memory.Batch) {
			b.StringLists["x"] = &memory.StringListVector{Offsets: []uint32{0, 2}, Values: &memory.StringViewVector{Offsets: []uint32{0}}}
		}},
		{"missing struct children", memory.StructField("x", true, memory.Field{Name: "n", Type: memory.TypeInt8}), func(b *memory.Batch) { b.Structs["x"] = &memory.StructVector{Rows: 1} }},
		{"dictionary code", memory.Field{Name: "x", Type: memory.TypeString, Encoding: memory.EncodingDictionary}, func(b *memory.Batch) {
			b.DictStrings["x"] = &memory.DictionaryStringVector{Indices: []uint32{1}, Offsets: []uint32{0, 1}, Data: []byte("a")}
		}},
		{"dictionary window", memory.Field{Name: "x", Type: memory.TypeString, Encoding: memory.EncodingDictionary}, func(b *memory.Batch) {
			b.DictStrings["x"] = &memory.DictionaryStringVector{Indices: []uint32{0}, Validity: memory.Bitmap{0}, Offsets: []uint32{1, 0}, Data: []byte("a")}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, _ := externalAccumulatorTestWorkspace(t, 0)
			a, err := newExternalSortAccumulator(context.Background(), memory.NewSchema(tc.field), w, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer a.close()
			src := memory.NewBatch()
			src.Rows = 1
			tc.source(src)
			if err := a.appendRow(context.Background(), src, 0); err == nil {
				t.Fatal("malformed row accepted")
			}
			token := a.reservation.Bytes()
			if !a.failed {
				t.Fatal("failure was not terminal")
			}
			if err := a.appendRow(context.Background(), src, 0); !errors.Is(err, errExternalSortAccumulatorFailed) {
				t.Fatal(err)
			}
			if err := a.finish(context.Background()); !errors.Is(err, errExternalSortAccumulatorFailed) {
				t.Fatal(err)
			}
			if a.reservation.Bytes() != token || w.Used() != token {
				t.Fatal("failed graph lost reservation")
			}
		})
	}
}
