package memory

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

func structValidationFixture(typ DataType, rows int) (*StructVector, Field) {
	child := Field{Name: "child", Type: typ, Nullable: true}
	b := NewBatch()
	b.Rows = rows
	switch typ {
	case TypeInt8:
		b.Int8s["child"] = &Int8Vector{Values: make([]int8, rows)}
	case TypeInt16:
		b.Int16s["child"] = &Int16Vector{Values: make([]int16, rows)}
	case TypeInt32, TypeDate32, TypeTime32:
		b.Int32s["child"] = &Int32Vector{Values: make([]int32, rows)}
	case TypeInt64, TypeDate64, TypeTimestamp, TypeTime64, TypeDuration:
		b.Int64s["child"] = &Int64Vector{Values: make([]int64, rows)}
	case TypeUint8:
		b.Uint8s["child"] = &Uint8Vector{Values: make([]uint8, rows)}
	case TypeUint16:
		b.Uint16s["child"] = &Uint16Vector{Values: make([]uint16, rows)}
	case TypeUint32, TypeBool:
		b.Uint32s["child"] = &Uint32Vector{Values: make([]uint32, rows)}
	case TypeUint64:
		b.Uint64s["child"] = &Uint64Vector{Values: make([]uint64, rows)}
	case TypeFloat32:
		b.Float32s["child"] = &Float32Vector{Values: make([]float32, rows)}
	case TypeFloat64:
		b.Float64s["child"] = &Float64Vector{Values: make([]float64, rows)}

	case TypeString:
		b.Strings["child"] = &StringViewVector{Offsets: make([]uint32, rows+1)}
	case TypeList:
		child = ListStringField("child", true, true)
		b.StringLists["child"] = &StringListVector{Offsets: make([]uint32, rows+1), Values: &StringViewVector{Offsets: []uint32{0}}}
	}
	field := StructField("record", true, child)
	return &StructVector{Rows: rows, Fields: field.Children, Values: b}, field
}

func structValidationPhysical(v *StructVector) reflect.Value {
	b := reflect.ValueOf(v.Values).Elem()
	for i := 0; i < b.NumField(); i++ {
		m := b.Field(i)
		if m.Kind() == reflect.Map {
			child := m.MapIndex(reflect.ValueOf("child"))
			if child.IsValid() && !child.IsNil() {
				return child.Elem()
			}
		}
	}
	panic("fixture has no child")
}

func requireStructValidationError(t *testing.T, v *StructVector, f Field) {
	t.Helper()
	defer func() {
		if p := recover(); p != nil {
			t.Fatalf("Validate panicked instead of returning error: %v", p)
		}
	}()
	if err := v.Validate(f, v.Rows); err == nil {
		t.Fatal("malformed struct accepted")
	}
}

func TestStructValidationRejectsMalformedScalarStorage(t *testing.T) {
	for _, typ := range []DataType{TypeInt8, TypeInt16, TypeInt32, TypeInt64, TypeUint8, TypeUint16, TypeUint32, TypeUint64, TypeFloat32, TypeFloat64, TypeBool, TypeDate32, TypeDate64, TypeTimestamp, TypeTime32, TypeTime64, TypeDuration} {
		for _, kind := range []string{"short", "long", "nil-values", "empty-validity", "short-validity"} {
			for _, nullable := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/%s/nullable=%t", typ, kind, nullable), func(t *testing.T) {
					v, f := structValidationFixture(typ, 65)
					f.Children[0].Nullable = nullable
					child := structValidationPhysical(v)
					switch kind {
					case "short":
						child.FieldByName("Values").SetLen(64)
					case "long":
						child.FieldByName("Values").Set(reflect.MakeSlice(child.FieldByName("Values").Type(), 66, 66))
					case "nil-values":
						child.FieldByName("Values").SetZero()
					case "empty-validity":
						child.FieldByName("Validity").Set(reflect.ValueOf(Bitmap{}))
					case "short-validity":
						child.FieldByName("Validity").Set(reflect.ValueOf(Bitmap{math.MaxUint64}))
					}
					requireStructValidationError(t, v, f)
				})
			}
		}
	}
}

func TestStructValidationRejectsMalformedVariableStorage(t *testing.T) {
	tests := []struct {
		name   string
		typ    DataType
		change func(*StructVector)
	}{
		{"string-short", TypeString, func(v *StructVector) { v.Values.Strings["child"].Offsets = []uint32{0, 0} }},
		{"string-decreasing", TypeString, func(v *StructVector) {
			v.Values.Strings["child"].Offsets = []uint32{0, 1, 0}
			v.Values.Strings["child"].Data = []byte("a")
		}},
		{"string-out-of-bounds", TypeString, func(v *StructVector) { v.Values.Strings["child"].Offsets = []uint32{0, 0, 2} }},
		{"string-empty-validity", TypeString, func(v *StructVector) { v.Values.Strings["child"].Validity = Bitmap{} }},
		{"list-nil", TypeList, func(v *StructVector) { v.Values.StringLists["child"] = nil }},
		{"list-bad-validity", TypeList, func(v *StructVector) { v.Values.StringLists["child"].Validity = Bitmap{} }},
		{"list-bad-child-validity", TypeList, func(v *StructVector) {
			l := v.Values.StringLists["child"]
			l.Offsets = []uint32{0, 1, 1}
			l.Values.Offsets = []uint32{0, 0}
			l.Values.Validity = Bitmap{}
		}},
	}
	for _, tt := range tests {
		for _, hidden := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/hidden=%t", tt.name, hidden), func(t *testing.T) {
				v, f := structValidationFixture(tt.typ, 2)
				if hidden {
					v.Validity = Bitmap{0}
				}
				tt.change(v)
				requireStructValidationError(t, v, f)
			})
		}
	}
}

func TestStructValidationRejectsMalformedParent(t *testing.T) {
	for _, kind := range []string{"negative-rows", "empty-validity", "short-validity"} {
		t.Run(kind, func(t *testing.T) {
			v, f := structValidationFixture(TypeInt64, 65)
			switch kind {
			case "negative-rows":
				v.Rows = -1
				v.Values.Rows = -1
			case "empty-validity":
				v.Validity = Bitmap{}
			case "short-validity":
				v.Validity = Bitmap{math.MaxUint64}
			}
			requireStructValidationError(t, v, f)
		})
	}
}

func structValidationDictionary() (*StructVector, Field) {
	v, f := structValidationFixture(TypeString, 2)
	delete(v.Values.Strings, "child")
	v.Values.DictStrings["child"] = &DictionaryStringVector{Indices: []uint32{0, 1}, Offsets: []uint32{1, 2, 3}, Data: []byte("_ab"), DictionaryDomain: "local"}
	return v, f
}

func TestStructValidationDictionaryStorage(t *testing.T) {
	for _, kind := range []string{"short", "empty-validity", "decreasing", "out-of-bounds", "missing-offsets", "bad-code"} {
		t.Run(kind, func(t *testing.T) {
			v, f := structValidationDictionary()
			d := v.Values.DictStrings["child"]
			switch kind {
			case "short":
				d.Indices = d.Indices[:1]
			case "empty-validity":
				d.Validity = Bitmap{}
			case "decreasing":
				d.Offsets = []uint32{2, 1, 3}
			case "out-of-bounds":
				d.Offsets[2] = 4
			case "missing-offsets":
				d.Offsets = nil
			case "bad-code":
				d.Indices[1] = math.MaxUint32
			}
			requireStructValidationError(t, v, f)
		})
	}
	for _, parentNull := range []bool{false, true} {
		v, f := structValidationDictionary()
		d := v.Values.DictStrings["child"]
		d.Indices[1] = math.MaxUint32
		if parentNull {
			v.Validity = Bitmap{1}
		} else {
			d.Validity = Bitmap{1}
		}
		if err := v.Validate(f, 2); err != nil {
			t.Fatalf("hidden code rejected: %v", err)
		}
	}
	// Runtime string storage takes precedence over a shadowed dictionary map.
	v, f := structValidationDictionary()
	v.Values.DictStrings["child"].Indices = nil
	v.Values.Strings["child"] = &StringViewVector{Offsets: []uint32{0, 0, 0}}
	if err := v.Validate(f, 2); err != nil {
		t.Fatal(err)
	}
}

func TestStructValidationPreservesVisibleNullability(t *testing.T) {
	for _, hiddenBy := range []string{"neither", "struct", "list"} {
		t.Run(hiddenBy, func(t *testing.T) {
			v, f := structValidationFixture(TypeList, 2)
			f.Children[0].Children[0].Nullable = false
			l := v.Values.StringLists["child"]
			l.Offsets = []uint32{0, 1, 2}
			l.Values.Offsets = []uint32{0, 1, 1}
			l.Values.Data = []byte("a")
			l.Values.Validity = Bitmap{1}
			if hiddenBy == "struct" {
				v.Validity = Bitmap{1}
			} else if hiddenBy == "list" {
				l.Validity = Bitmap{1}
			}
			err := v.Validate(f, 2)
			if hiddenBy == "neither" {
				if err == nil {
					t.Fatal("visible NULL required list element accepted")
				}
			} else if err != nil {
				t.Fatalf("hidden NULL rejected: %v", err)
			}
		})
	}
	for _, typ := range []DataType{TypeInt64, TypeString, TypeList} {
		v, f := structValidationFixture(typ, 2)
		f.Children[0].Nullable = false
		structValidationPhysical(v).FieldByName("Validity").Set(reflect.ValueOf(Bitmap{1}))
		v.Validity = Bitmap{1}
		if err := v.Validate(f, 2); err != nil {
			t.Fatal(err)
		}
		v.Validity = nil
		requireStructValidationError(t, v, f)
	}
}

func TestStructValidationAcceptsLogicalStorageBoundaries(t *testing.T) {
	for _, rows := range []int{0, 1, 63, 64, 65} {
		for _, typ := range []DataType{TypeInt64, TypeString, TypeList} {
			v, f := structValidationFixture(typ, rows)
			if err := v.Validate(f, rows); err != nil {
				t.Fatalf("%s/%d: %v", typ, rows, err)
			}
			valid := make(Bitmap, WordsFor(rows)+1)
			for i := range valid {
				valid[i] = math.MaxUint64
			}
			v.Validity = valid
			structValidationPhysical(v).FieldByName("Validity").Set(reflect.ValueOf(valid))
			if err := v.Validate(f, rows); err != nil {
				t.Fatal(err)
			}
		}
	}
	v, f := structValidationFixture(TypeString, 2)
	v.Values.Strings["child"] = &StringViewVector{Offsets: []uint32{2, 3, 4}, Data: []byte("__ab_unused")}
	if err := v.Validate(f, 2); err != nil {
		t.Fatalf("valid offset view rejected: %v", err)
	}
	v, f = structValidationFixture(TypeString, 0)
	v.Values.Strings["child"].Offsets = nil
	if err := v.Validate(f, 0); err != nil {
		t.Fatal(err)
	}
}

func TestStructValidationCodecRejectsMalformedChildrenWithoutCreditLeak(t *testing.T) {
	v, f := structValidationFixture(TypeString, 2)
	v.Values.Strings["child"].Offsets = []uint32{0, 2, 1}
	v.Values.Strings["child"].Data = []byte("ab")
	b := NewBatch()
	b.Rows = 2
	b.Structs["record"] = v
	budget := NewBudgetManager(1 << 20)
	encoded, err := EncodeColumnarRun(context.Background(), NewSchema(f), b, budget)
	if encoded != nil {
		encoded.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "child") {
		t.Fatalf("expected child validation error, got %v", err)
	}
	if budget.Used() != 0 {
		t.Fatalf("leaked %d bytes", budget.Used())
	}
}
