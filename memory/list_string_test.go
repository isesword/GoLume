package memory

import "testing"

func TestStringListVectorValidateAndAppendPreserveNestedNulls(t *testing.T) {
	src := testStringListVector()
	if err := src.Validate(4); err != nil {
		t.Fatal(err)
	}
	if start, end, ok := src.ValueRange(0); !ok || start != 0 || end != 3 {
		t.Fatalf("row 0 range=(%d,%d,%v)", start, end, ok)
	}
	if _, _, ok := src.ValueRange(1); ok {
		t.Fatal("NULL list must not expose a value range")
	}
	if start, end, ok := src.ValueRange(2); !ok || start != end {
		t.Fatalf("empty row range=(%d,%d,%v)", start, end, ok)
	}

	dst := &StringListVector{}
	for _, row := range []int{3, 0, 1, 2} {
		if err := AppendStringListRow(dst, src, row); err != nil {
			t.Fatal(err)
		}
	}
	if err := dst.Validate(4); err != nil {
		t.Fatal(err)
	}
	requireStringListRow(t, dst, 0, true, []string{""}, []bool{true})
	requireStringListRow(t, dst, 1, true, []string{"a", "", "中"}, []bool{true, false, true})
	requireStringListRow(t, dst, 2, false, nil, nil)
	requireStringListRow(t, dst, 3, true, nil, nil)
}

func TestStringListVectorRejectsMalformedOffsets(t *testing.T) {
	for name, mutate := range map[string]func(*StringListVector){
		"missing parent offset": func(v *StringListVector) { v.Offsets = v.Offsets[:4] },
		"nonzero base":          func(v *StringListVector) { v.Offsets[0] = 1 },
		"decreasing parent":     func(v *StringListVector) { v.Offsets[2] = 2 },
		"terminal mismatch":     func(v *StringListVector) { v.Offsets[4] = 3 },
		"decreasing child":      func(v *StringListVector) { v.Values.Offsets[2] = 0 },
		"child byte overflow":   func(v *StringListVector) { v.Values.Offsets[4] = 99 },
	} {
		t.Run(name, func(t *testing.T) {
			v, err := CloneBatch(&Batch{Rows: 4, StringLists: map[string]*StringListVector{"x": testStringListVector()}, Int64s: map[string]*Int64Vector{}, Int32s: map[string]*Int32Vector{}, Uint32s: map[string]*Uint32Vector{}, Uint64s: map[string]*Uint64Vector{}, Float64s: map[string]*Float64Vector{}, Strings: map[string]*StringViewVector{}, DictStrings: map[string]*DictionaryStringVector{}, Opaque: map[string]*OpaqueVector{}})
			if err != nil {
				t.Fatal(err)
			}
			list := v.StringLists["x"]
			mutate(list)
			if err := list.Validate(4); err == nil {
				t.Fatal("malformed vector should fail validation")
			}
		})
	}
}

func testStringListVector() *StringListVector {
	parentValidity := EnsureBitmap(nil, 4)
	Set(parentValidity, 0)
	Set(parentValidity, 2)
	Set(parentValidity, 3)
	childValidity := EnsureBitmap(nil, 4)
	Set(childValidity, 0)
	Set(childValidity, 2)
	Set(childValidity, 3)
	return &StringListVector{
		Offsets:  []uint32{0, 3, 3, 3, 4},
		Validity: parentValidity,
		Encoding: EncodingView,
		Values: &StringViewVector{
			Offsets:  []uint32{0, 1, 1, 4, 4},
			Data:     []byte("a中"),
			Validity: childValidity,
			Encoding: EncodingView,
		},
	}
}

func requireStringListRow(t *testing.T, v *StringListVector, row int, valid bool, values []string, elementValid []bool) {
	t.Helper()
	if v.IsValid(row) != valid {
		t.Fatalf("row %d valid=%v want=%v", row, v.IsValid(row), valid)
	}
	if !valid {
		return
	}
	start, end, ok := v.ValueRange(row)
	if !ok || end-start != len(values) {
		t.Fatalf("row %d range=(%d,%d,%v) values=%d", row, start, end, ok, len(values))
	}
	for i, want := range values {
		child := start + i
		if v.Values.IsValid(child) != elementValid[i] {
			t.Fatalf("row %d child %d valid=%v want=%v", row, i, v.Values.IsValid(child), elementValid[i])
		}
		if elementValid[i] && string(v.Values.ValueBytes(child)) != want {
			t.Fatalf("row %d child %d=%q want=%q", row, i, v.Values.ValueBytes(child), want)
		}
	}
}
