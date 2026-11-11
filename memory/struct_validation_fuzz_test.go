package memory

import "testing"

// FuzzStructValidation exercises malformed shape combinations without allocating
// from attacker-controlled sizes. Any accepted vector must remain safe to gather.
func FuzzStructValidation(f *testing.F) {
	for _, seed := range [][]byte{{0, 65, 0, 0, 1}, {1, 2, 1, 0, 0}, {2, 2, 2, 1, 1}, {3, 0, 0, 0, 0}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 5 {
			return
		}
		typ := []DataType{TypeInt64, TypeString, TypeList}[int(data[0])%3]
		rows := int(data[1]) % 70
		v, field := structValidationFixture(typ, rows)
		bitmap := func(kind byte) Bitmap {
			switch kind % 4 {
			case 0:
				return nil
			case 1:
				return Bitmap{}
			case 2:
				return Bitmap{0}
			default:
				return Bitmap{^uint64(0), ^uint64(0)}
			}
		}
		v.Validity = bitmap(data[2])
		field.Children[0].Nullable = data[4]&1 == 0
		switch typ {
		case TypeInt64:
			child := v.Values.Int64s["child"]
			child.Validity = bitmap(data[3])
			if data[4]&2 != 0 && rows > 0 {
				child.Values = child.Values[:rows-1]
			}
		case TypeString:
			child := v.Values.Strings["child"]
			child.Validity = bitmap(data[3])
			for i, b := range data[5:] {
				child.Offsets[i%len(child.Offsets)] = uint32(b)
			}
		case TypeList:
			child := v.Values.StringLists["child"]
			child.Validity = bitmap(data[3])
			for i, b := range data[5:] {
				child.Offsets[i%len(child.Offsets)] = uint32(b)
			}
		}
		if err := v.Validate(field, rows); err != nil {
			return
		}
		selection := make([]int, rows)
		for i := range selection {
			selection[i] = rows - 1 - i
		}
		got, err := TakeStructRows(v, selection)
		if err != nil {
			t.Fatalf("accepted vector cannot be gathered: %v", err)
		}
		if err := got.Validate(field, rows); err != nil {
			t.Fatalf("gathered vector invalid: %v", err)
		}
	})
}
