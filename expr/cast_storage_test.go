package expr

import (
	"gocolumn/memory"
	"math"
	"testing"
)

func TestCheckedCastStringStorageContract(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		typ        memory.DataType
	}{{"numeric", "7", memory.TypeInt64}, {"date", "2000-01-01", memory.TypeDate32}} {
		for _, dictionary := range []bool{false, true} {
			t.Run(tc.name+map[bool]string{false: "/view", true: "/dictionary"}[dictionary], func(t *testing.T) {
				field := memory.Field{Name: "s", Type: memory.TypeString, Nullable: true}
				if dictionary {
					field.Encoding = memory.EncodingDictionary
				}
				schema := memory.NewSchema(field, memory.Field{Name: "id", Type: memory.TypeInt64})
				b := memory.NewBatch()
				b.Rows = 2
				b.Int64s["id"] = &memory.Int64Vector{Values: []int64{0, 1}}
				if dictionary {
					b.DictStrings["s"] = &memory.DictionaryStringVector{Indices: []uint32{math.MaxUint32, 0}, Offsets: []uint32{0, uint32(len(tc.text))}, Data: []byte(tc.text)}
				} else {
					b.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{uint32(len(tc.text) + 1), 0, uint32(len(tc.text))}, Data: []byte(tc.text)}
				}
				p, err := Compile(schema, TryCastTo(Col("s"), tc.typ))
				if err != nil {
					t.Fatal(err)
				}
				var w evaluationState
				defer w.Close()
				if _, err := evaluateValueTest(p, b, &w, testKernelSet()); err == nil {
					t.Fatal("TRY swallowed live malformed storage")
				}
				guarded := If(EQ(Col("id"), I64(1)), TryCastTo(Col("s"), tc.typ), Null(tc.typ))
				p, err = Compile(schema, guarded)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := evaluateValueTest(p, b, &w, testKernelSet()); err != nil {
					t.Fatal("guard read invalid inactive row", err)
				}
				if dictionary {
					b.DictStrings["s"].Validity = memory.Bitmap{2}
				} else {
					b.Strings["s"].Validity = memory.Bitmap{2}
				}
				p, _ = Compile(schema, TryCastTo(Col("s"), tc.typ))
				if _, err := evaluateValueTest(p, b, &w, testKernelSet()); err != nil {
					t.Fatal("NULL read invalid payload", err)
				}
			})
		}
	}
}
