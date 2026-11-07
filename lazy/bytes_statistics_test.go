package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

func TestBytesEndpointsDictionaryAndEmpty(t *testing.T) {
	for _, empty := range []bool{false, true} {
		schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Encoding: memory.EncodingDictionary, DictionaryDomain: "test", Nullable: true}, memory.Field{Name: "b", Type: memory.TypeBinary})
		var batches []*memory.Batch
		if !empty {
			b := memory.NewBatch()
			b.Rows = 3
			b.DictStrings["s"] = &memory.DictionaryStringVector{Indices: []uint32{1, 0, 2}, Offsets: []uint32{0, 1, 3, 3}, Data: []byte("abc"), DictionaryDomain: "test", Encoding: memory.EncodingDictionary}
			b.Strings["b"] = &memory.StringViewVector{Offsets: []uint32{0, 2, 2, 3}, Data: []byte{0, 255, 0}}
			batches = []*memory.Batch{b}
		}
		src, err := inmemory.New(schema, batches...)
		if err != nil {
			t.Fatal(err)
		}
		sess := NewSession(engine.Options{MemoryLimitBytes: 1 << 20, ExecutionPolicy: engine.ExecutionBounded})
		q := sess.FromSource(src).Agg(First("s", "first"), Last("s", "last"), First("b", "binary"))
		outSchema, err := q.Schema()
		if err != nil {
			t.Fatal(err)
		}
		if outSchema.Fields[0].Encoding == memory.EncodingDictionary || outSchema.Fields[2].Type != memory.TypeBinary {
			t.Fatal(outSchema)
		}
		df, err := q.Collect(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		sess.Close()
		out := df.batches()[0]
		if out.Rows != 1 {
			t.Fatal(out.Rows)
		}
		for _, name := range []string{"first", "last", "binary"} {
			if out.Strings[name].IsValid(0) == empty {
				t.Fatal("empty validity", name)
			}
		}
		if !empty {
			batches[0].DictStrings["s"].Data[1] = 'X'
			if string(out.Strings["first"].ValueBytes(0)) != "bc" || string(out.Strings["last"].ValueBytes(0)) != "" || string(out.Strings["binary"].ValueBytes(0)) != string([]byte{0, 255}) {
				t.Fatal("dictionary bytes ownership")
			}
		}
		df.Close()
	}
}
