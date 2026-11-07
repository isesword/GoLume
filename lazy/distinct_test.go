package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

func TestDistinctColumnarStringsAndTemporal(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 5
	b.Strings["k"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2, 3, 4, 5}, Data: []byte("aaaaa")}
	b.Strings["v"] = &memory.StringViewVector{Offsets: []uint32{0, 0, 0, 1, 2, 3}, Data: []byte{0, 0, 255}, Validity: memory.Bitmap{0b11101}}
	b.Int32s["date"] = &memory.Int32Vector{Values: []int32{1, 2, 2, 3, 3}}
	schema := memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeString}, memory.Field{Name: "v", Type: memory.TypeBinary, Nullable: true}, memory.Field{Name: "date", Type: memory.TypeDate32})
	src, err := inmemory.New(schema, b)
	if err != nil {
		t.Fatal(err)
	}
	sess := NewSession(engine.Options{MemoryLimitBytes: 1 << 20, ExecutionPolicy: engine.ExecutionBounded, Spill: engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 8 << 20}})
	defer sess.Close()
	q := sess.FromSource(src).GroupBy("k").Agg(NUnique("v", "u"), NUnique("date", "days"), Count("n"))
	outSchema, err := q.Schema()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range outSchema.Fields[1:] {
		if f.Type != memory.TypeUint64 || f.Nullable {
			t.Fatal(f)
		}
	}
	df, err := q.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	out := df.batches()[0]
	if out.Rows != 1 || out.Uint64s["u"].Values[0] != 4 || out.Uint64s["days"].Values[0] != 3 || out.Uint64s["n"].Values[0] != 5 {
		t.Fatal(out)
	}
}
