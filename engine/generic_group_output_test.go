package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"testing"
)

func TestGenericGroupNativeOutputRoundtrip(t *testing.T) {
	types := []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64, memory.TypeBool, memory.TypeDate32, memory.TypeDate64, memory.TypeTime32, memory.TypeTime64, memory.TypeTimestamp, memory.TypeDuration, memory.TypeString, memory.TypeBinary}
	for _, typ := range types {
		t.Run(typ.String(), func(t *testing.T) {
			field := memory.Field{Name: "x", Type: typ, Nullable: true}
			dst := memory.NewBatch()
			if e := allocateGenericGroupColumn(dst, field, 2, 3); e != nil {
				t.Fatal(e)
			}
			dst.Rows = 2
			bits := uint64(42)
			if typ == memory.TypeBool {
				bits = 1
			}
			if typ == memory.TypeUint64 {
				bits = math.MaxUint64
			}
			if e := setGenericGroupColumn(dst, field, 0, true, bits, []byte{0, 255, 1}); e != nil {
				t.Fatal(e)
			}
			if e := setGenericGroupColumn(dst, field, 1, false, 0, nil); e != nil {
				t.Fatal(e)
			}
			binding, e := bindCanonicalGroupKeys([]memory.Field{field}, dst)
			if e != nil {
				t.Fatal(e)
			}
			key := canonicalTestKey(t, binding, 0)
			e = visitCanonicalGroupKey(key, []memory.Field{field}, func(_ int, _ memory.DataType, valid bool, got uint64, data []byte) error {
				if !valid {
					t.Fatal("valid")
				}
				if typ == memory.TypeString || typ == memory.TypeBinary {
					if len(data) != 3 || data[1] != 255 {
						t.Fatal("bytes")
					}
				} else if got != bits {
					t.Fatalf("bits %x want %x", got, bits)
				}
				return nil
			})
			if e != nil {
				t.Fatal(e)
			}
			shrinkAggregateOutput(dst, memory.NewSchema(field), 1)
			if dst.Rows != 1 {
				t.Fatal("shrink")
			}
		})
	}
}
func TestGenericGroupOutputMultipleStringBudget(t *testing.T) {
	for _, limit := range []int64{200, 1000} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			root := memory.NewEngineBudget(limit)
			budget, e := root.BeginQuery(memory.BudgetOptions{})
			if e != nil {
				t.Fatal(e)
			}
			defer budget.Close()
			fields := []memory.Field{{Name: "a", Type: memory.TypeString}, {Name: "b", Type: memory.TypeString}}
			src := memory.NewBatch()
			src.Rows = 1
			src.Strings["a"] = &memory.StringViewVector{Offsets: []uint32{0, 40}, Data: make([]byte, 40)}
			src.Strings["b"] = &memory.StringViewVector{Offsets: []uint32{0, 40}, Data: make([]byte, 40)}
			binding, e := bindCanonicalGroupKeys(fields, src)
			if e != nil {
				t.Fatal(e)
			}
			key := canonicalTestKey(t, binding, 0)
			dst := memory.NewBatch()
			for _, f := range fields {
				if e := allocateGenericGroupColumn(dst, f, 2, 0); e != nil {
					t.Fatal(e)
				}
			}
			s := &sortedAggregateStream{query: &PreparedQuery{cq: CompiledQuery{GroupShape: GroupShapeGeneric, GenericFields: fields}}, outputBudget: budget.Operator("out", memory.MemoryOutput)}
			if e = s.ensureGenericOutputStrings(context.Background(), dst, key); e != nil {
				t.Fatal(e)
			}
			if root.Used() != 128 {
				t.Fatal("first capacity", root.Used())
			}
			for _, f := range fields {
				if e = setGenericGroupColumn(dst, f, 0, true, 0, make([]byte, 40)); e != nil {
					t.Fatal(e)
				}
			}
			e = s.ensureGenericOutputStrings(context.Background(), dst, key)
			if limit == 200 {
				if !errors.Is(e, memory.ErrBudgetExceeded) {
					t.Fatalf("expected overlap failure: %v", e)
				}
				if root.Used() != 128 || cap(dst.Strings["a"].Data) != 64 || cap(dst.Strings["b"].Data) != 64 {
					t.Fatal("failed growth mutated buffers")
				}
			} else {
				if e != nil {
					t.Fatal(e)
				}
				if root.Used() != 256 {
					t.Fatal("combined capacity", root.Used())
				}
			}
			s.releaseOutput()
			if root.Used() != 0 {
				t.Fatal("leaked strings", root.Used())
			}
		})
	}
}

func TestGenericGroupMultipleStringStreams(t *testing.T) {
	for _, strategy := range []string{"resident", "sorted", "spill"} {
		t.Run(strategy, func(t *testing.T) {
			batch := memory.NewBatch()
			batch.Rows = 3
			batch.Strings["a"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2, 4}, Data: []byte("xxzz")}
			batch.DictStrings["b"] = &memory.DictionaryStringVector{Indices: []uint32{0, 0, 1}, Offsets: []uint32{0, 3, 4}, Data: []byte("abcq")}
			batch.Int64s["time"] = &memory.Int64Vector{Values: []int64{10, 10, 20}}
			fields := []memory.Field{{Name: "a", Type: memory.TypeString, Encoding: memory.EncodingView}, {Name: "b", Type: memory.TypeString, Encoding: memory.EncodingDictionary}, {Name: "time", Type: memory.TypeTimestamp, ArrowType: "timestamp[ns, tz=UTC]"}}
			src, e := inmemory.New(memory.NewSchema(fields...), batch)
			if e != nil {
				t.Fatal(e)
			}
			src.WithOrdering(source.OrderingColumn{Name: "a"}, source.OrderingColumn{Name: "b"}, source.OrderingColumn{Name: "time"})
			opts := streamTestOptions()
			opts.MemoryLimitBytes = 1 << 20
			if strategy == "spill" {
				opts.ExecutionPolicy = ExecutionBounded
				opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20}
			}
			eng := New(opts)
			defer eng.Close()
			p, e := eng.Prepare(context.Background(), src, expr.Query{GroupBy: []string{"a", "b", "time"}, Aggregates: []expr.Aggregate{expr.Count("n")}}, PrepareOptions{})
			if e != nil {
				t.Fatal(e)
			}
			defer p.Close()
			schema := p.resultSchema()
			if f, _ := schema.Field("time"); f.ArrowType != fields[2].ArrowType {
				t.Fatal("temporal metadata lost")
			}
			if f, _ := schema.Field("b"); f.Encoding != memory.EncodingView || f.DictionaryDomain != "" {
				t.Fatal("dictionary not normalized")
			}
			budget, e := eng.BeginQueryMemory()
			if e != nil {
				t.Fatal(e)
			}
			defer budget.Close()
			ctx := memory.WithQueryBudget(context.Background(), budget)
			var stream source.BatchStream
			switch strategy {
			case "resident":
				stream, e = p.NewResidentAggregateStream(ctx, schema, 1)
			case "sorted":
				stream, e = p.NewSortedAggregateStream(ctx, schema, 1)
			case "spill":
				stream, e = p.NewSpillableAggregateStream(ctx, schema, 1)
			}
			if e != nil {
				t.Fatal(e)
			}
			defer stream.Close()
			dst := memory.NewBatch()
			groups := 0
			for {
				ok, e := stream.Next(ctx, dst)
				if e != nil {
					t.Fatal(e)
				}
				if !ok {
					break
				}
				if dst.Rows != 1 {
					t.Fatal("chunk", dst.Rows)
				}
				a, b := string(dst.Strings["a"].Data), string(dst.Strings["b"].Data)
				if !((a == "x" && b == "abc") || (a == "zz" && b == "q")) {
					t.Fatalf("bad strings %q %q", a, b)
				}
				if len(dst.Int64s["time"].Values) != 1 {
					t.Fatal("temporal shrink")
				}
				groups++
			}
			if groups != 2 {
				t.Fatal("groups", groups)
			}
			if e = stream.Close(); e != nil {
				t.Fatal(e)
			}
			if budget.Used() != 0 {
				t.Fatal("leak", budget.Used())
			}
		})
	}
}
