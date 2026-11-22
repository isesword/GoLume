package physical_test

import (
	"context"
	"math"
	"reflect"
	"testing"

	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/lazy"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

func TestInt128RangeFilterEndToEnd(t *testing.T) {
	ctx := context.Background()
	schema := memory.NewSchema(memory.Field{Name: "i", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "u", Type: memory.TypeUint64}, memory.Field{Name: "id", Type: memory.TypeInt64})
	b := memory.NewBatch()
	b.Rows = 6
	b.Int64s["i"] = &memory.Int64Vector{Values: []int64{0, 1, 2, 3, 2, -2}, Validity: memory.Bitmap{47}}
	b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64, 1}}
	b.Int64s["id"] = &memory.Int64Vector{Values: []int64{0, 1, 2, 3, 4, 5}}
	src, err := inmemory.New(schema, b)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		t.Run(mode.String(), func(t *testing.T) {
			opts := engine.DefaultOptions()
			opts.KernelMode = mode
			s := lazy.NewSession(opts)
			defer s.Close()
			wide := expr.AddInt128(expr.Col("i"), expr.Col("u"))
			frame := s.FromSource(src).
				Filter(expr.And(expr.GE(wide, expr.I128(1, 0)), expr.LT(wide, expr.I128(1, 2)))).
				SelectExprs(expr.Col("id").Alias("id"), expr.Named(expr.Int128ToString(wide), "exact"))
			df, err := frame.Collect(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer df.Close()
			id, err := df.Column("id")
			if err != nil {
				t.Fatal(err)
			}
			exact, err := df.Column("exact")
			if err != nil {
				t.Fatal(err)
			}
			ids, idvalid, err := id.CopyInt64()
			if err != nil || idvalid != nil || !reflect.DeepEqual(ids, []int64{1, 2}) {
				t.Fatalf("ids=%v validity=%v err=%v", ids, idvalid, err)
			}
			txt, valid, err := exact.CopyStrings()
			if err != nil || valid != nil || !reflect.DeepEqual(txt, []string{"18446744073709551616", "18446744073709551617"}) {
				t.Fatalf("values=%q validity=%v err=%v", txt, valid, err)
			}
		})
	}
}
