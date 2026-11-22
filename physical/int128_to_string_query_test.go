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

func TestInt128ToStringSelectExprs(t *testing.T) {
	ctx := context.Background()
	schema := memory.NewSchema(
		memory.Field{Name: "i", Type: memory.TypeInt64, Nullable: true},
		memory.Field{Name: "u", Type: memory.TypeUint64},
	)
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		t.Run(mode.String(), func(t *testing.T) {
			b := memory.NewBatch()
			b.Rows = 5
			b.Int64s["i"] = &memory.Int64Vector{Values: []int64{math.MinInt64, math.MaxInt64, 0, -1, math.MinInt64}, Validity: memory.Bitmap{15}}
			b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{math.MaxUint64, math.MaxUint64, 10, 42, math.MaxUint64}}
			src, err := inmemory.New(schema, b)
			if err != nil {
				t.Fatal(err)
			}
			opts := engine.DefaultOptions()
			opts.KernelMode = mode
			s := lazy.NewSession(opts)
			defer s.Close()
			wide := expr.MulInt128(expr.Col("i"), expr.Col("u"))
			f := s.FromSource(src).Filter(expr.Not(expr.EQ(expr.Col("u"), expr.I64(10)))).SelectExprs(expr.Named(expr.Int128ToString(wide), "exact"))
			df, err := f.Collect(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer df.Close()
			fields := df.Schema().Fields
			if len(fields) != 1 || fields[0].Name != "exact" || fields[0].Type != memory.TypeString || !fields[0].Nullable {
				t.Fatalf("unexpected schema: %+v", fields)
			}
			c, err := df.Column("exact")
			if err != nil {
				t.Fatal(err)
			}
			got, valid, err := c.CopyStrings()
			want := []string{"-170141183460469231722463931679029329920", "170141183460469231704017187605319778305", "-42", ""}
			if err != nil || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(valid, memory.Bitmap{7}) {
				t.Fatalf("got=%q valid=%v err=%v", got, valid, err)
			}
		})
	}
}
