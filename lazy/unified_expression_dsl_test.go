package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func TestLazyUnifiedConstructorsFluentAlias(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		t.Run(mode.String(), func(t *testing.T) {
			opts := engine.DefaultOptions()
			opts.KernelMode = mode
			session := NewSession(opts)
			defer session.Close()
			frame := session.FromSource(expressionDomainSource(t)).SelectExprs(
				Bool(true).Alias("constant"),
				Null(memory.TypeBool).Alias("missing"),
				If(Col("keep"), Cast(Col("x"), memory.TypeInt8), Lit(0)).Alias("chosen"),
				Case([]expr.WhenThen{{When: Col("keep"), Then: Lit(10)}}, Lit(20)).Alias("case_value"),
				Coalesce(Col("keep"), Bool(true)).Alias("known"),
				In(Col("x"), Lit(1), Null(memory.TypeInt64)).Alias("member"),
				expr.Not(In(Col("x"), Lit(1), Null(memory.TypeInt64))).Alias("not_member"),
			)
			data, err := frame.Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer data.Close()
			for name, want := range map[string][]any{
				"constant":   {true, true, true},
				"missing":    {nil, nil, nil},
				"chosen":     {int64(1), int64(0), int64(0)},
				"case_value": {int64(10), int64(20), int64(20)},
				"known":      {true, false, true},
				"member":     {true, nil, nil},
				"not_member": {false, nil, nil},
			} {
				column, err := data.Column(name)
				if err != nil {
					t.Fatal(err)
				}
				got, err := column.ToAnySlice()
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("%s=%v want=%v err=%v", name, got, want, err)
				}
			}
			filtered, err := frame.Filter(Col("member")).Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer filtered.Close()
			if rows, columns := filtered.Shape(); rows != 1 || columns != 7 {
				t.Fatalf("projected alias filter shape=(%d,%d)", rows, columns)
			}
		})
	}
}
