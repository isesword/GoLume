package expr

import (
	"context"
	"errors"
	"gocolumn/kernel"
	"gocolumn/memory"
	"testing"
)

func TestN7ConstructAllLegacyTypesWithArray(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		schema, _, args := constructFixture(0, true)
		ns, _ := nestedListExprFixture(t, false, memory.TypeInt64, 0)
		schema = memory.NewSchema(append(schema.Fields, ns.Fields[0])...)
		args = append(args, Col("xs").Alias("nested"))
		e := StructFrom(args...)
		p, err := CompileExpressions(schema, []NamedExpr{e.Alias("one"), e.Alias("same")})
		if err != nil {
			t.Fatal(err)
		}
		if p.outputs[0].root != p.outputs[1].root {
			t.Fatal("CSE")
		}
		m := memory.NewBudgetManager(0)
		w, err := NewWorkspace(context.Background(), p, m)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range []int{0, 1, 65, 7, 65, 0} {
			_, b, _ := constructFixture(n, true)
			_, nb := nestedListExprFixture(t, false, memory.TypeInt64, n)
			b.Arrays["xs"] = nb.Arrays["xs"]
			out, err := w.Evaluate(context.Background(), b, kernel.New(mode))
			if err != nil {
				t.Fatal(err)
			}
			if len(out[0].Struct.Fields) != 21 {
				t.Fatal("old20 lost")
			}
			for i, k := range structGetKinds {
				structGetAssert(t, k, constructChildValue(out[0].Struct, k, args[i].Alias), n, false, true)
			}
			if out[0].Struct.Values.Arrays["nested"] == b.Arrays["xs"] {
				t.Fatal("aliased Array")
			}
			if m.Used() != w.MemoryBytes() {
				t.Fatalf("ledger %d != %d", m.Used(), w.MemoryBytes())
			}
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("leak")
		}
	}
}

func TestN7ConstructDepthAndProfiles(t *testing.T) {
	for _, d := range []int{1, 7, 8} {
		s, _ := recursiveBFixture(t, d, 0)
		_, err := Compile(s, StructFrom(Col("root").Alias("wrapped")))
		if (err != nil) != (d == 8) {
			t.Fatalf("depth%d err%v", d, err)
		}
	}
	for _, text := range []bool{false, true} {
		s, _ := nestedListExprFixture(t, text, memory.TypeInt64, 0)
		if _, err := Compile(s, StructFrom(Col("xs").Alias("x"))); err != nil {
			t.Fatal(err)
		}
	}
	s, _ := listStructExprFixture(t, memory.TypeInt64, 0)
	if _, err := Compile(s, StructFrom(Col("xs").Alias("x"))); err != nil {
		t.Fatal(err)
	}
	if _, err := Compile(memory.NewSchema(), StructFrom(StructFrom(Named(I64(1), "inner")).Alias("nested"))); err != nil {
		t.Fatal(err)
	}
}

func TestN7ConstructExtendedFailurePreservesOwner(t *testing.T) {
	s, b := recursiveBFixture(t, 2, 65)
	f := s.Fields[0]
	f.Name = "child"
	in := scalarRegister{typ: memory.TypeStruct, strct: b.Structs["root"], validity: b.Structs["root"].Validity}
	m := memory.NewBudgetManager(0)
	out := scalarRegister{typ: memory.TypeStruct, acquire: func(n int64) (memory.Reservation, error) { return m.Reserve(context.Background(), n) }}
	if err := evalStructFrom(&out, []*scalarRegister{&in}, []int{0}, []memory.Field{f}, 65, nil); err != nil {
		t.Fatal(err)
	}
	old, charge := out.strct, m.Used()
	for fail := 1; fail <= 8; fail++ {
		calls := 0
		out.acquire = func(n int64) (memory.Reservation, error) {
			calls++
			if calls == fail {
				return nil, memory.ErrBudgetExceeded
			}
			return m.Reserve(context.Background(), n)
		}
		err := evalStructFrom(&out, []*scalarRegister{&in}, []int{0}, []memory.Field{f}, 65, nil)
		if err == nil {
			old, charge = out.strct, m.Used()
			continue
		}
		if !errors.Is(err, memory.ErrBudgetExceeded) || out.strct != old || m.Used() != charge {
			t.Fatalf("fault%d %v used%d want%d", fail, err, m.Used(), charge)
		}
	}
	out.checkContext = func() error { return context.Canceled }
	if err := evalStructFrom(&out, []*scalarRegister{&in}, []int{0}, []memory.Field{f}, 65, nil); !errors.Is(err, context.Canceled) || out.strct != old || m.Used() != charge {
		t.Fatal("cancel atomicity", err)
	}
	out.structFromReservation.Release()
	if m.Used() != 0 {
		t.Fatal("owner leak")
	}
}
