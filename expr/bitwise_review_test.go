package expr

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"strings"
	"testing"
)

func bitReviewPut(b *memory.Batch, name string, typ memory.DataType, bits []uint64, valid memory.Bitmap) {
	switch typ {
	case memory.TypeInt32:
		v := make([]int32, len(bits))
		for i, x := range bits {
			v[i] = int32(x)
		}
		b.Int32s[name] = &memory.Int32Vector{Values: v, Validity: valid}
	case memory.TypeUint32:
		v := make([]uint32, len(bits))
		for i, x := range bits {
			v[i] = uint32(x)
		}
		b.Uint32s[name] = &memory.Uint32Vector{Values: v, Validity: valid}
	case memory.TypeInt64:
		v := make([]int64, len(bits))
		for i, x := range bits {
			v[i] = int64(x)
		}
		b.Int64s[name] = &memory.Int64Vector{Values: v, Validity: valid}
	case memory.TypeUint64:
		b.Uint64s[name] = &memory.Uint64Vector{Values: bits, Validity: valid}
	}
}
func bitReviewRead(v ScalarValue, row int) (uint64, bool) {
	switch v.Type {
	case memory.TypeInt32:
		return uint64(uint32(v.Int32.Values[row])), v.Int32.IsValid(row)
	case memory.TypeUint32:
		return uint64(v.Uint32.Values[row]), v.Uint32.IsValid(row)
	case memory.TypeInt64:
		return uint64(v.Int64.Values[row]), v.Int64.IsValid(row)
	case memory.TypeUint64:
		return v.Uint64.Values[row], v.Uint64.IsValid(row)
	}
	panic("not a native bitwise result")
}

func TestBitwiseReviewExactWidthsMasksReuse(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInt32, memory.TypeUint32, memory.TypeInt64, memory.TypeUint64} {
		t.Run(fmt.Sprint(typ), func(t *testing.T) {
			schema := memory.NewSchema(memory.Field{Name: "a", Type: typ, Nullable: true}, memory.Field{Name: "b", Type: typ, Nullable: true}, memory.Field{Name: "gate", Type: memory.TypeInt64})
			shared := BitXor(Col("a"), Col("b"))
			expressions := []Expr{BitAnd(Col("a"), Col("b")), BitOr(Col("a"), Col("b")), shared, BitNot(Col("a")), BitAnd(Col("a"), I64(5)), BitOr(I64(5), Col("a")), shared, If(EQ(Col("gate"), I64(0)), shared, BitNot(Col("a"))), Case([]WhenThen{{EQ(Col("gate"), I64(1)), BitAnd(Col("a"), Col("b"))}}, BitOr(Col("a"), Col("b"))), Col("a")}
			named := make([]NamedExpr, len(expressions))
			for i, e := range expressions {
				named[i] = Named(e, fmt.Sprint(i))
			}
			p, err := CompileExpressions(schema, named)
			if err != nil {
				t.Fatal(err)
			}
			if !p.SupportsSelection() {
				t.Fatal("selection unsupported")
			}
			mask := uint64(math.MaxUint64)
			if typ == memory.TypeInt32 || typ == memory.TypeUint32 {
				mask = math.MaxUint32
			}
			for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD, kernel.ModeAuto} {
				manager := memory.NewBudgetManager(0)
				w, err := NewWorkspace(context.Background(), p, manager)
				if err != nil {
					t.Fatal(err)
				}
				for step, n := range []int{8193, 1, 63, 64, 65, 4095, 4096, 4097, 0, 8193} {
					batch := memory.NewBatch()
					batch.Rows = n
					a, b, g := make([]uint64, n), make([]uint64, n), make([]int64, n)
					av, bv := make(memory.Bitmap, memory.WordsFor(n)), make(memory.Bitmap, memory.WordsFor(n))
					special := []uint64{0, 1, math.MaxUint64, 1 << 63, math.MaxInt64, 1 << 31, math.MaxInt32, 1<<53 + 1, 0xaaaaaaaaaaaaaaaa, 0x5555555555555555}
					for i := range a {
						a[i] = special[i%len(special)] & mask
						b[i] = special[(i*3+2)%len(special)] & mask
						g[i] = int64(i % 3)
						if step%3 == 1 || step%3 == 0 && i%7 != 0 {
							memory.Set(av, i)
						}
						if step%3 == 1 || step%3 == 0 && i%11 != 0 {
							memory.Set(bv, i)
						}
					}
					if n > 0 {
						av[len(av)-1] |= ^memory.TailMask(n)
						bv[len(bv)-1] |= ^memory.TailMask(n)
					}
					bitReviewPut(batch, "a", typ, a, av)
					bitReviewPut(batch, "b", typ, b, bv)
					batch.Int64s["gate"] = &memory.Int64Vector{Values: g}
					check := func(got []ScalarValue, rows []int) {
						t.Helper()
						if len(got) != len(expressions) {
							t.Fatalf("got %d outputs, want %d", len(got), len(expressions))
						}
						for out, v := range got {
							var size int
							switch v.Type {
							case memory.TypeInt32:
								size = len(v.Int32.Values)
							case memory.TypeUint32:
								size = len(v.Uint32.Values)
							case memory.TypeInt64:
								size = len(v.Int64.Values)
							case memory.TypeUint64:
								size = len(v.Uint64.Values)
							}
							if size != len(rows) {
								t.Fatalf("output %d length=%d want=%d", out, size, len(rows))
							}
							if v.Type != typ {
								t.Fatalf("wrong type %v", v.Type)
							}
							for j, i := range rows {
								validA, validB := memory.IsSet(av, i), memory.IsSet(bv, i)
								valid := validA && validB
								var want uint64
								switch out {
								case 0:
									want = a[i] & b[i]
								case 1:
									want = a[i] | b[i]
								case 2, 6:
									want = a[i] ^ b[i]
								case 3:
									want = ^a[i] & mask
									valid = validA
								case 4:
									want = a[i] & 5
									valid = validA
								case 5:
									want = a[i] | 5
									valid = validA
								case 7:
									if g[i] == 0 {
										want = a[i] ^ b[i]
									} else {
										want = ^a[i] & mask
										valid = validA
									}
								case 8:
									if g[i] == 1 {
										want = a[i] & b[i]
									} else {
										want = a[i] | b[i]
									}
								case 9:
									want = a[i]
									valid = validA
								}
								x, vld := bitReviewRead(v, j)
								if vld != valid || valid && x != want {
									t.Fatalf("mode=%v n=%d output=%d row=%d got=%x/%v want=%x/%v", mode, n, out, i, x, vld, want, valid)
								}
							}
						}
					}
					rows := make([]int, n)
					for i := range rows {
						rows[i] = i
					}
					got, err := w.Evaluate(context.Background(), batch, kernel.New(mode))
					if err != nil {
						t.Fatal(err)
					}
					check(got, rows)
					selected := make([]int, 0)
					for i := 0; i < n; i += 13 {
						selected = append(selected, i)
					}
					got, err = w.EvaluateSelected(context.Background(), batch, selected, kernel.New(mode))
					if err != nil {
						t.Fatal(err)
					}
					check(got, selected)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				if manager.Used() != 0 {
					t.Fatal("leaked", manager.Used())
				}
			}
		})
	}
}

func TestBitwiseReviewBindingAndRejection(t *testing.T) {
	types := []memory.DataType{memory.TypeInt32, memory.TypeUint32, memory.TypeInt64, memory.TypeUint64}
	for _, typ := range types {
		schema := memory.NewSchema(memory.Field{Name: "a", Type: typ, Nullable: true})
		for _, e := range []Expr{Col("a").BitAnd(I64(7)), I64(7).BitOr(Col("a")), Col("a").BitNot(), Col("a").BitXor(I64(7)).BitNot().BitAnd(Col("a"))} {
			b, err := Bind(schema, e)
			if err != nil {
				t.Fatal(err)
			}
			if b.Field.Type != typ || !b.Field.Nullable || b.MayError {
				t.Fatalf("bad metadata %+v", b)
			}
		}
		for _, other := range types {
			if typ == other {
				continue
			}
			s := memory.NewSchema(memory.Field{Name: "a", Type: typ}, memory.Field{Name: "b", Type: other})
			for _, e := range []Expr{BitAnd(Col("a"), Col("b")), BitOr(Col("b"), Col("a")), BitXor(Col("a"), Col("b"))} {
				_, err := Compile(s, e)
				if err == nil || !strings.Contains(strings.ToLower(err.Error()), "cast") {
					t.Fatalf("mixed %v/%v: %v", typ, other, err)
				}
			}
		}
	}
	for _, tc := range []struct {
		typ   memory.DataType
		value int64
	}{{memory.TypeInt32, math.MaxInt32 + 1}, {memory.TypeInt32, math.MinInt32 - 1}, {memory.TypeUint32, -1}, {memory.TypeUint32, math.MaxUint32 + 1}, {memory.TypeUint64, -1}} {
		s := memory.NewSchema(memory.Field{Name: "a", Type: tc.typ})
		for _, e := range []Expr{BitAnd(Col("a"), I64(tc.value)), BitXor(I64(tc.value), Col("a"))} {
			_, err := Compile(s, e)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), "cast") {
				t.Fatalf("out of range %v/%d: %v", tc.typ, tc.value, err)
			}
		}
	}
	for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeUint8, memory.TypeInt16, memory.TypeUint16, memory.TypeFloat32, memory.TypeFloat64, memory.TypeBool, memory.TypeString, memory.TypeDate32} {
		s := memory.NewSchema(memory.Field{Name: "a", Type: typ})
		for _, e := range []Expr{BitNot(Col("a")), BitAnd(Col("a"), Col("a"))} {
			if _, err := Compile(s, e); err == nil || !strings.Contains(strings.ToLower(err.Error()), "cast") {
				t.Fatalf("unsupported type %v: %v", typ, err)
			}
		}
	}
	for _, e := range []Expr{BitAnd(nil, I64(1)), BitOr(I64(1), nil), BitNot(nil)} {
		if _, err := Compile(memory.NewSchema(), e); err == nil {
			t.Fatal("nil accepted")
		}
	}
}

func TestBitwiseReviewBudgetAndAllocation(t *testing.T) {
	p, err := Compile(memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeUint64}), Col("a").BitXor(I64(7)).BitNot())
	if err != nil {
		t.Fatal(err)
	}
	batch := memory.NewBatch()
	batch.Rows = 65
	batch.Uint64s["a"] = &memory.Uint64Vector{Values: make([]uint64, 65)}
	manager := &predicateRecordingManager{base: memory.NewBudgetManager(0), deny: true}
	w, err := NewWorkspace(context.Background(), p, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	k := kernel.New(kernel.ModeAuto)
	ctx := memory.WithQueryBudget(context.Background(), w.query)
	if _, err = w.Evaluate(ctx, batch, k); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("admission %v", err)
	}
	if manager.Used() != 0 || w.MemoryBytes() != 0 {
		t.Fatal("denied evaluation allocated")
	}
	manager.deny = false
	if _, err = w.Evaluate(ctx, batch, k); err != nil {
		t.Fatal(err)
	}
	charge := manager.Used()
	expected, err := p.WorkspaceBytes(65)
	if err != nil || charge < expected || charge != w.MemoryBytes() {
		t.Fatalf("charge=%d estimate=%d err=%v", charge, expected, err)
	}
	allocations := testing.AllocsPerRun(100, func() {
		if _, err := w.Evaluate(ctx, batch, k); err != nil {
			panic(err)
		}
	})
	// Public export allocates one vector wrapper; the execution itself must not.
	if allocations != 1 {
		t.Fatalf("public scalar export allocations=%g, want 1", allocations)
	}
	allocations = testing.AllocsPerRun(100, func() {
		if err := evaluateInstructions(p.instructions, batch, &w.storage, k); err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("warmed bitwise execution allocations=%g", allocations)
	}
	manager.deny = true
	batch.Rows = 8193
	batch.Uint64s["a"].Values = make([]uint64, 8193)
	if _, err = w.Evaluate(ctx, batch, k); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if manager.Used() != charge {
		t.Fatal("failed growth changed charge")
	}
	batch.Rows = 1
	batch.Uint64s["a"].Values = batch.Uint64s["a"].Values[:1]
	if _, err = w.Evaluate(ctx, batch, k); err != nil {
		t.Fatal(err)
	}
	w.Close()
	w.Close()
	if manager.Used() != 0 {
		t.Fatal("release leaked")
	}
}

// An error-producing child must only run in its active IF/CASE region.
func TestBitwiseReviewConditionalDemand(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64}, memory.Field{Name: "power", Type: memory.TypeInt64}, memory.Field{Name: "gate", Type: memory.TypeInt64})
	b := memory.NewBatch()
	b.Rows = 8193
	a, power, gate := make([]int64, b.Rows), make([]int64, b.Rows), make([]int64, b.Rows)
	for i := range a {
		a[i] = 3
		gate[i] = int64(i % 2)
		power[i] = 2
		if gate[i] == 1 {
			power[i] = -1
		}
	}
	b.Int64s["a"] = &memory.Int64Vector{Values: a}
	b.Int64s["power"] = &memory.Int64Vector{Values: power}
	b.Int64s["gate"] = &memory.Int64Vector{Values: gate}
	dangerous := Col("a").Pow(Col("power")).BitAnd(I64(7)).BitNot()
	for _, e := range []Expr{If(EQ(Col("gate"), I64(0)), dangerous, I64(19)), Case([]WhenThen{{EQ(Col("gate"), I64(0)), dangerous}}, I64(19))} {
		program, err := Compile(schema, e)
		if err != nil {
			t.Fatal(err)
		}
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD, kernel.ModeAuto} {
			w, err := NewWorkspace(context.Background(), program, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, err := w.Evaluate(context.Background(), b, kernel.New(mode))
			if err != nil {
				t.Fatal("inactive error evaluated", err)
			}
			for i, v := range got[0].Int64.Values {
				want := int64(-2)
				if gate[i] == 1 {
					want = 19
				}
				if v != want {
					t.Fatalf("row %d got %d want %d", i, v, want)
				}
			}
			power[100] = -1
			_, err = w.Evaluate(context.Background(), b, kernel.New(mode))
			power[100] = 2
			if err == nil || !strings.Contains(err.Error(), "row 100") {
				t.Fatalf("active error: %v", err)
			}
			w.Close()
		}
	}
}

func TestBitwiseReviewLiteralBoundaries(t *testing.T) {
	for _, tc := range []struct {
		typ memory.DataType
		lit int64
	}{{memory.TypeInt32, math.MinInt32}, {memory.TypeInt32, math.MaxInt32}, {memory.TypeUint32, math.MaxUint32}, {memory.TypeInt64, math.MinInt64}, {memory.TypeInt64, math.MaxInt64}, {memory.TypeUint64, math.MaxInt64}} {
		schema := memory.NewSchema(memory.Field{Name: "a", Type: tc.typ})
		expressions := []Expr{BitAnd(Col("a"), I64(tc.lit)), BitAnd(I64(tc.lit), Col("a")), BitOr(Col("a"), I64(tc.lit)), BitOr(I64(tc.lit), Col("a")), BitXor(Col("a"), I64(tc.lit)), BitXor(I64(tc.lit), Col("a"))}
		named := make([]NamedExpr, len(expressions))
		for i, e := range expressions {
			named[i] = Named(e, fmt.Sprint(i))
		}
		p, err := CompileExpressions(schema, named)
		if err != nil {
			t.Fatal(err)
		}
		b := memory.NewBatch()
		b.Rows = 3
		values := []uint64{math.MaxUint64, 0, 0xaaaaaaaaaaaaaaaa}
		bitReviewPut(b, "a", tc.typ, values, nil)
		w, err := NewWorkspace(context.Background(), p, nil)
		if err != nil {
			t.Fatal(err)
		}
		got, err := w.Evaluate(context.Background(), b, kernel.New(kernel.ModeAuto))
		if err != nil {
			t.Fatal(err)
		}
		mask := uint64(math.MaxUint64)
		if tc.typ == memory.TypeInt32 || tc.typ == memory.TypeUint32 {
			mask = math.MaxUint32
		}
		for j, v := range got {
			for i, x := range values {
				var want uint64
				switch j / 2 {
				case 0:
					want = x & uint64(tc.lit)
				case 1:
					want = x | uint64(tc.lit)
				case 2:
					want = x ^ uint64(tc.lit)
				}
				actual, valid := bitReviewRead(v, i)
				if !valid || actual != want&mask {
					t.Fatalf("type=%v literal=%d op=%d got=%x want=%x", tc.typ, tc.lit, j, actual, want&mask)
				}
			}
		}
		w.Close()
	}
	p, err := Compile(memory.NewSchema(), I64(math.MinInt64).BitXor(I64(math.MaxInt64)).BitNot())
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWorkspace(context.Background(), p, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	b := memory.NewBatch()
	b.Rows = 65
	got, err := w.Evaluate(context.Background(), b, kernel.New(kernel.ModeSIMD))
	if err != nil {
		t.Fatal(err)
	}
	for i, x := range got[0].Int64.Values {
		if x != 0 || !got[0].Int64.IsValid(i) {
			t.Fatal("literal-only result", i, x)
		}
	}
}
