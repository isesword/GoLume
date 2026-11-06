package expr_test

import (
	"context"
	"errors"
	"github.com/apache/arrow-go/v18/arrow/array"
	amemory "github.com/apache/arrow-go/v18/arrow/memory"
	e "gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"math/big"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

var ks = kernel.New(kernel.ModeScalar)

func ev(t *testing.T, s memory.Schema, b *memory.Batch, x e.Expr) e.ScalarValue {
	t.Helper()
	p, err := e.Compile(s, x)
	if err != nil {
		t.Fatal(err)
	}
	w := newWorkspace(t, context.Background(), p, nil)
	t.Cleanup(func() { w.Close() })
	v, err := w.Evaluate(context.Background(), b, ks)
	if err != nil {
		t.Fatal(err)
	}
	return v[0]
}
func bools(t *testing.T, s memory.Schema, b *memory.Batch, x e.Expr, want []int) {
	t.Helper()
	v := ev(t, s, b, x)
	if v.Uint32 == nil {
		t.Fatalf("no bool value: %+v", v)
	}
	for i, w := range want {
		g := -1
		if v.Uint32.IsValid(i) {
			g = int(v.Uint32.Values[i])
		}
		if g != w {
			t.Fatalf("%s row%d got=%d want=%d", x, i, g, w)
		}
	}
}
func TestIndependentIdentityFloat32Infinity(t *testing.T) {
	s := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeFloat32})
	b := memory.NewBatch()
	b.Float32s["x"] = &memory.Float32Vector{Values: []float32{float32(math.Inf(1)), float32(math.Inf(-1)), float32(math.NaN())}}
	b.Rows = 3
	v := ev(t, s, b, e.CastTo(e.Col("x"), memory.TypeFloat32))
	if !math.IsInf(float64(v.Float32.Values[0]), 1) || !math.IsInf(float64(v.Float32.Values[1]), -1) || !math.IsNaN(float64(v.Float32.Values[2])) {
		t.Fatal(v)
	}
}
func TestIndependentExactAndComposedComparisons(t *testing.T) {
	s := memory.NewSchema(memory.Field{Name: "u", Type: memory.TypeUint64}, memory.Field{Name: "i", Type: memory.TypeInt64}, memory.Field{Name: "f", Type: memory.TypeFloat32})
	b := memory.NewBatch()
	b.Rows = 4
	b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{9007199254740993, math.MaxUint64, 0, math.MaxInt64}}
	b.Int64s["i"] = &memory.Int64Vector{Values: []int64{9007199254740992, math.MaxInt64, -1, math.MaxInt64}}
	b.Float32s["f"] = &memory.Float32Vector{Values: []float32{1, 1, 1, 1}}
	bools(t, s, b, e.GT(e.Col("u"), e.Col("i")), []int{1, 1, 1, 0})
	bools(t, s, b, e.EQ(e.Col("u"), e.I64(9007199254740992)), []int{0, 0, 0, 0})
	bools(t, s, b, e.LT(e.Col("f"), e.F64(1+math.Ldexp(1, -24))), []int{1, 1, 1, 1})
	bools(t, s, b, e.EQ(e.Col("f"), e.F64(1+math.Ldexp(1, -24))), []int{0, 0, 0, 0})
	bools(t, s, b, e.GT(e.Coalesce(e.Col("u"), e.Null(memory.TypeUint64)), e.Col("i")), []int{1, 1, 1, 0})
}
func TestIndependentTruthTablesAndReuse(t *testing.T) {
	s := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeBool, Nullable: true}, memory.Field{Name: "b", Type: memory.TypeBool, Nullable: true})
	b := memory.NewBatch()
	b.Rows = 9
	b.Uint32s["a"] = &memory.Uint32Vector{Values: []uint32{0, 0, 0, 1, 1, 1, 0, 0, 0}, Validity: memory.Bitmap{63}}
	b.Uint32s["b"] = &memory.Uint32Vector{Values: []uint32{0, 1, 0, 0, 1, 0, 0, 1, 0}, Validity: memory.Bitmap{219}}
	bools(t, s, b, e.And(e.Col("a"), e.Col("b")), []int{0, 0, 0, 0, 1, -1, 0, -1, -1})
	bools(t, s, b, e.Or(e.Col("a"), e.Col("b")), []int{0, 1, -1, 1, 1, 1, -1, 1, -1})
	bools(t, s, b, e.If(e.Col("a"), e.Col("b"), e.Bool(true)), []int{1, 1, 1, 0, 1, -1, 1, 1, 1})
	bools(t, s, b, e.Coalesce(e.Col("a"), e.Col("b"), e.Bool(true)), []int{0, 0, 0, 1, 1, 1, 0, 1, 1})
}
func TestIndependentGuardedRegionsAndCSE(t *testing.T) {
	s := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}, memory.Field{Name: "c", Type: memory.TypeBool, Nullable: true})
	b := memory.NewBatch()
	b.Rows = 4
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{7, 300, 8, 300}}
	b.Uint32s["c"] = &memory.Uint32Vector{Values: []uint32{1, 0, 1, 1}, Validity: memory.Bitmap{7}}
	checked := e.CastTo(e.Col("x"), memory.TypeInt8)
	v := ev(t, s, b, e.If(e.Col("c"), checked, e.CastTo(e.I64(-1), memory.TypeInt8)))
	if !reflect.DeepEqual(v.Int8.Values, []int8{7, -1, 8, -1}) {
		t.Fatal(v.Int8)
	}
	v = ev(t, s, b, e.Case([]e.WhenThen{{When: e.Col("c"), Then: checked}, {When: e.Bool(true), Then: e.CastTo(e.I64(-2), memory.TypeInt8)}}, checked))
	if !reflect.DeepEqual(v.Int8.Values, []int8{7, -2, 8, -2}) {
		t.Fatal(v.Int8)
	}
	// Distinct demanded domains must not hoist checked out of a guarded output.
	p, err := e.CompileExpressions(s, []e.NamedExpr{e.Named(e.If(e.Col("c"), checked, e.CastTo(e.I64(0), memory.TypeInt8)), "safe"), e.Named(checked, "unsafe")})
	if err != nil {
		t.Fatal(err)
	}
	w := newWorkspace(t, context.Background(), p, nil)
	defer w.Close()
	if _, err = w.Evaluate(context.Background(), b, ks); err == nil {
		t.Fatal("unguarded shared output incorrectly skipped error")
	}
	// Nested domains must survive input-prefix shrink/growth and workspace reuse.
	x := e.If(e.Col("c"), e.If(e.GT(e.Col("x"), e.I64(7)), checked, e.CastTo(e.I64(6), memory.TypeInt8)), e.CastTo(e.I64(-1), memory.TypeInt8))
	p1, err := e.Compile(s, x)
	if err != nil {
		t.Fatal(err)
	}
	w = newWorkspace(t, context.Background(), p1, nil)
	defer w.Close()
	for _, n := range []int{4, 1, 3, 4, 0, 4} {
		b.Rows = n
		results, err := w.Evaluate(context.Background(), b, ks)
		out := results[0]
		if err != nil {
			t.Fatal(err)
		}
		want := []int8{6, -1, 8, -1}
		if !reflect.DeepEqual(out.Int8.Values, want[:n]) {
			t.Fatalf("n=%d %v", n, out.Int8.Values)
		}
	}
}
func TestIndependentInGuardedNullAndMatches(t *testing.T) {
	s := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "bad", Type: memory.TypeInt64})
	b := memory.NewBatch()
	b.Rows = 4
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 2, 3, 4}, Validity: memory.Bitmap{7}}
	b.Int64s["bad"] = &memory.Int64Vector{Values: []int64{300, 2, 3, 300}}
	bools(t, s, b, e.In(e.Col("x"), e.I64(1), e.Null(memory.TypeInt64), e.CastTo(e.Col("bad"), memory.TypeInt8)), []int{1, 1, 1, -1})
	bools(t, s, b, e.In(e.Col("x"), e.I64(1), e.Null(memory.TypeInt64)), []int{1, -1, -1, -1})
}
func TestIndependentLeafBorrowAndMasks(t *testing.T) {
	s := memory.NewSchema(memory.Field{Name: "c", Type: memory.TypeBool, Nullable: true}, memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true})
	b := memory.NewBatch()
	b.Rows = 3
	b.Uint32s["c"] = &memory.Uint32Vector{Values: []uint32{1, 0, 1}, Validity: memory.Bitmap{7}}
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 2, 3}, Validity: memory.Bitmap{7}}
	p, err := e.CompileExpressions(s, []e.NamedExpr{e.Named(e.If(e.Col("c"), e.Col("x"), e.Null(memory.TypeInt64)), "masked"), e.Named(e.Col("x"), "all"), e.Named(e.Col("c"), "c"), e.Named(e.Not(e.Col("c")), "not")})
	if err != nil {
		t.Fatal(err)
	}
	w := newWorkspace(t, context.Background(), p, nil)
	defer w.Close()
	v, err := w.Evaluate(context.Background(), b, ks)
	if err != nil {
		t.Fatal(err)
	}
	if b.Int64s["x"].Validity[0] != 7 || b.Uint32s["c"].Validity[0] != 7 {
		t.Fatal("input mutated")
	}
	if v[0].Int64.IsValid(1) || !v[1].Int64.IsValid(1) || v[2].Uint32.Values[0] != 1 || v[3].Uint32.Values[0] != 0 {
		t.Fatal("shared result corrupted")
	}
}
func TestIndependentBudgetCloseAndCancel(t *testing.T) {
	s := memory.NewSchema(memory.Field{Name: "c", Type: memory.TypeBool}, memory.Field{Name: "x", Type: memory.TypeString})
	b := memory.NewBatch()
	b.Rows = 2
	b.Uint32s["c"] = &memory.Uint32Vector{Values: []uint32{1, 0}}
	b.Strings["x"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2}, Data: []byte("ab")}
	p, err := e.Compile(s, e.If(e.Col("c"), e.Concat(e.Col("x"), e.Str("hello")), e.Upper(e.Col("x"))))
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int64{1, 3000, 1000000} {
		manager := memory.NewBudgetManager(limit)
		w := newWorkspace(t, context.Background(), p, manager)
		_, err = w.Evaluate(context.Background(), b, ks)
		if err != nil && !errors.Is(err, memory.ErrBudgetExceeded) {
			t.Fatal(err)
		}
		w.Close()
		if manager.Used() != 0 {
			t.Fatalf("limit %d leaked %d", limit, manager.Used())
		}
		w.Close()
	}
	manager := memory.NewBudgetManager(0)
	ctx, cancel := context.WithCancel(context.Background())
	w := newWorkspace(t, ctx, p, manager)
	if _, err = w.Evaluate(ctx, b, ks); err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err = w.Evaluate(ctx, b, ks); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("cancel leaked %d", manager.Used())
	}
}
func TestIndependentOpaqueNull(t *testing.T) {
	s := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeArrow, Nullable: true, ArrowType: "list<item: int32>"})
	b := memory.NewBatch()
	b.Rows = 2
	b.Opaque["x"] = &memory.OpaqueVector{Rows: 2, Type: memory.TypeArrow, Validity: memory.Bitmap{1}, Value: struct{}{}}
	bools(t, s, b, e.IsNotNull(e.Col("x")), []int{1, 0})
}
func TestIndependentGuardedBooleanPrefixCrossesBitmapWords(t *testing.T) {
	s := memory.NewSchema(memory.Field{Name: "c", Type: memory.TypeBool}, memory.Field{Name: "v", Type: memory.TypeBool})
	b := memory.NewBatch()
	b.Uint32s["c"] = &memory.Uint32Vector{Values: make([]uint32, 130)}
	b.Uint32s["v"] = &memory.Uint32Vector{Values: make([]uint32, 130)}
	for i := range b.Uint32s["c"].Values {
		b.Uint32s["c"].Values[i] = 1
		b.Uint32s["v"].Values[i] = 1
	}
	p, err := e.Compile(s, e.If(e.Col("c"), e.Col("v"), e.Bool(false)))
	if err != nil {
		t.Fatal(err)
	}
	w := newWorkspace(t, context.Background(), p, nil)
	defer w.Close()
	for _, n := range []int{130, 1, 65, 0, 130} {
		b.Rows = n
		results, err := w.Evaluate(context.Background(), b, ks)
		v := results[0]
		if err != nil {
			t.Fatal(err)
		}
		if len(v.Uint32.Values) != n {
			t.Fatal("bad prefix")
		}
	}
}
func TestIndependentNilWorkspaceReturnsError(t *testing.T) {
	var w *e.Workspace
	b := memory.NewBatch()
	b.Rows = 1
	if _, err := w.Evaluate(context.Background(), b, ks); err == nil {
		t.Fatal("nil workspace accepted")
	}
}
func TestIndependentMixedIntegerFloatExactBoundaries(t *testing.T) {
	s := memory.NewSchema(memory.Field{Name: "u", Type: memory.TypeUint64}, memory.Field{Name: "f", Type: memory.TypeFloat64}, memory.Field{Name: "i", Type: memory.TypeInt64})
	b := memory.NewBatch()
	b.Rows = 7
	b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{1<<53 + 1, 1<<63 + 1, math.MaxUint64, 0, 0, 0, 1 << 63}}
	b.Float64s["f"] = &memory.Float64Vector{Values: []float64{1 << 53, 1 << 63, 1 << 64, math.NaN(), math.Inf(1), math.Inf(-1), 1 << 63}}
	b.Int64s["i"] = &memory.Int64Vector{Values: []int64{1<<53 + 1, math.MaxInt64, 0, 0, 0, 0, math.MaxInt64}}
	bools(t, s, b, e.EQ(e.Col("u"), e.Col("f")), []int{0, 0, 0, 0, 0, 0, 1})
	bools(t, s, b, e.GT(e.Col("u"), e.Col("f")), []int{1, 1, 0, 0, 0, 1, 0})
	bools(t, s, b, e.LT(e.Col("u"), e.Col("f")), []int{0, 0, 1, 0, 1, 0, 0})
	bools(t, s, b, e.EQ(e.Col("f"), e.I64(1<<53+1)), []int{0, 0, 0, 0, 0, 0, 0})
}
func TestIndependentNestedVariableWidthReuseAndAccounting(t *testing.T) {
	s := memory.NewSchema(memory.Field{Name: "c", Type: memory.TypeBool, Nullable: true}, memory.Field{Name: "x", Type: memory.TypeString, Nullable: true})
	x := e.Coalesce(e.If(e.Col("c"), e.Upper(e.Col("x")), e.Null(memory.TypeString)), e.Concat(e.Col("x"), e.Str("!")), e.Str("nil"))
	p, err := e.CompileExpressions(s, []e.NamedExpr{e.Named(x, "v"), e.Named(e.If(e.Col("c"), e.Col("c"), e.Not(e.Col("c"))), "b"), e.Named(e.Col("c"), "original")})
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(0)
	w := newWorkspace(t, context.Background(), p, manager)
	defer w.Close()
	for pass := 0; pass < 4; pass++ {
		for _, n := range []int{0, 1, 63, 64, 65, 130, 1, 0, 130, 64, 63} {
			b := memory.NewBatch()
			b.Rows = n
			cv := make([]uint32, n)
			valid := make(memory.Bitmap, memory.WordsFor(n))
			xvalid := make(memory.Bitmap, memory.WordsFor(n))
			off := make([]uint32, n+1)
			var data []byte
			want := make([]string, n)
			for row := 0; row < n; row++ {
				if row%3 == 0 {
					cv[row] = 1
				}
				if row%5 != 0 {
					memory.Set(valid, row)
				}
				text := strings.Repeat("ab", 1+(row+pass*17)%180)
				data = append(data, text...)
				off[row+1] = uint32(len(data))
				if row%7 != 0 {
					memory.Set(xvalid, row)
					if row%5 != 0 && cv[row] != 0 {
						want[row] = strings.ToUpper(text)
					} else {
						want[row] = text + "!"
					}
				} else {
					want[row] = "nil"
				}
			}
			b.Uint32s["c"] = &memory.Uint32Vector{Values: cv, Validity: valid}
			b.Strings["x"] = &memory.StringViewVector{Offsets: off, Data: data, Validity: xvalid}
			beforeC := append(memory.Bitmap{}, valid...)
			beforeX := append(memory.Bitmap{}, xvalid...)
			out, err := w.Evaluate(context.Background(), b, ks)
			if err != nil {
				t.Fatalf("pass%d rows%d: %v", pass, n, err)
			}
			for row := 0; row < n; row++ {
				if string(out[0].String.ValueBytes(row)) != want[row] || !out[0].String.IsValid(row) {
					t.Fatalf("pass%d n%d row%d got=%q want=%q", pass, n, row, out[0].String.ValueBytes(row), want[row])
				}
			}
			if !reflect.DeepEqual(beforeC, valid) || !reflect.DeepEqual(beforeX, xvalid) {
				t.Fatalf("pass%d n%d source validity mutated", pass, n)
			}
			if manager.Used() != w.MemoryBytes() {
				t.Fatalf("pass%d n%d owned=%d reported=%d", pass, n, manager.Used(), w.MemoryBytes())
			}
		}
	}
	w.Close()
	if manager.Used() != 0 || w.MemoryBytes() != 0 {
		t.Fatal("close leaked nested payload")
	}
}
func TestIndependentMixedNumericRationalOracle(t *testing.T) {
	us := []uint64{0, 1, 2, 1<<24 - 1, 1<<24 + 1, 1<<53 - 1, 1 << 53, 1<<53 + 1, 1<<63 - 1, 1 << 63, 1<<63 + 1, math.MaxUint64 - 1, math.MaxUint64}
	is := []int64{math.MinInt64, math.MinInt64 + 1, -1<<53 - 1, -1 << 53, -1<<53 + 1, -2, -1, 0, 1, 1<<53 - 1, 1 << 53, 1<<53 + 1, math.MaxInt64 - 1, math.MaxInt64}
	fs := []float64{math.NaN(), math.Inf(-1), math.Inf(1), math.Copysign(0, -1), 0, -.5, .5, -1.5, 1.5, math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64, -0x1p64, -0x1p63, math.Nextafter(-0x1p63, math.Inf(-1)), math.Nextafter(-0x1p63, 0), 0x1p53, math.Nextafter(0x1p53, math.Inf(1)), 0x1p63, math.Nextafter(0x1p63, 0), math.Nextafter(0x1p63, math.Inf(1)), 0x1p64, math.Nextafter(0x1p64, 0), math.MaxFloat64}
	var uv []uint64
	var iv []int64
	var fv []float64
	for _, u := range us {
		for _, i := range is {
			for _, f := range fs {
				uv = append(uv, u)
				iv = append(iv, i)
				fv = append(fv, f)
			}
		}
	}
	rnd := rand.New(rand.NewSource(1))
	for n := 0; n < 4000; n++ {
		uv = append(uv, rnd.Uint64())
		iv = append(iv, int64(rnd.Uint64()))
		fv = append(fv, math.Float64frombits(rnd.Uint64()))
	}
	s := memory.NewSchema(memory.Field{Name: "u", Type: memory.TypeUint64}, memory.Field{Name: "i", Type: memory.TypeInt64}, memory.Field{Name: "f", Type: memory.TypeFloat64})
	b := memory.NewBatch()
	b.Rows = len(uv)
	b.Uint64s["u"] = &memory.Uint64Vector{Values: uv}
	b.Int64s["i"] = &memory.Int64Vector{Values: iv}
	b.Float64s["f"] = &memory.Float64Vector{Values: fv}
	cmp := func(n *big.Rat, f float64) (int, bool) {
		if math.IsNaN(f) {
			return 0, false
		}
		if math.IsInf(f, 1) {
			return -1, true
		}
		if math.IsInf(f, -1) {
			return 1, true
		}
		return n.Cmp(new(big.Rat).SetFloat64(f)), true
	}
	truth := func(c int, valid bool, op e.Op) int {
		if !valid {
			return 0
		}
		v := false
		switch op {
		case e.OpEQ:
			v = c == 0
		case e.OpLT:
			v = c < 0
		case e.OpLE:
			v = c <= 0
		case e.OpGT:
			v = c > 0
		case e.OpGE:
			v = c >= 0
		}
		if v {
			return 1
		}
		return 0
	}
	for _, op := range []e.Op{e.OpEQ, e.OpLT, e.OpLE, e.OpGT, e.OpGE} {
		for _, col := range []string{"u", "i"} {
			for _, reverse := range []bool{false, true} {
				left, right := e.Expr(e.Col(col)), e.Expr(e.Col("f"))
				if reverse {
					left, right = right, left
				}
				want := make([]int, b.Rows)
				for row, f := range fv {
					n := new(big.Rat)
					if col == "u" {
						n.SetInt(new(big.Int).SetUint64(uv[row]))
					} else {
						n.SetInt64(iv[row])
					}
					c, valid := cmp(n, f)
					if reverse {
						c = -c
					}
					want[row] = truth(c, valid, op)
				}
				bools(t, s, b, e.Binary{Op: op, Left: left, Right: right}, want)
			}
		}
		// Also cover float-column / integer-literal specialized decisions.
		for _, i := range is {
			want := make([]int, b.Rows)
			for row, f := range fv {
				c, valid := cmp(new(big.Rat).SetInt64(i), f)
				want[row] = truth(-c, valid, op)
			}
			bools(t, s, b, e.Binary{Op: op, Left: e.Col("f"), Right: e.I64(i)}, want)
		}
	}
}
func TestIndependentOwnedNestedPayloadReporting(t *testing.T) {
	for _, json := range []bool{false, true} {
		t.Run(map[bool]string{false: "list", true: "json"}[json], func(t *testing.T) {
			s := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeString})
			text := strings.Repeat("x", 100000)
			value := e.Expr(e.Split(e.Col("x"), e.Str(",")))
			if json {
				text = `{"s":"` + text + `"}`
				value = e.JSONDecode(e.Col("x"), memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}))
			}
			b := memory.NewBatch()
			b.Rows = 1
			b.Strings["x"] = &memory.StringViewVector{Offsets: []uint32{0, uint32(len(text))}, Data: []byte(text)}
			p, err := e.Compile(s, value)
			if err != nil {
				t.Fatal(err)
			}
			manager := memory.NewBudgetManager(0)
			w := newWorkspace(t, context.Background(), p, manager)
			defer w.Close()
			if _, err = w.Evaluate(context.Background(), b, ks); err != nil {
				t.Fatal(err)
			}
			if w.MemoryBytes() < 100000 {
				t.Fatalf("owned 100k nested payload absent from reporting: MemoryBytes=%d Reserved=%d", w.MemoryBytes(), manager.Used())
			}
		})
	}
}
func TestIndependentBorrowedStringLogicalPrefix(t *testing.T) {
	s := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeString})
	b := memory.NewBatch()
	b.Rows = 1
	b.Strings["x"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2}, Data: []byte("ab")}
	v := ev(t, s, b, e.Col("x"))
	if v.String.Len() != 1 {
		t.Fatalf("projected vector exposes %d rows for batch.Rows=1", v.String.Len())
	}
}

func newWorkspace(t *testing.T, ctx context.Context, p e.Program, m memory.Manager) *e.Workspace {
	t.Helper()
	w, err := e.NewWorkspace(ctx, p, m)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

type auditFaultManager struct {
	*memory.BudgetManager
	calls, failAt int
	failure       error
}

func (m *auditFaultManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	if m.calls == m.failAt {
		return nil, m.failure
	}
	return m.BudgetManager.Reserve(ctx, n)
}
func TestIndependentOutputAdmissionCancellationReleases(t *testing.T) {
	p, err := e.Compile(memory.NewSchema(), e.I64(1))
	if err != nil {
		t.Fatal(err)
	}
	b := memory.NewBatch()
	b.Rows = 1
	m := &auditFaultManager{BudgetManager: memory.NewBudgetManager(0), failAt: 2, failure: context.Canceled}
	w := newWorkspace(t, context.Background(), p, m)
	defer w.Close()
	_, err = w.Evaluate(context.Background(), b, ks)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("calls=%d err=%v", m.calls, err)
	}
	if m.Used() != 0 {
		t.Fatalf("output admission cancellation retained %d bytes", m.Used())
	}
}
func TestIndependentRegionAdmissionFailureSweep(t *testing.T) {
	s := memory.NewSchema(memory.Field{Name: "c", Type: memory.TypeBool}, memory.Field{Name: "x", Type: memory.TypeString})
	p, err := e.Compile(s, e.If(e.Col("c"), e.Concat(e.Col("x"), e.Str("abc")), e.Coalesce(e.Null(memory.TypeString), e.Upper(e.Col("x")))))
	if err != nil {
		t.Fatal(err)
	}
	b := memory.NewBatch()
	b.Rows = 3
	b.Uint32s["c"] = &memory.Uint32Vector{Values: []uint32{1, 0, 1}}
	b.Strings["x"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2, 3}, Data: []byte("abc")}
	for fail := 1; fail <= 80; fail++ {
		m := &auditFaultManager{BudgetManager: memory.NewBudgetManager(0), failAt: fail, failure: memory.ErrBudgetExceeded}
		w := newWorkspace(t, context.Background(), p, m)
		_, err = w.Evaluate(context.Background(), b, ks)
		if err != nil && !errors.Is(err, memory.ErrBudgetExceeded) {
			t.Fatalf("fail=%d err=%v", fail, err)
		}
		m.failAt = 0
		v, err := w.Evaluate(context.Background(), b, ks)
		if err != nil {
			t.Fatalf("retry fail=%d err=%v", fail, err)
		}
		if got := string(v[0].String.ValueBytes(1)); got != "B" {
			t.Fatalf("fail=%d got=%q", fail, got)
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatalf("fail=%d leaked %d", fail, m.Used())
		}
	}
}

type auditReleaseCounter struct{ releases int }

func (v *auditReleaseCounter) Release() { v.releases++ }
func TestIndependentOpaqueResultDoesNotStealOwnership(t *testing.T) {
	s := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeArrow})
	counter := &auditReleaseCounter{}
	b := memory.NewBatch()
	b.Rows = 1
	b.Opaque["x"] = &memory.OpaqueVector{Rows: 1, Type: memory.TypeArrow, Value: counter, Owned: true}
	v := ev(t, s, b, e.Col("x"))
	dst := memory.NewBatch()
	dst.Rows = 1
	dst.Opaque["x"] = v.Opaque
	dst.ClearColumns()
	if counter.releases != 0 || !b.Opaque["x"].Owned {
		t.Fatalf("borrowed expression output released caller ownership: releases=%d sourceOwned=%v", counter.releases, b.Opaque["x"].Owned)
	}
	b.ClearColumns()
	if counter.releases != 1 {
		t.Fatalf("caller release count=%d", counter.releases)
	}
}
func TestIndependentBorrowedNestedAndArrowPrefixes(t *testing.T) {
	s := memory.NewSchema(memory.Field{Name: "d", Type: memory.TypeString, Encoding: memory.EncodingDictionary, DictionaryDomain: "test"}, memory.ListStringField("l", true, false), memory.StructField("s", true, memory.Field{Name: "n", Type: memory.TypeInt64}), memory.Field{Name: "a", Type: memory.TypeArrow})
	b := memory.NewBatch()
	n := 130
	b.Rows = n
	b.DictStrings["d"] = &memory.DictionaryStringVector{Indices: make([]uint32, n), Offsets: []uint32{0, 1}, Data: []byte("x"), DictionaryDomain: "test"}
	off := make([]uint32, n+1)
	for i := range off {
		off[i] = uint32(i)
	}
	b.StringLists["l"] = &memory.StringListVector{Offsets: off, Values: &memory.StringViewVector{Offsets: append([]uint32{}, off...), Data: []byte(strings.Repeat("x", n))}}
	child := memory.NewBatch()
	child.Rows = n
	child.Int64s["n"] = &memory.Int64Vector{Values: make([]int64, n)}
	for i := range child.Int64s["n"].Values {
		child.Int64s["n"].Values[i] = int64(i)
	}
	b.Structs["s"] = &memory.StructVector{Rows: n, Values: child, Fields: []memory.Field{{Name: "n", Type: memory.TypeInt64}}}
	alloc := amemory.NewCheckedAllocator(amemory.NewGoAllocator())
	builder := array.NewInt32Builder(alloc)
	for i := 0; i < n; i++ {
		builder.Append(int32(i))
	}
	arr := builder.NewInt32Array()
	builder.Release()
	b.Opaque["a"] = &memory.OpaqueVector{Rows: n, Type: memory.TypeArrow, Value: arr, Owned: true}
	defer func() { b.ClearColumns(); alloc.AssertSize(t, 0) }()
	p, err := e.CompileExpressions(s, []e.NamedExpr{e.Named(e.Col("d"), "d"), e.Named(e.Col("l"), "l"), e.Named(e.Col("s"), "s"), e.Named(e.Col("a"), "a")})
	if err != nil {
		t.Fatal(err)
	}
	m := memory.NewBudgetManager(0)
	w := newWorkspace(t, context.Background(), p, m)
	for _, rows := range []int{130, 1, 63, 64, 65, 0, 130} {
		b.Rows = rows
		values, err := w.Evaluate(context.Background(), b, ks)
		if err != nil {
			t.Fatal(err)
		}
		if values[0].DictionaryString.Len() != rows || values[1].StringList.Len() != rows || values[2].Struct.Len() != rows || values[2].Struct.Values.Rows != rows || len(values[2].Struct.Values.Int64s["n"].Values) != rows || values[3].Opaque.Len() != rows || values[3].Opaque.Value.(*array.Int32).Len() != rows {
			t.Fatalf("rows=%d prefix shape mismatch", rows)
		}
		if b.Structs["s"].Rows != n || b.Opaque["a"].Rows != n || !b.Opaque["a"].Owned || arr.Len() != n {
			t.Fatalf("rows=%d source shape/ownership changed", rows)
		}
	}
	w.Close()
	if m.Used() != 0 {
		t.Fatalf("nested prefix close retained %d", m.Used())
	}
}
func TestIndependentCroppedArrowAliasesAreBorrowed(t *testing.T) {
	alloc := amemory.NewCheckedAllocator(amemory.NewGoAllocator())
	builder := array.NewInt32Builder(alloc)
	builder.AppendValues([]int32{7, 8}, nil)
	arr := builder.NewInt32Array()
	builder.Release()
	b := memory.NewBatch()
	b.Rows = 1
	b.Opaque["a"] = &memory.OpaqueVector{Rows: 2, Type: memory.TypeArrow, Value: arr, Owned: true}
	defer func() { b.ClearColumns(); alloc.AssertSize(t, 0) }()
	p, err := e.CompileExpressions(memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeArrow}), []e.NamedExpr{e.Named(e.Col("a"), "left"), e.Named(e.Col("a"), "right")})
	if err != nil {
		t.Fatal(err)
	}
	m := memory.NewBudgetManager(0)
	w := newWorkspace(t, context.Background(), p, m)
	defer w.Close()
	v, err := w.Evaluate(context.Background(), b, ks)
	if err != nil {
		t.Fatal(err)
	}
	left, right := v[0].Opaque, v[1].Opaque
	d := memory.NewBatch()
	d.Rows = 1
	d.Opaque["v"] = left
	d.ClearColumns()
	rightArray := right.Value.(*array.Int32)
	if reflect.ValueOf(rightArray.Data()).IsNil() {
		t.Fatal("clearing one borrowed alias released another alias's Arrow slice")
	}
	if rightArray.Len() != 1 || rightArray.Value(0) != 7 || arr.Len() != 2 || !b.Opaque["a"].Owned {
		t.Fatal("alias clear changed source or peer")
	}
	w.Close()
	if m.Used() != 0 {
		t.Fatalf("Close retained %d", m.Used())
	}
}
func TestIndependentCanonicalInvalidUsage(t *testing.T) {
	if _, err := e.NewWorkspace(context.Background(), e.Program{}, nil); err == nil {
		t.Fatal("zero Program accepted")
	}
	b := memory.NewBatch()
	b.Rows = 1
	for _, values := range [][]e.NamedExpr{{e.Named(e.Bool(true), "a"), e.Named(e.I64(1), "b")}, {e.Named(e.Bool(true), "a")}} {
		p, err := e.CompileExpressions(memory.NewSchema(), values)
		if err != nil {
			t.Fatal(err)
		}
		m := memory.NewBudgetManager(0)
		w := newWorkspace(t, context.Background(), p, m)
		if _, err = w.EvaluateBool(context.Background(), b, ks); err == nil {
			t.Fatal("named-output program accepted as implicit Boolean root")
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("invalid Boolean consumption leaked")
		}
	}
	var w *e.Workspace
	if _, err := w.EvaluateBool(context.Background(), b, ks); err == nil {
		t.Fatal("nil workspace accepted")
	}
}
func TestIndependentCompiledSchemaDetached(t *testing.T) {
	input := memory.NewSchema(memory.Field{Name: "j", Type: memory.TypeString})
	result := memory.NewSchema(memory.Field{Name: "n", Type: memory.TypeInt8})
	value := e.JSONDecode(e.Col("j"), result)
	p, err := e.Compile(input, value)
	if err != nil {
		t.Fatal(err)
	}
	input.Fields[0].Name = "wrong"
	input.Fields[0].Type = memory.TypeInt64
	result.Fields[0].Type = memory.TypeString
	result.Fields[0].Name = "wrong"
	value.Schema.Fields[0].Type = memory.TypeFloat64
	b := memory.NewBatch()
	b.Rows = 1
	b.Strings["j"] = &memory.StringViewVector{Offsets: []uint32{0, 7}, Data: []byte(`{"n":7}`)}
	w := newWorkspace(t, context.Background(), p, nil)
	defer w.Close()
	out, err := w.Evaluate(context.Background(), b, ks)
	if err != nil {
		t.Fatal(err)
	}
	if out[0].Struct.Fields[0].Type != memory.TypeInt8 || out[0].Struct.Values.Int8s["n"].Values[0] != 7 {
		t.Fatal("compiled schema mutated through source schema/AST")
	}
}
func TestIndependentProgramMetadataIsDetached(t *testing.T) {
	typ := reflect.TypeOf(e.Program{})
	for i := 0; i < typ.NumField(); i++ {
		if typ.Field(i).PkgPath == "" {
			t.Fatalf("public semantic field %s", typ.Field(i).Name)
		}
	}
	zero := e.Program{}
	if zero.Type() != memory.TypeInvalid || zero.Nullable() || zero.InstructionCount() != 0 || zero.OutputCount() != 0 || len(zero.OutputFields()) != 0 {
		t.Fatal("zero Program metadata unsafe")
	}
	s := memory.NewSchema(memory.Field{Name: "j", Type: memory.TypeString}, memory.Field{Name: "d", Type: memory.TypeString, Encoding: memory.EncodingDictionary, DictionaryDomain: "domain"})
	decoded := e.JSONDecode(e.Col("j"), memory.NewSchema(memory.Field{Name: "n", Type: memory.TypeInt8}))
	named := []e.NamedExpr{e.Named(decoded, "record"), e.Named(e.Col("d"), "dict")}
	p, err := e.CompileExpressions(s, named)
	if err != nil {
		t.Fatal(err)
	}
	field := p.OutputField(0)
	field.Name = "bad"
	field.Children[0].Name = "bad"
	field.Children[0].Type = memory.TypeString
	fields := p.OutputFields()
	fields[0].Children[0].Type = memory.TypeFloat64
	fields[1].DictionaryDomain = "bad"
	named[0] = e.Named(e.Bool(false), "bad")
	s.Fields[1].DictionaryDomain = "bad"
	if p.OutputName(0) != "record" || p.OutputField(0).Children[0].Name != "n" || p.OutputField(0).Children[0].Type != memory.TypeInt8 || p.OutputField(1).DictionaryDomain != "domain" {
		t.Fatal("program output metadata aliased caller")
	}
	b := memory.NewBatch()
	b.Rows = 1
	b.Strings["j"] = &memory.StringViewVector{Offsets: []uint32{0, 7}, Data: []byte(`{"n":7}`)}
	b.DictStrings["d"] = &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{0, 1}, Data: []byte("x"), DictionaryDomain: "domain"}
	w := newWorkspace(t, context.Background(), p, nil)
	defer w.Close()
	out, err := w.Evaluate(context.Background(), b, ks)
	if err != nil {
		t.Fatal(err)
	}
	if out[0].Struct.Values.Int8s["n"].Values[0] != 7 || out[1].DictionaryString.DictionaryDomain != "domain" {
		t.Fatal("metadata mutation changed runtime")
	}
	root, err := e.Compile(memory.NewSchema(memory.Field{Name: "j", Type: memory.TypeString}), decoded)
	if err != nil {
		t.Fatal(err)
	}
	rootField := root.ResultField()
	rootField.Children[0].Type = memory.TypeString
	if root.ResultField().Children[0].Type != memory.TypeInt8 {
		t.Fatal("root field aliases metadata")
	}
	comparison, err := e.Compile(memory.NewSchema(memory.Field{Name: "n", Type: memory.TypeInt64}), e.GT(e.Col("n"), e.I64(4)))
	if err != nil {
		t.Fatal(err)
	}
	simple := comparison.SimpleComparison()
	if simple == nil {
		t.Fatal("simple comparison missing")
	}
	simple.LiteralI64 = 900
	if comparison.SimpleComparison().LiteralI64 != 4 {
		t.Fatal("comparison descriptor aliases plan")
	}
}
