package expr

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"sync"
	"testing"

	"gocolumn/kernel"
	"gocolumn/memory"
)

// This oracle deliberately uses arbitrary-precision rational equality, not the
// production comparison helpers or normalized keys. Float equality handles
// NaNs and infinities separately, matching SQL's IEEE comparison contract.
func inOracleEqual(a, b any) bool {
	if a == nil || b == nil {
		return false
	}
	if av, ok := a.(string); ok {
		bv, ok := b.(string)
		return ok && av == bv
	}
	if av, ok := a.(bool); ok {
		if av {
			a = int64(1)
		} else {
			a = int64(0)
		}
	}
	if bv, ok := b.(bool); ok {
		if bv {
			b = int64(1)
		} else {
			b = int64(0)
		}
	}
	for _, value := range []any{a, b} {
		if f, ok := value.(float64); ok && (math.IsNaN(f) || math.IsInf(f, 0)) {
			af, aok := a.(float64)
			bf, bok := b.(float64)
			return aok && bok && af == bf
		}
	}
	toRat := func(v any) *big.Rat {
		switch n := v.(type) {
		case int64:
			return new(big.Rat).SetInt64(n)
		case uint64:
			return new(big.Rat).SetInt(new(big.Int).SetUint64(n))
		case float64:
			return new(big.Rat).SetFloat64(n)
		default:
			panic(fmt.Sprintf("unexpected oracle value %T", v))
		}
	}
	return toRat(a).Cmp(toRat(b)) == 0
}

func inOracleResult(value any, candidates []any) (truth, valid bool) {
	if value == nil {
		return false, false
	}
	valid = true
	for _, candidate := range candidates {
		if candidate == nil {
			valid = false
			continue
		}
		if inOracleEqual(value, candidate) {
			return true, true
		}
	}
	return false, valid
}

func inNumericFixture(typ memory.DataType) (*memory.Batch, []any) {
	base := []int64{0, 1, -1, 2, -2, 127, 128, 255, 256, 32767, -32768, 1<<24 + 1, 1<<32 - 1, 1<<53 - 1, 1 << 53, 1<<53 + 1, math.MinInt64, math.MaxInt64}
	floats := []float64{0, math.Copysign(0, -1), 1, -1, 1.5, -1.5, math.NaN(), math.Inf(1), math.Inf(-1), math.SmallestNonzeroFloat64, -0x1p63, 0x1p63, 0x1p64, math.Nextafter(0x1p64, 0), 0x1p53, 0x1p53 + 2, 16777217}
	const rows = 129
	b := memory.NewBatch()
	b.Rows = rows
	validity := make(memory.Bitmap, memory.WordsFor(rows))
	want := make([]any, rows)
	i64 := make([]int64, rows)
	u64 := make([]uint64, rows)
	f64 := make([]float64, rows)
	for row := range rows {
		i64[row] = base[row%len(base)]
		u64[row] = uint64(i64[row])
		f64[row] = floats[row%len(floats)]
		if row%11 != 3 {
			memory.Set(validity, row)
		}
	}
	switch typ {
	case memory.TypeInt8:
		v := make([]int8, rows)
		for i := range v {
			v[i] = int8(i64[i])
			want[i] = int64(v[i])
		}
		b.Int8s["x"] = &memory.Int8Vector{Values: v, Validity: validity}
	case memory.TypeInt16:
		v := make([]int16, rows)
		for i := range v {
			v[i] = int16(i64[i])
			want[i] = int64(v[i])
		}
		b.Int16s["x"] = &memory.Int16Vector{Values: v, Validity: validity}
	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		v := make([]int32, rows)
		for i := range v {
			v[i] = int32(i64[i])
			want[i] = int64(v[i])
		}
		b.Int32s["x"] = &memory.Int32Vector{Values: v, Validity: validity}
	case memory.TypeInt64, memory.TypeTimestamp, memory.TypeDuration, memory.TypeTime64, memory.TypeDate64:
		for i := range i64 {
			want[i] = i64[i]
		}
		b.Int64s["x"] = &memory.Int64Vector{Values: i64, Validity: validity}
	case memory.TypeUint8:
		v := make([]uint8, rows)
		for i := range v {
			v[i] = uint8(u64[i])
			want[i] = uint64(v[i])
		}
		b.Uint8s["x"] = &memory.Uint8Vector{Values: v, Validity: validity}
	case memory.TypeUint16:
		v := make([]uint16, rows)
		for i := range v {
			v[i] = uint16(u64[i])
			want[i] = uint64(v[i])
		}
		b.Uint16s["x"] = &memory.Uint16Vector{Values: v, Validity: validity}
	case memory.TypeUint32, memory.TypeBool:
		v := make([]uint32, rows)
		for i := range v {
			v[i] = uint32(u64[i])
			if typ == memory.TypeBool {
				v[i] %= 2
			}
			want[i] = uint64(v[i])
		}
		b.Uint32s["x"] = &memory.Uint32Vector{Values: v, Validity: validity}
	case memory.TypeUint64:
		for i := range u64 {
			want[i] = u64[i]
		}
		b.Uint64s["x"] = &memory.Uint64Vector{Values: u64, Validity: validity}
	case memory.TypeFloat32:
		v := make([]float32, rows)
		for i := range v {
			v[i] = float32(f64[i])
			want[i] = float64(v[i])
		}
		b.Float32s["x"] = &memory.Float32Vector{Values: v, Validity: validity}
	case memory.TypeFloat64:
		for i := range f64 {
			want[i] = f64[i]
		}
		b.Float64s["x"] = &memory.Float64Vector{Values: f64, Validity: validity}
	}
	for row := range rows {
		if !memory.IsSet(validity, row) {
			want[row] = nil
		}
	}
	return b, want
}

func TestLiteralInIndependentNumericOracle(t *testing.T) {
	types := []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64, memory.TypeBool, memory.TypeDate32, memory.TypeDate64, memory.TypeTime32, memory.TypeTime64, memory.TypeTimestamp, memory.TypeDuration}
	for _, typ := range types {
		t.Run(typ.String(), func(t *testing.T) {
			b, values := inNumericFixture(typ)
			for _, count := range []int{16, 64, 257} {
				for _, nulls := range []bool{false, true} {
					rng := rand.New(rand.NewSource(int64(count)))
					candidates := make([]Expr, count)
					oracle := make([]any, count)
					ints := []int64{0, 1, -1, 127, 128, math.MinInt64, math.MaxInt64, 1<<53 + 1, 1 << 53, 1<<24 + 1}
					floats := []float64{math.NaN(), math.Inf(1), math.Inf(-1), math.Copysign(0, -1), 0x1p63, 0x1p64, math.Nextafter(0x1p64, 0), 0.5, -1.5, 1, math.SmallestNonzeroFloat64}
					for i := range candidates {
						v := ints[rng.Intn(len(ints))]
						if typ == memory.TypeBool {
							v = int64(rng.Intn(4))
						}
						candidates[i] = I64(v)
						oracle[i] = v
						if IsNumeric(typ) && i%2 == 1 {
							f := floats[rng.Intn(len(floats))]
							candidates[i] = F64(f)
							oracle[i] = f
						}
					}
					if nulls {
						candidates[count/2] = Null(typ)
						oracle[count/2] = nil
					}
					p, err := Compile(memory.NewSchema(memory.Field{Name: "x", Type: typ, Nullable: true}), In(Col("x"), candidates...))
					if err != nil {
						t.Fatal(err)
					}
					if len(p.instructions) != 2 || p.instructions[p.root].Code != scalarInSet {
						t.Fatal("large literals did not lower to set")
					}
					for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
						w, err := NewWorkspace(context.Background(), p, nil)
						if err != nil {
							t.Fatal(err)
						}
						got, err := w.EvaluateBool(context.Background(), b, kernel.New(mode))
						if err != nil {
							t.Fatal(err)
						}
						for row, value := range values {
							truth, valid := inOracleResult(value, oracle)
							if memory.IsSet(got.Validity, row) != valid || memory.IsSet(got.Truth, row) != truth {
								t.Fatalf("count=%d null=%v row=%d value=%v got=%v/%v want=%v/%v", count, nulls, row, value, memory.IsSet(got.Truth, row), memory.IsSet(got.Validity, row), truth, valid)
							}
						}
						w.Close()
					}
				}
			}
		})
	}
}

func literalInInts(n int) []Expr {
	candidates := make([]Expr, n)
	for i := range candidates {
		candidates[i] = I64(int64(i))
	}
	return candidates
}

func TestLiteralInFallbackAndGuardedErrors(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}, memory.Field{Name: "s", Type: memory.TypeString})
	b := memory.NewBatch()
	b.Rows = 3
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{0, 1, 2}}
	b.Strings["s"] = phase32StringVector([]string{"bad", "bad", "bad"}, nil)
	for _, count := range []int{1, literalInThreshold - 1, literalInThreshold, literalInThreshold + 1} {
		p, err := Compile(schema, In(Col("x"), literalInInts(count)...))
		if err != nil {
			t.Fatal(err)
		}
		if (p.instructions[p.root].Code == scalarInSet) != (count >= literalInThreshold) {
			t.Fatal("threshold mismatch")
		}
	}
	for _, dynamic := range []Expr{Col("x"), CastTo(StringLiteral{Value: "bad"}, memory.TypeInt64), CastTo(Col("s"), memory.TypeInt64)} {
		list := append(literalInInts(32), dynamic)
		p, err := Compile(schema, In(Col("x"), list...))
		if err != nil {
			t.Fatal(err)
		}
		if p.instructions[p.root].Code != scalarConditional {
			t.Fatal("dynamic/error-prone candidate was hoisted")
		}
		w, err := NewWorkspace(context.Background(), p, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = w.EvaluateBool(context.Background(), b, testKernelSet()); err != nil {
			t.Fatal("matched earlier candidates must suppress later errors", err)
		}
		w.Close()
	}
	badNeedle := In(CastTo(Col("s"), memory.TypeInt64), literalInInts(32)...)
	p, err := Compile(schema, If(Bool(false), badNeedle, Bool(true)))
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWorkspace(context.Background(), p, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.EvaluateBool(context.Background(), b, testKernelSet()); err != nil {
		t.Fatal("unselected IN needle evaluated", err)
	}
	w.Close()
	p, err = Compile(schema, badNeedle)
	if err != nil {
		t.Fatal(err)
	}
	w, err = NewWorkspace(context.Background(), p, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.EvaluateBool(context.Background(), b, testKernelSet()); err == nil {
		t.Fatal("demanded needle error lost")
	}
	if w.MemoryBytes() != 0 {
		t.Fatal("error retained memory")
	}
}

func TestLiteralInBudgetLifecycleAndConcurrency(t *testing.T) {
	b, _ := inNumericFixture(memory.TypeInt64)
	p, err := Compile(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true}), In(Col("x"), literalInInts(1024)...))
	if err != nil {
		t.Fatal(err)
	}
	n, err := p.WorkspaceBytes(b.Rows)
	if err != nil {
		t.Fatal(err)
	}
	for _, budget := range []int64{n - 1, n} {
		m := memory.NewBudgetManager(budget)
		w, err := NewWorkspace(context.Background(), p, m)
		if err != nil {
			t.Fatal(err)
		}
		_, err = w.EvaluateBool(context.Background(), b, testKernelSet())
		if budget < n {
			if !errors.Is(err, memory.ErrBudgetExceeded) || m.Used() != 0 || w.MemoryBytes() != 0 {
				t.Fatalf("admission: %v used=%d retained=%d", err, m.Used(), w.MemoryBytes())
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			if m.Used() != n || w.MemoryBytes() != n {
				t.Fatalf("accounting used=%d retained=%d estimated=%d", m.Used(), w.MemoryBytes(), n)
			}
			for range 3 {
				if _, err = w.EvaluateBool(context.Background(), b, testKernelSet()); err != nil {
					t.Fatal(err)
				}
			}
		}
		w.Close()
		w.Close()
		if m.Used() != 0 {
			t.Fatal("close leaked")
		}
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			m := memory.NewBudgetManager(n)
			w, err := NewWorkspace(context.Background(), p, m)
			if err != nil {
				t.Error(err)
				return
			}
			defer w.Close()
			for range 10 {
				if _, err := w.EvaluateBool(context.Background(), b, testKernelSet()); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	// Immutable sets remain valid after every independent workspace has closed.
	w, err := NewWorkspace(context.Background(), p, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = w.EvaluateBool(ctx, b, testKernelSet()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if w.MemoryBytes() != 0 {
		t.Fatal("cancel retained storage")
	}
}

// Test each lossy boundary in isolation so another equal literal in a larger
// mixed list cannot conceal accidental integer rounding or Float32 narrowing.
func TestLiteralInIsolatedExactBoundaries(t *testing.T) {
	cases := []struct {
		typ       memory.DataType
		needle    any
		candidate Expr
		value     any
	}{
		{memory.TypeInt64, int64(1<<53 + 1), F64(0x1p53), float64(0x1p53)},
		{memory.TypeFloat64, float64(0x1p53), I64(1<<53 + 1), int64(1<<53 + 1)},
		{memory.TypeInt64, int64(math.MaxInt64), F64(0x1p63), float64(0x1p63)},
		{memory.TypeInt64, int64(math.MinInt64), F64(-0x1p63), float64(-0x1p63)},
		{memory.TypeUint64, uint64(math.MaxUint64), F64(0x1p64), float64(0x1p64)},
		{memory.TypeUint64, uint64(1 << 63), F64(0x1p63), float64(0x1p63)},
		{memory.TypeUint64, uint64(math.MaxUint64), I64(-1), int64(-1)},
		{memory.TypeFloat64, math.Inf(1), I64(math.MaxInt64), int64(math.MaxInt64)},
		{memory.TypeFloat32, float64(float32(.1)), F64(.1), float64(.1)},
		{memory.TypeFloat32, float64(float32(1 << 24)), I64(1<<24 + 1), int64(1<<24 + 1)},
		{memory.TypeFloat64, math.NaN(), F64(math.NaN()), math.NaN()},
		{memory.TypeFloat64, math.Copysign(0, -1), F64(0), float64(0)},
	}
	for i, tc := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			b := memory.NewBatch()
			b.Rows = 1
			switch tc.typ {
			case memory.TypeInt64:
				b.Int64s["x"] = &memory.Int64Vector{Values: []int64{tc.needle.(int64)}}
			case memory.TypeUint64:
				b.Uint64s["x"] = &memory.Uint64Vector{Values: []uint64{tc.needle.(uint64)}}
			case memory.TypeFloat32:
				b.Float32s["x"] = &memory.Float32Vector{Values: []float32{float32(tc.needle.(float64))}}
			case memory.TypeFloat64:
				b.Float64s["x"] = &memory.Float64Vector{Values: []float64{tc.needle.(float64)}}
			}
			candidates := make([]Expr, literalInThreshold)
			for i := range candidates {
				candidates[i] = tc.candidate
			}
			p, err := Compile(memory.NewSchema(memory.Field{Name: "x", Type: tc.typ}), In(Col("x"), candidates...))
			if err != nil {
				t.Fatal(err)
			}
			w, err := NewWorkspace(context.Background(), p, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			got, err := w.EvaluateBool(context.Background(), b, testKernelSet())
			if err != nil {
				t.Fatal(err)
			}
			if !memory.IsSet(got.Validity, 0) || memory.IsSet(got.Truth, 0) != inOracleEqual(tc.needle, tc.value) {
				t.Fatalf("%v IN %v: got %+v", tc.needle, tc.value, got)
			}
		})
	}
}

func TestLiteralInSelectedDemandAndBudget(t *testing.T) {
	p, err := Compile(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}), In(CastTo(Div(I64(100), Col("x")), memory.TypeInt64), literalInInts(64)...))
	if err != nil {
		t.Fatal(err)
	}
	if !p.SupportsSelection() {
		t.Fatal("literal sets lost the existing fixed-width selection path")
	}
	b := memory.NewBatch()
	b.Rows = 4
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{0, 2, 4, 0}}
	m := memory.NewBudgetManager(0)
	w, err := NewWorkspace(context.Background(), p, m)
	if err != nil {
		t.Fatal(err)
	}
	for _, rows := range [][]int{{1, 2}, {2}, {}, {1, 2}} {
		out, err := w.EvaluateSelected(context.Background(), b, rows, testKernelSet())
		if err != nil {
			t.Fatal("unselected divide by zero was evaluated", err)
		}
		for row := range rows {
			if !out[0].Uint32.IsValid(row) || out[0].Uint32.Values[row] != 1 {
				t.Fatal("wrong selected membership")
			}
		}
		if m.Used() != w.MemoryBytes() {
			t.Fatal("selected accounting mismatch")
		}
	}
	if _, err := w.EvaluateSelected(context.Background(), b, []int{0}, testKernelSet()); err == nil {
		t.Fatal("demanded division error suppressed")
	}
	if m.Used() != 0 || w.MemoryBytes() != 0 {
		t.Fatal("selected failure retained memory")
	}
}
