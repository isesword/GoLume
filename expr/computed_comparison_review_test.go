package expr

import (
	"context"
	"fmt"
	"math"
	"testing"

	"gocolumn/kernel"
	"gocolumn/memory"
)

var computedReviewOps = []struct {
	name  string
	build func(Expr, Expr) Binary
	want  func(float64, float64) bool
}{
	{"GT", GT, func(a, b float64) bool { return a > b }},
	{"GE", GE, func(a, b float64) bool { return a >= b }},
	{"LT", LT, func(a, b float64) bool { return a < b }},
	{"LE", LE, func(a, b float64) bool { return a <= b }},
	{"EQ", EQ, func(a, b float64) bool { return a == b }},
}

// Casts are real computed registers. Values stay exactly representable in every
// tested type so the oracle is independent of the evaluator's coercion helpers.
func TestComputedComparisonReviewAllNumericTypes(t *testing.T) {
	types := []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64}
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "y", Type: memory.TypeInt64, Nullable: true})
	for _, typ := range types {
		for _, op := range computedReviewOps {
			t.Run(fmt.Sprint(typ, "/", op.name), func(t *testing.T) {
				left := CastTo(Add(Col("x"), I64(1)), typ)
				right := CastTo(Add(Col("y"), I64(1)), typ)
				p, err := Compile(schema, op.build(left, right))
				if err != nil {
					t.Fatal(err)
				}
				for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
					var w evaluationState
					for _, n := range []int{257, 1, 65, 0, 129, 63, 256} {
						b := memory.NewBatch()
						b.Rows = n
						x := &memory.Int64Vector{Values: make([]int64, n), Validity: make(memory.Bitmap, memory.WordsFor(n))}
						y := &memory.Int64Vector{Values: make([]int64, n), Validity: make(memory.Bitmap, memory.WordsFor(n))}
						b.Int64s["x"], b.Int64s["y"] = x, y
						for i := range n {
							x.Values[i] = int64(i % 101)
							y.Values[i] = int64((i * 7) % 101)
							if i%7 != 0 {
								memory.Set(x.Validity, i)
							}
							if i%11 != 0 {
								memory.Set(y.Validity, i)
							}
						}
						got, err := evaluateBoolTest(p, b, &w, kernel.New(mode))
						if err != nil {
							t.Fatal(err)
						}
						for i := range n {
							valid := i%7 != 0 && i%11 != 0
							want := valid && op.want(float64(x.Values[i]+1), float64(y.Values[i]+1))
							if memory.IsSet(got.Validity, i) != valid || memory.IsSet(got.Truth, i) != want {
								t.Fatalf("mode=%v n=%d row=%d got truth=%v valid=%v want %v/%v", mode, n, i, memory.IsSet(got.Truth, i), memory.IsSet(got.Validity, i), want, valid)
							}
						}
					}
					w.Close()
				}
			})
		}
	}
}

func TestComputedComparisonReviewFloatSpecialsAndLiteralSides(t *testing.T) {
	values := []float64{math.NaN(), math.Inf(-1), math.Inf(1), math.Copysign(0, -1), 0, -3, 3, 1.5}
	for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeFloat64} {
		schema := memory.NewSchema(memory.Field{Name: "x", Type: typ, Nullable: true}, memory.Field{Name: "y", Type: typ, Nullable: true})
		for _, op := range computedReviewOps {
			for _, side := range []string{"dense", "left", "right"} {
				t.Run(fmt.Sprint(typ, "/", op.name, "/", side), func(t *testing.T) {
					// Multiplication by one preserves signed zero, infinities and NaN.
					var a Expr = CastTo(Mul(Col("x"), F64(1)), typ)
					var b Expr = CastTo(Mul(Col("y"), F64(1)), typ)
					if typ == memory.TypeFloat32 {
						a = Coalesce(Col("x"), Null(typ))
						b = Coalesce(Col("y"), Null(typ))
					}
					if side == "left" {
						a = F64(0)
					}
					if side == "right" {
						b = F64(0)
					}
					p, err := Compile(schema, op.build(a, b))
					if err != nil {
						t.Fatal(err)
					}
					batch := memory.NewBatch()
					batch.Rows = 137
					x := &memory.Float64Vector{Values: make([]float64, 137), Validity: make(memory.Bitmap, memory.WordsFor(137))}
					y := &memory.Float64Vector{Values: make([]float64, 137), Validity: make(memory.Bitmap, memory.WordsFor(137))}
					batch.Float64s["x"], batch.Float64s["y"] = x, y
					for i := range batch.Rows {
						x.Values[i] = values[i%len(values)]
						y.Values[i] = values[(i/len(values))%len(values)]
						if i%13 != 0 {
							memory.Set(x.Validity, i)
						}
						if i%17 != 0 {
							memory.Set(y.Validity, i)
						}
					}

					if typ == memory.TypeFloat32 {
						xf := &memory.Float32Vector{Values: make([]float32, batch.Rows), Validity: x.Validity}
						yf := &memory.Float32Vector{Values: make([]float32, batch.Rows), Validity: y.Validity}
						for i := range batch.Rows {
							xf.Values[i] = float32(x.Values[i])
							yf.Values[i] = float32(y.Values[i])
						}
						batch.Float32s["x"], batch.Float32s["y"] = xf, yf
					}
					for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
						var w evaluationState
						got, err := evaluateBoolTest(p, batch, &w, kernel.New(mode))
						if err != nil {
							t.Fatal(err)
						}
						for i := range batch.Rows {
							a, b := x.Values[i], y.Values[i]
							valid := i%13 != 0 && i%17 != 0
							if side == "left" {
								a = 0
								valid = i%17 != 0
							}
							if side == "right" {
								b = 0
								valid = i%13 != 0
							}
							want := valid && op.want(a, b)
							if memory.IsSet(got.Validity, i) != valid || memory.IsSet(got.Truth, i) != want {
								t.Fatalf("mode=%v row=%d a=%v b=%v truth=%v valid=%v want=%v/%v", mode, i, a, b, memory.IsSet(got.Truth, i), memory.IsSet(got.Validity, i), want, valid)
							}
						}
						w.Close()
					}
				})
			}
		}
	}
}

// These results require exact mixed-domain ordering, not float64 conversion.
func TestComputedComparisonReviewExactFallbackAndIntegerEdges(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "i", Type: memory.TypeInt64}, memory.Field{Name: "u", Type: memory.TypeUint64}, memory.Field{Name: "f", Type: memory.TypeFloat64})
	batch := memory.NewBatch()
	batch.Rows = 5
	batch.Int64s["i"] = &memory.Int64Vector{Values: []int64{9007199254740993, -1, math.MaxInt64, math.MinInt64, 0}}
	batch.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{9007199254740992, math.MaxUint64, 1 << 63, 0, 0}}
	batch.Float64s["f"] = &memory.Float64Vector{Values: []float64{9007199254740992, 0, float64(math.MaxInt64), float64(math.MinInt64), math.Copysign(0, -1)}}
	i := Add(Col("i"), I64(0))
	f := Mul(Col("f"), F64(1))
	// Coalesce materializes the uint64 branch without narrowing its values.
	u := Coalesce(Col("u"), CastTo(I64(0), memory.TypeUint64))
	cases := []struct {
		name string
		e    Expr
		want []bool
	}{
		{"mixed_float_gt", GT(i, f), []bool{true, false, false, false, false}},
		{"mixed_float_eq", EQ(i, f), []bool{false, false, false, true, true}},
		{"mixed_unsigned_lt", LT(i, u), []bool{false, true, true, true, false}},
		{"mixed_unsigned_gt", GT(u, i), []bool{false, true, true, true, false}},
		{"int64_max_literal", LT(i, I64(math.MaxInt64)), []bool{true, true, false, true, true}},
		{"int64_min_left", LE(I64(math.MinInt64), i), []bool{true, true, true, true, true}},
		{"uint64_dense", EQ(u, u), []bool{true, true, true, true, true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := Compile(schema, tc.e)
			if err != nil {
				t.Fatal(err)
			}
			for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
				var w evaluationState
				got, err := evaluateBoolTest(p, batch, &w, kernel.New(mode))
				if err != nil {
					t.Fatal(err)
				}
				for row, want := range tc.want {
					if !memory.IsSet(got.Validity, row) || memory.IsSet(got.Truth, row) != want {
						t.Fatalf("mode=%v row=%d got=%v want=%v", mode, row, memory.IsSet(got.Truth, row), want)
					}
				}
				w.Close()
			}
		})
	}
}

func TestComputedComparisonReviewSelectedBranchesSharedOutputs(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	x := Add(Col("x"), I64(1))
	cmp := GT(x, I64(4))
	// The narrow cast must never see rows >= 128. CASE and IF both restrict
	// evaluation domains, including the computed comparison's input registers.
	guarded := LT(CastTo(x, memory.TypeInt8), CastTo(I64(4), memory.TypeInt8))
	branch := If(LT(Col("x"), I64(100)), guarded, cmp)
	choice := Case([]WhenThen{{When: LT(Col("x"), I64(100)), Then: guarded}}, cmp)
	p, err := CompileExpressions(schema, []NamedExpr{Named(cmp, "a"), Named(cmp, "shared"), Named(branch, "if"), Named(choice, "case"), Named(x, "live")})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		budget := memory.NewBudgetManager(0)
		w, err := NewWorkspace(context.Background(), p, budget)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range []int{257, 1, 65, 0, 129, 63} {
			b := memory.NewBatch()
			b.Rows = n
			v := &memory.Int64Vector{Values: make([]int64, n)}
			b.Int64s["x"] = v
			for row := range n {
				v.Values[row] = int64(row)
			}
			for _, step := range []int{1, 3, 17} {
				selected := []int{}
				for row := 0; row < n; row += step {
					selected = append(selected, row)
				}
				got, err := w.EvaluateSelected(context.Background(), b, selected, kernel.New(mode))
				if err != nil {
					t.Fatal(err)
				}
				for j, row := range selected {
					wantCmp := row+1 > 4
					wantBranch := wantCmp
					if row < 100 {
						wantBranch = row+1 < 4
					}
					for col, want := range []bool{wantCmp, wantCmp, wantBranch, wantBranch} {
						v := got[col].Uint32
						if !v.IsValid(j) || (v.Values[j] != 0) != want {
							t.Fatalf("mode=%v n=%d row=%d col=%d", mode, n, row, col)
						}
					}
					if got[4].Int64.Values[j] != int64(row+1) {
						t.Fatal("shared numeric output overwritten")
					}
				}
				if budget.Used() != w.MemoryBytes() {
					t.Fatalf("accounting charged=%d retained=%d", budget.Used(), w.MemoryBytes())
				}
			}
		}
		w.Close()
		if budget.Used() != 0 {
			t.Fatal("workspace leaked")
		}
	}
}

func TestComputedComparisonReviewWarmAllocationAndAccounting(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}, memory.Field{Name: "y", Type: memory.TypeInt64})
	p, err := Compile(schema, GT(Add(Col("x"), I64(1)), Add(Col("y"), I64(2))))
	if err != nil {
		t.Fatal(err)
	}
	b := memory.NewBatch()
	b.Rows = 257
	b.Int64s["x"] = &memory.Int64Vector{Values: make([]int64, b.Rows)}
	b.Int64s["y"] = &memory.Int64Vector{Values: make([]int64, b.Rows)}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		budget := memory.NewBudgetManager(0)
		var w evaluationState
		w.SetMemoryManager(context.Background(), budget)
		kernels := kernel.New(mode)
		if err := evaluateInstructions(p.instructions, b, &w, kernels); err != nil {
			t.Fatal(err)
		}
		charged := budget.Used()
		var evalErr error
		allocs := testing.AllocsPerRun(100, func() { evalErr = evaluateInstructions(p.instructions, b, &w, kernels) })
		if evalErr != nil {
			t.Fatal(evalErr)
		}
		if allocs != 0 {
			t.Fatalf("mode=%v warmed comparison allocated %v", mode, allocs)
		}
		if budget.Used() != charged || budget.Used() != w.MemoryBytes() {
			t.Fatal("warm evaluation changed accounting")
		}
		w.Close()
		if budget.Used() != 0 {
			t.Fatal("close leaked reservation")
		}
	}
}
