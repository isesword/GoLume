package expr

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"gocolumn/kernel"
	"gocolumn/memory"
)

// withoutStoragePlan clones every region before removing ownership assignments.
// The reference evaluator then retains one physical owner per logical register,
// matching execution before liveness planning without changing the compiled DAG.
func withoutStoragePlan(p Program) Program {
	p.instructions = append([]instruction(nil), p.instructions...)
	for i := range p.instructions {
		in := &p.instructions[i]
		in.storageOwner = 0
		in.Regions = append([]Program(nil), in.Regions...)
		for j := range in.Regions {
			in.Regions[j] = withoutStoragePlan(in.Regions[j])
		}
	}
	return p
}

func TestLivenessSeededDAGDifferential(t *testing.T) {
	const seed = 91823712
	schema := memory.NewSchema(
		memory.Field{Name: "a", Type: memory.TypeInt64, Nullable: true},
		memory.Field{Name: "b", Type: memory.TypeInt64, Nullable: true},
		memory.Field{Name: "c", Type: memory.TypeBool, Nullable: true},
		memory.Field{Name: "s", Type: memory.TypeString, Nullable: true},
	)
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			for trial := range 128 {
				numbers := []Expr{Col("a"), Col("b"), I64(0), I64(1), I64(2), Null(memory.TypeInt64), Length(Col("s"))}
				booleans := []Expr{Col("c"), Bool(true), Bool(false), Null(memory.TypeBool), GT(Col("a"), I64(3)), Contains(Col("s"), Str("a"))}
				pickNumber := func() Expr { return numbers[rng.Intn(len(numbers))] }
				pickBoolean := func() Expr { return booleans[rng.Intn(len(booleans))] }
				for range 45 {
					a, b := pickNumber(), pickNumber()
					switch rng.Intn(14) {
					case 0:
						numbers = append(numbers, Add(a, b))
					case 1:
						numbers = append(numbers, Sub(a, b))
					case 2:
						numbers = append(numbers, If(pickBoolean(), a, b))
					case 3:
						numbers = append(numbers, Coalesce(a, b, I64(5)))
					case 4:
						numbers = append(numbers, Case([]WhenThen{{When: pickBoolean(), Then: a}, {When: pickBoolean(), Then: b}}, I64(11)))
					case 5:
						numbers = append(numbers, CastTo(CastTo(a, memory.TypeFloat64), memory.TypeInt64))
					case 6:
						booleans = append(booleans, GT(a, b))
					case 7:
						booleans = append(booleans, Not(pickBoolean()))
					case 8:
						booleans = append(booleans, And(pickBoolean(), pickBoolean()))
					case 9:
						booleans = append(booleans, Or(pickBoolean(), pickBoolean()))
					case 10:
						booleans = append(booleans, IsNull(a))
					case 11:
						booleans = append(booleans, In(a, b, I64(7), pickNumber()))
					case 12:
						booleans = append(booleans, If(pickBoolean(), pickBoolean(), pickBoolean()))
					case 13:
						booleans = append(booleans, CastTo(pickBoolean(), memory.TypeBool))
					}
				}
				named := []NamedExpr{Named(numbers[len(numbers)-1], "number"), Named(booleans[len(booleans)-1], "boolean")}
				if trial%2 == 0 {
					for i := range 5 {
						named = append(named, Named(pickNumber(), fmt.Sprint("n", i)), Named(pickBoolean(), fmt.Sprint("p", i)))
					}
				}
				named = append(named, Named(numbers[len(numbers)-1], "duplicate_number"), Named(booleans[len(booleans)-1], "duplicate_boolean"))
				planned, err := CompileExpressions(schema, named)
				if err != nil {
					t.Fatalf("seed=%d trial=%d compile: %v", seed, trial, err)
				}
				checkLivenessDifferential(t, seed, trial, mode, planned)
			}
		})
	}
}

func checkLivenessDifferential(t *testing.T, seed, trial int, mode kernel.Mode, planned Program) {
	t.Helper()
	var workspaces [2]*Workspace
	var managers [2]*memory.BudgetManager
	for i, program := range []Program{planned, withoutStoragePlan(planned)} {
		managers[i] = memory.NewBudgetManager(0)
		var err error
		workspaces[i], err = NewWorkspace(context.Background(), program, managers[i])
		if err != nil {
			t.Fatal(err)
		}
		defer workspaces[i].Close()
	}
	for run, rows := range []int{65, 1, 0, 129, 3, 255, 64, 130} {
		batch := differentialLivenessBatch(rows, run)
		var results [2][]ScalarValue
		for i, workspace := range workspaces {
			var err error
			results[i], err = workspace.Evaluate(context.Background(), batch, kernel.New(mode))
			if err != nil {
				t.Fatalf("seed=%d trial=%d run=%d executor=%d: %v", seed, trial, run, i, err)
			}
		}
		for column, got := range results[0] {
			want := results[1][column]
			for row := range rows {
				var gotValid, wantValid bool
				var gotValue, wantValue int64
				if got.Type == memory.TypeBool {
					gotValid, wantValid = got.Uint32.IsValid(row), want.Uint32.IsValid(row)
					gotValue, wantValue = int64(got.Uint32.Values[row]), int64(want.Uint32.Values[row])
				} else {
					gotValid, wantValid = got.Int64.IsValid(row), want.Int64.IsValid(row)
					gotValue, wantValue = got.Int64.Values[row], want.Int64.Values[row]
				}
				if gotValid != wantValid || (gotValid && gotValue != wantValue) {
					t.Fatalf("seed=%d trial=%d run=%d column=%d row=%d: planned=%d/%v unplanned=%d/%v", seed, trial, run, column, row, gotValue, gotValid, wantValue, wantValid)
				}
			}
		}
	}
	for i, workspace := range workspaces {
		if err := workspace.Close(); err != nil || managers[i].Used() != 0 || workspace.MemoryBytes() != 0 {
			t.Fatalf("seed=%d trial=%d executor=%d close: err=%v used=%d retained=%d", seed, trial, i, err, managers[i].Used(), workspace.MemoryBytes())
		}
	}
}

func differentialLivenessBatch(rows, run int) *memory.Batch {
	batch := memory.NewBatch()
	batch.Rows = rows
	a, b := make([]int64, rows), make([]int64, rows)
	c := make([]uint32, rows)
	va, vb := make(memory.Bitmap, memory.WordsFor(rows)), make(memory.Bitmap, memory.WordsFor(rows))
	vc, vs := make(memory.Bitmap, memory.WordsFor(rows)), make(memory.Bitmap, memory.WordsFor(rows))
	offsets := make([]uint32, rows+1)
	var data []byte
	for row := range rows {
		a[row], b[row], c[row] = int64((row+run)%13-6), int64((3*row+run)%9-4), uint32((row+run)%2)
		for i, validity := range []memory.Bitmap{va, vb, vc, vs} {
			if (row+run)%[]int{5, 7, 3, 4}[i] != 0 {
				memory.Set(validity, row)
			}
		}
		data = append(data, []string{"ab", "bc", "abc", ""}[(row+run)%4]...)
		offsets[row+1] = uint32(len(data))
	}
	batch.Int64s["a"] = &memory.Int64Vector{Values: a, Validity: va}
	batch.Int64s["b"] = &memory.Int64Vector{Values: b, Validity: vb}
	batch.Uint32s["c"] = &memory.Uint32Vector{Values: c, Validity: vc}
	batch.Strings["s"] = &memory.StringViewVector{Offsets: offsets, Data: data, Validity: vs}
	return batch
}

type livenessFaultManager struct {
	*memory.BudgetManager
	calls, failAt int
	failure       error
}

func (m *livenessFaultManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	if m.calls == m.failAt {
		return nil, m.failure
	}
	return m.BudgetManager.Reserve(ctx, n)
}

func TestLivenessPooledBooleanExportAdmission(t *testing.T) {
	var expression Expr = GT(Col("a"), I64(3))
	for range 31 {
		expression = Not(expression)
	}
	program, err := Compile(memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64, Nullable: true}), expression)
	if err != nil {
		t.Fatal(err)
	}
	if storageIndex(program.instructions, program.root) == program.root {
		t.Fatal("fixture must export a root whose physical owner differs from its logical register")
	}
	// The fixed workspace, result headers, and BOOL-to-uint32 expansion are
	// separately admitted. Reject each stage, then check both retry and close.
	for _, failure := range []error{memory.ErrBudgetExceeded, context.Canceled} {
		for failAt := 1; failAt <= 3; failAt++ {
			manager := &livenessFaultManager{BudgetManager: memory.NewBudgetManager(0), failAt: failAt, failure: failure}
			workspace, err := NewWorkspace(context.Background(), program, manager)
			if err != nil {
				t.Fatal(err)
			}
			batch := differentialLivenessBatch(65, 0)
			if _, err := workspace.Evaluate(context.Background(), batch, testKernelSet()); !errors.Is(err, failure) {
				t.Fatalf("failAt=%d: error=%v want=%v", failAt, err, failure)
			}
			if errors.Is(failure, memory.ErrBudgetExceeded) {
				manager.failAt = 0
				values, err := workspace.Evaluate(context.Background(), batch, testKernelSet())
				if err != nil {
					t.Fatalf("failAt=%d retry: %v", failAt, err)
				}
				input, got := batch.Int64s["a"], values[0].Uint32
				for row := range batch.Rows {
					if got.IsValid(row) != input.IsValid(row) || (got.IsValid(row) && (got.Values[row] != 0) != (input.Values[row] <= 3)) {
						t.Fatalf("failAt=%d retry row=%d value=%d valid=%v", failAt, row, got.Values[row], got.IsValid(row))
					}
				}
			} else if manager.Used() != 0 || !workspace.closed {
				t.Fatalf("failAt=%d cancellation retained %d bytes, closed=%v", failAt, manager.Used(), workspace.closed)
			}
			if err := workspace.Close(); err != nil || manager.Used() != 0 {
				t.Fatalf("failAt=%d close: error=%v retained=%d", failAt, err, manager.Used())
			}
		}
	}
}
