package expr

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
)

type constantReviewCancelManager struct {
	*memory.BudgetManager
	bytes  int64
	cancel context.CancelFunc
}

func (m *constantReviewCancelManager) Reserve(ctx context.Context, bytes int64) (memory.Reservation, error) {
	lease, err := m.BudgetManager.Reserve(ctx, bytes)
	if err == nil && bytes == m.bytes {
		m.cancel()
	}
	return lease, err
}

func TestCompactReviewCancellationDuringExport(t *testing.T) {
	const rows = 65
	for _, tc := range []struct {
		expr  Expr
		bytes int64
	}{
		{I64(7), rows * 8}, {F64(0.25), rows * 8}, {Date32(2), rows * 4}, {Str("x"), rows},
	} {
		t.Run(fmt.Sprint(tc.expr), func(t *testing.T) {
			p, err := Compile(memory.NewSchema(), tc.expr)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			manager := &constantReviewCancelManager{BudgetManager: memory.NewBudgetManager(0), bytes: tc.bytes, cancel: cancel}
			w, err := NewWorkspace(ctx, p, manager)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			b := memory.NewBatch()
			b.Rows = rows
			values, err := w.Evaluate(ctx, b, testKernelSet())
			if !errors.Is(ctx.Err(), context.Canceled) {
				t.Fatal("export allocation did not trigger cancellation")
			}
			if !errors.Is(err, context.Canceled) || values != nil || !w.closed || manager.Used() != 0 {
				t.Fatalf("canceled export: err=%v values=%d closed=%v retained=%d", err, len(values), w.closed, manager.Used())
			}
		})
	}
}

// The reference uses the pre-compaction representation and dedicated owners.
// Clone nested regions too: immutable plans must not be changed by a test or
// by one workspace sharing them with another execution.
func constantReviewDense(p Program) Program {
	p.instructions = append([]instruction(nil), p.instructions...)
	for i := range p.instructions {
		in := &p.instructions[i]
		in.compactLiteral, in.storageOwner = false, 0
		in.Regions = append([]Program(nil), in.Regions...)
		for region := range in.Regions {
			in.Regions[region] = constantReviewDense(in.Regions[region])
		}
	}
	return p
}

// Snapshot valid logical values rather than undefined payloads under NULLs.
// Floating values use their bits so NaN payloads and signed zero are compared.
func constantReviewValues(v ScalarValue, rows int) any {
	if v.Struct != nil {
		return v.Struct
	}
	values := make([]any, rows)
	for row := range rows {
		switch {
		case v.String != nil:
			if v.String.IsValid(row) {
				values[row] = string(v.String.ValueBytes(row))
			}
		case v.StringList != nil:
			lo, hi, valid := v.StringList.ValueRange(row)
			if valid {
				children := make([]any, hi-lo)
				for child := lo; child < hi; child++ {
					if v.StringList.Values.IsValid(child) {
						children[child-lo] = string(v.StringList.Values.ValueBytes(child))
					}
				}
				values[row] = children
			}
		default:
			value := reflect.ValueOf(v)
			for field := range value.NumField() {
				vector := value.Field(field)
				if vector.Kind() != reflect.Pointer || vector.IsNil() {
					continue
				}
				payload := vector.Elem().FieldByName("Values")
				if !payload.IsValid() || payload.Kind() != reflect.Slice {
					continue
				}
				validity := vector.Elem().FieldByName("Validity").Interface().(memory.Bitmap)
				if validity == nil || memory.IsSet(validity, row) {
					cell := payload.Index(row)
					switch cell.Kind() {
					case reflect.Float32:
						values[row] = math.Float32bits(float32(cell.Float()))
					case reflect.Float64:
						values[row] = math.Float64bits(cell.Float())
					default:
						values[row] = cell.Interface()
					}
				}
				break
			}
		}
	}
	return values
}

func TestCompactReviewConsumerDifferential(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "b", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "c", Type: memory.TypeBool, Nullable: true}, memory.Field{Name: "s", Type: memory.TypeString, Nullable: true})
	jsonSchema := memory.NewSchema(memory.Field{Name: "n", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "s", Type: memory.TypeString, Nullable: true})
	cases := []struct {
		name string
		expr Expr
	}{
		{"int-root", I64(math.MinInt64)}, {"float-root", F64(math.Copysign(0, -1))}, {"nan-root", F64(math.Float64frombits(0x7ff8000000000042))},
		{"string-root", Str("雪\x00abc")}, {"empty-string-root", Str("")}, {"date-root", Date32(-25)}, {"timestamp-root", Timestamp(123)},
		{"compare-integer", GT(Add(Col("a"), Col("b")), I64(-3))}, {"compare-floating", LT(F64(0.25), CastTo(Col("a"), memory.TypeFloat64))},
		{"compare-string", EQ(Str("ab"), Trim(Col("s")))}, {"compare-infinity", GT(F64(math.Inf(1)), CastTo(Col("a"), memory.TypeFloat64))},
		{"strict-cast", CastTo(I64(-17), memory.TypeFloat64)}, {"float-to-uint", CastTo(F64(42), memory.TypeUint64)},
		{"string-to-integer", CastTo(Str("-123"), memory.TypeInt16)}, {"try-invalid", TryCastTo(Str("bad"), memory.TypeInt64)},
		{"try-nan", TryCastTo(F64(math.NaN()), memory.TypeInt64)}, {"numeric-to-string", CastTo(I64(math.MinInt64), memory.TypeString)},
		{"timestamp-string", CastTo(Str("2020-01-02T03:04:05Z"), memory.TypeTimestamp)},
		{"length", Length(Str("a雪b"))}, {"byte-length", ByteLength(Str("a雪b"))}, {"year", Year(Date32(18262))},
		{"trim", Trim(Str(" 雪 \n"))}, {"trim-start", TrimStart(Str(" abc "))}, {"trim-end", TrimEnd(Str(" abc "))},
		{"lower", Lower(Str("ÄBC"))}, {"upper", Upper(Str("äbc"))},
		{"concat", Concat(Str("雪\x00"), Col("s"))}, {"find", Find(Str("ab雪cd"), Str("雪"))}, {"split", Split(Str("a::雪::"), Str("::"))},
		{"contains", Contains(Trim(Col("s")), Str("a"))}, {"starts", StartsWith(Str("abc"), Str("ab"))}, {"ends", EndsWith(Str("abc"), Str("bc"))},
		{"regex-match", RegexMatch(Str("abc123"), Str("[0-9]+"))},
		{"slice", Slice(Str("ab雪cd"), I64(1), I64(3))}, {"replace", Replace(Str("abab"), Str("ab"), Str("雪"))},
		{"replace-all", ReplaceAll(Str("abab"), Str("ab"), Str("雪"))}, {"regex-extract", RegexExtract(Str("ab12cd34"), Str("([0-9]+)"), I64(1))},
		{"regex-all", RegexExtractAll(Str("ab12cd34"), Str("([0-9]+)"), I64(1))},
		{"json", JSONDecode(Str(`{"n":123,"s":"雪"}`), jsonSchema)},
		{"is-null", IsNull(I64(7))}, {"is-not-null", IsNotNull(Str(""))},
		{"literal-in", In(I64(7), I64(3), I64(7), Null(memory.TypeInt64))},
		{"dynamic-in", In(I64(7), Col("a"), Add(Col("b"), I64(5)))},
		{"if-string", If(Col("c"), Str("yes"), Str("no"))},
		{"if-scratch", If(Col("c"), CastTo(I64(-1), memory.TypeFloat64), CastTo(CastTo(F64(3), memory.TypeUint64), memory.TypeFloat64))},
		{"case-scratch", Case([]WhenThen{{When: Col("c"), Then: CastTo(I64(-7), memory.TypeFloat64)}, {When: IsNull(Col("a")), Then: Add(F64(3), F64(4))}}, F64(11))},
		{"coalesce-scratch", Coalesce(CastTo(Col("a"), memory.TypeFloat64), F64(2.25), F64(9))},
		{"guarded-string-error", If(Col("c"), I64(7), If(Bool(false), CastTo(Str("bad"), memory.TypeInt64), I64(9)))},
		{"dense-arithmetic", Add(I64(5), Col("a"))}, {"dense-math", Abs(I64(-7))},
	}
	for _, tc := range cases {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			t.Run(tc.name+"/"+mode.String(), func(t *testing.T) {
				p, err := CompileExpressions(schema, []NamedExpr{Named(tc.expr, "value"), Named(tc.expr, "alias")})
				if err != nil {
					t.Fatal(err)
				}
				programs := []Program{p, constantReviewDense(p)}
				var workspaces [2]*Workspace
				var managers [2]*memory.BudgetManager
				for i := range programs {
					managers[i] = memory.NewBudgetManager(0)
					workspaces[i], err = NewWorkspace(context.Background(), programs[i], managers[i])
					if err != nil {
						t.Fatal(err)
					}
					defer workspaces[i].Close()
				}
				for run, rows := range []int{65, 1, 0, 129, 3, 64, 65} {
					b := differentialLivenessBatch(rows, run)
					var got [2][]ScalarValue
					for i, w := range workspaces {
						got[i], err = w.Evaluate(context.Background(), b, kernel.New(mode))
						if err != nil {
							t.Fatalf("rows=%d executor=%d: %v", rows, i, err)
						}
					}
					for output := range got[0] {
						if a, b := constantReviewValues(got[0][output], rows), constantReviewValues(got[1][output], rows); got[0][output].Type != got[1][output].Type || !reflect.DeepEqual(a, b) {
							t.Fatalf("rows=%d output=%d compact=%v dense=%v", rows, output, a, b)
						}
					}
				}
				for i, w := range workspaces {
					w.Close()
					if managers[i].Used() != 0 {
						t.Fatalf("executor=%d retained=%d", i, managers[i].Used())
					}
				}
			})
		}
	}
}

func TestCompactReviewEveryExportAdmissionRetry(t *testing.T) {
	text := strings.Repeat("雪\x00a", 37)
	p, err := CompileExpressions(memory.NewSchema(), []NamedExpr{Named(Str(text), "s"), Named(Str(text), "alias"), Named(I64(-7), "i"), Named(F64(0.25), "f")})
	if err != nil {
		t.Fatal(err)
	}
	b := memory.NewBatch()
	b.Rows = 65
	probe := &livenessFaultManager{BudgetManager: memory.NewBudgetManager(0)}
	w, _ := NewWorkspace(context.Background(), p, probe)
	if _, err := w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	calls := probe.calls
	w.Close()
	for failAt := 1; failAt <= calls; failAt++ {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			manager := &livenessFaultManager{BudgetManager: memory.NewBudgetManager(0), failAt: failAt, failure: memory.ErrBudgetExceeded}
			w, err := NewWorkspace(context.Background(), p, manager)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			b.Rows = 65
			if values, err := w.Evaluate(context.Background(), b, testKernelSet()); !errors.Is(err, memory.ErrBudgetExceeded) || values != nil || w.closed || manager.Used() != w.MemoryBytes() {
				t.Fatalf("failure=%v values=%d closed=%v used=%d capacity=%d", err, len(values), w.closed, manager.Used(), w.MemoryBytes())
			}
			manager.failAt = 0
			for _, rows := range []int{65, 1, 0, 129, 3} {
				b.Rows = rows
				values, err := w.Evaluate(context.Background(), b, testKernelSet())
				if err != nil || manager.Used() != w.MemoryBytes() {
					t.Fatalf("rows=%d retry=%v used=%d capacity=%d", rows, err, manager.Used(), w.MemoryBytes())
				}
				for row := range rows {
					if string(values[0].String.ValueBytes(row)) != text || string(values[1].String.ValueBytes(row)) != text || values[2].Int64.Values[row] != -7 || values[3].Float64.Values[row] != 0.25 {
						t.Fatalf("rows=%d row=%d retry changed value", rows, row)
					}
				}
			}
			w.Close()
			if manager.Used() != 0 {
				t.Fatalf("retained=%d", manager.Used())
			}
		})
	}
}

func TestCompactReviewStorageClassesAndSelection(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "c", Type: memory.TypeBool, Nullable: true})
	p, err := CompileExpressions(schema, []NamedExpr{
		Named(GT(Add(Col("a"), I64(1)), I64(7)), "comparison"),
		Named(If(Col("c"), CastTo(I64(-3), memory.TypeFloat64), CastTo(Add(Col("a"), I64(5)), memory.TypeFloat64)), "guarded"),
		Named(I64(17), "root"), Named(I64(17), "alias"),
	})
	if err != nil {
		t.Fatal(err)
	}
	var inspect func(Program)
	inspect = func(p Program) {
		classes := map[int]bool{}
		for i, in := range p.instructions {
			owner := storageIndex(p.instructions, i)
			if compact, ok := classes[owner]; ok {
				if compact && !in.compactLiteral {
					t.Fatalf("physical owner %d requires dense capacity after compact introduction", owner)
				}
			} else {
				classes[owner] = in.compactLiteral
			}
			for _, region := range in.Regions {
				inspect(region)
			}
		}
	}
	inspect(p)
	if !p.SupportsSelection() {
		t.Fatal("fixture must use selected evaluation")
	}
	w, err := NewWorkspace(context.Background(), p, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for round := range 4 {
		b := differentialLivenessBatch(129, round)
		for _, selected := range [][]int{{}, {0, 64, 128}, {1}, nil} {
			got, err := w.EvaluateSelected(context.Background(), b, selected, testKernelSet())
			if err != nil {
				t.Fatal(err)
			}
			indices := selected
			if selected == nil {
				indices = make([]int, b.Rows)
				for i := range indices {
					indices[i] = i
				}
			}
			if got[2].Int64.Len() != len(indices) {
				t.Fatal("constant export used source rows rather than selected rows")
			}
			for output, input := range indices {
				a, c := b.Int64s["a"], b.Uint32s["c"]
				valid, truth := a.IsValid(input), a.Values[input]+1 > 7
				if got[0].Uint32.IsValid(output) != valid || (valid && (got[0].Uint32.Values[output] != 0) != truth) {
					t.Fatal("selected comparison changed")
				}
				want, valid := float64(a.Values[input]+5), a.IsValid(input)
				if c.IsValid(input) && c.Values[input] != 0 {
					want, valid = -3, true
				}
				if got[1].Float64.IsValid(output) != valid || (valid && got[1].Float64.Values[output] != want) || got[2].Int64.Values[output] != 17 || got[3].Int64.Values[output] != 17 {
					t.Fatal("selected guarded or exported constant changed")
				}
			}
		}
	}
}

func TestCompactReviewDenseCompactDenseReuse(t *testing.T) {
	p, err := CompileExpressions(memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64}), []NamedExpr{
		Named(GT(Add(Col("a"), I64(1)), I64(50)), "test"),
		Named(Add(Col("a"), I64(7)), "sum"),
	})
	if err != nil {
		t.Fatal(err)
	}
	indices := map[int64]int{}
	for i, in := range p.instructions {
		if in.Code == scalarLiteralI64 {
			indices[in.I64] = i
		}
	}
	one, fifty, seven := indices[1], indices[50], indices[7]
	if len(indices) != 3 || p.instructions[one].compactLiteral || !p.instructions[fifty].compactLiteral || p.instructions[seven].compactLiteral || storageIndex(p.instructions, one) != storageIndex(p.instructions, fifty) || storageIndex(p.instructions, fifty) != storageIndex(p.instructions, seven) {
		t.Fatal("fixture must reuse one initially dense owner as dense, compact, then dense")
	}
	manager := memory.NewBudgetManager(0)
	w, err := NewWorkspace(context.Background(), p, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for round, rows := range []int{65, 1, 0, 129, 3, 65} {
		b := memory.NewBatch()
		b.Rows = rows
		input := make([]int64, rows)
		for row := range input {
			input[row] = int64(row + round)
		}
		b.Int64s["a"] = &memory.Int64Vector{Values: input}
		values, err := w.Evaluate(context.Background(), b, testKernelSet())
		if err != nil || manager.Used() != w.MemoryBytes() {
			t.Fatalf("rows=%d error=%v charged=%d retained=%d", rows, err, manager.Used(), w.MemoryBytes())
		}
		for row, value := range input {
			if (values[0].Uint32.Values[row] != 0) != (value+1 > 50) || values[1].Int64.Values[row] != value+7 {
				t.Fatalf("rows=%d row=%d reused owner changed output", rows, row)
			}
		}
	}
	w.Close()
	if manager.Used() != 0 {
		t.Fatalf("retained=%d", manager.Used())
	}
}

func TestCompactReviewConcurrentGuardedPlan(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "c", Type: memory.TypeBool, Nullable: true})
	p, err := CompileExpressions(schema, []NamedExpr{
		Named(If(Col("c"), Str("雪"), Str("no")), "string"),
		Named(If(Col("c"), CastTo(I64(-3), memory.TypeFloat64), F64(0.5)), "numeric"),
		Named(Str("雪"), "root"),
	})
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for worker := range 6 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			manager := memory.NewBudgetManager(0)
			w, err := NewWorkspace(context.Background(), p, manager)
			if err != nil {
				t.Error(err)
				return
			}
			defer w.Close()
			for run := range 12 {
				rows := []int{0, 1, 65, 129}[(worker+run)%4]
				b := differentialLivenessBatch(rows, worker+run)
				values, err := w.Evaluate(context.Background(), b, testKernelSet())
				if err != nil {
					t.Error(err)
					return
				}
				for row := range rows {
					s, n := "no", 0.5
					if c := b.Uint32s["c"]; c.IsValid(row) && c.Values[row] != 0 {
						s, n = "雪", -3
					}
					if string(values[0].String.ValueBytes(row)) != s || values[1].Float64.Values[row] != n || string(values[2].String.ValueBytes(row)) != "雪" {
						t.Errorf("shared plan changed in worker=%d run=%d row=%d", worker, run, row)
						return
					}
				}
			}
			w.Close()
			if manager.Used() != 0 {
				t.Errorf("worker=%d retained=%d", worker, manager.Used())
			}
		}()
	}
	workers.Wait()
}
