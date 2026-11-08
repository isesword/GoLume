package expr

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"

	"gocolumn/memory"
)

// Independent differential oracle: preserve identical literal values but wrap
// the first candidate in COALESCE to force the ordered guarded implementation.
func reviewInCompare(t *testing.T, schema memory.Schema, batch *memory.Batch, needle Expr, candidates []Expr) {
	t.Helper()
	slow := append([]Expr(nil), candidates...)
	slow[0] = Coalesce(slow[0], slow[0])
	var results [2][][2]bool
	for k, list := range [][]Expr{candidates, slow} {
		e := Expr(In(needle, list...))
		if k == 1 {
			if f, ok := schema.Field("x"); ok && f.Type == memory.TypeBinary {
				e = In(needle, candidates[:8]...)
				for start := 8; start < len(candidates); start += 8 {
					e = Or(e, In(needle, candidates[start:min(start+8, len(candidates))]...))
				}
			}
		}
		p, err := Compile(schema, e)
		if err != nil {
			t.Fatal(err)
		}
		if k == 0 && p.instructions[p.root].InSet == nil {
			t.Fatal("optimized path not selected")
		}
		if k == 1 && p.instructions[p.root].InSet != nil {
			t.Fatal("oracle unexpectedly optimized")
		}
		m := memory.NewBudgetManager(0)
		w, err := NewWorkspace(context.Background(), p, m)
		if err != nil {
			t.Fatal(err)
		}
		values, err := w.Evaluate(context.Background(), batch, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		r := values[0].Uint32
		for row := 0; row < batch.Rows; row++ {
			results[k] = append(results[k], [2]bool{r.IsValid(row), r.IsValid(row) && r.Values[row] != 0})
		}
		if m.Used() != w.MemoryBytes() {
			t.Fatalf("accounting used=%d retained=%d", m.Used(), w.MemoryBytes())
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("leaked budget")
		}
	}
	for row := range results[0] {
		if results[0][row] != results[1][row] {
			t.Fatalf("row %d optimized=%v guarded=%v", row, results[0][row], results[1][row])
		}
	}
}

func TestReviewLiteralInNumericDifferential(t *testing.T) {
	ints := []int64{math.MinInt64, math.MinInt64 + 1, -(1 << 53) - 1, -(1 << 53), -1, 0, 1, (1 << 53) - 1, 1 << 53, (1 << 53) + 1, math.MaxInt64 - 1, math.MaxInt64}
	floats := []float64{math.Inf(-1), -0x1p63, math.Nextafter(-0x1p63, 0), -0x1p53, -1.5, math.Copysign(0, -1), 0, 0.5, 1, 0x1p53, math.Nextafter(0x1p63, 0), 0x1p63, math.Nextafter(0x1p64, 0), 0x1p64, math.Inf(1), math.NaN()}
	for _, nullable := range []bool{false, true} {
		for seed := 0; seed < 9; seed++ {
			candidates := []Expr{}
			for j := 0; j < 32; j++ {
				if j%2 == 0 {
					candidates = append(candidates, I64(ints[(j+seed)%len(ints)]))
				} else {
					candidates = append(candidates, F64(floats[(j*3+seed)%len(floats)]))
				}
			}
			if nullable {
				candidates = append(candidates, Null(memory.TypeInt64))
			}
			t.Run(fmt.Sprintf("signed/%v/%d", nullable, seed), func(t *testing.T) {
				b := memory.NewBatch()
				b.Rows = len(ints)
				b.Int64s["x"] = &memory.Int64Vector{Values: ints}
				reviewInCompare(t, memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}), b, Col("x"), candidates)
			})
			t.Run(fmt.Sprintf("floating/%v/%d", nullable, seed), func(t *testing.T) {
				b := memory.NewBatch()
				b.Rows = len(floats)
				b.Float64s["x"] = &memory.Float64Vector{Values: floats}
				reviewInCompare(t, memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeFloat64}), b, Col("x"), candidates)
			})
			t.Run(fmt.Sprintf("unsigned/%v/%d", nullable, seed), func(t *testing.T) {
				values := []uint64{0, 1, 1 << 53, (1 << 53) + 1, math.MaxInt64, 1 << 63, (1 << 63) + 1, math.MaxUint64 - 1, math.MaxUint64}
				b := memory.NewBatch()
				b.Rows = len(values)
				b.Uint64s["x"] = &memory.Uint64Vector{Values: values}
				reviewInCompare(t, memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeUint64}), b, Col("x"), candidates)
			})
		}
	}
}

func TestReviewLiteralInStringDictionaryAndBinary(t *testing.T) {
	texts := []string{"", "a", "\x00", "\xff\x00", "漢字", "absent"}
	data := []byte{}
	offsets := []uint32{0}
	for _, s := range texts {
		data = append(data, []byte(s)...)
		offsets = append(offsets, uint32(len(data)))
	}
	for _, typ := range []memory.DataType{memory.TypeString, memory.TypeBinary} {
		for _, dictionary := range []bool{false, true} {
			for _, nullCandidate := range []bool{false, true} {
				if typ == memory.TypeBinary && nullCandidate {
					continue
				}
				t.Run(fmt.Sprintf("%s/dict=%v/null=%v", typ, dictionary, nullCandidate), func(t *testing.T) {
					b := memory.NewBatch()
					b.Rows = 7
					valid := memory.Bitmap{0x3f}
					if dictionary {
						b.DictStrings["x"] = &memory.DictionaryStringVector{Indices: []uint32{0, 1, 2, 3, 4, 5, 0}, Data: data, Offsets: offsets, Validity: valid}
					} else {
						b.Strings["x"] = &memory.StringViewVector{Data: data, Offsets: append(append([]uint32(nil), offsets...), uint32(len(data))), Validity: valid}
					}
					list := []Expr{}
					for j := 0; j < 32; j++ {
						list = append(list, Str(texts[[]int{0, 1, 2, 4}[j%4]]))
					}
					if nullCandidate {
						list = append(list, Null(typ))
					}
					reviewInCompare(t, memory.NewSchema(memory.Field{Name: "x", Type: typ, Nullable: true}), b, Col("x"), list)
				})
			}
		}
	}
}

func TestReviewLiteralInConcurrentSharedProgram(t *testing.T) {
	list := []Expr{}
	for j := 0; j < 64; j++ {
		list = append(list, I64(int64(j)))
	}
	p, err := Compile(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}), In(Col("x"), list...))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m := memory.NewBudgetManager(0)
			w, e := NewWorkspace(context.Background(), p, m)
			if e != nil {
				t.Error(e)
				return
			}
			defer w.Close()
			b := memory.NewBatch()
			b.Rows = 4
			b.Int64s["x"] = &memory.Int64Vector{Values: []int64{0, 63, 64, -1}}
			for repeat := 0; repeat < 20; repeat++ {
				v, e := w.Evaluate(context.Background(), b, testKernelSet())
				if e != nil {
					t.Error(e)
					return
				}
				for row, want := range []uint32{1, 1, 0, 0} {
					if v[0].Uint32.Values[row] != want {
						t.Errorf("row %d", row)
					}
				}
			}
		}()
	}
	wg.Wait()
}

func TestReviewLiteralInGuardedDemandAndRetry(t *testing.T) {
	list := []Expr{}
	for j := 0; j < 64; j++ {
		list = append(list, I64(int64(j)))
	}
	e := If(EQ(Col("x"), I64(0)), Bool(false), In(Div(I64(1), Col("x")), list...))
	p, err := Compile(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}), e)
	if err != nil {
		t.Fatal(err)
	}
	b := memory.NewBatch()
	b.Rows = 3
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{0, 1, 2}}
	// Every denial boundary must leave reservations charged and retryable.
	for _, limit := range []int64{1, 1024, 2048, 4096, 8192, 16384, 32768} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			m := &scalarTestManager{base: memory.NewBudgetManager(0), limit: limit}
			w, err := NewWorkspace(context.Background(), p, m)
			if err != nil {
				t.Fatal(err)
			}
			_, err = w.Evaluate(context.Background(), b, testKernelSet())
			if err != nil && !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatal(err)
			}
			if m.Used() != w.MemoryBytes() {
				t.Fatalf("after denial used=%d memory=%d", m.Used(), w.MemoryBytes())
			}
			m.limit = 0
			v, err := w.Evaluate(context.Background(), b, testKernelSet())
			if err != nil {
				t.Fatal(err)
			}
			for row, want := range []uint32{0, 1, 0} {
				if v[0].Uint32.Values[row] != want {
					t.Fatalf("row %d", row)
				}
			}
			if m.Used() != w.MemoryBytes() {
				t.Fatalf("after retry used=%d memory=%d", m.Used(), w.MemoryBytes())
			}
			w.Close()
			w.Close()
			if m.Used() != 0 {
				t.Fatal("leaked budget")
			}
		})
	}
}

func TestReviewLiteralInBooleanAndTemporal(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 4
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{-1, 0, 1, 1000000000}}
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	bools := make([]Expr, 32)
	for j := range bools {
		bools[j] = Bool(true)
	}
	reviewInCompare(t, schema, b, EQ(Col("x"), I64(1)), bools)
	nulls := make([]Expr, 32)
	for j := range nulls {
		nulls[j] = Null(memory.TypeInt64)
	}
	reviewInCompare(t, schema, b, Col("x"), nulls)
	reviewInCompare(t, schema, b, Null(memory.TypeInt64), nulls)
	timestamps := make([]Expr, 32)
	for j := range timestamps {
		if j%2 == 0 {
			timestamps[j] = TimestampSeconds(int64(j))
		} else {
			timestamps[j] = TimestampMillis(int64(j))
		}
	}
	reviewInCompare(t, schema, b, CastTo(Col("x"), memory.TypeTimestamp), timestamps)
	dates := make([]Expr, 32)
	for j := range dates {
		dates[j] = Date32(int32(j))
	}
	reviewInCompare(t, schema, b, CastTo(Col("x"), memory.TypeDate32), dates)
}

func TestReviewLiteralInInstructionLayout(t *testing.T) {
	typ := reflect.TypeOf(instruction{})
	fields := make([]reflect.StructField, 0, typ.NumField()-1)
	for j := 0; j < typ.NumField(); j++ {
		f := typ.Field(j)
		if f.Name != "InSet" {
			fields = append(fields, f)
		}
	}
	baseline := reflect.StructOf(fields)
	t.Logf("compiled instruction bytes: without InSet=%d with InSet=%d delta=%d; compiled plan storage is not execution workspace", baseline.Size(), typ.Size(), typ.Size()-baseline.Size())
	cacheType := reflect.TypeOf(dictionaryPredicateCache{})
	cacheFields := make([]reflect.StructField, cacheType.NumField())
	for j := range cacheFields {
		cacheFields[j] = cacheType.Field(j)
		if cacheFields[j].Name == "instruction" {
			cacheFields[j].Type = baseline
		}
	}
	baselineCache := reflect.StructOf(cacheFields)
	registerType := reflect.TypeOf(scalarRegister{})
	registerFields := make([]reflect.StructField, registerType.NumField())
	for j := range registerFields {
		registerFields[j] = registerType.Field(j)
		if registerFields[j].Name == "dictionary" {
			registerFields[j].Type = baselineCache
		}
	}
	baselineRegister := reflect.StructOf(registerFields)
	t.Logf("scalar register bytes: without InSet=%d with InSet=%d delta=%d; dictionary cache embeds an instruction, so all logical and owner records carry this metadata increase", baselineRegister.Size(), registerType.Size(), registerType.Size()-baselineRegister.Size())

}

func TestReviewLiteralInRetainedRootsAndBorrowedInput(t *testing.T) {
	list := make([]Expr, 32)
	for j := range list {
		list[j] = I64(int64(j))
	}
	list = append(list, Null(memory.TypeInt64))
	needle := Add(Col("x"), I64(1))
	member := In(needle, list...)
	combined := And(Or(member, Not(member)), Not(Not(member)))
	named := []NamedExpr{Named(member, "member"), Named(member, "repeat"), Named(Not(member), "negated"), Named(combined, "combined"), Named(needle, "needle"), Named(Col("x"), "borrowed"), Named(Add(needle, I64(99)), "later_numeric")}
	p, err := CompileExpressions(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true}), named)
	if err != nil {
		t.Fatal(err)
	}
	sets := 0
	for _, in := range p.instructions {
		if in.InSet != nil {
			sets++
		}
	}
	if sets != 1 || p.outputs[0].root != p.outputs[1].root {
		t.Fatalf("shared output CSE: sets=%d roots=%d/%d", sets, p.outputs[0].root, p.outputs[1].root)
	}
	for _, program := range []Program{p, withoutStoragePlan(p)} {
		manager := memory.NewBudgetManager(0)
		w, err := NewWorkspace(context.Background(), program, manager)
		if err != nil {
			t.Fatal(err)
		}
		for _, rows := range []int{65, 1, 129, 64, 0, 129} {
			b := memory.NewBatch()
			b.Rows = rows
			input := make([]int64, rows)
			valid := make(memory.Bitmap, memory.WordsFor(rows))
			for row := range input {
				input[row] = int64(row%40) - 2
				if row%7 != 0 {
					memory.Set(valid, row)
				}
			}
			original := append([]int64(nil), input...)
			originalValid := append(memory.Bitmap(nil), valid...)
			b.Int64s["x"] = &memory.Int64Vector{Values: input, Validity: valid}
			v, err := w.Evaluate(context.Background(), b, testKernelSet())
			if err != nil {
				t.Fatal(err)
			}
			for row := 0; row < rows; row++ {
				validNeedle := row%7 != 0
				matched := validNeedle && input[row]+1 >= 0 && input[row]+1 < 32
				for _, idx := range []int{0, 1, 2, 3} {
					if v[idx].Uint32.IsValid(row) != matched {
						t.Fatalf("rows=%d row=%d output=%d validity", rows, row, idx)
					}
					want := uint32(1)
					if idx == 2 {
						want = 0
					}
					if matched && v[idx].Uint32.Values[row] != want {
						t.Fatalf("rows=%d row=%d output=%d truth", rows, row, idx)
					}
				}
				for _, idx := range []int{4, 5, 6} {
					if v[idx].Int64.IsValid(row) != validNeedle {
						t.Fatalf("numeric validity output=%d row=%d", idx, row)
					}
				}
				if validNeedle && (v[4].Int64.Values[row] != input[row]+1 || v[6].Int64.Values[row] != input[row]+100) {
					t.Fatalf("retained needle overwritten at row %d", row)
				}
			}
			if rows > 0 && &v[0].Uint32.Values[0] != &v[1].Uint32.Values[0] {
				t.Fatal("duplicate roots lost shared view")
			}
			if rows > 0 && &v[5].Int64.Values[0] != &input[0] {
				t.Fatal("borrowed input lost alias")
			}
			for row := range input {
				if input[row] != original[row] {
					t.Fatalf("input changed row %d", row)
				}
			}
			for word := range valid {
				if valid[word] != originalValid[word] {
					t.Fatal("input validity changed")
				}
			}
			if manager.Used() != w.MemoryBytes() {
				t.Fatalf("used=%d memory=%d", manager.Used(), w.MemoryBytes())
			}
		}
		w.Close()
		if manager.Used() != 0 {
			t.Fatal("leaked budget")
		}
	}
}

func TestReviewLiteralInCompactEmptyPayloads(t *testing.T) {
	for _, rows := range []int{0, 1, 3} {
		for _, kind := range []string{"null", "empty", "duplicates"} {
			t.Run(fmt.Sprintf("%s/rows=%d", kind, rows), func(t *testing.T) {
				b := memory.NewBatch()
				b.Rows = rows
				b.Strings["x"] = &memory.StringViewVector{Offsets: []uint32{0, 0, 1, 2}[:rows+1], Data: []byte("ab")}
				list := make([]Expr, 32)
				for j := range list {
					switch kind {
					case "null":
						list[j] = Null(memory.TypeString)
					case "empty":
						list[j] = Str("")
					default:
						if j%2 == 0 {
							list[j] = Str("a")
						} else {
							list[j] = Str("")
						}
					}
				}
				reviewInCompare(t, memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeString}), b, Col("x"), list)
				p, err := Compile(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeString}), In(Col("x"), list...))
				if err != nil {
					t.Fatal(err)
				}
				in := p.instructions[p.root]
				want := int64(8)
				if kind == "empty" {
					want = 16
				} else if kind == "duplicates" {
					want = 25
				}
				if got := literalInBytes(in); got != want {
					t.Fatalf("packed payload got=%d want=%d", got, want)
				}
			})
		}
	}
}
