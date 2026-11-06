package expr

import (
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"sync"
	"testing"
	"unsafe"
)

func testKernelSet() kernel.Set { return kernel.New(kernel.ModeScalar) }

func TestProgramThreeValuedLogic(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true})
	// x=[5,15,NULL,35]
	valid := make(memory.Bitmap, 1)
	memory.Set(valid, 0)
	memory.Set(valid, 1)
	memory.Set(valid, 3)
	b := memory.NewBatch()
	b.Rows = 4
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{5, 15, 0, 35}, Validity: valid}
	p, _, err := CompilePredicate(schema, And(GT(Col("x"), I64(10)), LT(Col("x"), I64(40))))
	if err != nil {
		t.Fatal(err)
	}
	var w evaluationState
	v, err := evaluateBoolTest(p, b, &w, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if !memory.IsSet(v.Truth, 1) || !memory.IsSet(v.Truth, 3) || memory.CountBitmap(v.Truth) != 2 {
		t.Fatalf("truth=%064b", v.Truth[0])
	}
	if memory.IsSet(v.Validity, 2) {
		t.Fatalf("NULL comparison must remain unknown")
	}
	p2, _, err := CompilePredicate(schema, Or(IsNull(Col("x")), GT(Col("x"), I64(30))))
	if err != nil {
		t.Fatal(err)
	}
	v2, err := evaluateBoolTest(p2, b, &w, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if memory.CountBitmap(v2.Truth) != 2 || !memory.IsSet(v2.Truth, 2) || !memory.IsSet(v2.Truth, 3) {
		t.Fatalf("OR truth=%064b", v2.Truth[0])
	}
	for i := 0; i < 4; i++ {
		if !memory.IsSet(v2.Validity, i) {
			t.Fatalf("IS NULL OR comparison should be known at row %d", i)
		}
	}
}

func TestProgramNativeFloat32Predicate(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "f", Type: memory.TypeFloat32, Nullable: true})
	valid := memory.Bitmap{0b101}
	b := memory.NewBatch()
	b.Rows = 3
	b.Float32s["f"] = &memory.Float32Vector{Values: []float32{1.5, 2.5, 3.5}, Validity: valid}
	p, _, err := CompilePredicate(schema, GT(Col("f"), F64(2)))
	if err != nil {
		t.Fatal(err)
	}
	out, err := evaluateBoolTest(p, b, &evaluationState{}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if !memory.IsSet(out.Truth, 2) || memory.IsSet(out.Truth, 0) || memory.IsSet(out.Truth, 1) {
		t.Fatalf("truth=%v", out.Truth)
	}
	if memory.IsSet(out.Validity, 1) {
		t.Fatalf("NULL row became known: %v", out.Validity)
	}
}

func TestProgramFloat32ColumnComparisonUsesBothValidityBitmaps(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "a", Type: memory.TypeFloat32, Nullable: true},
		memory.Field{Name: "b", Type: memory.TypeFloat32, Nullable: true},
	)
	b := memory.NewBatch()
	b.Rows = 5
	b.Float32s["a"] = &memory.Float32Vector{Values: []float32{1, 4, 3, 9, 5}, Validity: memory.Bitmap{0b11011}}
	b.Float32s["b"] = &memory.Float32Vector{Values: []float32{0, 5, 2, 8, 5}, Validity: memory.Bitmap{0b10111}}
	p, simple, err := CompilePredicate(schema, GT(Col("a"), Col("b")))
	if err != nil {
		t.Fatal(err)
	}
	if simple != nil {
		t.Fatal("column-column comparison must not be exposed as a literal fast path")
	}
	out, err := evaluateBoolTest(p, b, &evaluationState{}, kernel.New(kernel.ModeScalar))
	if err != nil {
		t.Fatal(err)
	}
	if !memory.IsSet(out.Truth, 0) || memory.IsSet(out.Truth, 1) || memory.IsSet(out.Truth, 2) || memory.IsSet(out.Truth, 3) || memory.IsSet(out.Truth, 4) {
		t.Fatalf("truth=%v", out.Truth)
	}
	for row := 0; row < 5; row++ {
		known := row == 0 || row == 1 || row == 4
		if memory.IsSet(out.Validity, row) != known {
			t.Fatalf("row=%d validity=%v want=%v", row, memory.IsSet(out.Validity, row), known)
		}
	}
}

func TestProgramFloat32ColumnComparisonForcedMode(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeFloat32}, memory.Field{Name: "b", Type: memory.TypeFloat32})
	b := memory.NewBatch()
	b.Rows = 4
	b.Float32s["a"] = &memory.Float32Vector{Values: []float32{1, 4, 3, 9}}
	b.Float32s["b"] = &memory.Float32Vector{Values: []float32{0, 5, 2, 8}}
	p, _, err := CompilePredicate(schema, GT(Col("a"), Col("b")))
	if err != nil {
		t.Fatal(err)
	}
	out, err := evaluateBoolTest(p, b, &evaluationState{}, kernel.New(kernel.ModeSIMD))
	if err != nil {
		t.Fatal(err)
	}
	if memory.CountBitmap(out.Truth) != 3 || !memory.IsSet(out.Truth, 0) || !memory.IsSet(out.Truth, 2) || !memory.IsSet(out.Truth, 3) {
		t.Fatalf("truth=%v", out.Truth)
	}
}

func TestProgramInt32ColumnComparison(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt32, Nullable: true}, memory.Field{Name: "b", Type: memory.TypeInt32, Nullable: true})
	b := memory.NewBatch()
	b.Rows = 5
	b.Int32s["a"] = &memory.Int32Vector{Values: []int32{1, 4, 3, 9, 5}, Validity: memory.Bitmap{0b11011}}
	b.Int32s["b"] = &memory.Int32Vector{Values: []int32{0, 5, 2, 8, 5}, Validity: memory.Bitmap{0b10111}}
	p, simple, err := CompilePredicate(schema, GT(Col("a"), Col("b")))
	if err != nil {
		t.Fatal(err)
	}
	if simple != nil {
		t.Fatal("column-column comparison must not be a literal fast path")
	}
	out, err := evaluateBoolTest(p, b, &evaluationState{}, kernel.New(kernel.ModeSIMD))
	if err != nil {
		t.Fatal(err)
	}
	if !memory.IsSet(out.Truth, 0) || memory.IsSet(out.Truth, 1) || memory.IsSet(out.Truth, 2) || memory.IsSet(out.Truth, 3) || memory.IsSet(out.Truth, 4) {
		t.Fatalf("truth=%v", out.Truth)
	}
	for row := 0; row < 5; row++ {
		known := row == 0 || row == 1 || row == 4
		if memory.IsSet(out.Validity, row) != known {
			t.Fatalf("row=%d validity=%v want=%v", row, memory.IsSet(out.Validity, row), known)
		}
	}
}

func TestProgramUint32ColumnComparisonKeepsUnsignedOrdering(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeUint32}, memory.Field{Name: "b", Type: memory.TypeUint32})
	b := memory.NewBatch()
	b.Rows = 4
	b.Uint32s["a"] = &memory.Uint32Vector{Values: []uint32{0, 1, ^uint32(0), 9}}
	b.Uint32s["b"] = &memory.Uint32Vector{Values: []uint32{0, 2, 1, 8}}
	p, _, err := CompilePredicate(schema, GT(Col("a"), Col("b")))
	if err != nil {
		t.Fatal(err)
	}
	out, err := evaluateBoolTest(p, b, &evaluationState{}, kernel.New(kernel.ModeSIMD))
	if err != nil {
		t.Fatal(err)
	}
	if memory.CountBitmap(out.Truth) != 2 || !memory.IsSet(out.Truth, 2) || !memory.IsSet(out.Truth, 3) {
		t.Fatalf("truth=%v", out.Truth)
	}
}

func TestProgramInt64ColumnComparison(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64}, memory.Field{Name: "b", Type: memory.TypeInt64})
	b := memory.NewBatch()
	b.Rows = 4
	b.Int64s["a"] = &memory.Int64Vector{Values: []int64{-1, 0, 1, 1 << 62}}
	b.Int64s["b"] = &memory.Int64Vector{Values: []int64{-2, 1, 1, -1}}
	p, _, err := CompilePredicate(schema, GT(Col("a"), Col("b")))
	if err != nil {
		t.Fatal(err)
	}
	out, err := evaluateBoolTest(p, b, &evaluationState{}, kernel.New(kernel.ModeSIMD))
	if err != nil {
		t.Fatal(err)
	}
	if memory.CountBitmap(out.Truth) != 2 || !memory.IsSet(out.Truth, 0) || !memory.IsSet(out.Truth, 3) {
		t.Fatalf("truth=%v", out.Truth)
	}
}

func TestProgramFloat64ColumnComparisonSpecialValues(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeFloat64}, memory.Field{Name: "b", Type: memory.TypeFloat64})
	b := memory.NewBatch()
	b.Rows = 4
	b.Float64s["a"] = &memory.Float64Vector{Values: []float64{1, math.NaN(), math.Inf(1), -0}}
	b.Float64s["b"] = &memory.Float64Vector{Values: []float64{0, math.NaN(), 0, 0}}
	p, _, err := CompilePredicate(schema, GT(Col("a"), Col("b")))
	if err != nil {
		t.Fatal(err)
	}
	out, err := evaluateBoolTest(p, b, &evaluationState{}, kernel.New(kernel.ModeSIMD))
	if err != nil {
		t.Fatal(err)
	}
	if memory.CountBitmap(out.Truth) != 2 || !memory.IsSet(out.Truth, 0) || !memory.IsSet(out.Truth, 2) {
		t.Fatalf("truth=%v", out.Truth)
	}
}

func TestProgramUint64ColumnComparisonKeepsUnsignedOrdering(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeUint64}, memory.Field{Name: "b", Type: memory.TypeUint64})
	b := memory.NewBatch()
	b.Rows = 4
	b.Uint64s["a"] = &memory.Uint64Vector{Values: []uint64{0, 1, ^uint64(0), 9}}
	b.Uint64s["b"] = &memory.Uint64Vector{Values: []uint64{0, 2, 1, 8}}
	p, _, err := CompilePredicate(schema, GT(Col("a"), Col("b")))
	if err != nil {
		t.Fatal(err)
	}
	out, err := evaluateBoolTest(p, b, &evaluationState{}, kernel.New(kernel.ModeScalar))
	if err != nil {
		t.Fatal(err)
	}
	if memory.CountBitmap(out.Truth) != 2 || !memory.IsSet(out.Truth, 2) || !memory.IsSet(out.Truth, 3) {
		t.Fatalf("truth=%v", out.Truth)
	}
}

func TestProgramNarrowColumnComparisons(t *testing.T) {
	cases := []struct {
		name string
		typ  memory.DataType
		fill func(*memory.Batch)
	}{
		{"int8", memory.TypeInt8, func(b *memory.Batch) {
			b.Int8s["a"] = &memory.Int8Vector{Values: []int8{1, 3}}
			b.Int8s["b"] = &memory.Int8Vector{Values: []int8{0, 4}}
		}},
		{"int16", memory.TypeInt16, func(b *memory.Batch) {
			b.Int16s["a"] = &memory.Int16Vector{Values: []int16{1, 3}}
			b.Int16s["b"] = &memory.Int16Vector{Values: []int16{0, 4}}
		}},
		{"uint8", memory.TypeUint8, func(b *memory.Batch) {
			b.Uint8s["a"] = &memory.Uint8Vector{Values: []uint8{1, 3}}
			b.Uint8s["b"] = &memory.Uint8Vector{Values: []uint8{0, 4}}
		}},
		{"uint16", memory.TypeUint16, func(b *memory.Batch) {
			b.Uint16s["a"] = &memory.Uint16Vector{Values: []uint16{1, 3}}
			b.Uint16s["b"] = &memory.Uint16Vector{Values: []uint16{0, 4}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := memory.NewBatch()
			b.Rows = 2
			tc.fill(b)
			p, _, err := CompilePredicate(memory.NewSchema(memory.Field{Name: "a", Type: tc.typ}, memory.Field{Name: "b", Type: tc.typ}), GT(Col("a"), Col("b")))
			if err != nil {
				t.Fatal(err)
			}
			out, err := evaluateBoolTest(p, b, &evaluationState{}, kernel.New(kernel.ModeScalar))
			if err != nil {
				t.Fatal(err)
			}
			if memory.CountBitmap(out.Truth) != 1 || !memory.IsSet(out.Truth, 0) {
				t.Fatalf("truth=%v", out.Truth)
			}
		})
	}
}

func TestProgramNativeNarrowPredicates(t *testing.T) {
	cases := []struct {
		name      string
		typ       memory.DataType
		values    []int64
		makeBatch func(*memory.Batch)
	}{
		{"i8", memory.TypeInt8, []int64{0, 2, 3}, func(b *memory.Batch) { b.Int8s["x"] = &memory.Int8Vector{Values: []int8{0, 2, 3}} }},
		{"i16", memory.TypeInt16, []int64{0, 2, 3}, func(b *memory.Batch) { b.Int16s["x"] = &memory.Int16Vector{Values: []int16{0, 2, 3}} }},
		{"u8", memory.TypeUint8, []int64{1, 3, 5}, func(b *memory.Batch) { b.Uint8s["x"] = &memory.Uint8Vector{Values: []uint8{1, 3, 5}} }},
		{"u16", memory.TypeUint16, []int64{1, 3, 5}, func(b *memory.Batch) { b.Uint16s["x"] = &memory.Uint16Vector{Values: []uint16{1, 3, 5}} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := memory.NewBatch()
			b.Rows = len(tc.values)
			tc.makeBatch(b)
			p, _, err := CompilePredicate(memory.NewSchema(memory.Field{Name: "x", Type: tc.typ}), GT(Col("x"), I64(1)))
			if err != nil {
				t.Fatal(err)
			}
			got, err := evaluateBoolTest(p, b, &evaluationState{}, testKernelSet())
			if err != nil {
				t.Fatal(err)
			}
			if memory.CountBitmap(got.Truth) != 2 || !memory.IsSet(got.Truth, 2) {
				t.Fatalf("truth=%v", got.Truth)
			}
		})
	}
}

func TestCompilePredicateNilMeansAllRows(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	program, simple, err := CompilePredicate(schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	if simple != nil || program.complexity != 1 {
		t.Fatalf("program=%+v simple=%+v", program, simple)
	}
	batch := memory.NewBatch()
	batch.Rows = 3
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 2, 3}}
	var workspace evaluationState
	out, err := evaluateBoolTest(program, batch, &workspace, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if memory.CountBitmap(out.Truth) != 3 || memory.CountBitmap(out.Validity) != 3 {
		t.Fatalf("truth=%v validity=%v", out.Truth, out.Validity)
	}
}

func TestOpaqueColumnNullPredicate(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "nested", Type: memory.TypeArrow, Nullable: true, ArrowType: "list<item: int32>"})
	program, _, err := CompilePredicate(schema, IsNotNull(Col("nested")))
	if err != nil {
		t.Fatal(err)
	}
	valid := memory.EnsureBitmap(nil, 3)
	memory.Set(valid, 0)
	memory.Set(valid, 2)
	b := memory.NewBatch()
	b.Rows = 3
	b.Opaque["nested"] = &memory.OpaqueVector{Rows: 3, Type: memory.TypeArrow, Validity: valid, Value: struct{}{}}
	var ws evaluationState
	got, err := evaluateBoolTest(program, b, &ws, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if !memory.IsSet(got.Truth, 0) || memory.IsSet(got.Truth, 1) || !memory.IsSet(got.Truth, 2) {
		t.Fatalf("truth=%v", got.Truth)
	}
}

func TestProgramStringLiteralWorkspaceCacheInvalidates(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString})
	batch := &memory.Batch{Rows: 2, Strings: map[string]*memory.StringViewVector{"s": {Offsets: []uint32{0, 3, 6}, Data: []byte("foobar"), Encoding: memory.EncodingView}}}
	p1, _, err := CompilePredicate(schema, Contains(Col("s"), StringLiteral{Value: "foo"}))
	if err != nil {
		t.Fatal(err)
	}
	p2, _, err := CompilePredicate(schema, Contains(Col("s"), StringLiteral{Value: "bar"}))
	if err != nil {
		t.Fatal(err)
	}
	var w evaluationState
	got, err := evaluateBoolTest(p1, batch, &w, kernel.New(kernel.ModeScalar))
	if err != nil {
		t.Fatal(err)
	}
	if got.Truth[0]&1 == 0 || got.Truth[0]&2 != 0 {
		t.Fatalf("p1 result=%v", got.Truth)
	}
	got, err = evaluateBoolTest(p2, batch, &w, kernel.New(kernel.ModeScalar))
	if err != nil {
		t.Fatal(err)
	}
	if got.Truth[0]&1 != 0 || got.Truth[0]&2 == 0 {
		t.Fatalf("p2 result=%v", got.Truth)
	}
}

func TestProgramStringLiteralWorkspaceConcurrentIsolation(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString})
	batch := &memory.Batch{Rows: 2, Strings: map[string]*memory.StringViewVector{"s": {Offsets: []uint32{0, 3, 6}, Data: []byte("foobar"), Encoding: memory.EncodingView}}}
	p, _, err := CompilePredicate(schema, Contains(Col("s"), StringLiteral{Value: "foo"}))
	if err != nil {
		t.Fatal(err)
	}
	ks := kernel.New(kernel.ModeScalar)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var w evaluationState
			got, err := evaluateBoolTest(p, batch, &w, ks)
			if err != nil || got.Truth[0]&1 == 0 || got.Truth[0]&2 != 0 {
				t.Errorf("concurrent result=%v err=%v", got.Truth, err)
			}
		}()
	}
	wg.Wait()
}

func TestEvaluationStateMemoryBytesTracksRetainedCapacity(t *testing.T) {
	literal := make([]byte, 0, 6)
	literal = append(literal, "needle"...)
	w := &evaluationState{owned: []scalarRegister{{BoolVector: BoolVector{Truth: memory.Bitmap{1, 2}}, ownedValidity: memory.Bitmap{3}, literal: literal, literalKey: "needle", dictionary: dictionaryPredicateCache{domain: "d", matches: make([]bool, 2)}}}}
	want := int64(32 + unsafe.Sizeof(scalarRegister{}))
	if got := w.MemoryBytes(); got != want {
		t.Fatalf("workspace memory bytes=%d want=%d", got, want)
	}
	// These strings are borrowed. Longer contents must not change owned bytes.
	w.owned[0].literalKey = "a much longer instruction literal"
	w.owned[0].dictionary.domain = "a much longer dictionary domain"
	if got := w.MemoryBytes(); got != want {
		t.Fatalf("borrowed strings changed accounting: got=%d want=%d", got, want)
	}
	w.Ensure(0, 0)
	if got := w.MemoryBytes(); got != want {
		t.Fatalf("inactive retained slots lost from accounting: got=%d want=%d", got, want)
	}
	if allocs := testing.AllocsPerRun(100, func() { _ = w.MemoryBytes() }); allocs != 0 {
		t.Fatalf("accounting allocated: %v", allocs)
	}
	*w = evaluationState{}
	if got := w.MemoryBytes(); got != 0 {
		t.Fatalf("reset workspace retains %d bytes", got)
	}
	if (*evaluationState)(nil).MemoryBytes() != 0 {
		t.Fatal("nil workspace must report zero bytes")
	}
}

func TestDictionaryPredicateReserveHookPropagatesFailure(t *testing.T) {
	v := &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{0, 3, 6}, Data: []byte("abcdef"), DictionaryDomain: "d"}
	out := make(memory.Bitmap, memory.WordsFor(1))
	cache := &dictionaryPredicateCache{reserve: func(int64) (memory.Reservation, error) { return nil, fmt.Errorf("budget exceeded") }}
	selectDictionary(v, 1, out, instruction{Column: "s"}, cache, func([]byte) bool { return true })
	if cache.reserveErr == nil || cache.reserveErr.Error() != "budget exceeded" {
		t.Fatalf("reserveErr=%v", cache.reserveErr)
	}
}
