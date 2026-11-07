package expr

import (
	"strings"
	"testing"

	"gocolumn/memory"
)

func TestStringTransformsProjectionUsesUnicodeAndPropagatesNull(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView})
	batch := memory.NewBatch()
	batch.Strings["s"] = phase33StringVector([]string{
		"\u3000 Go语言 \t",
		"one-one-one",
		"MIXED Ünicode",
		"",
		"must-not-be-read",
	}, []bool{true, true, true, true, false})
	batch.Reset(5)

	trimmed := Trim(Col("s"))
	program, err := CompileExpressions(schema, []NamedExpr{
		Named(trimmed, "trim"),
		Named(TrimStart(Col("s")), "trim_start"),
		Named(TrimEnd(Col("s")), "trim_end"),
		Named(Lower(Col("s")), "lower"),
		Named(Upper(Col("s")), "upper"),
		Named(Replace(Col("s"), Str("-"), Str("_")), "replace_one"),
		Named(ReplaceAll(Col("s"), Str("-"), Str("_")), "replace_all"),
		Named(Slice(trimmed, I64(1), I64(2)), "slice"),
		Named(Slice(trimmed, I64(-2), I64(2)), "suffix"),
		Named(Find(trimmed, Str("语")), "find"),
		Named(Length(trimmed), "chars"),
		Named(ByteLength(trimmed), "bytes"),
	})
	if err != nil {
		t.Fatal(err)
	}
	values, err := evaluateProjectionTest(program, batch, &evaluationState{}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	valid := []bool{true, true, true, true, false}
	phase33RequireStrings(t, values[0].String, []string{"Go语言", "one-one-one", "MIXED Ünicode", "", ""}, valid)
	phase33RequireStrings(t, values[1].String, []string{"Go语言 \t", "one-one-one", "MIXED Ünicode", "", ""}, valid)
	phase33RequireStrings(t, values[2].String, []string{"\u3000 Go语言", "one-one-one", "MIXED Ünicode", "", ""}, valid)
	phase33RequireStrings(t, values[3].String, []string{"\u3000 go语言 \t", "one-one-one", "mixed ünicode", "", ""}, valid)
	phase33RequireStrings(t, values[4].String, []string{"\u3000 GO语言 \t", "ONE-ONE-ONE", "MIXED ÜNICODE", "", ""}, valid)
	phase33RequireStrings(t, values[5].String, []string{"\u3000 Go语言 \t", "one_one-one", "MIXED Ünicode", "", ""}, valid)
	phase33RequireStrings(t, values[6].String, []string{"\u3000 Go语言 \t", "one_one_one", "MIXED Ünicode", "", ""}, valid)
	phase33RequireStrings(t, values[7].String, []string{"o语", "ne", "IX", "", ""}, valid)
	phase33RequireStrings(t, values[8].String, []string{"语言", "ne", "de", "", ""}, valid)
	phase33RequireInt64(t, values[9].Int64, []int64{2, -1, -1, -1, 0}, valid)
	phase33RequireInt64(t, values[10].Int64, []int64{4, 11, 13, 0, 0}, valid)
	phase33RequireInt64(t, values[11].Int64, []int64{8, 11, 14, 0, 0}, valid)
}

func TestStringTransformsMaterializeDictionaryInput(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingDictionary, DictionaryDomain: "phase33"})
	valid := memory.EnsureBitmap(nil, 4)
	memory.Set(valid, 0)
	memory.Set(valid, 1)
	memory.Set(valid, 3)
	batch := memory.NewBatch()
	batch.DictStrings["s"] = &memory.DictionaryStringVector{
		Indices:          []uint32{0, 1, 0, 2},
		Validity:         valid,
		Offsets:          []uint32{0, 11, 18, 23},
		Data:             []byte(" Go语言 \tone-oneMIXED"),
		DictionaryDomain: "phase33",
		Encoding:         memory.EncodingDictionary,
	}
	batch.Reset(4)

	program, err := CompileExpressions(schema, []NamedExpr{
		Named(Trim(Col("s")), "trim"),
		Named(ReplaceAll(Col("s"), Str("-"), Str("_")), "replace"),
		Named(Slice(Trim(Col("s")), I64(-2), I64(2)), "suffix"),
		Named(Find(Col("s"), Str("语")), "find"),
		Named(ByteLength(Trim(Col("s"))), "bytes"),
	})
	if err != nil {
		t.Fatal(err)
	}
	values, err := evaluateProjectionTest(program, batch, &evaluationState{}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range []int{0, 1, 2} {
		if values[index].String == nil || values[index].DictionaryString != nil {
			t.Fatalf("output %d should materialize dictionary input as a plain string", index)
		}
	}
	validRows := []bool{true, true, false, true}
	phase33RequireStrings(t, values[0].String, []string{"Go语言", "one-one", "", "MIXED"}, validRows)
	phase33RequireStrings(t, values[1].String, []string{" Go语言 \t", "one_one", "", "MIXED"}, validRows)
	phase33RequireStrings(t, values[2].String, []string{"语言", "ne", "", "ED"}, validRows)
	phase33RequireInt64(t, values[3].Int64, []int64{3, -1, 0, -1}, validRows)
	phase33RequireInt64(t, values[4].Int64, []int64{8, 7, 0, 5}, validRows)
}

func TestDictionaryPredicateWorkspaceReusesStableDomainWithoutAllocations(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Encoding: memory.EncodingDictionary, DictionaryDomain: "stable"})
	batch := memory.NewBatch()
	batch.DictStrings["s"] = &memory.DictionaryStringVector{
		Indices:          []uint32{0, 1, 2, 1, 0, 2},
		Offsets:          []uint32{0, 5, 9, 14},
		Data:             []byte("alphabetaomega"),
		DictionaryDomain: "stable",
		Encoding:         memory.EncodingDictionary,
	}
	batch.Reset(6)
	program, _, err := CompilePredicate(schema, Contains(Col("s"), Str("a")))
	if err != nil {
		t.Fatal(err)
	}
	var workspace evaluationState
	kernels := testKernelSet()
	if _, err := evaluateBoolTest(program, batch, &workspace, kernels); err != nil {
		t.Fatal(err)
	}
	var evaluateErr error
	if got := testing.AllocsPerRun(100, func() {
		_, evaluateErr = evaluateBoolTest(program, batch, &workspace, kernels)
	}); got != 0 {
		t.Fatalf("repeated dictionary predicate allocations=%v", got)
	}
	if evaluateErr != nil {
		t.Fatal(evaluateErr)
	}
	batch.DictStrings["s"].DictionaryDomain = "changed"
	batch.DictStrings["s"].Data = []byte("xxxxxbetaxxxxx")
	result, err := evaluateBoolTest(program, batch, &workspace, kernels)
	if err != nil {
		t.Fatal(err)
	}
	if result.Truth[0]&1 != 0 {
		t.Fatal("dictionary domain change did not invalidate cached predicate")
	}
}

func TestByteLengthAndFindPredicateFastPathsPreserveThreeValuedLogic(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView})
	batch := memory.NewBatch()
	batch.Strings["s"] = phase33StringVector([]string{"Go语言", "abc", "not-evaluated"}, []bool{true, true, false})
	batch.Reset(3)

	predicate := And(
		EQ(ByteLength(Col("s")), I64(8)),
		EQ(Find(Col("s"), Str("语")), I64(2)),
	)
	program, _, err := CompilePredicate(schema, predicate)
	if err != nil {
		t.Fatal(err)
	}
	result, err := evaluateBoolTest(program, batch, &evaluationState{}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if !memory.IsSet(result.Truth, 0) || memory.IsSet(result.Truth, 1) || memory.IsSet(result.Truth, 2) {
		t.Fatalf("truth=%v", result.Truth)
	}
	if !memory.IsSet(result.Validity, 0) || !memory.IsSet(result.Validity, 1) || memory.IsSet(result.Validity, 2) {
		t.Fatalf("validity=%v", result.Validity)
	}
}

func TestStringTransformTypeAndSliceErrors(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "s", Type: memory.TypeString},
		memory.Field{Name: "i", Type: memory.TypeInt64},
	)
	for _, bad := range []Expr{
		Trim(Col("i")),
		Find(Col("s"), Col("i")),
		Replace(Col("s"), Str("a"), Col("i")),
		Slice(Col("s"), F64(1), I64(1)),
		Str(string([]byte{0xff})),
	} {
		if _, err := Compile(schema, bad); err == nil {
			t.Fatalf("%s should fail type validation", bad)
		}
	}

	batch := memory.NewBatch()
	batch.Strings["s"] = phase33StringVector([]string{"Go语言"}, nil)
	batch.Reset(1)
	program, err := Compile(schema, Slice(Col("s"), I64(0), I64(-1)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := evaluateValueTest(program, batch, &evaluationState{}, testKernelSet()); err == nil || !strings.Contains(err.Error(), "row 0") {
		t.Fatalf("negative slice length error=%v", err)
	}
}

func phase33StringVector(values []string, valid []bool) *memory.StringViewVector {
	v := &memory.StringViewVector{Offsets: make([]uint32, len(values)+1), Encoding: memory.EncodingView}
	if valid != nil {
		v.Validity = memory.EnsureBitmap(nil, len(values))
	}
	for i, value := range values {
		v.Data = append(v.Data, value...)
		v.Offsets[i+1] = uint32(len(v.Data))
		if valid != nil && valid[i] {
			memory.Set(v.Validity, i)
		}
	}
	if valid == nil {
		v.Validity = nil
	}
	return v
}

func phase33RequireStrings(t *testing.T, got *memory.StringViewVector, want []string, valid []bool) {
	t.Helper()
	if got == nil || got.Len() != len(want) {
		t.Fatalf("string vector rows=%v want=%d", got, len(want))
	}
	for row := range want {
		if got.IsValid(row) != valid[row] {
			t.Fatalf("row %d valid=%v want=%v", row, got.IsValid(row), valid[row])
		}
		if valid[row] && string(got.ValueBytes(row)) != want[row] {
			t.Fatalf("row %d value=%q want=%q", row, got.ValueBytes(row), want[row])
		}
	}
}

func phase33RequireInt64(t *testing.T, got *memory.Int64Vector, want []int64, valid []bool) {
	t.Helper()
	if got == nil || len(got.Values) != len(want) {
		t.Fatalf("int64 vector rows=%v want=%d", got, len(want))
	}
	for row := range want {
		if got.IsValid(row) != valid[row] {
			t.Fatalf("row %d valid=%v want=%v", row, got.IsValid(row), valid[row])
		}
		if valid[row] && got.Values[row] != want[row] {
			t.Fatalf("row %d value=%d want=%d", row, got.Values[row], want[row])
		}
	}
}
