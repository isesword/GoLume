package expr

import (
	"strings"
	"testing"

	"gocolumn/memory"
)

func TestRegexMatchPreservesThreeValuedLogicAndUnicode(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView})
	batch := memory.NewBatch()
	batch.Strings["s"] = phase33StringVector(
		[]string{"abc-123", "语言42", "none", "must-not-be-read"},
		[]bool{true, true, true, false},
	)
	batch.Reset(4)

	predicate := And(
		RegexMatch(Col("s"), Str(`^(?:abc-[0-9]+|语言[0-9]+)$`)),
		Not(Contains(Col("s"), Str("none"))),
	)
	program, _, err := CompilePredicate(schema, predicate)
	if err != nil {
		t.Fatal(err)
	}
	result, err := program.Evaluate(batch, &EvalWorkspace{}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []int{0, 1} {
		if !memory.IsSet(result.Truth, row) || !memory.IsSet(result.Validity, row) {
			t.Fatalf("row %d truth=%v validity=%v", row, result.Truth, result.Validity)
		}
	}
	if memory.IsSet(result.Truth, 2) || !memory.IsSet(result.Validity, 2) {
		t.Fatalf("non-match truth=%v validity=%v", result.Truth, result.Validity)
	}
	if memory.IsSet(result.Truth, 3) || memory.IsSet(result.Validity, 3) {
		t.Fatalf("NULL row truth=%v validity=%v", result.Truth, result.Validity)
	}
}

func TestRegexMatchCachesDictionaryResults(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Encoding: memory.EncodingDictionary, DictionaryDomain: "regex"})
	batch := memory.NewBatch()
	batch.DictStrings["s"] = &memory.DictionaryStringVector{
		Indices:          []uint32{0, 1, 0, 2},
		Offsets:          []uint32{0, 4, 8, 12},
		Data:             []byte("A-12nopeB-99"),
		DictionaryDomain: "regex",
		Encoding:         memory.EncodingDictionary,
	}
	batch.Reset(4)
	program, _, err := CompilePredicate(schema, RegexMatch(Col("s"), Str(`^[A-Z]-[0-9]+$`)))
	if err != nil {
		t.Fatal(err)
	}
	var workspace EvalWorkspace
	result, err := program.Evaluate(batch, &workspace, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	for row, want := range []bool{true, false, true, true} {
		if memory.IsSet(result.Truth, row) != want {
			t.Fatalf("row %d match=%v want=%v", row, memory.IsSet(result.Truth, row), want)
		}
	}
	if len(workspace.dictionary) != 1 || !workspace.dictionary[0].valid {
		t.Fatal("dictionary regex results were not retained")
	}
}

func TestRegexExtractCapturesNullsAndEmptyMatches(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView})
	batch := memory.NewBatch()
	batch.Strings["s"] = phase33StringVector(
		[]string{"user:42", "plain", "语言:7", "bad:", "must-not-be-read"},
		[]bool{true, true, true, true, false},
	)
	batch.Reset(5)
	pattern := Str(`^([^:]+)(?::([0-9]+))?$`)
	extractName := RegexExtract(Col("s"), pattern, I64(1))
	program, err := CompileProjection(schema, []NamedExpr{
		Named(RegexExtract(Col("s"), pattern, I64(0)), "whole"),
		Named(extractName, "name"),
		Named(RegexExtract(Col("s"), pattern, I64(2)), "number"),
		Named(extractName, "name_again"),
		Named(RegexExtract(Col("s"), Str(`^`), I64(0)), "empty"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if program.Outputs[1].Root != program.Outputs[3].Root {
		t.Fatal("identical REGEX_EXTRACT expressions did not share their compiled CSE root")
	}
	for _, instruction := range program.Instructions {
		if instruction.Code == scalarLiteralString {
			t.Fatal("REGEX_EXTRACT pattern must remain instruction metadata, not a row-materialized string literal")
		}
	}
	values, err := program.Evaluate(batch, &ScalarWorkspace{}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	phase33RequireStrings(t, values[0].String, []string{"user:42", "plain", "语言:7", "", ""}, []bool{true, true, true, false, false})
	phase33RequireStrings(t, values[1].String, []string{"user", "plain", "语言", "", ""}, []bool{true, true, true, false, false})
	phase33RequireStrings(t, values[2].String, []string{"42", "", "7", "", ""}, []bool{true, false, true, false, false})
	phase33RequireStrings(t, values[3].String, []string{"user", "plain", "语言", "", ""}, []bool{true, true, true, false, false})
	phase33RequireStrings(t, values[4].String, []string{"", "", "", "", ""}, []bool{true, true, true, true, false})
}

func TestRegexCompilationContracts(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "s", Type: memory.TypeString, Encoding: memory.EncodingView},
		memory.Field{Name: "pattern", Type: memory.TypeString, Encoding: memory.EncodingView},
	)
	predicateErrors := []Expr{
		RegexMatch(Col("s"), Str("(")),
		RegexMatch(Col("s"), Col("pattern")),
		RegexMatch(Col("s"), Str(strings.Repeat("a", maxRegexPatternBytes+1))),
	}
	for _, expression := range predicateErrors {
		if _, _, err := CompilePredicate(schema, expression); err == nil {
			t.Fatalf("predicate %s should fail compilation", expression)
		}
	}
	scalarErrors := []Expr{
		RegexExtract(Col("s"), Str("("), I64(0)),
		RegexExtract(Col("s"), Col("pattern"), I64(0)),
		RegexExtract(Col("s"), Str("(a)"), I64(2)),
		RegexExtract(Col("s"), Str("(a)"), I64(-1)),
		RegexExtract(Col("s"), Str("(a)"), Col("pattern")),
	}
	for _, expression := range scalarErrors {
		if _, err := CompileScalar(schema, expression); err == nil {
			t.Fatalf("scalar %s should fail compilation", expression)
		}
	}
}

func TestRegexExecutionHandlesLongInput(t *testing.T) {
	input := strings.Repeat("a", 1<<20) + "语言42"
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Encoding: memory.EncodingView})
	batch := memory.NewBatch()
	batch.Strings["s"] = phase33StringVector([]string{input}, nil)
	batch.Reset(1)

	predicate, _, err := CompilePredicate(schema, RegexMatch(Col("s"), Str(`a+语言[0-9]+$`)))
	if err != nil {
		t.Fatal(err)
	}
	result, err := predicate.Evaluate(batch, &EvalWorkspace{}, testKernelSet())
	if err != nil || !memory.IsSet(result.Truth, 0) {
		t.Fatalf("long input match truth=%v err=%v", result.Truth, err)
	}
	extract, err := CompileScalar(schema, RegexExtract(Col("s"), Str(`(语言[0-9]+)$`), I64(1)))
	if err != nil {
		t.Fatal(err)
	}
	value, err := extract.Evaluate(batch, &ScalarWorkspace{}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	phase33RequireStrings(t, value.String, []string{"语言42"}, []bool{true})
}
