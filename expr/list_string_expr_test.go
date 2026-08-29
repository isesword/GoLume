package expr

import (
	"bytes"
	"strings"
	"testing"

	"gocolumn/memory"
)

func TestSplitDynamicDelimiterUnicodeAndNulls(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "value", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView},
		memory.Field{Name: "delimiter", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView},
	)
	batch := memory.NewBatch()
	batch.Strings["value"] = testStringVector([]string{"a,,b,", "中文", "", "ignored", "x-y"}, []bool{true, true, true, false, true})
	batch.Strings["delimiter"] = testStringVector([]string{",", "", ",", ",", "ignored"}, []bool{true, true, true, true, false})
	batch.Reset(5)

	program, err := CompileScalar(schema, Split(Col("value"), Col("delimiter")))
	if err != nil {
		t.Fatal(err)
	}
	got, err := program.Evaluate(batch, &ScalarWorkspace{}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	requireStringLists(t, got.StringList,
		[][]string{{"a", "", "b", ""}, {"中", "文"}, {""}, nil, nil},
		[][]bool{{true, true, true, true}, {true, true}, {true}, nil, nil},
		[]bool{true, true, true, false, false},
	)
}

func TestListExpressionCompilationContracts(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "s", Type: memory.TypeString, Encoding: memory.EncodingView},
		memory.Field{Name: "pattern", Type: memory.TypeString, Encoding: memory.EncodingView},
	)
	for _, expression := range []Expr{
		Split(Col("s"), I64(1)),
		RegexExtractAll(Col("s"), Col("pattern"), I64(0)),
		RegexExtractAll(Col("s"), Str("("), I64(0)),
		RegexExtractAll(Col("s"), Str("(a)"), I64(2)),
		RegexExtractAll(Col("s"), Str(strings.Repeat("a", maxRegexPatternBytes+1)), I64(0)),
	} {
		if _, err := CompileScalar(schema, expression); err == nil {
			t.Fatalf("CompileScalar(%s) succeeded", expression)
		}
	}
}

func BenchmarkSingleByteSplitScanner(b *testing.B) {
	value := bytes.Repeat([]byte("abcdefghijk,"), 256)
	b.Run("index-byte", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			remaining := value
			for len(remaining) != 0 {
				at := indexSplitDelimiter(remaining, []byte{','})
				if at < 0 {
					break
				}
				remaining = remaining[at+1:]
			}
		}
	})
	b.Run("bytes-index", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			remaining := value
			for len(remaining) != 0 {
				at := bytes.Index(remaining, []byte{','})
				if at < 0 {
					break
				}
				remaining = remaining[at+1:]
			}
		}
	})
}

func TestRegexExtractAllPreservesMatchCardinality(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "value", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView})
	batch := memory.NewBatch()
	batch.Strings["value"] = testStringVector([]string{"a1 b a2", "none", "", "ignored"}, []bool{true, true, true, false})
	batch.Reset(4)

	all := RegexExtractAll(Col("value"), Str(`a([0-9])?|b`), I64(1))
	program, err := CompileProjection(schema, []NamedExpr{Named(all, "captures"), Named(all, "again")})
	if err != nil {
		t.Fatal(err)
	}
	if program.Outputs[0].Root != program.Outputs[1].Root {
		t.Fatal("identical RegexExtractAll expressions did not share their CSE root")
	}
	for _, instruction := range program.Instructions {
		if instruction.Code == scalarLiteralString {
			t.Fatal("regex pattern was row-materialized")
		}
	}
	values, err := program.Evaluate(batch, &ScalarWorkspace{}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	requireStringLists(t, values[0].StringList,
		[][]string{{"1", "", "2"}, {}, {}, nil},
		[][]bool{{true, false, true}, {}, {}, nil},
		[]bool{true, true, true, false},
	)
}

func testStringVector(values []string, valid []bool) *memory.StringViewVector {
	v := &memory.StringViewVector{Offsets: make([]uint32, len(values)+1), Encoding: memory.EncodingView}
	if valid != nil {
		v.Validity = memory.EnsureBitmap(nil, len(values))
	}
	for i, value := range values {
		if valid == nil || valid[i] {
			v.Data = append(v.Data, value...)
			if v.Validity != nil {
				memory.Set(v.Validity, i)
			}
		}
		v.Offsets[i+1] = uint32(len(v.Data))
	}
	return v
}

func requireStringLists(t *testing.T, got *memory.StringListVector, values [][]string, childValid [][]bool, parentValid []bool) {
	t.Helper()
	if got == nil || got.Len() != len(values) {
		t.Fatalf("list vector=%v len=%d want=%d", got, got.Len(), len(values))
	}
	for row := range values {
		if got.IsValid(row) != parentValid[row] {
			t.Fatalf("row %d parent valid=%v want=%v", row, got.IsValid(row), parentValid[row])
		}
		if !parentValid[row] {
			continue
		}
		start, end, ok := got.ValueRange(row)
		if !ok || end-start != len(values[row]) {
			t.Fatalf("row %d range=(%d,%d,%v) want elements=%d", row, start, end, ok, len(values[row]))
		}
		for i, want := range values[row] {
			child := start + i
			if got.Values.IsValid(child) != childValid[row][i] {
				t.Fatalf("row %d child %d valid=%v want=%v", row, i, got.Values.IsValid(child), childValid[row][i])
			}
			if childValid[row][i] && string(got.Values.ValueBytes(child)) != want {
				t.Fatalf("row %d child %d=%q want=%q", row, i, got.Values.ValueBytes(child), want)
			}
		}
	}
}
