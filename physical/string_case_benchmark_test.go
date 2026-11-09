package physical

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

// This opens, drains and closes a real Filter -> Project stream over 8199 rows
// in 4096-row batches. It includes source reads, NULLs, sparse/dense selection,
// materialization and query workspace cleanup, but excludes fixture creation
// and expression compilation. Managed peak excludes the immutable input fixture.
func BenchmarkStringCaseFilterProject(b *testing.B) {
	for _, profile := range []string{"ascii", "unicode", "unchanged"} {
		for _, dictionary := range []bool{false, true} {
			for _, pct := range []int{1, 50, 100} {
				for _, op := range []string{"lower", "upper", "trim_control"} {
					b.Run(fmt.Sprintf("%s/dict=%t/pct=%d/op=%s", profile, dictionary, pct, op), func(b *testing.B) {
						drain, m := stringCaseQuery(b, profile, dictionary, pct, op, kernel.ModeAuto)
						drain(true)
						m.peak = 0
						b.ReportAllocs()
						b.ResetTimer()
						for range b.N {
							drain(false)
						}
						b.StopTimer()
						b.ReportMetric(float64(m.peak), "peak_bytes")
						b.ReportMetric(8199, "source_rows/op")
					})
				}
			}
		}
	}
}

func TestStringCaseFilterProject(t *testing.T) {
	for _, dictionary := range []bool{false, true} {
		for _, pct := range []int{1, 50, 100} {
			for _, op := range []string{"lower", "upper", "trim_control"} {
				for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
					drain, _ := stringCaseQuery(t, "unicode", dictionary, pct, op, mode)
					drain(true)
				}
			}
		}
	}
}

func stringCaseQuery(t testing.TB, profile string, dictionary bool, pct int, op string, mode kernel.Mode) (func(bool), *selectionPeakManager) {
	t.Helper()
	const rows = 8199
	texts := []string{"MiXeD ASCII Word", "more ASCII 09", "a", "", " already lower ", "ALREADY UPPER", "middle-SKU", "two words"}
	if profile == "unicode" {
		texts = []string{"İıſȺȾⱥⱦKµ", "Σσς", "Över Ünder", "你好世界", "Straße", "", "U+FFFD �", "mixed ASCII"}
	}
	if profile == "unchanged" {
		texts = []string{"1234567890", "-=+", "00000", "", "456", "789", "0123", "!@$%"}
	}
	field := memory.Field{Name: "s", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView}
	batch := memory.NewBatch()
	batch.Reset(rows)
	batch.Int64s["gate"] = &memory.Int64Vector{Values: make([]int64, rows)}
	valid := make(memory.Bitmap, memory.WordsFor(rows))
	values := &memory.StringViewVector{Offsets: []uint32{0}, Validity: valid, Encoding: memory.EncodingView}
	indices := make([]uint32, rows)
	var wanted []string
	var wantedValid []bool
	for row := 0; row < rows; row++ {
		gate := int64((row * 37) % 100)
		batch.Int64s["gate"].Values[row] = gate
		indices[row] = uint32(row % len(texts))
		text := texts[indices[row]]
		if row%17 != 0 {
			memory.Set(valid, row)
		}
		if !dictionary || row < len(texts) {
			values.Data = append(values.Data, text...)
			values.Offsets = append(values.Offsets, uint32(len(values.Data)))
		}
		if gate < int64(pct) {
			want := strings.TrimSpace(text)
			if op == "lower" {
				want = strings.ToLower(text)
			}
			if op == "upper" {
				want = strings.ToUpper(text)
			}
			wanted, wantedValid = append(wanted, want), append(wantedValid, row%17 != 0)
		}
	}
	if dictionary {
		field.Encoding, field.DictionaryDomain = memory.EncodingDictionary, "string-case-query"
		batch.DictStrings["s"] = &memory.DictionaryStringVector{Indices: indices, Offsets: values.Offsets, Data: values.Data, Validity: valid, Encoding: field.Encoding, DictionaryDomain: field.DictionaryDomain}
	} else {
		batch.Strings["s"] = values
	}
	input, err := inmemory.New(memory.NewSchema(field, memory.Field{Name: "gate", Type: memory.TypeInt64}), batch)
	if err != nil {
		t.Fatal(err)
	}
	filtered, err := NewFilterSource(input, expr.LT(expr.Col("gate"), expr.I64(int64(pct))))
	if err != nil {
		t.Fatal(err)
	}
	e := expr.Trim(expr.Col("s"))
	if op == "lower" {
		e = expr.Lower(expr.Col("s"))
	}
	if op == "upper" {
		e = expr.Upper(expr.Col("s"))
	}
	m := &selectionPeakManager{base: memory.NewBudgetManager(0)}
	p, err := NewProjectSource(filtered, []expr.NamedExpr{expr.Named(e, "result")}, false, m)
	if err != nil {
		t.Fatal(err)
	}
	ctx, out := context.Background(), memory.NewBatch()
	opts := source.ReadOptions{Kernels: kernel.New(mode), BatchRows: 4096, MemoryManager: m}
	return func(check bool) {
		stream, err := p.NewBatchStream(ctx, opts)
		if err != nil {
			t.Fatal(err)
		}
		seen := 0
		for {
			ok, err := stream.Next(ctx, out)
			if err != nil {
				stream.Close()
				t.Fatal(err)
			}
			if !ok {
				break
			}
			v := out.Strings["result"]
			if v == nil || out.DictStrings["result"] != nil {
				t.Fatal("wrong result encoding")
			}
			if check {
				for row := 0; row < out.Rows; row++ {
					index := seen + row
					if index >= len(wanted) || v.IsValid(row) != wantedValid[index] || wantedValid[index] && string(v.ValueBytes(row)) != wanted[index] {
						t.Fatalf("%s/%t/%d/%s/%s mismatch row %d", profile, dictionary, pct, op, mode, index)
					}
				}
			}
			seen += out.Rows
		}
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
		if seen != len(wanted) || m.Used() != 0 {
			t.Fatalf("rows=%d want=%d retained=%d", seen, len(wanted), m.Used())
		}
	}, m
}
