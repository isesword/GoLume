package physical

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"unicode"

	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

// Every iteration opens, drains and closes the real Filter -> Project query,
// including source reads, NULLs, filter materialization, output ownership and cleanup.
// Fixture setup, compile and the full output oracle are outside the timer.
func BenchmarkStringTrimFilterProject(b *testing.B) {
	for _, profile := range []string{"ascii", "unicode", "long", "unchanged"} {
		for _, encoding := range []struct {
			name       string
			dictionary bool
			entries    int
		}{{"view", false, 8}, {"dict8", true, 8}, {"dict4096", true, 4096}} {
			for _, pct := range []int{1, 50, 100} {
				for _, op := range []string{"both", "left", "right", "lower"} {
					b.Run(fmt.Sprintf("%s/%s/pct=%d/op=%s", profile, encoding.name, pct, op), func(b *testing.B) {
						drain, m := stringTrimQuery(b, profile, encoding.dictionary, pct, op, kernel.ModeAuto, encoding.entries)
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

func TestStringTrimFilterProject(t *testing.T) {
	for _, profile := range []string{"ascii", "unicode", "invalid", "long", "unchanged"} {
		for _, dictionary := range []bool{false, true} {
			for _, entries := range []int{1, 8, 257, 4096} {
				if !dictionary && entries != 8 {
					continue
				}
				for _, pct := range []int{0, 1, 50, 100} {
					for _, op := range []string{"both", "left", "right"} {
						for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
							drain, _ := stringTrimQuery(t, profile, dictionary, pct, op, mode, entries)
							drain(true)
						}
					}
				}
			}
		}
	}
}

func stringTrimQuery(t testing.TB, profile string, dictionary bool, pct int, op string, mode kernel.Mode, entries int) (func(bool), *selectionPeakManager) {
	t.Helper()
	const rows = 8199
	texts := []string{"  MiXeD ASCII Word\t", "\nmore ASCII 09 ", " a ", "", " already lower ", "ALREADY UPPER", "middle-SKU", "  two words  "}
	if profile == "unicode" {
		texts = []string{"\u3000İıſȺȾⱥⱦKµ\u0085", "\u00a0Σσς\u2009", "\u2028Över Ünder\u2029", "\u205f你好世界\u3000", " Straße ", "", "\u1680U+FFFD �\u202f", "mixed ASCII"}
	}
	if profile == "invalid" {
		texts = []string{" \xff\xfe ", "\u3000\xc0\x80\u3000", "\xed\xa0\x80 ", " \xe2\x82", " \x00 ", "", " \xef\xbf\xbd ", "\xf4\x90\x80\x80\t"}
	}
	if profile == "unchanged" {
		texts = []string{"1234567890", "-=+", "00000", "", "456", "789", "0123", "!@$%"}
	}
	if profile == "long" {
		for i := range texts {
			texts[i] = strings.Repeat(" \u3000\t", 128) + texts[i] + strings.Repeat("\n\u2009 ", 128)
		}
	}
	if entries != len(texts) {
		values := make([]string, entries)
		for i := range values {
			values[i] = texts[i%len(texts)]
			if entries > len(texts) {
				if profile == "unchanged" {
					values[i] = fmt.Sprintf("%d:%s", i, values[i])
				} else {
					values[i] = fmt.Sprintf(" \u3000%d:%s\u2009 ", i, values[i])
				}
			}
		}
		texts = values
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
			if op == "left" {
				want = strings.TrimLeftFunc(text, unicode.IsSpace)
			}
			if op == "right" {
				want = strings.TrimRightFunc(text, unicode.IsSpace)
			}
			wanted, wantedValid = append(wanted, want), append(wantedValid, row%17 != 0)
		}
	}
	if dictionary {
		field.Encoding, field.DictionaryDomain = memory.EncodingDictionary, "string-trim-query"
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
	if op == "left" {
		e = expr.TrimStart(expr.Col("s"))
	}
	if op == "right" {
		e = expr.TrimEnd(expr.Col("s"))
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
