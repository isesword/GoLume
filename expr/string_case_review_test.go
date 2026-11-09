package expr

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"strings"
	"testing"
	"unicode/utf8"

	"gocolumn/memory"
)

// Independently compare every Unicode scalar and random malformed UTF-8 with
// the Go library, rather than restating the production mapping algorithm.
func TestStringCaseReviewGoDifferential(t *testing.T) {
	corpus := []string{"", "ASCII", "already lower", "\xff", "\xc0\x80A", "A\xed\xa0\x80z", "İıſKȺⱥßẞΣσς", "\x00a\x00Z"}
	var chunk strings.Builder
	for r := rune(0); r <= utf8.MaxRune; r++ {
		if r >= 0xd800 && r <= 0xdfff {
			continue
		}
		chunk.WriteRune(r)
		if chunk.Len() >= 8192 {
			corpus = append(corpus, chunk.String())
			chunk.Reset()
		}
	}
	corpus = append(corpus, chunk.String())
	rng := rand.New(rand.NewSource(947))
	for range 3000 {
		b := make([]byte, rng.Intn(96))
		rng.Read(b)
		corpus = append(corpus, string(b))
	}
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString})
	p, err := CompileExpressions(schema, []NamedExpr{Named(Lower(Col("s")), "lo"), Named(Upper(Col("s")), "hi"), Named(Lower(Upper(Col("s"))), "chain")})
	if err != nil {
		t.Fatal(err)
	}
	b := memory.NewBatch()
	b.Strings["s"] = phase33StringVector(corpus, nil)
	b.Rows = len(corpus)
	original := bytes.Clone(b.Strings["s"].Data)
	var w evaluationState
	defer w.Close()
	values, err := evaluateProjectionTest(p, b, &w, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	for row, input := range corpus {
		wants := []string{strings.ToLower(input), strings.ToUpper(input), strings.ToLower(strings.ToUpper(input))}
		for col, want := range wants {
			got := values[col].String
			if got == nil || values[col].DictionaryString != nil {
				t.Fatal("output is not materialized")
			}
			if text := string(got.ValueBytes(row)); text != want {
				t.Fatalf("row=%d col=%d input=%q got=%q want=%q", row, col, input, text, want)
			}
		}
	}
	if !bytes.Equal(original, b.Strings["s"].Data) {
		t.Fatal("mutated borrowed input")
	}
}

func TestStringCaseReviewDictionaryReuseAndNull(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingDictionary, DictionaryDomain: "same"})
	p, err := CompileExpressions(schema, []NamedExpr{Named(Lower(Col("s")), "lo"), Named(Upper(Col("s")), "hi")})
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(0)
	w, err := NewWorkspace(context.Background(), p, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, rows := range []int{129, 1, 65, 0, 3, 257} {
		texts := []string{strings.Repeat("Ⱥſ\xffZ", rows+1), "İKß", ""}
		plain := phase33StringVector(texts, nil)
		dict := &memory.DictionaryStringVector{Indices: make([]uint32, rows), Offsets: plain.Offsets, Data: plain.Data, Validity: memory.EnsureBitmap(nil, rows), Encoding: memory.EncodingDictionary, DictionaryDomain: "same"}
		for i := range rows {
			if i%3 != 1 {
				memory.Set(dict.Validity, i)
				dict.Indices[i] = uint32(i % 3)
			} else {
				dict.Indices[i] = ^uint32(0)
			}
		}
		b := memory.NewBatch()
		b.Rows = rows
		b.DictStrings["s"] = dict
		values, err := w.Evaluate(context.Background(), b, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			for col, f := range []func(string) string{strings.ToLower, strings.ToUpper} {
				got := values[col].String
				if got == nil || values[col].DictionaryString != nil {
					t.Fatal("dictionary output escaped")
				}
				if got.IsValid(i) != (i%3 != 1) {
					t.Fatalf("validity row=%d", i)
				}
				if i%3 != 1 && string(got.ValueBytes(i)) != f(texts[i%3]) {
					t.Fatalf("stale dictionary row=%d", i)
				}
				if i%3 == 1 && got.Offsets[i] != got.Offsets[i+1] {
					t.Fatal("null consumed output bytes")
				}
			}
		}
		if manager.Used() != w.MemoryBytes() {
			t.Fatalf("used=%d accounted=%d", manager.Used(), w.MemoryBytes())
		}
	}
	w.Close()
	if manager.Used() != 0 {
		t.Fatalf("leaked %d", manager.Used())
	}
}

func TestStringCaseReviewReservationFailuresRetry(t *testing.T) {
	p, err := CompileExpressions(memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}), []NamedExpr{Named(Lower(Col("s")), "lo"), Named(Upper(Lower(Col("s"))), "hi")})
	if err != nil {
		t.Fatal(err)
	}
	texts := []string{strings.Repeat("Ⱥ\xffſ", 65536), "small", ""}
	b := memory.NewBatch()
	b.Rows = len(texts)
	b.Strings["s"] = phase33StringVector(texts, nil)
	probe := &livenessFaultManager{BudgetManager: memory.NewBudgetManager(0)}
	w, err := NewWorkspace(context.Background(), p, probe)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	calls := probe.calls
	w.Close()
	for fail := 1; fail <= calls; fail++ {
		m := &livenessFaultManager{BudgetManager: memory.NewBudgetManager(0), failAt: fail, failure: memory.ErrBudgetExceeded}
		w, err := NewWorkspace(context.Background(), p, m)
		if err != nil {
			t.Fatal(err)
		}
		values, err := w.Evaluate(context.Background(), b, testKernelSet())
		if !errors.Is(err, memory.ErrBudgetExceeded) || values != nil {
			t.Fatalf("fail=%d err=%v values=%v", fail, err, values)
		}
		if m.Used() != w.MemoryBytes() {
			t.Fatalf("failure accounting fail=%d", fail)
		}
		m.failAt = 0
		values, err = w.Evaluate(context.Background(), b, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		for i, input := range texts {
			if string(values[0].String.ValueBytes(i)) != strings.ToLower(input) || string(values[1].String.ValueBytes(i)) != strings.ToUpper(strings.ToLower(input)) {
				t.Fatalf("retry fail=%d row=%d", fail, i)
			}
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatalf("failure leak fail=%d bytes=%d", fail, m.Used())
		}
	}
}

func TestStringCaseReviewActiveMaskAndCancellation(t *testing.T) {
	p, err := Compile(memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Nullable: true}), Upper(Lower(Col("s"))))
	if err != nil {
		t.Fatal(err)
	}
	var w evaluationState
	defer w.Close()
	for _, rows := range []int{129, 1, 0, 65, 257} {
		texts := make([]string, rows)
		valid := make([]bool, rows)
		mask := memory.EnsureBitmap(nil, rows)
		wantBytes := 0
		for i := range rows {
			texts[i] = "Ⱥſ\xffa"
			valid[i] = i%5 != 0
			if i%3 == 0 {
				memory.Set(mask, i)
				if valid[i] {
					wantBytes += len(strings.ToUpper(strings.ToLower(texts[i])))
				}
			}
		}
		b := memory.NewBatch()
		b.Rows = rows
		b.Strings["s"] = phase33StringVector(texts, valid)
		if err := evaluateInstructionsActive(p.instructions, b, &w, testKernelSet(), mask); err != nil {
			t.Fatal(err)
		}
		got, err := w.outputValue(p.root, rows)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.String.Data) != wantBytes {
			t.Fatalf("inactive rows materialized: got=%d want=%d", len(got.String.Data), wantBytes)
		}
		for i := range rows {
			wantValid := valid[i] && i%3 == 0
			if got.String.IsValid(i) != wantValid {
				t.Fatalf("mask row=%d", i)
			}
			if wantValid && string(got.String.ValueBytes(i)) != strings.ToUpper(strings.ToLower(texts[i])) {
				t.Fatalf("value row=%d", i)
			}
		}
	}
	// Cancellation must be checked even when a warm output buffer needs no
	// reservation; the cancelled attempt must leave its previous bytes intact.
	out := scalarRegister{data: make([]byte, 3, 1024), checkContext: func() error { return context.Canceled }}
	copy(out.data, "old")
	for _, input := range []string{"ascii", "Ⱥſ\xff", ""} {
		err := appendScalarCase(&out, []byte(input), OpLower)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel input=%q err=%v", input, err)
		}
		if string(out.data) != "old" {
			t.Fatal("cancelled append wrote bytes")
		}
	}
}
