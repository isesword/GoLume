package expr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"unicode"

	"gocolumn/memory"
)

func trimReviewOracle(s string, op Op) string {
	switch op {
	case OpTrimStart:
		return strings.TrimLeftFunc(s, unicode.IsSpace)
	case OpTrimEnd:
		return strings.TrimRightFunc(s, unicode.IsSpace)
	default:
		return strings.TrimSpace(s)
	}
}

func TestTrimReviewUnicodeOwnershipAndWarmBudget(t *testing.T) {
	corpus := []string{"", " \t\r\n", "no padding", "\x00", " \xff\xc0\x80 ", "\u200bword\ufeff"}
	// Cover each Unicode whitespace rune, non-whitespace neighbors and malformed
	// bytes independently of the production byte-slice implementation.
	for r := rune(0); r <= unicode.MaxRune; r++ {
		if unicode.IsSpace(r) {
			corpus = append(corpus, string(r)+"x"+string(r), string(r))
		}
	}
	rng := rand.New(rand.NewSource(801))
	for range 1000 {
		b := make([]byte, rng.Intn(128))
		rng.Read(b)
		corpus = append(corpus, "\u2003 "+string(b)+" \u00a0")
	}
	for _, dictionary := range []bool{false, true} {
		t.Run(fmt.Sprint(dictionary), func(t *testing.T) {
			schema, batch := caseBatch(corpus, nil, dictionary)
			p, err := CompileExpressions(schema, []NamedExpr{Named(Trim(Col("s")), "both"), Named(TrimStart(Col("s")), "left"), Named(TrimEnd(Col("s")), "right")})
			if err != nil {
				t.Fatal(err)
			}
			m := &scalarTestManager{base: memory.NewBudgetManager(0)}
			w, err := NewWorkspace(context.Background(), p, m)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			var source []byte
			if dictionary {
				source = batch.DictStrings["s"].Data
			} else {
				source = batch.Strings["s"].Data
			}
			original := bytes.Clone(source)
			results, err := w.Evaluate(context.Background(), batch, testKernelSet())
			if err != nil {
				t.Fatal(err)
			}
			calls := m.calls
			for range 3 {
				results, err = w.Evaluate(context.Background(), batch, testKernelSet())
				if err != nil {
					t.Fatal(err)
				}
			}
			if m.calls != calls {
				t.Fatalf("warm evaluation reserved again: %d -> %d", calls, m.calls)
			}
			if !bytes.Equal(source, original) {
				t.Fatal("input was mutated")
			}
			for i := range source {
				source[i] = '!'
			}
			for col, op := range []Op{OpTrim, OpTrimStart, OpTrimEnd} {
				if results[col].String == nil || results[col].DictionaryString != nil {
					t.Fatal("result not owned materialized string")
				}
				for row, input := range corpus {
					if got, want := string(results[col].String.ValueBytes(row)), trimReviewOracle(input, op); got != want {
						t.Fatalf("%s row=%d got=%q want=%q", op, row, got, want)
					}
				}
			}
			if m.Used() != w.MemoryBytes() {
				t.Fatal("accounting mismatch")
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("close leaked")
			}
		})
	}
}

func TestTrimReviewCheckpointsAndPartialWrites(t *testing.T) {
	for _, op := range []Op{OpTrim, OpTrimStart, OpTrimEnd} {
		for failAt := 1; failAt <= 6; failAt++ {
			checks := 0
			out := scalarRegister{typ: memory.TypeString, offsets: []uint32{99, 99, 99, 99}, data: make([]byte, 0, 128)}
			out.checkContext = func() error {
				checks++
				if checks == failAt {
					return context.Canceled
				}
				return nil
			}
			// Warm storage isolates the pre-transform and pre-append checkpoints.
			in := scalarRegister{typ: memory.TypeString, strView: phase33StringVector([]string{" a ", " b ", " c "}, nil)}
			err := evalStringUnary(&out, &in, op, 3)
			if !errors.Is(err, context.Canceled) || checks != failAt {
				t.Fatalf("%s checkpoint=%d err=%v checks=%d", op, failAt, err, checks)
			}
			completed := (failAt - 1) / 2
			want := ""
			for row := 0; row < completed; row++ {
				want += trimReviewOracle([]string{" a ", " b ", " c "}[row], op)
			}
			if string(out.data) != want {
				t.Fatalf("partial bytes=%q want=%q", out.data, want)
			}
			expected := "context canceled"
			if failAt%2 == 0 {
				expected = fmt.Sprintf("%s at row %d: context canceled", op, completed)
			}
			if err.Error() != expected {
				t.Fatalf("error=%q want=%q", err, expected)
			}
			if out.offsets[completed+1] != 99 {
				t.Fatal("failed row published an offset")
			}
		}
	}
}

func TestTrimReviewMalformedAndSkippedRows(t *testing.T) {
	for _, op := range []Op{OpTrim, OpTrimStart, OpTrimEnd} {
		for _, kind := range []string{"computed", "view", "dictionary"} {
			in := scalarRegister{typ: memory.TypeString, data: []byte(" a "), offsets: []uint32{0, 3, 99, 1}}
			if kind == "view" {
				in.strView = &memory.StringViewVector{Data: in.data, Offsets: in.offsets}
			}
			if kind == "dictionary" {
				in.strDict = &memory.DictionaryStringVector{Data: in.data, Offsets: []uint32{0, 3, 99}, Indices: []uint32{0, 1, ^uint32(0)}}
			}
			out := scalarRegister{typ: memory.TypeString, data: make([]byte, 0, 64), checkContext: func() error { return nil }}
			err := evalStringUnary(&out, &in, op, 3)
			if kind == "computed" {
				if err == nil || err.Error() != "computed string offsets invalid at row 1" {
					t.Fatalf("computed error=%v", err)
				}
			} else if err != nil || !reflect.DeepEqual(out.offsets, []uint32{0, uint32(len(out.data)), uint32(len(out.data)), uint32(len(out.data))}) {
				t.Fatalf("%s malformed semantics err=%v offsets=%v", kind, err, out.offsets)
			}
			// A malformed demanded value wins over cancellation before transformation.
			in.strView, in.strDict = nil, nil
			in.offsets = []uint32{0, 99}
			out.checkContext = func() error { t.Fatal("checked cancellation before fetching malformed value"); return context.Canceled }
			if err = evalStringUnary(&out, &in, op, 1); err == nil || err.Error() != "computed string offsets invalid at row 0" {
				t.Fatalf("precedence err=%v", err)
			}
			// Null/masked rows cannot touch an absent dictionary code or context.
			in.strDict = &memory.DictionaryStringVector{}
			out.validity = memory.EnsureBitmap(nil, 3)
			if err = evalStringUnary(&out, &in, op, 3); err != nil || len(out.data) != 0 || !reflect.DeepEqual(out.offsets, []uint32{0, 0, 0, 0}) {
				t.Fatalf("skip err=%v offsets=%v", err, out.offsets)
			}
		}
	}
}

func TestTrimReviewSelectedRows(t *testing.T) {
	for _, dictionary := range []bool{false, true} {
		for _, rows := range []int{129, 1, 65, 0, 257} {
			texts, valid := make([]string, rows), make([]bool, rows)
			active := memory.EnsureBitmap(nil, rows)
			for i := range rows {
				texts[i] = " \u2003a\xff\u00a0 "
				valid[i] = i%5 != 0
				if i%3 == 0 {
					memory.Set(active, i)
				}
			}
			schema, b := caseBatch(texts, valid, dictionary)
			p, err := Compile(schema, Trim(Col("s")))
			if err != nil {
				t.Fatal(err)
			}
			var w evaluationState
			if err := evaluateInstructionsActive(p.instructions, b, &w, testKernelSet(), active); err != nil {
				w.Close()
				t.Fatal(err)
			}
			result, err := w.outputValue(p.root, rows)
			if err != nil {
				w.Close()
				t.Fatal(err)
			}
			for i := range rows {
				wantValid := valid[i] && i%3 == 0
				if result.String.IsValid(i) != wantValid {
					t.Fatalf("validity row=%d", i)
				}
				if wantValid && string(result.String.ValueBytes(i)) != "a\xff" {
					t.Fatalf("value row=%d", i)
				}
				if !wantValid && result.String.Offsets[i] != result.String.Offsets[i+1] {
					t.Fatalf("skipped bytes row=%d", i)
				}
			}
			w.Close()
		}
	}
}

func TestTrimReviewBudgetFailureRetry(t *testing.T) {
	for _, dictionary := range []bool{false, true} {
		schema, b := caseBatch([]string{" a ", strings.Repeat(" x ", 65536), "  "}, nil, dictionary)
		p, err := Compile(schema, Trim(Col("s")))
		if err != nil {
			t.Fatal(err)
		}
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
			result, err := w.Evaluate(context.Background(), b, testKernelSet())
			if !errors.Is(err, memory.ErrBudgetExceeded) || result != nil {
				t.Fatalf("fail=%d err=%v", fail, err)
			}
			if m.Used() != w.MemoryBytes() {
				t.Fatalf("failure accounting %d", fail)
			}
			m.failAt = 0
			result, err = w.Evaluate(context.Background(), b, testKernelSet())
			if err != nil {
				t.Fatal(err)
			}
			if string(result[0].String.ValueBytes(0)) != "a" || string(result[0].String.ValueBytes(1)) != strings.TrimSpace(strings.Repeat(" x ", 65536)) {
				t.Fatal("bad retry")
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("failure leak")
			}
		}
	}
}

func TestTrimReviewOutputBudgetPartialRow(t *testing.T) {
	for _, op := range []Op{OpTrim, OpTrimStart, OpTrimEnd} {
		first, second := trimReviewOracle(" a ", op), trimReviewOracle(" longer ", op)
		out := scalarRegister{typ: memory.TypeString, offsets: []uint32{99, 99, 99}, data: make([]byte, 0, len(first)), checkContext: func() error { return nil }}
		requests := 0
		out.acquire = func(n int64) (memory.Reservation, error) { requests++; return nil, memory.ErrBudgetExceeded }
		in := scalarRegister{typ: memory.TypeString, strView: phase33StringVector([]string{" a ", " longer "}, nil)}
		err := evalStringUnary(&out, &in, op, 2)
		if !errors.Is(err, memory.ErrBudgetExceeded) || err.Error() != fmt.Sprintf("%s at row 1: %s", op, memory.ErrBudgetExceeded) {
			t.Fatalf("error=%v", err)
		}
		if string(out.data) != first || !reflect.DeepEqual(out.offsets, []uint32{0, uint32(len(first)), 99}) || requests != 1 {
			t.Fatalf("partial output=%q offsets=%v requests=%d", out.data, out.offsets, requests)
		}
		out.acquire = func(n int64) (memory.Reservation, error) { return scalarNoopReservation{}, nil }
		if err = evalStringUnary(&out, &in, op, 2); err != nil || string(out.data) != first+second {
			t.Fatalf("retry=%q err=%v", out.data, err)
		}
	}
}
