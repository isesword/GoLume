package expr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode"

	"gocolumn/kernel"
	"gocolumn/memory"
)

func trimOracle(value string, op Op) string {
	switch op {
	case OpTrimStart:
		return strings.TrimLeftFunc(value, unicode.IsSpace)
	case OpTrimEnd:
		return strings.TrimRightFunc(value, unicode.IsSpace)
	default:
		return strings.TrimSpace(value)
	}
}

func TestStringTrimOracleAndWarmBuilder(t *testing.T) {
	texts := []string{"", " \t\n\r\v\f", " no change inside ", "\u0085\u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000雪\u3000", "\u200babc\ufeff", " \xff\xc0\x80\xed\xa0\x80 \x00 \xf4\x90\x80\x80 ", strings.Repeat(" \t\u3000", 4096) + "tail\u3000"}
	for value := range 256 {
		texts = append(texts, " \t"+string([]byte{byte(value)})+"\n ")
	}
	for _, dictionary := range []bool{false, true} {
		_, batch := caseBatch(texts, nil, dictionary)
		in := scalarRegister{typ: memory.TypeString, strView: batch.Strings["s"], strDict: batch.DictStrings["s"]}
		for _, op := range []Op{OpTrim, OpTrimStart, OpTrimEnd} {
			t.Run(fmt.Sprintf("%s/dictionary=%t", op, dictionary), func(t *testing.T) {
				m := &scalarTestManager{base: memory.NewBudgetManager(0)}
				w := evaluationState{manager: m}
				out := scalarRegister{typ: memory.TypeString, acquire: w.acquire, checkContext: w.checkContext}
				if err := prepareScalarBuffers(&out, len(texts)); err != nil {
					t.Fatal(err)
				}
				fillAll(out.validity, len(texts))
				// Mask one row to verify that its output range stays empty.
				out.validity[0] &^= 1 << 3
				if err := evalStringUnary(&out, &in, op, len(texts)); err != nil {
					t.Fatal(err)
				}
				for row, text := range texts {
					want := trimOracle(text, op)
					if row == 3 {
						want = ""
					}
					if got := string(out.data[out.offsets[row]:out.offsets[row+1]]); got != want {
						t.Fatalf("row=%d got=%q want=%q", row, got, want)
					}
				}
				calls := m.calls
				if allocs := testing.AllocsPerRun(50, func() {
					if err := evalStringUnary(&out, &in, op, len(texts)); err != nil {
						panic(err)
					}
				}); allocs != 0 || m.calls != calls {
					t.Fatalf("warm allocations=%v reservation calls=%d/%d", allocs, m.calls, calls)
				}
				w.Close()
				if m.Used() != 0 {
					t.Fatal("budget leak")
				}
			})
		}
	}
}

func TestStringTrimLifetimeAndGuards(t *testing.T) {
	for _, dict := range []bool{false, true} {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			texts := []string{" \t雪\u3000", " \xff A \x00 ", "not-read", "\u3000 B\t", "", " \n ", " last "}
			valid := []bool{true, true, false, true, true, true, true}
			schema, batch := caseBatch(texts, valid, dict)
			trimmed := Trim(Col("s"))
			p, err := CompileExpressions(schema, []NamedExpr{Named(trimmed, "both"), Named(trimmed, "alias"), Named(TrimStart(Col("s")), "left"), Named(TrimEnd(Col("s")), "right"), Named(TrimEnd(TrimStart(Col("s"))), "chain"), Named(If(GT(ByteLength(Col("s")), I64(3)), trimmed, TrimStart(Col("s"))), "guard"), Named(Col("s"), "original")})
			if err != nil {
				t.Fatal(err)
			}
			m := &scalarTestManager{base: memory.NewBudgetManager(0)}
			w, err := NewWorkspace(context.Background(), p, m)
			if err != nil {
				t.Fatal(err)
			}
			kernels := kernel.New(mode)
			for pass := range 5 {
				// Same dictionary domain and backing storage, different bytes each time.
				var data []byte
				if dict {
					data = batch.DictStrings["s"].Data
				} else {
					data = batch.Strings["s"].Data
				}
				for i := range data {
					if data[i] == 'A' || data[i] == 'C' {
						data[i] = byte('A' + 2*(pass%2))
					}
				}
				texts[1] = strings.ReplaceAll(texts[1], "C", "A")
				if pass%2 == 1 {
					texts[1] = strings.ReplaceAll(texts[1], "A", "C")
				}
				original := bytes.Clone(data)
				got, err := w.Evaluate(context.Background(), batch, kernels)
				if err != nil {
					t.Fatal(err)
				}
				for row := range texts {
					index := row
					text := texts[row]
					guard := strings.TrimLeftFunc(text, unicode.IsSpace)
					if len(text) > 3 {
						guard = strings.TrimSpace(text)
					}
					wants := []string{strings.TrimSpace(text), strings.TrimSpace(text), strings.TrimLeftFunc(text, unicode.IsSpace), strings.TrimRightFunc(text, unicode.IsSpace), strings.TrimSpace(text), guard}
					for col, want := range wants {
						v := got[col].String
						if v == nil || got[col].DictionaryString != nil {
							t.Fatal("computed output must be owned plain string")
						}
						if v.IsValid(index) != valid[row] || valid[row] && string(v.ValueBytes(index)) != want {
							t.Fatalf("dict=%t mode=%s pass=%d row=%d col=%d", dict, mode, pass, row, col)
						}
					}
				}
				if !bytes.Equal(original, data) || m.Used() != w.MemoryBytes() {
					t.Fatal("borrowed input or accounting changed")
				}
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("close leaked")
			}
		}
	}
}

func TestStringTrimBudgetGrowthRetry(t *testing.T) {
	for _, dict := range []bool{false, true} {
		for _, op := range []Op{OpTrim, OpTrimStart, OpTrimEnd} {
			schema, small := caseBatch([]string{" x "}, nil, dict)
			_, large := caseBatch([]string{" " + strings.Repeat("雪", 4096) + " "}, nil, dict)
			p, err := Compile(schema, Unary{Op: op, Value: Col("s")})
			if err != nil {
				t.Fatal(err)
			}
			m := &scalarTestManager{base: memory.NewBudgetManager(0)}
			w, err := NewWorkspace(context.Background(), p, m)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = w.Evaluate(context.Background(), small, testKernelSet()); err != nil {
				t.Fatal(err)
			}
			used, calls := m.Used(), m.calls
			m.limit = used
			if _, err = w.Evaluate(context.Background(), small, testKernelSet()); err != nil || m.calls != calls {
				t.Fatalf("warm trim reserved nonexistent scratch: %v calls=%d/%d", err, m.calls, calls)
			}
			if _, err = w.Evaluate(context.Background(), large, testKernelSet()); !errors.Is(err, memory.ErrBudgetExceeded) || !strings.Contains(err.Error(), "row 0") {
				t.Fatalf("growth rejection: %v", err)
			}
			if m.Used() != used || w.closed {
				t.Fatal("rejected growth lost owned memory")
			}
			m.limit = 0
			if _, err = w.Evaluate(context.Background(), large, testKernelSet()); err != nil {
				t.Fatalf("retry: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err = w.Evaluate(ctx, small, testKernelSet()); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel: %v", err)
			}
			if m.Used() != 0 {
				t.Fatal("cancel leaked")
			}
			w.Close()
		}
	}
}

func TestStringTrimWhitespaceRequiresNoPayload(t *testing.T) {
	// A long all-whitespace input has an empty result and needs no byte payload.
	for _, op := range []Op{OpTrim, OpTrimStart, OpTrimEnd} {
		text := []byte(strings.Repeat("\u3000 \t", 10000))
		m := &scalarTestManager{base: memory.NewBudgetManager(0)}
		w := evaluationState{manager: m}
		out := scalarRegister{typ: memory.TypeString, acquire: w.acquire, checkContext: w.checkContext}
		if err := prepareScalarBuffers(&out, 1); err != nil {
			t.Fatal(err)
		}
		fillAll(out.validity, 1)
		m.limit = m.Used()
		calls := m.calls
		in := scalarRegister{typ: memory.TypeString, strView: &memory.StringViewVector{Data: text, Offsets: []uint32{0, uint32(len(text))}}}
		if err := evalStringUnary(&out, &in, op, 1); err != nil {
			t.Fatal(err)
		}
		if len(out.data) != 0 || m.calls != calls {
			t.Fatal("empty trim output requested payload budget")
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("leak")
		}
	}
}
