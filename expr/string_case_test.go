package expr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"gocolumn/kernel"
	"gocolumn/memory"
)

func caseBatch(values []string, valid []bool, dictionary bool) (memory.Schema, *memory.Batch) {
	b := memory.NewBatch()
	b.Reset(len(values))
	v := phase33StringVector(values, valid)
	field := memory.Field{Name: "s", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView}
	if dictionary {
		field.Encoding, field.DictionaryDomain = memory.EncodingDictionary, "case-test"
		indices := make([]uint32, len(values))
		for i := range indices {
			indices[i] = uint32(i)
		}
		b.DictStrings["s"] = &memory.DictionaryStringVector{Indices: indices, Data: v.Data, Offsets: v.Offsets, Validity: v.Validity, DictionaryDomain: field.DictionaryDomain, Encoding: field.Encoding}
	} else {
		b.Strings["s"] = v
	}
	return memory.NewSchema(field), b
}

func TestStringCaseUnicodeOracle(t *testing.T) {
	// Width-changing mappings and malformed sequences complement the independent
	// full-Unicode corpus in string_case_review_test.go.
	values := []string{"", "ASCII !azAZ09", "İıſȺȾⱥⱦKµ", "ΣσςǅǆǄ", "你好世界", "a\x00Z"}
	for i := 0; i < 256; i++ {
		values = append(values, string([]byte{byte(i)}), string([]byte{'a', byte(i), 'Z'}))
	}
	values = append(values, "\xff\xfe", "\xe2\x82", "\xed\xa0\x80", "\xf4\x90\x80\x80", "\xef\xbf\xbd", "\xc0\x80")
	for _, op := range []Op{OpLower, OpUpper} {
		w := evaluationState{}
		out := scalarRegister{acquire: w.acquire, checkContext: w.checkContext}
		for _, value := range values {
			out.data = out.data[:0]
			if err := appendScalarCase(&out, []byte(value), op); err != nil {
				t.Fatal(err)
			}
			want := strings.ToLower(value)
			if op == OpUpper {
				want = strings.ToUpper(value)
			}
			if string(out.data) != want {
				t.Fatalf("%v(%q)=%q, want %q", op, value, out.data, want)
			}
		}
		w.Close()
	}
}

func TestStringCaseWorkspaceOwnershipAndReuse(t *testing.T) {
	values := []string{"Mixed ASCII", "İıſȺȾⱥⱦKµ", "\xffa\xe2\x82", "", "not-read"}
	valid := []bool{true, true, true, true, false}
	for _, dictionary := range []bool{false, true} {
		schema, batch := caseBatch(values, valid, dictionary)
		var original []byte
		if dictionary {
			original = append([]byte(nil), batch.DictStrings["s"].Data...)
		} else {
			original = append([]byte(nil), batch.Strings["s"].Data...)
		}
		low := Lower(Col("s"))
		p, err := CompileExpressions(schema, []NamedExpr{Named(low, "lower"), Named(low, "again"), Named(Upper(low), "chain"), Named(Col("s"), "original")})
		if err != nil {
			t.Fatal(err)
		}
		manager := &scalarTestManager{base: memory.NewBudgetManager(0)}
		w, err := NewWorkspace(context.Background(), p, manager)
		if err != nil {
			t.Fatal(err)
		}
		for pass := 0; pass < 3; pass++ {
			result, err := w.Evaluate(context.Background(), batch, kernel.New(kernel.ModeAuto))
			if err != nil {
				t.Fatal(err)
			}
			for output := 0; output < 3; output++ {
				if result[output].DictionaryString != nil || result[output].String == nil {
					t.Fatal("computed output did not materialize")
				}
				for row, text := range values {
					v := result[output].String
					if v.IsValid(row) != valid[row] {
						t.Fatal("validity changed")
					}
					if !valid[row] {
						continue
					}
					want := strings.ToLower(text)
					if output == 2 {
						want = strings.ToUpper(want)
					}
					if string(v.ValueBytes(row)) != want {
						t.Fatalf("output %d row %d got %q want %q", output, row, v.ValueBytes(row), want)
					}
				}
			}
			if manager.Used() != w.MemoryBytes() {
				t.Fatalf("managed=%d retained=%d", manager.Used(), w.MemoryBytes())
			}
		}
		calls := manager.calls
		var evalErr error
		kernels := kernel.New(kernel.ModeScalar)
		for range 100 {
			_, evalErr = w.Evaluate(context.Background(), batch, kernels)
		}
		if evalErr != nil || manager.calls != calls {
			t.Fatalf("warm reservation calls=%d/%d err=%v", manager.calls, calls, evalErr)
		}
		var input []byte
		if dictionary {
			input = batch.DictStrings["s"].Data
		} else {
			input = batch.Strings["s"].Data
		}
		if !bytes.Equal(input, original) {
			t.Fatal("borrowed input overwritten")
		}
		w.Close()
		if manager.Used() != 0 {
			t.Fatal("Close leaked bytes")
		}
	}
}

func TestStringCaseBudgetGrowthRetry(t *testing.T) {
	for _, dictionary := range []bool{false, true} {
		t.Run(fmt.Sprint(dictionary), func(t *testing.T) {
			schema, small := caseBatch([]string{"A"}, nil, dictionary)
			_, large := caseBatch([]string{strings.Repeat("Ⱥ", 4096)}, nil, dictionary)
			p, err := Compile(schema, Lower(Col("s")))
			if err != nil {
				t.Fatal(err)
			}
			m := &scalarTestManager{base: memory.NewBudgetManager(0)}
			w, err := NewWorkspace(context.Background(), p, m)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			if _, err = w.Evaluate(context.Background(), small, testKernelSet()); err != nil {
				t.Fatal(err)
			}
			used := m.Used()
			m.limit = used + 1
			if _, err = w.Evaluate(context.Background(), large, testKernelSet()); !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatalf("expected growth rejection, got %v", err)
			}
			if m.Used() != used || w.closed {
				t.Fatalf("failed replacement lost ownership: used=%d want=%d closed=%t", m.Used(), used, w.closed)
			}
			m.limit = 0
			result, err := w.Evaluate(context.Background(), large, testKernelSet())
			if err != nil || string(result[0].String.ValueBytes(0)) != strings.Repeat("ⱥ", 4096) {
				t.Fatalf("retry result: %v", err)
			}
			if m.Used() != w.MemoryBytes() {
				t.Fatalf("unaccounted growth: %d vs %d", m.Used(), w.MemoryBytes())
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err = w.Evaluate(ctx, small, testKernelSet()); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel err=%v", err)
			}
			if m.Used() != 0 {
				t.Fatal("cancellation leaked retained memory")
			}
		})
	}
}

func TestStringCaseWarmBuilderAllocations(t *testing.T) {
	m := &scalarTestManager{base: memory.NewBudgetManager(0)}
	w := evaluationState{manager: m}
	defer w.Close()
	out := scalarRegister{acquire: w.acquire, checkContext: w.checkContext}
	text := []byte("MiXeD İıſȺȾⱥⱦKµ\xff")
	if err := appendScalarCase(&out, text, OpUpper); err != nil {
		t.Fatal(err)
	}
	if err := appendScalarCase(&out, text, OpLower); err != nil {
		t.Fatal(err)
	}
	calls := m.calls
	var err error
	allocs := testing.AllocsPerRun(100, func() {
		out.data = out.data[:0]
		err = appendScalarCase(&out, text, OpLower)
	})
	if err != nil || allocs != 0 || m.calls != calls {
		t.Fatalf("allocs=%v reservations=%d/%d err=%v", allocs, m.calls, calls, err)
	}
}
