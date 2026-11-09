package expr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"gocolumn/memory"
)

func caseDictionary(rows, entries int) *memory.DictionaryStringVector {
	v := &memory.DictionaryStringVector{Indices: make([]uint32, rows), Offsets: []uint32{0}, Encoding: memory.EncodingDictionary, DictionaryDomain: "mutable"}
	corpus := []string{"MiXeD", "Ⱥſ\xffZ", "İıKßΣσς", "", "\x00a\xc0\x80Z", strings.Repeat("Long Ⱥ ", 33)}
	for code := range entries {
		v.Data = append(v.Data, corpus[code%len(corpus)]...)
		v.Offsets = append(v.Offsets, uint32(len(v.Data)))
	}
	for row := range rows {
		v.Indices[row] = uint32(row % max(entries, 1))
	}
	return v
}

func TestDictionaryStringCaseBoundaries(t *testing.T) {
	for _, rows := range []int{0, 1, 63, 64, 65, 511, 1024} {
		for _, entries := range []int{0, 1, 8, 255, 256, 257} {
			t.Run(fmt.Sprintf("rows=%d/entries=%d", rows, entries), func(t *testing.T) {
				v := caseDictionary(rows, entries)
				if rows > 4 {
					v.Indices[0], v.Indices[2], v.Indices[4] = ^uint32(0), uint32(entries), ^uint32(0)
				}
				original := bytes.Clone(v.Data)
				for _, op := range []Op{OpLower, OpUpper} {
					for _, masked := range []bool{false, true} {
						var w evaluationState
						out := scalarRegister{typ: memory.TypeString, acquire: w.acquire, checkContext: w.checkContext}
						if err := prepareScalarBuffers(&out, rows); err != nil {
							t.Fatal(err)
						}
						fillAll(out.validity, rows)
						if masked {
							for row := range rows {
								if row%3 != 1 {
									out.validity[row/64] &^= uint64(1) << uint(row%64)
								}
							}
						}
						in := scalarRegister{typ: memory.TypeString, strDict: v}
						if err := evalStringCase(&out, &in, op, rows); err != nil {
							t.Fatal(err)
						}
						for row := range rows {
							want := ""
							if isValidRegister(&out, row) {
								want = strings.ToLower(string(v.DictionaryValue(v.Indices[row])))
								if op == OpUpper {
									want = strings.ToUpper(string(v.DictionaryValue(v.Indices[row])))
								}
							}
							got := string(out.data[out.offsets[row]:out.offsets[row+1]])
							if got != want {
								t.Fatalf("op=%s masked=%t row=%d got=%q want=%q", op, masked, row, got, want)
							}
						}
						w.Close()
					}
				}
				if !bytes.Equal(original, v.Data) {
					t.Fatal("input mutated")
				}
			})
		}
	}
}

func TestDictionaryStringCaseMutableGuardsAndRoots(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingDictionary, DictionaryDomain: "mutable"}, memory.Field{Name: "gate", Type: memory.TypeInt64})
	low := Lower(Col("s"))
	p, err := CompileExpressions(schema, []NamedExpr{Named(low, "low"), Named(low, "alias"), Named(Upper(low), "chain"), Named(If(GT(Col("gate"), I64(0)), Upper(Col("s")), Lower(Col("s"))), "guard"), Named(Col("s"), "original")})
	if err != nil {
		t.Fatal(err)
	}
	m := memory.NewBudgetManager(0)
	w, err := NewWorkspace(context.Background(), p, m)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	v := caseDictionary(1024, 8)
	for run, rows := range []int{1024, 65, 1, 0, 257, 2048} {
		// Change bytes in place without changing the domain or dictionary size.
		for i := range v.Data {
			if v.Data[i] == 'M' || v.Data[i] == 'N' {
				v.Data[i] = byte('M' + run%2)
			}
		}
		v.Indices = make([]uint32, rows)
		v.Validity = memory.EnsureBitmap(nil, rows)
		batch := memory.NewBatch()
		batch.Rows = rows
		batch.DictStrings["s"] = v
		gate := &memory.Int64Vector{Values: make([]int64, rows)}
		batch.Int64s["gate"] = gate
		for row := range rows {
			v.Indices[row] = uint32((row + run) % 8)
			if row%5 != 0 {
				memory.Set(v.Validity, row)
			} else {
				v.Indices[row] = ^uint32(0)
			}
			gate.Values[row] = int64(row % 3)
		}
		got, err := w.Evaluate(context.Background(), batch, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		for row := range rows {
			text := string(v.DictionaryValue(v.Indices[row]))
			lower := strings.ToLower(text)
			guarded := lower
			if gate.Values[row] > 0 {
				guarded = strings.ToUpper(text)
			}
			for col, want := range []string{lower, lower, strings.ToUpper(lower), guarded} {
				if got[col].DictionaryString != nil || got[col].String == nil {
					t.Fatal("changed output encoding")
				}
				if got[col].String.IsValid(row) != v.IsValid(row) || v.IsValid(row) && string(got[col].String.ValueBytes(row)) != want {
					t.Fatalf("run=%d row=%d col=%d", run, row, col)
				}
			}
			if got[4].DictionaryString == nil || got[4].DictionaryString.IsValid(row) != v.IsValid(row) || v.IsValid(row) && string(got[4].DictionaryString.DictionaryValue(got[4].DictionaryString.Indices[row])) != text {
				t.Fatal("borrowed root changed")
			}
		}
		if m.Used() != w.MemoryBytes() {
			t.Fatal("unaccounted reservation")
		}
	}
	w.Close()
	if m.Used() != 0 {
		t.Fatal("leaked reservation")
	}
}

func TestDictionaryStringCaseAllocationAndBudgetParity(t *testing.T) {
	const rows = 1024
	v := caseDictionary(rows, 8)
	for _, op := range []Op{OpLower, OpUpper} {
		var peaks [2]int64
		var calls [2]int
		for path := range 2 {
			m := &scalarTestManager{base: memory.NewBudgetManager(0)}
			var w evaluationState
			w.SetMemoryManager(context.Background(), m)
			out := scalarRegister{typ: memory.TypeString, acquire: w.acquire, checkContext: w.checkContext}
			if err := prepareScalarBuffers(&out, rows); err != nil {
				t.Fatal(err)
			}
			fillAll(out.validity, rows)
			in := scalarRegister{typ: memory.TypeString, strDict: v}
			if path == 0 {
				plain := &memory.StringViewVector{Offsets: []uint32{0}}
				for _, code := range v.Indices {
					plain.Data = append(plain.Data, v.DictionaryValue(code)...)
					plain.Offsets = append(plain.Offsets, uint32(len(plain.Data)))
				}
				in.strDict, in.strView = nil, plain
			}
			if err := evalStringCase(&out, &in, op, rows); err != nil {
				t.Fatal(err)
			}
			peaks[path], calls[path] = m.peak, m.calls
			if allocs := testing.AllocsPerRun(100, func() {
				if err := evalStringCase(&out, &in, op, rows); err != nil {
					panic(err)
				}
			}); allocs != 0 {
				t.Fatalf("path=%d allocs=%f", path, allocs)
			}
			if m.calls != calls[path] {
				t.Fatal("warm buffer reservation")
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("leak")
			}
		}
		if peaks[0] != peaks[1] || calls[0] != calls[1] {
			t.Fatalf("reservation difference peak=%v calls=%v", peaks, calls)
		}
	}
}

func TestDictionaryStringCaseCopyCancellation(t *testing.T) {
	v := caseDictionary(64, 1)
	calls := 0
	var w evaluationState
	out := scalarRegister{typ: memory.TypeString, acquire: w.acquire, checkContext: func() error {
		calls++
		if calls == 2 {
			return context.Canceled
		}
		return nil
	}}
	if err := prepareScalarBuffers(&out, 64); err != nil {
		t.Fatal(err)
	}
	fillAll(out.validity, 64)
	err := evalStringCase(&out, &scalarRegister{strDict: v}, OpLower, 64)
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "row 1") {
		t.Fatalf("copy cancellation: %v", err)
	}
	if string(out.data) != strings.ToLower(string(v.DictionaryValue(0))) {
		t.Fatal("copy modified output after cancellation")
	}
	w.Close()
}
