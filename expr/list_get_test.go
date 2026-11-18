package expr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

var listGetIndexTypes = []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64}

func listGetFixture(typ memory.DataType, text bool, n int) (memory.Schema, *memory.Batch) {
	f := memory.ListInt64Field("xs", true, true)
	if text {
		f = memory.ListStringField("xs", true, true)
	}
	b := memory.NewBatch()
	b.Rows = n
	offsets := []uint32{0}
	lv := make(memory.Bitmap, memory.WordsFor(n))
	iv := make(memory.Bitmap, memory.WordsFor(n))
	var ints []int64
	var strs []string
	var elementValid []bool
	for row := 0; row < n; row++ {
		if row%7 != 0 {
			memory.Set(lv, row)
		}
		if row%11 != 0 {
			memory.Set(iv, row)
		}
		if row%5 != 0 {
			for j := 0; j < 3; j++ {
				ints = append(ints, int64(row*10+j))
				s := fmt.Sprintf("值%d:%d", row, j)
				if j == 2 {
					s = ""
				}
				strs = append(strs, s)
				elementValid = append(elementValid, j != 1)
			}
		}
		offsets = append(offsets, uint32(len(ints)))
	}
	cv := make(memory.Bitmap, memory.WordsFor(len(ints)))
	for i, v := range elementValid {
		if v {
			memory.Set(cv, i)
		}
	}
	if text {
		b.StringLists["xs"] = &memory.StringListVector{Offsets: offsets, Validity: lv, Values: testStringVector(strs, elementValid)}
	} else {
		b.Int64Lists["xs"] = &memory.Int64ListVector{Offsets: offsets, Validity: lv, Values: &memory.Int64Vector{Values: ints, Validity: cv}}
	}
	// Fixed typed vectors preserve extreme index values exactly.
	signed := func(row int) int64 {
		v := []int64{0, 1, 2, 3, -1, math.MinInt64}[row%6]
		if row%6 == 5 {
			switch typ {
			case memory.TypeInt8:
				v = math.MinInt8
			case memory.TypeInt16:
				v = math.MinInt16
			case memory.TypeInt32:
				v = math.MinInt32
			}
		}
		return v
	}
	unsigned := func(row int) uint64 { v := []uint64{0, 1, 2, 3, math.MaxUint64, math.MaxUint64}[row%6]; return v }
	switch typ {
	case memory.TypeInt8:
		v := make([]int8, n)
		for i := range v {
			v[i] = int8(signed(i))
		}
		b.Int8s["i"] = &memory.Int8Vector{Values: v, Validity: iv}
	case memory.TypeInt16:
		v := make([]int16, n)
		for i := range v {
			v[i] = int16(signed(i))
		}
		b.Int16s["i"] = &memory.Int16Vector{Values: v, Validity: iv}
	case memory.TypeInt32:
		v := make([]int32, n)
		for i := range v {
			v[i] = int32(signed(i))
		}
		b.Int32s["i"] = &memory.Int32Vector{Values: v, Validity: iv}
	case memory.TypeInt64:
		v := make([]int64, n)
		for i := range v {
			v[i] = signed(i)
		}
		b.Int64s["i"] = &memory.Int64Vector{Values: v, Validity: iv}
	case memory.TypeUint8:
		v := make([]uint8, n)
		for i := range v {
			v[i] = uint8(unsigned(i))
		}
		b.Uint8s["i"] = &memory.Uint8Vector{Values: v, Validity: iv}
	case memory.TypeUint16:
		v := make([]uint16, n)
		for i := range v {
			v[i] = uint16(unsigned(i))
		}
		b.Uint16s["i"] = &memory.Uint16Vector{Values: v, Validity: iv}
	case memory.TypeUint32:
		v := make([]uint32, n)
		for i := range v {
			v[i] = uint32(unsigned(i))
		}
		b.Uint32s["i"] = &memory.Uint32Vector{Values: v, Validity: iv}
	case memory.TypeUint64:
		v := make([]uint64, n)
		for i := range v {
			v[i] = unsigned(i)
		}
		b.Uint64s["i"] = &memory.Uint64Vector{Values: v, Validity: iv}
	}
	return memory.NewSchema(f, memory.Field{Name: "i", Type: typ, Nullable: true}), b
}
func listGetCheck(t *testing.T, text bool, n int, v ScalarValue) {
	t.Helper()
	for row := 0; row < n; row++ {
		idx := row % 6
		valid := row%7 != 0 && row%11 != 0 && row%5 != 0 && (idx == 0 || idx == 2)
		if text {
			if v.String.IsValid(row) != valid {
				t.Fatalf("row%d validity", row)
			}
			want := ""
			if valid && idx == 0 {
				want = fmt.Sprintf("值%d:0", row)
			}
			if string(v.String.ValueBytes(row)) != want {
				t.Fatalf("row%d got%q want%q", row, v.String.ValueBytes(row), want)
			}
		} else {
			if v.Int64.IsValid(row) != valid {
				t.Fatalf("row%d validity", row)
			}
			want := int64(0)
			if valid {
				want = int64(row*10 + idx)
			}
			if v.Int64.Values[row] != want {
				t.Fatalf("row%d got%d want%d", row, v.Int64.Values[row], want)
			}
		}
	}
}
func TestListGetAllIntegerIndices(t *testing.T) {
	for _, text := range []bool{false, true} {
		for _, typ := range listGetIndexTypes {
			t.Run(fmt.Sprintf("string%v/%s", text, typ), func(t *testing.T) {
				schema, _ := listGetFixture(typ, text, 0)
				e := ListGet(Col("xs"), Col("i"))
				bound, err := Bind(schema, e)
				if err != nil {
					t.Fatal(err)
				}
				want := memory.TypeInt64
				if text {
					want = memory.TypeString
				}
				if bound.Field.Type != want || !bound.Field.Nullable || bound.MayError || len(bound.Field.Children) != 0 || !reflect.DeepEqual(Columns(e), []string{"i", "xs"}) {
					t.Fatal("metadata/columns contract", bound)
				}
				p, err := Compile(schema, e)
				if err != nil {
					t.Fatal(err)
				}
				if p.SupportsSelection() {
					t.Fatal("unexpected selected fast path")
				}
				for _, km := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
					m := memory.NewBudgetManager(0)
					w, err := NewWorkspace(context.Background(), p, m)
					if err != nil {
						t.Fatal(err)
					}
					for _, n := range []int{0, 1, 63, 64, 65, 130, 1, 0} {
						_, b := listGetFixture(typ, text, n)
						before, _ := json.Marshal(b)
						out, err := w.Evaluate(context.Background(), b, kernel.New(km))
						if err != nil {
							t.Fatal(err)
						}
						listGetCheck(t, text, n, out[0])
						after, _ := json.Marshal(b)
						if string(before) != string(after) {
							t.Fatal("input mutated")
						}
						// Poison child payload after successful evaluation: owned results survive.
						if text {
							for i := range b.StringLists["xs"].Values.Data {
								b.StringLists["xs"].Values.Data[i] = 'X'
							}
						} else {
							for i := range b.Int64Lists["xs"].Values.Values {
								b.Int64Lists["xs"].Values.Values[i] = -777
							}
						}
						listGetCheck(t, text, n, out[0])
						if m.Used() != w.MemoryBytes() {
							t.Fatal("accounting drift")
						}
					}
					w.Close()
					if m.Used() != 0 {
						t.Fatal("close leaked")
					}
				}
			})
		}
	}
}

func TestListGetBindingAndCSE(t *testing.T) {
	schema, _ := listGetFixture(memory.TypeInt64, false, 0)
	for _, idx := range []Expr{Bool(true), F64(1), Str("1"), CastTo(I64(1), memory.TypeTimestamp), nil} {
		if _, err := Compile(schema, ListGet(Col("xs"), idx)); err == nil {
			t.Fatalf("invalid index accepted %v", idx)
		}
	}
	for _, v := range []Expr{I64(1), Str("x"), nil} {
		if _, err := Compile(schema, ListGet(v, I64(0))); err == nil {
			t.Fatal("invalid list accepted")
		}
	}
	f := schema.Fields[0]
	f.Name = "other"
	schema = memory.NewSchema(schema.Fields[0], schema.Fields[1], f)
	a := ListGet(Col("xs"), I64(0))
	p, err := CompileExpressions(schema, []NamedExpr{Named(a, "a"), Named(a, "duplicate"), Named(ListGet(Col("xs"), I64(1)), "index"), Named(ListGet(Col("other"), I64(0)), "value")})
	if err != nil {
		t.Fatal(err)
	}
	if p.outputs[0].root != p.outputs[1].root || p.outputs[0].root == p.outputs[2].root || p.outputs[0].root == p.outputs[3].root {
		t.Fatal("CSE two ordered children")
	}
	schema, _ = listGetFixture(memory.TypeInt64, false, 0)
	p, err = Compile(schema, ListGet(Col("xs"), Col("i")))
	if err != nil {
		t.Fatal(err)
	}
	w, _ := NewWorkspace(context.Background(), p, nil)
	defer w.Close()
	_, b := listGetFixture(memory.TypeInt64, false, 65)
	if _, err = w.EvaluateSelected(context.Background(), b, []int{1, 2}, testKernelSet()); err == nil {
		t.Fatal("selected execution must explicitly reject")
	}
}

func listGetSimple(text, required bool) (memory.Schema, *memory.Batch) {
	f := memory.ListInt64Field("xs", true, !required)
	if text {
		f = memory.ListStringField("xs", true, !required)
	}
	b := memory.NewBatch()
	b.Rows = 1
	if text {
		b.StringLists["xs"] = &memory.StringListVector{Offsets: []uint32{0, 3}, Values: testStringVector([]string{"a", "hidden", ""}, []bool{true, false, true})}
	} else {
		b.Int64Lists["xs"] = &memory.Int64ListVector{Offsets: []uint32{0, 3}, Values: &memory.Int64Vector{Values: []int64{10, 777, 30}, Validity: memory.Bitmap{5}}}
	}
	return memory.NewSchema(f), b
}
func TestListGetLogicalViewAndStructure(t *testing.T) {
	for _, text := range []bool{false, true} {
		t.Run(fmt.Sprint(text), func(t *testing.T) {
			schema, b := listGetSimple(text, false)
			// Nonzero logical base; malformed hidden tail must not be scanned.
			if text {
				b.StringLists["xs"].Offsets = []uint32{2, 3, 999}
				b.StringLists["xs"].Values.Offsets = append(b.StringLists["xs"].Values.Offsets, 999)
			} else {
				b.Int64Lists["xs"].Offsets = []uint32{2, 3, 999}
			}
			p, err := Compile(schema, ListGet(Col("xs"), I64(0)))
			if err != nil {
				t.Fatal(err)
			}
			w, _ := NewWorkspace(context.Background(), p, nil)
			out, err := w.Evaluate(context.Background(), b, testKernelSet())
			if err != nil {
				t.Fatal("logical prefix", err)
			}
			if text {
				if !out[0].String.IsValid(0) || len(out[0].String.ValueBytes(0)) != 0 {
					t.Fatal("valid empty string")
				}
			} else if !out[0].Int64.IsValid(0) || out[0].Int64.Values[0] != 30 {
				t.Fatal("nonzero base")
			}
			w.Close()
			for _, name := range []string{"short_offsets", "descending", "out_of_bounds", "short_parent_bitmap", "short_child_bitmap", "nil_child", "wrong_family", "string_offsets"} {
				if name == "string_offsets" && !text {
					continue
				}
				t.Run(name, func(t *testing.T) {
					schema, b := listGetSimple(text, false)
					if text {
						v := b.StringLists["xs"]
						switch name {
						case "short_offsets":
							v.Offsets = []uint32{0}
						case "descending":
							v.Offsets = []uint32{2, 1}
						case "out_of_bounds":
							v.Offsets[1] = 4
						case "short_parent_bitmap":
							v.Validity = memory.Bitmap{}
						case "short_child_bitmap":
							v.Values.Validity = memory.Bitmap{}
						case "nil_child":
							v.Values = nil
						case "wrong_family":
							delete(b.StringLists, "xs")
							b.Int64Lists["xs"] = &memory.Int64ListVector{Offsets: []uint32{0, 0}, Values: &memory.Int64Vector{}}
						case "string_offsets":
							v.Values.Offsets[2] = 999
						}
					} else {
						v := b.Int64Lists["xs"]
						switch name {
						case "short_offsets":
							v.Offsets = []uint32{0}
						case "descending":
							v.Offsets = []uint32{2, 1}
						case "out_of_bounds":
							v.Offsets[1] = 4
						case "short_parent_bitmap":
							v.Validity = memory.Bitmap{}
						case "short_child_bitmap":
							v.Values.Validity = memory.Bitmap{}
						case "nil_child":
							v.Values = nil
						case "wrong_family":
							delete(b.Int64Lists, "xs")
							b.StringLists["xs"] = &memory.StringListVector{Offsets: []uint32{0, 0}, Values: testStringVector(nil, nil)}
						}
					}
					p, err := Compile(schema, ListGet(Col("xs"), I64(-1)))
					if err != nil {
						t.Fatal(err)
					}
					w, _ := NewWorkspace(context.Background(), p, nil)
					defer w.Close()
					if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err == nil {
						t.Fatal("malformed structure hidden by negative index")
					}
				})
			}
		})
	}
}
func TestListGetRequiredElementsAndGuards(t *testing.T) {
	for _, text := range []bool{false, true} {
		for _, index := range []Expr{I64(0), I64(-1), CastTo(Str("18446744073709551615"), memory.TypeUint64), Null(memory.TypeInt64)} {
			t.Run(fmt.Sprintf("string%v/%s", text, index), func(t *testing.T) {
				schema, b := listGetSimple(text, true)
				p, err := Compile(schema, ListGet(Col("xs"), index))
				if err != nil {
					t.Fatal(err)
				}
				w, _ := NewWorkspace(context.Background(), p, nil)
				if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err == nil || (!strings.Contains(err.Error(), "NULL") && !strings.Contains(err.Error(), "null")) {
					t.Fatal("required unselected element must reject", err)
				}
				w.Close()
				if text {
					b.StringLists["xs"].Validity = memory.Bitmap{0}
				} else {
					b.Int64Lists["xs"].Validity = memory.Bitmap{0}
				}
				w, _ = NewWorkspace(context.Background(), p, nil)
				out, err := w.Evaluate(context.Background(), b, testKernelSet())
				if err != nil {
					t.Fatal("NULL parent must hide required values", err)
				}
				if text {
					if out[0].String.IsValid(0) {
						t.Fatal("NULL parent")
					}
				} else if out[0].Int64.IsValid(0) {
					t.Fatal("NULL parent")
				}
				w.Close()
			})
		}
		// Two-row dynamic domain: row0 malformed required child, row1 good.
		schema, b := listGetSimple(text, true)
		f := schema.Fields[0]
		schema = memory.NewSchema(f, memory.Field{Name: "keep", Type: memory.TypeBool})
		b.Rows = 2
		b.Uint32s["keep"] = &memory.Uint32Vector{Values: []uint32{0, 1}}
		if text {
			b.StringLists["xs"].Offsets = []uint32{0, 2, 3}
		} else {
			b.Int64Lists["xs"].Offsets = []uint32{0, 2, 3}
		}
		get := ListGet(Col("xs"), I64(0))
		var fallback Expr = I64(7)
		if text {
			fallback = Str("fallback")
		}
		for name, e := range map[string]Expr{"if": If(Col("keep"), get, fallback), "case": Case([]WhenThen{{When: Col("keep"), Then: get}}, fallback)} {
			t.Run(fmt.Sprintf("string%v/%s", text, name), func(t *testing.T) {
				p, err := Compile(schema, e)
				if err != nil {
					t.Fatal(err)
				}
				w, _ := NewWorkspace(context.Background(), p, nil)
				defer w.Close()
				out, err := w.Evaluate(context.Background(), b, testKernelSet())
				if err != nil {
					t.Fatal(err)
				}
				if text {
					if string(out[0].String.ValueBytes(0)) != "fallback" || !out[0].String.IsValid(1) || len(out[0].String.ValueBytes(1)) != 0 {
						t.Fatal("guard output")
					}
				} else if !reflect.DeepEqual(out[0].Int64.Values, []int64{7, 30}) {
					t.Fatal("guard output")
				}
			})
		}
	}
}

func TestListGetBudgetRetryAndCancel(t *testing.T) {
	for _, text := range []bool{false, true} {
		t.Run(fmt.Sprint(text), func(t *testing.T) {
			schema, b := listGetFixture(memory.TypeInt64, text, 130)
			p, err := Compile(schema, ListGet(Col("xs"), Col("i")))
			if err != nil {
				t.Fatal(err)
			}
			probe := &selectedFaultManager{Manager: memory.NewBudgetManager(0)}
			w, _ := NewWorkspace(context.Background(), p, probe)
			if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
				t.Fatal(err)
			}
			calls := probe.calls
			w.Close()
			for fail := 1; fail <= calls; fail++ {
				m := &selectedFaultManager{Manager: memory.NewBudgetManager(0), fail: fail}
				w, _ := NewWorkspace(context.Background(), p, m)
				if _, err = w.Evaluate(context.Background(), b, testKernelSet()); !errors.Is(err, memory.ErrBudgetExceeded) {
					t.Fatalf("reserve%d err%v", fail, err)
				}
				m.fail = 0
				out, err := w.Evaluate(context.Background(), b, testKernelSet())
				if err != nil {
					t.Fatal(err)
				}
				listGetCheck(t, text, 130, out[0])
				w.Close()
				if m.Used() != 0 {
					t.Fatal("budget leak")
				}
			}
			m := memory.NewBudgetManager(0)
			w, _ = NewWorkspace(context.Background(), p, m)
			if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err = w.Evaluate(ctx, b, testKernelSet()); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("cancel leak")
			}
		})
	}
}

func TestListGetCoalesceINAndChildErrors(t *testing.T) {
	for _, text := range []bool{false, true} {
		t.Run(fmt.Sprint(text), func(t *testing.T) {
			schema, b := listGetSimple(text, true)
			b.Rows = 2
			f := schema.Fields[0]
			typ := memory.TypeInt64
			if text {
				typ = memory.TypeString
				b.StringLists["xs"].Offsets = []uint32{0, 2, 3}
				b.Strings["first"] = testStringVector([]string{"fallback", "ignored"}, []bool{true, false})
				b.Strings["match"] = testStringVector([]string{"", "not"}, nil)
			} else {
				b.Int64Lists["xs"].Offsets = []uint32{0, 2, 3}
				b.Int64s["first"] = &memory.Int64Vector{Values: []int64{7, 99}, Validity: memory.Bitmap{1}}
				b.Int64s["match"] = &memory.Int64Vector{Values: []int64{30, 99}}
			}
			schema = memory.NewSchema(f, memory.Field{Name: "first", Type: typ, Nullable: true}, memory.Field{Name: "match", Type: typ})
			get := ListGet(Col("xs"), I64(0))
			var target Expr = I64(30)
			if text {
				target = Str("")
			}
			for name, e := range map[string]Expr{"coalesce": Coalesce(Col("first"), get), "in": In(target, Col("match"), get)} {
				p, err := Compile(schema, e)
				if err != nil {
					t.Fatal(err)
				}
				w, _ := NewWorkspace(context.Background(), p, nil)
				out, err := w.Evaluate(context.Background(), b, testKernelSet())
				if err != nil {
					t.Fatal(name, err)
				}
				if name == "in" {
					if !reflect.DeepEqual(out[0].Uint32.Values, []uint32{1, 1}) {
						t.Fatal("IN oracle")
					}
				} else if text {
					if string(out[0].String.ValueBytes(0)) != "fallback" || !out[0].String.IsValid(1) || len(out[0].String.ValueBytes(1)) != 0 {
						t.Fatal("Coalesce oracle")
					}
				} else if !reflect.DeepEqual(out[0].Int64.Values, []int64{7, 30}) {
					t.Fatal("Coalesce oracle")
				}
				w.Close()
			}
			// Structurally malformed inactive row is still rejected if another row executes.
			if text {
				b.StringLists["xs"].Offsets[0] = 3
			} else {
				b.Int64Lists["xs"].Offsets[0] = 3
			}
			p, _ := Compile(schema, Coalesce(Col("first"), get))
			w, _ := NewWorkspace(context.Background(), p, nil)
			if _, err := w.Evaluate(context.Background(), b, testKernelSet()); err == nil {
				t.Fatal("inactive offset corruption hidden")
			}
			w.Close()
			// NULL list is not a short-circuit guard for the Index child expression.
			schema, b = listGetSimple(text, false)
			if text {
				b.StringLists["xs"].Validity = memory.Bitmap{0}
			} else {
				b.Int64Lists["xs"].Validity = memory.Bitmap{0}
			}
			bad := ListGet(Col("xs"), CastTo(Str("bad"), memory.TypeInt64))
			bound, err := Bind(schema, bad)
			if err != nil {
				t.Fatal(err)
			}
			if !bound.MayError {
				t.Fatal("child MayError lost")
			}
			p, err = Compile(schema, bad)
			if err != nil {
				t.Fatal(err)
			}
			w, _ = NewWorkspace(context.Background(), p, nil)
			if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err == nil || !strings.Contains(err.Error(), "cast") {
				t.Fatal("NULL list swallowed fallible index", err)
			}
			w.Close()
		})
	}
}

func TestListGetSameAddressReuse(t *testing.T) {
	for _, text := range []bool{false, true} {
		schema, b := listGetSimple(text, false)
		schema = memory.NewSchema(schema.Fields[0], memory.Field{Name: "i", Type: memory.TypeInt64})
		b.Int64s["i"] = &memory.Int64Vector{Values: []int64{0}}
		p, err := Compile(schema, ListGet(Col("xs"), Col("i")))
		if err != nil {
			t.Fatal(err)
		}
		w, _ := NewWorkspace(context.Background(), p, nil)
		for _, idx := range []int64{0, 1, 2, -1, math.MaxInt64, 0} {
			b.Int64s["i"].Values[0] = idx
			out, err := w.Evaluate(context.Background(), b, testKernelSet())
			if err != nil {
				t.Fatal(err)
			}
			valid := idx == 0 || idx == 2
			if text {
				if out[0].String.IsValid(0) != valid {
					t.Fatal("stale index")
				}
			} else if out[0].Int64.IsValid(0) != valid {
				t.Fatal("stale index")
			}
		}
		if text {
			b.StringLists["xs"].Values.Data[0] = 'z'
		} else {
			b.Int64Lists["xs"].Values.Values[0] = 999
		}
		out, err := w.Evaluate(context.Background(), b, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		if text {
			if string(out[0].String.ValueBytes(0)) != "z" {
				t.Fatal("stale list")
			}
		} else if out[0].Int64.Values[0] != 999 {
			t.Fatal("stale list")
		}
		w.Close()
	}
}

type listGetCancelContext struct {
	context.Context
	cancel       context.CancelFunc
	checks, stop int
}

func (c *listGetCancelContext) Err() error {
	var pcs [32]uintptr
	n := runtime.Callers(2, pcs[:])
	fs := runtime.CallersFrames(pcs[:n])
	eval, acquire := false, false
	for {
		f, more := fs.Next()
		if strings.HasSuffix(f.Function, ".evalListGet") {
			eval = true
		}
		if strings.HasSuffix(f.Function, ".acquire") {
			acquire = true
		}
		if !more {
			break
		}
	}
	if eval && !acquire {
		c.checks++
		if c.checks == c.stop {
			c.cancel()
		}
	}
	return c.Context.Err()
}
func TestListGetRunningAndLongCopyCancellation(t *testing.T) {
	for _, text := range []bool{false, true} {
		t.Run(fmt.Sprint(text), func(t *testing.T) {
			rows, stop := 193, 2*193+1+65 // validation rows+1, required rows, then output row64.
			field := memory.ListInt64Field("xs", false, true)
			in := scalarRegister{typ: memory.TypeList}
			index := scalarRegister{typ: memory.TypeInt64, i64: make([]int64, rows)}
			out := scalarRegister{typ: memory.TypeInt64, i64: make([]int64, rows), ownedValidity: make(memory.Bitmap, memory.WordsFor(rows))}
			if text {
				rows, stop = 1, 9
				field = memory.ListStringField("xs", false, true)
				payload := []byte(strings.Repeat("a", 128<<10))
				in.strList = &memory.StringListVector{Offsets: []uint32{0, 1}, Values: &memory.StringViewVector{Offsets: []uint32{0, uint32(len(payload))}, Data: payload}}
				out.typ = memory.TypeString
				out.data = []byte(strings.Repeat("Z", len(payload)))
				out.offsets = make([]uint32, 2)
			} else {
				offs := make([]uint32, rows+1)
				xs := make([]int64, rows)
				for i := range xs {
					offs[i] = uint32(i)
					xs[i] = int64(i + 17)
				}
				offs[rows] = uint32(rows)
				in.intList = &memory.Int64ListVector{Offsets: offs, Values: &memory.Int64Vector{Values: xs}}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			checks := 0
			out.checkContext = func() error {
				checks++
				if checks == stop {
					cancel()
				}
				return ctx.Err()
			}
			err := evalListGet(&out, &in, &index, &field, rows, nil)
			if !errors.Is(err, context.Canceled) || checks != stop {
				t.Fatalf("checkpoint %d/%d %v", checks, stop, err)
			}
			if text {
				if string(out.data[:32<<10]) != strings.Repeat("a", 32<<10) || string(out.data[32<<10:]) != strings.Repeat("Z", 96<<10) {
					t.Fatal("not canceled during second copy chunk")
				}
			} else {
				for i, v := range out.i64 {
					want := int64(0)
					if i < 64 {
						want = int64(i + 17)
					}
					if v != want {
						t.Fatal("processed/unprocessed row", i, v)
					}
				}
			}
			b := memory.NewBatch()
			b.Rows = rows
			if text {
				b.StringLists["xs"] = in.strList
			} else {
				b.Int64Lists["xs"] = in.intList
			}
			p, err := Compile(memory.NewSchema(field), ListGet(Col("xs"), I64(0)))
			if err != nil {
				t.Fatal(err)
			}
			m := memory.NewBudgetManager(0)
			w, _ := NewWorkspace(context.Background(), p, m)
			base, cancel2 := context.WithCancel(context.Background())
			defer cancel2()
			running := &listGetCancelContext{Context: base, cancel: cancel2, stop: stop}
			if _, err = w.Evaluate(running, b, testKernelSet()); !errors.Is(err, context.Canceled) || running.checks != stop {
				t.Fatal("workspace running cancellation", err, running.checks)
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("running cancellation budget leak")
			}
		})
	}
}

func TestListGetIndexStorageValidation(t *testing.T) {
	for _, typ := range listGetIndexTypes {
		for _, short := range []string{"Values", "Validity"} {
			for _, guard := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/guard%v", typ, short, guard), func(t *testing.T) {
					schema, b := listGetFixture(typ, false, 65)
					maps := map[memory.DataType]string{memory.TypeInt8: "Int8s", memory.TypeInt16: "Int16s", memory.TypeInt32: "Int32s", memory.TypeInt64: "Int64s", memory.TypeUint8: "Uint8s", memory.TypeUint16: "Uint16s", memory.TypeUint32: "Uint32s", memory.TypeUint64: "Uint64s"}
					vector := reflect.ValueOf(b).Elem().FieldByName(maps[typ]).MapIndex(reflect.ValueOf("i")).Elem()
					member := vector.FieldByName(short)
					length := 64
					if short == "Validity" {
						length = 1
					}
					member.Set(member.Slice(0, length))
					var e Expr = ListGet(Col("xs"), Col("i"))
					if guard {
						schema = memory.NewSchema(schema.Fields[0], schema.Fields[1], memory.Field{Name: "keep", Type: memory.TypeBool})
						flags := make([]uint32, 65)
						flags[0] = 1
						b.Uint32s["keep"] = &memory.Uint32Vector{Values: flags}
						e = If(Col("keep"), e, I64(0))
					}
					p, err := Compile(schema, e)
					if err != nil {
						t.Fatal(err)
					}
					m := memory.NewBudgetManager(0)
					w, _ := NewWorkspace(context.Background(), p, m)
					if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err == nil {
						t.Fatal("short index storage accepted including inactive tail")
					}
					w.Close()
					if m.Used() != 0 {
						t.Fatal("malformed storage leak")
					}
				})
			}
		}
	}
}

func TestListGetMetadataAndSelectedPayloadBudget(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeBool, memory.TypeFloat32, memory.TypeFloat64, memory.TypeString, memory.TypeDate32, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime32, memory.TypeTime64, memory.TypeDuration, memory.TypeDecimal, memory.TypeArrow} {
		schema := memory.NewSchema(memory.ListInt64Field("xs", true, true), memory.Field{Name: "bad", Type: typ, Nullable: true})
		e := ListGet(Col("xs"), Col("bad"))
		if _, err := Compile(schema, If(Bool(false), e, I64(0))); err == nil {
			t.Fatalf("dead invalid index type %s accepted", typ)
		}
	}
	for _, text := range []bool{false, true} {
		schema, _ := listGetSimple(text, false)
		f := schema.Fields[0]
		child := f.Children[0]
		as, err := memory.ArrowSchema(memory.NewSchema(child))
		if err != nil {
			t.Fatal(err)
		}
		child.ArrowType = as.Field(0).Type.String()
		f.Children[0] = child
		p, err := Compile(memory.NewSchema(f), ListGet(Col("xs"), I64(0)))
		if err != nil {
			t.Fatal(err)
		}
		out := p.ResultField()
		if !out.Nullable || out.Type != child.Type || out.ArrowType != child.ArrowType || len(out.Children) != 0 || out.DictionaryDomain != "" {
			t.Fatal("element metadata lost", out)
		}
		f.Children[0].Children = []memory.Field{{Name: "nested", Type: memory.TypeInt64}}
		if _, err = Compile(memory.NewSchema(f), ListGet(Col("xs"), I64(0))); err == nil {
			t.Fatal("recursive metadata accepted")
		}
	}
	// Output owns only the selected byte, not the entire 1MiB source child.
	field := memory.ListStringField("xs", false, false)
	b := memory.NewBatch()
	b.Rows = 1
	b.StringLists["xs"] = &memory.StringListVector{Offsets: []uint32{0, 2}, Values: testStringVector([]string{"x", strings.Repeat("y", 1<<20)}, nil)}
	p, err := Compile(memory.NewSchema(field), ListGet(Col("xs"), I64(0)))
	if err != nil {
		t.Fatal(err)
	}
	m := memory.NewBudgetManager(64 << 10)
	w, _ := NewWorkspace(context.Background(), p, m)
	out, err := w.Evaluate(context.Background(), b, testKernelSet())
	if err != nil {
		t.Fatal("unselected payload charged/copied", err)
	}
	if string(out[0].String.ValueBytes(0)) != "x" {
		t.Fatal("selected payload")
	}
	w.Close()
	if m.Used() != 0 {
		t.Fatal("payload budget leak")
	}
}

func TestListGetSignedPositiveBounds(t *testing.T) {
	for _, tc := range []struct {
		typ     memory.DataType
		mapName string
		max     int64
	}{{memory.TypeInt8, "Int8s", math.MaxInt8}, {memory.TypeInt16, "Int16s", math.MaxInt16}, {memory.TypeInt32, "Int32s", math.MaxInt32}, {memory.TypeInt64, "Int64s", math.MaxInt64}} {
		for _, text := range []bool{false, true} {
			schema, b := listGetFixture(tc.typ, text, 3)
			vector := reflect.ValueOf(b).Elem().FieldByName(tc.mapName).MapIndex(reflect.ValueOf("i")).Elem()
			vector.FieldByName("Values").Index(2).SetInt(tc.max)
			p, err := Compile(schema, ListGet(Col("xs"), Col("i")))
			if err != nil {
				t.Fatal(err)
			}
			w, _ := NewWorkspace(context.Background(), p, nil)
			out, err := w.Evaluate(context.Background(), b, testKernelSet())
			if err != nil {
				t.Fatal(err)
			}
			if text {
				if out[0].String.IsValid(2) {
					t.Fatal("signed MAX not OOB")
				}
			} else if out[0].Int64.IsValid(2) {
				t.Fatal("signed MAX not OOB")
			}
			w.Close()
		}
	}
}
