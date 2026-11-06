package expr

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"gocolumn/kernel"
	"gocolumn/memory"
)

var predicateRowsTypes = []memory.DataType{
	memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64,
	memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64,
	memory.TypeFloat32, memory.TypeFloat64, memory.TypeBool,
	memory.TypeDate32, memory.TypeDate64, memory.TypeTimestamp,
	memory.TypeTime32, memory.TypeTime64, memory.TypeDuration,
}

// These tests call the public evaluator directly. Source adapters normally
// crop their vectors, which would hide a mismatch between Rows and backing length.
func TestPredicateLogicalRowsNumeric(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, typ := range predicateRowsTypes {
			t.Run(fmt.Sprintf("%s/%s", mode, typ), func(t *testing.T) {
				ks := kernel.New(mode)
				schema := memory.NewSchema(memory.Field{Name: "x", Type: typ, Nullable: true}, memory.Field{Name: "y", Type: typ, Nullable: true})
				for _, extra := range []int{1, 65} {
					for _, nulls := range []string{"none", "mixed", "all"} {
						for _, columns := range []bool{false, true} {
							if columns && !predicateRowsColumnComparison(typ) {
								continue
							}
							for _, op := range []Op{OpGT, OpGE, OpLT, OpLE, OpEQ} {
								right := Expr(I64(2))
								if columns {
									right = Col("y")
								}
								cmp := Binary{Op: op, Left: Col("x"), Right: right}
								for _, compound := range []bool{false, true} {
									pred := Expr(cmp)
									if compound {
										pred = Or(And(cmp, Not(EQ(Col("x"), I64(3)))), IsNull(Col("y")))
									}
									p, _, err := CompilePredicate(schema, pred)
									if err != nil {
										t.Fatal(err)
									}
									var w EvalWorkspace
									// Shrink and regrow a warmed workspace as a source reuses buffers.
									for _, rows := range []int{65, 64, 63, 1, 0, 1, 65} {
										left, rightValues := make([]int64, rows+extra), make([]int64, rows+extra+3)
										for i := range left {
											left[i] = int64(i % 5)
											if typ == memory.TypeBool {
												left[i] = int64(i % 2)
											}
										}
										for i := range rightValues {
											rightValues[i] = int64((i + 2) % 5)
											if typ == memory.TypeBool {
												rightValues[i] = int64((i + 1) % 2)
											}
										}
										lv := predicateRowsValidity(rows, nulls, 1)
										rv := predicateRowsValidity(rows, nulls, 2)
										b := memory.NewBatch()
										b.Rows = rows
										predicateRowsBind(b, "x", typ, left, lv)
										predicateRowsBind(b, "y", typ, rightValues, rv)
										// An unrelated malformed vector must not be inspected.
										b.Int8s["unreferenced"] = nil
										leftValidity, rightValidity := slices.Clone(lv), slices.Clone(rv)
										out, err := p.Evaluate(b, &w, ks)
										if err != nil {
											t.Fatalf("rows=%d extra=%d nulls=%s columns=%v: %v", rows, extra, nulls, columns, err)
										}
										predicateRowsAssert(t, out, rows, func(row int) (bool, bool) {
											leftKnown, rightKnown := predicateRowsKnown(lv, row), predicateRowsKnown(rv, row)
											value := int64(2)
											known := leftKnown
											if columns {
												value, known = rightValues[row], known && rightKnown
											}
											truth := known && predicateRowsCompare(left[row], value, op)
											if compound {
												// SQL AND with NOT(x=3), followed by OR with y IS NULL.
												other := leftKnown && left[row] != 3
												falseKnown := (known && !truth) || (leftKnown && !other)
												truth = truth && other
												known = truth || falseKnown
												truth = truth || !rightKnown
												known = truth || known
											}
											return truth, known
										})
										if !slices.Equal(lv, leftValidity) || !slices.Equal(rv, rightValidity) {
											t.Fatal("evaluation changed input validity")
										}
										if got := predicateRowsColumnLen(b, "x"); got != len(left) {
											t.Fatalf("left header changed: %d", got)
										}
										if got := predicateRowsColumnLen(b, "y"); got != len(rightValues) {
											t.Fatalf("right header changed: %d", got)
										}
									}
								}
							}
						}
					}
				}
			})
		}
	}
}

func TestPredicateLogicalRowsFullWidthLiterals(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, typ := range predicateRowsTypes {
			if typ == memory.TypeFloat32 || typ == memory.TypeFloat64 {
				continue
			}
			for _, rows := range []int{0, 1, 63, 64, 65} {
				validity := predicateRowsValidity(rows, "mixed", 1)
				b := memory.NewBatch()
				b.Rows = rows
				values := make([]int64, rows+65)
				for row := range values {
					values[row] = int64(row % 3)
					if typ == memory.TypeBool {
						values[row] = int64(row % 2)
					}
				}
				predicateRowsBind(b, "x", typ, values, validity)
				unsigned := typ == memory.TypeUint8 || typ == memory.TypeUint16 || typ == memory.TypeUint32 || typ == memory.TypeUint64 || typ == memory.TypeBool
				for _, op := range []Op{OpGT, OpGE, OpLT, OpLE, OpEQ} {
					for _, literal := range []int64{math.MinInt64, -(1 << 32), -1, 1 << 32, math.MaxInt64} {
						in := Instruction{Code: codeCmpScalar, Column: "x", Type: typ, Op: op, LiteralI64: literal, LiteralU64: uint64(literal)}
						p := Program{Instructions: []Instruction{in}, Root: 0}
						out, err := p.Evaluate(b, &EvalWorkspace{}, kernel.New(mode))
						if err != nil {
							t.Fatal(err)
						}
						predicateRowsAssert(t, out, rows, func(row int) (bool, bool) {
							known := predicateRowsKnown(validity, row)
							truth := predicateRowsCompare(values[row], literal, op)
							if unsigned {
								truth = predicateRowsCompare(uint64(values[row]), uint64(literal), op)
							}
							return known && truth, known
						})
					}
				}
			}
		}
	}
}

// Unsigned Instructions can retain the entire uint64 domain even though the
// public expression constructor intentionally accepts only non-negative I64.
func TestPredicateLogicalRowsInt64AndUint64Extremes(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, rows := range []int{0, 1, 63, 64, 65} {
			validity := predicateRowsValidity(rows, "mixed", 1)
			for _, unsigned := range []bool{false, true} {
				b := memory.NewBatch()
				b.Rows = rows
				left, right := make([]uint64, rows+65), make([]uint64, rows+67)
				pattern := []uint64{0, 1, math.MaxInt64, 1 << 63, math.MaxUint64}
				for i := range left {
					left[i] = pattern[i%len(pattern)]
				}
				for i := range right {
					right[i] = pattern[(i+1)%len(pattern)]
				}
				typ := memory.TypeUint64
				if unsigned {
					b.Uint64s["x"] = &memory.Uint64Vector{Values: left, Validity: validity}
					b.Uint64s["y"] = &memory.Uint64Vector{Values: right}
				} else {
					typ = memory.TypeInt64
					x, y := make([]int64, len(left)), make([]int64, len(right))
					for i, value := range left {
						x[i] = int64(value)
					}
					for i, value := range right {
						y[i] = int64(value)
					}
					b.Int64s["x"] = &memory.Int64Vector{Values: x, Validity: validity}
					b.Int64s["y"] = &memory.Int64Vector{Values: y}
				}
				for _, op := range []Op{OpGT, OpGE, OpLT, OpLE, OpEQ} {
					for _, columns := range []bool{false, true} {
						for _, literal := range pattern {
							in := Instruction{Code: codeCmpScalar, Column: "x", Type: typ, Op: op, LiteralI64: int64(literal), LiteralU64: literal}
							if columns {
								in.RightColumn = "y"
							}
							p := Program{Instructions: []Instruction{in}, Root: 0}
							out, err := p.Evaluate(b, &EvalWorkspace{}, kernel.New(mode))
							if err != nil {
								t.Fatal(err)
							}
							predicateRowsAssert(t, out, rows, func(row int) (bool, bool) {
								value := literal
								if columns {
									value = right[row]
								}
								known := predicateRowsKnown(validity, row)
								truth := predicateRowsCompare(left[row], value, op)
								if !unsigned {
									truth = predicateRowsCompare(int64(left[row]), int64(value), op)
								}
								return known && truth, known
							})
						}
					}
				}
			}
		}
	}
}

func TestPredicateLogicalRowsFloat32Precision(t *testing.T) {
	values := []float32{0, .1, -.1, 1, -1, math.SmallestNonzeroFloat32, math.MaxFloat32, float32(math.Inf(1)), float32(math.Inf(-1)), float32(math.NaN())}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, rows := range []int{0, 1, 63, 64, 65} {
			for _, nulls := range []string{"none", "mixed", "all"} {
				v := &memory.Float32Vector{Values: make([]float32, rows+65), Validity: predicateRowsValidity(rows, nulls, 1)}
				for row := range v.Values {
					v.Values[row] = values[row%len(values)]
				}
				b := memory.NewBatch()
				b.Rows = rows
				b.Float32s["x"] = v
				for _, literal := range []float64{.1, -.1, math.SmallestNonzeroFloat64, math.MaxFloat64, math.NaN(), math.Inf(1)} {
					for _, op := range []Op{OpGT, OpGE, OpLT, OpLE, OpEQ} {
						p, _, err := CompilePredicate(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeFloat32}), Binary{Op: op, Left: Col("x"), Right: F64(literal)})
						if err != nil {
							t.Fatal(err)
						}
						out, err := p.Evaluate(b, &EvalWorkspace{}, kernel.New(mode))
						if err != nil {
							t.Fatal(err)
						}
						predicateRowsAssert(t, out, rows, func(row int) (bool, bool) {
							known := predicateRowsKnown(v.Validity, row)
							return known && predicateRowsCompare(float64(v.Values[row]), literal, op), known
						})
					}
				}
			}
		}
	}
}

func TestPredicateLogicalRowsStrings(t *testing.T) {
	values := []string{"", "a", "éa", "alpha", "z"}
	tests := []struct {
		expr  Expr
		match func(string) bool
	}{
		{EQ(Col("s"), Str("a")), func(s string) bool { return s == "a" }},
		{GT(Col("s"), Str("a")), func(s string) bool { return s > "a" }},
		{GE(Col("s"), Str("a")), func(s string) bool { return s >= "a" }},
		{LT(Col("s"), Str("a")), func(s string) bool { return s < "a" }},
		{LE(Col("s"), Str("a")), func(s string) bool { return s <= "a" }},
		{Contains(Col("s"), Str("a")), func(s string) bool { return strings.Contains(s, "a") }},
		{StartsWith(Col("s"), Str("a")), func(s string) bool { return strings.HasPrefix(s, "a") }},
		{EndsWith(Col("s"), Str("a")), func(s string) bool { return strings.HasSuffix(s, "a") }},
		{RegexMatch(Col("s"), Str("^a")), func(s string) bool { return strings.HasPrefix(s, "a") }},
		{GT(Length(Col("s")), I64(1)), func(s string) bool { return utf8.RuneCountInString(s) > 1 }},
		{GT(ByteLength(Col("s")), I64(2)), func(s string) bool { return len(s) > 2 }},
		{EQ(Find(Col("s"), Str("a")), I64(1)), func(s string) bool { return s == "éa" }},
	}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, dictionary := range []bool{false, true} {
			for _, nulls := range []string{"none", "mixed", "all"} {
				for _, tc := range tests {
					p, _, err := CompilePredicate(memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}), tc.expr)
					if err != nil {
						t.Fatal(err)
					}
					var w EvalWorkspace
					for _, rows := range []int{65, 64, 63, 1, 0, 65} {
						b := memory.NewBatch()
						b.Rows = rows
						validity := predicateRowsValidity(rows, nulls, 1)
						plain := &memory.StringViewVector{Offsets: []uint32{0}, Validity: validity}
						dict := &memory.DictionaryStringVector{Indices: make([]uint32, rows+65), Offsets: []uint32{0}, Validity: validity, DictionaryDomain: "logical-row-test"}
						for _, value := range values {
							dict.Data = append(dict.Data, value...)
							dict.Offsets = append(dict.Offsets, uint32(len(dict.Data)))
						}
						for row := range dict.Indices {
							dict.Indices[row] = uint32(row % len(values))
							plain.Data = append(plain.Data, values[row%len(values)]...)
							plain.Offsets = append(plain.Offsets, uint32(len(plain.Data)))
						}
						if dictionary {
							b.DictStrings["s"] = dict
						} else {
							b.Strings["s"] = plain
						}
						out, err := p.Evaluate(b, &w, kernel.New(mode))
						if err != nil {
							t.Fatal(err)
						}
						predicateRowsAssert(t, out, rows, func(row int) (bool, bool) {
							known := predicateRowsKnown(validity, row)
							return known && tc.match(values[row%len(values)]), known
						})
						if len(dict.Indices) != rows+65 || len(plain.Offsets) != rows+66 || len(dict.Offsets) != len(values)+1 {
							t.Fatal("input string headers changed")
						}
					}
				}
			}
		}
	}
	// Both supported physical representations of an empty plain vector remain valid.
	for _, offsets := range [][]uint32{nil, {0}} {
		b := memory.NewBatch()
		b.Strings["s"] = &memory.StringViewVector{Offsets: offsets}
		for _, tc := range tests {
			p, _, err := CompilePredicate(memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}), tc.expr)
			if err != nil {
				t.Fatal(err)
			}
			out, err := p.Evaluate(b, &EvalWorkspace{}, testKernelSet())
			if err != nil {
				t.Fatal(err)
			}
			predicateRowsAssert(t, out, 0, nil)
		}
	}
}

func TestPredicateLogicalRowsCalendar(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, typ := range []memory.DataType{memory.TypeDate32, memory.TypeDate64, memory.TypeTimestamp} {
			for _, rows := range []int{0, 1, 63, 64, 65} {
				b := memory.NewBatch()
				b.Rows = rows
				validity := predicateRowsValidity(rows, "mixed", 1)
				values := make([]int64, rows+65)
				for row := range values {
					stamp := time.Date(1969+row%3, time.January, 1, 0, 0, 0, 0, time.UTC)
					switch typ {
					case memory.TypeDate32:
						values[row] = stamp.Unix() / 86400
					case memory.TypeDate64:
						values[row] = stamp.UnixMilli()
					default:
						values[row] = stamp.UnixNano()
					}
				}
				predicateRowsBind(b, "x", typ, values, validity)
				p, _, err := CompilePredicate(memory.NewSchema(memory.Field{Name: "x", Type: typ}), GT(Year(Col("x")), I64(1969)))
				if err != nil {
					t.Fatal(err)
				}
				out, err := p.Evaluate(b, &EvalWorkspace{}, kernel.New(mode))
				if err != nil {
					t.Fatal(err)
				}
				predicateRowsAssert(t, out, rows, func(row int) (bool, bool) {
					known := predicateRowsKnown(validity, row)
					return known && row%3 != 0, known
				})
			}
		}
	}
}

func TestPredicateLogicalRowsRejectMalformedBuffers(t *testing.T) {
	for _, typ := range predicateRowsTypes {
		schema := memory.NewSchema(memory.Field{Name: "x", Type: typ}, memory.Field{Name: "y", Type: typ})
		predicates := []Expr{GT(Col("x"), I64(0)), LT(Col("x"), I64(math.MaxInt64)), IsNull(Col("x")), IsNotNull(Col("x"))}
		if predicateRowsColumnComparison(typ) {
			predicates = append(predicates, GT(Col("y"), Col("x")))
		}
		for _, rows := range []int{1, 65} {
			for _, shortValues := range []bool{false, true} {
				b := memory.NewBatch()
				b.Rows = rows
				values, validity := make([]int64, rows+65), make(memory.Bitmap, (rows+63)/64-1)
				if shortValues {
					values, validity = values[:rows-1], nil
				}
				predicateRowsBind(b, "x", typ, values, validity)
				predicateRowsBind(b, "y", typ, make([]int64, rows+67), nil)
				for _, pred := range predicates {
					p, _, err := CompilePredicate(schema, pred)
					if err != nil {
						t.Fatal(err)
					}
					for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
						if _, err := p.Evaluate(b, &EvalWorkspace{}, kernel.New(mode)); err == nil || !strings.Contains(err.Error(), `column "x"`) {
							t.Fatalf("type=%s rows=%d shortValues=%v expected x length error, got %v", typ, rows, shortValues, err)
						}
					}
				}
			}
		}
	}
	for _, dictionary := range []bool{false, true} {
		for _, shortValues := range []bool{false, true} {
			b := memory.NewBatch()
			b.Rows = 65
			length, validity := 66, memory.Bitmap{^uint64(0)}
			if shortValues {
				length, validity = 64, nil
			}
			if dictionary {
				b.DictStrings["s"] = &memory.DictionaryStringVector{Indices: make([]uint32, length), Validity: validity}
			} else {
				b.Strings["s"] = &memory.StringViewVector{Offsets: make([]uint32, length+1), Validity: validity}
			}
			for _, pred := range []Expr{EQ(Col("s"), Str("")), Contains(Col("s"), Str("a")), EQ(Length(Col("s")), I64(0)), EQ(Find(Col("s"), Str("")), I64(0)), IsNull(Col("s"))} {
				p, _, err := CompilePredicate(memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}), pred)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := p.Evaluate(b, &EvalWorkspace{}, testKernelSet()); err == nil {
					t.Fatalf("accepted malformed string dictionary=%v shortValues=%v", dictionary, shortValues)
				}
			}
		}
	}
	for _, rows := range []int{-1, int(^uint(0) >> 1)} {
		b := memory.NewBatch()
		b.Rows = rows
		p, _, err := CompilePredicate(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}), GT(Col("x"), I64(0)))
		if err != nil {
			t.Fatal(err)
		}
		var w EvalWorkspace
		if _, err := p.Evaluate(b, &w, testKernelSet()); err == nil {
			t.Fatalf("accepted invalid rows %d", rows)
		}
		if w.MemoryBytes() != 0 {
			t.Fatal("invalid row count allocated workspace")
		}
	}
}

func TestPredicateLogicalRowsNilVectorsAndOpaqueValidity(t *testing.T) {
	for _, typ := range append(slices.Clone(predicateRowsTypes), memory.TypeString, memory.TypeArrow) {
		b := memory.NewBatch()
		b.Rows = 1
		switch typ {
		case memory.TypeString:
			b.Strings["x"] = nil
		case memory.TypeArrow:
			b.Opaque["x"] = nil
		default:
			predicateRowsBind(b, "x", typ, []int64{1}, nil)
			columns := reflect.ValueOf(b).Elem()
			for i := 0; i < columns.NumField(); i++ {
				field := columns.Field(i)
				if field.Kind() == reflect.Map && field.MapIndex(reflect.ValueOf("x")).IsValid() {
					field.SetMapIndex(reflect.ValueOf("x"), reflect.Zero(field.Type().Elem()))
				}
			}
		}
		p, _, err := CompilePredicate(memory.NewSchema(memory.Field{Name: "x", Type: typ}), IsNull(Col("x")))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Evaluate(b, &EvalWorkspace{}, testKernelSet()); err == nil || !strings.Contains(err.Error(), "nil vector") {
			t.Fatalf("type=%s nil vector error=%v", typ, err)
		}
		if typ == memory.TypeString {
			delete(b.Strings, "x")
			b.DictStrings["x"] = nil
			if _, err := p.Evaluate(b, &EvalWorkspace{}, testKernelSet()); err == nil || !strings.Contains(err.Error(), "nil vector") {
				t.Fatalf("nil dictionary error=%v", err)
			}
		}
	}
	for _, rows := range []int{0, 1, 63, 64, 65} {
		for _, pred := range []Expr{IsNull(Col("x")), IsNotNull(Col("x"))} {
			p, _, err := CompilePredicate(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeArrow}), pred)
			if err != nil {
				t.Fatal(err)
			}
			for _, nulls := range []string{"none", "mixed", "all"} {
				b := memory.NewBatch()
				b.Rows = rows
				validity := predicateRowsValidity(rows, nulls, 1)
				b.Opaque["x"] = &memory.OpaqueVector{Rows: rows + 65, Type: memory.TypeArrow, Validity: validity}
				out, err := p.Evaluate(b, &EvalWorkspace{}, testKernelSet())
				if err != nil {
					t.Fatal(err)
				}
				predicateRowsAssert(t, out, rows, func(row int) (bool, bool) {
					truth := !predicateRowsKnown(validity, row)
					if pred.(Unary).Op == OpIsNotNull {
						truth = !truth
					}
					return truth, true
				})
				if rows == 0 {
					continue
				}
				b.Opaque["x"].Rows = rows - 1
				if _, err := p.Evaluate(b, &EvalWorkspace{}, testKernelSet()); err == nil {
					t.Fatal("accepted short opaque rows")
				}
				b.Opaque["x"].Rows = rows + 65
				b.Opaque["x"].Validity = make(memory.Bitmap, (rows+63)/64-1)
				if _, err := p.Evaluate(b, &EvalWorkspace{}, testKernelSet()); err == nil {
					t.Fatal("accepted short opaque validity")
				}
			}
		}
	}
}

func TestPredicateLogicalRowsWorkspaceAndAllocations(t *testing.T) {
	for _, typ := range predicateRowsTypes {
		for _, rows := range []int{0, 1, 63, 64, 65} {
			b := memory.NewBatch()
			b.Rows = rows
			predicateRowsBind(b, "x", typ, make([]int64, rows+65), nil)
			pred := Expr(GT(Col("x"), I64(0)))
			if typ == memory.TypeFloat32 {
				pred = GT(Col("x"), F64(.1))
			}
			p, _, err := CompilePredicate(memory.NewSchema(memory.Field{Name: "x", Type: typ}), pred)
			if err != nil {
				t.Fatal(err)
			}
			var w EvalWorkspace
			ks := testKernelSet()
			if _, err := p.Evaluate(b, &w, ks); err != nil {
				t.Fatal(err)
			}
			want, err := p.WorkspaceBytes(rows)
			if err != nil {
				t.Fatal(err)
			}
			if w.MemoryBytes() != want {
				t.Fatalf("type=%s rows=%d retained=%d want=%d", typ, rows, w.MemoryBytes(), want)
			}
			if allocs := testing.AllocsPerRun(10, func() {
				if _, err := p.Evaluate(b, &w, ks); err != nil {
					panic(err)
				}
			}); allocs != 0 {
				t.Fatalf("type=%s rows=%d warmed evaluation allocs=%g", typ, rows, allocs)
			}
		}
	}
	for _, dictionary := range []bool{false, true} {
		b := memory.NewBatch()
		b.Rows = 1
		if dictionary {
			b.DictStrings["s"] = &memory.DictionaryStringVector{Indices: make([]uint32, 66), Offsets: []uint32{0, 1}, Data: []byte("a"), DictionaryDomain: "allocation-test"}
		} else {
			b.Strings["s"] = &memory.StringViewVector{Offsets: make([]uint32, 67)}
		}
		p, _, err := CompilePredicate(memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}), Contains(Col("s"), Str("a")))
		if err != nil {
			t.Fatal(err)
		}
		var w EvalWorkspace
		ks := testKernelSet()
		if _, err := p.Evaluate(b, &w, ks); err != nil {
			t.Fatal(err)
		}
		want, _ := p.WorkspaceBytes(1)
		if dictionary {
			want++
		}
		if w.MemoryBytes() != want {
			t.Fatalf("string retained=%d want=%d", w.MemoryBytes(), want)
		}
		if allocs := testing.AllocsPerRun(10, func() {
			if _, err := p.Evaluate(b, &w, ks); err != nil {
				panic(err)
			}
		}); allocs != 0 {
			t.Fatalf("string evaluation allocated: %g", allocs)
		}
	}
}

func predicateRowsAssert(t *testing.T, got BoolVector, rows int, want func(int) (bool, bool)) {
	t.Helper()
	truth, validity := make(memory.Bitmap, (rows+63)/64), make(memory.Bitmap, (rows+63)/64)
	for row := 0; row < rows; row++ {
		tr, va := want(row)
		if tr {
			truth[row/64] |= uint64(1) << uint(row%64)
		}
		if va {
			validity[row/64] |= uint64(1) << uint(row%64)
		}
	}
	if !slices.Equal(got.Truth, truth) || !slices.Equal(got.Validity, validity) {
		t.Fatalf("rows=%d truth=%x validity=%x want truth=%x validity=%x", rows, got.Truth, got.Validity, truth, validity)
	}
}

func predicateRowsKnown(validity memory.Bitmap, row int) bool {
	return validity == nil || validity[row/64]&(uint64(1)<<uint(row%64)) != 0
}

func predicateRowsValidity(rows int, nulls string, missing int) memory.Bitmap {
	if nulls == "none" {
		return nil
	}
	v := make(memory.Bitmap, (rows+63)/64)
	if nulls == "all" {
		return v
	}
	for i := range v {
		v[i] = ^uint64(0)
	} // Poison all tail bits, but do not supply extra validity words.
	for row := 0; row < rows; row++ {
		if row%3 == missing {
			v[row/64] &^= uint64(1) << uint(row%64)
		}
	}
	return v
}

func predicateRowsCompare[T int64 | uint64 | float64](a, b T, op Op) bool {
	switch op {
	case OpGT:
		return a > b
	case OpGE:
		return a >= b
	case OpLT:
		return a < b
	case OpLE:
		return a <= b
	case OpEQ:
		return a == b
	default:
		panic("unsupported test comparison")
	}
}

func predicateRowsConvert[T ~int8 | ~int16 | ~int32 | ~int64 | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64](values []int64) []T {
	out := make([]T, len(values))
	for i, value := range values {
		out[i] = T(value)
	}
	return out
}

func predicateRowsBind(b *memory.Batch, name string, typ memory.DataType, values []int64, validity memory.Bitmap) {
	switch typ {
	case memory.TypeInt8:
		b.Int8s[name] = &memory.Int8Vector{Values: predicateRowsConvert[int8](values), Validity: validity}
	case memory.TypeInt16:
		b.Int16s[name] = &memory.Int16Vector{Values: predicateRowsConvert[int16](values), Validity: validity}
	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		b.Int32s[name] = &memory.Int32Vector{Values: predicateRowsConvert[int32](values), Validity: validity}
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		b.Int64s[name] = &memory.Int64Vector{Values: values, Validity: validity}
	case memory.TypeUint8:
		b.Uint8s[name] = &memory.Uint8Vector{Values: predicateRowsConvert[uint8](values), Validity: validity}
	case memory.TypeUint16:
		b.Uint16s[name] = &memory.Uint16Vector{Values: predicateRowsConvert[uint16](values), Validity: validity}
	case memory.TypeUint32, memory.TypeBool:
		b.Uint32s[name] = &memory.Uint32Vector{Values: predicateRowsConvert[uint32](values), Validity: validity}
	case memory.TypeUint64:
		b.Uint64s[name] = &memory.Uint64Vector{Values: predicateRowsConvert[uint64](values), Validity: validity}
	case memory.TypeFloat32:
		b.Float32s[name] = &memory.Float32Vector{Values: predicateRowsConvert[float32](values), Validity: validity}
	case memory.TypeFloat64:
		b.Float64s[name] = &memory.Float64Vector{Values: predicateRowsConvert[float64](values), Validity: validity}
	default:
		panic("unsupported test type")
	}
}

func predicateRowsColumnLen(b *memory.Batch, name string) int {
	// Reflection is test-only and observes the caller's vector headers directly.
	for _, field := range []string{"Int8s", "Int16s", "Int32s", "Int64s", "Uint8s", "Uint16s", "Uint32s", "Uint64s", "Float32s", "Float64s"} {
		v := reflect.ValueOf(b).Elem().FieldByName(field).MapIndex(reflect.ValueOf(name))
		if v.IsValid() {
			return v.Elem().FieldByName("Values").Len()
		}
	}
	panic("missing test column")
}

func predicateRowsColumnComparison(typ memory.DataType) bool {
	switch typ {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64:
		return true
	default:
		return false
	}
}
