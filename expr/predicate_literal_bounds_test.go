package expr

import (
	"fmt"
	"math"
	"testing"

	"gocolumn/kernel"
	"gocolumn/memory"
)

func TestPredicateIntegerLiteralBoundsExactOracle(t *testing.T) {
	tests := []struct {
		typ      memory.DataType
		min, max int64
		unsigned bool
	}{
		{memory.TypeInt8, math.MinInt8, math.MaxInt8, false},
		{memory.TypeInt16, math.MinInt16, math.MaxInt16, false},
		{memory.TypeInt32, math.MinInt32, math.MaxInt32, false},
		{memory.TypeDate32, math.MinInt32, math.MaxInt32, false},
		{memory.TypeTime32, math.MinInt32, math.MaxInt32, false},
		{memory.TypeUint8, 0, math.MaxUint8, true},
		{memory.TypeUint16, 0, math.MaxUint16, true},
		{memory.TypeUint32, 0, math.MaxUint32, true},
		{memory.TypeBool, 0, math.MaxUint32, true},
	}
	for _, tc := range tests {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			t.Run(fmt.Sprintf("%s/%s", tc.typ, mode), func(t *testing.T) {
				values := []int64{tc.min, tc.min + 1, 0, 1, 5, tc.max - 1, tc.max}
				if tc.typ == memory.TypeBool {
					values = []int64{0, 1}
				}
				literals := []int64{tc.min, tc.min + 1, 0, 1, 5, tc.max - 1, tc.max, tc.max + 1, 1 << 32, 1 << 40, math.MaxInt64}
				if !tc.unsigned {
					literals = append(literals, tc.min-1, -(1 << 32), math.MinInt64)
				}
				for _, rows := range []int{0, 1, 63, 64, 65, 193} {
					for _, nulls := range []string{"none", "mixed", "all"} {
						validity := literalTestValidity(rows, nulls)
						exact := make([]int64, rows)
						for row := range exact {
							exact[row] = values[row%len(values)]
						}
						batch := literalTestIntegerBatch(tc.typ, exact, validity)
						schema := memory.NewSchema(memory.Field{Name: "v", Type: tc.typ, Nullable: nulls != "none"})
						var workspace EvalWorkspace
						for _, literal := range literals {
							for _, op := range []Op{OpGT, OpGE, OpLT, OpLE, OpEQ} {
								predicate, simple, err := CompilePredicate(schema, Binary{Op: op, Left: Col("v"), Right: I64(literal)})
								if err != nil {
									t.Fatal(err)
								}
								if simple == nil || simple.Type != tc.typ || simple.Op != op || (!tc.unsigned && simple.LiteralI64 != literal) || (tc.unsigned && simple.LiteralU64 != uint64(literal)) {
									t.Fatalf("compiled literal or type changed: %+v", simple)
								}
								out, err := predicate.Evaluate(batch, &workspace, kernel.New(mode))
								if err != nil {
									t.Fatal(err)
								}
								for row, value := range exact {
									known := validity == nil || memory.IsSet(validity, row)
									want := known && literalComparisonOracle(value, literal, op)
									if memory.IsSet(out.Truth, row) != want || memory.IsSet(out.Validity, row) != known {
										t.Fatalf("rows=%d nulls=%s row=%d: %d %s %d = (%v,%v), want (%v,%v)", rows, nulls, row, value, op, literal, memory.IsSet(out.Truth, row), memory.IsSet(out.Validity, row), want, known)
									}
								}
								assertLiteralTestTail(t, out, rows)
							}
						}
					}
				}
			})
		}
	}
}

func TestPredicateFloat32LiteralPrecisionExactOracle(t *testing.T) {
	values := []float32{
		0, float32(math.Copysign(0, -1)), 1, -1, 0.1, -0.1,
		math.Nextafter32(1, 0), math.Nextafter32(1, 2), math.Nextafter32(-1, 0), math.Nextafter32(-1, -2),
		math.SmallestNonzeroFloat32, -math.SmallestNonzeroFloat32,
		math.MaxFloat32, -math.MaxFloat32, float32(math.Inf(1)), float32(math.Inf(-1)), float32(math.NaN()),
		1 << 24, 1<<24 + 2, 1 << 32,
	}
	literals := []float64{
		0, math.Copysign(0, -1), 1, -1, 0.1, -0.1, float64(float32(0.1)), float64(float32(-0.1)),
		1 + 1.0/(1<<24), -1 - 1.0/(1<<24), math.Nextafter(1, 0), math.Nextafter(1, 2),
		math.SmallestNonzeroFloat32, -math.SmallestNonzeroFloat32,
		math.SmallestNonzeroFloat32 / 2, -math.SmallestNonzeroFloat32 / 2,
		math.SmallestNonzeroFloat32 * 0.75, -math.SmallestNonzeroFloat32 * 0.75,
		math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64,
		math.MaxFloat32, -math.MaxFloat32, math.Nextafter(math.MaxFloat32, math.Inf(1)),
		math.Nextafter(-math.MaxFloat32, math.Inf(-1)), math.MaxFloat64, -math.MaxFloat64,
		math.Inf(1), math.Inf(-1), math.NaN(),
		1<<24 + 1, 1<<32 + 1,
	}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		t.Run(string(mode), func(t *testing.T) {
			for _, rows := range []int{0, 1, 63, 64, 65, 193} {
				for _, nulls := range []string{"none", "mixed", "all"} {
					validity := literalTestValidity(rows, nulls)
					batch := memory.NewBatch()
					batch.Rows = rows
					native := make([]float32, rows)
					for row := range native {
						native[row] = values[row%len(values)]
					}
					batch.Float32s["v"] = &memory.Float32Vector{Values: native, Validity: validity}
					schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat32, Nullable: nulls != "none"})
					var workspace EvalWorkspace
					for _, literal := range literals {
						for _, op := range []Op{OpGT, OpGE, OpLT, OpLE, OpEQ} {
							predicate, _, err := CompilePredicate(schema, Binary{Op: op, Left: Col("v"), Right: F64(literal)})
							if err != nil {
								t.Fatal(err)
							}
							out, err := predicate.Evaluate(batch, &workspace, kernel.New(mode))
							if err != nil {
								t.Fatal(err)
							}
							for row, value := range native {
								known := validity == nil || memory.IsSet(validity, row)
								want := known && literalComparisonOracle(float64(value), literal, op)
								if memory.IsSet(out.Truth, row) != want || memory.IsSet(out.Validity, row) != known {
									t.Fatalf("rows=%d nulls=%s row=%d: %.17g %s %.17g = (%v,%v), want (%v,%v)", rows, nulls, row, value, op, literal, memory.IsSet(out.Truth, row), memory.IsSet(out.Validity, row), want, known)
								}
							}
							assertLiteralTestTail(t, out, rows)
						}
					}
				}
			}
		})
	}
}

func TestPredicateOutsideIntegerRangeRetainsUnknown(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt8, Nullable: true})
	batch := literalTestIntegerBatch(memory.TypeInt8, []int64{5, 5, 5}, memory.Bitmap{0b101})
	for _, op := range []Op{OpGT, OpGE, OpLT, OpLE, OpEQ} {
		comparison := Binary{Op: op, Left: Col("v"), Right: I64(1 << 32)}
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			predicate, _, err := CompilePredicate(schema, Not(comparison))
			if err != nil {
				t.Fatal(err)
			}
			out, err := predicate.Evaluate(batch, &EvalWorkspace{}, kernel.New(mode))
			if err != nil {
				t.Fatal(err)
			}
			for row := range batch.Rows {
				known := row != 1
				want := known && !literalComparisonOracle(int64(5), int64(1<<32), op)
				if memory.IsSet(out.Validity, row) != known || memory.IsSet(out.Truth, row) != want {
					t.Fatalf("%s %s row=%d: truth=%v validity=%v", mode, op, row, out.Truth, out.Validity)
				}
			}
		}
	}
}

func TestPredicateLiteralBoundsPreserveExplicitTypes(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeBool} {
		schema := memory.NewSchema(memory.Field{Name: "v", Type: typ})
		if _, _, err := CompilePredicate(schema, GT(Col("v"), I64(-1))); err == nil {
			t.Fatalf("%s accepted negative signed literal", typ)
		}
	}
	for _, tc := range []struct {
		typ     memory.DataType
		literal Expr
	}{
		{memory.TypeInt8, F64(128)},
		{memory.TypeInt32, Date32Literal{Days: 1}},
		{memory.TypeInt64, TimestampLiteral{Value: 1}},
		{memory.TypeDate32, TimestampLiteral{Value: 1}},
		{memory.TypeTime32, Date32Literal{Days: 1}},
	} {
		schema := memory.NewSchema(memory.Field{Name: "v", Type: tc.typ})
		if _, _, err := CompilePredicate(schema, GT(Col("v"), tc.literal)); err == nil {
			t.Fatalf("%s accepted incompatible %T literal", tc.typ, tc.literal)
		}
	}
	// Integer literals on float columns deliberately use the compiler's float64
	// conversion, not an extra rounding to the physical float32 column type.
	batch := memory.NewBatch()
	batch.Rows = 1
	batch.Float32s["v"] = &memory.Float32Vector{Values: []float32{1 << 24}}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, op := range []Op{OpGT, OpGE, OpLT, OpLE, OpEQ} {
			predicate, _, err := CompilePredicate(memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat32}), Binary{Op: op, Left: Col("v"), Right: I64(1<<24 + 1)})
			if err != nil {
				t.Fatal(err)
			}
			out, err := predicate.Evaluate(batch, &EvalWorkspace{}, kernel.New(mode))
			if err != nil {
				t.Fatal(err)
			}
			if want := literalComparisonOracle(float64(1<<24), float64(1<<24+1), op); memory.IsSet(out.Truth, 0) != want {
				t.Fatalf("%s %s rounded integer literal", mode, op)
			}
		}
	}
}

// The oracle uses Go's exact ordered comparison of widened native inputs and
// original literals. It is independent of predicate helpers and kernel output.
func literalComparisonOracle[T int64 | float64](value, literal T, op Op) bool {
	switch op {
	case OpGT:
		return value > literal
	case OpGE:
		return value >= literal
	case OpLT:
		return value < literal
	case OpLE:
		return value <= literal
	case OpEQ:
		return value == literal
	default:
		panic("unsupported test comparison")
	}
}

func literalTestValidity(rows int, nulls string) memory.Bitmap {
	if nulls == "none" {
		return nil
	}
	validity := make(memory.Bitmap, (rows+63)/64)
	if nulls == "mixed" {
		for row := range rows {
			if row%3 != 1 {
				memory.Set(validity, row)
			}
		}
	}
	return validity
}

func assertLiteralTestTail(t *testing.T, out BoolVector, rows int) {
	t.Helper()
	if rows == 0 {
		if len(out.Truth) != 0 || len(out.Validity) != 0 {
			t.Fatalf("empty input retained bitmap output: %+v", out)
		}
		return
	}
	tail := len(out.Truth) - 1
	if out.Truth[tail]&^memory.TailMask(rows) != 0 || out.Validity[tail]&^memory.TailMask(rows) != 0 {
		t.Fatalf("dirty tail bits: rows=%d truth=%b validity=%b", rows, out.Truth[tail], out.Validity[tail])
	}
}

func literalTestIntegerBatch(typ memory.DataType, exact []int64, validity memory.Bitmap) *memory.Batch {
	batch := memory.NewBatch()
	batch.Rows = len(exact)
	switch typ {
	case memory.TypeInt8:
		batch.Int8s["v"] = &memory.Int8Vector{Values: literalTestNarrowValues[int8](exact), Validity: validity}
	case memory.TypeInt16:
		batch.Int16s["v"] = &memory.Int16Vector{Values: literalTestNarrowValues[int16](exact), Validity: validity}
	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		batch.Int32s["v"] = &memory.Int32Vector{Values: literalTestNarrowValues[int32](exact), Validity: validity}
	case memory.TypeUint8:
		batch.Uint8s["v"] = &memory.Uint8Vector{Values: literalTestNarrowValues[uint8](exact), Validity: validity}
	case memory.TypeUint16:
		batch.Uint16s["v"] = &memory.Uint16Vector{Values: literalTestNarrowValues[uint16](exact), Validity: validity}
	case memory.TypeUint32, memory.TypeBool:
		batch.Uint32s["v"] = &memory.Uint32Vector{Values: literalTestNarrowValues[uint32](exact), Validity: validity}
	default:
		panic("unsupported test integer type")
	}
	return batch
}

func literalTestNarrowValues[T int8 | int16 | int32 | uint8 | uint16 | uint32](values []int64) []T {
	out := make([]T, len(values))
	for row, value := range values {
		out[row] = T(value)
	}
	return out
}
