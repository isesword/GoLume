package aggregate

import (
	"bytes"
	"encoding/json"
	"gocolumn/memory"
	"math"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

var numericAll = Layout{Sum: true, CountRows: true, CountValues: true, Min: true, Max: true, Avg: true}

func mustNumeric(t testing.TB, typ memory.DataType, layout Layout) NumericVector {
	t.Helper()
	v, err := NewNumericVector(1, typ, layout)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestNumericVectorInt64ParityAllLayouts(t *testing.T) {
	rng := rand.New(rand.NewSource(17))
	for mask := 1; mask < 64; mask++ {
		l := Layout{Sum: mask&1 != 0, CountRows: mask&2 != 0, CountValues: mask&4 != 0, Min: mask&8 != 0, Max: mask&16 != 0, Avg: mask&32 != 0}
		v := mustNumeric(t, memory.TypeInt64, l)
		old := NewInt64Vector(1, l)
		for i := 0; i < 1000; i++ {
			x := int64(rng.Uint64())
			valid := rng.Intn(5) != 0
			v.AddBits(0, uint64(x), valid)
			old.AddValue(0, x, valid)
		}
		got, want := v.Result(0), old.Result(0)
		if got.int64State() != (Int64Result{Sum: want.Sum, SumValid: want.SumValid, CountRows: want.CountRows, CountValues: want.CountValues, Min: want.Min, MinValid: want.MinValid, Max: want.Max, MaxValid: want.MaxValid}) {
			t.Fatalf("layout %+v: got %+v want %+v", l, got, want)
		}
		if l.Avg {
			bits, valid := got.Value(KindAvg)
			if valid != want.AvgValid || math.Float64frombits(bits) != want.Avg {
				t.Fatalf("mean parity %+v %+v", got, want)
			}
		}
		copy := mustNumeric(t, memory.TypeInt64, l)
		if err := v.Copy(&copy, 0, 0); err != nil {
			t.Fatal(err)
		}
		if copy.Result(0) != got {
			t.Fatal("copy lost state")
		}
		if err := copy.MergeResult(0, got); err != nil {
			t.Fatal(err)
		}
		old.MergeResult(0, want)
		merged := copy.Result(0)
		expected := old.Result(0)
		if merged.SumBits != uint64(expected.Sum) || merged.CountRows != expected.CountRows || merged.CountValues != expected.CountValues {
			t.Fatal("merge differs from legacy")
		}
	}
}

func TestNumericVectorWidthsAndMeans(t *testing.T) {
	cases := []struct {
		typ      memory.DataType
		values   []uint64
		sum      uint64
		mean     float64
		min, max uint64
	}{
		{memory.TypeInt8, []uint64{127, 127, 0xff}, 253, 253.0 / 3, ^uint64(0), 127},
		{memory.TypeInt16, []uint64{32767, 32767, 0xffff}, 65533, 65533.0 / 3, ^uint64(0), 32767},
		{memory.TypeUint8, []uint64{255, 255}, 510, 255, 255, 255},
		{memory.TypeUint16, []uint64{65535, 65535}, 131070, 65535, 65535, 65535},
		{memory.TypeInt32, []uint64{math.MaxInt32, math.MaxInt32}, uint64(math.MaxUint64 - 1), math.MaxInt32, math.MaxInt32, math.MaxInt32},
		{memory.TypeUint32, []uint64{math.MaxUint32, math.MaxUint32}, math.MaxUint32 - 1, math.MaxUint32, math.MaxUint32, math.MaxUint32},
		{memory.TypeUint64, []uint64{math.MaxUint64, math.MaxUint64}, math.MaxUint64 - 1, float64(uint64(math.MaxUint64)), math.MaxUint64, math.MaxUint64},
		{memory.TypeUint64, []uint64{1<<53 + 1, 1<<53 + 3}, 1<<54 + 4, float64(1<<53 + 2), 1<<53 + 1, 1<<53 + 3},
	}
	for _, tc := range cases {
		t.Run(tc.typ.String()+strconv.Itoa(len(tc.values)), func(t *testing.T) {
			v := mustNumeric(t, tc.typ, numericAll)
			v.AddBits(0, 123, false)
			for _, x := range tc.values {
				v.AddBits(0, x, true)
			}
			r := v.Result(0)
			bits, valid := r.Value(KindAvg)
			if !valid || math.Float64frombits(bits) != tc.mean || r.SumBits != tc.sum || r.MinBits != tc.min || r.MaxBits != tc.max || r.CountRows != uint64(len(tc.values)+1) || r.CountValues != uint64(len(tc.values)) {
				t.Fatalf("state %+v avg=%g expected %+v", r, math.Float64frombits(bits), tc)
			}
		})
	}
}

func TestNumericFloatNullNaNInfinityAndZero(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeFloat64} {
		bits := func(f float64) uint64 {
			if typ == memory.TypeFloat32 {
				return uint64(math.Float32bits(float32(f)))
			}
			return math.Float64bits(f)
		}
		for _, tc := range []struct {
			name          string
			values        []float64
			sum, min, max float64
		}{
			{"all-nan", []float64{math.NaN(), math.NaN()}, math.NaN(), math.NaN(), math.NaN()},
			{"mixed-nan", []float64{math.NaN(), 3, -2, math.NaN()}, math.NaN(), -2, 3},
			{"opposite-infinities", []float64{math.Inf(1), math.Inf(-1)}, math.NaN(), math.Inf(-1), math.Inf(1)},
			{"positive-infinity", []float64{2, math.Inf(1)}, math.Inf(1), 2, math.Inf(1)},
			{"signed-zero", []float64{0, math.Copysign(0, -1)}, 0, math.Copysign(0, -1), 0},
		} {
			t.Run(typ.String()+"/"+tc.name, func(t *testing.T) {
				v := mustNumeric(t, typ, numericAll)
				v.AddBits(0, bits(123), false)
				for _, x := range tc.values {
					v.AddBits(0, bits(x), true)
				}
				r := v.Result(0)
				for _, check := range []struct {
					kind Kind
					want float64
				}{{KindSum, tc.sum}, {KindMin, tc.min}, {KindMax, tc.max}} {
					b, valid := r.Value(check.kind)
					got := numericAsFloat(typ, b)
					if !valid || !(got == check.want || math.IsNaN(got) && math.IsNaN(check.want)) {
						t.Fatalf("kind %d got %v valid=%v want=%v", check.kind, got, valid, check.want)
					}
					if check.want == 0 && math.Signbit(got) != math.Signbit(check.want) {
						t.Fatalf("zero sign kind %d", check.kind)
					}
				}
				if r.CountRows != uint64(len(tc.values)+1) || r.CountValues != uint64(len(tc.values)) {
					t.Fatal("NaN/null counts wrong")
				}
				avg, valid := r.Value(KindAvg)
				if !valid {
					t.Fatal("nonempty AVG null")
				}
				got := numericAsFloat(typ, avg)
				if math.IsNaN(tc.sum) && !math.IsNaN(got) {
					t.Fatal("AVG did not propagate NaN")
				}
			})
		}
		v := mustNumeric(t, typ, numericAll)
		v.AddBits(0, bits(math.NaN()), false)
		r := v.Result(0)
		for _, kind := range []Kind{KindSum, KindMin, KindMax, KindAvg} {
			if _, valid := r.Value(kind); valid {
				t.Fatalf("null kind %d valid", kind)
			}
		}
	}
}

func TestNumericVectorRandomSplitMerge(t *testing.T) {
	types := []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64}
	rng := rand.New(rand.NewSource(93))
	for _, typ := range types {
		t.Run(typ.String(), func(t *testing.T) {
			all := mustNumeric(t, typ, numericAll)
			a := mustNumeric(t, typ, numericAll)
			b := mustNumeric(t, typ, numericAll)
			for i := 0; i < 1000; i++ {
				bits := uint64(rng.Int63n(2000) - 1000)
				if typ == memory.TypeFloat32 {
					bits = uint64(math.Float32bits(float32(int64(bits)) / 13))
				}
				if typ == memory.TypeFloat64 {
					bits = math.Float64bits(float64(int64(bits)) / 13)
				}
				valid := rng.Intn(7) != 0
				all.AddBits(0, bits, valid)
				if i%3 == 0 {
					a.AddBits(0, bits, valid)
				} else {
					b.AddBits(0, bits, valid)
				}
			}
			if err := a.MergeResult(0, b.Result(0)); err != nil {
				t.Fatal(err)
			}
			got, want := a.Result(0), all.Result(0)
			for _, kind := range []Kind{KindSum, KindCountRows, KindCountValues, KindMin, KindMax, KindAvg} {
				gb, gv := got.Value(kind)
				wb, wv := want.Value(kind)
				if gv != wv {
					t.Fatal("validity mismatch")
				}
				output, err := NumericAggregateType(typ, kind)
				if err != nil {
					t.Fatal(err)
				}
				if output == memory.TypeFloat32 || output == memory.TypeFloat64 {
					gf, wf := numericAsFloat(output, gb), numericAsFloat(output, wb)
					tol := 1e-12
					if output == memory.TypeFloat32 {
						tol = 1e-5
					}
					if math.Abs(gf-wf) > tol*max(1, math.Abs(wf)) {
						t.Fatalf("kind %d merged=%g sequential=%g", kind, gf, wf)
					}
				} else if gb != wb {
					t.Fatalf("kind %d bits %x != %x", kind, gb, wb)
				}
			}
		})
	}
}

func TestNumericVectorCountOnlyAndInvalidStates(t *testing.T) {
	for typ := memory.TypeInt32; typ <= memory.TypeStruct; typ++ {
		v := mustNumeric(t, typ, Layout{CountRows: true, CountValues: true})
		v.AddBits(0, math.MaxUint64, true)
		v.AddBits(0, 0, false)
		r := v.Result(0)
		if r.CountRows != 2 || r.CountValues != 1 {
			t.Fatalf("count %s %+v", typ, r)
		}
	}
	for _, tc := range []struct {
		slots int
		typ   memory.DataType
		l     Layout
	}{{-1, memory.TypeInt8, numericAll}, {math.MaxInt, memory.TypeFloat64, numericAll}, {1, memory.TypeInvalid, numericAll}, {1, memory.DataType(255), Layout{CountRows: true}}, {1, memory.TypeString, numericAll}, {1, memory.TypeInt64, Layout{}}} {
		if _, err := NumericVectorBytesForSlots(tc.slots, tc.typ, tc.l); err == nil {
			t.Fatalf("accepted invalid %+v", tc)
		}
	}
	v := mustNumeric(t, memory.TypeFloat64, numericAll)
	v.AddBits(0, math.Float64bits(12), true)
	before := v.Result(0)
	bad := before
	bad.Type = memory.TypeFloat32
	if err := v.MergeResult(0, bad); err == nil {
		t.Fatal("type mismatch accepted")
	}
	bad = before
	bad.Layout.Min = false
	if err := v.InitResult(0, bad); err == nil {
		t.Fatal("layout mismatch accepted")
	}
	if v.Result(0) != before {
		t.Fatal("rejected state mutated destination")
	}
	if err := v.InitResult(0, NumericResult{Type: v.Type(), Layout: v.Layout()}); err != nil {
		t.Fatal(err)
	}
	if r := v.Result(0); r.SumValid || r.CountRows != 0 || r.CountValues != 0 {
		t.Fatalf("clear %+v", r)
	}
}

func TestNumericVectorBytesAndAllocationFreeUpdates(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt64, memory.TypeFloat32, memory.TypeString} {
		l := numericAll
		if typ == memory.TypeString {
			l = Layout{CountRows: true, CountValues: true}
		}
		v := mustNumeric(t, typ, l)
		bytes, err := NumericVectorBytesForSlots(1, typ, l)
		if err != nil {
			t.Fatal(err)
		}
		var actual int64
		if typ == memory.TypeInt64 {
			actual = Int64VectorBytesForSlots(1, l)
		} else {
			actual = int64(len(v.sums)+len(v.mins)+len(v.maxs)+len(v.countRows)+len(v.countValues)+len(v.avgSums))*8 + int64(len(v.sumValid)+len(v.minValid)+len(v.maxValid))
		}
		if actual != bytes {
			t.Fatalf("%s bytes=%d actual=%d", typ, bytes, actual)
		}
		if got := testing.AllocsPerRun(100, func() { v.AddBits(0, 1, true); r := v.Result(0); _, _ = r.Value(KindSum) }); got != 0 {
			t.Fatalf("%s allocs=%g", typ, got)
		}
	}
}

func TestNumericAggregateOutputTypes(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeUint8, memory.TypeUint16} {
		got, err := NumericAggregateType(typ, KindSum)
		if err != nil || got != memory.TypeInt64 {
			t.Fatalf("sum %s: %s %v", typ, got, err)
		}
	}
	for _, typ := range []memory.DataType{memory.TypeInt32, memory.TypeInt64, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64} {
		got, err := NumericAggregateType(typ, KindSum)
		if err != nil || got != typ {
			t.Fatalf("sum %s: %s %v", typ, got, err)
		}
	}
	if _, err := NumericAggregateType(memory.TypeBool, KindSum); err == nil {
		t.Fatal("bool numeric aggregate accepted")
	}
	if _, err := NumericAggregateType(memory.TypeInt32, Kind(255)); err == nil {
		t.Fatal("invalid kind accepted")
	}
}

func TestNumericVectorPolarsOracle(t *testing.T) {
	type oracleValue struct {
		DType  string `json:"dtype"`
		Values []any  `json:"values"`
	}
	var fixture struct {
		Cases []struct {
			ID      string          `json:"id"`
			Input   json.RawMessage `json:"input"`
			DType   string          `json:"input_dtype"`
			Results json.RawMessage `json:"results"`
		} `json:"cases"`
	}
	f, err := os.Open("../testdata/fundamentals/polars-1.35.2.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	dec.UseNumber()
	if err := dec.Decode(&fixture); err != nil {
		t.Fatal(err)
	}
	types := map[string]memory.DataType{"Int8": memory.TypeInt8, "Int16": memory.TypeInt16, "Int32": memory.TypeInt32, "Int64": memory.TypeInt64, "UInt8": memory.TypeUint8, "UInt16": memory.TypeUint16, "UInt32": memory.TypeUint32, "UInt64": memory.TypeUint64, "Float32": memory.TypeFloat32, "Float64": memory.TypeFloat64}
	parsed := 0
	for _, tc := range fixture.Cases {
		typ, ok := types[tc.DType]
		if !ok || !strings.HasPrefix(tc.ID, "aggregate/") {
			continue
		}
		t.Run(tc.ID, func(t *testing.T) {
			v := mustNumeric(t, typ, numericAll)
			var results map[string]oracleValue
			rd := json.NewDecoder(bytes.NewReader(tc.Results))
			rd.UseNumber()
			if err := rd.Decode(&results); err != nil {
				t.Fatal(err)
			}
			var input []any
			decoder := json.NewDecoder(bytes.NewReader(tc.Input))
			decoder.UseNumber()
			if err := decoder.Decode(&input); err != nil {
				t.Fatal(err)
			}
			for _, x := range input {
				bits, valid := oracleNumericBits(t, typ, x)
				v.AddBits(0, bits, valid)
			}
			r := v.Result(0)
			for name, kind := range map[string]Kind{"sum": KindSum, "min": KindMin, "max": KindMax, "avg": KindAvg, "count": KindCountValues, "len": KindCountRows} {
				expected, ok := results[name]
				if !ok || len(expected.Values) != 1 {
					continue
				}
				output, err := NumericAggregateType(typ, kind)
				if err != nil {
					t.Fatal(err)
				}
				got, valid := r.Value(kind)
				want, wvalid := oracleNumericBits(t, output, expected.Values[0])
				// GoLume preserves SQL NULL SUM identity, rather than Polars typed zero.
				if kind == KindSum && r.CountValues == 0 {
					if valid {
						t.Fatal("empty SUM valid")
					}
					continue
				}
				// Explicitly retained Int64 mean-overflow compatibility is tested against
				// Int64Vector above, not mistaken for Polars semantic parity here.
				if kind == KindAvg && typ == memory.TypeInt64 {
					continue
				}
				if valid != wvalid {
					t.Fatalf("%s validity %v want %v", name, valid, wvalid)
				}
				if !valid {
					continue
				}
				if output == memory.TypeFloat32 || output == memory.TypeFloat64 {
					gf, wf := numericAsFloat(output, got), numericAsFloat(output, want)
					tol := 1e-12
					if output == memory.TypeFloat32 {
						tol = 1e-6
					}
					if !(gf == wf || math.IsNaN(gf) && math.IsNaN(wf) || !math.IsInf(gf, 0) && !math.IsInf(wf, 0) && math.Abs(gf-wf) <= tol*max(1, math.Abs(wf))) {
						t.Fatalf("%s got %g want %g", name, gf, wf)
					}
				} else if got != want {
					t.Fatalf("%s bits %x want %x", name, got, want)
				}
			}
		})
		parsed++
	}
	if parsed < 20 {
		t.Fatalf("only %d oracle cases consumed", parsed)
	}
}

func oracleNumericBits(t testing.TB, typ memory.DataType, value any) (uint64, bool) {
	t.Helper()
	if value == nil {
		return 0, false
	}
	s := ""
	switch x := value.(type) {
	case json.Number:
		s = x.String()
	case string:
		s = x
	default:
		t.Fatalf("unexpected oracle scalar %v (%s)", value, reflect.TypeOf(value))
	}
	if typ == memory.TypeFloat32 || typ == memory.TypeFloat64 {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			t.Fatal(err)
		}
		if typ == memory.TypeFloat32 {
			return uint64(math.Float32bits(float32(f))), true
		}
		return math.Float64bits(f), true
	}
	switch typ {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64:
		x, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return uint64(x), true
	default:
		x, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return x, true
	}
}

func TestNumericFloatSignedZeroOrderAndMerge(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeFloat64} {
		encode := func(f float64) uint64 {
			if typ == memory.TypeFloat32 {
				return uint64(math.Float32bits(float32(f)))
			}
			return math.Float64bits(f)
		}
		for _, reverse := range []bool{false, true} {
			zeroes := []float64{0, math.Copysign(0, -1)}
			if reverse {
				zeroes[0], zeroes[1] = zeroes[1], zeroes[0]
			}
			sequential := mustNumeric(t, typ, numericAll)
			left := mustNumeric(t, typ, numericAll)
			right := mustNumeric(t, typ, numericAll)
			for _, x := range zeroes {
				sequential.AddBits(0, encode(x), true)
			}
			left.AddBits(0, encode(zeroes[0]), true)
			right.AddBits(0, encode(zeroes[1]), true)
			if err := left.MergeResult(0, right.Result(0)); err != nil {
				t.Fatal(err)
			}
			for _, state := range []NumericResult{sequential.Result(0), left.Result(0)} {
				minBits, minValid := state.Value(KindMin)
				maxBits, maxValid := state.Value(KindMax)
				if !minValid || !maxValid || !math.Signbit(numericAsFloat(typ, minBits)) || math.Signbit(numericAsFloat(typ, maxBits)) {
					t.Fatalf("type %s reverse=%v zero extrema %+v", typ, reverse, state)
				}
			}
		}
	}
}

func TestNumericFloatingAverageRawTotals(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeFloat64} {
		encode := func(f float64) uint64 {
			if typ == memory.TypeFloat32 {
				return uint64(math.Float32bits(float32(f)))
			}
			return math.Float64bits(f)
		}
		a := mustNumeric(t, typ, numericAll)
		b := mustNumeric(t, typ, numericAll)
		a.AddBits(0, encode(math.Inf(1)), true)
		b.AddBits(0, encode(math.Inf(-1)), true)
		if !math.IsInf(a.Result(0).AvgSum, 1) || !math.IsInf(b.Result(0).AvgSum, -1) {
			t.Fatal("raw average totals lost infinity")
		}
		if err := a.MergeResult(0, b.Result(0)); err != nil {
			t.Fatal(err)
		}
		r := a.Result(0)
		bits, valid := r.Value(KindAvg)
		if !math.IsNaN(r.AvgSum) || !valid || !math.IsNaN(numericAsFloat(typ, bits)) {
			t.Fatal("opposite infinity mean did not propagate NaN")
		}
	}
	f32 := mustNumeric(t, memory.TypeFloat32, numericAll)
	for _, x := range []float32{1 << 24, 1, -(1 << 24)} {
		f32.AddBits(0, uint64(math.Float32bits(x)), true)
	}
	r := f32.Result(0)
	avg, _ := r.Value(KindAvg)
	if math.Float32frombits(uint32(r.SumBits)) != 0 || r.AvgSum != 1 || math.Float32frombits(uint32(avg)) != float32(1.0/3) {
		t.Fatalf("native Float32 SUM vs Float64 mean total %+v", r)
	}
	f64 := mustNumeric(t, memory.TypeFloat64, numericAll)
	f64.AddBits(0, math.Float64bits(math.MaxFloat64), true)
	f64.AddBits(0, math.Float64bits(math.MaxFloat64), true)
	r = f64.Result(0)
	avg, valid := r.Value(KindAvg)
	if !math.IsInf(r.AvgSum, 1) || !valid || !math.IsInf(math.Float64frombits(avg), 1) {
		t.Fatal("documented floating-total overflow changed")
	}
}

func TestNumericBoolAndStringCountsIgnorePayload(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeBool, memory.TypeString} {
		v := mustNumeric(t, typ, Layout{CountRows: true, CountValues: true})
		for i, valid := range []bool{true, false, true, false, true} {
			v.AddBits(0, uint64(i)*math.MaxUint32, valid)
		}
		rows, rv := v.Result(0).Value(KindCountRows)
		values, vv := v.Result(0).Value(KindCountValues)
		if !rv || !vv || rows != 5 || values != 3 {
			t.Fatalf("count-only %s rows=%d values=%d", typ, rows, values)
		}
	}
}
