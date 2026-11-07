package aggregate

import (
	"encoding/json"
	"gocolumn/memory"
	"math"
	"math/big"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
)

func TestMomentsPinnedPolarsAccuracyDifferences(t *testing.T) {
	data, err := os.ReadFile("../testdata/fundamentals/statistics-polars-1.35.2.json")
	if err != nil {
		t.Fatal(err)
	}
	var oracle struct {
		Version string `json:"version"`
		Cases   []struct {
			ID      string                     `json:"id"`
			DType   string                     `json:"dtype"`
			Input   []json.RawMessage          `json:"input"`
			Results map[string]json.RawMessage `json:"results"`
		} `json:"cases"`
	}
	if err = json.Unmarshal(data, &oracle); err != nil {
		t.Fatal(err)
	}
	if oracle.Version != "1.35.2" || len(oracle.Cases) != 7 {
		t.Fatal("unexpected oracle")
	}
	for _, tc := range oracle.Cases {
		t.Run(tc.ID, func(t *testing.T) {
			typ := memory.TypeFloat64
			if tc.DType == "Int64" {
				typ = memory.TypeInt64
			}
			if tc.DType == "UInt64" {
				typ = memory.TypeUint64
			}
			s, _ := NewMomentsState(typ)
			for _, raw := range tc.Input {
				var bits uint64
				switch typ {
				case memory.TypeInt64:
					v, e := strconv.ParseInt(string(raw), 10, 64)
					if e != nil {
						t.Fatal(e)
					}
					bits = uint64(v)
				case memory.TypeUint64:
					v, e := strconv.ParseUint(string(raw), 10, 64)
					if e != nil {
						t.Fatal(e)
					}
					bits = v
				default:
					var v float64
					switch string(raw) {
					case `"NaN"`:
						v = math.NaN()
					case `"+Inf"`:
						v = math.Inf(1)
					default:
						if e := json.Unmarshal(raw, &v); e != nil {
							t.Fatal(e)
						}
					}
					bits = math.Float64bits(v)
				}
				if e := s.AddBits(bits, true); e != nil {
					t.Fatal(e)
				}
			}
			switch tc.ID {
			case "finite_variance_overflowing_m2":
				if string(tc.Results["var_ddof0"]) != `"+Inf"` {
					t.Fatal("reference changed")
				}
				v, _ := s.Variance(0)
				requireMomentClose(t, v, 1e308, 3e-15)
				v, _ = s.StdDev(0)
				requireMomentClose(t, v, 1e154, 3e-15)
			case "finite_std_overflowing_variance":
				if string(tc.Results["std_ddof0"]) != `"+Inf"` {
					t.Fatal("reference changed")
				}
				v, _ := s.Variance(0)
				if !math.IsInf(v, 1) {
					t.Fatal(v)
				}
				v, _ = s.StdDev(0)
				requireMomentClose(t, v, math.MaxFloat64, 3e-15)
			case "int64_unit_differences", "uint64_unit_differences":
				var reference float64
				if err := json.Unmarshal(tc.Results["var_ddof1"], &reference); err != nil || reference != 0 {
					t.Fatal("reference changed")
				}
				v, _ := s.Variance(1)
				requireMomentClose(t, v, 1, 2e-15)
				v, _ = s.Variance(0)
				requireMomentClose(t, v, 2.0/3, 2e-15)
			default:
				for d := uint64(0); d < 3; d++ {
					v, ok := s.Variance(d)
					observed := string(tc.Results["var_ddof"+strconv.Itoa(int(d))])
					if observed == "null" {
						if ok {
							t.Fatal("expected insufficient-sample NULL")
						}
					} else if observed != `"NaN"` || !ok || !math.IsNaN(v) {
						t.Fatalf("got %g/%v reference%s", v, ok, observed)
					}
				}
			}
		})
	}
}

func testMoments(t *testing.T, typ memory.DataType, bits []uint64) MomentsState {
	t.Helper()
	s, err := NewMomentsState(typ)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range bits {
		if err := s.AddBits(b, true); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func requireMomentClose(t *testing.T, got, want float64, relative float64) {
	t.Helper()
	if math.IsNaN(want) {
		if !math.IsNaN(got) {
			t.Fatalf("got %g want NaN", got)
		}
		return
	}
	if math.IsInf(want, 0) {
		if got != want {
			t.Fatalf("got %g want %g", got, want)
		}
		return
	}
	if math.IsNaN(got) || math.IsInf(got, 0) || math.Abs(got-want) > relative*math.Max(math.Abs(want), math.SmallestNonzeroFloat64) {
		t.Fatalf("got %.17g want %.17g", got, want)
	}
}

func exactIntegerVariance(typ memory.DataType, bits []uint64, ddof int) float64 {
	sum, squares := new(big.Rat), new(big.Rat)
	for _, b := range bits {
		v := new(big.Int)
		if typ == memory.TypeInt64 {
			v.SetInt64(int64(b))
		} else {
			v.SetUint64(b)
		}
		x := new(big.Rat).SetInt(v)
		sum.Add(sum, x)
		squares.Add(squares, new(big.Rat).Mul(x, x))
	}
	n := big.NewRat(int64(len(bits)), 1)
	center := new(big.Rat).Quo(new(big.Rat).Mul(sum, sum), n)
	squares.Sub(squares, center)
	squares.Quo(squares, big.NewRat(int64(len(bits)-ddof), 1))
	f, _ := squares.Float64()
	return f
}

func TestMomentsNullDDoFAndNonFinite(t *testing.T) {
	s, _ := NewMomentsState(memory.TypeFloat64)
	for _, d := range []uint64{0, 1, 2, math.MaxUint64} {
		if _, valid := s.Variance(d); valid {
			t.Fatal("empty variance valid")
		}
		if _, valid := s.StdDev(d); valid {
			t.Fatal("empty std valid")
		}
	}
	if err := s.AddBits(math.Float64bits(math.NaN()), false); err != nil {
		t.Fatal(err)
	}
	if s.Snapshot().Count != 0 {
		t.Fatal("null incremented count")
	}
	_ = s.AddBits(math.Float64bits(1), true)
	if v, ok := s.Variance(0); !ok || v != 0 {
		t.Fatalf("singleton %g %v", v, ok)
	}
	if _, ok := s.Variance(1); ok {
		t.Fatal("singleton sample valid")
	}
	_ = s.AddBits(math.Float64bits(3), true)
	for d, expected := range []float64{1, 2} {
		v, ok := s.Variance(uint64(d))
		if !ok {
			t.Fatal("missing variance")
		}
		requireMomentClose(t, v, expected, 1e-15)
		v, ok = s.StdDev(uint64(d))
		if !ok {
			t.Fatal("missing std")
		}
		requireMomentClose(t, v, math.Sqrt(expected), 1e-15)
	}
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		s.Reset()
		_ = s.AddBits(math.Float64bits(value), true)
		v, ok := s.Variance(0)
		if !ok || !math.IsNaN(v) {
			t.Fatalf("nonfinite singleton %g %v", v, ok)
		}
		if _, ok := s.Variance(1); ok {
			t.Fatal("ddof must precede poison")
		}
		if _, err := RestoreMoments(s.Snapshot()); err != nil {
			t.Fatal(err)
		}
		other := testMoments(t, memory.TypeFloat64, []uint64{math.Float64bits(4)})
		if err := other.Merge(s); err != nil {
			t.Fatal(err)
		}
		if v, ok := other.StdDev(1); !ok || !math.IsNaN(v) {
			t.Fatal("merge lost nonfinite")
		}
	}
	s.Reset()
	if s.Snapshot() != (MomentsResult{Type: memory.TypeFloat64}) {
		t.Fatal("reset retained payload")
	}
}

func TestMomentsExactLargeIntegersAndMerge(t *testing.T) {
	for _, tc := range []struct {
		typ    memory.DataType
		values []uint64
	}{
		{memory.TypeInt64, []uint64{math.MaxInt64, math.MaxInt64 - 1, math.MaxInt64 - 2}},
		{memory.TypeInt64, []uint64{uint64(1) << 63, uint64(1)<<63 + 1, uint64(1)<<63 + 2}},
		{memory.TypeUint64, []uint64{math.MaxUint64, math.MaxUint64 - 1, math.MaxUint64 - 2}},
		{memory.TypeInt64, []uint64{uint64(1) << 63, math.MaxInt64, 0}},
		{memory.TypeUint64, []uint64{0, math.MaxUint64, 1}},
	} {
		for split := 0; split <= len(tc.values); split++ {
			a := testMoments(t, tc.typ, tc.values[:split])
			b := testMoments(t, tc.typ, tc.values[split:])
			if err := a.Merge(b); err != nil {
				t.Fatal(err)
			}
			for d := 0; d < 2; d++ {
				v, ok := a.Variance(uint64(d))
				if !ok {
					t.Fatal("invalid")
				}
				requireMomentClose(t, v, exactIntegerVariance(tc.typ, tc.values, d), 2e-15)
			}
			if _, err := RestoreMoments(a.Snapshot()); err != nil {
				t.Fatal(err)
			}
		}
	}
	rng := rand.New(rand.NewSource(9907))
	for _, typ := range []memory.DataType{memory.TypeInt64, memory.TypeUint64} {
		for trial := 0; trial < 200; trial++ {
			values := make([]uint64, 31)
			for i := range values {
				if trial%2 == 0 {
					values[i] = math.MaxUint64 - uint64(rng.Intn(10000))
				} else {
					values[i] = rng.Uint64()
				}
			}
			states := make([]MomentsState, 7)
			for i := range states {
				states[i], _ = NewMomentsState(typ)
			}
			for i, x := range values {
				_ = states[i%7].AddBits(x, true)
			}
			rng.Shuffle(len(states), func(i, j int) { states[i], states[j] = states[j], states[i] })
			for i := 1; i < len(states); i++ {
				if err := states[0].Merge(states[i]); err != nil {
					t.Fatal(err)
				}
			}
			v, _ := states[0].Variance(1)
			requireMomentClose(t, v, exactIntegerVariance(typ, values, 1), 4e-14)
		}
	}
}

func TestMomentsNativeTypesAndScaledExtremes(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64} {
		bits := []uint64{1, 3}
		if typ == memory.TypeFloat32 {
			bits = []uint64{uint64(math.Float32bits(1)), uint64(math.Float32bits(3))}
		}
		if typ == memory.TypeFloat64 {
			bits = []uint64{math.Float64bits(1), math.Float64bits(3)}
		}
		s := testMoments(t, typ, bits)
		v, _ := s.Variance(1)
		requireMomentClose(t, v, 2, 1e-15)
	}
	for _, magnitude := range []float64{1e154, math.MaxFloat64, 1e-150, 1e-300} {
		a := testMoments(t, memory.TypeFloat64, []uint64{math.Float64bits(magnitude), math.Float64bits(-magnitude)})
		b := testMoments(t, memory.TypeFloat64, []uint64{math.Float64bits(magnitude)})
		c := testMoments(t, memory.TypeFloat64, []uint64{math.Float64bits(-magnitude)})
		if err := b.Merge(c); err != nil {
			t.Fatal(err)
		}
		for _, s := range []MomentsState{a, b} {
			v, _ := s.Variance(0)
			requireMomentClose(t, v, magnitude*magnitude, 3e-15)
			std, _ := s.StdDev(0)
			requireMomentClose(t, std, magnitude, 3e-15)
			if _, err := RestoreMoments(s.Snapshot()); err != nil {
				t.Fatal(err)
			}
		}
	}
	s := testMoments(t, memory.TypeFloat64, []uint64{math.Float64bits(1e16), math.Float64bits(1e16 + 2), math.Float64bits(1e16 + 4)})
	if v, _ := s.Variance(1); v != 4 {
		t.Fatalf("centered float variance=%g", v)
	}
}

func TestMomentsValidationAndAtomicErrors(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInvalid, memory.TypeBool, memory.TypeString} {
		if _, err := NewMomentsState(typ); err == nil {
			t.Fatal("accepted nonnumeric")
		}
	}
	base := testMoments(t, memory.TypeInt64, []uint64{1, 3}).Snapshot()
	for _, mutate := range []func(*MomentsResult){
		func(r *MomentsResult) { r.Count = 0 }, func(r *MomentsResult) { r.Scale = -1 }, func(r *MomentsResult) { r.ScaledM2 = -1 }, func(r *MomentsResult) { r.Mean = math.NaN() }, func(r *MomentsResult) { r.Scale = math.Inf(1) }, func(r *MomentsResult) { r.Uncentered = true }, func(r *MomentsResult) { r.Scale = 0 },
	} {
		r := base
		mutate(&r)
		if _, err := RestoreMoments(r); err == nil {
			t.Fatalf("accepted %+v", r)
		}
	}
	s := testMoments(t, memory.TypeInt64, []uint64{1})
	s.result.Count = math.MaxUint64
	before := s.Snapshot()
	if err := s.AddBits(2, true); err == nil || s.Snapshot() != before {
		t.Fatal("count overflow mutated state")
	}
	o := testMoments(t, memory.TypeInt64, []uint64{3})
	if err := s.Merge(o); err == nil || s.Snapshot() != before {
		t.Fatal("merge overflow mutated state")
	}
	o, _ = NewMomentsState(memory.TypeUint64)
	if err := s.Merge(o); err == nil || s.Snapshot() != before {
		t.Fatal("type mismatch mutated state")
	}
}

func TestMomentsFloatExactRationalDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(44123))
	for trial := 0; trial < 300; trial++ {
		exponent := -1074 + rng.Intn(2048)
		values := make([]float64, 19)
		sum, squares := new(big.Rat), new(big.Rat)
		states := make([]MomentsState, 5)
		for i := range states {
			states[i], _ = NewMomentsState(memory.TypeFloat64)
		}
		for i := range values {
			values[i] = math.Ldexp(float64(rng.Intn(33)-16), exponent)
			x := new(big.Rat).SetFloat64(values[i])
			sum.Add(sum, x)
			squares.Add(squares, new(big.Rat).Mul(x, x))
			if err := states[i%len(states)].AddBits(math.Float64bits(values[i]), true); err != nil {
				t.Fatal(err)
			}
		}
		squares.Sub(squares, new(big.Rat).Quo(new(big.Rat).Mul(sum, sum), big.NewRat(int64(len(values)), 1)))
		squares.Quo(squares, big.NewRat(int64(len(values)-1), 1))
		wantVar, _ := squares.Float64()
		high := new(big.Float).SetPrec(256).SetRat(squares)
		wantStd, _ := high.Sqrt(high).Float64()
		rng.Shuffle(len(states), func(i, j int) { states[i], states[j] = states[j], states[i] })
		for i := 1; i < len(states); i++ {
			if err := states[0].Merge(states[i]); err != nil {
				t.Fatal(err)
			}
		}
		gotVar, _ := states[0].Variance(1)
		gotStd, _ := states[0].StdDev(1)
		for _, pair := range [][2]float64{{gotVar, wantVar}, {gotStd, wantStd}} {
			if math.IsInf(pair[1], 0) {
				if pair[0] != pair[1] {
					t.Fatalf("trial%d got%g want%g", trial, pair[0], pair[1])
				}
				continue
			}
			if math.IsNaN(pair[0]) || math.Abs(pair[0]-pair[1]) > math.Abs(pair[1])*2e-12+4*math.SmallestNonzeroFloat64 {
				t.Fatalf("trial%d exponent%d got%.17g want%.17g", trial, exponent, pair[0], pair[1])
			}
		}
	}
}

func TestEndpointsLogicalOrderNullAndMerge(t *testing.T) {
	s, _ := NewEndpointState(memory.TypeUint64)
	rows := []struct {
		bits    uint64
		valid   bool
		ordinal SourceOrdinal
	}{
		{math.MaxUint64, true, SourceOrdinal{2, 5}}, {0, false, SourceOrdinal{0, 8}}, {7, true, SourceOrdinal{1, 0}}, {0, false, SourceOrdinal{4, 1}},
	}
	for _, i := range []int{2, 0, 3, 1} {
		r := rows[i]
		if err := s.AddBits(r.bits, r.valid, r.ordinal); err != nil {
			t.Fatal(err)
		}
	}
	if _, valid := s.First(); valid {
		t.Fatal("first skipped NULL")
	}
	if _, valid := s.Last(); valid {
		t.Fatal("last skipped NULL")
	}
	a, _ := NewEndpointState(memory.TypeUint64)
	b, _ := NewEndpointState(memory.TypeUint64)
	for i, r := range rows {
		target := &a
		if i%2 == 1 {
			target = &b
		}
		_ = target.AddBits(r.bits, r.valid, r.ordinal)
	}
	if err := b.Merge(a); err != nil {
		t.Fatal(err)
	}
	if b.Snapshot() != s.Snapshot() {
		t.Fatal("merge arrival changed endpoints")
	}
	if _, err := RestoreEndpoints(s.Snapshot()); err != nil {
		t.Fatal(err)
	}
	before := s.Snapshot()
	if err := s.AddBits(3, true, SourceOrdinal{0, 8}); err == nil || s.Snapshot() != before {
		t.Fatal("conflicting ordinal accepted")
	}
	s.Reset()
	if s.Snapshot() != (EndpointResult{Type: memory.TypeUint64}) {
		t.Fatal("reset retained state")
	}
	_ = s.AddBits(math.MaxUint64, true, SourceOrdinal{0, 0})
	if bits, valid := s.First(); !valid || bits != math.MaxUint64 {
		t.Fatal("lost native UInt64 bits")
	}
	if err := s.Merge(s); err != nil {
		t.Fatal(err)
	}
}

func TestEndpointsNativeBitsAndValidation(t *testing.T) {
	for _, tc := range []struct {
		typ        memory.DataType
		bits, want uint64
	}{
		{memory.TypeInt8, 255, math.MaxUint64}, {memory.TypeDate32, math.MaxUint32, math.MaxUint64}, {memory.TypeFloat32, 0x80000000, 0x80000000}, {memory.TypeFloat64, math.Float64bits(math.NaN()), math.Float64bits(math.NaN())}, {memory.TypeBool, 1, 1},
	} {
		s, err := NewEndpointState(tc.typ)
		if err != nil {
			t.Fatal(err)
		}
		if err = s.AddBits(tc.bits, true, SourceOrdinal{3, 4}); err != nil {
			t.Fatal(err)
		}
		if bits, ok := s.First(); !ok || bits != tc.want {
			t.Fatalf("%s: %x", tc.typ, bits)
		}
	}
	for _, typ := range []memory.DataType{memory.TypeString, memory.TypeBinary, memory.TypeDecimal, memory.TypeInvalid} {
		if _, err := NewEndpointState(typ); err == nil {
			t.Fatal("accepted variable or unsupported type")
		}
	}
	s, _ := NewEndpointState(memory.TypeBool)
	if err := s.AddBits(2, true, SourceOrdinal{}); err == nil {
		t.Fatal("invalid bool")
	}
	s, _ = NewEndpointState(memory.TypeInt64)
	_ = s.AddBits(2, true, SourceOrdinal{0, 5})
	base := s.Snapshot()
	for _, mutate := range []func(*EndpointResult){func(r *EndpointResult) { r.Seen = false }, func(r *EndpointResult) { r.LastOrdinal.Row = 4 }, func(r *EndpointResult) { r.LastBits = 3 }, func(r *EndpointResult) { r.FirstValid = false }} {
		r := base
		mutate(&r)
		if _, err := RestoreEndpoints(r); err == nil {
			t.Fatalf("accepted %+v", r)
		}
	}
	before := s.Snapshot()
	o, _ := NewEndpointState(memory.TypeUint64)
	if err := s.Merge(o); err == nil || !reflect.DeepEqual(before, s.Snapshot()) {
		t.Fatal("mismatch changed endpoint")
	}
}

func BenchmarkMomentsInt64Anchored(b *testing.B) {
	s, _ := NewMomentsState(memory.TypeInt64)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.Reset()
		for j := 0; j < 4096; j++ {
			_ = s.AddBits(uint64(math.MaxInt64-j%17), true)
		}
	}
}
