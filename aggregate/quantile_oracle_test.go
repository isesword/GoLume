package aggregate

import (
	"encoding/json"
	"gocolumn/memory"
	"math"
	"os"
	"sort"
	"strconv"
	"testing"
)

func TestQuantilePolarsOracle(t *testing.T) {
	data, err := os.ReadFile("../testdata/fundamentals/quantile-polars-1.35.2.json")
	if err != nil {
		t.Fatal(err)
	}
	var oracle struct {
		Version string
		Cases   []struct {
			Dtype       string
			Values      []json.RawMessage
			Q           float64
			Mode        string
			Result      json.RawMessage
			OutputDtype string `json:"output_dtype"`
		}
	}
	if err = json.Unmarshal(data, &oracle); err != nil {
		t.Fatal(err)
	}
	if oracle.Version != "1.35.2" {
		t.Fatal(oracle.Version)
	}
	types := map[string]memory.DataType{"Int8": memory.TypeInt8, "Int16": memory.TypeInt16, "Int32": memory.TypeInt32, "Int64": memory.TypeInt64, "UInt8": memory.TypeUint8, "UInt16": memory.TypeUint16, "UInt32": memory.TypeUint32, "UInt64": memory.TypeUint64, "Float32": memory.TypeFloat32, "Float64": memory.TypeFloat64}
	modes := map[string]QuantileMode{"nearest": QuantileNearest, "lower": QuantileLower, "higher": QuantileHigher, "midpoint": QuantileMidpoint, "linear": QuantileLinear, "equiprobable": QuantileEquiprobable, "median": QuantileMidpoint}
	parseFloat := func(raw json.RawMessage) float64 {
		switch string(raw) {
		case `"NaN"`:
			return math.NaN()
		case `"+Inf"`:
			return math.Inf(1)
		case `"-Inf"`:
			return math.Inf(-1)
		}
		v, e := strconv.ParseFloat(string(raw), 64)
		if e != nil {
			t.Fatal(e)
		}
		return v
	}
	for index, c := range oracle.Cases {
		typ := types[c.Dtype]
		var values []uint64
		for _, raw := range c.Values {
			if string(raw) == "null" {
				continue
			}
			var bits uint64
			switch typ {
			case memory.TypeFloat32:
				bits = uint64(math.Float32bits(float32(parseFloat(raw))))
			case memory.TypeFloat64:
				bits = math.Float64bits(parseFloat(raw))
			case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64:
				v, e := strconv.ParseInt(string(raw), 10, 64)
				if e != nil {
					t.Fatal(e)
				}
				bits = uint64(v)
			default:
				v, e := strconv.ParseUint(string(raw), 10, 64)
				if e != nil {
					t.Fatal(e)
				}
				bits = v
			}
			values = append(values, bits)
		}
		sort.SliceStable(values, func(i, j int) bool {
			a, _ := QuantileOrderKey(typ, values[i])
			b, _ := QuantileOrderKey(typ, values[j])
			return a < b
		})
		p, e := PlanQuantile(uint64(len(values)), c.Q, modes[c.Mode])
		if e != nil {
			t.Fatal(e)
		}
		if !p.Valid {
			if string(c.Result) != "null" {
				t.Fatal(index, c)
			}
			continue
		}
		bits, valid, e := InterpolateQuantile(typ, values[p.Lower], values[p.Upper], p)
		if e != nil || !valid {
			t.Fatal(index, e)
		}
		got := math.Float64frombits(bits)
		expectedType := "Float64"
		if typ == memory.TypeFloat32 {
			got = float64(math.Float32frombits(uint32(bits)))
			expectedType = "Float32"
		}
		want := parseFloat(c.Result)
		// Deliberate accuracy improvement: preserve exact opposite-Int64 residual
		// instead of converting both endpoints to Float64 before interpolation.
		if c.Dtype == "Int64" && len(values) == 2 && values[0] == uint64(1)<<63 && values[1] == math.MaxInt64 && p.Lower != p.Upper && p.Weight == .5 {
			want = -.5
		}
		if c.OutputDtype != expectedType {
			t.Fatalf("%d output=%s expected %s", index, c.OutputDtype, expectedType)
		}
		if got == want || math.IsNaN(got) && math.IsNaN(want) {
			continue
		}
		if !math.IsInf(got, 0) && !math.IsInf(want, 0) && math.Abs(got-want) <= 2e-15*math.Max(1, math.Abs(want)) {
			continue
		}
		t.Fatalf("case%d %s q=%g %s got=%g want=%g", index, c.Dtype, c.Q, c.Mode, got, want)
	}
}
