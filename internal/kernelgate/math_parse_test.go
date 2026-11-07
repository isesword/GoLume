package kernelgate

import "testing"

func TestParseMathKernelID(t *testing.T) {
	for _, tc := range []struct{ id, family, typ, op string }{
		{"math-int8-abs", "math-unary", "int8", "abs"},
		{"math-float64-sqrt", "math-unary", "float64", "sqrt"},
		{"math-uint64-floor-div", "math-binary", "uint64", "floor-div"},
	} {
		family, typ, op, err := ParseKernelID(tc.id)
		if err != nil || family != tc.family || typ != tc.typ || op != tc.op || !ValidFamily(family) {
			t.Fatalf("%s => %s %s %s %v", tc.id, family, typ, op, err)
		}
	}
}
