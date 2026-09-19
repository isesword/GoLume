package kernelgate

import (
	"fmt"
	"strings"
	"testing"
)

// rawBenchOutput renders synthetic "go test -bench" output with one
// line per sample for both dispatch modes of kernel id.
func rawBenchOutput(id string, scalarNs, simdNs []float64) []byte {
	var b strings.Builder
	b.WriteString("goos: darwin\ngoarch: arm64\npkg: gocolumn/kernel\ncpu: Apple M4 Pro\n")
	for _, ns := range scalarNs {
		fmt.Fprintf(&b, "BenchmarkKernelID/%s/mode=scalar-12\t1000\t%.1f ns/op\t0 B/op\t0 allocs/op\n", id, ns)
	}
	for _, ns := range simdNs {
		fmt.Fprintf(&b, "BenchmarkKernelID/%s/mode=simd-12\t4000\t%.1f ns/op\t0 B/op\t0 allocs/op\n", id, ns)
	}
	b.WriteString("PASS\nok  \tgocolumn/kernel\t5.0s\n")
	return []byte(b.String())
}

func spread(base float64, n int) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = base + float64(i%3)*base*0.01
	}
	return out
}

var testCriteria = Criteria{
	MinimumSamples:            10,
	Benchtime:                 "1s",
	MinimumImprovementPercent: 10,
	MaximumPValue:             0.05,
}

func TestJudgePass(t *testing.T) {
	raw := rawBenchOutput("compare-int32-gt", spread(70000, 10), spread(20000, 10))
	dec, err := Judge(raw, "compare-int32-gt", testCriteria)
	if err != nil {
		t.Fatal(err)
	}
	if !dec.Pass {
		t.Fatalf("expected pass, got note %q", dec.Note)
	}
	if dec.ScalarSamples != 10 || dec.SIMDSamples != 10 {
		t.Errorf("samples = %d+%d", dec.ScalarSamples, dec.SIMDSamples)
	}
	if dec.ImprovementPercent < 70 || dec.ImprovementPercent > 72 {
		t.Errorf("improvement = %.2f%%, want ~71%%", dec.ImprovementPercent)
	}
	if dec.PValue > 0.05 {
		t.Errorf("p-value = %v, want significant", dec.PValue)
	}
}

func TestJudgeReject(t *testing.T) {
	cases := []struct {
		name     string
		scalarNs []float64
		simdNs   []float64
		wantNote string
	}{
		{"insufficient improvement", spread(70000, 10), spread(68000, 10), "below minimum"},
		{"too few samples", spread(70000, 5), spread(20000, 5), "too few samples"},
	}
	for _, tc := range cases {
		raw := rawBenchOutput("compare-int32-gt", tc.scalarNs, tc.simdNs)
		dec, err := Judge(raw, "compare-int32-gt", testCriteria)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if dec.Pass {
			t.Errorf("%s: unexpected pass", tc.name)
		}
		if !strings.Contains(dec.Note, tc.wantNote) {
			t.Errorf("%s: note %q, want substring %q", tc.name, dec.Note, tc.wantNote)
		}
	}
}

func TestJudgeMissingSamples(t *testing.T) {
	raw := rawBenchOutput("compare-int32-gt", spread(70000, 10), nil)
	if _, err := Judge(raw, "compare-int32-gt", testCriteria); err == nil {
		t.Error("missing simd samples: expected error")
	}
	raw = rawBenchOutput("compare-int32-gt", spread(70000, 10), spread(20000, 10))
	if _, err := Judge(raw, "compare-int64-gt", testCriteria); err == nil {
		t.Error("wrong kernel id: expected error")
	}
	if _, err := Judge([]byte("BenchmarkBroken\n"), "compare-int32-gt", testCriteria); err == nil {
		t.Error("malformed output: expected error")
	}
}

func TestJudgeIgnoresOtherKernels(t *testing.T) {
	var b strings.Builder
	b.Write(rawBenchOutput("compare-int32-gt", spread(70000, 10), spread(20000, 10)))
	b.Write(rawBenchOutput("compare-int64-gt", spread(100, 10), spread(100000, 10)))
	dec, err := Judge([]byte(b.String()), "compare-int32-gt", testCriteria)
	if err != nil {
		t.Fatal(err)
	}
	if !dec.Pass {
		t.Errorf("expected pass despite unrelated slow kernel, note %q", dec.Note)
	}
	if dec.ScalarMeanNs < 69000 || dec.ScalarMeanNs > 72000 {
		t.Errorf("scalar mean = %.0f, contamination from other kernel", dec.ScalarMeanNs)
	}
}
