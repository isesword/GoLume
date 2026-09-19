package kernelgate

import (
	"bytes"
	"fmt"
	"strings"

	"golang.org/x/perf/benchmath"

	"gocolumn/internal/benchtrack"
)

// Decision is the statistical verdict of one gate run for one kernel.
type Decision struct {
	KernelID           string
	Pass               bool
	ScalarMeanNs       float64
	SIMDMeanNs         float64
	ScalarSamples      int
	SIMDSamples        int
	ImprovementPercent float64
	PValue             float64
	// Note explains a failure that is not a plain threshold miss: too
	// few samples, statistical warnings, missing benchmark rows, and
	// so on.
	Note string
}

// modeMarker locates the "/mode=scalar" or "/mode=simd" element that
// BenchmarkKernelID appends to every sub-benchmark name.
const modeMarker = "/mode="

// Judge applies the gate criteria to one kernel's raw benchmark output.
// The output must contain both mode=scalar and mode=simd sub-benchmarks
// for the target kernel, produced for example by
//
//	GOEXPERIMENT=simd go test ./kernel -run '^$' \
//	    -bench '^BenchmarkKernelID/<id>/' -benchtime 1s -count 10 -benchmem
//
// The significance test is the Mann-Whitney U test from
// golang.org/x/perf/benchmath — the same test the benchstat tool
// applies to the archived CSV evidence.
func Judge(raw []byte, kernelID string, criteria Criteria) (Decision, error) {
	dec := Decision{KernelID: kernelID}
	parsed, err := benchtrack.ParseGoTestOutput(bytes.NewReader(raw))
	if err != nil {
		return dec, err
	}
	prefix := "BenchmarkKernelID/" + kernelID + modeMarker
	var scalarValues, simdValues []float64
	for _, pb := range parsed {
		if !strings.HasPrefix(pb.Name, prefix) {
			continue
		}
		switch mode := pb.Name[len(prefix):]; {
		case strings.HasPrefix(mode, "scalar"):
			scalarValues = append(scalarValues, pb.NsPerOp)
		case strings.HasPrefix(mode, "simd"):
			simdValues = append(simdValues, pb.NsPerOp)
		}
	}
	dec.ScalarSamples, dec.SIMDSamples = len(scalarValues), len(simdValues)
	if dec.ScalarSamples == 0 || dec.SIMDSamples == 0 {
		return dec, fmt.Errorf("kernelgate: raw benchmark output lacks mode=scalar or mode=simd samples for %q", kernelID)
	}
	dec.ScalarMeanNs = mean(scalarValues)
	dec.SIMDMeanNs = mean(simdValues)
	if dec.ScalarSamples < criteria.MinimumSamples || dec.SIMDSamples < criteria.MinimumSamples {
		dec.Note = fmt.Sprintf("too few samples: scalar=%d simd=%d, want >= %d",
			dec.ScalarSamples, dec.SIMDSamples, criteria.MinimumSamples)
		return dec, nil
	}
	if dec.ScalarMeanNs <= 0 {
		dec.Note = "scalar mean is zero"
		return dec, nil
	}
	dec.ImprovementPercent = (dec.ScalarMeanNs - dec.SIMDMeanNs) / dec.ScalarMeanNs * 100

	thresholds := benchmath.DefaultThresholds
	thresholds.CompareAlpha = criteria.MaximumPValue
	oldSample := benchmath.NewSample(append([]float64(nil), scalarValues...), &thresholds)
	newSample := benchmath.NewSample(append([]float64(nil), simdValues...), &thresholds)
	comparison := benchmath.AssumeNothing.Compare(oldSample, newSample)
	dec.PValue = comparison.P
	var warnings []string
	for _, w := range comparison.Warnings {
		warnings = append(warnings, w.Error())
	}

	switch {
	case dec.ImprovementPercent < criteria.MinimumImprovementPercent:
		dec.Note = fmt.Sprintf("improvement %.2f%% below minimum %.2f%%",
			dec.ImprovementPercent, criteria.MinimumImprovementPercent)
	case dec.PValue > criteria.MaximumPValue:
		dec.Note = fmt.Sprintf("p-value %.4f above maximum %.4f", dec.PValue, criteria.MaximumPValue)
	default:
		dec.Pass = true
	}
	if len(warnings) > 0 {
		if dec.Note != "" {
			dec.Note += "; "
		}
		dec.Note += "warnings: " + strings.Join(warnings, "; ")
	}
	return dec, nil
}

func mean(values []float64) float64 {
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}
