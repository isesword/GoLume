package platformcal

import (
	"fmt"
	"sort"
)

// Decision is the outcome of interpreting one measurement table: the chosen
// value, whether it is backed by evidence, and the reason when it is not.
type Decision struct {
	// Value is the chosen constant; it equals the fallback when Calibrated is
	// false.
	Value int64
	// Calibrated reports whether Value is backed by measurements.
	Calibrated bool
	// Reason explains an uncalibrated decision.
	Reason string
}

// CrossoverPoint pairs a serial and a parallel timing at one problem size.
type CrossoverPoint struct {
	// Size is the problem size (selected rows, distinct keys, build rows).
	Size int64
	// SerialNs is the median serial time in nanoseconds.
	SerialNs float64
	// ParallelNs is the median parallel time in nanoseconds.
	ParallelNs float64
}

// DecideCrossover picks the smallest problem size at which the parallel
// strategy beats the serial one by at least minGain (a fraction of the serial
// time, e.g. 0.10) and keeps beating it at every larger measured size. The
// suffix rule protects the decision against a single noisy point: a crossover
// that later flips back is not evidence. Without such a size the decision
// falls back uncalibrated.
func DecideCrossover(points []CrossoverPoint, minGain float64, fallback int64) Decision {
	if len(points) == 0 {
		return Decision{Value: fallback, Reason: "no measurements"}
	}
	sorted := append([]CrossoverPoint(nil), points...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Size < sorted[j].Size })
	wins := make([]bool, len(sorted))
	for i, p := range sorted {
		wins[i] = parallelWins(p.SerialNs, p.ParallelNs, minGain)
	}
	suffix := true
	for i := len(sorted) - 1; i >= 0; i-- {
		suffix = suffix && wins[i]
		if suffix {
			return Decision{Value: sorted[i].Size, Calibrated: true}
		}
	}
	return Decision{Value: fallback, Reason: fmt.Sprintf("parallel strategy never beat serial by %.0f%% at every larger size", minGain*100)}
}

func parallelWins(serialNs, parallelNs, minGain float64) bool {
	if serialNs <= 0 || parallelNs <= 0 {
		return false
	}
	return (serialNs-parallelNs)/serialNs >= minGain
}

// ScalingPoint is one worker-count throughput measurement.
type ScalingPoint struct {
	// Workers is the measured worker count.
	Workers int
	// Ns is the median wall time of the workload at Workers.
	Ns float64
}

// DecideWorkerKnee picks the largest worker count whose marginal speedup over
// the previous measured count still clears minGain: past the knee, extra
// workers no longer pay for their scheduling cost. The knee never moves
// backwards, so a plateau followed by another gain still reports the highest
// paying count. An empty table falls back uncalibrated; a table where no
// added worker ever paid reports the first (smallest) measured count as
// calibrated evidence against parallelism.
func DecideWorkerKnee(points []ScalingPoint, minGain float64, fallback int) Decision {
	usable := make([]ScalingPoint, 0, len(points))
	for _, p := range points {
		if p.Workers >= 1 && p.Ns > 0 {
			usable = append(usable, p)
		}
	}
	if len(usable) == 0 {
		return Decision{Value: int64(fallback), Reason: "no measurements"}
	}
	sort.Slice(usable, func(i, j int) bool { return usable[i].Workers < usable[j].Workers })
	knee := usable[0].Workers
	for i := 1; i < len(usable); i++ {
		gain := usable[i-1].Ns/usable[i].Ns - 1
		if gain >= minGain {
			knee = usable[i].Workers
		}
	}
	return Decision{Value: int64(knee), Calibrated: true}
}
