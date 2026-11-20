package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/memory"
	"math"
	"math/bits"
)

// errGlobalFloat64AverageFallback requests replay of this whole lane and batch
// through NumericBundle.AddBits. No snapshot state has been committed.
var errGlobalFloat64AverageFallback = errors.New("global Float64 AVG requires original row arithmetic")

func globalAverageFinite(value float64) bool {
	return math.Float64bits(value)&0x7ff0000000000000 != 0x7ff0000000000000
}

// consumeGlobalFloat64AverageBatch continues complete, independent AVG and SUM
// states in encounter order. It never adds a horizontally reduced batch partial.
// Input shape/validity has already been validated by bindSortedBatch; selectedRows
// is the caller's selected-row count, independently of value validity. The local
// snapshot is committed only after the final cancellation check. Non-finite
// demanded inputs, seeds, or intermediate results return the private fallback
// sentinel without committing any field. Empty valid domains preserve raw seeds.
// The caller must replay every selected row of this lane, not only its suffix.
func consumeGlobalFloat64AverageBatch(ctx context.Context, values []float64, validity, selection memory.Bitmap, selectedRows uint64, state *aggregate.NumericResult) error {
	if ctx == nil {
		return fmt.Errorf("global Float64 AVG context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if state == nil || state.Type != memory.TypeFloat64 || !state.Layout.Avg || state.Layout.Min || state.Layout.Max {
		return fmt.Errorf("global Float64 AVG requires compatible AVG state without extrema")
	}
	words := memory.WordsFor(len(values))
	if len(selection) < words || validity != nil && len(validity) < words {
		return fmt.Errorf("global Float64 AVG bitmap is too short")
	}
	next := *state
	avg := next.AvgSum
	sum := math.Float64frombits(next.SumBits)
	var validValues uint64
	for base := 0; base < len(values); base += 64 {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(base+64, len(values))
		word := selection[base>>6] & memory.TailMask(end-base)
		if validity != nil {
			word &= validity[base>>6]
		}
		if word != 0 && (!globalAverageFinite(avg) || next.Layout.Sum && !globalAverageFinite(sum)) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return errGlobalFloat64AverageFallback
		}
		validValues += uint64(bits.OnesCount64(word))
		if word == memory.TailMask(end-base) {
			for _, value := range values[base:end] {
				if !globalAverageFinite(value) {
					if err := ctx.Err(); err != nil {
						return err
					}
					return errGlobalFloat64AverageFallback
				}
				avg += value
				if next.Layout.Sum {
					sum += value
				}
				if !globalAverageFinite(avg) || next.Layout.Sum && !globalAverageFinite(sum) {
					if err := ctx.Err(); err != nil {
						return err
					}
					return errGlobalFloat64AverageFallback
				}
			}
			continue
		}
		for word != 0 {
			value := values[base+bits.TrailingZeros64(word)]
			if !globalAverageFinite(value) {
				if err := ctx.Err(); err != nil {
					return err
				}
				return errGlobalFloat64AverageFallback
			}
			avg += value
			if next.Layout.Sum {
				sum += value
			}
			if !globalAverageFinite(avg) || next.Layout.Sum && !globalAverageFinite(sum) {
				if err := ctx.Err(); err != nil {
					return err
				}
				return errGlobalFloat64AverageFallback
			}
			word &= word - 1
		}
	}
	// Do not reconstruct empty-domain sum fields: preserve raw -0/NaN seeds.
	if validValues != 0 {
		next.AvgSum = avg
		if next.Layout.Sum {
			next.SumBits = math.Float64bits(sum)
			next.SumValid = true
		}
	}
	if next.Layout.CountRows {
		next.CountRows += selectedRows
	}
	// AVG implicitly requires CountValues, even without a requested COUNT alias.
	next.CountValues += validValues
	if err := ctx.Err(); err != nil {
		return err
	}
	*state = next
	return nil
}

// consumeGlobalFloat64AverageRows replays only the exceptional lane through the
// original updater. NULL rows still count, but their hidden values are not read.
// Cancellation is terminal for the stream, as in the original row path.
func (s *sortedAggregateStream) consumeGlobalFloat64AverageRows(ctx context.Context, lane, rows int) error {
	bound := &s.globalBindings[lane]
	for base := 0; base < rows; base += 64 {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(base+64, rows)
		word := s.selection[base>>6] & memory.TailMask(end-base)
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			valid := bound.validity == nil || bound.validity[base>>6]&(uint64(1)<<bit) != 0
			var valueBits uint64
			if valid {
				valueBits = math.Float64bits(bound.float64s.Values[base+bit])
			}
			s.numeric.AddBits(0, lane, valueBits, valid)
			word &= word - 1
		}
	}
	return ctx.Err()
}
