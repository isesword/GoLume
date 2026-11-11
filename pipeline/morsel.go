package pipeline

import (
	"fmt"
	"gocolumn/memory"
	"math"
)

// MorselRange is one contiguous output slice and its pre-allocation byte cost.
type MorselRange struct {
	Offset int
	Rows   int
	Bytes  int64
}

// MorselTooLargeError reports one row that cannot fit the configured hard
// morsel limit. Row is relative to the current Batch.
type MorselTooLargeError struct {
	Row        int
	Bytes      int64
	LimitBytes int64
}

// Error describes the oversized row and configured byte limit.
func (e *MorselTooLargeError) Error() string {
	if e == nil {
		return "morsel exceeds hard byte limit"
	}
	return fmt.Sprintf("row %d requires %d bytes, exceeding max morsel bytes %d", e.Row, e.Bytes, e.LimitBytes)
}

// ByteSizer combines schema width with an exponentially weighted history of
// actual Batch bytes. It is query-local and not safe for concurrent use.
type ByteSizer struct {
	opts        Options
	bytesPerRow float64
	estimate    func(*memory.Batch, int, int) (int64, error)
	owned       bool
}

// NewByteSizer constructs an adaptive byte-first morsel sizer.
func NewByteSizer(schema memory.Schema, opts Options) (*ByteSizer, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return &ByteSizer{opts: opts, bytesPerRow: float64(EstimateSchemaRowBytes(schema)), estimate: memory.EstimateBatchRangeBytes}, nil
}

// newOwnedByteSizer is reserved for fan-out copies. Public sizing and borrowed
// partition handoff continue to use logical payload bytes.
func newOwnedByteSizer(schema memory.Schema, opts Options) (*ByteSizer, error) {
	s, err := NewByteSizer(schema, opts)
	if err != nil {
		return nil, err
	}
	s.estimate = nativeOwnedRangeBytes
	s.owned = true
	return s, nil
}

// nativeOwnedRangeBytes combines native copy admission with unchanged legacy
// Opaque logical accounting. The latter is explicitly not a retained-size bound.
func nativeOwnedRangeBytes(batch *memory.Batch, offset, rows int) (int64, error) {
	estimate, err := memory.EstimateNativeOwnedBatchRangeBytes(batch, offset, rows)
	return estimate.Bytes, err
}

// ObservedBytesPerRow returns the current schema/history width estimate.
func (s *ByteSizer) ObservedBytesPerRow() float64 {
	if s == nil {
		return 0
	}
	return s.bytesPerRow
}

// Split returns byte-targeted contiguous ranges. The minimum row count is a
// soft target; target and minimum may be exceeded only up to MaxMorselBytes.
func (s *ByteSizer) Split(batch *memory.Batch) ([]MorselRange, error) {
	if s == nil {
		return nil, fmt.Errorf("morsel sizer is nil")
	}
	if batch == nil {
		return nil, fmt.Errorf("cannot split nil batch")
	}
	if batch.Rows == 0 {
		return nil, nil
	}
	fullBytes, err := s.estimate(batch, 0, batch.Rows)
	if err != nil {
		return nil, err
	}
	actualWidth := float64(fullBytes) / float64(batch.Rows)
	if actualWidth > 0 {
		s.bytesPerRow = s.bytesPerRow*0.75 + actualWidth*0.25
	}
	// The first estimate already validates the entire input. Reuse it only
	// when the original target/minimum rules prove that one whole-batch range
	// is the exact result. Never apply this shortcut to public logical sizing.
	if s.owned && batch.Rows <= s.opts.MaxMorselRows &&
		(fullBytes <= s.opts.TargetMorselBytes ||
			(batch.Rows <= s.opts.MinMorselRows && fullBytes <= s.opts.MaxMorselBytes)) {
		if fullBytes == 0 {
			fullBytes = 1
		}
		return []MorselRange{{Rows: batch.Rows, Bytes: fullBytes}}, nil
	}
	ranges := make([]MorselRange, 0, batch.Rows/s.opts.MinMorselRows+1)
	for offset := 0; offset < batch.Rows; {
		maxEnd := offset + s.opts.MaxMorselRows
		if maxEnd > batch.Rows {
			maxEnd = batch.Rows
		}
		guessRows := int(float64(s.opts.TargetMorselBytes) / math.Max(s.bytesPerRow, 1))
		if guessRows < 1 {
			guessRows = 1
		}
		if guessRows > s.opts.MaxMorselRows {
			guessRows = s.opts.MaxMorselRows
		}
		targetEnd, targetBytes, err := s.largestEndWithin(batch, offset, maxEnd, offset+guessRows, s.opts.TargetMorselBytes)
		if err != nil {
			return nil, err
		}
		minEnd := offset + s.opts.MinMorselRows
		if minEnd > maxEnd {
			minEnd = maxEnd
		}
		end, bytes := targetEnd, targetBytes
		if end < minEnd {
			minBytes, err := s.estimate(batch, offset, minEnd-offset)
			if err != nil {
				return nil, err
			}
			if minBytes <= s.opts.MaxMorselBytes {
				end, bytes = minEnd, minBytes
			} else {
				end, bytes, err = s.largestEndWithin(batch, offset, minEnd, offset+1, s.opts.MaxMorselBytes)
				if err != nil {
					return nil, err
				}
			}
		}
		if end == offset {
			one, err := s.estimate(batch, offset, 1)
			if err != nil {
				return nil, err
			}
			return nil, &MorselTooLargeError{Row: offset, Bytes: one, LimitBytes: s.opts.MaxMorselBytes}
		}
		if bytes > s.opts.MaxMorselBytes {
			return nil, &MorselTooLargeError{Row: offset, Bytes: bytes, LimitBytes: s.opts.MaxMorselBytes}
		}
		if bytes == 0 {
			bytes = 1
		}
		ranges = append(ranges, MorselRange{Offset: offset, Rows: end - offset, Bytes: bytes})
		offset = end
	}
	return ranges, nil
}

func (s *ByteSizer) largestEndWithin(batch *memory.Batch, offset, maxEnd, guessedEnd int, limit int64) (int, int64, error) {
	if s.owned {
		return s.largestOwnedEndWithin(batch, offset, maxEnd, guessedEnd, limit)
	}
	if guessedEnd < offset+1 {
		guessedEnd = offset + 1
	}
	if guessedEnd > maxEnd {
		guessedEnd = maxEnd
	}
	probeBytes, err := s.estimate(batch, offset, guessedEnd-offset)
	if err != nil {
		return 0, 0, err
	}
	bestEnd, bestBytes := offset, int64(0)
	lo, hi := offset+1, maxEnd
	if probeBytes <= limit {
		bestEnd, bestBytes = guessedEnd, probeBytes
		lo = guessedEnd + 1
	} else {
		hi = guessedEnd - 1
	}
	for lo <= hi {
		mid := lo + (hi-lo)/2
		bytes, err := s.estimate(batch, offset, mid-offset)
		if err != nil {
			return 0, 0, err
		}
		if bytes <= limit {
			bestEnd, bestBytes = mid, bytes
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return bestEnd, bestBytes, nil
}
