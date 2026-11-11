package pipeline

import "gocolumn/memory"

// largestOwnedEndWithin changes only probe selection. Every bound is certified
// by the real estimator. Two interpolation attempts (each with a neighboring
// probe) are followed by ordinary binary search, so skew and plateaus cannot
// degrade into a row-at-a-time search. No estimator result is cached across Split.
func (s *ByteSizer) largestOwnedEndWithin(batch *memory.Batch, offset, maxEnd, guess int, limit int64) (int, int64, error) {
	if guess < offset+1 {
		guess = offset + 1
	}
	if guess > maxEnd {
		guess = maxEnd
	}
	probe := func(end int) (int64, error) { return s.estimate(batch, offset, end-offset) }
	first, err := probe(guess)
	if err != nil {
		return 0, 0, err
	}
	if guess == maxEnd && first <= limit {
		return guess, first, nil
	}
	one := first
	if guess != offset+1 {
		one, err = probe(offset + 1)
		if err != nil {
			return 0, 0, err
		}
	}
	if one > limit {
		return offset, 0, nil
	}
	best, bestBytes := offset+1, one
	upper := maxEnd
	fail, failBytes, haveFail := 0, int64(0), false
	if first <= limit {
		best, bestBytes = guess, first
	} else {
		upper = guess - 1
		fail, failBytes, haveFail = guess, first, true
	}
	// Keep the interpolation arithmetic advisory: floating-point rounding cannot
	// decide admission, and candidates are clamped before conversion to int.
	candidate := func() int {
		base, baseBytes := offset+1, one
		end, endBytes := best, bestBytes
		if haveFail {
			base, baseBytes = best, bestBytes
			end, endBytes = fail, failBytes
		}
		if end <= base || endBytes <= baseBytes {
			return upper
		}
		distance := float64(limit-baseBytes) / float64(endBytes-baseBytes) * float64(end-base)
		if distance >= float64(upper-base) {
			return upper
		}
		if distance <= float64(best-base) {
			return best
		}
		return base + int(distance)
	}
	accept := func(end int, bytes int64) {
		if bytes <= limit {
			if end > best {
				best, bestBytes = end, bytes
			}
		} else {
			if end-1 < upper {
				upper = end - 1
				fail, failBytes, haveFail = end, bytes, true
			}
		}
	}
	for attempt := 0; attempt < 2 && best < upper; attempt++ {
		at := candidate()
		bytes := bestBytes
		if at != best {
			bytes, err = probe(at)
			if err != nil {
				return 0, 0, err
			}
			accept(at, bytes)
		}
		if best == upper {
			break
		}
		// An adjacent failure/success proves the exact largest fitting boundary.
		neighbor := at + 1
		if bytes > limit {
			neighbor = at - 1
		}
		if neighbor > best && neighbor <= upper {
			n, err := probe(neighbor)
			if err != nil {
				return 0, 0, err
			}
			accept(neighbor, n)
		}
	}
	for best < upper {
		mid := best + 1 + (upper-best-1)/2
		n, err := probe(mid)
		if err != nil {
			return 0, 0, err
		}
		accept(mid, n)
	}
	return best, bestBytes, nil
}
