package memory

import "fmt"

// EstimateArrayRangeBytes reports logical two-axis payload, not owned allocation
// or a bound on retained source backing. It checks every requested offset range.
func EstimateArrayRangeBytes(v *ArrayNode, offset, length int) (int64, error) {
	return estimateArrayRangeBytes(v, offset, length, 0, nil)
}
func estimateArrayRangeBytes(v *ArrayNode, offset, length, depth int, check func() error) (int64, error) {
	if err := recursiveCheck(check); err != nil {
		return 0, err
	}
	if v == nil || depth > MaxNestedListDepth || offset < 0 || length < 0 || offset > v.Len() || length > v.Len()-offset {
		return 0, fmt.Errorf("invalid array payload range")
	}
	end := offset + length
	switch v.Kind {
	case TypeList:
		if len(v.Offsets) < end+1 {
			return 0, fmt.Errorf("missing array List sentinel")
		}
		if depth == MaxNestedListDepth || v.Child == nil || !structValidityFits(v.Validity, end) {
			return 0, fmt.Errorf("invalid array List storage")
		}
		lo, hi := v.Offsets[offset], v.Offsets[end]
		if lo > hi || uint64(hi) > uint64(v.Child.Len()) {
			return 0, fmt.Errorf("invalid array offsets")
		}
		for i := offset; i < end; i++ {
			if err := recursiveCheck(check); err != nil {
				return 0, err
			}
			if v.Offsets[i] > v.Offsets[i+1] {
				return 0, fmt.Errorf("decreasing array offsets")
			}
		}
		n, err := estimateArrayRangeBytes(v.Child, int(lo), int(hi-lo), depth+1, check)
		if err != nil {
			return 0, err
		}
		n, err = ownedRangeAdd(n, int64(length)+1, 4)
		if err != nil {
			return 0, err
		}
		return ownedRangeAdd(n, int64(WordsFor(length)), 8)
	case TypeInt64:
		if depth != MaxNestedListDepth || v.Int64 == nil || !structValidityFits(v.Int64.Validity, end) {
			return 0, fmt.Errorf("invalid array Int64 storage")
		}
		return ownedRangeAdd(int64(WordsFor(length))*8, int64(length), 8)
	case TypeString:
		if v.Strings == nil || len(v.Strings.Offsets) < end+1 {
			return 0, fmt.Errorf("missing array String sentinel")
		}
		if depth != MaxNestedListDepth || v.Strings == nil || !structValidityFits(v.Strings.Validity, end) {
			return 0, fmt.Errorf("invalid array String storage")
		}
		lo, hi := v.Strings.Offsets[offset], v.Strings.Offsets[end]
		if lo > hi || uint64(hi) > uint64(len(v.Strings.Data)) {
			return 0, fmt.Errorf("invalid array String offsets")
		}
		for i := offset; i < end; i++ {
			if err := recursiveCheck(check); err != nil {
				return 0, err
			}
			if v.Strings.Offsets[i] > v.Strings.Offsets[i+1] {
				return 0, fmt.Errorf("decreasing array String offsets")
			}
		}
		n, err := ownedRangeAdd(int64(hi-lo), int64(length)+1, 4)
		if err != nil {
			return 0, err
		}
		return ownedRangeAdd(n, int64(WordsFor(length)), 8)
	}
	return 0, fmt.Errorf("unsupported array payload kind")
}
