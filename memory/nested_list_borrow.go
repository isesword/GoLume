package memory

import "fmt"

func arrayBorrowSize(v *ArrayNode, offset, length, depth int, check func() error) (int64, error) {
	if err := recursiveCheck(check); err != nil {
		return 0, err
	}
	if v == nil || depth > MaxNestedListDepth || offset < 0 || length < 0 || offset > v.Len() || length > v.Len()-offset {
		return 0, fmt.Errorf("invalid array borrowed range")
	}
	end := offset + length
	n := int64(1024)
	switch v.Kind {
	case TypeList:
		if len(v.Offsets) < end+1 {
			return 0, fmt.Errorf("missing array List sentinel")
		}
		if depth == MaxNestedListDepth || v.Child == nil || !structValidityFits(v.Validity, end) {
			return 0, fmt.Errorf("invalid array List")
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
		c, e := arrayBorrowSize(v.Child, int(lo), int(hi-lo), depth+1, check)
		if e != nil {
			return 0, e
		}
		n, e = ownedRangeAdd(n, c, 1)
		if e != nil {
			return 0, e
		}
		if lo != 0 {
			n, e = ownedRangeAdd(n, int64(length)+1, 4)
			if e != nil {
				return 0, e
			}
		}
		if v.Validity != nil && offset%64 != 0 {
			return ownedRangeAdd(n, int64(WordsFor(length)), 8)
		}
	case TypeInt64:
		if depth != MaxNestedListDepth || v.Int64 == nil || !structValidityFits(v.Int64.Validity, end) {
			return 0, fmt.Errorf("invalid array leaf")
		}
		if v.Int64.Validity != nil && offset%64 != 0 {
			return ownedRangeAdd(n, int64(WordsFor(length)), 8)
		}
	case TypeString:
		if v.Strings == nil || len(v.Strings.Offsets) < end+1 {
			return 0, fmt.Errorf("missing array String sentinel")
		}
		if depth != MaxNestedListDepth || v.Strings == nil || !structValidityFits(v.Strings.Validity, end) {
			return 0, fmt.Errorf("invalid array leaf")
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
		var e error
		if lo != 0 {
			n, e = ownedRangeAdd(n, int64(length)+1, 4)
			if e != nil {
				return 0, e
			}
		}
		if v.Strings.Validity != nil && offset%64 != 0 {
			return ownedRangeAdd(n, int64(WordsFor(length)), 8)
		}
	default:
		return 0, fmt.Errorf("unsupported array borrowed kind")
	}
	return n, nil
}
func borrowArrayRange(v *ArrayNode, offset, length int, check func() error) (*ArrayNode, error) {
	if err := recursiveCheck(check); err != nil {
		return nil, err
	}
	offsets := func(in []uint32) ([]uint32, error) {
		a := in[offset : offset+length+1]
		if a[0] == 0 {
			return a, nil
		}
		out := make([]uint32, len(a))
		for i, x := range a {
			if err := recursiveCheck(check); err != nil {
				return nil, err
			}
			out[i] = x - a[0]
		}
		return out, nil
	}
	validity := func(in Bitmap) (Bitmap, error) {
		if in == nil || length == 0 {
			return nil, nil
		}
		if offset%64 == 0 {
			return in[offset/64 : offset/64+WordsFor(length)], nil
		}
		out := make(Bitmap, WordsFor(length))
		for i := 0; i < length; i++ {
			if err := recursiveCheck(check); err != nil {
				return nil, err
			}
			if IsSet(in, offset+i) {
				Set(out, i)
			}
		}
		return out, nil
	}
	out := &ArrayNode{Kind: v.Kind, Encoding: v.Encoding}
	var err error
	switch v.Kind {
	case TypeList:
		lo, hi := v.Offsets[offset], v.Offsets[offset+length]
		out.Child, err = borrowArrayRange(v.Child, int(lo), int(hi-lo), check)
		if err != nil {
			return nil, err
		}
		out.Offsets, err = offsets(v.Offsets)
		if err != nil {
			return nil, err
		}
		out.Validity, err = validity(v.Validity)
	case TypeInt64:
		bits, e := validity(v.Int64.Validity)
		if e != nil {
			return nil, e
		}
		out.Int64 = &Int64Vector{Values: v.Int64.Values[offset : offset+length], Validity: bits, Encoding: v.Int64.Encoding}
	case TypeString:
		bits, e := validity(v.Strings.Validity)
		if e != nil {
			return nil, e
		}
		off, e := offsets(v.Strings.Offsets)
		if e != nil {
			return nil, e
		}
		out.Strings = &StringViewVector{Offsets: off, Data: v.Strings.Data[v.Strings.Offsets[offset]:v.Strings.Offsets[offset+length]], Validity: bits, Encoding: v.Strings.Encoding}
	}
	if err != nil {
		return nil, err
	}
	if err = recursiveCheck(check); err != nil {
		return nil, err
	}
	return out, nil
}
