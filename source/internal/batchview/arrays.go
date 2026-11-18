package batchview

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"math"
)

func arrayScratch(v *memory.ArrayNode, offset, length, depth int, check func() error) (int64, error) {
	if check != nil {
		if err := check(); err != nil {
			return 0, err
		}
	}
	if v == nil || depth > memory.MaxNestedListDepth || offset < 0 || length < 0 || offset > v.Len() || length > v.Len()-offset {
		return 0, fmt.Errorf("invalid array view range")
	}
	end := offset + length
	n := int64(1024)
	switch v.Kind {
	case memory.TypeList:
		if len(v.Offsets) < end+1 {
			return 0, fmt.Errorf("missing array List sentinel")
		}
		if depth == memory.MaxNestedListDepth || v.Child == nil || v.Validity != nil && len(v.Validity) < memory.WordsFor(end) {
			return 0, fmt.Errorf("invalid array List storage")
		}
		lo, hi := v.Offsets[offset], v.Offsets[end]
		if lo > hi || uint64(hi) > uint64(v.Child.Len()) {
			return 0, fmt.Errorf("invalid array List offsets")
		}
		for i := offset; i < end; i++ {
			if check != nil {
				if err := check(); err != nil {
					return 0, err
				}
			}
			if v.Offsets[i] > v.Offsets[i+1] {
				return 0, fmt.Errorf("decreasing array offsets")
			}
		}
		child, err := arrayScratch(v.Child, int(lo), int(hi-lo), depth+1, check)
		if err != nil {
			return 0, err
		}
		extra := validityBytes(v.Validity, offset, length)
		if lo != 0 {
			extra += int64(length+1) * 4
		}
		if child > math.MaxInt64-n-extra {
			return 0, fmt.Errorf("array scratch overflow")
		}
		n += child + extra
	case memory.TypeInt64:
		if depth != memory.MaxNestedListDepth || v.Int64 == nil || v.Int64.Validity != nil && len(v.Int64.Validity) < memory.WordsFor(end) {
			return 0, fmt.Errorf("invalid array Int64 storage")
		}
		n += validityBytes(v.Int64.Validity, offset, length)
	case memory.TypeString:
		if v.Strings == nil || len(v.Strings.Offsets) < end+1 {
			return 0, fmt.Errorf("missing array String sentinel")
		}
		if depth != memory.MaxNestedListDepth {
			return 0, fmt.Errorf("invalid array terminal depth")
		}
		extra, err := stringBytes(v.Strings, offset, length)
		if err != nil {
			return 0, err
		}
		for i := offset; i < end; i++ {
			if check != nil {
				if err := check(); err != nil {
					return 0, err
				}
			}
			if v.Strings.Offsets[i] > v.Strings.Offsets[i+1] {
				return 0, fmt.Errorf("decreasing array String offsets")
			}
		}
		n += extra
	default:
		return 0, fmt.Errorf("unsupported array view kind")
	}
	return n, nil
}
func arrayOffsets(ctx context.Context, in []uint32, offset, length int) ([]uint32, error) {
	out := in[offset : offset+length+1]
	if out[0] == 0 {
		return out, ctx.Err()
	}
	result := make([]uint32, len(out))
	base := out[0]
	for i, n := range out {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result[i] = n - base
	}
	return result, nil
}
func arrayValidity(ctx context.Context, bits memory.Bitmap, offset, length int) (memory.Bitmap, error) {
	if bits == nil || length == 0 {
		return nil, ctx.Err()
	}
	if offset%64 == 0 {
		return bits[offset/64 : offset/64+memory.WordsFor(length)], ctx.Err()
	}
	out := make(memory.Bitmap, memory.WordsFor(length))
	for i := 0; i < length; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if memory.IsSet(bits, offset+i) {
			memory.Set(out, i)
		}
	}
	return out, nil
}
func arrayView(ctx context.Context, v *memory.ArrayNode, offset, length int) (*memory.ArrayNode, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := &memory.ArrayNode{Kind: v.Kind, Encoding: v.Encoding}
	var err error
	switch v.Kind {
	case memory.TypeList:
		lo, hi := v.Offsets[offset], v.Offsets[offset+length]
		out.Child, err = arrayView(ctx, v.Child, int(lo), int(hi-lo))
		if err != nil {
			return nil, err
		}
		out.Offsets, err = arrayOffsets(ctx, v.Offsets, offset, length)
		if err != nil {
			return nil, err
		}
		out.Validity, err = arrayValidity(ctx, v.Validity, offset, length)
	case memory.TypeInt64:
		bits, e := arrayValidity(ctx, v.Int64.Validity, offset, length)
		if e != nil {
			return nil, e
		}
		out.Int64 = &memory.Int64Vector{Values: v.Int64.Values[offset : offset+length], Validity: bits, Encoding: v.Int64.Encoding}
	case memory.TypeString:
		bits, e := arrayValidity(ctx, v.Strings.Validity, offset, length)
		if e != nil {
			return nil, e
		}
		off, e := arrayOffsets(ctx, v.Strings.Offsets, offset, length)
		if e != nil {
			return nil, e
		}
		out.Strings = &memory.StringViewVector{Offsets: off, Data: v.Strings.Data[v.Strings.Offsets[offset]:v.Strings.Offsets[offset+length]], Validity: bits, Encoding: v.Strings.Encoding}
	}
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
