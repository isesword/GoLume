package parquet

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"math"
)

type arrayBuffer struct {
	node   *memory.ArrayNode
	caps   [memory.MaxRepeatedListDepth]int // inner list rows, then terminal rows
	depth  int                              // number of inner lists plus the terminal node
	bytes  int
	lease  memory.Reservation
	charge int64
}

func (b *arrayBuffer) close() {
	if b.lease != nil {
		b.lease.Release()
	}
	*b = arrayBuffer{}
}
func (b *arrayBuffer) axisCounts() (counts [memory.MaxRepeatedListDepth]int, bytes int) {
	v := b.node
	for i := 0; i < b.depth && v != nil; i++ {
		counts[i] = v.Len()
		if v.Kind == memory.TypeString {
			bytes = len(v.Strings.Data)
		}
		v = v.Child
	}
	return
}
func (b *arrayBuffer) counts() (int, int, int) {
	c, n := b.axisCounts()
	if b.depth == 0 {
		return 0, 0, 0
	}
	return c[0], c[b.depth-1], n
}
func arrayBufferSize(kind memory.DataType, depth int, counts [memory.MaxRepeatedListDepth]int, bytes int) (int64, error) {
	if depth < 2 || depth > memory.MaxRepeatedListDepth || bytes < 0 || uint64(bytes) >= math.MaxUint32 {
		return 0, fmt.Errorf("array capacity overflow: %w", memory.ErrBudgetExceeded)
	}
	var charge int64
	for i := 0; i < depth; i++ {
		n := counts[i]
		if n < 0 || n >= math.MaxInt-63 || uint64(n) >= math.MaxUint32 {
			return 0, memory.ErrBudgetExceeded
		}
		charge += int64(memory.WordsFor(n)) * 8
		if i < depth-1 || kind == memory.TypeString {
			charge += (int64(n) + 1) * 4
		} else {
			charge += int64(n) * 8
		}
	}
	return charge + int64(bytes), nil
}
func newRepeatedArrayBuffer(kind memory.DataType, depth int, counts [memory.MaxRepeatedListDepth]int, bytes int) arrayBuffer {
	b := arrayBuffer{depth: depth, caps: counts, bytes: bytes}
	var prev *memory.ArrayNode
	for i := 0; i < depth; i++ {
		v := &memory.ArrayNode{Encoding: memory.EncodingView}
		if i == 0 {
			b.node = v
		} else {
			prev.Child = v
		}
		n := counts[i]
		if i < depth-1 {
			v.Kind = memory.TypeList
			v.Offsets = make([]uint32, 1, n+1)
			v.Validity = make(memory.Bitmap, memory.WordsFor(n))
		} else {
			v.Kind = kind
			if kind == memory.TypeString {
				v.Strings = &memory.StringViewVector{Offsets: make([]uint32, 1, n+1), Data: make([]byte, 0, bytes), Validity: make(memory.Bitmap, memory.WordsFor(n)), Encoding: memory.EncodingView}
			} else {
				v.Int64 = &memory.Int64Vector{Values: make([]int64, 0, n), Validity: make(memory.Bitmap, memory.WordsFor(n)), Encoding: memory.EncodingPlain}
			}
		}
		prev = v
	}
	b.charge, _ = arrayBufferSize(kind, depth, counts, bytes)
	return b
}
func copyArrayBuffer(ctx context.Context, c *nestedListCursor, dst, src *arrayBuffer) error {
	if src.node == nil {
		return nil
	}
	dc, db := dst.axisCounts()
	sc, _ := src.axisCounts()
	d, s := dst.node, src.node
	for axis := 0; axis < src.depth; axis++ {
		if err := c.check(ctx); err != nil {
			return err
		}
		n, m := dc[axis], sc[axis]
		if s.Kind == memory.TypeList {
			d.Offsets = d.Offsets[:n+m+1]
			for i := 0; i < m; i++ {
				if i&1023 == 0 {
					if err := c.check(ctx); err != nil {
						return err
					}
				}
				d.Offsets[n+i+1] = uint32(dc[axis+1]) + s.Offsets[i+1]
				if s.IsValid(i) {
					memory.Set(d.Validity, n+i)
				}
			}
		} else if s.Kind == memory.TypeString {
			a, b := d.Strings, s.Strings
			a.Offsets = a.Offsets[:n+m+1]
			a.Data = a.Data[:db+len(b.Data)]
			if err := c.strings.copyBytes(ctx, a.Data[db:], b.Data); err != nil {
				return err
			}
			for i := 0; i < m; i++ {
				if i&1023 == 0 {
					if err := c.check(ctx); err != nil {
						return err
					}
				}
				a.Offsets[n+i+1] = uint32(db) + b.Offsets[i+1]
				if b.IsValid(i) {
					memory.Set(a.Validity, n+i)
				}
			}
		} else {
			a, b := d.Int64, s.Int64
			a.Values = a.Values[:n+m]
			if err := c.copyElements(ctx, a.Values[n:], b.Values); err != nil {
				return err
			}
			for i := 0; i < m; i++ {
				if i&1023 == 0 {
					if err := c.check(ctx); err != nil {
						return err
					}
				}
				if b.IsValid(i) {
					memory.Set(a.Validity, n+i)
				}
			}
		}
		d, s = d.Child, s.Child
	}
	return c.check(ctx)
}
