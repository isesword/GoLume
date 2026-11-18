package parquet

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"math"
)

type arrayBuffer struct {
	node                 *memory.ArrayNode
	slots, values, bytes int
	lease                memory.Reservation
	charge               int64
}

func (b *arrayBuffer) close() {
	if b.lease != nil {
		b.lease.Release()
	}
	*b = arrayBuffer{}
}
func (b *arrayBuffer) counts() (int, int, int) {
	if b.node == nil {
		return 0, 0, 0
	}
	bytes := 0
	if b.node.Child.Strings != nil {
		bytes = len(b.node.Child.Strings.Data)
	}
	return b.node.Len(), b.node.Child.Len(), bytes
}
func arrayBufferBytes(kind memory.DataType, n, e, bytes int) (int64, error) {
	for _, v := range []int{n, e, bytes} {
		if v < 0 || v >= math.MaxInt-63 || uint64(v) >= math.MaxUint32 {
			return 0, fmt.Errorf("array capacity overflow: %w", memory.ErrBudgetExceeded)
		}
	}
	charge := (int64(n)+1)*4 + int64(memory.WordsFor(n)+memory.WordsFor(e))*8
	if kind == memory.TypeString {
		charge += (int64(e)+1)*4 + int64(bytes)
	} else {
		charge += int64(e) * 8
	}
	return charge, nil
}
func newArrayBuffer(kind memory.DataType, n, e, bytes int) arrayBuffer {
	leaf := &memory.ArrayNode{Kind: kind, Encoding: memory.EncodingView}
	if kind == memory.TypeString {
		leaf.Strings = &memory.StringViewVector{Offsets: make([]uint32, 1, e+1), Data: make([]byte, 0, bytes), Validity: make(memory.Bitmap, memory.WordsFor(e)), Encoding: memory.EncodingView}
	} else {
		leaf.Int64 = &memory.Int64Vector{Values: make([]int64, 0, e), Validity: make(memory.Bitmap, memory.WordsFor(e)), Encoding: memory.EncodingPlain}
	}
	v := &memory.ArrayNode{Kind: memory.TypeList, Offsets: make([]uint32, 1, n+1), Validity: make(memory.Bitmap, memory.WordsFor(n)), Child: leaf, Encoding: memory.EncodingView}
	charge, _ := arrayBufferBytes(kind, n, e, bytes)
	return arrayBuffer{node: v, slots: n, values: e, bytes: bytes, charge: charge}
}
func copyArrayBuffer(ctx context.Context, c *nestedListCursor, dst, src *arrayBuffer) error {
	if src.node == nil {
		return nil
	}
	ni, ne, nb := dst.counts()
	si, se, _ := src.counts()
	d, s := dst.node, src.node
	d.Offsets = d.Offsets[:ni+si+1]
	for i := 0; i < si; i++ {
		if i&1023 == 0 {
			if err := c.check(ctx); err != nil {
				return err
			}
		}
		d.Offsets[ni+i+1] = uint32(ne) + s.Offsets[i+1]
		if s.IsValid(i) {
			memory.Set(d.Validity, ni+i)
		}
	}
	if c.kind == memory.TypeString {
		a, b := d.Child.Strings, s.Child.Strings
		a.Offsets = a.Offsets[:ne+se+1]
		a.Data = a.Data[:nb+len(b.Data)]
		if err := c.strings.copyBytes(ctx, a.Data[nb:], b.Data); err != nil {
			return err
		}
		for i := 0; i < se; i++ {
			if i&1023 == 0 {
				if err := c.check(ctx); err != nil {
					return err
				}
			}
			a.Offsets[ne+i+1] = uint32(nb) + b.Offsets[i+1]
			if b.IsValid(i) {
				memory.Set(a.Validity, ne+i)
			}
		}
	} else {
		a, b := d.Child.Int64, s.Child.Int64
		a.Values = a.Values[:ne+se]
		if err := c.copyElements(ctx, a.Values[ne:], b.Values); err != nil {
			return err
		}
		for i := 0; i < se; i++ {
			if i&1023 == 0 {
				if err := c.check(ctx); err != nil {
					return err
				}
			}
			if b.IsValid(i) {
				memory.Set(a.Validity, ne+i)
			}
		}
	}
	return c.check(ctx)
}
