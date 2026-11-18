package parquet

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"math"
)

type repeatedFrame struct{ present, slot bool }
type nestedArrayCursor struct {
	cursor                   *nestedListCursor
	plan                     *nestedArrayPlan
	stack                    [2]repeatedFrame
	stage, output, candidate arrayBuffer
	vector                   *memory.ArrayNode
	empty                    [1]uint32
	target                   [3]int
}

func (g *nestedArrayCursor) payload(b *arrayBuffer) int64 {
	if b.node == nil {
		return 0
	}
	n, e, bytes := b.counts()
	v, err := arrayBufferBytes(g.cursor.kind, n, e, bytes)
	if err != nil {
		return -1
	}
	return v
}
func (g *nestedArrayCursor) grow(ctx context.Context, n, e, bytes int) error {
	c := g.cursor
	needed, err := arrayBufferBytes(c.kind, n, e, bytes)
	if err != nil {
		return err
	}
	limit, err := c.owner.stageLimit(c, needed)
	if err != nil {
		return err
	}
	if n <= g.stage.slots && e <= g.stage.values && bytes <= g.stage.bytes && g.stage.node != nil {
		return nil
	}
	caps := [3]int{n, e, bytes}
	olds := [3]int{g.stage.slots, g.stage.values, g.stage.bytes}
	for j := range caps {
		if olds[j] > 0 && olds[j] <= math.MaxInt/2 && caps[j] < olds[j]*2 {
			caps[j] = olds[j] * 2
		}
	}
	size, err := arrayBufferBytes(c.kind, caps[0], caps[1], caps[2])
	if err != nil || size > limit-g.stage.charge {
		caps = [3]int{n, e, bytes}
		size = needed
	}
	if size > limit-g.stage.charge {
		return fmt.Errorf("array stage peak: %w", memory.ErrBudgetExceeded)
	}
	var lease memory.Reservation
	if c.base.opts.MemoryManager != nil {
		lease, err = c.base.opts.MemoryManager.Reserve(ctx, size)
		if err != nil {
			return err
		}
	}
	b := newArrayBuffer(c.kind, caps[0], caps[1], caps[2])
	b.lease = lease
	if err = copyArrayBuffer(ctx, c, &b, &g.stage); err != nil {
		b.close()
		return err
	}
	g.stage.close()
	g.stage = b
	return nil
}
func (g *nestedArrayCursor) assemble(ctx context.Context) error {
	c := g.cursor
	if c.complete {
		return nil
	}
	for {
		if err := c.peek(ctx); err != nil {
			return err
		}
		if c.mode == 0 {
			if !c.tuple.present || c.tuple.rep[0] != 0 {
				return fmt.Errorf("array row must start rep0")
			}
			c.captureAncestors(c.tuple.def[0])
			c.parentValid = c.ancestorValid && c.tuple.def[0] >= g.plan.boundaries[0].present
			c.mode = 1
			c.first = true
			g.stack[0] = repeatedFrame{present: c.parentValid, slot: c.tuple.def[0] >= g.plan.boundaries[0].slot}
		}
		if !c.tuple.present {
			if c.first {
				return fmt.Errorf("array premature EOF")
			}
			c.complete = true
			return nil
		}
		rep, def := c.tuple.rep[0], c.tuple.def[0]
		if !c.first && rep == 0 {
			c.complete = true
			return nil
		}
		if !c.first {
			if rep == 1 && (!g.stack[0].slot || def < g.plan.boundaries[0].slot) {
				return fmt.Errorf("array rep1 after outer placeholder")
			}
			if rep == 2 && (!g.stack[1].slot || !g.stack[1].present || def < g.plan.boundaries[1].slot) {
				return fmt.Errorf("array rep2 after inner placeholder")
			}
			for j := 0; j < c.ancestors.count; j++ {
				if (def >= c.ancestors.thresholds[j]) != c.witness[j] {
					return fmt.Errorf("array ancestor changes within row")
				}
			}
		}
		if !g.stack[0].slot {
			if !c.first {
				return fmt.Errorf("array outer placeholder continuation")
			}
			c.tuple.present = false
			c.bytes[0] = nil
			c.first = false
			continue
		}
		fresh := c.first || rep == 1
		inner := g.stack[1]
		if fresh {
			inner = repeatedFrame{present: def >= g.plan.boundaries[1].present, slot: def >= g.plan.boundaries[1].slot}
		}
		n, e, bytes := g.stage.counts()
		nn, ne, nb := n, e, bytes
		if fresh {
			nn++
		}
		if inner.slot {
			ne++
			if c.kind == memory.TypeString && c.tuple.valid {
				if len(c.bytes[0]) > math.MaxInt-bytes {
					return memory.ErrBudgetExceeded
				}
				nb += len(c.bytes[0])
				if err := c.strings.validateUTF8(ctx, c.bytes[0]); err != nil {
					return err
				}
			}
		}
		if err := g.grow(ctx, nn, ne, nb); err != nil {
			return err
		}
		v := g.stage.node
		if fresh {
			v.Offsets = v.Offsets[:nn+1]
			v.Offsets[nn] = uint32(e)
			if inner.present {
				memory.Set(v.Validity, n)
			}
		}
		if inner.slot {
			if c.kind == memory.TypeString {
				x := v.Child.Strings
				x.Offsets = x.Offsets[:ne+1]
				x.Data = x.Data[:nb]
				if c.tuple.valid {
					if err := c.strings.copyBytes(ctx, x.Data[bytes:], c.bytes[0]); err != nil {
						return err
					}
					memory.Set(x.Validity, e)
				}
				x.Offsets[ne] = uint32(nb)
			} else {
				x := v.Child.Int64
				x.Values = x.Values[:ne]
				if c.tuple.valid {
					x.Values[e] = c.tuple.value[0]
					memory.Set(x.Validity, e)
				}
			}
			v.Offsets[nn] = uint32(ne)
		}
		g.stack[1] = inner
		c.tuple.present = false
		c.bytes[0] = nil
		c.first = false
	}
}
func (g *nestedArrayCursor) prepareSize() (int64, int64, error) {
	a, b, c := g.output.counts()
	x, y, z := g.stage.counts()
	if x > math.MaxInt-a || y > math.MaxInt-b || z > math.MaxInt-c {
		return 0, 0, memory.ErrBudgetExceeded
	}
	g.target = [3]int{a + x, b + y, c + z}
	if a+x == 0 {
		g.cursor.candidateBytes = 0
		return 0, 0, nil
	}
	size, err := arrayBufferBytes(g.cursor.kind, a+x, b+y, c+z)
	if err != nil {
		return 0, 0, err
	}
	extra := int64(0)
	if a > 0 && x > 0 {
		extra = size
	}
	g.cursor.candidateBytes = extra
	return size, extra, nil
}
func (g *nestedArrayCursor) copyCandidate(ctx context.Context) error {
	c := g.cursor
	if c.candidateBytes == 0 {
		return c.check(ctx)
	}
	g.candidate = newArrayBuffer(c.kind, g.target[0], g.target[1], g.target[2])
	if err := copyArrayBuffer(ctx, c, &g.candidate, &g.output); err != nil {
		return err
	}
	return copyArrayBuffer(ctx, c, &g.candidate, &g.stage)
}
func (g *nestedArrayCursor) commit(row int) {
	c := g.cursor
	n, _, _ := g.output.counts()
	add, _, _ := g.stage.counts()
	if n == 0 && add > 0 {
		g.output.close()
		g.output = g.stage
		g.stage = arrayBuffer{}
	} else if g.candidate.node != nil {
		g.output.close()
		g.output = g.candidate
		g.output.lease = c.replacementLease
		c.replacementLease = nil
		g.candidate = arrayBuffer{}
	}
	if g.output.node != nil {
		g.vector.Child = g.output.node
	}
	g.vector.Offsets[row+1] = uint32(g.target[0])
	if c.parentValid {
		memory.Set(g.vector.Validity, row)
	}
	c.clearStage()
}
func (g *nestedArrayCursor) allocate(rows int, b *memory.Batch) {
	leaf := &memory.ArrayNode{Kind: g.cursor.kind, Encoding: memory.EncodingView}
	if g.cursor.kind == memory.TypeString {
		leaf.Strings = &memory.StringViewVector{Offsets: g.empty[:], Encoding: memory.EncodingView}
	} else {
		leaf.Int64 = &memory.Int64Vector{Encoding: memory.EncodingPlain}
	}
	inner := &memory.ArrayNode{Kind: memory.TypeList, Offsets: g.empty[:], Child: leaf, Encoding: memory.EncodingView}
	g.vector = &memory.ArrayNode{Kind: memory.TypeList, Offsets: make([]uint32, rows+1), Validity: make(memory.Bitmap, memory.WordsFor(rows)), Child: inner, Encoding: memory.EncodingView}
	if b.Arrays == nil {
		b.Arrays = make(map[string]*memory.ArrayNode)
	}
	b.Arrays[g.cursor.plan.name] = g.vector
}
func (g *nestedArrayCursor) crop(rows int) {
	g.vector.Offsets = g.vector.Offsets[:rows+1]
	g.vector.Validity = g.vector.Validity[:memory.WordsFor(rows)]
	v := g.vector.Child
	v.Validity = v.Validity[:memory.WordsFor(v.Len())]
	leaf := v.Child
	if leaf.Kind == memory.TypeString {
		leaf.Strings.Validity = leaf.Strings.Validity[:memory.WordsFor(leaf.Len())]
	} else {
		leaf.Int64.Validity = leaf.Int64.Validity[:memory.WordsFor(leaf.Len())]
	}
}
