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
	stack                    [memory.MaxRepeatedListDepth]repeatedFrame
	stage, output, candidate arrayBuffer
	vector                   *memory.ArrayNode
	empty                    [1]uint32
	target                   [memory.MaxRepeatedListDepth]int
	targetBytes              int
}

func (g *nestedArrayCursor) payload(b *arrayBuffer) int64 {
	if b.node == nil {
		return 0
	}
	c, n := b.axisCounts()
	size, e := arrayBufferSize(g.cursor.kind, g.plan.depth, c, n)
	if e != nil {
		return -1
	}
	return size
}
func (g *nestedArrayCursor) grow(ctx context.Context, counts [memory.MaxRepeatedListDepth]int, bytes int) error {
	c := g.cursor
	needed, err := arrayBufferSize(c.kind, g.plan.depth, counts, bytes)
	if err != nil {
		return err
	}
	limit, err := c.owner.stageLimit(c, needed)
	if err != nil {
		return err
	}
	fits := g.stage.node != nil && bytes <= g.stage.bytes
	for i := 0; i < g.plan.depth; i++ {
		fits = fits && counts[i] <= g.stage.caps[i]
	}
	if fits {
		return nil
	}
	caps := counts
	capBytes := bytes
	for i := 0; i < g.plan.depth; i++ {
		old := g.stage.caps[i]
		if old > 0 && old <= math.MaxInt/2 && caps[i] < old*2 {
			caps[i] = old * 2
		}
	}
	if old := g.stage.bytes; old > 0 && old <= math.MaxInt/2 && capBytes < old*2 {
		capBytes = old * 2
	}
	size, err := arrayBufferSize(c.kind, g.plan.depth, caps, capBytes)
	if err != nil || size > limit-g.stage.charge {
		caps = counts
		capBytes = bytes
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
	b := newRepeatedArrayBuffer(c.kind, g.plan.depth, caps, capBytes)
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
		rep, def := int(c.tuple.rep[0]), c.tuple.def[0]
		if !c.first && rep == 0 {
			c.complete = true
			return nil
		}
		if rep < 0 || rep > g.plan.depth {
			return fmt.Errorf("array repetition exceeds depth")
		}
		if !c.first {
			if rep == 0 || !g.stack[rep-1].slot || !g.stack[rep-1].present || def < g.plan.boundaries[rep-1].slot {
				if g.plan.depth == memory.MaxNestedListDepth {
					if rep == 1 {
						return fmt.Errorf("array rep1 after outer placeholder")
					}
					if rep == 2 {
						return fmt.Errorf("array rep2 after inner placeholder")
					}
				}
				return fmt.Errorf("array rep%d after axis %d placeholder", rep, rep-1)
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
		counts, bytes := g.stage.axisCounts()
		next := counts
		nb := bytes
		var fresh [memory.MaxRepeatedListDepth]bool
		active := true
		for axis := 1; axis < g.plan.depth; axis++ {
			if !g.stack[axis-1].slot {
				active = false
				break
			}
			fresh[axis] = c.first || rep <= axis
			if fresh[axis] {
				g.stack[axis] = repeatedFrame{present: def >= g.plan.boundaries[axis].present, slot: def >= g.plan.boundaries[axis].slot}
				if next[axis-1] == math.MaxInt {
					return memory.ErrBudgetExceeded
				}
				next[axis-1]++
			}
		}
		leaf := g.plan.depth - 1
		addLeaf := active && g.stack[leaf].slot
		if addLeaf {
			if next[leaf] == math.MaxInt {
				return memory.ErrBudgetExceeded
			}
			next[leaf]++
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
		if err := g.grow(ctx, next, nb); err != nil {
			return err
		}
		v := g.stage.node
		for axis := 1; axis < g.plan.depth; axis++ {
			if !g.stack[axis-1].slot {
				break
			}
			n := next[axis-1]
			if fresh[axis] {
				v.Offsets = v.Offsets[:n+1]
				v.Offsets[n] = uint32(counts[axis])
				if g.stack[axis].present {
					memory.Set(v.Validity, n-1)
				}
			}
			v.Offsets[n] = uint32(next[axis])
			v = v.Child
		}
		if addLeaf {
			v = g.stage.node
			for v.Child != nil {
				v = v.Child
			}
			e := counts[leaf]
			ne := next[leaf]
			if c.kind == memory.TypeString {
				x := v.Strings
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
				x := v.Int64
				x.Values = x.Values[:ne]
				if c.tuple.valid {
					x.Values[e] = c.tuple.value[0]
					memory.Set(x.Validity, e)
				}
			}
		}
		c.tuple.present = false
		c.bytes[0] = nil
		c.first = false
	}
}
func (g *nestedArrayCursor) prepareSize() (int64, int64, error) {
	a, ab := g.output.axisCounts()
	b, bb := g.stage.axisCounts()
	for i := 0; i < g.plan.depth; i++ {
		if b[i] > math.MaxInt-a[i] {
			return 0, 0, memory.ErrBudgetExceeded
		}
		g.target[i] = a[i] + b[i]
	}
	if bb > math.MaxInt-ab {
		return 0, 0, memory.ErrBudgetExceeded
	}
	g.targetBytes = ab + bb
	if g.target[0] == 0 {
		g.cursor.candidateBytes = 0
		return 0, 0, nil
	}
	size, err := arrayBufferSize(g.cursor.kind, g.plan.depth, g.target, g.targetBytes)
	if err != nil {
		return 0, 0, err
	}
	extra := int64(0)
	if a[0] > 0 && b[0] > 0 {
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
	g.candidate = newRepeatedArrayBuffer(c.kind, g.plan.depth, g.target, g.targetBytes)
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
	var child *memory.ArrayNode
	for axis := g.plan.depth; axis >= 1; axis-- {
		v := &memory.ArrayNode{Kind: memory.TypeList, Encoding: memory.EncodingView, Offsets: g.empty[:]}
		if axis == g.plan.depth {
			v.Kind = g.cursor.kind
			v.Offsets = nil
			if g.cursor.kind == memory.TypeString {
				v.Strings = &memory.StringViewVector{Offsets: g.empty[:], Encoding: memory.EncodingView}
			} else {
				v.Int64 = &memory.Int64Vector{Encoding: memory.EncodingPlain}
			}
		} else {
			v.Child = child
		}
		child = v
	}
	g.vector = &memory.ArrayNode{Kind: memory.TypeList, Offsets: make([]uint32, rows+1), Validity: make(memory.Bitmap, memory.WordsFor(rows)), Child: child, Encoding: memory.EncodingView}
	if b.Arrays == nil {
		b.Arrays = make(map[string]*memory.ArrayNode)
	}
	b.Arrays[g.cursor.plan.name] = g.vector
}
func (g *nestedArrayCursor) crop(rows int) {
	g.vector.Offsets = g.vector.Offsets[:rows+1]
	g.vector.Validity = g.vector.Validity[:memory.WordsFor(rows)]
	for v := g.vector.Child; v != nil; v = v.Child {
		switch v.Kind {
		case memory.TypeList:
			if v.Validity != nil {
				v.Validity = v.Validity[:memory.WordsFor(v.Len())]
			}
		case memory.TypeString:
			if v.Strings.Validity != nil {
				v.Strings.Validity = v.Strings.Validity[:memory.WordsFor(v.Len())]
			}
		case memory.TypeInt64:
			if v.Int64.Validity != nil {
				v.Int64.Validity = v.Int64.Validity[:memory.WordsFor(v.Len())]
			}
		}
	}
}
