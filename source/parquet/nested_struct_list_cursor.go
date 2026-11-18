package parquet

import (
	"context"
	"errors"
	"fmt"
	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"gocolumn/memory"
	"io"
	"math"
)

type structListLeafCursor struct {
	plan                     fixedLeaf
	reader                   file.ColumnChunkReader
	scratch                  fixedScratch
	rep                      [1]int16
	bytes                    [1]arrowparquet.ByteArray
	emptyOffsets             [1]uint32
	present, valid, eof      bool
	levels, starts, expected int64
}
type nestedStructListGroup struct {
	cursor                                *nestedListCursor
	plan                                  *nestedStructListPlan
	leaves                                []structListLeafCursor
	stage, output, candidate              structListBuffer
	vector                                *memory.StructListVector
	neededCaps, chosenCaps, candidateCaps []int
}

func newNestedStructListGroup(c *nestedListCursor, p *nestedStructListPlan) *nestedStructListGroup {
	g := &nestedStructListGroup{cursor: c, plan: p, leaves: make([]structListLeafCursor, len(p.leaves)), neededCaps: make([]int, len(p.leaves)), chosenCaps: make([]int, len(p.leaves)), candidateCaps: make([]int, len(p.leaves))}
	for i, l := range p.leaves {
		g.leaves[i].plan = l
	}
	return g
}
func (g *nestedStructListGroup) fields() []memory.Field { return g.plan.field.Children[0].Children }
func (g *nestedStructListGroup) count(b *structListBuffer) int {
	if b.vector == nil {
		return 0
	}
	return b.vector.Rows
}
func (g *nestedStructListGroup) payload(b *structListBuffer) int64 {
	if b.vector == nil {
		return 0
	}
	n := b.vector.Rows
	total := int64(memory.WordsFor(n)) * 8
	for _, f := range g.fields() {
		size := int64(memory.WordsFor(n)) * 8
		if f.Type == memory.TypeString {
			size += (int64(n)+1)*4 + int64(len(b.vector.Values.Strings[f.Name].Data))
		} else {
			size += int64(n) * fixedWidth(f.Type)
		}
		if size < 0 || total > math.MaxInt64-size {
			return -1
		}
		total += size
	}
	return total
}
func (g *nestedStructListGroup) closeReaders() error {
	var err error
	for i := range g.leaves {
		l := &g.leaves[i]
		l.bytes[0] = nil
		l.present = false
		if l.reader != nil {
			e := l.reader.Close()
			if err == nil {
				err = e
			}
			l.reader = nil
		}
	}
	return err
}
func (g *nestedStructListGroup) open(ctx context.Context, rg *file.RowGroupReader) error {
	for i := range g.leaves {
		if err := g.cursor.check(ctx); err != nil {
			return err
		}
		l := &g.leaves[i]
		meta, err := rg.MetaData().ColumnChunk(l.plan.binding.Index)
		if err != nil {
			return err
		}
		if meta.NumValues() != g.cursor.expected {
			return fmt.Errorf("StructList leaf declared levels disagree")
		}
		l.expected = meta.NumValues()
		l.levels, l.starts = 0, 0
		l.present = false
		l.eof = g.cursor.groupRows == 0
		l.bytes[0] = nil
		l.reader, err = rg.Column(l.plan.binding.Index)
		if err != nil {
			return err
		}
	}
	return nil
}
func (g *nestedStructListGroup) peek(ctx context.Context) error {
	c := g.cursor
	for i := range g.leaves {
		l := &g.leaves[i]
		if l.present || l.eof {
			continue
		}
		if err := c.check(ctx); err != nil {
			return err
		}
		l.scratch = fixedScratch{}
		l.rep[0] = 0
		l.bytes[0] = nil
		var total int64
		var count int
		var err error
		switch r := l.reader.(type) {
		case *file.Int32ColumnChunkReader:
			total, count, err = r.ReadBatchInPage(1, l.scratch.i32[:], l.scratch.defs[:], l.rep[:])
		case *file.Int64ColumnChunkReader:
			total, count, err = r.ReadBatchInPage(1, l.scratch.i64[:], l.scratch.defs[:], l.rep[:])
		case *file.Float32ColumnChunkReader:
			total, count, err = r.ReadBatchInPage(1, l.scratch.f32[:], l.scratch.defs[:], l.rep[:])
		case *file.Float64ColumnChunkReader:
			total, count, err = r.ReadBatchInPage(1, l.scratch.f64[:], l.scratch.defs[:], l.rep[:])
		case *file.BooleanColumnChunkReader:
			total, count, err = r.ReadBatchInPage(1, l.scratch.boolean[:], l.scratch.defs[:], l.rep[:])
		case *file.ByteArrayColumnChunkReader:
			total, count, err = r.ReadBatchInPage(1, l.bytes[:], l.scratch.defs[:], l.rep[:])
		default:
			return fmt.Errorf("unsupported StructList physical reader")
		}
		if err != nil {
			if errors.Is(err, io.EOF) && total == 0 && count == 0 && l.levels == l.expected && l.starts == c.groupRows {
				l.eof = true
				continue
			}
			return err
		}
		if total == 0 {
			if count != 0 || l.levels != l.expected || l.starts != c.groupRows {
				return fmt.Errorf("StructList leaf premature EOF")
			}
			l.eof = true
			continue
		}
		if total != 1 || l.levels >= l.expected {
			return fmt.Errorf("StructList excess levels")
		}
		l.levels++
		def, rep := l.scratch.defs[0], l.rep[0]
		if rep < 0 || rep > 1 || def < 0 || def > l.plan.maxDef {
			return fmt.Errorf("StructList invalid levels")
		}
		l.valid = def == l.plan.maxDef
		if (l.valid && count != 1) || (!l.valid && count != 0) {
			return fmt.Errorf("StructList dense value count mismatch")
		}
		if rep == 0 {
			if l.starts >= c.groupRows {
				return fmt.Errorf("StructList excess row starts")
			}
			l.starts++
		}
		l.present = true
	}
	first := &g.leaves[0]
	for i := 1; i < len(g.leaves); i++ {
		l := &g.leaves[i]
		if l.present != first.present || l.eof != first.eof {
			return fmt.Errorf("StructList leaf EOF disagreement")
		}
		if l.present && l.rep[0] != first.rep[0] {
			return fmt.Errorf("StructList leaf repetition disagreement")
		}
	}
	c.tuple.present = first.present
	c.tuple.rep[0] = first.rep[0]
	c.tuple.def[0] = first.scratch.defs[0]
	c.levels, c.starts, c.eof = first.levels, first.starts, first.eof
	if !first.present {
		return nil
	}
	// Compare semantic parent/item witnesses, not child-specific NULL bits.
	itemThreshold := c.plan.maxDef
	firstClass := min(first.scratch.defs[0], itemThreshold)
	for i := 1; i < len(g.leaves); i++ {
		if min(g.leaves[i].scratch.defs[0], itemThreshold) != firstClass {
			return fmt.Errorf("StructList leaf ancestor/item presence disagreement")
		}
	}
	return nil
}
func (g *nestedStructListGroup) consume() {
	for i := range g.leaves {
		g.leaves[i].present = false
		g.leaves[i].bytes[0] = nil
	}
	g.cursor.tuple.present = false
}
func (g *nestedStructListGroup) assemble(ctx context.Context) error {
	c := g.cursor
	if c.complete {
		return nil
	}
	p := c.ancestors.optionalCount()
	if c.plan.optional {
		p++
	}
	for {
		if err := g.peek(ctx); err != nil {
			return err
		}
		if c.mode == 0 {
			if !c.tuple.present || c.tuple.rep[0] != 0 {
				return fmt.Errorf("StructList row must start rep0")
			}
			c.captureAncestors(c.tuple.def[0])
			c.parentValid = c.ancestorValid && c.tuple.def[0] >= p
			if c.tuple.def[0] <= p {
				c.mode = 1
				g.consume()
			} else {
				c.mode = 2
				c.first = true
			}
			continue
		}
		if !c.tuple.present {
			if !c.eof || c.base.remaining != 1 {
				return fmt.Errorf("StructList premature row EOF")
			}
			c.complete = true
			return nil
		}
		if c.mode == 1 {
			if c.tuple.rep[0] != 0 {
				return fmt.Errorf("StructList placeholder continuation")
			}
			c.complete = true
			return nil
		}
		if !c.first && c.tuple.rep[0] == 0 {
			c.complete = true
			return nil
		}
		if (c.first && c.tuple.rep[0] != 0) || (!c.first && c.tuple.rep[0] != 1) || c.tuple.def[0] < p+1 {
			return fmt.Errorf("StructList invalid element continuation")
		}
		if err := g.appendElement(ctx); err != nil {
			return err
		}
		g.consume()
		c.first = false
	}
}
func (g *nestedStructListGroup) appendElement(ctx context.Context) error {
	c := g.cursor
	n := g.count(&g.stage)
	if n >= math.MaxInt-64 {
		return fmt.Errorf("StructList element overflow: %w", memory.ErrBudgetExceeded)
	}
	itemValid := c.tuple.def[0] >= c.plan.maxDef
	h := stringListStream{base: c.base}
	for i, f := range g.fields() {
		needed := 0
		if f.Type == memory.TypeString {
			if g.stage.vector != nil {
				needed = len(g.stage.vector.Values.Strings[f.Name].Data)
			}
			value := g.leaves[i].bytes[0]
			if !itemValid || !g.leaves[i].valid {
				value = nil
			}
			if len(value) > math.MaxInt-needed {
				return fmt.Errorf("StructList String overflow: %w", memory.ErrBudgetExceeded)
			}
			needed += len(value)
			if err := h.validateUTF8(ctx, value); err != nil {
				return err
			}
		}
		g.neededCaps[i] = needed
	}
	if err := g.growStage(ctx, n+1); err != nil {
		return err
	}
	v := g.stage.vector
	resizeGroupBuffer(v, n+1)
	if itemValid {
		memory.Set(v.Validity, n)
	}
	for i, f := range g.fields() {
		if err := c.check(ctx); err != nil {
			return err
		}
		l := &g.leaves[i]
		valid := itemValid && l.valid
		if f.Type == memory.TypeString {
			s := v.Values.Strings[f.Name]
			old := len(s.Data)
			value := l.bytes[0]
			if !valid {
				value = nil
			}
			s.Data = s.Data[:old+len(value)]
			if err := h.copyBytes(ctx, s.Data[old:], value); err != nil {
				return err
			}
			s.Offsets[n+1] = uint32(len(s.Data))
			if valid {
				memory.Set(s.Validity, n)
			}
		} else {
			tmp := fixedStream{scratch: l.scratch}
			if err := tmp.storeOne(v.Values, l.plan.binding, n, valid); err != nil {
				return err
			}
		}
	}
	return c.check(ctx)
}
func (g *nestedStructListGroup) growStage(ctx context.Context, n int) error {
	c := g.cursor
	logical, err := groupBufferBytes(g.fields(), n, g.neededCaps)
	if err != nil {
		return err
	}
	limit, err := c.owner.stageLimit(c, logical)
	if err != nil {
		return err
	}
	fits := g.stage.vector != nil && n <= g.stage.slots
	for i, f := range g.fields() {
		if f.Type == memory.TypeString && g.stage.vector != nil && g.neededCaps[i] > cap(g.stage.vector.Values.Strings[f.Name].Data) {
			fits = false
		}
	}
	if fits {
		return nil
	}
	slots := max(n, max(1, g.stage.slots*2))
	if slots < n || slots >= math.MaxInt-63 {
		slots = n
	}
	for i, f := range g.fields() {
		capBytes := 0
		if f.Type == memory.TypeString && g.stage.vector != nil {
			capBytes = cap(g.stage.vector.Values.Strings[f.Name].Data)
		}
		g.chosenCaps[i] = max(g.neededCaps[i], capBytes)
		if g.neededCaps[i] > capBytes && capBytes <= math.MaxInt/2 {
			g.chosenCaps[i] = max(g.neededCaps[i], capBytes*2)
		}
	}
	charge, err := groupBufferBytes(g.fields(), slots, g.chosenCaps)
	if err != nil || g.stage.charge > limit || charge > limit-g.stage.charge {
		slots = n
		copy(g.chosenCaps, g.neededCaps)
		charge, err = groupBufferBytes(g.fields(), slots, g.chosenCaps)
	}
	if err != nil {
		return err
	}
	if g.stage.charge > limit || charge > limit-g.stage.charge {
		return fmt.Errorf("StructList stage peak: %w", memory.ErrBudgetExceeded)
	}
	var lease memory.Reservation
	if c.base.opts.MemoryManager != nil {
		lease, err = c.base.opts.MemoryManager.Reserve(ctx, charge)
		if err != nil {
			return err
		}
	}
	ok := false
	defer func() {
		if !ok && lease != nil {
			lease.Release()
		}
	}()
	next, err := newGroupBuffer(ctx, c, g.fields(), slots, g.chosenCaps)
	if err != nil {
		return err
	}
	if err = copyGroupBuffer(ctx, c, next, g.stage.vector, 0); err != nil {
		return err
	}
	resizeGroupBuffer(next, g.count(&g.stage))
	if err = c.check(ctx); err != nil {
		return err
	}
	g.stage.close()
	g.stage = structListBuffer{vector: next, slots: slots, charge: charge, lease: lease}
	ok = true
	return nil
}

func (g *nestedStructListGroup) allocate(ctx context.Context, rows int, dest *memory.Batch) error {
	v := &memory.StructVector{Fields: g.fields(), Values: memory.NewBatch(), Encoding: memory.EncodingView}
	for i, f := range g.fields() {
		if err := g.cursor.check(ctx); err != nil {
			return err
		}
		if f.Type == memory.TypeString {
			g.leaves[i].emptyOffsets[0] = 0
			v.Values.Strings[f.Name] = &memory.StringViewVector{Offsets: g.leaves[i].emptyOffsets[:], Encoding: memory.EncodingView}
		} else {
			allocateFixedColumn(v.Values, f, 0)
		}
	}
	g.output = structListBuffer{vector: v}
	g.vector = &memory.StructListVector{Offsets: make([]uint32, rows+1), Validity: make(memory.Bitmap, memory.WordsFor(rows)), Values: v, Encoding: memory.EncodingView}
	dest.StructLists[g.cursor.plan.name] = g.vector
	return nil
}
func (g *nestedStructListGroup) prepareSize() (int64, int64, error) {
	c := g.cursor
	old, n := g.count(&g.output), g.count(&g.stage)
	if n > math.MaxInt-old || uint64(old)+uint64(n) >= math.MaxUint32 {
		return 0, 0, fmt.Errorf("StructList output elements overflow: %w", memory.ErrBudgetExceeded)
	}
	c.target = old + n
	for i, f := range g.fields() {
		g.candidateCaps[i] = 0
		if f.Type == memory.TypeString {
			a, b := 0, 0
			if g.output.vector != nil {
				a = len(g.output.vector.Values.Strings[f.Name].Data)
			}
			if g.stage.vector != nil {
				b = len(g.stage.vector.Values.Strings[f.Name].Data)
			}
			if b > math.MaxInt-a {
				return 0, 0, fmt.Errorf("StructList output String overflow")
			}
			g.candidateCaps[i] = a + b
		}
	}
	logical, err := groupBufferBytes(g.fields(), c.target, g.candidateCaps)
	if err != nil {
		return 0, 0, err
	}
	c.candidateBytes = 0
	if old > 0 && n > 0 {
		c.candidateBytes = logical
	}
	return logical, c.candidateBytes, nil
}
func (g *nestedStructListGroup) copyCandidate(ctx context.Context) error {
	c := g.cursor
	if c.candidateBytes == 0 {
		return c.check(ctx)
	}
	v, err := newGroupBuffer(ctx, c, g.fields(), c.target, g.candidateCaps)
	if err != nil {
		return err
	}
	if err := copyGroupBuffer(ctx, c, v, g.output.vector, 0); err != nil {
		return err
	}
	if err := copyGroupBuffer(ctx, c, v, g.stage.vector, g.count(&g.output)); err != nil {
		return err
	}
	resizeGroupBuffer(v, c.target)
	g.candidate = structListBuffer{vector: v, slots: c.target, charge: c.candidateBytes}
	return c.check(ctx)
}
func (g *nestedStructListGroup) commit(row int) {
	c := g.cursor
	if g.count(&g.output) == 0 && g.count(&g.stage) > 0 {
		g.output.close()
		g.output = g.stage
		g.stage = structListBuffer{}
	} else if g.candidate.vector != nil {
		g.output.close()
		g.output = g.candidate
		g.output.lease = c.replacementLease
		c.replacementLease = nil
		g.candidate = structListBuffer{}
	}
	g.vector.Values = g.output.vector
	g.vector.Offsets[row+1] = uint32(c.target)
	if c.parentValid {
		memory.Set(g.vector.Validity, row)
	}
	c.clearStage()
}
func (g *nestedStructListGroup) clearStage() {
	g.stage.close()
	g.cursor.mode = 0
	g.cursor.complete = false
	g.cursor.first = false
	g.cursor.parentValid = false
}
func (g *nestedStructListGroup) crop(rows int) {
	g.vector.Offsets = g.vector.Offsets[:rows+1]
	g.vector.Validity = g.vector.Validity[:memory.WordsFor(rows)]
	e := int(g.vector.Offsets[rows])
	resizeGroupBuffer(g.vector.Values, e)
	g.vector.Values.Validity = g.vector.Values.Validity[:memory.WordsFor(e)]
	for _, f := range g.fields() {
		if f.Type == memory.TypeString {
			v := g.vector.Values.Values.Strings[f.Name]
			v.Validity = v.Validity[:memory.WordsFor(e)]
		}
	}
	// Fixed children may retain extra validity capacity; their logical Values
	// slices and Rows are cropped on the element axis, never the parent axis.
}
