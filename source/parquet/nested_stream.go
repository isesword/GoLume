package parquet

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/parquet/file"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
)

type nestedListStream struct {
	nodes                   []nestedStructNode
	parentVectors           []*memory.StructVector
	parentBatches           []*memory.Batch
	parentIDs               map[string]int
	parentNames             []string
	parentSeen, parentValid []bool

	base              *fixedStream
	cursors           []*nestedListCursor
	capacity, current int
}

func (s *Source) newNestedListStream(ctx context.Context, opts source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	raw, err := s.newFixedStream(ctx, opts, parts)
	if err != nil {
		return nil, err
	}
	b := raw.(*fixedStream)

	out := &nestedListStream{base: b, parentIDs: map[string]int{}}
	ids := map[int]int{}
	for old, n := range s.structNodes {
		if err := fixedContext(b.ctx, ctx); err != nil {
			b.Close()
			return nil, err
		}
		if _, ok := s.schema.Field(n.root); !ok {
			continue
		}
		id := len(out.nodes)
		ids[old] = id
		if n.parent >= 0 {
			n.parent = ids[n.parent]
		}
		out.nodes = append(out.nodes, n)
	}
	out.parentNames = make([]string, len(out.nodes))
	out.parentSeen = make([]bool, len(out.nodes))
	out.parentValid = make([]bool, len(out.nodes))
	out.parentVectors = make([]*memory.StructVector, len(out.nodes))
	out.parentBatches = make([]*memory.Batch, len(out.nodes))
	for id, n := range out.nodes {
		out.parentNames[id] = n.name
		if n.parent < 0 {
			out.parentIDs[n.name] = id
		}
	}
	for i := range b.leaves {
		if err := fixedContext(b.ctx, ctx); err != nil {
			b.Close()
			return nil, err
		}
		b.leaves[i].ancestors = remapNestedAncestors(b.leaves[i].ancestors, ids)
	}
	for _, p := range s.nestedLists {
		if err := fixedContext(b.ctx, ctx); err != nil {
			b.Close()
			return nil, err
		}
		name := p.plan.name
		if p.parent != "" {
			name = p.parent
		}
		if _, ok := s.schema.Field(name); ok {
			p.ancestors = remapNestedAncestors(p.ancestors, ids)
			c := &nestedListCursor{int64ListStream: int64ListStream{base: b, plan: p.plan}, owner: out, kind: p.kind, scalar: p.scalar, parentName: p.parent, parentID: -1, ancestorOptional: p.parentOptional, ancestors: p.ancestors}
			if p.parent != "" {
				c.parentID = p.ancestors.parentID()
			}
			if p.group != nil {
				c.group = newNestedStructListGroup(c, p.group)
			}
			if p.array != nil {
				c.array = &nestedArrayCursor{cursor: c, plan: p.array}
			}
			c.admission = c
			if p.kind == memory.TypeString {
				c.strings = &stringListStream{base: b, plan: p.plan, admission: c}
			}
			out.cursors = append(out.cursors, c)
		}
	}
	kept := b.leaves[:0]
	for _, l := range b.leaves {
		if l.binding.Type != memory.TypeList && l.binding.Type != memory.TypeString {
			kept = append(kept, l)
		}
	}
	b.leaves = kept
	return out, nil
}
func (s *nestedListStream) check(ctx context.Context) error { return fixedContext(s.base.ctx, ctx) }
func (s *nestedListStream) metadataBytes() int64 {
	if s.base.metadataBytes != 0 {
		return s.base.metadataBytes
	}
	return 2048 + int64(len(s.base.leaves)+len(s.cursors))*2048
}
func (s *nestedListStream) headers(rows int) int64 {
	if rows < 0 || int64(rows) > (math.MaxInt64-4)/16 {
		return -1
	}
	r := int64(rows)
	words := r / 64
	if r%64 != 0 {
		words++
	}
	bits := words * 8
	n := int64(0)
	add := func(f memory.Field) bool {
		var v int64
		if f.Type == memory.TypeList || f.Type == memory.TypeString {
			v = (r+1)*4 + bits
		} else {
			w := fixedWidth(f.Type)
			if w <= 0 || r > (math.MaxInt64-bits)/w {
				return false
			}
			v = r*w + bits
		}
		if n > math.MaxInt64-v {
			return false
		}
		n += v
		return true
	}
	var walk func([]memory.Field) bool
	walk = func(fields []memory.Field) bool {
		for _, f := range fields {
			if f.Type == memory.TypeStruct {
				if n > math.MaxInt64-bits {
					return false
				}
				n += bits
				if !walk(f.Children) {
					return false
				}
			} else if !add(f) {
				return false
			}
		}
		return true
	}
	if !walk(s.base.fields) {
		return -1
	}
	return n
}
func (s *nestedListStream) live() int64 {
	n := s.metadataBytes()
	add := func(v int64) bool {
		if v < 0 || n > math.MaxInt64-v {
			return false
		}
		n += v
		return true
	}
	if !add(s.headers(s.capacity)) {
		return math.MaxInt64
	}
	for _, c := range s.cursors {
		if !add(c.outputBytes()) || !add(c.stageBytes()) {
			return math.MaxInt64
		}
	}
	return n
}
func (s *nestedListStream) stageLimit(growing *nestedListCursor, needed int64) (int64, error) {
	logical := s.headers(1)
	if logical < 0 || needed < 0 {
		return 0, fmt.Errorf("nested LIST size overflow: %w", memory.ErrBudgetExceeded)
	}
	for _, c := range s.cursors {
		v := c.stagePayload()
		if c == growing {
			v = needed
		}
		if v < 0 || logical > math.MaxInt64-v {
			return 0, fmt.Errorf("nested row overflow: %w", memory.ErrBudgetExceeded)
		}
		logical += v
	}
	hard := s.base.opts.BatchPayloadBytes
	if hard > 0 && logical > hard {
		return 0, fmt.Errorf("nested row hard payload: %w", memory.ErrBudgetExceeded)
	}
	live, old := s.live(), growing.stageBytes()
	if live > s.base.opts.BatchBytes {
		return 0, fmt.Errorf("nested live peak: %w", memory.ErrBudgetExceeded)
	}
	return s.base.opts.BatchBytes - (live - old), nil
}
func (s *nestedListStream) rollback() {
	for _, c := range s.cursors {
		if c.replacementLease != nil {
			c.replacementLease.Release()
			c.replacementLease = nil
		}
		if c.array != nil {
			c.array.candidate = arrayBuffer{}
		}
		if c.group != nil {
			c.group.candidate = structListBuffer{}
		}
		c.replacement = nil
		c.replacementOffsets = nil
		c.replacementData = nil
		c.replacementValid = nil
	}
}
func (s *nestedListStream) clearOutput() {
	s.rollback()
	for _, c := range s.cursors {
		if c.array != nil {
			c.array.output.close()
			c.array.vector = nil
			continue
		}
		if c.group != nil {
			c.group.output.close()
			c.group.vector = nil
			continue
		}
		if c.scalar {
			if c.scalarLease != nil {
				c.scalarLease.Release()
				c.scalarLease = nil
			}
			c.scalarOutput = nil
			c.scalarRows = 0
			continue
		}
		if c.strings != nil {
			if c.strings.outputLease != nil {
				c.strings.outputLease.Release()
				c.strings.outputLease = nil
			}
			c.strings.output = nil
		} else {
			if c.outputLease != nil {
				c.outputLease.Release()
				c.outputLease = nil
			}
			c.output = nil
		}
	}
	clear(s.parentVectors)
	clear(s.parentBatches)
	s.capacity = 0
	s.base.clearBatch()
}
func (s *nestedListStream) release() error {
	s.clearOutput()
	var err error
	for _, c := range s.cursors {
		c.clearStage()
		c.tuple.present = false
		if c.hasReader() {
			if e := c.closeReader(); err == nil {
				err = e
			}
		}
	}
	if e := s.base.release(); err == nil {
		err = e
	}
	return err
}

// Close releases all lists' output, partial stages and pending tuples together.
func (s *nestedListStream) Close() error {
	if s.base.closed {
		return nil
	}
	s.base.closed = true
	return s.release()
}
func (s *nestedListStream) fail(err error) (bool, error) {
	s.base.terminal = err
	s.release()
	return false, err
}
func (s *nestedListStream) openGroup(ctx context.Context) error {
	b := s.base
	for b.remaining == 0 {
		for _, c := range s.cursors {
			if c.mode != 0 || c.tuple.present {
				return fmt.Errorf("uncommitted nested LIST state at group boundary")
			}
			if c.hasReader() {
				if !c.eof || c.levels != c.expected || c.starts != c.groupRows {
					return fmt.Errorf("LIST %q counts do not match row group", c.plan.name)
				}
				if err := c.closeReader(); err != nil {
					return err
				}
			}
		}
		if err := b.closeChunks(); err != nil {
			return err
		}
		if b.part == len(b.parts) {
			b.exhausted = true
			return b.release()
		}
		p := b.parts[b.part]
		b.part++
		if p.ID < 0 || p.ID >= b.reader.NumRowGroups() {
			return fmt.Errorf("invalid row group")
		}
		rg := b.reader.RowGroup(p.ID)
		b.remaining = rg.MetaData().NumRows()
		if b.remaining < 0 {
			return fmt.Errorf("negative row count")
		}
		for _, c := range s.cursors {
			if err := s.check(ctx); err != nil {
				return err
			}
			c.groupRows = b.remaining
			meta, err := rg.MetaData().ColumnChunk(c.plan.index)
			if err != nil {
				return err
			}
			c.expected = meta.NumValues()
			if c.expected < b.remaining || (b.remaining == 0 && c.expected != 0) || (c.scalar && c.expected != b.remaining) {
				return fmt.Errorf("LIST %q level count cannot represent rows", c.plan.name)
			}
			c.levels, c.starts = 0, 0
			c.eof = b.remaining == 0
			if err := c.openReader(ctx, rg); err != nil {
				return err
			}
		}

		b.chunks = make([]file.ColumnChunkReader, len(b.leaves))
		for i, l := range b.leaves {
			if err := s.check(ctx); err != nil {
				return err
			}
			meta, err := rg.MetaData().ColumnChunk(l.binding.Index)
			if err != nil {
				return err
			}
			if meta.NumValues() != b.remaining {
				return fmt.Errorf("fixed sibling value count mismatch")
			}
			r, err := rg.Column(l.binding.Index)
			if err != nil {
				return err
			}
			b.chunks[i] = r
		}
	}
	return nil
}
func (s *nestedListStream) allocate(ctx context.Context, rows int) error {
	n := s.headers(rows)
	live := s.metadataBytes()
	for _, c := range s.cursors {
		v := c.stageBytes()
		if v < 0 || live > math.MaxInt64-v {
			return fmt.Errorf("nested LIST size overflow")
		}
		live += v
	}
	if n < 0 || live > s.base.opts.BatchBytes || n > s.base.opts.BatchBytes-live {
		return fmt.Errorf("nested LIST headers: %w", memory.ErrBudgetExceeded)
	}
	if s.base.opts.MemoryManager != nil {
		lease, err := s.base.opts.MemoryManager.Reserve(ctx, n)
		if err != nil {
			return err
		}
		s.base.lease = lease
	}
	b := memory.NewBatch()
	b.Reset(rows)
	s.base.batch = b
	s.capacity = rows

	clear(s.parentVectors)
	clear(s.parentBatches)
	if err := s.allocateForest(ctx, b, s.base.fields, rows, -1); err != nil {
		return err
	}
	for _, c := range s.cursors {
		if err := s.check(ctx); err != nil {
			return err
		}
		dest := b
		if c.parentID >= 0 {
			dest = s.parentBatches[c.parentID]
		}
		if c.array != nil {
			c.array.allocate(rows, dest)
			continue
		}
		if c.group != nil {
			if err := c.group.allocate(ctx, rows, dest); err != nil {
				return err
			}
			continue
		}
		if c.scalar {
			c.scalarOutput = &memory.StringViewVector{Offsets: make([]uint32, rows+1), Validity: make(memory.Bitmap, memory.WordsFor(rows)), Encoding: memory.EncodingView}
			dest.Strings[c.plan.name] = c.scalarOutput
		} else if c.strings != nil {
			v := c.strings
			v.output = &memory.StringListVector{Offsets: make([]uint32, rows+1), Validity: make(memory.Bitmap, memory.WordsFor(rows)), Values: &memory.StringViewVector{Offsets: v.emptyOffsets[:], Encoding: memory.EncodingView}, Encoding: memory.EncodingView}
			dest.StringLists[c.plan.name] = v.output
		} else {
			c.output = &memory.Int64ListVector{Offsets: make([]uint32, rows+1), Validity: make(memory.Bitmap, memory.WordsFor(rows)), Values: &memory.Int64Vector{Encoding: memory.EncodingPlain}, Encoding: memory.EncodingView}
			dest.Int64Lists[c.plan.name] = c.output
		}
	}
	return nil
}
func (s *nestedListStream) commit(ctx context.Context, row int) error {
	logical, extra := s.headers(row+1), int64(0)
	if logical < 0 {
		return fmt.Errorf("nested header overflow: %w", memory.ErrBudgetExceeded)
	}
	for _, c := range s.cursors {
		if err := s.check(ctx); err != nil {
			return err
		}
		v, replace, err := c.prepareSize()
		if err != nil {
			return err
		}
		if logical > math.MaxInt64-v || extra > math.MaxInt64-replace {
			return fmt.Errorf("nested commit overflow: %w", memory.ErrBudgetExceeded)
		}
		logical += v
		extra += replace
	}
	o := s.base.opts
	if o.BatchPayloadBytes > 0 && logical > o.BatchPayloadBytes {
		return fmt.Errorf("nested hard payload: %w", memory.ErrBudgetExceeded)
	}
	if row > 0 && o.BatchPayloadTargetBytes > 0 && logical > o.BatchPayloadTargetBytes {
		return fmt.Errorf("nested target boundary: %w", memory.ErrBudgetExceeded)
	}
	live := s.live()
	if live > o.BatchBytes || extra > o.BatchBytes-live {
		return fmt.Errorf("nested commit peak: %w", memory.ErrBudgetExceeded)
	}
	for _, c := range s.cursors {
		if err := s.check(ctx); err != nil {
			s.rollback()
			return err
		}
		if c.candidateBytes == 0 {
			continue
		}
		if o.MemoryManager != nil {
			lease, err := o.MemoryManager.Reserve(ctx, c.candidateBytes)
			if err != nil {
				s.rollback()
				return err
			}
			c.replacementLease = lease
		}
	}
	for _, c := range s.cursors {
		if err := c.copyCandidate(ctx); err != nil {
			s.rollback()
			return err
		}
	}
	clear(s.parentSeen)
	clear(s.parentValid)
	for _, c := range s.cursors {
		if err := s.observeAncestors(c.ancestors, c.witness); err != nil {
			s.rollback()
			return err
		}
		if err := c.finishScalarGroup(ctx); err != nil {
			s.rollback()
			return err
		}
	}
	for i, l := range s.base.leaves {
		if err := s.check(ctx); err != nil {
			s.rollback()
			return err
		}
		def, count, err := s.base.readOne(s.base.chunks[i], l)
		if err != nil {
			s.rollback()
			return err
		}
		valid := def == l.maxDef
		if def < 0 || def > l.maxDef || (valid && count != 1) || (!valid && count != 0) {
			s.rollback()
			return fmt.Errorf("invalid fixed sibling definition")
		}
		dest := s.base.batch
		if l.ancestors.count > 0 {
			var witness [memory.MaxStructDepth]bool
			for j := 0; j < l.ancestors.count; j++ {
				witness[j] = def >= l.ancestors.thresholds[j]
			}
			if err := s.observeAncestors(l.ancestors, witness); err != nil {
				s.rollback()
				return err
			}
			dest = s.parentBatches[l.ancestors.parentID()]
		}
		if err := s.base.storeOne(dest, l.binding, row, valid); err != nil {
			s.rollback()
			return err
		}
	}
	if err := s.check(ctx); err != nil {
		s.rollback()
		return err
	}
	for _, c := range s.cursors {
		c.commitCandidate(row)
	}
	for id, v := range s.parentVectors {
		if v != nil && s.parentValid[id] {
			memory.Set(v.Validity, row)
		}
	}
	s.current = 0
	s.base.remaining--
	return nil
}

// Next publishes complete synchronized rows across all List columns. A budget
// boundary preserves every stage and lookahead; returned vectors are borrowed.
func (s *nestedListStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if dst == nil {
		return false, fmt.Errorf("nil destination")
	}
	dst.ClearColumns()
	b := s.base
	if ctx == nil {
		return s.fail(fmt.Errorf("nil context"))
	}
	if b.closed {
		return false, source.ErrBatchStreamClosed
	}
	if b.terminal != nil {
		return false, b.terminal
	}
	if b.exhausted {
		return false, nil
	}
	if err := s.check(ctx); err != nil {
		return s.fail(err)
	}
	s.clearOutput()
	if err := s.openGroup(ctx); err != nil {
		return s.fail(err)
	}
	if b.exhausted {
		return false, nil
	}
	want := b.opts.BatchRows
	if int64(want) > b.remaining {
		want = int(b.remaining)
	}
	available := b.opts.BatchBytes - s.metadataBytes()
	for _, c := range s.cursors {
		v := c.stageBytes()
		if v < 0 || v > available {
			return s.fail(fmt.Errorf("nested LIST retained stage budget: %w", memory.ErrBudgetExceeded))
		}
		available -= v
	}
	if s.headers(1) < 0 || s.headers(1) > available {
		return s.fail(fmt.Errorf("nested LIST row headers: %w", memory.ErrBudgetExceeded))
	}
	lo, hi := 1, want
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		n := s.headers(mid)
		if n >= 0 && n <= available {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	if err := s.allocate(ctx, lo); err != nil {
		return s.fail(err)
	}
	rows := 0
	for rows < s.capacity && b.remaining > 0 {
		var err error
		for s.current < len(s.cursors) {
			err = s.cursors[s.current].assemble(ctx)
			if err != nil {
				break
			}
			s.current++
		}
		if err == nil {
			err = s.commit(ctx, rows)
		}
		if err != nil {
			if errors.Is(err, memory.ErrBudgetExceeded) {
				if rows > 0 {
					break
				}
				if s.capacity > 1 {
					s.clearOutput()
					if e := s.allocate(ctx, 1); e != nil {
						return s.fail(e)
					}
					continue
				}
			}
			return s.fail(err)
		}
		rows++
	}
	if err := s.check(ctx); err != nil {
		return s.fail(err)
	}
	cropStructBatch(b.batch, rows)
	bindStreamBatch(dst, b.batch)
	if dst.Int64Lists == nil {
		dst.Int64Lists = make(map[string]*memory.Int64ListVector)
	}
	if dst.StringLists == nil {
		dst.StringLists = make(map[string]*memory.StringListVector)
	}
	for _, c := range s.cursors {
		if c.array != nil {
			c.array.crop(rows)
			if c.parentID < 0 {
				if dst.Arrays == nil {
					dst.Arrays = make(map[string]*memory.ArrayNode)
				}
				dst.Arrays[c.plan.name] = c.array.vector
			}
			continue
		}
		if c.group != nil {
			c.group.crop(rows)
			if c.parentID < 0 {
				if dst.StructLists == nil {
					dst.StructLists = make(map[string]*memory.StructListVector)
				}
				dst.StructLists[c.plan.name] = c.group.vector
			}
			continue
		}
		if c.scalar {
			continue
		}
		if c.strings != nil {
			v := c.strings.output
			v.Offsets = v.Offsets[:rows+1]
			v.Validity = v.Validity[:memory.WordsFor(rows)]
			v.Values.Validity = v.Values.Validity[:memory.WordsFor(c.outputCount())]
			if c.parentID < 0 {
				dst.StringLists[c.plan.name] = v
			}
		} else {
			c.output.Offsets = c.output.Offsets[:rows+1]
			c.output.Validity = c.output.Validity[:memory.WordsFor(rows)]
			c.output.Values.Validity = c.output.Values.Validity[:memory.WordsFor(c.outputCount())]
			if c.parentID < 0 {
				dst.Int64Lists[c.plan.name] = c.output
			}
		}
	}
	if dst.Structs == nil {
		dst.Structs = make(map[string]*memory.StructVector)
	}
	for name, v := range b.batch.Structs {
		dst.Structs[name] = v
	}
	return true, nil
}

func (s *nestedListStream) observeParent(id int, value bool) error {
	if id < 0 {
		return nil
	}
	if s.parentSeen[id] && s.parentValid[id] != value {
		return fmt.Errorf("inconsistent Struct parent validity for %q", s.parentNames[id])
	}
	s.parentSeen[id] = true
	s.parentValid[id] = value
	return nil
}
