// Package tuplesort provides an exact, budgeted external sorter for opaque keys.
package tuplesort

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"gocolumn/memory"
	"sort"
)

// ErrClosed reports operations after explicit closure.
var ErrClosed = errors.New("tuple sorter closed")

const codec = "golume.exact-tuples.v1"
const header = 16

// Config controls memory admission and query-owned spill storage. BlockBytes
// bounds regular encoded segments. Larger tuples use explicitly budgeted singleton
// segments. Without Spill, resident buffers grow until memory admission fails.
// KeepDuplicates disables deduplication, for callers sorting state records.
type Config struct {
	Manager        memory.Manager
	Spill          memory.SpillManager
	BlockBytes     int
	KeepDuplicates bool
}
type segment struct {
	handle               memory.SpillHandle
	next                 *segment
	ownerPrev, ownerNext *segment
	mem                  memory.Reservation
}
type run struct {
	first, last *segment
	depth       int
}

// Stats records cumulative physical writes and maximum merge depth, including
// intermediate runs. It remains available after EOF or Close.
type Stats struct {
	SpillRuns, SpillBytes int64
	MergeLevels           int
}

// Stats returns cumulative writes and merge depth, including after cleanup.
func (s *Sorter) Stats() Stats { return s.stats }

// Sorter is single-threaded. Close releases all memory and spill objects owned by it.
type Sorter struct {
	cfg              Config
	stats            Stats
	owned            *segment
	base, inputMem   memory.Reservation
	buf              []byte
	rows             [][]byte
	levels           [64]*run
	final            *run
	cursor           *cursor
	index            int
	finished, closed bool
	exhausted        bool
	err              error
}

// New validates the sorter configuration; buffers are admitted on first use.
func New(c Config) (*Sorter, error) {
	if c.Manager == nil {
		return nil, fmt.Errorf("tuple sorter requires a memory manager")
	}
	switch c.Spill.(type) {
	case memory.NoSpill, *memory.NoSpill:
		c.Spill = nil
	}
	if c.BlockBytes == 0 {
		c.BlockBytes = 64 * 1024
	}
	if c.BlockBytes < 32 || c.BlockBytes > int(^uint(0)>>1)/5 {
		return nil, fmt.Errorf("invalid tuple block size")
	}
	return &Sorter{cfg: c}, nil
}
func (s *Sorter) init(ctx context.Context) error {
	if s.base == nil {
		r, e := s.cfg.Manager.Reserve(ctx, 4096)
		if e != nil {
			return e
		}
		s.base = r
	}
	if s.inputMem == nil {
		n := s.cfg.BlockBytes
		r, e := s.cfg.Manager.Reserve(ctx, int64(n)+int64(n/8)*24)
		if e != nil {
			return e
		}
		s.inputMem = r
		s.buf = make([]byte, 0, n)
		s.rows = make([][]byte, 0, n/8)
	}
	return nil
}
func (s *Sorter) fail(e error) error {
	if e != nil {
		e = errors.Join(e, s.Close())
		s.err = e
	}
	return e
}

// Add copies key before returning. Failures close the sorter and clean up.
func (s *Sorter) Add(ctx context.Context, key []byte) error {
	if s.err != nil {
		return s.err
	}
	if s.closed {
		return ErrClosed
	}
	if s.finished {
		return fmt.Errorf("tuple sorter already finished")
	}
	if e := ctx.Err(); e != nil {
		return s.fail(e)
	}
	if len(key) > int(^uint(0)>>1)-header-8 {
		return s.fail(memory.ErrBudgetExceeded)
	}
	if e := s.init(ctx); e != nil {
		return s.fail(e)
	}
	if s.cfg.Spill != nil && len(key)+8 > s.cfg.BlockBytes {
		if e := s.flush(ctx); e != nil {
			return s.fail(e)
		}
		if e := s.singleton(ctx, key); e != nil {
			return s.fail(e)
		}
		return nil
	}
	if s.cfg.Spill != nil && len(s.buf)+len(key)+8 > s.cfg.BlockBytes {
		if e := s.flush(ctx); e != nil {
			return s.fail(e)
		}
	}
	if e := s.growInput(ctx, len(key)+8); e != nil {
		return s.fail(e)
	}
	start := len(s.buf)
	s.buf = append(s.buf, make([]byte, 8)...)
	s.buf = append(s.buf, key...)
	s.rows = append(s.rows, s.buf[start+8:])
	return nil
}

// growInput reserves the complete replacement while old storage is still live.
func (s *Sorter) growInput(ctx context.Context, extra int) error {
	maxInt := int(^uint(0) >> 1)
	if extra > maxInt-len(s.buf) {
		return memory.ErrBudgetExceeded
	}
	need := len(s.buf) + extra
	if need <= cap(s.buf) && len(s.rows) < cap(s.rows) {
		return nil
	}
	n := cap(s.buf)
	if n <= maxInt/2 {
		n *= 2
	}
	if n < need {
		n = need
	}
	nr := cap(s.rows)
	if len(s.rows) == nr {
		if nr > maxInt/2 {
			return memory.ErrBudgetExceeded
		}
		nr *= 2
	}
	if nr > maxInt/24 || int64(n) > int64(^uint64(0)>>1)-int64(nr)*24 {
		return memory.ErrBudgetExceeded
	}
	r, e := s.cfg.Manager.Reserve(ctx, int64(n)+int64(nr)*24)
	if e != nil {
		return e
	}
	buf := make([]byte, len(s.buf), n)
	copy(buf, s.buf)
	rows := make([][]byte, len(s.rows), nr)
	offset := 0
	for i, key := range s.rows {
		offset += 8
		rows[i] = buf[offset : offset+len(key)]
		offset += len(key)
	}
	s.buf = buf
	s.rows = rows
	s.inputMem.Release()
	s.inputMem = r
	return nil
}
func (s *Sorter) singleton(ctx context.Context, key []byte) error {
	n := len(key) + header + 8
	mem, e := s.cfg.Manager.Reserve(ctx, int64(n))
	if e != nil {
		return e
	}
	defer mem.Release()
	p := make([]byte, header, n)
	p = binary.LittleEndian.AppendUint64(p, uint64(len(key)))
	p = append(p, key...)
	r := &run{}
	if e = s.write(ctx, r, p, 1); e != nil {
		return e
	}
	return s.addRun(ctx, r)
}
func (s *Sorter) sorted() {
	sort.Slice(s.rows, func(i, j int) bool { return bytes.Compare(s.rows[i], s.rows[j]) < 0 })
}
func (s *Sorter) write(ctx context.Context, r *run, p []byte, count uint64) error {
	if s.cfg.Spill == nil {
		return memory.ErrSpillUnsupported
	}
	m, e := s.cfg.Manager.Reserve(ctx, 256)
	if e != nil {
		return e
	}
	binary.LittleEndian.PutUint64(p[:8], 1)
	binary.LittleEndian.PutUint64(p[8:16], count)
	provider, ok := s.cfg.Spill.(memory.MetadataSpillManager)
	if !ok {
		m.Release()
		return fmt.Errorf("tuple sorter requires budgeted spill metadata: %w", memory.ErrSpillUnsupported)
	}
	h, e := provider.SpillWithMetadata(ctx, memory.SpillObject{Kind: memory.SpillAggregateState, Codec: codec, Payload: p}, s.cfg.Manager)
	if e != nil {
		m.Release()
		return e
	}
	s.stats.SpillRuns++
	s.stats.SpillBytes += h.Bytes()
	x := &segment{handle: h, mem: m, ownerNext: s.owned}
	if s.owned != nil {
		s.owned.ownerPrev = x
	}
	s.owned = x
	if r.last == nil {
		r.first = x
	} else {
		r.last.next = x
	}
	r.last = x
	return nil
}

// deleteSegment retains ownership and metadata admission when deletion fails.
func (s *Sorter) deleteSegment(x *segment) error {
	if e := s.cfg.Spill.Delete(context.Background(), x.handle); e != nil {
		return e
	}
	if x.ownerPrev == nil {
		s.owned = x.ownerNext
	} else {
		x.ownerPrev.ownerNext = x.ownerNext
	}
	if x.ownerNext != nil {
		x.ownerNext.ownerPrev = x.ownerPrev
	}
	x.ownerNext = nil
	x.ownerPrev = nil
	x.mem.Release()
	x.mem = nil
	x.handle = nil
	x.next = nil
	return nil
}
func (s *Sorter) drop(r *run) error {
	if r == nil {
		return nil
	}
	var result error
	for x := r.first; x != nil; {
		next := x.next
		x.next = nil
		result = errors.Join(result, s.deleteSegment(x))
		x = next
	}
	r.first = nil
	r.last = nil
	return result
}
func (s *Sorter) flush(ctx context.Context) error {
	if len(s.rows) == 0 {
		return nil
	}
	s.sorted()
	mem, e := s.cfg.Manager.Reserve(ctx, int64(s.cfg.BlockBytes+header))
	if e != nil {
		return e
	}
	defer mem.Release()
	p := make([]byte, header, s.cfg.BlockBytes+header)
	var count uint64
	var prev []byte
	have := false
	for _, key := range s.rows {
		if !s.cfg.KeepDuplicates && have && bytes.Equal(prev, key) {
			continue
		}
		p = binary.LittleEndian.AppendUint64(p, uint64(len(key)))
		p = append(p, key...)
		count++
		prev = key
		have = true
	}
	r := &run{}
	if e = s.write(ctx, r, p, count); e != nil {
		return e
	}
	s.buf = s.buf[:0]
	clear(s.rows)
	s.rows = s.rows[:0]
	return s.addRun(ctx, r)
}
func (s *Sorter) addRun(ctx context.Context, r *run) error {
	for i := range s.levels {
		if s.levels[i] == nil {
			s.levels[i] = r
			return nil
		}
		old := s.levels[i]
		s.levels[i] = nil
		merged, e := s.merge(ctx, old, r)
		if e != nil {
			return errors.Join(e, s.drop(old), s.drop(r))
		}
		r = merged
	}
	return errors.Join(fmt.Errorf("too many tuple runs"), s.drop(r))
}

type cursor struct {
	s        *Sorter
	run      *run
	restored *memory.RestoredSpill
	payload  []byte
	remain   uint64
	key      []byte
	valid    bool
}

func (c *cursor) close() {
	if c.restored != nil {
		_ = c.restored.Close()
		c.restored = nil
	}
	c.payload = nil
	c.key = nil
}
func corrupt() error { return fmt.Errorf("tuple segment: %w", memory.ErrSpillCorrupt) }
func (c *cursor) advance(ctx context.Context) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if c.remain == 0 {
		if len(c.payload) != 0 {
			return corrupt()
		}
		c.close()
		if c.run.first == nil {
			c.valid = false
			return nil
		}
		x := c.run.first
		r, e := c.s.cfg.Spill.Restore(ctx, x.handle, [32]byte{}, c.s.cfg.Manager)
		if e != nil {
			return e
		}
		c.restored = r
		p := r.Object.Payload
		if r.Object.Kind != memory.SpillAggregateState || r.Object.Codec != codec || len(p) < header || binary.LittleEndian.Uint64(p[:8]) != 1 {
			return corrupt()
		}
		c.remain = binary.LittleEndian.Uint64(p[8:16])
		if len(p) > c.s.cfg.BlockBytes+header && c.remain != 1 {
			return corrupt()
		}
		c.payload = p[16:]
		if c.remain == 0 || c.remain > uint64(len(c.payload)/8) {
			return corrupt()
		}
		// Restore owns an admitted payload copy. The file and its metadata no
		// longer need to overlap the rest of the merge output. Advance the run
		// only after deletion succeeds so failures remain owned for Close retry.
		next := x.next
		if e = c.s.deleteSegment(x); e != nil {
			return e
		}
		c.run.first = next
		if next == nil {
			c.run.last = nil
		}
	}
	if len(c.payload) < 8 {
		return corrupt()
	}
	n := binary.LittleEndian.Uint64(c.payload[:8])
	c.payload = c.payload[8:]
	if n > uint64(len(c.payload)) {
		return corrupt()
	}
	c.key = c.payload[:int(n)]
	c.payload = c.payload[int(n):]
	c.remain--
	c.valid = true
	if c.remain == 0 && len(c.payload) != 0 {
		return corrupt()
	}
	return nil
}
func (s *Sorter) merge(ctx context.Context, a, b *run) (out *run, err error) {
	out = &run{depth: max(a.depth, b.depth) + 1}
	s.stats.MergeLevels = max(s.stats.MergeLevels, out.depth)
	defer func() {
		if err != nil {
			err = errors.Join(err, s.drop(out))
		}
	}()
	ca := &cursor{s: s, run: a}
	cb := &cursor{s: s, run: b}
	defer ca.close()
	defer cb.close()
	mem, e := s.cfg.Manager.Reserve(ctx, int64(2*s.cfg.BlockBytes+header))
	if e != nil {
		return out, e
	}
	defer func() { mem.Release() }()
	p := make([]byte, header, s.cfg.BlockBytes+header)
	prev := make([]byte, 0, s.cfg.BlockBytes)
	have := false
	var count uint64
	if e = ca.advance(ctx); e != nil {
		return out, e
	}
	if e = cb.advance(ctx); e != nil {
		return out, e
	}
	for ca.valid || cb.valid {
		c := ca
		if !ca.valid || (cb.valid && bytes.Compare(cb.key, ca.key) < 0) {
			c = cb
		}
		key := c.key
		if s.cfg.KeepDuplicates || !have || !bytes.Equal(key, prev) {
			if count > 0 && len(p)+8+len(key) > s.cfg.BlockBytes+header {
				if e = s.write(ctx, out, p, count); e != nil {
					return out, e
				}
				p = p[:header]
				count = 0
			}
			needed := len(p) + 8 + len(key)
			if needed > cap(p) || len(key) > cap(prev) {
				pc := max(cap(p), needed)
				vc := max(cap(prev), len(key))
				if int64(pc) > int64(^uint64(0)>>1)-int64(vc) {
					return out, memory.ErrBudgetExceeded
				}
				next, e := s.cfg.Manager.Reserve(ctx, int64(pc)+int64(vc))
				if e != nil {
					return out, e
				}
				np := make([]byte, len(p), pc)
				copy(np, p)
				nv := make([]byte, len(prev), vc)
				copy(nv, prev)
				p = np
				prev = nv
				mem.Release()
				mem = next
			}
			p = binary.LittleEndian.AppendUint64(p, uint64(len(key)))
			p = append(p, key...)
			count++
			prev = append(prev[:0], key...)
			have = true
			if len(key)+8 > s.cfg.BlockBytes {
				if e = s.write(ctx, out, p, count); e != nil {
					return out, e
				}
				p = p[:header]
				count = 0
			}
		}
		if e = c.advance(ctx); e != nil {
			return out, e
		}
	}
	if count > 0 {
		if e = s.write(ctx, out, p, count); e != nil {
			return out, e
		}
	}
	if e = s.drop(a); e != nil {
		return out, e
	}
	if e = s.drop(b); e != nil {
		return out, e
	}
	return out, nil
}

// Finish seals ingestion. Small inputs are sorted without touching the spill store.
func (s *Sorter) Finish(ctx context.Context) error {
	if s.err != nil {
		return s.err
	}
	if s.closed {
		return ErrClosed
	}
	if s.finished {
		return ctx.Err()
	}
	if e := ctx.Err(); e != nil {
		return s.fail(e)
	}
	spilled := false
	for _, r := range s.levels {
		if r != nil {
			spilled = true
			break
		}
	}
	if !spilled {
		s.sorted()
		s.finished = true
		return nil
	}
	if e := s.flush(ctx); e != nil {
		return s.fail(e)
	}
	s.buf = nil
	s.rows = nil
	if s.inputMem != nil {
		s.inputMem.Release()
		s.inputMem = nil
	}
	for i, r := range s.levels {
		if r == nil {
			continue
		}
		s.levels[i] = nil
		if s.final == nil {
			s.final = r
		} else {
			out, e := s.merge(ctx, s.final, r)
			if e != nil {
				return s.fail(errors.Join(e, s.drop(r)))
			}
			s.final = out
		}
	}
	s.cursor = &cursor{s: s, run: s.final}
	s.finished = true
	return nil
}

// Next returns a borrowed tuple valid until the next Next or Close call.
func (s *Sorter) Next(ctx context.Context) ([]byte, bool, error) {
	if s.err != nil {
		return nil, false, s.err
	}
	if s.exhausted {
		return nil, false, nil
	}
	if s.closed {
		return nil, false, ErrClosed
	}
	if !s.finished {
		return nil, false, fmt.Errorf("tuple sorter not finished")
	}
	if e := ctx.Err(); e != nil {
		return nil, false, s.fail(e)
	}
	if s.cursor != nil {
		if e := s.cursor.advance(ctx); e != nil {
			return nil, false, s.fail(e)
		}
		if !s.cursor.valid {
			return s.eof()
		}
		return s.cursor.key, s.cursor.valid, nil
	}
	if s.index >= len(s.rows) {
		return s.eof()
	}
	key := s.rows[s.index]
	s.index++
	if !s.cfg.KeepDuplicates {
		for s.index < len(s.rows) && bytes.Equal(key, s.rows[s.index]) {
			s.index++
		}
	}
	return key, true, nil
}
func (s *Sorter) eof() ([]byte, bool, error) {
	s.exhausted = true
	e := s.Close()
	if e != nil {
		s.err = e
	}
	return nil, false, e
}

// Close releases owned resources and retries failed object deletions.
func (s *Sorter) Close() error {
	if s.closed && s.owned == nil {
		return nil
	}
	s.closed = true
	if s.cursor != nil {
		s.cursor.close()
		s.cursor = nil
	}
	var result error
	for x := s.owned; x != nil; {
		next := x.ownerNext
		x.next = nil
		result = errors.Join(result, s.deleteSegment(x))
		x = next
	}
	clear(s.levels[:])
	s.final = nil
	s.rows = nil
	s.buf = nil
	if s.inputMem != nil {
		s.inputMem.Release()
		s.inputMem = nil
	}
	if s.base != nil && s.owned == nil {
		s.base.Release()
		s.base = nil
	}
	return result
}
