package parquet

import (
	"context"
	"fmt"
	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"gocolumn/memory"
	"math"
	"unicode/utf8"
)

type stringListTuple struct {
	value    [1]arrowparquet.ByteArray
	def, rep [1]int16
	present  bool
	valid    bool
}
type stringListStream struct {
	admission                    *nestedListCursor
	base                         *fixedStream
	plan                         int64ListPlan
	listReader                   *file.ByteArrayColumnChunkReader
	tuple                        stringListTuple
	levels, expected, starts     int64
	groupRows                    int64
	eof                          bool
	stageOffsets                 []uint32
	stageData                    []byte
	emptyOffsets                 [1]uint32
	stageValid                   memory.Bitmap
	stageLease                   memory.Reservation
	mode                         int
	first, complete, parentValid bool
	output                       *memory.StringListVector
	outputLease                  memory.Reservation
	capacity                     int
}

func (s *stringListStream) check(ctx context.Context) error { return fixedContext(s.base.ctx, ctx) }
func (s *stringListStream) metadataBytes() int64            { return 2048 + int64(len(s.base.leaves)+1)*2048 }

// stringElementBytes is logical child payload, including the terminal offset.
func stringElementBytes(n, bytes int) int64 {
	if n < 0 || n >= math.MaxInt || bytes < 0 || uint64(n) > math.MaxUint32 || uint64(bytes) > math.MaxUint32 {
		return -1
	}
	r := int64(n)
	return (r+1)*4 + ((r+63)/64)*8 + int64(bytes)
}
func (s *stringListStream) stageCount() int {
	if len(s.stageOffsets) == 0 {
		return 0
	}
	return len(s.stageOffsets) - 1
}
func (s *stringListStream) stageBytes() int64 {
	if s.stageOffsets == nil {
		return 0
	}
	return stringElementBytes(cap(s.stageOffsets)-1, cap(s.stageData))
}
func (s *stringListStream) outputCount() int { return len(s.output.Values.Offsets) - 1 }
func (s *stringListStream) headers(rows int) int64 {
	if rows < 0 || int64(rows) > (math.MaxInt64-4)/16 {
		return -1
	}
	r := int64(rows)
	words := r / 64
	if r%64 != 0 {
		words++
	}
	bits := words * 8
	n := (r+1)*4 + bits
	for _, f := range s.base.fields {
		if f.Type == memory.TypeList {
			continue
		}
		w := fixedWidth(f.Type)
		if r > (math.MaxInt64-bits)/w {
			return -1
		}
		v := r*w + bits
		if n > math.MaxInt64-v {
			return -1
		}
		n += v
	}
	return n
}
func (s *stringListStream) clearStage() {
	if s.stageLease != nil {
		s.stageLease.Release()
		s.stageLease = nil
	}
	s.stageOffsets = nil
	s.stageData = nil
	s.stageValid = nil
	s.mode = 0
	s.first = false
	s.complete = false
	s.parentValid = false
}
func (s *stringListStream) outputBytes() int64 {
	if s.output == nil || cap(s.output.Values.Offsets) <= 1 {
		return 0
	}
	return stringElementBytes(cap(s.output.Values.Offsets)-1, cap(s.output.Values.Data))
}
func (s *stringListStream) copyBytes(ctx context.Context, dst, src []byte) error {
	for len(src) > 0 {
		if e := s.check(ctx); e != nil {
			return e
		}
		n := min(len(src), 32*1024)
		copy(dst[:n], src[:n])
		dst, src = dst[n:], src[n:]
	}
	return s.check(ctx)
}
func (s *stringListStream) validateUTF8(ctx context.Context, src []byte) error {
	checkpoint := 0
	for i := 0; i < len(src); {
		if i >= checkpoint {
			if e := s.check(ctx); e != nil {
				return e
			}
			checkpoint = i + 32*1024
		}
		r, n := utf8.DecodeRune(src[i:])
		if r == utf8.RuneError && n == 1 {
			return fmt.Errorf("LIST String invalid UTF-8")
		}
		i += n
	}
	return s.check(ctx)
}
func (s *stringListStream) growStage(ctx context.Context, needed, bytes int) error {
	logical := stringElementBytes(needed, bytes)
	header := s.headers(1)
	if logical < 0 || header < 0 {
		return fmt.Errorf("LIST String offset overflow: %w", memory.ErrBudgetExceeded)
	}
	hard := s.base.opts.BatchPayloadBytes
	if s.admission == nil && hard > 0 && (header > hard || logical > hard-header) {
		return fmt.Errorf("LIST String row payload: %w", memory.ErrBudgetExceeded)
	}
	if s.admission != nil {
		if _, err := s.admission.owner.stageLimit(s.admission, logical); err != nil {
			return err
		}
	}
	if s.stageOffsets != nil && needed+1 <= cap(s.stageOffsets) && bytes <= cap(s.stageData) {
		return nil
	}
	old := s.stageBytes()
	limit := s.base.opts.BatchBytes - s.metadataBytes() - s.headers(s.capacity) - s.outputBytes()
	if s.admission != nil {
		var err error
		limit, err = s.admission.owner.stageLimit(s.admission, logical)
		if err != nil {
			return err
		}
	}
	if old > limit || logical > limit-old {
		return fmt.Errorf("LIST String staging peak: %w", memory.ErrBudgetExceeded)
	}
	slots, data := needed, bytes
	oldSlots := max(0, cap(s.stageOffsets)-1)
	if oldSlots > 0 && oldSlots <= math.MaxInt/2 {
		candidate := max(needed, oldSlots*2)
		n := stringElementBytes(candidate, data)
		if n >= 0 && n <= limit-old {
			slots = candidate
		}
	}
	if cap(s.stageData) > 0 && cap(s.stageData) <= math.MaxInt/2 {
		candidate := max(bytes, cap(s.stageData)*2)
		n := stringElementBytes(slots, candidate)
		if n >= 0 && n <= limit-old {
			data = candidate
		}
	}
	size := stringElementBytes(slots, data)
	var lease memory.Reservation
	if s.base.opts.MemoryManager != nil {
		var err error
		lease, err = s.base.opts.MemoryManager.Reserve(ctx, size)
		if err != nil {
			return err
		}
	}
	release := func() {
		if lease != nil {
			lease.Release()
		}
	}
	offsets := make([]uint32, s.stageCount()+1, slots+1)
	valid := make(memory.Bitmap, memory.WordsFor(slots))
	payload := make([]byte, len(s.stageData), data)
	if e := s.copyBytes(ctx, payload, s.stageData); e != nil {
		release()
		return e
	}
	for i, v := range s.stageOffsets {
		if i&1023 == 0 {
			if e := s.check(ctx); e != nil {
				release()
				return e
			}
		}
		offsets[i] = v
	}
	// A byte-driven replacement may shrink spare element capacity. Only
	// committed element bits belong to the logical stage.
	for i, v := range s.stageValid[:memory.WordsFor(s.stageCount())] {
		if i&127 == 0 {
			if e := s.check(ctx); e != nil {
				release()
				return e
			}
		}
		valid[i] = v
	}
	if s.stageLease != nil {
		s.stageLease.Release()
	}
	s.stageLease = lease
	s.stageOffsets = offsets
	s.stageValid = valid
	s.stageData = payload
	return nil
}
