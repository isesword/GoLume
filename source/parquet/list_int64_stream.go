package parquet

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/parquet/file"
	"gocolumn/memory"
	"math"
)

type listTuple struct {
	value    [1]int64
	def, rep [1]int16
	present  bool
	valid    bool
}
type int64ListStream struct {
	admission                    *nestedListCursor
	base                         *fixedStream
	plan                         int64ListPlan
	listReader                   *file.Int64ColumnChunkReader
	tuple                        listTuple
	levels, expected, starts     int64
	groupRows                    int64
	eof                          bool
	stage                        []int64
	stageValid                   memory.Bitmap
	stageLease                   memory.Reservation
	mode                         int
	first, complete, parentValid bool
	output                       *memory.Int64ListVector
	outputLease                  memory.Reservation
	capacity                     int
}

func (s *int64ListStream) check(ctx context.Context) error { return fixedContext(s.base.ctx, ctx) }
func (s *int64ListStream) metadataBytes() int64            { return 2048 + int64(len(s.base.leaves)+1)*2048 }
func listElementBytes(n int) int64 {
	if n < 0 || int64(n) > (math.MaxInt64-8)/9 {
		return -1
	}
	r := int64(n)
	words := r / 64
	if r%64 != 0 {
		words++
	}
	return r*8 + words*8
}
func (s *int64ListStream) headers(rows int) int64 {
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
func (s *int64ListStream) clearStage() {
	if s.stageLease != nil {
		s.stageLease.Release()
		s.stageLease = nil
	}
	s.stage = nil
	s.stageValid = nil
	s.mode = 0
	s.first = false
	s.complete = false
	s.parentValid = false
}
func (s *int64ListStream) outputBytes() int64 {
	if s.output == nil {
		return 0
	}
	return listElementBytes(cap(s.output.Values.Values))
}
func (s *int64ListStream) copyElements(ctx context.Context, dst, src []int64) error {
	for len(src) > 0 {
		if err := s.check(ctx); err != nil {
			return err
		}
		n := min(len(src), 1024)
		copy(dst[:n], src[:n])
		dst, src = dst[n:], src[n:]
	}
	return s.check(ctx)
}
func (s *int64ListStream) growStage(ctx context.Context, needed int) error {
	if uint64(needed) > math.MaxUint32 || needed < 0 {
		return fmt.Errorf("LIST element offset overflow: %w", memory.ErrBudgetExceeded)
	}
	logical := listElementBytes(needed)
	hard := s.base.opts.BatchPayloadBytes
	if s.admission == nil && (logical < 0 || (hard > 0 && (s.headers(1) > hard || logical > hard-s.headers(1)))) {
		return fmt.Errorf("LIST row payload limit: %w", memory.ErrBudgetExceeded)
	}
	if s.admission != nil {
		if _, err := s.admission.owner.stageLimit(s.admission, logical); err != nil {
			return err
		}
	}
	if needed <= cap(s.stage) {
		return nil
	}
	old := listElementBytes(cap(s.stage))
	limit := s.base.opts.BatchBytes - s.metadataBytes() - s.headers(s.capacity) - s.outputBytes()
	if s.admission != nil {
		var err error
		limit, err = s.admission.owner.stageLimit(s.admission, logical)
		if err != nil {
			return err
		}
	}
	if old > limit || logical > limit-old {
		return fmt.Errorf("LIST staging peak: %w", memory.ErrBudgetExceeded)
	}
	target := needed
	if cap(s.stage) > 0 && cap(s.stage) <= math.MaxInt/2 {
		double := cap(s.stage) * 2
		n := listElementBytes(double)
		if uint64(double) <= math.MaxUint32 && n >= 0 && n <= limit-old {
			target = double
		}
	}
	size := listElementBytes(target)
	var lease memory.Reservation
	var err error
	if s.base.opts.MemoryManager != nil {
		lease, err = s.base.opts.MemoryManager.Reserve(ctx, size)
		if err != nil {
			return err
		}
	}
	values := make([]int64, len(s.stage), target)
	valid := make(memory.Bitmap, memory.WordsFor(target))
	if err = s.copyElements(ctx, values, s.stage); err != nil {
		if lease != nil {
			lease.Release()
		}
		return err
	}
	for i := range s.stageValid {
		if i&127 == 0 {
			if err = s.check(ctx); err != nil {
				if lease != nil {
					lease.Release()
				}
				return err
			}
		}
		valid[i] = s.stageValid[i]
	}
	if s.stageLease != nil {
		s.stageLease.Release()
	}
	s.stageLease = lease
	s.stage = values
	s.stageValid = valid
	return nil
}
