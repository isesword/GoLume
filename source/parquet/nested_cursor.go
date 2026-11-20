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

// One common level/row state machine; only the owned storage is typed.
type nestedListCursor struct {
	array                           *nestedArrayCursor
	group                           *nestedStructListGroup
	ancestors                       nestedAncestorChain
	witness                         [memory.MaxStructDepth]bool
	scalar                          bool
	parentName                      string
	parentID                        int
	ancestorOptional, ancestorValid bool
	scalarOutput                    *memory.StringViewVector
	scalarLease                     memory.Reservation
	scalarRows                      int
	candidateBytes                  int64

	int64ListStream
	owner              *nestedListStream
	kind               memory.DataType
	strings            *stringListStream
	byteReader         *file.ByteArrayColumnChunkReader
	bytes              [1]arrowparquet.ByteArray
	target             int
	targetBytes        int
	replacement        []int64
	replacementOffsets []uint32
	replacementData    []byte
	replacementValid   memory.Bitmap
	replacementLease   memory.Reservation
}

func (c *nestedListCursor) stageCount() int {
	if c.array != nil {
		n, _, _ := c.array.stage.counts()
		return n
	}
	if c.group != nil {
		return c.group.count(&c.group.stage)
	}
	if c.scalar {
		if c.complete {
			return 1
		}
		return 0
	}
	if c.strings != nil {
		return c.strings.stageCount()
	}
	return len(c.stage)
}
func (c *nestedListCursor) stageBytes() int64 {
	if c.array != nil {
		return c.array.stage.charge
	}
	if c.group != nil {
		return c.group.stage.charge
	}
	if c.scalar {
		return 0
	}
	if c.strings != nil {
		return c.strings.stageBytes()
	}
	return listElementBytes(cap(c.stage))
}
func (c *nestedListCursor) stagePayload() int64 {
	if c.array != nil {
		return c.array.payload(&c.array.stage)
	}
	if c.group != nil {
		return c.group.payload(&c.group.stage)
	}
	if c.scalar {
		if c.tuple.present && c.tuple.valid {
			return int64(len(c.bytes[0]))
		}
		return 0
	}
	if c.strings != nil {
		return stringElementBytes(c.strings.stageCount(), len(c.strings.stageData))
	}
	return listElementBytes(len(c.stage))
}
func (c *nestedListCursor) outputCount() int {
	if c.array != nil {
		n, _, _ := c.array.output.counts()
		return n
	}
	if c.group != nil {
		return c.group.count(&c.group.output)
	}
	if c.scalar {
		return c.scalarRows
	}
	if c.strings != nil {
		return c.strings.outputCount()
	}
	return len(c.output.Values.Values)
}
func (c *nestedListCursor) outputBytes() int64 {
	if c.array != nil {
		return c.array.output.charge
	}
	if c.group != nil {
		return c.group.output.charge
	}
	if c.scalar {
		if c.scalarOutput == nil {
			return 0
		}
		return int64(cap(c.scalarOutput.Data))
	}
	if c.strings != nil {
		return c.strings.outputBytes()
	}
	return c.int64ListStream.outputBytes()
}
func (c *nestedListCursor) outputPayload() int64 {
	if c.array != nil {
		return c.array.payload(&c.array.output)
	}
	if c.group != nil {
		return c.group.payload(&c.group.output)
	}
	if c.scalar {
		if c.scalarOutput == nil {
			return 0
		}
		return int64(len(c.scalarOutput.Data))
	}
	if c.strings != nil {
		return stringElementBytes(c.outputCount(), len(c.strings.output.Values.Data))
	}
	return listElementBytes(c.outputCount())
}
func (c *nestedListCursor) clearStage() {
	if c.array != nil {
		c.array.stage.close()
		c.array.stack = [memory.MaxRepeatedListDepth]repeatedFrame{}
		c.int64ListStream.clearStage()
		return
	}
	if c.group != nil {
		c.group.clearStage()
		return
	}
	c.int64ListStream.clearStage()
	if c.strings != nil {
		c.strings.clearStage()
	}
}
func (c *nestedListCursor) closeReader() error {
	if c.group != nil {
		return c.group.closeReaders()
	}
	c.bytes[0] = nil
	if c.byteReader != nil {
		e := c.byteReader.Close()
		c.byteReader = nil
		return e
	}
	if c.listReader != nil {
		e := c.listReader.Close()
		c.listReader = nil
		return e
	}
	return nil
}
func (c *nestedListCursor) hasReader() bool {
	if c.group != nil {
		for i := range c.group.leaves {
			if c.group.leaves[i].reader != nil {
				return true
			}
		}
		return false
	}
	return c.byteReader != nil || c.listReader != nil
}
func (c *nestedListCursor) openReader(ctx context.Context, r *file.RowGroupReader) error {
	if c.kind == memory.TypeTime64 {
		binding := columnBinding{Name: c.plan.name, Index: c.plan.index}
		if err := validateParquetDataPages(func() error { return c.check(ctx) }, r, binding, arrowparquet.Types.Int64, "wall-clock list", time64ValueEncodingSupported, c.expected); err != nil {
			return fmt.Errorf("LIST %q page preflight: %w", c.plan.name, err)
		}
	}
	if c.group != nil {
		return c.group.open(ctx, r)
	}
	reader, err := r.Column(c.plan.index)
	if err != nil {
		return err
	}
	var ok bool
	if c.kind == memory.TypeString {
		c.byteReader, ok = reader.(*file.ByteArrayColumnChunkReader)
	} else {
		c.listReader, ok = reader.(*file.Int64ColumnChunkReader)
	}
	if !ok {
		reader.Close()
		return fmt.Errorf("nested LIST reader kind mismatch")
	}
	return nil
}
func (c *nestedListCursor) peek(ctx context.Context) error {
	if c.tuple.present || c.eof {
		return nil
	}
	if err := c.check(ctx); err != nil {
		return err
	}
	t := &c.tuple
	t.value[0] = 0
	t.def[0], t.rep[0] = 0, 0
	c.bytes[0] = nil
	var total int64
	var count int
	var err error
	if c.strings != nil {
		total, count, err = c.byteReader.ReadBatchInPage(1, c.bytes[:], t.def[:], t.rep[:])
	} else {
		total, count, err = c.listReader.ReadBatchInPage(1, t.value[:], t.def[:], t.rep[:])
	}
	if err != nil {
		if errors.Is(err, io.EOF) && total == 0 && count == 0 && c.levels == c.expected && c.starts == c.groupRows {
			c.eof = true
			return nil
		}
		return err
	}
	if total == 0 {
		if count != 0 || c.levels != c.expected {
			return fmt.Errorf("nested LIST EOF count mismatch")
		}
		c.eof = true
		return nil
	}
	if total != 1 || c.levels >= c.expected {
		return fmt.Errorf("nested LIST excess/invalid level count")
	}
	c.levels++
	maxRep := int16(1)
	if c.array != nil {
		maxRep = int16(c.array.plan.depth)
	}
	if t.rep[0] < 0 || (t.rep[0] > maxRep || (c.scalar && t.rep[0] != 0)) || t.def[0] < 0 || t.def[0] > c.plan.maxDef {
		return fmt.Errorf("nested LIST invalid levels")
	}
	t.valid = t.def[0] == c.plan.maxDef
	if (t.valid && count != 1) || (!t.valid && count != 0) {
		return fmt.Errorf("nested LIST dense count mismatch")
	}
	if t.rep[0] == 0 {
		if c.starts >= c.groupRows {
			return fmt.Errorf("nested LIST excess row starts")
		}
		c.starts++
	}
	t.present = true
	return nil
}
func (c *nestedListCursor) assemble(ctx context.Context) error {
	if c.array != nil {
		return c.array.assemble(ctx)
	}
	if c.group != nil {
		return c.group.assemble(ctx)
	}
	if c.scalar {
		if c.complete {
			return nil
		}
		if err := c.peek(ctx); err != nil {
			return err
		}
		if !c.tuple.present {
			return fmt.Errorf("scalar String premature EOF")
		}
		c.captureAncestors(c.tuple.def[0])
		c.parentValid = c.tuple.valid
		if !c.tuple.valid {
			c.bytes[0] = nil
		}
		if _, err := c.owner.stageLimit(c, c.stagePayload()); err != nil {
			return err
		}
		c.complete = true
		return nil
	}

	if c.complete {
		return nil
	}
	p := int16(0)
	if c.plan.optional {
		p = 1
	}
	a := c.ancestors.optionalCount()
	p += a
	for {
		if err := c.peek(ctx); err != nil {
			return err
		}
		if c.mode == 0 {
			if !c.tuple.present || c.tuple.rep[0] != 0 {
				return fmt.Errorf("nested LIST row must begin at rep0")
			}
			c.captureAncestors(c.tuple.def[0])
			c.parentValid = c.ancestorValid && c.tuple.def[0] >= p
			if c.tuple.def[0] <= p {
				c.mode = 1
				c.tuple.present = false
				c.bytes[0] = nil
			} else {
				c.mode = 2
				c.first = true
			}
			continue
		}
		if !c.tuple.present {
			if !c.eof || c.base.remaining != 1 {
				return fmt.Errorf("nested LIST premature EOF")
			}
			c.complete = true
			return nil
		}
		if c.mode == 1 {
			if c.tuple.rep[0] != 0 {
				return fmt.Errorf("nested LIST placeholder continuation")
			}
			c.complete = true
			return nil
		}
		if !c.first && c.tuple.rep[0] == 0 {
			c.complete = true
			return nil
		}
		if (c.first && c.tuple.rep[0] != 0) || (!c.first && c.tuple.rep[0] != 1) || c.tuple.def[0] < p+1 {
			return fmt.Errorf("nested LIST bad element continuation")
		}
		count := c.stageCount()
		if count == math.MaxInt {
			return fmt.Errorf("nested LIST element overflow: %w", memory.ErrBudgetExceeded)
		}
		if c.kind == memory.TypeTime64 && c.tuple.valid && (c.tuple.value[0] < 0 || c.tuple.value[0] >= dayNanoseconds) {
			return fmt.Errorf("LIST %q wall-clock time outside [0, 24h)", c.plan.name)
		}
		if c.strings == nil {
			if err := c.int64ListStream.growStage(ctx, count+1); err != nil {
				return err
			}
			c.stage = c.stage[:count+1]
			c.stage[count] = 0
			if c.tuple.valid {
				c.stage[count] = c.tuple.value[0]
				memory.Set(c.stageValid, count)
			}
		} else {
			s := c.strings
			value := c.bytes[0]
			if !c.tuple.valid {
				value = nil
			}
			if len(value) > math.MaxInt-len(s.stageData) {
				return fmt.Errorf("nested String byte overflow: %w", memory.ErrBudgetExceeded)
			}
			n := len(s.stageData) + len(value)
			if err := s.growStage(ctx, count+1, n); err != nil {
				return err
			}
			if err := s.validateUTF8(ctx, value); err != nil {
				return err
			}
			old := len(s.stageData)
			s.stageData = s.stageData[:n]
			if err := s.copyBytes(ctx, s.stageData[old:], value); err != nil {
				return err
			}
			s.stageOffsets = s.stageOffsets[:count+2]
			s.stageOffsets[count+1] = uint32(n)
			if c.tuple.valid {
				memory.Set(s.stageValid, count)
			}
		}
		c.tuple.present = false
		c.bytes[0] = nil
		c.first = false
	}
}
func (c *nestedListCursor) prepareSize() (int64, int64, error) {
	if c.array != nil {
		return c.array.prepareSize()
	}
	if c.group != nil {
		return c.group.prepareSize()
	}
	if c.scalar {
		old := len(c.scalarOutput.Data)
		add := len(c.bytes[0])
		if add > math.MaxInt-old || uint64(old)+uint64(add) > math.MaxUint32 {
			return 0, 0, fmt.Errorf("scalar String byte overflow: %w", memory.ErrBudgetExceeded)
		}
		c.targetBytes = old + add
		c.candidateBytes = 0
		if add > 0 {
			c.candidateBytes = int64(c.targetBytes)
		}
		return int64(c.targetBytes), c.candidateBytes, nil
	}

	old, count := c.outputCount(), c.stageCount()
	if count > math.MaxInt-old || uint64(old)+uint64(count) > math.MaxUint32 {
		return 0, 0, fmt.Errorf("nested element offset overflow: %w", memory.ErrBudgetExceeded)
	}
	c.target = old + count
	var size int64
	if c.strings != nil {
		s := c.strings
		if len(s.stageData) > math.MaxInt-len(s.output.Values.Data) {
			return 0, 0, fmt.Errorf("nested bytes overflow: %w", memory.ErrBudgetExceeded)
		}
		c.targetBytes = len(s.stageData) + len(s.output.Values.Data)
		size = stringElementBytes(c.target, c.targetBytes)
	} else {
		size = listElementBytes(c.target)
	}
	if size < 0 {
		return 0, 0, fmt.Errorf("nested payload overflow: %w", memory.ErrBudgetExceeded)
	}
	extra := int64(0)
	if old > 0 && count > 0 {
		extra = size
	}
	c.candidateBytes = extra
	return size, extra, nil
}
func (c *nestedListCursor) copyCandidate(ctx context.Context) error {
	if c.array != nil {
		return c.array.copyCandidate(ctx)
	}
	if c.group != nil {
		return c.group.copyCandidate(ctx)
	}
	if c.scalar {
		if err := c.strings.validateUTF8(ctx, c.bytes[0]); err != nil {
			return err
		}
		if c.candidateBytes == 0 {
			return c.check(ctx)
		}
		c.replacementData = make([]byte, c.targetBytes)
		old := len(c.scalarOutput.Data)
		if err := c.strings.copyBytes(ctx, c.replacementData[:old], c.scalarOutput.Data); err != nil {
			return err
		}
		return c.strings.copyBytes(ctx, c.replacementData[old:], c.bytes[0])
	}

	if err := c.check(ctx); err != nil {
		return err
	}
	old := c.outputCount()
	if old == 0 || c.stageCount() == 0 {
		return nil
	}
	n := c.target
	c.replacementValid = make(memory.Bitmap, memory.WordsFor(n))
	if c.strings == nil {
		c.replacement = make([]int64, n)
		if e := c.copyElements(ctx, c.replacement[:old], c.output.Values.Values); e != nil {
			return e
		}
		if e := c.copyElements(ctx, c.replacement[old:], c.stage); e != nil {
			return e
		}
	} else {
		s := c.strings
		c.replacementOffsets = make([]uint32, n+1)
		c.replacementData = make([]byte, c.targetBytes)
		oldBytes := len(s.output.Values.Data)
		if e := s.copyBytes(ctx, c.replacementData[:oldBytes], s.output.Values.Data); e != nil {
			return e
		}
		if e := s.copyBytes(ctx, c.replacementData[oldBytes:], s.stageData); e != nil {
			return e
		}
		for i := 0; i <= n; i++ {
			if i&1023 == 0 {
				if e := c.check(ctx); e != nil {
					return e
				}
			}
			if i <= old {
				c.replacementOffsets[i] = s.output.Values.Offsets[i]
			} else {
				c.replacementOffsets[i] = uint32(oldBytes) + s.stageOffsets[i-old]
			}
		}
	}
	for i := 0; i < n; i++ {
		if i&1023 == 0 {
			if e := c.check(ctx); e != nil {
				return e
			}
		}
		var valid bool
		if c.strings == nil {
			if i < old {
				valid = memory.IsSet(c.output.Values.Validity, i)
			} else {
				valid = memory.IsSet(c.stageValid, i-old)
			}
		} else {
			if i < old {
				valid = memory.IsSet(c.strings.output.Values.Validity, i)
			} else {
				valid = memory.IsSet(c.strings.stageValid, i-old)
			}
		}
		if valid {
			memory.Set(c.replacementValid, i)
		}
	}
	return c.check(ctx)
}
func (c *nestedListCursor) commitCandidate(row int) {
	if c.array != nil {
		c.array.commit(row)
		return
	}
	if c.group != nil {
		c.group.commit(row)
		return
	}
	if c.scalar {
		if c.replacementData != nil {
			if c.scalarLease != nil {
				c.scalarLease.Release()
			}
			c.scalarOutput.Data = c.replacementData
			c.scalarLease = c.replacementLease
			c.replacementLease = nil
			c.replacementData = nil
		}
		c.scalarOutput.Offsets[row+1] = uint32(len(c.scalarOutput.Data))
		if c.parentValid {
			memory.Set(c.scalarOutput.Validity, row)
		}
		c.scalarRows++
		c.bytes[0] = nil
		c.tuple.present = false
		c.clearStage()
		return
	}

	if c.strings != nil {
		s := c.strings
		if c.outputCount() == 0 && c.stageCount() > 0 {
			if s.outputLease != nil {
				s.outputLease.Release()
			}
			s.output.Values.Offsets, s.output.Values.Data, s.output.Values.Validity = s.stageOffsets, s.stageData, s.stageValid
			s.outputLease = s.stageLease
			s.stageLease = nil
			s.stageOffsets = nil
			s.stageData = nil
			s.stageValid = nil
		} else if c.replacementOffsets != nil {
			if s.outputLease != nil {
				s.outputLease.Release()
			}
			s.outputLease = c.replacementLease
			c.replacementLease = nil
			s.output.Values.Offsets, s.output.Values.Data, s.output.Values.Validity = c.replacementOffsets, c.replacementData, c.replacementValid
		}
		if c.parentValid {
			memory.Set(s.output.Validity, row)
		}
		s.output.Offsets[row+1] = uint32(c.target)
	} else {
		if c.outputCount() == 0 {
			if c.outputLease != nil {
				c.outputLease.Release()
			}
			c.output.Values.Values, c.output.Values.Validity = c.stage, c.stageValid
			c.outputLease = c.stageLease
			c.stageLease = nil
			c.stage = nil
			c.stageValid = nil
		} else if c.replacement != nil {
			if c.outputLease != nil {
				c.outputLease.Release()
			}
			c.outputLease = c.replacementLease
			c.replacementLease = nil
			c.output.Values.Values, c.output.Values.Validity = c.replacement, c.replacementValid
		}
		if c.parentValid {
			memory.Set(c.output.Validity, row)
		}
		c.output.Offsets[row+1] = uint32(c.target)
	}
	c.replacement = nil
	c.replacementOffsets = nil
	c.replacementData = nil
	c.replacementValid = nil
	c.clearStage()
}

func (c *nestedListCursor) finishScalarGroup(ctx context.Context) error {
	if !c.scalar || c.base.remaining != 1 {
		return nil
	}
	c.tuple.present = false
	if err := c.peek(ctx); err != nil {
		return err
	}
	if !c.eof || c.tuple.present || c.levels != c.expected || c.starts != c.groupRows {
		return fmt.Errorf("scalar String trailing row/level")
	}
	return nil
}

func (c *nestedListCursor) captureAncestors(def int16) {
	for i := 0; i < c.ancestors.count; i++ {
		c.witness[i] = def >= c.ancestors.thresholds[i]
	}
	c.ancestorValid = def >= c.ancestors.optionalCount()
}
