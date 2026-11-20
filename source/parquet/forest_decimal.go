package parquet

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/parquet/file"
	"gocolumn/memory"
)

// The page verifier consults Err between pages. Keep both stream creation and
// individual Next cancellation without a goroutine or changing reader lifetime.
type forestDecimalContext struct {
	context.Context
	base context.Context
}

func (c forestDecimalContext) Err() error { return fixedContext(c.base, c.Context) }
func (s *fixedStream) validateRootDecimalPages(ctx context.Context, rg *file.RowGroupReader, l fixedLeaf) error {
	if l.binding.Type != memory.TypeDecimal {
		return nil
	}
	if l.parent != "" || l.ancestors.count != 0 {
		return fmt.Errorf("nested Decimal fixed leaf is unsupported")
	}
	if err := fixedContext(s.ctx, ctx); err != nil {
		return err
	}
	physical := s.reader.MetaData().Schema.Column(l.binding.Index).PhysicalType()
	return validateDecimalPages(forestDecimalContext{Context: ctx, base: s.ctx}, rg, l.binding, physical)
}

// Only the long sign-extension scan needs checkpoints; after trimming the
// existing decoder handles at most sixteen bytes with unchanged error semantics.
func forestDecimalFromBigEndian(data []byte, p int32, check func() error) (memory.Decimal128, error) {
	if check != nil {
		if err := check(); err != nil {
			return memory.Decimal128{}, err
		}
	}
	for i := 0; len(data) > 16; i++ {
		if i&4095 == 0 && check != nil {
			if err := check(); err != nil {
				return memory.Decimal128{}, err
			}
		}
		if data[0] == 0 && data[1]&0x80 == 0 || data[0] == 0xff && data[1]&0x80 != 0 {
			data = data[1:]
		} else {
			return memory.Decimal128{}, fmt.Errorf("decimal coefficient exceeds signed 128-bit range")
		}
	}
	if check != nil {
		if err := check(); err != nil {
			return memory.Decimal128{}, err
		}
	}
	return memory.Decimal128FromBigEndian(data, p)
}

// Called only after the shared coordinator has admitted all repeated candidates.
// No post-consumption admission is performed, so budget-prefix retries cannot
// advance a root coefficient. Page bytes are converted before another read.
func (s *fixedStream) readRootDecimal(ctx context.Context, c file.ColumnChunkReader, l fixedLeaf) (int16, int, error) {
	if err := fixedContext(s.ctx, ctx); err != nil {
		return 0, 0, err
	}
	if l.parent != "" || l.ancestors.count != 0 {
		return 0, 0, fmt.Errorf("nested Decimal fixed leaf is unsupported")
	}
	s.scratch.decimal = memory.Decimal128{}
	s.scratch.defs[0] = l.maxDef
	var defs []int16
	if l.maxDef > 0 {
		defs = s.scratch.defs[:]
	}
	var total int64
	var count int
	var err error
	var data []byte
	bytesKind := false
	defer func() { s.scratch.decimalBytes[0] = nil; s.scratch.decimalFixed[0] = nil }()
	switch r := c.(type) {
	case *file.Int32ColumnChunkReader:
		total, count, err = r.ReadBatchInPage(1, s.scratch.i32[:], defs, nil)
		s.scratch.decimal = decimal128.FromI64(int64(s.scratch.i32[0]))
	case *file.Int64ColumnChunkReader:
		total, count, err = r.ReadBatchInPage(1, s.scratch.i64[:], defs, nil)
		s.scratch.decimal = decimal128.FromI64(s.scratch.i64[0])
	case *file.ByteArrayColumnChunkReader:
		total, count, err = r.ReadBatchInPage(1, s.scratch.decimalBytes[:], defs, nil)
		data = s.scratch.decimalBytes[0]
		bytesKind = true
	case *file.FixedLenByteArrayColumnChunkReader:
		total, count, err = r.ReadBatchInPage(1, s.scratch.decimalFixed[:], defs, nil)
		data = s.scratch.decimalFixed[0]
		bytesKind = true
	default:
		return 0, 0, fmt.Errorf("unsupported native root Decimal chunk %T", c)
	}
	if err != nil {
		return 0, 0, err
	}
	def := s.scratch.defs[0]
	if total != 1 || def < 0 || def > l.maxDef || (def == l.maxDef && count != 1) || (def != l.maxDef && count != 0) {
		return 0, 0, fmt.Errorf("root Decimal definition/value count mismatch")
	}
	if def == l.maxDef {
		if bytesKind {
			s.scratch.decimal, err = forestDecimalFromBigEndian(data, l.binding.DecimalPrecision, func() error { return fixedContext(s.ctx, ctx) })
			if err != nil {
				return 0, 0, err
			}
		}
		if !memory.Decimal128FitsPrecision(s.scratch.decimal, l.binding.DecimalPrecision) {
			return 0, 0, fmt.Errorf("decimal column %q coefficient exceeds precision %d", l.binding.Name, l.binding.DecimalPrecision)
		}
	} else {
		s.scratch.decimal = memory.Decimal128{}
	}
	if err := fixedContext(s.ctx, ctx); err != nil {
		return 0, 0, err
	}
	return def, count, nil
}
