package parquet

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"math"
)

type structListBuffer struct {
	vector *memory.StructVector
	slots  int
	lease  memory.Reservation
	charge int64
}

func (b *structListBuffer) close() {
	if b.lease != nil {
		b.lease.Release()
	}
	*b = structListBuffer{}
}
func groupBufferBytes(fields []memory.Field, n int, byteCaps []int) (int64, error) {
	if n < 0 || n >= math.MaxInt-63 || uint64(n) >= math.MaxUint32 || len(fields) != len(byteCaps) {
		return 0, fmt.Errorf("StructList capacity overflow: %w", memory.ErrBudgetExceeded)
	}
	total := int64(memory.WordsFor(n)) * 8
	for i, f := range fields {
		width := fixedWidth(f.Type)
		size := int64(0)
		if f.Type == memory.TypeString {
			if byteCaps[i] < 0 || uint64(byteCaps[i]) > math.MaxUint32 {
				return 0, fmt.Errorf("StructList String capacity overflow: %w", memory.ErrBudgetExceeded)
			}
			size = (int64(n)+1)*4 + int64(byteCaps[i])
		} else {
			if width <= 0 || int64(n) > math.MaxInt64/width {
				return 0, fmt.Errorf("StructList fixed size overflow")
			}
			size = int64(n) * width
		}
		size += int64(memory.WordsFor(n)) * 8
		if total > math.MaxInt64-size {
			return 0, fmt.Errorf("StructList bytes overflow")
		}
		total += size
	}
	return total, nil
}
func newGroupBuffer(ctx context.Context, c *nestedListCursor, fields []memory.Field, n int, caps []int) (*memory.StructVector, error) {
	v := &memory.StructVector{Fields: fields, Values: memory.NewBatch(), Validity: make(memory.Bitmap, memory.WordsFor(n)), Encoding: memory.EncodingView}
	for i, f := range fields {
		if err := c.check(ctx); err != nil {
			return nil, err
		}
		if f.Type == memory.TypeString {
			v.Values.Strings[f.Name] = &memory.StringViewVector{Offsets: make([]uint32, n+1), Data: make([]byte, 0, caps[i]), Validity: make(memory.Bitmap, memory.WordsFor(n)), Encoding: memory.EncodingView}
		} else {
			allocateFixedColumn(v.Values, f, n)
		}
	}
	return v, nil
}
func resizeGroupBuffer(v *memory.StructVector, n int) {
	v.Rows = n
	v.Values.Rows = n
	for _, f := range v.Fields {
		switch f.Type {
		case memory.TypeInt8:
			b := v.Values.Int8s[f.Name]
			b.Values = b.Values[:n]
		case memory.TypeInt16:
			b := v.Values.Int16s[f.Name]
			b.Values = b.Values[:n]
		case memory.TypeInt32, memory.TypeDate32:
			b := v.Values.Int32s[f.Name]
			b.Values = b.Values[:n]
		case memory.TypeInt64, memory.TypeTimestamp:
			b := v.Values.Int64s[f.Name]
			b.Values = b.Values[:n]
		case memory.TypeUint8:
			b := v.Values.Uint8s[f.Name]
			b.Values = b.Values[:n]
		case memory.TypeUint16:
			b := v.Values.Uint16s[f.Name]
			b.Values = b.Values[:n]
		case memory.TypeUint32, memory.TypeBool:
			b := v.Values.Uint32s[f.Name]
			b.Values = b.Values[:n]
		case memory.TypeUint64:
			b := v.Values.Uint64s[f.Name]
			b.Values = b.Values[:n]
		case memory.TypeFloat32:
			b := v.Values.Float32s[f.Name]
			b.Values = b.Values[:n]
		case memory.TypeFloat64:
			b := v.Values.Float64s[f.Name]
			b.Values = b.Values[:n]
		case memory.TypeString:
			b := v.Values.Strings[f.Name]
			b.Offsets = b.Offsets[:n+1]
		}
	}
}
func copyGroupBuffer(ctx context.Context, c *nestedListCursor, dst, src *memory.StructVector, start int) error {
	if src == nil {
		return nil
	}
	for row := 0; row < src.Rows; row++ {
		if row&255 == 0 {
			if err := c.check(ctx); err != nil {
				return err
			}
		}
		if src.IsValid(row) {
			memory.Set(dst.Validity, start+row)
		}
	}
	for _, f := range src.Fields {
		if err := c.check(ctx); err != nil {
			return err
		}
		switch f.Type {
		case memory.TypeInt8:
			a, b := dst.Values.Int8s[f.Name], src.Values.Int8s[f.Name]
			for row := 0; row < src.Rows; row++ {
				if row&1023 == 0 {
					if err := c.check(ctx); err != nil {
						return err
					}
				}
				a.Values[start+row] = b.Values[row]
				if b.IsValid(row) {
					memory.Set(a.Validity, start+row)
				}
			}
		case memory.TypeInt16:
			a, b := dst.Values.Int16s[f.Name], src.Values.Int16s[f.Name]
			for row := 0; row < src.Rows; row++ {
				if row&1023 == 0 {
					if err := c.check(ctx); err != nil {
						return err
					}
				}
				a.Values[start+row] = b.Values[row]
				if b.IsValid(row) {
					memory.Set(a.Validity, start+row)
				}
			}
		case memory.TypeInt32, memory.TypeDate32:
			a, b := dst.Values.Int32s[f.Name], src.Values.Int32s[f.Name]
			for row := 0; row < src.Rows; row++ {
				if row&1023 == 0 {
					if err := c.check(ctx); err != nil {
						return err
					}
				}
				a.Values[start+row] = b.Values[row]
				if b.IsValid(row) {
					memory.Set(a.Validity, start+row)
				}
			}
		case memory.TypeInt64, memory.TypeTimestamp:
			a, b := dst.Values.Int64s[f.Name], src.Values.Int64s[f.Name]
			for row := 0; row < src.Rows; row++ {
				if row&1023 == 0 {
					if err := c.check(ctx); err != nil {
						return err
					}
				}
				a.Values[start+row] = b.Values[row]
				if b.IsValid(row) {
					memory.Set(a.Validity, start+row)
				}
			}
		case memory.TypeUint8:
			a, b := dst.Values.Uint8s[f.Name], src.Values.Uint8s[f.Name]
			for row := 0; row < src.Rows; row++ {
				if row&1023 == 0 {
					if err := c.check(ctx); err != nil {
						return err
					}
				}
				a.Values[start+row] = b.Values[row]
				if b.IsValid(row) {
					memory.Set(a.Validity, start+row)
				}
			}
		case memory.TypeUint16:
			a, b := dst.Values.Uint16s[f.Name], src.Values.Uint16s[f.Name]
			for row := 0; row < src.Rows; row++ {
				if row&1023 == 0 {
					if err := c.check(ctx); err != nil {
						return err
					}
				}
				a.Values[start+row] = b.Values[row]
				if b.IsValid(row) {
					memory.Set(a.Validity, start+row)
				}
			}
		case memory.TypeUint32, memory.TypeBool:
			a, b := dst.Values.Uint32s[f.Name], src.Values.Uint32s[f.Name]
			for row := 0; row < src.Rows; row++ {
				if row&1023 == 0 {
					if err := c.check(ctx); err != nil {
						return err
					}
				}
				a.Values[start+row] = b.Values[row]
				if b.IsValid(row) {
					memory.Set(a.Validity, start+row)
				}
			}
		case memory.TypeUint64:
			a, b := dst.Values.Uint64s[f.Name], src.Values.Uint64s[f.Name]
			for row := 0; row < src.Rows; row++ {
				if row&1023 == 0 {
					if err := c.check(ctx); err != nil {
						return err
					}
				}
				a.Values[start+row] = b.Values[row]
				if b.IsValid(row) {
					memory.Set(a.Validity, start+row)
				}
			}
		case memory.TypeFloat32:
			a, b := dst.Values.Float32s[f.Name], src.Values.Float32s[f.Name]
			for row := 0; row < src.Rows; row++ {
				if row&1023 == 0 {
					if err := c.check(ctx); err != nil {
						return err
					}
				}
				a.Values[start+row] = b.Values[row]
				if b.IsValid(row) {
					memory.Set(a.Validity, start+row)
				}
			}
		case memory.TypeFloat64:
			a, b := dst.Values.Float64s[f.Name], src.Values.Float64s[f.Name]
			for row := 0; row < src.Rows; row++ {
				if row&1023 == 0 {
					if err := c.check(ctx); err != nil {
						return err
					}
				}
				a.Values[start+row] = b.Values[row]
				if b.IsValid(row) {
					memory.Set(a.Validity, start+row)
				}
			}
		case memory.TypeString:
			a, b := dst.Values.Strings[f.Name], src.Values.Strings[f.Name]
			old := len(a.Data)
			n := old + len(b.Data)
			a.Data = a.Data[:n]
			h := stringListStream{base: c.base}
			if err := h.copyBytes(ctx, a.Data[old:], b.Data); err != nil {
				return err
			}
			for row := 0; row <= src.Rows; row++ {
				if row&1023 == 0 {
					if err := c.check(ctx); err != nil {
						return err
					}
				}
				a.Offsets[start+row] = uint32(old) + b.Offsets[row]
			}
			for row := 0; row < src.Rows; row++ {
				if row&1023 == 0 {
					if err := c.check(ctx); err != nil {
						return err
					}
				}
				if b.IsValid(row) {
					memory.Set(a.Validity, start+row)
				}
			}
		}
	}
	return c.check(ctx)
}
