package memory

import (
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/array"
	"math"
)

func validateArrayArrowLimits(b *Batch, f Field, offset, length int, parents [MaxStructDepth]Bitmap, depth int) error {
	if f.Type == TypeStruct {
		v := b.Structs[f.Name]
		if v == nil || depth >= MaxStructDepth {
			return fmt.Errorf("invalid array Arrow Struct")
		}
		parents[depth] = v.Validity
		for _, c := range f.Children {
			if err := validateArrayArrowLimits(v.Values, c, offset, length, parents, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	if !IsNestedList(f) {
		return nil
	}
	v := b.Arrays[f.Name]
	if v == nil {
		return fmt.Errorf("missing array Arrow column")
	}
	var counts [3]uint64
	for r := offset; r < offset+length; r++ {
		visible := true
		for d := 0; d < depth; d++ {
			if parents[d] != nil && !IsSet(parents[d], r) {
				visible = false
				break
			}
		}
		if !visible || !v.IsValid(r) {
			continue
		}
		lo, hi, _ := v.ValueRange(r)
		var err error
		counts[0], err = addArrayArrowCount(counts[0], uint64(hi-lo))
		if err != nil {
			return err
		}
		for i := lo; i < hi; i++ {
			if !v.Child.IsValid(i) {
				continue
			}
			a, z, _ := v.Child.ValueRange(i)
			counts[1], err = addArrayArrowCount(counts[1], uint64(z-a))
			if err != nil {
				return err
			}
			if v.Child.Child.Kind == TypeString {
				leaf := v.Child.Child
				for e := a; e < z; e++ {
					if leaf.IsValid(e) {
						counts[2], err = addArrayArrowCount(counts[2], uint64(leaf.Strings.Offsets[e+1]-leaf.Strings.Offsets[e]))
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return nil
}
func appendArrowArrayNode(builder array.Builder, v *ArrayNode, row int) error {
	if !v.IsValid(row) {
		builder.AppendNull()
		return nil
	}
	switch v.Kind {
	case TypeList:
		b, ok := builder.(*array.ListBuilder)
		if !ok {
			return fmt.Errorf("array node builder mismatch")
		}
		lo, hi, ok := v.ValueRange(row)
		if !ok {
			return fmt.Errorf("invalid array row")
		}
		b.Append(true)
		for e := lo; e < hi; e++ {
			if err := appendArrowArrayNode(b.ValueBuilder(), v.Child, e); err != nil {
				return err
			}
		}
	case TypeInt64:
		builder.(*array.Int64Builder).Append(v.Int64.Values[row])
	case TypeString:
		builder.(*array.StringBuilder).Append(string(v.Strings.ValueBytes(row)))
	default:
		return fmt.Errorf("unsupported array node builder kind")
	}
	return nil
}

func addArrayArrowCount(current, additional uint64) (uint64, error) {
	if current > math.MaxInt32 || additional > uint64(math.MaxInt32)-current {
		return 0, fmt.Errorf("Arrow nested array offset exceeds signed int32")
	}
	return current + additional, nil
}
