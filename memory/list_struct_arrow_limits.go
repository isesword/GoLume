package memory

import "fmt"

// checkedStructListArrowOffset bounds the new profile's Arrow signed offsets.
func checkedStructListArrowOffset(total, add uint64) (uint64, error) {
	const limit = uint64(1<<31 - 1)
	if total > limit || add > limit-total {
		return 0, fmt.Errorf("List<Struct> Arrow offset exceeds int32")
	}
	return total + add, nil
}

// validateStructListArrowLimits runs before builders allocate. Each nested List
// has its own element offset and each item String its own byte offset.
func validateStructListArrowLimits(b *Batch, f Field, offset, length int, parents [MaxStructDepth]Bitmap, depth int) error {
	if f.Type == TypeStruct {
		v := b.Structs[f.Name]
		parents[depth] = v.Validity
		for _, c := range f.Children {
			if err := validateStructListArrowLimits(v.Values, c, offset, length, parents, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	if !IsStructList(f) {
		return nil
	}
	v := b.StructLists[f.Name]
	visible := func(r int) bool {
		for i := 0; i < depth; i++ {
			if parents[i] != nil && parents[i][r>>6]&(uint64(1)<<uint(r&63)) == 0 {
				return false
			}
		}
		return v.IsValid(r)
	}
	var elements uint64
	for r := offset; r < offset+length; r++ {
		if visible(r) {
			var err error
			elements, err = checkedStructListArrowOffset(elements, uint64(v.Offsets[r+1]-v.Offsets[r]))
			if err != nil {
				return err
			}
		}
	}
	for _, c := range f.Children[0].Children {
		if c.Type != TypeString {
			continue
		}
		sv := v.Values.Values.Strings[c.Name]
		var bytes uint64
		for r := offset; r < offset+length; r++ {
			if !visible(r) {
				continue
			}
			for e := int(v.Offsets[r]); e < int(v.Offsets[r+1]); e++ {
				if v.Values.IsValid(e) && sv.IsValid(e) {
					var err error
					bytes, err = checkedStructListArrowOffset(bytes, uint64(sv.Offsets[e+1]-sv.Offsets[e]))
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}
