package memory

import "fmt"

// AppendStructRow iterates Fields, not runtime maps. Repeated fields therefore
// append repeatedly to one destination vector. Count each occurrence without
// allocating a seen-name set. The caller takes max(map payload, this payload),
// preserving the previous conservative accounting for unused runtime vectors.
// A missing numeric/list child is synthesized as NULL by the existing clone;
// charge its newly created descriptors as well as values and validity.
func nativeStructAppendEstimate(src *StructVector, offset, length int, inheritedNulls bool) (int64, int64, error) {
	total, metadata := int64(0), int64(0)
	add := func(n int64) error { var err error; total, err = ownedRangeAdd(total, n, 1); return err }
	missing := func(n int64) error { var err error; metadata, err = ownedRangeAdd(metadata, n, 1); return err }
	validity := func(v Bitmap) int64 {
		if length == 0 || (v == nil && !inheritedNulls) {
			return 0
		}
		return int64(WordsFor(length)) * 8
	}
	for _, field := range src.Fields {
		switch field.Type {
		case TypeInt8:
			v := src.Values.Int8s[field.Name]
			n, err := ownedRangeAdd(0, int64(length), 1)
			if err != nil {
				return 0, 0, err
			}
			bitmap := int64(WordsFor(length)) * 8
			if v == nil {
				if err := missing(1024); err != nil {
					return 0, 0, err
				}
			} else {
				bitmap = validity(v.Validity)
			}
			n, err = ownedRangeAdd(n, bitmap, 1)
			if err != nil {
				return 0, 0, err
			}
			if err := add(n); err != nil {
				return 0, 0, err
			}
		case TypeInt16:
			v := src.Values.Int16s[field.Name]
			n, err := ownedRangeAdd(0, int64(length), 2)
			if err != nil {
				return 0, 0, err
			}
			bitmap := int64(WordsFor(length)) * 8
			if v == nil {
				if err := missing(1024); err != nil {
					return 0, 0, err
				}
			} else {
				bitmap = validity(v.Validity)
			}
			n, err = ownedRangeAdd(n, bitmap, 1)
			if err != nil {
				return 0, 0, err
			}
			if err := add(n); err != nil {
				return 0, 0, err
			}
		case TypeInt32, TypeDate32, TypeTime32:
			v := src.Values.Int32s[field.Name]
			n, err := ownedRangeAdd(0, int64(length), 4)
			if err != nil {
				return 0, 0, err
			}
			bitmap := int64(WordsFor(length)) * 8
			if v == nil {
				if err := missing(1024); err != nil {
					return 0, 0, err
				}
			} else {
				bitmap = validity(v.Validity)
			}
			n, err = ownedRangeAdd(n, bitmap, 1)
			if err != nil {
				return 0, 0, err
			}
			if err := add(n); err != nil {
				return 0, 0, err
			}
		case TypeInt64, TypeDate64, TypeTimestamp, TypeTime64, TypeDuration:
			v := src.Values.Int64s[field.Name]
			n, err := ownedRangeAdd(0, int64(length), 8)
			if err != nil {
				return 0, 0, err
			}
			bitmap := int64(WordsFor(length)) * 8
			if v == nil {
				if err := missing(1024); err != nil {
					return 0, 0, err
				}
			} else {
				bitmap = validity(v.Validity)
			}
			n, err = ownedRangeAdd(n, bitmap, 1)
			if err != nil {
				return 0, 0, err
			}
			if err := add(n); err != nil {
				return 0, 0, err
			}
		case TypeUint8:
			v := src.Values.Uint8s[field.Name]
			n, err := ownedRangeAdd(0, int64(length), 1)
			if err != nil {
				return 0, 0, err
			}
			bitmap := int64(WordsFor(length)) * 8
			if v == nil {
				if err := missing(1024); err != nil {
					return 0, 0, err
				}
			} else {
				bitmap = validity(v.Validity)
			}
			n, err = ownedRangeAdd(n, bitmap, 1)
			if err != nil {
				return 0, 0, err
			}
			if err := add(n); err != nil {
				return 0, 0, err
			}
		case TypeUint16:
			v := src.Values.Uint16s[field.Name]
			n, err := ownedRangeAdd(0, int64(length), 2)
			if err != nil {
				return 0, 0, err
			}
			bitmap := int64(WordsFor(length)) * 8
			if v == nil {
				if err := missing(1024); err != nil {
					return 0, 0, err
				}
			} else {
				bitmap = validity(v.Validity)
			}
			n, err = ownedRangeAdd(n, bitmap, 1)
			if err != nil {
				return 0, 0, err
			}
			if err := add(n); err != nil {
				return 0, 0, err
			}
		case TypeUint32, TypeBool:
			v := src.Values.Uint32s[field.Name]
			n, err := ownedRangeAdd(0, int64(length), 4)
			if err != nil {
				return 0, 0, err
			}
			bitmap := int64(WordsFor(length)) * 8
			if v == nil {
				if err := missing(1024); err != nil {
					return 0, 0, err
				}
			} else {
				bitmap = validity(v.Validity)
			}
			n, err = ownedRangeAdd(n, bitmap, 1)
			if err != nil {
				return 0, 0, err
			}
			if err := add(n); err != nil {
				return 0, 0, err
			}
		case TypeUint64:
			v := src.Values.Uint64s[field.Name]
			n, err := ownedRangeAdd(0, int64(length), 8)
			if err != nil {
				return 0, 0, err
			}
			bitmap := int64(WordsFor(length)) * 8
			if v == nil {
				if err := missing(1024); err != nil {
					return 0, 0, err
				}
			} else {
				bitmap = validity(v.Validity)
			}
			n, err = ownedRangeAdd(n, bitmap, 1)
			if err != nil {
				return 0, 0, err
			}
			if err := add(n); err != nil {
				return 0, 0, err
			}
		case TypeFloat32:
			v := src.Values.Float32s[field.Name]
			n, err := ownedRangeAdd(0, int64(length), 4)
			if err != nil {
				return 0, 0, err
			}
			bitmap := int64(WordsFor(length)) * 8
			if v == nil {
				if err := missing(1024); err != nil {
					return 0, 0, err
				}
			} else {
				bitmap = validity(v.Validity)
			}
			n, err = ownedRangeAdd(n, bitmap, 1)
			if err != nil {
				return 0, 0, err
			}
			if err := add(n); err != nil {
				return 0, 0, err
			}
		case TypeFloat64:
			v := src.Values.Float64s[field.Name]
			n, err := ownedRangeAdd(0, int64(length), 8)
			if err != nil {
				return 0, 0, err
			}
			bitmap := int64(WordsFor(length)) * 8
			if v == nil {
				if err := missing(1024); err != nil {
					return 0, 0, err
				}
			} else {
				bitmap = validity(v.Validity)
			}
			n, err = ownedRangeAdd(n, bitmap, 1)
			if err != nil {
				return 0, 0, err
			}
			if err := add(n); err != nil {
				return 0, 0, err
			}
		case TypeString:
			if v := src.Values.Strings[field.Name]; v != nil {
				lo, hi, err := stringRangeBounds(v, offset, length)
				if err != nil {
					return 0, 0, err
				}
				n, err := ownedRangeAdd(validity(v.Validity), int64(length)+1, 4)
				if err != nil {
					return 0, 0, err
				}
				n, err = ownedRangeAdd(n, int64(hi-lo), 1)
				if err != nil {
					return 0, 0, err
				}
				if err := add(n); err != nil {
					return 0, 0, err
				}
			} else {
				v := src.Values.DictStrings[field.Name]
				if v == nil {
					return 0, 0, fmt.Errorf("struct string child %q is missing", field.Name)
				}
				n, err := ownedRangeAdd(validity(v.Validity), int64(length), 4)
				if err != nil {
					return 0, 0, err
				}
				n, err = ownedRangeAdd(n, int64(len(v.Offsets)), 4)
				if err != nil {
					return 0, 0, err
				}
				n, err = ownedRangeAdd(n, int64(len(v.Data)), 1)
				if err != nil {
					return 0, 0, err
				}
				if err := add(n); err != nil {
					return 0, 0, err
				}
			}
		case TypeList:
			v := src.Values.StringLists[field.Name]
			if v == nil {
				n, err := ownedRangeAdd(int64(WordsFor(length))*8+4, int64(length)+1, 4)
				if err != nil {
					return 0, 0, err
				}
				if err := add(n); err != nil {
					return 0, 0, err
				}
				if err := missing(2048); err != nil {
					return 0, 0, err
				}
				continue
			}
			lo, hi, dataLo, dataHi, err := stringListRangeBounds(v, offset, length)
			if err != nil {
				return 0, 0, err
			}
			n, err := ownedRangeAdd(validity(v.Validity), int64(length)+1, 4)
			if err != nil {
				return 0, 0, err
			}
			childLength := int64(hi - lo)
			n, err = ownedRangeAdd(n, childLength+1, 4)
			if err != nil {
				return 0, 0, err
			}
			n, err = ownedRangeAdd(n, int64(dataHi-dataLo), 1)
			if err != nil {
				return 0, 0, err
			}
			if v.Values.Validity != nil && childLength > 0 {
				n, err = ownedRangeAdd(n, int64(WordsFor(int(childLength))), 8)
				if err != nil {
					return 0, 0, err
				}
			}
			if err := add(n); err != nil {
				return 0, 0, err
			}
		default:
			return 0, 0, fmt.Errorf("unsupported struct child %q type %s", field.Name, field.Type)
		}
	}
	return total, metadata, nil
}
