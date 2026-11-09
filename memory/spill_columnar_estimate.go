package memory

import (
	"fmt"
	"math"
)

// EstimateColumnarRunRangeBytes bounds the encoded payload for an owned copy or
// permutation of a contiguous range of supported specialized vectors. It includes
// codec framing and dictionary domains, which BatchBytes does not include. The
// estimate does not allocate the copy or encoder buffer. Schema projection may
// make the estimate conservative when batch contains additional columns.
func EstimateColumnarRunRangeBytes(schema Schema, batch *Batch, offset, length int) (int64, error) {
	data, err := EstimateBatchRangeBytes(batch, offset, length)
	if err != nil {
		return 0, err
	}
	overhead, err := columnarRunMetadataBytes(schema.Fields, batch)
	if err != nil {
		return 0, err
	}
	return addRunSizes(columnarRunHeader, data, overhead)
}

func columnarRunMetadataBytes(fields []Field, batch *Batch) (int64, error) {
	var total int64
	for _, field := range fields {
		bytes := int64(4) // validity length, including an absent bitmap
		switch field.Type {
		case TypeStruct:
			v := batch.Structs[field.Name]
			if v == nil || v.Values == nil {
				return 0, fmt.Errorf("struct field %q is missing", field.Name)
			}
			children, err := columnarRunMetadataBytes(field.Children, v.Values)
			if err != nil {
				return 0, err
			}
			bytes, err = addRunSizes(bytes, children)
			if err != nil {
				return 0, err
			}
		case TypeList:
			bytes = 24 // parent/child validity, offset counts, and data length
		case TypeString, TypeBinary:
			bytes = 16 // validity, offset count, and data length
			if field.Encoding == EncodingDictionary {
				v := batch.DictStrings[field.Name]
				if v == nil {
					return 0, fmt.Errorf("dictionary field %q is missing", field.Name)
				}
				var err error
				if _, err := columnarRunMetadataLength(uint64(len(v.Offsets))); err != nil {
					return 0, err
				}
				domainBytes, err := columnarRunMetadataLength(uint64(len(v.DictionaryDomain)))
				if err != nil {
					return 0, err
				}
				bytes, err = addRunSizes(bytes, 4, domainBytes)
				if err != nil {
					return 0, err
				}
			}
		case TypeDecimal:
			bytes += 8 // precision and scale
		}
		var err error
		total, err = addRunSizes(total, bytes)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

func columnarRunMetadataLength(bytes uint64) (int64, error) {
	if bytes > math.MaxUint32 {
		return 0, fmt.Errorf("columnar metadata length exceeds uint32 representation")
	}
	return int64(bytes), nil
}
