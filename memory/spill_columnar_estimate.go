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
	return EstimateColumnarRunRangeBytesWithCheck(schema, batch, offset, length, nil)
}

// EstimateColumnarRunRangeBytesWithCheck adds cancellation during metadata and
// array-axis preflight without allocating selection indices or changing fees.
func EstimateColumnarRunRangeBytesWithCheck(schema Schema, batch *Batch, offset, length int, check func() error) (int64, error) {
	if _, err := columnarRunDecodeMetadataBytesContext(schema, check); err != nil {
		return 0, err
	}
	if schema.HasNestedLists() {
		if err := checkArrayBatchGraph(batch, schema.Fields, 0, check); err != nil {
			return 0, err
		}
	} else if schema.HasStructLists() {
		if err := checkStructListsGraph(batch, schema.Fields, 0, check); err != nil {
			return 0, err
		}
	} else if schema.HasRecursiveStruct() {
		if err := checkRecursiveCollectGraph(batch, 0, check); err != nil {
			return 0, err
		}
	}

	data, err := EstimateBatchRangeBytesWithCheck(batch, offset, length, check)
	if err != nil {
		return 0, err
	}
	overhead, err := columnarRunMetadataBytesCheck(schema.Fields, batch, check)
	if err != nil {
		return 0, err
	}
	return addRunSizes(columnarRunHeader, data, overhead)
}

func columnarRunMetadataBytes(fields []Field, batch *Batch) (int64, error) {
	return columnarRunMetadataBytesCheck(fields, batch, nil)
}
func columnarRunMetadataBytesCheck(fields []Field, batch *Batch, check func() error) (int64, error) {
	var total int64
	for _, field := range fields {
		if err := recursiveCheck(check); err != nil {
			return 0, err
		}
		bytes := int64(4) // validity length, including an absent bitmap
		switch field.Type {
		case TypeStruct:
			v := batch.Structs[field.Name]
			if v == nil || v.Values == nil {
				return 0, fmt.Errorf("struct field %q is missing", field.Name)
			}
			children, err := columnarRunMetadataBytesCheck(field.Children, v.Values, check)
			if err != nil {
				return 0, err
			}
			bytes, err = addRunSizes(bytes, children)
			if err != nil {
				return 0, err
			}
		case TypeList:
			if IsNestedList(field) {
				bytes = 28
				if field.Children[0].Children[0].Type == TypeString {
					bytes += 12
				}
				break
			}
			if IsStructList(field) {
				v := batch.StructLists[field.Name]
				if v == nil || v.Values == nil {
					return 0, fmt.Errorf("missing StructList")
				}
				children, err := columnarRunMetadataBytesCheck(field.Children[0].Children, v.Values.Values, check)
				if err != nil {
					return 0, err
				}
				bytes, err = addRunSizes(12, children)
				if err != nil {
					return 0, err
				}
				break
			}
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
