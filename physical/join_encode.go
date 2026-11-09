package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"math"
)

// reserveJoinEncoding admits both phases before either vector or payload
// allocation. The encoder reservation cannot be released while encoded bytes
// are passed to Spill. Failed admission leaves no partial reservation behind.
func reserveJoinEncoding(ctx context.Context, vectors, encoder memory.Manager, vectorBytes, encodedBytes int64) (memory.Reservation, memory.Reservation, error) {
	reservation, err := vectors.Reserve(ctx, vectorBytes)
	if err != nil {
		return nil, nil, err
	}
	encoding, err := encoder.Reserve(ctx, encodedBytes)
	if err != nil {
		reservation.Release()
		return nil, nil, err
	}
	return reservation, encoding, nil
}

// The materialization estimate is twice the logical vector bytes plus fixed
// scratch. Reserve codec framing and actual dictionary domains separately;
// BatchBytes and schema estimates do not account for those domains.
func estimateJoinEncodingBytes(s *JoinSource, materialization int64) (int64, error) {
	if materialization < 1024 {
		return 0, fmt.Errorf("invalid join materialization estimate")
	}
	total := (materialization-1024)/2 + 22
	for _, binding := range s.bindings {
		framing, err := joinEncodingFieldMetadata(binding.field)
		if err != nil {
			return 0, err
		}
		if binding.field.Encoding == memory.EncodingDictionary {
			if uint64(len(binding.dict.Offsets)) > math.MaxUint32 || uint64(len(binding.dict.Domain)) > math.MaxUint32 {
				return 0, fmt.Errorf("join dictionary exceeds columnar codec representation")
			}
			if err := addJoinEstimate(&framing, int64(len(binding.dict.Domain))); err != nil {
				return 0, err
			}
		}
		if err := addJoinEstimate(&total, framing); err != nil {
			return 0, err
		}
	}
	return total, nil
}

func joinEncodingFieldMetadata(field memory.Field) (int64, error) {
	switch field.Type {
	case memory.TypeString, memory.TypeBinary:
		if field.Encoding == memory.EncodingDictionary {
			return 20, nil
		}
		return 16, nil
	case memory.TypeList:
		return 24, nil
	case memory.TypeStruct:
		total := int64(4)
		for _, child := range field.Children {
			bytes, err := joinEncodingFieldMetadata(child)
			if err != nil {
				return 0, err
			}
			if err := addJoinEstimate(&total, bytes); err != nil {
				return 0, err
			}
		}
		return total, nil
	case memory.TypeDecimal:
		return 12, nil
	default:
		return 4, nil
	}
}
