package expr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"gocolumn/memory"
	"math"
)

func decodeJSONInt64List(batch *memory.Batch, field memory.Field, raw json.RawMessage, valid bool, row int, checkContext func() error) error {
	list := batch.Int64Lists[field.Name]
	if list == nil {
		list = &memory.Int64ListVector{Offsets: make([]uint32, 2), Values: &memory.Int64Vector{Encoding: memory.EncodingPlain}, Encoding: memory.EncodingView}
		batch.Int64Lists[field.Name] = list
	}
	list.Offsets[1] = 0
	list.Values.Values = nil
	list.Values.Validity = nil
	list.Validity = setJSONRowValidity(list.Validity, valid)
	if !valid {
		return nil
	}
	var elements []json.RawMessage
	if err := json.Unmarshal(raw, &elements); err != nil {
		return jsonFieldError(row, field.Name, fmt.Errorf("expected int64 array"))
	}
	if uint64(len(elements)) > math.MaxUint32 {
		return jsonFieldError(row, field.Name, fmt.Errorf("element count overflows uint32 offsets"))
	}
	list.Offsets[1] = uint32(len(elements))
	list.Values.Values = make([]int64, len(elements))
	for i, element := range elements {
		if i%256 == 0 && checkContext != nil {
			if err := checkContext(); err != nil {
				return err
			}
		}
		null := bytes.Equal(bytes.TrimSpace(element), []byte("null"))
		if null {
			if !field.Children[0].Nullable {
				return fmt.Errorf("JSON_DECODE at row %d field %q[%d]: null element is not allowed", row, field.Name, i)
			}
			if list.Values.Validity == nil {
				list.Values.Validity = memory.EnsureBitmap(nil, len(elements))
				for j := 0; j < i; j++ {
					memory.Set(list.Values.Validity, j)
				}
			}
			continue
		}
		if err := json.Unmarshal(element, &list.Values.Values[i]); err != nil {
			return fmt.Errorf("JSON_DECODE at row %d field %q[%d]: expected int64", row, field.Name, i)
		}
		if list.Values.Validity != nil {
			memory.Set(list.Values.Validity, i)
		}
	}
	return nil
}
