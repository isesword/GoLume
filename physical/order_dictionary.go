package physical

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"gocolumn/memory"
	"math"
)

var errSortMixedDictionaries = errors.New("mixed dictionary values require the budgeted external sorter")

// appendSortDictionaryRow is the owned, budgeted external-sort variant.
func appendSortDictionaryRow(dst, src *memory.DictionaryStringVector, row int, valid bool) error {
	return appendSortDictionaryRowMode(dst, src, row, valid, true)
}

// Unbudgeted shared accumulators retain the first dictionary without growing
// it. They may remap existing values, but must reject a value not represented
// there rather than silently reinterpret its code or allocate unadmitted data.
func appendSortDictionaryRowMode(dst, src *memory.DictionaryStringVector, row int, valid, owned bool) error {
	if dst == nil || src == nil || row < 0 || row >= len(src.Indices) {
		return fmt.Errorf("dictionary append requires an in-range row")
	}
	var value []byte
	var code uint32
	if valid {
		code = src.Indices[row]
		if uint64(code)+1 >= uint64(len(src.Offsets)) {
			return fmt.Errorf("dictionary code out of range")
		}
		start, end := src.Offsets[code], src.Offsets[code+1]
		if start > end || uint64(end) > uint64(len(src.Data)) {
			return fmt.Errorf("dictionary offsets out of range")
		}
		value = src.Data[start:end]
	}
	if len(dst.Indices) == 0 && len(dst.Offsets) == 0 {
		if uint64(len(src.Offsets)) > math.MaxUint32 {
			return fmt.Errorf("dictionary offset count exceeds uint32")
		}
		dst.DictionaryDomain, dst.Encoding = src.DictionaryDomain, src.Encoding
		if owned {
			if len(src.Offsets) == 0 {
				dst.Offsets = []uint32{0}
			} else {
				base, end := src.Offsets[0], src.Offsets[len(src.Offsets)-1]
				if base > end || uint64(end) > uint64(len(src.Data)) {
					return fmt.Errorf("dictionary data window out of range")
				}
				previous := base
				for _, offset := range src.Offsets {
					if offset < previous || offset > end {
						return fmt.Errorf("dictionary offsets are not ordered")
					}
					previous = offset
				}
				dst.Offsets = make([]uint32, len(src.Offsets))
				for i, offset := range src.Offsets {
					dst.Offsets[i] = offset - base
				}
				dst.Data = append([]byte(nil), src.Data[base:end]...)
			}
		} else {
			dst.Offsets, dst.Data = src.Offsets, src.Data
		}
	} else if valid && !(uint64(code)+1 < uint64(len(dst.Offsets)) && bytes.Equal(dst.DictionaryValue(code), value)) {
		found := false
		// The same-code fast path avoids scanning normal stable dictionaries.
		// A linear lookup avoids both duplicate value growth and an unbounded map.
		for i := 0; i+1 < len(dst.Offsets); i++ {
			if bytes.Equal(dst.DictionaryValue(uint32(i)), value) {
				code = uint32(i)
				found = true
				break
			}
		}
		if !found {
			if !owned {
				return errSortMixedDictionaries
			}
			if !sortDictionaryCanAppend(uint64(len(dst.Offsets)), uint64(len(dst.Data)), uint64(len(value))) {
				return fmt.Errorf("appended dictionary exceeds uint32 representation")
			}
			code = uint32(len(dst.Offsets) - 1)
			dst.Data = append(dst.Data, value...)
			dst.Offsets = append(dst.Offsets, uint32(len(dst.Data)))
			// Finish assigns a content identity once, after all mutation is complete.
			dst.DictionaryDomain = ""
		}
	}
	dst.Indices = append(dst.Indices, code)
	dst.Validity = appendOneValidity(dst.Validity, src.Validity, row, len(dst.Indices)-1)
	return nil
}

func sortDictionaryCanAppend(offsets, data, value uint64) bool {
	return offsets > 0 && offsets < math.MaxUint32 && data <= math.MaxUint32 && value <= math.MaxUint32-data
}

func finishSortDictionary(v *memory.DictionaryStringVector) {
	h := sha256.New()
	_, _ = h.Write([]byte("GoLume sorted dictionary v1"))
	var word [8]byte
	binary.LittleEndian.PutUint64(word[:], uint64(len(v.Offsets)))
	_, _ = h.Write(word[:])
	for _, offset := range v.Offsets {
		binary.LittleEndian.PutUint32(word[:4], offset)
		_, _ = h.Write(word[:4])
	}
	_, _ = h.Write(v.Data)
	sum := h.Sum(nil)
	v.DictionaryDomain = hex.EncodeToString(sum)
}
