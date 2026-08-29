package memory

import "math/bits"

// Bitmap stores one row-selection or validity bit per logical row, packed little-endian within each uint64 word.
type Bitmap []uint64

// WordsFor returns the number of uint64 words required to represent rows bits.
func WordsFor(rows int) int {
	if rows <= 0 {
		return 0
	}
	return (rows + 63) >> 6
}

// EnsureBitmap resizes b for rows bits, reusing capacity when possible, and clears all words.
func EnsureBitmap(b Bitmap, rows int) Bitmap {
	words := WordsFor(rows)
	if cap(b) < words {
		return make(Bitmap, words)
	}
	b = b[:words]
	clear(b)
	return b
}

// CountBitmap returns the number of set bits in b.
func CountBitmap(b Bitmap) int {
	n := 0
	for _, w := range b {
		n += bits.OnesCount64(w)
	}
	return n
}

// HasNulls reports whether validity contains any clear logical row bit. A nil
// bitmap means all rows are valid; tail bits outside rows are ignored.
func HasNulls(validity Bitmap, rows int) bool {
	if validity == nil || rows <= 0 {
		return false
	}
	words := WordsFor(rows)
	if len(validity) < words {
		return true
	}
	for i := 0; i < words; i++ {
		want := ^uint64(0)
		if i == words-1 {
			want = TailMask(rows)
		}
		if validity[i]&want != want {
			return true
		}
	}
	return false
}

// TailMask returns a word mask that keeps only valid bits in the final bitmap word.
func TailMask(rows int) uint64 {
	if rows <= 0 {
		return 0
	}
	r := rows & 63
	if r == 0 {
		return ^uint64(0)
	}
	return (uint64(1) << r) - 1
}

// IsSet reports whether row is set in b. Callers must provide an in-range row.
func IsSet(b Bitmap, row int) bool {
	return b[row>>6]&(uint64(1)<<uint(row&63)) != 0
}

// Set marks row in b. Callers must provide an in-range row.
func Set(b Bitmap, row int) {
	b[row>>6] |= uint64(1) << uint(row&63)
}
