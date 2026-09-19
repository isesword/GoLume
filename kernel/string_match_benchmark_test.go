package kernel

import (
	"gocolumn/memory"
	"testing"
)

func BenchmarkStringMatchKernel(b *testing.B) {
	const rows = 1 << 16
	offsets := make([]uint32, rows+1)
	data := make([]byte, 0, rows*16)
	for row := 0; row < rows; row++ {
		value := []byte("alpha-")
		if row%7 == 0 {
			value = []byte("中文-😀-alpha")
		} else if row%11 == 0 {
			value = []byte("omega")
		}
		data = append(data, value...)
		offsets[row+1] = uint32(len(data))
	}
	valid := memory.EnsureBitmap(nil, rows)
	for row := 0; row < rows; row++ {
		if row%13 != 0 {
			memory.Set(valid, row)
		}
	}
	out := make(memory.Bitmap, memory.WordsFor(rows))
	for _, tc := range []struct {
		name string
		op   StringMatchOp
		lit  []byte
	}{
		{"equal-ascii", StringEqual, []byte("alpha-")},
		{"prefix-ascii", StringPrefix, []byte("alpha")},
		{"suffix-ascii", StringSuffix, []byte("alpha")},
		{"contains-mixed", StringContains, []byte("😀")},
	} {
		b.Run(tc.name, func(b *testing.B) {
			set := New(ModeScalar)
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				set.CompareStringMatch(offsets, data, valid, out, tc.lit, tc.op)
			}
		})
	}
}
