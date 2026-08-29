package kernel

import (
	"gocolumn/memory"
	"testing"
)

func BenchmarkComparisonSelectivity(b *testing.B) {
	const rows = 1 << 16
	for _, layout := range []string{"ordered", "random", "alternating"} {
		intValues := make([]int32, rows)
		uintValues := make([]uint32, rows)
		state := uint32(1)
		for row := range intValues {
			var value uint32
			switch layout {
			case "ordered":
				value = uint32(row % 100)
			case "random":
				state = state*1664525 + 1013904223
				value = state % 100
			case "alternating":
				if row&1 == 0 {
					value = uint32(row % 50)
				} else {
					value = uint32(50 + row%50)
				}
			}
			intValues[row], uintValues[row] = int32(value), value
		}
		for _, selectedPercent := range []int{1, 10, 50, 90, 100} {
			threshold := 99 - selectedPercent
			uintThreshold, uintOp := uint32(max(threshold, 0)), CompareGT
			if selectedPercent == 100 {
				threshold = -1
				uintThreshold, uintOp = 0, CompareGE
			}
			for _, mode := range []struct {
				name string
				mode Mode
			}{{"mode=scalar", ModeScalar}, {"mode=simd", ModeSIMD}} {
				set := New(mode.mode)
				b.Run("int32/"+layout+"/select="+percentName(selectedPercent)+"/"+mode.name, func(b *testing.B) {
					out := memory.EnsureBitmap(nil, rows)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						set.CompareInt32(intValues, nil, out, int32(threshold), CompareGT)
					}
				})
				b.Run("uint32/"+layout+"/select="+percentName(selectedPercent)+"/"+mode.name, func(b *testing.B) {
					out := memory.EnsureBitmap(nil, rows)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						set.CompareUint32(uintValues, nil, out, uintThreshold, uintOp)
					}
				})
			}
		}
	}
}

func BenchmarkHashBackend(b *testing.B) {
	const rows = 1 << 16
	a, out := make([]uint64, rows), make([]uint64, rows)
	for row := range a {
		a[row] = uint64(row)*0x9e3779b97f4a7c15 + 17
	}
	for _, backend := range []struct {
		name   string
		hash64 hash64BatchFunc
	}{{"backend=scalar", scalarHash64Batch}, {"backend=unrolled", unrolledHash64Batch}} {
		b.Run("hash64/"+backend.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				backend.hash64(a, out)
			}
		})
	}
}

func BenchmarkUTF8Profiles(b *testing.B) {
	profiles := []struct {
		name  string
		value []byte
	}{
		{"short-ascii", []byte("sixteen-byte-key")},
		{"long-ascii", []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789--abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789--")},
		{"mixed", []byte("Go语言🙂Go语言🙂Go语言🙂Go语言🙂Go语言🙂Go语言🙂Go语言🙂Go语言🙂")},
		{"malformed", append([]byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789--"), 0xe0, 0x80, 0x80)},
	}
	const rows = 1 << 14
	for _, profile := range profiles {
		offsets := make([]uint32, rows+1)
		data := make([]byte, 0, rows*len(profile.value))
		for row := 0; row < rows; row++ {
			data = append(data, profile.value...)
			offsets[row+1] = uint32(len(data))
		}
		for _, mode := range []struct {
			name string
			mode Mode
		}{{"mode=scalar", ModeScalar}, {"mode=simd", ModeSIMD}} {
			set := New(mode.mode)
			b.Run(profile.name+"/"+mode.name, func(b *testing.B) {
				out := memory.EnsureBitmap(nil, rows)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					set.CompareStringLength(offsets, data, nil, out, 32, CompareGT, true)
				}
			})
		}
	}
}

func percentName(percent int) string {
	if percent == 100 {
		return "100"
	}
	if percent >= 10 {
		return string([]byte{'0' + byte(percent/10), '0' + byte(percent%10)})
	}
	return string([]byte{'0' + byte(percent)})
}
