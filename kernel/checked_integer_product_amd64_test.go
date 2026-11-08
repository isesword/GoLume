//go:build goexperiment.simd && amd64

package kernel

import (
	"math/bits"
	"math/rand/v2"
	"simd/archsimd"
	"testing"
)

func TestIndependentCheckedProduct128(t *testing.T) {
	if !archsimd.X86.AVX2() {
		t.Skip("requires AVX2")
	}
	limbs := []uint64{0, 1, 2, 0x7fffffff, 0x80000000, 0xfffffffe, 0xffffffff}
	pairs := [][2]uint64{}
	for _, al := range limbs {
		for _, ah := range limbs {
			for _, bl := range limbs {
				for _, bh := range limbs {
					pairs = append(pairs, [2]uint64{ah<<32 | al, bh<<32 | bl})
				}
			}
		}
	}
	r := rand.New(rand.NewPCG(793, 1901))
	for i := 0; i < 50000; i++ {
		pairs = append(pairs, [2]uint64{r.Uint64(), r.Uint64()})
	}
	for _, width := range []int{128, 256, 512} {
		if width == 512 && !archsimd.X86.AVX512() {
			continue
		}
		lanes := width / 64
		a, b, lo, hi := make([]uint64, lanes), make([]uint64, lanes), make([]uint64, lanes), make([]uint64, lanes)
		for base := 0; base < len(pairs); base += lanes {
			for i := range a {
				p := pairs[(base+i)%len(pairs)]
				a[i] = p[0]
				b[i] = p[1]
			}
			switch width {
			case 128:
				l, h := checkedProductamd64128(archsimd.LoadUint64x2(a), archsimd.LoadUint64x2(b))
				l.Store(lo)
				h.Store(hi)
			case 256:
				l, h := checkedProductamd64256(archsimd.LoadUint64x4(a), archsimd.LoadUint64x4(b))
				l.Store(lo)
				h.Store(hi)
			case 512:
				l, h := checkedProductamd64512(archsimd.LoadUint64x8(a), archsimd.LoadUint64x8(b))
				l.Store(lo)
				h.Store(hi)
			}
			for i := range a {
				wh, wl := bits.Mul64(a[i], b[i])
				if lo[i] != wl || hi[i] != wh {
					t.Fatalf("width%d a%x b%x got%x:%x want%x:%x", width, a[i], b[i], hi[i], lo[i], wh, wl)
				}
			}
		}
	}
}
