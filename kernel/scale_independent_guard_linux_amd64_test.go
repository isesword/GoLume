//go:build linux && amd64 && goexperiment.simd

package kernel

import (
	"gocolumn/memory"
	"math"
	"simd/archsimd"
	"slices"
	"syscall"
	"testing"
	"unsafe"
)

func TestScaleIndependentGuardPages(t *testing.T) {
	if !archsimd.X86.AVX2() {
		t.Skip("requires AVX2")
	}
	funcs := []func([]int64, memory.Bitmap, int64) int{scaleamd64128, scaleamd64256}
	if archsimd.X86.AVX512() {
		funcs = append(funcs, scaleamd64512)
	}
	p := syscall.Getpagesize()
	m, err := syscall.Mmap(-1, 0, 2*p, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Munmap(m)
	if err = syscall.Mprotect(m[p:], syscall.PROT_NONE); err != nil {
		t.Fatal(err)
	}
	for width, fn := range funcs {
		for n := 0; n <= 137; n++ {
			values := unsafe.Slice((*int64)(unsafe.Pointer(&m[p-n*8])), n)
			for pattern := 0; pattern < 4; pattern++ {
				for fail := -1; fail < n; fail++ {
					for _, factor := range []int64{0, 1, -1, 2, -3, math.MinInt64} {
						valid := make(memory.Bitmap, (n+63)/64)
						for i := range values {
							values[i] = int64(i) - 69
							if pattern == 0 || pattern == 1 || pattern == 2 && i%3 == 0 {
								valid[i/64] |= uint64(1) << uint(i%64)
							}
						}
						if n > 0 {
							valid[len(valid)-1] |= ^memory.TailMask(n)
						}
						if pattern == 0 {
							valid = nil
						}
						if fail >= 0 {
							values[fail] = math.MaxInt64
						}
						want := slices.Clone(values)
						wantFail := scalarInt64Scale(want, valid, factor)
						if got := fn(values, valid, factor); got != wantFail || !slices.Equal(values, want) {
							t.Fatalf("width=%d n=%d pattern=%d fail=%d got=%d want=%d", width, n, pattern, fail, got, wantFail)
						}
					}
				}
			}
		}
	}
}
