//go:build !goexperiment.simd

package kernel

func newSIMDInt64GT(Mode) (Int64GTFunc, Capabilities, bool) {
	return nil, Capabilities{}, false
}
