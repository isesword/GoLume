//go:build !goexperiment.simd

package kernel

import (
	"runtime"
	"testing"
)

func TestColumn32GateEnvironment(t *testing.T) {
	t.Logf("arch=%s GOEXPERIMENT excludes simd; column32 hardware SIMD unavailable", runtime.GOARCH)
	column32CheckDispatch(t, "scalar")
}
