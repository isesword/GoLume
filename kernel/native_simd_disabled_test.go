//go:build !goexperiment.simd

package kernel

import "testing"

func TestSIMDDisabledModesRetainScalarCapabilities(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		set := New(mode)
		for _, id := range KernelIDs() {
			capability, ok := set.Capability(id)
			if !ok || capability != (KernelCapability{Backend: "scalar"}) {
				t.Fatalf("mode=%s id=%s capability=%+v ok=%t", mode, id, capability, ok)
			}
			if id.kind == KernelComparison {
				column, ok := set.ColumnComparisonCapability(id.typ, CompareOp(id.op))
				if !ok || column != capability {
					t.Fatalf("mode=%s column=%s capability=%+v ok=%t", mode, id, column, ok)
				}
			}
		}
	}
}
