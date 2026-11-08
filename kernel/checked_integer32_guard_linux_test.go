//go:build linux

package kernel

import (
	"gocolumn/memory"
	"testing"
)

func TestCheckedInteger32GuardPages(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		reviewCheckedPages(t, true, s.Int32Arithmetic, func(op ArithmeticOp) bool {
			id, _ := ArithmeticID(memory.TypeInt32, op)
			c, _ := s.Capability(id)
			return c.HardwareSIMD
		})
		reviewCheckedPages(t, false, s.Uint32Arithmetic, func(op ArithmeticOp) bool {
			id, _ := ArithmeticID(memory.TypeUint32, op)
			c, _ := s.Capability(id)
			return c.HardwareSIMD
		})
	}
}
