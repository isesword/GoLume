package kernel

import (
	"gocolumn/memory"
	"testing"
)

func TestColumnComparisonCapabilitiesAreSeparateAndExact(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		set := New(mode)
		for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64} {
			for op := CompareGT; op <= CompareEQ; op++ {
				capability, ok := set.ColumnComparisonCapability(typ, op)
				if !ok {
					t.Fatalf("mode=%s type=%s op=%s missing capability", mode, typ, op)
				}
				if mode == ModeScalar || (typ != memory.TypeInt32 && typ != memory.TypeUint32 && typ != memory.TypeFloat32 && typ != memory.TypeFloat64) {
					if capability != (KernelCapability{Backend: "scalar"}) {
						t.Fatalf("mode=%s type=%s op=%s false native claim: %+v", mode, typ, op, capability)
					}
				}
			}
		}
		date, _ := set.ColumnComparisonCapability(memory.TypeDate32, CompareGT)
		physical, _ := set.ColumnComparisonCapability(memory.TypeInt32, CompareGT)
		if date != physical {
			t.Fatalf("physical alias mismatch: date=%+v int32=%+v", date, physical)
		}
		if _, ok := set.ColumnComparisonCapability(memory.TypeString, CompareGT); ok {
			t.Fatal("string comparison claimed")
		}
		if _, ok := set.ColumnComparisonCapability(memory.TypeInt32, CompareOp(0)); ok {
			t.Fatal("invalid operation claimed")
		}
	}
	if _, ok := (Capabilities{}).ForColumns(memory.TypeInt32, CompareGT); ok {
		t.Fatal("zero capability view is valid")
	}
}
