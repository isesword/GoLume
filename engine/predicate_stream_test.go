package engine

import (
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"testing"
)

func TestFusedPredicateUsesNativeVectorTypes(t *testing.T) {
	const rows = 131
	for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64, memory.TypeBool} {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			t.Run(typ.String()+"/"+mode.String(), func(t *testing.T) {
				batch := memory.NewBatch()
				batch.Rows = rows
				valid := memory.EnsureBitmap(nil, rows)
				for row := 0; row < rows; row++ {
					if row%7 != 0 {
						memory.Set(valid, row)
					}
				}
				switch typ {
				case memory.TypeInt8:
					v := &memory.Int8Vector{Values: make([]int8, rows), Validity: valid}
					for i := range v.Values {
						v.Values[i] = int8(i % 2)
					}
					batch.Int8s["pred"] = v
				case memory.TypeInt16:
					v := &memory.Int16Vector{Values: make([]int16, rows), Validity: valid}
					for i := range v.Values {
						v.Values[i] = int16(i % 2)
					}
					batch.Int16s["pred"] = v
				case memory.TypeInt32:
					v := &memory.Int32Vector{Values: make([]int32, rows), Validity: valid}
					for i := range v.Values {
						v.Values[i] = int32(i % 2)
					}
					batch.Int32s["pred"] = v
				case memory.TypeInt64:
					v := &memory.Int64Vector{Values: make([]int64, rows), Validity: valid}
					for i := range v.Values {
						v.Values[i] = int64(i % 2)
					}
					batch.Int64s["pred"] = v
				case memory.TypeUint8:
					v := &memory.Uint8Vector{Values: make([]uint8, rows), Validity: valid}
					for i := range v.Values {
						v.Values[i] = uint8(i % 2)
					}
					batch.Uint8s["pred"] = v
				case memory.TypeUint16:
					v := &memory.Uint16Vector{Values: make([]uint16, rows), Validity: valid}
					for i := range v.Values {
						v.Values[i] = uint16(i % 2)
					}
					batch.Uint16s["pred"] = v
				case memory.TypeUint32, memory.TypeBool:
					v := &memory.Uint32Vector{Values: make([]uint32, rows), Validity: valid}
					for i := range v.Values {
						v.Values[i] = uint32(i % 2)
					}
					batch.Uint32s["pred"] = v
				case memory.TypeUint64:
					v := &memory.Uint64Vector{Values: make([]uint64, rows), Validity: valid}
					for i := range v.Values {
						v.Values[i] = uint64(i % 2)
					}
					batch.Uint64s["pred"] = v
				case memory.TypeFloat32:
					v := &memory.Float32Vector{Values: make([]float32, rows), Validity: valid}
					for i := range v.Values {
						v.Values[i] = float32(i % 2)
					}
					batch.Float32s["pred"] = v
				case memory.TypeFloat64:
					v := &memory.Float64Vector{Values: make([]float64, rows), Validity: valid}
					for i := range v.Values {
						v.Values[i] = float64(i % 2)
					}
					batch.Float64s["pred"] = v
				}
				comparison := &expr.SimpleComparison{Column: "pred", Type: typ, Op: expr.OpGT}
				count := 0
				selected, err := forSimpleSelected(batch, comparison, kernel.New(mode), memory.Bitmap{0}, func(row int) error {
					if row%2 != 1 || row%7 == 0 {
						t.Fatalf("unexpected selected row=%d", row)
					}
					count++
					return nil
				})
				want := 0
				for row := 0; row < rows; row++ {
					if row%2 == 1 && row%7 != 0 {
						want++
					}
				}
				if err != nil || selected != want || count != want {
					t.Fatalf("selected=%d count=%d want=%d err=%v", selected, count, want, err)
				}
			})
		}
	}
}

func TestFusedPredicatePreservesWideLiteralSemantics(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeBool, memory.TypeFloat32} {
			for _, op := range []expr.Op{expr.OpGT, expr.OpGE, expr.OpLT, expr.OpLE, expr.OpEQ} {
				t.Run(typ.String()+"/"+op.String()+"/"+mode.String(), func(t *testing.T) {
					batch := memory.NewBatch()
					batch.Rows = 2
					valid := memory.Bitmap{1}
					comparison := &expr.SimpleComparison{Column: "pred", Type: typ, Op: op, LiteralI64: (1 << 40) + 5, LiteralU64: (1 << 40) + 5, LiteralF64: 5.0000001}
					switch typ {
					case memory.TypeInt8:
						batch.Int8s["pred"] = &memory.Int8Vector{Values: []int8{5, 5}, Validity: valid}
					case memory.TypeInt16:
						batch.Int16s["pred"] = &memory.Int16Vector{Values: []int16{5, 5}, Validity: valid}
					case memory.TypeInt32:
						batch.Int32s["pred"] = &memory.Int32Vector{Values: []int32{5, 5}, Validity: valid}
					case memory.TypeUint8:
						batch.Uint8s["pred"] = &memory.Uint8Vector{Values: []uint8{5, 5}, Validity: valid}
					case memory.TypeUint16:
						batch.Uint16s["pred"] = &memory.Uint16Vector{Values: []uint16{5, 5}, Validity: valid}
					case memory.TypeUint32, memory.TypeBool:
						batch.Uint32s["pred"] = &memory.Uint32Vector{Values: []uint32{5, 5}, Validity: valid}
					case memory.TypeFloat32:
						batch.Float32s["pred"] = &memory.Float32Vector{Values: []float32{5, 5}, Validity: valid}
					}
					seen := 0
					selected, err := forSimpleSelected(batch, comparison, kernel.New(mode), memory.Bitmap{0}, func(row int) error {
						if row != 0 {
							t.Fatalf("NULL row selected")
						}
						seen++
						return nil
					})
					want := 0
					if op == expr.OpLT || op == expr.OpLE {
						want = 1
					}
					if err != nil || selected != want || seen != want {
						t.Fatalf("selected=%d seen=%d want=%d err=%v", selected, seen, want, err)
					}
				})
			}
		}
	}
}

func TestFusedWidePredicateHonorsLogicalRows(t *testing.T) {
	for _, rows := range []int{0, 1, 65} {
		for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeBool, memory.TypeFloat32} {
			batch := memory.NewBatch()
			batch.Rows = rows
			comparison := &expr.SimpleComparison{Column: "pred", Type: typ, Op: expr.OpLT, LiteralI64: 1 << 40, LiteralU64: 1 << 40, LiteralF64: 5.0000001}
			switch typ {
			case memory.TypeInt8:
				batch.Int8s["pred"] = &memory.Int8Vector{Values: make([]int8, 131)}
			case memory.TypeInt16:
				batch.Int16s["pred"] = &memory.Int16Vector{Values: make([]int16, 131)}
			case memory.TypeInt32:
				batch.Int32s["pred"] = &memory.Int32Vector{Values: make([]int32, 131)}
			case memory.TypeUint8:
				batch.Uint8s["pred"] = &memory.Uint8Vector{Values: make([]uint8, 131)}
			case memory.TypeUint16:
				batch.Uint16s["pred"] = &memory.Uint16Vector{Values: make([]uint16, 131)}
			case memory.TypeUint32, memory.TypeBool:
				batch.Uint32s["pred"] = &memory.Uint32Vector{Values: make([]uint32, 131)}
			case memory.TypeFloat32:
				batch.Float32s["pred"] = &memory.Float32Vector{Values: make([]float32, 131)}
			}
			seen := 0
			selected, err := forSimpleSelected(batch, comparison, kernel.New(kernel.ModeScalar), memory.Bitmap{0}, func(row int) error {
				if row >= rows {
					t.Fatalf("type=%s rows=%d selected backing row%d", typ, rows, row)
				}
				seen++
				return nil
			})
			if err != nil || selected != rows || seen != rows {
				t.Fatalf("type=%s rows=%d selected=%d seen=%d err=%v", typ, rows, selected, seen, err)
			}
		}
	}
}
