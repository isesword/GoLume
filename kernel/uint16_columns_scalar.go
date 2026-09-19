package kernel

import "gocolumn/memory"

func installUint16ColumnScalar(d *dispatch) {
	d.compareUint16Columns[CompareGT] = scalarUint16ColumnsGT
	d.compareUint16Columns[CompareGE] = scalarUint16ColumnsGE
	d.compareUint16Columns[CompareLT] = scalarUint16ColumnsLT
	d.compareUint16Columns[CompareLE] = scalarUint16ColumnsLE
	d.compareUint16Columns[CompareEQ] = scalarUint16ColumnsEQ
}

func scalarUint16Columns(left, right []uint16, leftValid, rightValid, out memory.Bitmap, op CompareOp) int {
	if len(left) != len(right) {
		panic("kernel: uint16 column comparison length mismatch")
	}
	prepareComparison(len(left), out)
	selected := 0
	for row, a := range left {
		if !validAt(leftValid, row) || !validAt(rightValid, row) {
			continue
		}
		b := right[row]
		match := false
		switch op {
		case CompareGT:
			match = a > b
		case CompareGE:
			match = a >= b
		case CompareLT:
			match = a < b
		case CompareLE:
			match = a <= b
		case CompareEQ:
			match = a == b
		}
		if match {
			setSelected(out, row, &selected)
		}
	}
	return selected
}

func scalarUint16ColumnsGT(a, b []uint16, av, bv, out memory.Bitmap) int {
	return scalarUint16Columns(a, b, av, bv, out, CompareGT)
}
func scalarUint16ColumnsGE(a, b []uint16, av, bv, out memory.Bitmap) int {
	return scalarUint16Columns(a, b, av, bv, out, CompareGE)
}
func scalarUint16ColumnsLT(a, b []uint16, av, bv, out memory.Bitmap) int {
	return scalarUint16Columns(a, b, av, bv, out, CompareLT)
}
func scalarUint16ColumnsLE(a, b []uint16, av, bv, out memory.Bitmap) int {
	return scalarUint16Columns(a, b, av, bv, out, CompareLE)
}
func scalarUint16ColumnsEQ(a, b []uint16, av, bv, out memory.Bitmap) int {
	return scalarUint16Columns(a, b, av, bv, out, CompareEQ)
}
