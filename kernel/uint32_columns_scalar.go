package kernel

import "gocolumn/memory"

func installUint32ColumnScalar(d *dispatch) {
	d.compareUint32Columns[CompareGT] = scalarUint32ColumnsGT
	d.compareUint32Columns[CompareGE] = scalarUint32ColumnsGE
	d.compareUint32Columns[CompareLT] = scalarUint32ColumnsLT
	d.compareUint32Columns[CompareLE] = scalarUint32ColumnsLE
	d.compareUint32Columns[CompareEQ] = scalarUint32ColumnsEQ
}

func scalarUint32Columns(left, right []uint32, leftValid, rightValid, out memory.Bitmap, op CompareOp) int {
	if len(left) != len(right) {
		panic("kernel: uint32 column comparison length mismatch")
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

func scalarUint32ColumnsGT(a, b []uint32, av, bv, out memory.Bitmap) int {
	return scalarUint32Columns(a, b, av, bv, out, CompareGT)
}
func scalarUint32ColumnsGE(a, b []uint32, av, bv, out memory.Bitmap) int {
	return scalarUint32Columns(a, b, av, bv, out, CompareGE)
}
func scalarUint32ColumnsLT(a, b []uint32, av, bv, out memory.Bitmap) int {
	return scalarUint32Columns(a, b, av, bv, out, CompareLT)
}
func scalarUint32ColumnsLE(a, b []uint32, av, bv, out memory.Bitmap) int {
	return scalarUint32Columns(a, b, av, bv, out, CompareLE)
}
func scalarUint32ColumnsEQ(a, b []uint32, av, bv, out memory.Bitmap) int {
	return scalarUint32Columns(a, b, av, bv, out, CompareEQ)
}
