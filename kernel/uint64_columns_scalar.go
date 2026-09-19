package kernel

import "gocolumn/memory"

func installUint64ColumnScalar(d *dispatch) {
	d.compareUint64Columns[CompareGT] = scalarUint64ColumnsGT
	d.compareUint64Columns[CompareGE] = scalarUint64ColumnsGE
	d.compareUint64Columns[CompareLT] = scalarUint64ColumnsLT
	d.compareUint64Columns[CompareLE] = scalarUint64ColumnsLE
	d.compareUint64Columns[CompareEQ] = scalarUint64ColumnsEQ
}

func scalarUint64Columns(left, right []uint64, leftValid, rightValid, out memory.Bitmap, op CompareOp) int {
	if len(left) != len(right) {
		panic("kernel: uint64 column comparison length mismatch")
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

func scalarUint64ColumnsGT(a, b []uint64, av, bv, out memory.Bitmap) int {
	return scalarUint64Columns(a, b, av, bv, out, CompareGT)
}
func scalarUint64ColumnsGE(a, b []uint64, av, bv, out memory.Bitmap) int {
	return scalarUint64Columns(a, b, av, bv, out, CompareGE)
}
func scalarUint64ColumnsLT(a, b []uint64, av, bv, out memory.Bitmap) int {
	return scalarUint64Columns(a, b, av, bv, out, CompareLT)
}
func scalarUint64ColumnsLE(a, b []uint64, av, bv, out memory.Bitmap) int {
	return scalarUint64Columns(a, b, av, bv, out, CompareLE)
}
func scalarUint64ColumnsEQ(a, b []uint64, av, bv, out memory.Bitmap) int {
	return scalarUint64Columns(a, b, av, bv, out, CompareEQ)
}
