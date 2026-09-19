package kernel

import "gocolumn/memory"

func installInt16ColumnScalar(d *dispatch) {
	d.compareInt16Columns[CompareGT] = scalarInt16ColumnsGT
	d.compareInt16Columns[CompareGE] = scalarInt16ColumnsGE
	d.compareInt16Columns[CompareLT] = scalarInt16ColumnsLT
	d.compareInt16Columns[CompareLE] = scalarInt16ColumnsLE
	d.compareInt16Columns[CompareEQ] = scalarInt16ColumnsEQ
}

func scalarInt16Columns(left, right []int16, leftValid, rightValid, out memory.Bitmap, op CompareOp) int {
	if len(left) != len(right) {
		panic("kernel: int16 column comparison length mismatch")
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

func scalarInt16ColumnsGT(a, b []int16, av, bv, out memory.Bitmap) int {
	return scalarInt16Columns(a, b, av, bv, out, CompareGT)
}
func scalarInt16ColumnsGE(a, b []int16, av, bv, out memory.Bitmap) int {
	return scalarInt16Columns(a, b, av, bv, out, CompareGE)
}
func scalarInt16ColumnsLT(a, b []int16, av, bv, out memory.Bitmap) int {
	return scalarInt16Columns(a, b, av, bv, out, CompareLT)
}
func scalarInt16ColumnsLE(a, b []int16, av, bv, out memory.Bitmap) int {
	return scalarInt16Columns(a, b, av, bv, out, CompareLE)
}
func scalarInt16ColumnsEQ(a, b []int16, av, bv, out memory.Bitmap) int {
	return scalarInt16Columns(a, b, av, bv, out, CompareEQ)
}
