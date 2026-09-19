package kernel

import "gocolumn/memory"

func installFloat64ColumnScalar(d *dispatch) {
	d.compareFloat64Columns[CompareGT] = scalarFloat64ColumnsGT
	d.compareFloat64Columns[CompareGE] = scalarFloat64ColumnsGE
	d.compareFloat64Columns[CompareLT] = scalarFloat64ColumnsLT
	d.compareFloat64Columns[CompareLE] = scalarFloat64ColumnsLE
	d.compareFloat64Columns[CompareEQ] = scalarFloat64ColumnsEQ
}

func scalarFloat64Columns(left, right []float64, leftValid, rightValid, out memory.Bitmap, op CompareOp) int {
	if len(left) != len(right) {
		panic("kernel: float64 column comparison length mismatch")
	}
	prepareComparison(len(left), out)
	selected := 0
	for row, a := range left {
		if !validAt(leftValid, row) || !validAt(rightValid, row) {
			continue
		}
		if compareFloat64(a, right[row], op) {
			setSelected(out, row, &selected)
		}
	}
	return selected
}

func compareFloat64(a, b float64, op CompareOp) bool {
	switch op {
	case CompareGT:
		return a > b
	case CompareGE:
		return a >= b
	case CompareLT:
		return a < b
	case CompareLE:
		return a <= b
	case CompareEQ:
		return a == b
	default:
		return false
	}
}

func scalarFloat64ColumnsGT(a, b []float64, av, bv, out memory.Bitmap) int {
	return scalarFloat64Columns(a, b, av, bv, out, CompareGT)
}
func scalarFloat64ColumnsGE(a, b []float64, av, bv, out memory.Bitmap) int {
	return scalarFloat64Columns(a, b, av, bv, out, CompareGE)
}
func scalarFloat64ColumnsLT(a, b []float64, av, bv, out memory.Bitmap) int {
	return scalarFloat64Columns(a, b, av, bv, out, CompareLT)
}
func scalarFloat64ColumnsLE(a, b []float64, av, bv, out memory.Bitmap) int {
	return scalarFloat64Columns(a, b, av, bv, out, CompareLE)
}
func scalarFloat64ColumnsEQ(a, b []float64, av, bv, out memory.Bitmap) int {
	return scalarFloat64Columns(a, b, av, bv, out, CompareEQ)
}
