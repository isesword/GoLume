// Package physical contains reusable columnar source operators produced by the
// lazy physical lowering layer. Operators are selected once per plan and avoid
// interface dispatch inside row-level arithmetic loops.
package physical
