package expr

import (
	"fmt"
	"testing"
)

// Compilation includes binding, structural CSE and physical storage planning.
// Expressions are built outside the timer. Shared diamonds deliberately repeat
// source subtrees; this measures the public API, including its tree binder.
func BenchmarkExpressionCompile(b *testing.B) {
	for _, depth := range []int{8, 32, 128, 512} {
		chain := livenessChain(Col("x"), depth, false)
		cases := []struct {
			name    string
			outputs []NamedExpr
		}{
			{"chain", []NamedExpr{Named(chain, "out")}},
			{"outputs", []NamedExpr{Named(chain, "a"), Named(Add(chain, I64(2)), "b"), Named(Mul(chain, I64(3)), "c")}},
			{"regions", []NamedExpr{Named(If(Col("c"), chain, If(Not(Col("c")), chain, I64(0))), "out")}},
		}
		for _, tc := range cases {
			b.Run(fmt.Sprintf("%s/depth=%d", tc.name, depth), func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					if _, err := CompileExpressions(livenessSchema(), tc.outputs); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
	for _, depth := range []int{4, 8, 12} {
		var e Expr = Col("x")
		for range depth {
			e = Add(e, e)
		}
		outputs := []NamedExpr{Named(e, "out")}
		b.Run(fmt.Sprintf("diamond/depth=%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if _, err := CompileExpressions(livenessSchema(), outputs); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
