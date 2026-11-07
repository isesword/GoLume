package expr

import (
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

func TestReviewDecimalOverflowClassification(t *testing.T) {
	r := rand.New(rand.NewSource(9173))
	for i := 0; i < 100000; i++ {
		n := 1 + r.Intn(700)
		var b strings.Builder
		if r.Intn(2) == 0 {
			b.WriteByte('-')
		}
		dot := r.Intn(n + 1)
		for j := 0; j < n; j++ {
			if j == dot {
				b.WriteByte('.')
			}
			b.WriteByte(byte('0' + r.Intn(10)))
		}
		if dot == n {
			b.WriteByte('.')
		}
		b.WriteByte('e')
		b.WriteString(strconv.Itoa(r.Intn(2200) - 1100))
		s := b.String()
		for _, bits := range []int{32, 64} {
			want, _ := strconv.ParseFloat(s, bits)
			got, err := parseDecimalFloat([]byte(s), bits)
			if err != nil || math.Float64bits(want) != math.Float64bits(got) {
				t.Fatalf("bits=%d s=%s got=%v err=%v want=%v", bits, s, got, err, want)
			}
		}
	}
}
