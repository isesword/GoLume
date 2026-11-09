package physical

import (
	"fmt"
	"gocolumn/join"
	"math/rand"
	"reflect"
	"testing"
)

func TestGenericSpillRandomOracle(t *testing.T) {
	choices := []string{"", "a", "aa", "a\x00", "\x00a", "\x00", "汉🙂", string([]byte{255, 0, 128}), "bbbb"}
	rng := rand.New(rand.NewSource(0x5871))
	names := []string{"s", "x", "n", "u"}
	for trial := 0; trial < 20; trial++ {
		makeRows := func(n int) []genericJoinRow {
			out := make([]genericJoinRow, n)
			for i := range out {
				out[i] = genericJoinRow{s: choices[rng.Intn(len(choices))], x: choices[rng.Intn(len(choices))], n: int64(rng.Intn(3) - 1), u: uint32(rng.Intn(3)), null: rng.Intn(6)}
			}
			return out
		}
		left, right := makeRows(rng.Intn(19)), makeRows(rng.Intn(20))
		if len(left) > 0 && len(right) > 1 {
			right[0], right[1] = left[0], left[0]
		}
		for keys := 1; keys <= 4; keys++ {
			for typ := join.Inner; typ <= join.Full; typ++ {
				t.Run(fmt.Sprintf("trial%d/k%d/%s", trial, keys, typ), func(t *testing.T) {
					ld, rd := rng.Intn(2) == 0, rng.Intn(2) == 0
					if keys == 1 && ld && rd {
						rd = false
					}
					ks := make([]JoinKey, keys)
					for i := range ks {
						ks[i] = JoinKey{names[i], names[i]}
					}
					ctx, o, s, q, dir := openByteSpill(t, genericJoinSource(t, left, ld, 1+rng.Intn(5)), genericJoinSource(t, right, rd, 1+rng.Intn(6)), typ, ks, 1<<20, 64<<20)
					got, err := drainByteSpill(ctx, s)
					if err != nil {
						t.Fatal(err)
					}
					want := genericJoinExpected(left, right, keys, typ)
					sortBytePairs(got)
					sortBytePairs(want)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("got=%v want=%v", got, want)
					}
					assertByteSpillClean(t, o, q, dir)
				})
			}
		}
	}
}
