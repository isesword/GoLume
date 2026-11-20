package aggregate_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/aggregate"
)

// n29PublicWire is a test-only exact integer encoding, not a production codec oracle.
func n29PublicWire(sum *big.Int, count uint64) []byte {
	out := make([]byte, 40)
	u := new(big.Int).Set(sum)
	if u.Sign() < 0 {
		u.Add(u, new(big.Int).Lsh(big.NewInt(1), 256))
	}
	raw := u.Bytes()
	for i := 0; i < len(raw); i++ {
		out[i] = raw[len(raw)-1-i]
	}
	binary.LittleEndian.PutUint64(out[32:], count)
	return out
}
func n29PublicDecode(t *testing.T, sum *big.Int, count uint64, p int32) aggregate.DecimalSumState {
	t.Helper()
	var s aggregate.DecimalSumState
	if e := s.DecodeChecked(n29PublicWire(sum, count), p); e != nil {
		t.Fatal(e)
	}
	return s
}
func n29PublicEncode(t *testing.T, s aggregate.DecimalSumState, p int32) []byte {
	t.Helper()
	out := make([]byte, aggregate.DecimalSumStateEncodedBytes)
	if e := s.EncodeChecked(out, p); e != nil {
		t.Fatal(e)
	}
	return out
}
func n29PublicAssert(t *testing.T, s aggregate.DecimalSumState, sum *big.Int, count uint64, p int32) {
	t.Helper()
	if s.Count() != count {
		t.Fatal("count", s.Count(), count)
	}
	if got := n29PublicEncode(t, s, p); !bytes.Equal(got, n29PublicWire(sum, count)) {
		t.Fatalf("exact 40B mismatch: got%x want%x", got, n29PublicWire(sum, count))
	}
	v, valid, e := s.Finalize()
	limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil)
	if count == 0 {
		if e != nil || valid {
			t.Fatal("empty must remain NULL", v, valid, e)
		}
		return
	}
	if new(big.Int).Abs(sum).Cmp(limit) >= 0 {
		if !errors.Is(e, aggregate.ErrDecimalSumOverflow) {
			t.Fatal("final overflow", e)
		}
		return
	}
	if e != nil || !valid || v.BigInt().Cmp(sum) != 0 {
		t.Fatal("final coefficient", v, valid, e, sum)
	}
}
func TestN29ExternalDecimalCodecWideCancellation(t *testing.T) {
	max := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1))
	wide := new(big.Int).Mul(max, big.NewInt(3))
	// Each partial is outside signed128 and p38; only the final complete sum narrows.
	pos := n29PublicDecode(t, wide, 3, 38)
	neg := n29PublicDecode(t, new(big.Int).Neg(wide), 3, 38)
	small := n29PublicDecode(t, big.NewInt(7), 1, 38)
	n29PublicAssert(t, pos, wide, 3, 38)
	n29PublicAssert(t, neg, new(big.Int).Neg(wide), 3, 38)
	parts := []aggregate.DecimalSumState{pos, neg, small}
	for _, order := range [][3]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}} {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			var dst aggregate.DecimalSumState
			for _, i := range order {
				wire := n29PublicEncode(t, parts[i], 38)
				var restored aggregate.DecimalSumState
				if e := restored.DecodeChecked(wire, 38); e != nil {
					t.Fatal(e)
				}
				for j := range wire {
					wire[j] = 0xa5
				}
				if e := dst.Merge(restored); e != nil {
					t.Fatal(e)
				}
			}
			n29PublicAssert(t, dst, big.NewInt(7), 7, 38)
		})
	}
	for i, p := range parts {
		expect := []*big.Int{wide, new(big.Int).Neg(wide), big.NewInt(7)}[i]
		n29PublicAssert(t, p, expect, []uint64{3, 3, 1}[i], 38)
	}
}
func TestN29ExternalDecimalCodecIndependentPartitions(t *testing.T) {
	rng := rand.New(rand.NewSource(290040))
	for _, p := range []int32{1, 9, 19, 38} {
		t.Run(fmt.Sprint(p), func(t *testing.T) {
			max := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(p)), nil), big.NewInt(1))
			for trial := 0; trial < 75; trial++ {
				sum := new(big.Int)
				var count uint64
				var parts []aggregate.DecimalSumState
				for part := 0; part < 5; part++ {
					var state aggregate.DecimalSumState
					local := new(big.Int)
					var n uint64
					for row := 0; row < 13; row++ {
						if rng.Intn(5) == 0 {
							continue
						} // NULL never contributes to count or sum.
						x := new(big.Int).Rand(rng, new(big.Int).Add(max, big.NewInt(1)))
						if rng.Intn(2) == 0 {
							x.Neg(x)
						}
						if e := state.Add(decimal128.FromBigInt(x)); e != nil {
							t.Fatal(e)
						}
						local.Add(local, x)
						sum.Add(sum, x)
						n++
						count++
					}
					n29PublicAssert(t, state, local, n, p)
					wire := n29PublicEncode(t, state, p)
					var restored aggregate.DecimalSumState
					if e := restored.DecodeChecked(wire, p); e != nil {
						t.Fatal(e)
					}
					parts = append(parts, restored)
				}
				rng.Shuffle(len(parts), func(i, j int) { parts[i], parts[j] = parts[j], parts[i] })
				var merged aggregate.DecimalSumState
				for _, s := range parts {
					if e := merged.Merge(s); e != nil {
						t.Fatal(e)
					}
				}
				n29PublicAssert(t, merged, sum, count, p)
			}
		})
	}
	var empty aggregate.DecimalSumState
	n29PublicAssert(t, empty, big.NewInt(0), 0, 38)
	restored := n29PublicDecode(t, big.NewInt(0), 0, 38)
	if e := restored.Merge(empty); e != nil {
		t.Fatal(e)
	}
	n29PublicAssert(t, restored, big.NewInt(0), 0, 38)
	zero := n29PublicDecode(t, big.NewInt(0), 2, 38)
	n29PublicAssert(t, zero, big.NewInt(0), 2, 38)
}
func TestN29ExternalDecimalCodecCorruptionAtomicity(t *testing.T) {
	// A precise nonzero receiver makes accidental partial writes observable.
	original := n29PublicDecode(t, big.NewInt(-12345), 7, 5)
	saved := n29PublicEncode(t, original, 5)
	for name, tc := range map[string]struct {
		wire []byte
		p    int32
	}{
		"short": {make([]byte, 39), 38}, "trailing": {make([]byte, 41), 38}, "p0": {n29PublicWire(big.NewInt(0), 0), 0}, "p39": {n29PublicWire(big.NewInt(0), 0), 39}, "negative-p": {n29PublicWire(big.NewInt(0), 0), -1},
		"zero-count-positive": {n29PublicWire(big.NewInt(1), 0), 38}, "zero-count-negative": {n29PublicWire(big.NewInt(-1), 0), 38}, "count-over-limit": {n29PublicWire(big.NewInt(0), uint64(math.MaxInt64)+1), 38},
		"p1-impossible-positive": {n29PublicWire(big.NewInt(19), 2), 1}, "p1-impossible-negative": {n29PublicWire(big.NewInt(-19), 2), 1}, "positive-high-limb": {n29PublicWire(new(big.Int).Lsh(big.NewInt(1), 200), uint64(math.MaxInt64)), 38}, "negative-high-limb": {n29PublicWire(new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 200)), uint64(math.MaxInt64)), 38},
	} {
		t.Run(name, func(t *testing.T) {
			dst := original
			input := append([]byte(nil), tc.wire...)
			e := dst.DecodeChecked(input, tc.p)
			if !errors.Is(e, aggregate.ErrDecimalSumStateEncoding) {
				t.Fatal("invalid wire accepted", e)
			}
			if !bytes.Equal(input, tc.wire) || !bytes.Equal(saved, n29PublicEncode(t, dst, 5)) {
				t.Fatal("failed decode mutated input or receiver")
			}
		})
	}
	for _, p := range []int32{0, 1, 39} {
		dst := bytes.Repeat([]byte{0x5a}, 40)
		before := append([]byte(nil), dst...)
		if e := original.EncodeChecked(dst, p); !errors.Is(e, aggregate.ErrDecimalSumStateEncoding) {
			t.Fatal("invalid encode precision", p, e)
		}
		if !bytes.Equal(dst, before) || !bytes.Equal(saved, n29PublicEncode(t, original, 5)) {
			t.Fatal("failed encode mutated buffer or original")
		}
	}
	for _, size := range []int{0, 39, 41} {
		dst := bytes.Repeat([]byte{0xc3}, size)
		before := append([]byte(nil), dst...)
		if e := original.EncodeChecked(dst, 5); !errors.Is(e, aggregate.ErrDecimalSumStateEncoding) {
			t.Fatal("invalid wire size", e)
		}
		if !bytes.Equal(dst, before) {
			t.Fatal("partial encode write")
		}
	}
	full := n29PublicDecode(t, big.NewInt(0), uint64(math.MaxInt64), 38)
	snapshot := n29PublicEncode(t, full, 38)
	one := n29PublicDecode(t, big.NewInt(1), 1, 38)
	if e := full.Merge(one); !errors.Is(e, aggregate.ErrDecimalSumCount) {
		t.Fatal("merge count overflow", e)
	}
	if !bytes.Equal(snapshot, n29PublicEncode(t, full, 38)) {
		t.Fatal("failed merge mutated destination")
	}
	n29PublicAssert(t, one, big.NewInt(1), 1, 38)
}
func TestN29ExternalDecimalCodecMaxCountMagnitude(t *testing.T) {
	for _, p := range []int32{1, 17, 38} {
		max := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(p)), nil), big.NewInt(1))
		bound := new(big.Int).Mul(max, new(big.Int).SetUint64(uint64(math.MaxInt64)))
		for _, sign := range []int64{-1, 1} {
			v := new(big.Int).Mul(bound, big.NewInt(sign))
			s := n29PublicDecode(t, v, uint64(math.MaxInt64), p)
			n29PublicAssert(t, s, v, uint64(math.MaxInt64), p)
			invalid := new(big.Int).Add(bound, big.NewInt(1))
			invalid.Mul(invalid, big.NewInt(sign))
			before := n29PublicEncode(t, s, p)
			if e := s.DecodeChecked(n29PublicWire(invalid, uint64(math.MaxInt64)), p); !errors.Is(e, aggregate.ErrDecimalSumStateEncoding) {
				t.Fatal("count*precision boundary+1 accepted", p, sign, e)
			}
			if !bytes.Equal(before, n29PublicEncode(t, s, p)) {
				t.Fatal("boundary failure mutated state")
			}
		}
	}
}
