package aggregate

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"math/big"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

func n29Wire(sum *big.Int, count uint64) []byte {
	u := new(big.Int).Set(sum)
	if u.Sign() < 0 {
		u.Add(u, new(big.Int).Lsh(big.NewInt(1), 256))
	}
	out := make([]byte, 40)
	for i := 0; i < 32; i++ {
		out[i] = byte(u.Uint64())
		u.Rsh(u, 8)
	}
	binary.LittleEndian.PutUint64(out[32:], count)
	return out
}
func TestN29DecimalPartialBoundOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(29))
	counts := []uint64{0, 1, 2, math.MaxInt64 - 1, math.MaxInt64, math.MaxInt64 + 1, math.MaxUint64}
	for p := int32(1); p <= 38; p++ {
		max := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(p)), nil), big.NewInt(1))
		for _, count := range counts {
			bound := new(big.Int).Mul(max, new(big.Int).SetUint64(count))
			for _, delta := range []int64{-1, 0, 1} {
				for _, negative := range []bool{false, true} {
					sum := new(big.Int).Add(bound, big.NewInt(delta))
					if negative {
						sum.Neg(sum)
					}
					wire := n29Wire(sum, count)
					var state DecimalSumState
					_ = state.Add(decimal128.FromI64(3))
					before := state
					err := state.DecodeChecked(wire, p)
					abs := new(big.Int).Abs(new(big.Int).Set(sum))
					valid := count <= math.MaxInt64 && abs.Cmp(bound) <= 0
					if (err == nil) != valid {
						t.Fatalf("p=%d n=%d delta=%d negative=%v err=%v", p, count, delta, negative, err)
					}
					if !valid {
						if state != before || !errors.Is(err, ErrDecimalSumStateEncoding) {
							t.Fatal("failed decode mutated state")
						}
						continue
					}
					var out [40]byte
					if err := state.EncodeChecked(out[:], p); err != nil || !bytes.Equal(out[:], wire) {
						t.Fatalf("round trip err=%v", err)
					}
				}
			}
		}
		for sample := 0; sample < 64; sample++ {
			var wire [40]byte
			_, _ = rng.Read(wire[:])
			count := rng.Uint64()
			binary.LittleEndian.PutUint64(wire[32:], count)
			magnitude := new(big.Int)
			for i := 31; i >= 0; i-- {
				magnitude.Lsh(magnitude, 8)
				magnitude.Add(magnitude, big.NewInt(int64(wire[i])))
			}
			if wire[31]&128 != 0 {
				magnitude.Sub(magnitude, new(big.Int).Lsh(big.NewInt(1), 256))
			}
			magnitude.Abs(magnitude)
			bound := new(big.Int).Mul(max, new(big.Int).SetUint64(count))
			want := count <= math.MaxInt64 && magnitude.Cmp(bound) <= 0
			var state DecimalSumState
			if err := state.DecodeChecked(wire[:], p); (err == nil) != want {
				t.Fatal("random oracle disagreement")
			}
		}
	}
}

func TestN29DecimalPartialFailureAtomicity(t *testing.T) {
	var s DecimalSumState
	_ = s.Add(decimal128.FromI64(9))
	before := s
	for _, p := range []int32{-1, 0, 39, math.MaxInt32} {
		dst := bytes.Repeat([]byte{0xa5}, 40)
		copyBefore := append([]byte(nil), dst...)
		if s.EncodeChecked(dst, p) == nil || !bytes.Equal(dst, copyBefore) {
			t.Fatal("invalid precision changed output")
		}
		if s.DecodeChecked(make([]byte, 40), p) == nil || s != before {
			t.Fatal("invalid precision changed state")
		}
	}
	for _, n := range []int{0, 1, 39, 41, 80} {
		b := bytes.Repeat([]byte{0x5a}, n)
		old := append([]byte(nil), b...)
		if s.EncodeChecked(b, 38) == nil || !bytes.Equal(b, old) {
			t.Fatal("bad length encode")
		}
		if s.DecodeChecked(b, 38) == nil || s != before {
			t.Fatal("bad length decode")
		}
	}
	forged := []DecimalSumState{{limbs: [4]uint64{1}, count: 0}, {limbs: [4]uint64{10}, count: 1}, {limbs: [4]uint64{0, 0, 0, 1 << 63}, count: math.MaxInt64}, {count: math.MaxUint64}}
	for _, bad := range forged {
		dst := bytes.Repeat([]byte{0xa5}, 40)
		old := append([]byte(nil), dst...)
		if bad.EncodeChecked(dst, 1) == nil || !bytes.Equal(dst, old) {
			t.Fatal("forged encode mutated destination")
		}
	}
	var nilState *DecimalSumState
	if nilState.DecodeChecked(make([]byte, 40), 38) == nil {
		t.Fatal("nil receiver")
	}
}

func TestN29DecimalPartialWideCancellationAndNoAlloc(t *testing.T) {
	max := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1))
	wide := new(big.Int).Mul(max, big.NewInt(4))
	var a, b DecimalSumState
	if err := a.DecodeChecked(n29Wire(wide, 4), 38); err != nil {
		t.Fatal(err)
	}
	if _, _, err := a.Finalize(); !errors.Is(err, ErrDecimalSumOverflow) {
		t.Fatal("fixture must exceed final range")
	}
	if err := b.DecodeChecked(n29Wire(new(big.Int).Neg(wide), 4), 38); err != nil {
		t.Fatal(err)
	}
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	v, valid, err := a.Finalize()
	if err != nil || !valid || v != decimal128.FromI64(0) || a.Count() != 8 {
		t.Fatal("wide cancellation lost")
	}
	var wire [40]byte
	if allocations := testing.AllocsPerRun(1000, func() {
		if a.EncodeChecked(wire[:], 38) != nil {
			panic("encode")
		}
		if b.DecodeChecked(wire[:], 38) != nil {
			panic("decode")
		}
	}); allocations != 0 {
		t.Fatalf("allocations %v", allocations)
	}
}
