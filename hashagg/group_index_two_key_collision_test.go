package hashagg

import (
	"encoding/binary"
	"testing"
)

func TestN24GroupIndexTwoInt64KeysForcedHashCollision(t *testing.T) {
	// Match the Decimal resident grouping key shape: one validity byte
	// followed by an eight-byte value for each Int64 lane.
	encode := func(a, b int64, av, bv bool) (key [18]byte) {
		if av {
			key[0] = 1
			binary.LittleEndian.PutUint64(key[1:9], uint64(a))
		}
		if bv {
			key[9] = 1
			binary.LittleEndian.PutUint64(key[10:18], uint64(b))
		}
		return
	}
	keys := [][18]byte{
		encode(-42, 0, true, true),
		encode(-42, 1, true, true),
		encode(-42, 0, true, false),
	}
	// Seed a valid probe chain with intentionally identical full hashes.
	// This forces byte equality to do the distinguishing; generating three
	// natural 64-bit hash collisions is not required to test that branch.
	const hash = uint64(7)
	x := GroupIndex{slots: make([]int, 8), entries: make([]groupIndexEntry, len(keys)), size: len(keys)}
	const mask = 7
	for id, key := range keys {
		slot := int(hash+uint64(id)) & mask // wrap through slot zero
		x.entries[id] = groupIndexEntry{hash: hash, offset: len(x.arena), length: len(key)}
		x.slots[slot] = id + 1
		x.arena = append(x.arena, key[:]...)
	}
	for id, key := range keys {
		slot, found, err := x.findWithCheck(key[:], hash, nil)
		if err != nil || !found || x.slots[slot] != id+1 {
			t.Fatalf("tuple %d: slot=%d found=%t err=%v", id, slot, found, err)
		}
	}

	missing := encode(-42, 2, true, true)
	firstNull := encode(0, 0, false, true)
	for _, tc := range []struct {
		name string
		key  []byte
	}{
		{"different second value", missing[:]},
		{"different first validity", firstNull[:]},
		{"matching prefix only", keys[0][:9]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			slot, found, err := x.findWithCheck(tc.key, hash, nil)
			if err != nil || found || x.slots[slot] != 0 {
				t.Fatalf("nonidentical tuple resolved to existing group: slot=%d found=%t err=%v", slot, found, err)
			}
		})
	}
}
