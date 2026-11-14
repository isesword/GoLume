package memory

import (
	"context"
	"errors"
	"testing"
)

type sharingTestManager struct {
	*BudgetManager
	result  Reservation
	ok      bool
	err     error
	invoked bool
}

func (m *sharingTestManager) Reserve(context.Context, int64) (Reservation, error) {
	return nil, errors.New("ordinary policy")
}
func (m *sharingTestManager) TryReserveImmediate(context.Context, int64) (Reservation, bool, error) {
	m.invoked = true
	return m.result, m.ok, m.err
}

type onlyOrdinaryManager struct{ Manager }

func TestNativeSharingManagerContract(t *testing.T) {
	var nilManager *BudgetManager
	for _, m := range []Manager{nil, nilManager, onlyOrdinaryManager{NewBudgetManager(10)}} {
		if _, e := NewNativeSharingManager(m); e == nil {
			t.Fatal("accepted invalid manager")
		}
	}
	for _, mode := range []string{"success", "miss", "error", "wrong", "nil", "typednil"} {
		t.Run(mode, func(t *testing.T) {
			base := NewBudgetManager(100)
			r, _ := base.Reserve(context.Background(), 10)
			raw := &sharingTestManager{BudgetManager: base, result: r, ok: true}
			switch mode {
			case "miss":
				raw.ok = false
			case "error":
				raw.err = errors.New("denied")
			case "wrong":
			case "nil":
				r.Release()
				raw.result = nil
			case "typednil":
				r.Release()
				var token *reservation
				raw.result = token
			}
			m, e := NewNativeSharingManager(raw)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = m.Reserve(context.Background(), 1); e == nil {
				t.Fatal("bypassed ordinary policy")
			}
			n := int64(10)
			if mode == "wrong" {
				n = 11
			}
			got, ok, e := m.TryReserveImmediate(context.Background(), n)
			if mode == "success" {
				if e != nil || !ok || got == nil {
					t.Fatal("success rejected")
				}
				got.Release()
			} else {
				if ok || got != nil {
					t.Fatal("malformed success")
				}
				if mode != "miss" && e == nil {
					t.Fatal("missing error")
				}
			}
			if base.Used() != 0 {
				t.Fatal("leaked token")
			}
		})
	}
	raw := &sharingTestManager{BudgetManager: NewBudgetManager(100)}
	m, _ := NewNativeSharingManager(raw)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, c := range []context.Context{nil, ctx} {
		if _, _, e := m.TryReserveImmediate(c, 1); e == nil {
			t.Fatal("context accepted")
		}
	}
	if raw.invoked {
		t.Fatal("called with invalid context")
	}
}

func TestNativeSharingManagerZeroAndInvalid(t *testing.T) {
	for _, m := range []*NativeSharingManager{nil, {}} {
		if _, err := m.Reserve(context.Background(), 1); err == nil {
			t.Fatal("invalid reserve")
		}
		if _, _, err := m.TryReserveImmediate(context.Background(), 1); err == nil {
			t.Fatal("invalid immediate")
		}
		if m.Used() != 0 || m.Limit() != 0 {
			t.Fatal("invalid observation")
		}
	}
	base := NewBudgetManager(10)
	m, err := NewNativeSharingManager(base)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int64{0, -1} {
		r, ok, err := m.TryReserveImmediate(context.Background(), n)
		if err != nil || !ok || r == nil || r.Bytes() != 0 {
			t.Fatalf("n=%d %v %v", n, ok, err)
		}
		r.Release()
	}
	if base.Used() != 0 {
		t.Fatal("non-positive allocation")
	}
}
