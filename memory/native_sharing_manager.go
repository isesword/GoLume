package memory

import (
	"context"
	"fmt"
	"reflect"
)

// NativeSharingManager explicitly opts a custom manager into native fan-out
// sharing. Ordinary reservations still use the supplied manager's Reserve.
// Wrappers around this adapter are not implicitly opted in.
type NativeSharingManager struct {
	manager   Manager
	immediate ImmediateReservationManager
}

// NewNativeSharingManager opts manager into speculative native sharing.
// manager must implement ImmediateReservationManager against the SAME accounting
// and policy as Reserve. Its immediate method must never wait, reclaim, spill, or
// call user callbacks. In particular, an inherited immediate method is unsafe if
// an overriding Reserve adds policy that method does not enforce. The caller
// certifies this contract by explicitly constructing the adapter.
// Opaque data and unavailable immediate admission retain the guard fallback.
func NewNativeSharingManager(manager Manager) (*NativeSharingManager, error) {
	if nilSharingValue(manager) {
		return nil, fmt.Errorf("native sharing requires a non-nil manager")
	}
	immediate, ok := manager.(ImmediateReservationManager)
	if !ok {
		return nil, fmt.Errorf("native sharing requires ImmediateReservationManager")
	}
	return &NativeSharingManager{manager: manager, immediate: immediate}, nil
}
func nilSharingValue(v any) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return r.IsNil()
	}
	return false
}

// Reserve preserves the custom manager's ordinary reservation policy.
func (m *NativeSharingManager) Reserve(ctx context.Context, bytes int64) (Reservation, error) {
	if m == nil || m.manager == nil {
		return nil, fmt.Errorf("native sharing manager is uninitialized")
	}
	return m.manager.Reserve(ctx, bytes)
}

// Used reports the underlying manager's live accounting.
func (m *NativeSharingManager) Used() int64 {
	if m == nil || m.manager == nil {
		return 0
	}
	return m.manager.Used()
}

// Limit reports the underlying manager's configured limit.
func (m *NativeSharingManager) Limit() int64 {
	if m == nil || m.manager == nil {
		return 0
	}
	return m.manager.Limit()
}

// TryReserveImmediate delegates nonblocking admission to the opted-in manager.
// Malformed returned tokens are released and rejected, never used for sharing.
func (m *NativeSharingManager) TryReserveImmediate(ctx context.Context, bytes int64) (Reservation, bool, error) {
	if m == nil || m.immediate == nil {
		return nil, false, fmt.Errorf("native sharing manager is uninitialized")
	}
	if err := immediateReservationContextError(ctx); err != nil {
		return nil, false, err
	}
	r, ok, err := m.immediate.TryReserveImmediate(ctx, bytes)
	if err != nil || !ok {
		if !nilSharingValue(r) {
			r.Release()
		}
		return nil, false, err
	}
	want := bytes
	if want < 0 {
		want = 0
	}
	if nilSharingValue(r) {
		return nil, false, fmt.Errorf("native sharing admission returned nil token")
	}
	if r.Bytes() != want {
		r.Release()
		return nil, false, fmt.Errorf("native sharing admission returned wrong token charge")
	}
	return r, true, nil
}
