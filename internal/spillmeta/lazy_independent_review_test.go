package spillmeta

import (
	"context"
	"errors"
	"gocolumn/memory"
	"sync"
	"sync/atomic"
	"testing"
)

func TestIndependentLazyConcurrentOpenClose(t *testing.T) {
	for run := 0; run < 25; run++ {
		base := testBase(t)
		manager := memory.NewBudgetManager(1 << 20)
		var opens atomic.Int32
		s := NewLazyStore(func() (memory.SpillManager, error) { opens.Add(1); return base, nil }, manager)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, err := s.Spill(context.Background(), testObject())
				if err != nil && !errors.Is(err, memory.ErrSpillClosed) {
					t.Error(err)
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		}()
		close(start)
		wg.Wait()
		s.Close()
		if opens.Load() > 1 || manager.Used() != 0 || base.Used() != 0 {
			t.Fatalf("opens=%d memory=%d disk=%d", opens.Load(), manager.Used(), base.Used())
		}
	}
}
