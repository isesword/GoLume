package observer

import (
	"sync"
	"time"
)

// Event records one execution-stage measurement emitted by the engine.
type Event struct {
	Stage      string
	Duration   time.Duration
	Rows       int64
	Bytes      int64
	Workers    int
	Attributes map[string]string
}

// Observer receives execution events without participating in operator hot loops.
type Observer interface {
	Observe(Event)
}

// Nop discards all execution events.
type Nop struct{}

// Observe discards e.
func (Nop) Observe(Event) {}

// Collector stores events in memory and is safe for concurrent observers.
type Collector struct {
	mu     sync.Mutex
	events []Event
}

// Observe appends e to the collector.
func (c *Collector) Observe(e Event) {
	c.mu.Lock()
	c.events = append(c.events, e)
	c.mu.Unlock()
}

// Events returns a snapshot copy of collected events.
func (c *Collector) Events() []Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Event, len(c.events))
	copy(out, c.events)
	return out
}
