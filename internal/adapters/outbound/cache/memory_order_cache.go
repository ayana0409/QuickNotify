package cache

import (
	"context"
	"sync"
	"time"

	"quicknotify/internal/core/ports"
)

// MemoryOrderCache provides an in-memory implementation of ports.OrderCache for testing and local fallback.
type MemoryOrderCache struct {
	data map[string]ports.OrderMetadata
	mu   sync.RWMutex
}

// NewMemoryOrderCache creates a new in-memory cache instance.
func NewMemoryOrderCache() *MemoryOrderCache {
	return &MemoryOrderCache{
		data: make(map[string]ports.OrderMetadata),
	}
}

func (m *MemoryOrderCache) SetOrder(ctx context.Context, orderID string, meta ports.OrderMetadata, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[orderID] = meta
	return nil
}

func (m *MemoryOrderCache) GetOrder(ctx context.Context, orderID string) (ports.OrderMetadata, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	meta, ok := m.data[orderID]
	return meta, ok, nil
}

func (m *MemoryOrderCache) DeleteOrder(ctx context.Context, orderID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, orderID)
	return nil
}

func (m *MemoryOrderCache) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string]ports.OrderMetadata)
	return nil
}

