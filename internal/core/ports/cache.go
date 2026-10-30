package ports

import (
	"context"
	"time"
)

// OrderMetadata represents cached order attributes for cross-event customer reference.
type OrderMetadata struct {
	CustomerID  string  `json:"customer_id"`
	OrderCode   string  `json:"order_code"`
	TotalAmount float64 `json:"total_amount"`
	Currency    string  `json:"currency"`
}

// OrderCache defines the port for storing and retrieving order session metadata.
type OrderCache interface {
	// SetOrder stores the order metadata with a specified TTL.
	SetOrder(ctx context.Context, orderID string, meta OrderMetadata, ttl time.Duration) error

	// GetOrder retrieves the cached order metadata by orderID.
	GetOrder(ctx context.Context, orderID string) (OrderMetadata, bool, error)

	// DeleteOrder removes the cached order metadata immediately upon order completion or cancellation.
	DeleteOrder(ctx context.Context, orderID string) error

	// Close safely closes the cache connection.
	Close() error
}

