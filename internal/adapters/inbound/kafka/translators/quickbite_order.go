package translators

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"quicknotify/internal/core/domain"
	"quicknotify/internal/core/ports"

	"go.uber.org/zap"
)

const (
	// OrderTTL defines the 9-day retention period in Redis.
	OrderTTL = 9 * 24 * time.Hour
)

// QuickbiteOrderItem represents an individual item in the food order.
type QuickbiteOrderItem struct {
	FoodItemID          string  `json:"foodItemId"`
	ItemName            string  `json:"itemName"`
	Quantity            int     `json:"quantity"`
	UnitPrice           float64 `json:"unitPrice"`
	SelectedVariantName string  `json:"selectedVariantName,omitempty"`
	SelectedToppings    string  `json:"selectedToppings,omitempty"`
}

// QuickbiteOrderRawEvent represents the JSON payload across all order event types in the 'order-events' topic.
type QuickbiteOrderRawEvent struct {
	EventID       string               `json:"eventId"`
	OrderID       string               `json:"orderId"`
	OrderCode     string               `json:"orderCode,omitempty"`
	CustomerID    string               `json:"customerId,omitempty"`
	CustomerEmail string               `json:"customerEmail,omitempty"`
	RestaurantID  string               `json:"restaurantId,omitempty"`
	TotalAmount   float64              `json:"totalAmount,omitempty"`
	Currency      string               `json:"currency,omitempty"`
	Reason        string               `json:"reason,omitempty"`
	Code          string               `json:"code,omitempty"`
	CorrelationID string               `json:"correlationId,omitempty"`
	OccurredAt    string               `json:"occurredAt,omitempty"`
	Items         []QuickbiteOrderItem `json:"items,omitempty"`
}

// QuickbiteOrderTranslator translates QuickBite order Kafka events into standard NotificationEnvelopes,
// utilizing Redis (ports.OrderCache) for cross-event customer and order code resolution.
type QuickbiteOrderTranslator struct {
	cache  ports.OrderCache
	logger *zap.Logger
}

// NewQuickbiteOrderTranslator creates a new QuickbiteOrderTranslator instance with cache support.
func NewQuickbiteOrderTranslator(cache ports.OrderCache, logger *zap.Logger) ports.MessageTranslator {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &QuickbiteOrderTranslator{
		cache:  cache,
		logger: logger,
	}
}

// Translate converts a raw Kafka key and message payload from the 'order-events' topic into a NotificationEnvelope.
func (t *QuickbiteOrderTranslator) Translate(key []byte, value []byte) (domain.NotificationEnvelope, error) {
	var raw QuickbiteOrderRawEvent
	if err := json.Unmarshal(value, &raw); err != nil {
		return domain.NotificationEnvelope{}, fmt.Errorf("quickbite unmarshal error: %w", err)
	}

	if raw.OrderID == "" {
		return domain.NotificationEnvelope{}, fmt.Errorf("quickbite event missing orderId")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	eventKey := strings.Trim(string(key), "\" \t\r\n")

	// 1. Maintain Redis cache: store order metadata with a 9-day TTL on events that include customerId
	if raw.CustomerID != "" {
		if t.cache != nil {
			_ = t.cache.SetOrder(ctx, raw.OrderID, ports.OrderMetadata{
				CustomerID:  raw.CustomerID,
				OrderCode:   raw.OrderCode,
				TotalAmount: raw.TotalAmount,
				Currency:    raw.Currency,
			}, OrderTTL)
		}
	} else {
		// Look up cached customerId and orderCode for events that omit them (confirmed, preparing, delivering, completed)
		if t.cache != nil {
			if meta, found, err := t.cache.GetOrder(ctx, raw.OrderID); err == nil && found {
				raw.CustomerID = meta.CustomerID
				if raw.OrderCode == "" {
					raw.OrderCode = meta.OrderCode
				}
				if raw.TotalAmount == 0 {
					raw.TotalAmount = meta.TotalAmount
				}
				if raw.Currency == "" {
					raw.Currency = meta.Currency
				}
			}
		}
	}

	// 2. Determine target user and friendly display code
	targetUserID := raw.CustomerID
	if targetUserID == "" {
		targetUserID = raw.OrderID
	}

	displayCode := raw.OrderCode
	if displayCode == "" {
		displayCode = raw.OrderID
	}

	// 3. Generate friendly notification message and template ID based on Kafka event key
	message, templateID := t.formatNotification(eventKey, displayCode, raw)

	// 4. Delete cache immediately upon completion or cancellation (order.completed, order.reverted-to-draft)
	if eventKey == "order.completed" || eventKey == "order.reverted-to-draft" {
		if t.cache != nil {
			if err := t.cache.DeleteOrder(ctx, raw.OrderID); err != nil {
				t.logger.Warn("Failed to delete order from cache upon completion",
					zap.String("order_id", raw.OrderID),
					zap.String("event_key", eventKey),
					zap.Error(err),
				)
			} else {
				t.logger.Debug("Removed order from cache upon completion/cancellation",
					zap.String("order_id", raw.OrderID),
					zap.String("event_key", eventKey),
				)
			}
		}
	}

	eventID := raw.EventID
	if eventID == "" {
		eventID = fmt.Sprintf("qb_%s_%d", raw.OrderID, time.Now().UnixNano())
	}

	channels := []domain.Channel{domain.ChannelWebSocket}
	if raw.CustomerEmail != "" {
		channels = append(channels, domain.ChannelEmail)
	}

	contentData := map[string]interface{}{
		"message":        message,
		"event_key":      eventKey,
		"order_id":       raw.OrderID,
		"order_code":     raw.OrderCode,
		"customer_id":    raw.CustomerID,
		"restaurant_id":  raw.RestaurantID,
		"total_amount":   raw.TotalAmount,
		"currency":       raw.Currency,
		"correlation_id": raw.CorrelationID,
		"occurred_at":    raw.OccurredAt,
	}

	if raw.Reason != "" {
		contentData["reason"] = raw.Reason
		contentData["code"] = raw.Code
	}

	if len(raw.Items) > 0 {
		contentData["items"] = raw.Items
	}

	return domain.NotificationEnvelope{
		EventID:   eventID,
		ProjectID: "quickbite",
		Scope:     domain.ScopeUser,
		Channels:  channels,
		Target: domain.TargetInfo{
			UserID: targetUserID,
			Email:  raw.CustomerEmail,
		},
		Content: domain.ContentInfo{
			Type:       "template",
			TemplateID: templateID,
			Data:       contentData,
		},
	}, nil
}

// formatNotification generates the Vietnamese user notification text and corresponding template ID.
func (t *QuickbiteOrderTranslator) formatNotification(eventKey, displayCode string, raw QuickbiteOrderRawEvent) (message, templateID string) {
	currency := raw.Currency
	if currency == "" {
		currency = "VND"
	}

	switch eventKey {
	case "order.submitted":
		return fmt.Sprintf("Đơn hàng %s đã được chấp nhận.", displayCode), "ORDER_SUBMITTED"

	case "order.waiting-payment":
		amountStr := formatAmount(raw.TotalAmount)
		return fmt.Sprintf("Đơn hàng %s đang chờ bạn thanh toán %s %s.", displayCode, amountStr, currency), "ORDER_WAITING_PAYMENT"

	case "order.confirmed":
		return fmt.Sprintf("Đơn hàng %s đã được xác nhận.", displayCode), "ORDER_CONFIRMED"

	case "order.preparing":
		return fmt.Sprintf("Đơn hàng %s đang được chuẩn bị.", displayCode), "ORDER_PREPARING"

	case "order.delivering":
		return fmt.Sprintf("Đơn hàng %s đang trên đường giao.", displayCode), "ORDER_DELIVERING"

	case "order.completed":
		return fmt.Sprintf("Đơn hàng %s đã được giao thành công.", displayCode), "ORDER_COMPLETED"

	case "order.reverted-to-draft":
		reason := raw.Reason
		if reason == "" {
			reason = "Hết hàng"
		}
		return fmt.Sprintf("Đơn hàng %s không thể thực hiện do hết hàng: %s", displayCode, reason), "ORDER_REVERTED_TO_DRAFT"

	default:
		return fmt.Sprintf("Đơn hàng %s có cập nhật mới.", displayCode), "ORDER_UPDATED"
	}
}

// formatAmount formats numbers with comma thousands separator (e.g., 200000 -> "200,000").
func formatAmount(amount float64) string {
	intAmount := int64(amount)
	in := fmt.Sprintf("%d", intAmount)
	n := len(in)
	if n <= 3 {
		return in
	}

	var sb strings.Builder
	remainder := n % 3
	if remainder > 0 {
		sb.WriteString(in[:remainder])
		if remainder < n {
			sb.WriteString(",")
		}
	}

	for i := remainder; i < n; i += 3 {
		sb.WriteString(in[i : i+3])
		if i+3 < n {
			sb.WriteString(",")
		}
	}
	return sb.String()
}
