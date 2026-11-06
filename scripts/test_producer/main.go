package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	broker := flag.String("broker", "localhost:9092", "Kafka broker address (e.g. localhost:9092 or localhost:9094)")
	httpURL := flag.String("http", "", "Direct HTTP endpoint (e.g. http://localhost:8080/api/v1/notify) - bypasses Kafka for local standalone demo")
	apiKey := flag.String("api-key", "", "API Key for project authorization (e.g. qn_sec_shorterlink_live_9a8b7c6d5e4f or qn_sec_root_master_system_key_2026)")
	topicType := flag.String("type", "core_user", "Event type to publish: core_user, core_project, core_system, quickbite_created, quickbite_delivering, quickbite_completed, edutrack")
	projectID := flag.String("project", "quickbite", "Project ID for the notification")
	userID := flag.String("user", "user_101", "Target User ID")
	orderID := flag.String("order", "ord_9988", "Order ID (for quickbite events)")
	messageText := flag.String("message", "", "Custom notification message text (optional)")
	flag.Parse()

	var targetTopic string
	var messageKey string
	var payloadBytes []byte
	now := time.Now()

	switch *topicType {
	case "core_user":
		targetTopic = "global-notify-events"
		messageKey = *projectID + ":" + *userID
		msg := *messageText
		if msg == "" {
			msg = fmt.Sprintf("Hello %s! You have a personalized notification delivered at %s", *userID, now.Format("15:04:05"))
		}
		payload := map[string]interface{}{
			"event_id":   fmt.Sprintf("evt_user_%d", now.UnixNano()),
			"project_id": *projectID,
			"scope":      "user",
			"priority":   "normal",
			"channels":   []string{"websocket"},
			"target": map[string]string{
				"user_id": *userID,
			},
			"content": map[string]interface{}{
				"type": "raw",
				"data": map[string]interface{}{
					"message": msg,
				},
			},
		}
		payloadBytes, _ = json.Marshal(payload)

	case "core_project":
		targetTopic = "global-notify-events"
		messageKey = *projectID
		msg := *messageText
		if msg == "" {
			msg = fmt.Sprintf("Flash Sale: 50%% discount for all %s users! (Broadcast at %s)", *projectID, now.Format("15:04:05"))
		}
		payload := map[string]interface{}{
			"event_id":   fmt.Sprintf("evt_proj_%d", now.UnixNano()),
			"project_id": *projectID,
			"scope":      "project_global",
			"priority":   "high",
			"channels":   []string{"websocket"},
			"target":     map[string]string{},
			"content": map[string]interface{}{
				"type": "raw",
				"data": map[string]interface{}{
					"message": msg,
				},
			},
		}
		payloadBytes, _ = json.Marshal(payload)

	case "core_system":
		targetTopic = "global-notify-events"
		messageKey = "system"
		msg := *messageText
		if msg == "" {
			msg = fmt.Sprintf("SYSTEM NOTICE: Scheduled maintenance in 15 minutes across all services! (Sent at %s)", now.Format("15:04:05"))
		}
		payload := map[string]interface{}{
			"event_id":   fmt.Sprintf("evt_sys_%d", now.UnixNano()),
			"project_id": "system",
			"scope":      "system_global",
			"priority":   "high",
			"channels":   []string{"websocket"},
			"target":     map[string]string{},
			"content": map[string]interface{}{
				"type": "raw",
				"data": map[string]interface{}{
					"message": msg,
				},
			},
		}
		payloadBytes, _ = json.Marshal(payload)

	case "quickbite_created":
		targetTopic = "order-events"
		messageKey = "order.created"
		payload := map[string]interface{}{
			"eventId":      fmt.Sprintf("qb_evt_%d", now.UnixNano()),
			"orderId":      *orderID,
			"orderCode":    "QB-" + strings.ToUpper((*orderID)[len(*orderID)-4:]),
			"customerId":   *userID,
			"restaurantId": "rest_pho_24",
			"totalAmount":  125000,
			"currency":     "VND",
			"occurredAt":   now.Format(time.RFC3339),
			"items": []map[string]interface{}{
				{
					"foodItemId": "pho_bo_dac_biet",
					"itemName":   "Pho Bo Dac Biet",
					"quantity":   2,
					"unitPrice":  60000,
				},
			},
		}
		payloadBytes, _ = json.Marshal(payload)

	case "quickbite_delivering":
		targetTopic = "order-events"
		messageKey = "order.delivering"
		// customerId omitted: QuickNotify Redis cache resolves customerId from order.created!
		payload := map[string]interface{}{
			"eventId":      fmt.Sprintf("qb_evt_%d", now.UnixNano()),
			"orderId":      *orderID,
			"restaurantId": "rest_pho_24",
			"occurredAt":   now.Format(time.RFC3339),
		}
		payloadBytes, _ = json.Marshal(payload)

	case "quickbite_completed":
		targetTopic = "order-events"
		messageKey = "order.completed"
		// Completes the order and purges cache
		payload := map[string]interface{}{
			"eventId":    fmt.Sprintf("qb_evt_%d", now.UnixNano()),
			"orderId":    *orderID,
			"occurredAt": now.Format(time.RFC3339),
		}
		payloadBytes, _ = json.Marshal(payload)

	case "edutrack":
		targetTopic = "edutrack-exam-events"
		messageKey = *userID
		payload := map[string]interface{}{
			"exam_id":       "EXAM_GOLANG_FINAL",
			"student_id":    *userID,
			"student_email": *userID + "@school.edu.vn",
			"subject":       "Distributed Systems & Golang",
			"score":         9.5,
			"max_score":     10.0,
			"graded_at":     now.Format(time.RFC3339),
		}
		payloadBytes, _ = json.Marshal(payload)

	default:
		fmt.Fprintf(os.Stderr, "Unknown type: %s\nAvailable types: core_user, core_project, core_system, quickbite_created, quickbite_delivering, quickbite_completed, edutrack\n", *topicType)
		os.Exit(1)
	}

	// If HTTP endpoint is specified, send directly over HTTP REST API (standalone local demo)
	if *httpURL != "" {
		fmt.Printf("Publishing directly over HTTP to: %s\n", *httpURL)
		fmt.Printf("Payload: %s\n", string(payloadBytes))

		req, err := http.NewRequest(http.MethodPost, *httpURL, bytes.NewReader(payloadBytes))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating HTTP request: %v\n", err)
			os.Exit(1)
		}
		req.Header.Set("Content-Type", "application/json")
		if *apiKey != "" {
			req.Header.Set("X-API-Key", *apiKey)
			fmt.Printf("Authenticated with API Key: %s\n", *apiKey)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error sending HTTP POST request: %v\n", err)
			os.Exit(1)
		}
		defer func() { _ = resp.Body.Close() }()

		respBody, _ := io.ReadAll(resp.Body)
		if resp.StatusCode >= 400 {
			fmt.Fprintf(os.Stderr, "Server returned error (HTTP %d): %s\n", resp.StatusCode, string(respBody))
			os.Exit(1)
		}

		fmt.Printf("Notification successfully dispatched via HTTP (Status %d): %s\n", resp.StatusCode, string(respBody))
		return
	}

	fmt.Printf("Publishing to Kafka broker %s...\n", *broker)
	fmt.Printf("Topic:   %s\n", targetTopic)
	fmt.Printf("Key:     %s\n", messageKey)
	fmt.Printf("Payload: %s\n", string(payloadBytes))

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(*broker),
		Topic:                  targetTopic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
		WriteTimeout:           5 * time.Second,
	}
	defer func() { _ = writer.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(messageKey),
		Value: payloadBytes,
		Time:  now,
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing message to Kafka: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Message published successfully to Kafka!")
}

