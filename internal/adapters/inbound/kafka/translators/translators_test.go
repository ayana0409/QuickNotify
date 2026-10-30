package translators

import (
	"context"
	"strings"
	"testing"

	"quicknotify/internal/adapters/outbound/cache"
	"quicknotify/internal/core/domain"
)

func TestDirectTranslator_Translate(t *testing.T) {
	translator := NewDirectTranslator()

	raw := []byte(`{
		"event_id": "evt_test_01",
		"project_id": "quicktest",
		"scope": "user",
		"channels": ["websocket"],
		"target": {"user_id": "usr_123"},
		"content": {"type": "raw", "data": {"message": "hello direct"}}
	}`)

	env, err := translator.Translate(nil, raw)
	if err != nil {
		t.Fatalf("unexpected error translating direct payload: %v", err)
	}

	if env.EventID != "evt_test_01" {
		t.Errorf("expected EventID 'evt_test_01', got '%s'", env.EventID)
	}
	if env.ProjectID != "quicktest" {
		t.Errorf("expected ProjectID 'quicktest', got '%s'", env.ProjectID)
	}
	if env.Scope != domain.ScopeUser {
		t.Errorf("expected Scope '%s', got '%s'", domain.ScopeUser, env.Scope)
	}
}

func TestEduTrackExamTranslator_Translate(t *testing.T) {
	translator := NewEduTrackExamTranslator()

	raw := []byte(`{
		"exam_id": "exam_midterm_1",
		"student_id": "stu_007",
		"student_email": "stu007@school.edu.vn",
		"subject": "Cloud Computing",
		"score": 9.5,
		"max_score": 10.0,
		"graded_at": "2026-10-30T10:30:00Z"
	}`)

	env, err := translator.Translate([]byte("exam.graded"), raw)
	if err != nil {
		t.Fatalf("unexpected error translating edutrack payload: %v", err)
	}

	if env.ProjectID != "edutrack" {
		t.Errorf("expected ProjectID 'edutrack', got '%s'", env.ProjectID)
	}
	if env.Target.UserID != "stu_007" {
		t.Errorf("expected Target UserID 'stu_007', got '%s'", env.Target.UserID)
	}
	if env.Content.TemplateID != "EDUTRACK_EXAM_GRADED" {
		t.Errorf("expected TemplateID 'EDUTRACK_EXAM_GRADED', got '%s'", env.Content.TemplateID)
	}
}

func TestQuickbiteOrderTranslator_AllEventTypes_AndCacheDeletion(t *testing.T) {
	memCache := cache.NewMemoryOrderCache()
	translator := NewQuickbiteOrderTranslator(memCache, nil)
	ctx := context.Background()

	orderID := "3a2484da-14de-c9f6-8b88-719595417493"

	// 1. order.submitted
	submittedJSON := []byte(`{
		"eventId": "3a2484f2-a5fe-83d3-fa4f-a0624651f29d",
		"orderId": "3a2484f2-614a-0242-f86f-2194edab561a",
		"orderCode": "QB-20261025111510-1875",
		"customerId": "3a2475af-0258-04a0-c31f-8d0a7ee5ea73",
		"restaurantId": "edcbebdd-df63-497d-9368-befd997c62bb",
		"totalAmount": 180000.00,
		"currency": "VND",
		"correlationId": "d5bce20b-29a8-4e1f-a74d-bfba7f7300d3",
		"occurredAt": "2026-10-25T11:15:28.3827542Z",
		"items": [
			{
				"foodItemId": "d573c97e-34fd-42ba-919a-fd65510963eb",
				"itemName": "Cơm chiên cá mặn",
				"quantity": 6,
				"unitPrice": 30000.00
			}
		]
	}`)

	env1, err := translator.Translate([]byte("order.submitted"), submittedJSON)
	if err != nil {
		t.Fatalf("unexpected error on order.submitted: %v", err)
	}

	if env1.ProjectID != "quickbite" {
		t.Errorf("expected ProjectID 'quickbite', got '%s'", env1.ProjectID)
	}
	if env1.Target.UserID != "3a2475af-0258-04a0-c31f-8d0a7ee5ea73" {
		t.Errorf("expected customer ID, got: %s", env1.Target.UserID)
	}
	msg1 := env1.Content.Data["message"].(string)
	if !strings.Contains(msg1, "QB-20261025111510-1875") || !strings.Contains(msg1, "đã được chấp nhận") {
		t.Errorf("expected submitted message, got: %s", msg1)
	}

	// 2. order.waiting-payment
	waitingJSON := []byte(`{
		"eventId": "c0af296d-0120-415f-ac1c-65a09dba3423",
		"orderId": "` + orderID + `",
		"orderCode": "QB-20261025104838-8600",
		"customerId": "3a234511-8634-e520-57e8-690a0ca0819a",
		"totalAmount": 200000.00,
		"currency": "VND",
		"correlationId": "b68f2502-0aa9-490b-907d-bd051286a945",
		"occurredAt": "2026-10-25T10:48:40.5399412Z"
	}`)

	env2, err := translator.Translate([]byte("order.waiting-payment"), waitingJSON)
	if err != nil {
		t.Fatalf("unexpected error on order.waiting-payment: %v", err)
	}
	msg2 := env2.Content.Data["message"].(string)
	if !strings.Contains(msg2, "200,000 VND") || !strings.Contains(msg2, "chờ bạn thanh toán") {
		t.Errorf("expected waiting payment message with formatted amount, got: %s", msg2)
	}

	// Verify order was stored in cache
	meta, found, _ := memCache.GetOrder(ctx, orderID)
	if !found || meta.CustomerID != "3a234511-8634-e520-57e8-690a0ca0819a" {
		t.Fatalf("expected order to be stored in cache after waiting-payment event")
	}

	// 3. order.confirmed (omits customerId and orderCode, tests cache retrieval)
	confirmedJSON := []byte(`{
		"eventId": "6b40660b-2623-49b3-9f40-9579d6d54266",
		"orderId": "` + orderID + `",
		"correlationId": "bfc9aee4-2185-41eb-b875-e0d9e7f539bb",
		"occurredAt": "2026-10-25T10:38:11.3740337Z"
	}`)

	env3, err := translator.Translate([]byte("order.confirmed"), confirmedJSON)
	if err != nil {
		t.Fatalf("unexpected error on order.confirmed: %v", err)
	}
	if env3.Target.UserID != "3a234511-8634-e520-57e8-690a0ca0819a" {
		t.Errorf("expected cached customerId '3a234511-8634-e520-57e8-690a0ca0819a', got: %s", env3.Target.UserID)
	}
	msg3 := env3.Content.Data["message"].(string)
	if !strings.Contains(msg3, "QB-20261025104838-8600") || !strings.Contains(msg3, "đã được xác nhận") {
		t.Errorf("expected cached order code in confirmed message, got: %s", msg3)
	}

	// 4. order.preparing
	preparingJSON := []byte(`{
		"eventId": "232f1390-ca9b-4ff4-8c0a-11dc503298c3",
		"orderId": "` + orderID + `",
		"correlationId": "b68f2502-0aa9-490b-907d-bd051286a945",
		"occurredAt": "2026-10-25T10:49:33.2159391Z"
	}`)
	env4, err := translator.Translate([]byte("order.preparing"), preparingJSON)
	if err != nil {
		t.Fatalf("unexpected error on order.preparing: %v", err)
	}
	msg4 := env4.Content.Data["message"].(string)
	if !strings.Contains(msg4, "đang được chuẩn bị") {
		t.Errorf("expected preparing message, got: %s", msg4)
	}

	// 5. order.delivering
	deliveringJSON := []byte(`{
		"eventId": "80e21998-6640-4511-9aaa-1ca2d55474ca",
		"orderId": "` + orderID + `",
		"correlationId": "b68f2502-0aa9-490b-907d-bd051286a945",
		"occurredAt": "2026-10-25T10:49:46.8825026Z"
	}`)
	env5, err := translator.Translate([]byte("order.delivering"), deliveringJSON)
	if err != nil {
		t.Fatalf("unexpected error on order.delivering: %v", err)
	}
	msg5 := env5.Content.Data["message"].(string)
	if !strings.Contains(msg5, "đang trên đường giao") {
		t.Errorf("expected delivering message, got: %s", msg5)
	}

	// 6. order.completed (must resolve correctly and delete cache immediately)
	completedJSON := []byte(`{
		"eventId": "e22ee78c-a55b-4e78-8559-913ada41a38e",
		"orderId": "` + orderID + `",
		"correlationId": "b68f2502-0aa9-490b-907d-bd051286a945",
		"occurredAt": "2026-10-25T10:50:01.9451483Z"
	}`)
	env6, err := translator.Translate([]byte("order.completed"), completedJSON)
	if err != nil {
		t.Fatalf("unexpected error on order.completed: %v", err)
	}
	msg6 := env6.Content.Data["message"].(string)
	if !strings.Contains(msg6, "đã được giao thành công") {
		t.Errorf("expected completed message, got: %s", msg6)
	}

	// Check that cache was deleted immediately!
	_, foundAfterCompleted, _ := memCache.GetOrder(ctx, orderID)
	if foundAfterCompleted {
		t.Fatalf("expected order cache to be deleted immediately upon order.completed")
	}

	// 7. order.reverted-to-draft (tests deletion on cancellation)
	draftOrderID := "3a2484f2-614a-0242-f86f-2194edab561a"
	revertedJSON := []byte(`{
		"eventId": "f41b0147-8dbf-4839-9740-41107070600d",
		"orderId": "` + draftOrderID + `",
		"orderCode": "QB-20261025111510-1875",
		"customerId": "3a2475af-0258-04a0-c31f-8d0a7ee5ea73",
		"restaurantId": "edcbebdd-df63-497d-9368-befd997c62bb",
		"reason": "Insufficient available stock for item d573c97e-34fd-42ba-919a-fd65510963eb. Available: 5, Requested: 6",
		"code": "OUT_OF_STOCK",
		"correlationId": "d5bce20b-29a8-4e1f-a74d-bfba7f7300d3",
		"occurredAt": "2026-10-25T11:15:12.3809796Z"
	}`)
	env7, err := translator.Translate([]byte("order.reverted-to-draft"), revertedJSON)
	if err != nil {
		t.Fatalf("unexpected error on order.reverted-to-draft: %v", err)
	}
	msg7 := env7.Content.Data["message"].(string)
	if !strings.Contains(msg7, "hết hàng") || !strings.Contains(msg7, "Insufficient available stock") {
		t.Errorf("expected reverted message with reason, got: %s", msg7)
	}

	// Check that cache was deleted immediately on reverted-to-draft!
	_, foundAfterReverted, _ := memCache.GetOrder(ctx, draftOrderID)
	if foundAfterReverted {
		t.Fatalf("expected order cache to be deleted immediately upon order.reverted-to-draft")
	}
}
