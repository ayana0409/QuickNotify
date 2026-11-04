# HƯỚNG DẪN KIỂM THỬ TOÀN DIỆN (END-TO-END TESTING GUIDE)

Tài liệu này hướng dẫn chi tiết các bước kiểm thử hệ thống QuickNotify từ đầu đến cuối (E2E), bao gồm:
1. Xác thực kết nối WebSocket an toàn qua JWT.
2. Kiểm tra Multi-session (một user mở đồng thời nhiều client/tab).
3. Kiểm tra cơ chế Scope Filtering (`user`, `project_global`, `system_global`).
4. Kiểm tra khả năng Non-blocking send khi có client chậm (slow consumer).
5. Kiểm thử định tuyến thông qua Traefik Ingress Reverse Proxy.

---

## 1. Chuẩn Bị Môi Trường

### Cách 1: Khởi chạy cụm Docker Compose (Khuyến nghị)
Hệ thống QuickNotify đã được đóng gói kèm Dockerfile (multi-stage) và `docker-compose.yml` (giới hạn tài nguyên **1 CPU** và **256MB RAM**):

```bash
# Khởi chạy toàn bộ stack: QuickNotify, Traefik, Kafka KRaft, Redis
docker compose up -d
```

Kiểm tra trạng thái container và giới hạn tài nguyên:
```bash
docker stats quicknotify-service
```
Kết quả hiển thị `CPUs: 1.0` và `MEM USAGE / LIMIT: ... / 256MiB`.

### Cách 2: Chạy trực tiếp qua Go (Local Dev)
```bash
go run ./cmd/quicknotify -config configs/config.yaml
```

---

## 2. Các Bước Kiểm Thử End-to-End

### Bước 2.1: Sinh Mock JWT Token
Sử dụng công cụ `scripts/gen_token/main.go` để sinh JWT Token cho các đối tượng kiểm thử:

```bash
# Token cho user_101 thuộc project quickbite
go run ./scripts/gen_token/main.go -project quickbite -user user_101

# Token cho user_202 thuộc project quickbite
go run ./scripts/gen_token/main.go -project quickbite -user user_202

# Token cho student_55 thuộc project edutrack
go run ./scripts/gen_token/main.go -project edutrack -user student_55
```

---

### Bước 2.2: Kết nối Client Kiểm thử

Mở file [`test_client.html`](../test_client.html) trên trình duyệt (có thể mở 2-3 tab hoặc cửa sổ ẩn danh song song):

* **Tab A (Client 1):** Kết nối với danh tính `quickbite` &bull; `user_101`.
* **Tab B (Client 2 - Multi-session):** Kết nối cùng danh tính `quickbite` &bull; `user_101`.
* **Tab C (Client 3):** Kết nối với danh tính `quickbite` &bull; `user_202`.
* **Tab D (Client 4 - Khác Project):** Kết nối với danh tính `edutrack` &bull; `student_55`.

---

### Bước 2.3: Kiểm thử Kịch bản 1 - Scope `user` & Multi-session

Sử dụng tool `scripts/test_producer/main.go` để publish message cho `user_101`:

```bash
go run ./scripts/test_producer/main.go -type core_user -project quickbite -user user_101 -message "Đơn hàng của bạn đang được giao!"
```

**Kết quả kỳ vọng:**
* **Tab A và Tab B:** Cùng nhận được thông báo đồng thời (xác thực tính năng **Multi-session**).
* **Tab C và Tab D:** Hoàn toàn **KHÔNG** nhận được tin nhắn (xác thực tính năng **Tenant Isolation & User Scope**).

---

### Bước 2.4: Kiểm thử Kịch bản 2 - Scope `project_global`

Bắn một thông báo cấp dự án tới toàn bộ người dùng `quickbite`:

```bash
go run ./scripts/test_producer/main.go -type core_project -project quickbite -message "QuickBite tặng bạn mã giảm giá 50%!"
```

**Kết quả kỳ vọng:**
* **Tab A, Tab B, và Tab C:** Đều nhận được thông báo do cùng thuộc dự án `quickbite`.
* **Tab D (`edutrack`):** **KHÔNG** nhận được thông báo.

---

### Bước 2.5: Kiểm thử Kịch bản 3 - Scope `system_global`

Bắn thông báo toàn hệ thống:

```bash
go run ./scripts/test_producer/main.go -type core_system -message "Toàn bộ hệ thống sẽ bảo trì trong 10 phút!"
```

**Kết quả kỳ vọng:**
* **Toàn bộ các Tab (A, B, C, D):** Đều nhận được thông báo đỏ (System Scope) với popup toast nổi bật.

---

### Bước 2.6: Kiểm thử Kịch bản 4 - Custom Translator (QuickBite Order Events & Redis Cache)

Bắn chuỗi sự kiện đơn hàng QuickBite:

```bash
# 1. Sự kiện tạo đơn hàng (có lưu CustomerID vào Redis Cache)
go run ./scripts/test_producer/main.go -type quickbite_created -order ord_5566 -user user_101

# 2. Sự kiện đang giao (không có CustomerID trong payload, QuickNotify tự tra cứu Redis)
go run ./scripts/test_producer/main.go -type quickbite_delivering -order ord_5566

# 3. Sự kiện hoàn tất (tự động xóa cache khỏi Redis)
go run ./scripts/test_producer/main.go -type quickbite_completed -order ord_5566
```

**Kết quả kỳ vọng:**
* Tab A và Tab B (`user_101`) nhận đầy đủ chuỗi 3 thông báo trạng thái đơn hàng thân thiện ("Đơn hàng QB-5566 đã được tạo", "Đơn hàng QB-5566 đang được giao", "Đơn hàng QB-5566 đã giao thành công").
* Khắc phục hoàn toàn bài toán khuyết `CustomerID` nhờ cơ chế Redis Session Cache.

---

### Bước 2.7: Kiểm thử Kịch bản 5 - Non-blocking Send

1. Mở một kết nối raw WebSocket (ví dụ thông qua lệnh shell hoặc script) nhưng không đọc dữ liệu từ socket để buffer 256 tin nhắn bị đầy (`sendBufferSize = 256`).
2. Dispatch liên tiếp 300 tin nhắn vào hệ thống.
3. **Kết quả kỳ vọng:**
   * Các client khác vẫn nhận tin nhắn bình thường với độ trễ cực thấp (<5ms), không bị block hay giật lag.
   * Client bị treo sẽ tự động bị Hub evict và giải phóng tài nguyên mà không làm chậm server.

