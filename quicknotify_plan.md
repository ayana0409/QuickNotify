# KẾ HOẠCH TRIỂN KHAI DỰ ÁN QUICKNOTIFY (STEP-BY-STEP)

**Dự án:** QuickNotify - Hệ thống thông báo đa kênh, đa dự án (Multi-tenant)
**Kiến trúc:** Hexagonal Architecture
**Ngôn ngữ:** Golang

Sử dụng checklist này để theo dõi tiến độ phát triển hệ thống.

## Giai đoạn 1: Khởi tạo và Xây dựng Lõi (Core & Domain)

**Mục tiêu:** Thiết lập khung dự án Hexagonal và định nghĩa các đối tượng cốt lõi.

* [x] **Bước 1.1: Khởi tạo dự án Go**
  * [x] Khởi tạo module (`go mod init quicknotify`).
  * [x] Tạo cấu trúc thư mục chuẩn Hexagonal (`cmd/`, `internal/core/`, `internal/adapters/`, `configs/`).
  * [x] Cài đặt các thư viện cơ bản (`viper`, `zap`, `validator`).

* [x] **Bước 1.2: Định nghĩa Domain Models**
  * [x] Tạo file `internal/core/domain/notification.go`.
  * [x] Định nghĩa struct `NotificationEnvelope` (EventID, ProjectID, Priority, Channels, Scope, Target, Content).
  * [x] Định nghĩa các hằng số Scope (`user`, `project_global`, `system_global`).

* [x] **Bước 1.3: Định nghĩa Interfaces (Ports)**
  * [x] Tạo file `internal/core/ports/dispatcher.go`.
  * [x] Định nghĩa interface `NotificationService` (hàm `ProcessEvent`).
  * [x] Định nghĩa các Outbound Ports (`WSPort`, `EmailPort`, v.v.).

## Giai đoạn 2: Xây dựng Hệ thống WebSocket & Outbound (Outbound Adapters)

**Mục tiêu:** Quản lý kết nối an toàn, phân luồng theo Project/User, hỗ trợ Multi-session và các kênh liên lạc khác.

* [x] **Bước 2.1: Thiết lập Hub Manager**
  * [x] Tạo file `internal/adapters/outbound/websocket/hub.go`.
  * [x] Cài đặt cấu trúc Map 3 cấp: `map[ProjectID]map[UserID]map[*Client]bool`.
  * [x] Xây dựng các method quản lý: `Register`, `Unregister`.
  * [x] Xây dựng method `Broadcast` xử lý logic định tuyến theo Scope (Targeted, Project Global, System Global).

* [x] **Bước 2.2: Xây dựng Client Goroutines**
  * [x] Tạo file `internal/adapters/outbound/websocket/client.go` với `gorilla/websocket`.
  * [x] Viết logic vòng lặp `ReadPump` (lắng nghe pong/disconnect).
  * [x] Viết logic vòng lặp `WritePump` (đẩy dữ liệu xuống network).
  * [x] Áp dụng **Non-blocking send** (`select-default`) khi đẩy tin từ Hub vào channel của Client.

* [x] **Bước 2.3: Viết Middleware Xác thực (Security)**
  * [x] Tạo file `internal/adapters/outbound/websocket/middleware.go`.
  * [x] Viết hàm parse và validate JWT Token.
  * [x] Trích xuất `project_id` và `user_id` từ Token claims và gán vào HTTP Context.

* [x] **Bước 2.4: Mở Endpoint WebSocket**
  * [x] Tạo file `handler.go` để hứng route `/ws`.
  * [x] Upgrade HTTP request, khởi tạo `Client` với Context an toàn, và đăng ký vào Hub.

* [x] **Bước 2.5: Xây dựng Email Adapter (SMTP)**
  * [x] Tạo file `internal/adapters/outbound/email/smtp_sender.go`.
  * [x] Implement interface `EmailPort`.
  * [x] Viết logic kết nối SMTP (hoặc tích hợp API SendGrid/Mailgun) để gửi email dựa trên `NotificationEnvelope`.

## Giai đoạn 3: Tích hợp Kafka Core, Dispatcher & Quản lý Tiến trình

**Mục tiêu:** Nhận thông điệp chuẩn, định tuyến đến các kênh và đảm bảo hệ thống vận hành bền bỉ.

* [x] **Bước 3.1: Phát triển Dispatcher Service (Core)**
  * [x] Tạo file `internal/core/services/dispatcher.go`.
  * [x] Implement `NotificationService`.
  * [x] Viết logic duyệt mảng `Channels` và đẩy payload tương ứng sang các Adapter (WebSocket, Email).

* [x] **Bước 3.2: Xây dựng Kafka Consumer Adapter**
  * [x] Tạo file `internal/adapters/inbound/kafka/consumer.go` (`sarama` hoặc `kafka-go`).
  * [x] Cấu hình Worker Pool / Consumer Group lắng nghe topic chuẩn `global-notify-events`.
  * [x] Viết logic unmarshal JSON từ Kafka Message sang `NotificationEnvelope`.

* [x] **Bước 3.3: Kết nối Pipeline**
  * [x] Trong `cmd/quicknotify/main.go`, khởi tạo Hub, Dispatcher, Email Sender và Kafka Consumer.
  * [x] Inject Dispatcher vào Kafka Consumer.

* [x] **Bước 3.4: Liveness Probe & Graceful Shutdown (Quan trọng)**
  * [x] Tạo endpoint HTTP `/healthz` (trả về 200 OK) để phục vụ Docker/K8s liveness/readiness probe.
  * [x] Bổ sung channel lắng nghe tín hiệu `os.Interrupt`, `syscall.SIGTERM`.
  * [x] Khi nhận tín hiệu tắt: 
    * [x] Dừng Kafka consumer để đảm bảo commit offset an toàn.
    * [x] Duyệt qua toàn bộ Hub, gửi `Close Frame` chuẩn cho toàn bộ WebSocket clients và đóng kết nối TCP.

## Giai đoạn 4: Mở rộng với Custom Translators (Inbound Adapters)

**Mục tiêu:** Linh hoạt consume các topic khác mà không sửa code Core.

* [x] **Bước 4.1: Định nghĩa Translator Interface**
  * [x] Tạo interface `MessageTranslator` (hàm biến đổi `[]byte` sang `NotificationEnvelope`).

* [x] **Bước 4.2: Xây dựng các hàm Translate cụ thể**
  * [x] Tạo thư mục `translators/` trong adapter kafka.
  * [x] Viết các translator đặc thù (VD: `QuickBiteOrderTranslator`, `EduTrackExamTranslator`).

* [x] **Bước 4.3: Viết Registry Map Translator**
  * [x] Cấu hình load từ `configs/consumers.yaml`.
  * [x] Ánh xạ Tên Topic với Translator tương ứng.
  * [x] Điều chỉnh Kafka Consumer để gọi Translator trước khi đẩy vào Dispatcher đối với các topic custom.

## Giai đoạn 4.5: Tái cấu trúc `main.go` → `internal/app/`

**Mục tiêu:** Module hóa wiring code để dễ đọc, dễ test, `main.go` chỉ ~20 dòng.

* [x] **Bước 4.5.1: Tạo `internal/app/logger.go`**
  * [x] Di chuyển hàm `initLogger()` từ `main.go` vào đây.

* [x] **Bước 4.5.2: Tạo `internal/app/storage.go`**
  * [x] Viết `initOrderCache()` – khởi tạo Redis với fallback in-memory.

* [x] **Bước 4.5.3: Tạo `internal/app/kafka.go`**
  * [x] Viết `initKafkaConsumers()` – tạo registry, load consumers.yaml, fallback standard topic.
  * [x] Struct `kafkaBundle` gom manager + context + cancel func.

* [x] **Bước 4.5.4: Tạo `internal/app/server.go`**
  * [x] Viết `initHTTPServer()` – mux, /healthz, /ws, CORS wrapper, *http.Server.

* [x] **Bước 4.5.5: Tạo `internal/app/app.go`**
  * [x] Struct `App`, hàm `New()` orchestrates toàn bộ wiring.
  * [x] Hàm `Run()` – start consumers, start HTTP, block on signal.
  * [x] Hàm `shutdown()` – ordered shutdown: Kafka → HTTP → WS → Cache.

* [x] **Bước 4.5.6: Simplify `cmd/quicknotify/main.go`**
  * [x] Gọi `app.New()` rồi `application.Run()` – chỉ ~20 dòng.
  * [x] `go vet ./...` PASS, `go build` PASS, `go test -v ./...` PASS.

## Giai đoạn 5: Triển khai, Kiểm thử & Công cụ Demo

**Mục tiêu:** Đóng gói, đưa lên môi trường và trang bị công cụ để test/demo hiệu quả.

* [x] **Bước 5.1: Đóng gói Docker**
  * [x] Viết `Dockerfile` đa tầng (Multi-stage build) đặt tại thư mục gốc, tối ưu kích thước, bảo mật non-root user và tích hợp healthcheck qua `/healthz`.
  * [x] Hỗ trợ chạy container độc lập với giới hạn tài nguyên nghiêm ngặt **1 CPU và 256MB RAM** qua cờ `--cpus=1.0 --memory=256m`.

* [x] **Bước 5.2: Triển khai Độc lập & Cấu hình Môi trường**
  * [x] Loại bỏ Docker Compose theo yêu cầu, chuyển sang quản lý trực tiếp bằng Docker standalone hoặc orchestrator ngoài.

* [x] **Bước 5.3: End-to-End Testing**
  * [x] Xây dựng công cụ kiểm thử E2E (`scripts/test_producer/main.go`).
  * [x] Viết tài liệu kịch bản kiểm thử End-to-End (`docs/e2e_testing_guide.md`) bao gồm Multi-session, Scope Filtering, Redis Cache session và Non-blocking send.

* [x] **Bước 5.4: Xây dựng bộ công cụ Demo & Test nội bộ**
  * [x] **Tool tạo Mock JWT Token:** Viết script Go `scripts/gen_token/main.go` để sinh nhanh JWT với `project_id` và `user_id` tùy biến truyền qua CLI (phục vụ test handshake ngay không cần Auth Service).
  * [x] **Trang Web Test Client (`test_client.html` & `web/test_client.html`):** Tạo file HTML/JS thuần cực nhẹ, không dependency ngoài. Chứa form nhập JWT, preset danh tính, nút Connect WS, màn hình in log realtime, thống kê uptime/heartbeat, filter theo Scope và hiển thị popup toast thông báo realtime kèm âm thanh synthesizer.