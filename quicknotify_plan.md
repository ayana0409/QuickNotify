# KẾ HOẠCH TRIỂN KHAI DỰ ÁN QUICKNOTIFY (STEP-BY-STEP)

**Dự án:** QuickNotify - Hệ thống thông báo đa kênh, đa dự án (Multi-tenant)
**Kiến trúc:** Hexagonal Architecture
**Ngôn ngữ:** Golang

Sử dụng checklist này để theo dõi tiến độ phát triển hệ thống.

## Giai đoạn 1: Khởi tạo và Xây dựng Lõi (Core & Domain)

**Mục tiêu:** Thiết lập khung dự án Hexagonal và định nghĩa các đối tượng cốt lõi.

* [ ] **Bước 1.1: Khởi tạo dự án Go**
  * [ ] Khởi tạo module (`go mod init quicknotify`).
  * [ ] Tạo cấu trúc thư mục chuẩn Hexagonal (`cmd/`, `internal/core/`, `internal/adapters/`, `configs/`).
  * [ ] Cài đặt các thư viện cơ bản (`viper`, `zap`, `validator`).

* [ ] **Bước 1.2: Định nghĩa Domain Models**
  * [ ] Tạo file `internal/core/domain/notification.go`.
  * [ ] Định nghĩa struct `NotificationEnvelope` (EventID, ProjectID, Priority, Channels, Scope, Target, Content).
  * [ ] Định nghĩa các hằng số Scope (`user`, `project_global`, `system_global`).

* [ ] **Bước 1.3: Định nghĩa Interfaces (Ports)**
  * [ ] Tạo file `internal/core/ports/dispatcher.go`.
  * [ ] Định nghĩa interface `NotificationService` (hàm `ProcessEvent`).
  * [ ] Định nghĩa các Outbound Ports (`WSPort`, `EmailPort`, v.v.).

## Giai đoạn 2: Xây dựng Hệ thống WebSocket & Outbound (Outbound Adapters)

**Mục tiêu:** Quản lý kết nối an toàn, phân luồng theo Project/User, hỗ trợ Multi-session và các kênh liên lạc khác.

* [ ] **Bước 2.1: Thiết lập Hub Manager**
  * [ ] Tạo file `internal/adapters/outbound/websocket/hub.go`.
  * [ ] Cài đặt cấu trúc Map 3 cấp: `map[ProjectID]map[UserID]map[*Client]bool`.
  * [ ] Xây dựng các method quản lý: `Register`, `Unregister`.
  * [ ] Xây dựng method `Broadcast` xử lý logic định tuyến theo Scope (Targeted, Project Global, System Global).

* [ ] **Bước 2.2: Xây dựng Client Goroutines**
  * [ ] Tạo file `internal/adapters/outbound/websocket/client.go` với `gorilla/websocket`.
  * [ ] Viết logic vòng lặp `ReadPump` (lắng nghe pong/disconnect).
  * [ ] Viết logic vòng lặp `WritePump` (đẩy dữ liệu xuống network).
  * [ ] Áp dụng **Non-blocking send** (`select-default`) khi đẩy tin từ Hub vào channel của Client.

* [ ] **Bước 2.3: Viết Middleware Xác thực (Security)**
  * [ ] Tạo file `internal/adapters/outbound/websocket/middleware.go`.
  * [ ] Viết hàm parse và validate JWT Token.
  * [ ] Trích xuất `project_id` và `user_id` từ Token claims và gán vào HTTP Context.

* [ ] **Bước 2.4: Mở Endpoint WebSocket**
  * [ ] Tạo file `handler.go` để hứng route `/ws`.
  * [ ] Upgrade HTTP request, khởi tạo `Client` với Context an toàn, và đăng ký vào Hub.

* [ ] **Bước 2.5: Xây dựng Email Adapter (SMTP)**
  * [ ] Tạo file `internal/adapters/outbound/email/smtp_sender.go`.
  * [ ] Implement interface `EmailPort`.
  * [ ] Viết logic kết nối SMTP (hoặc tích hợp API SendGrid/Mailgun) để gửi email dựa trên `NotificationEnvelope`.

## Giai đoạn 3: Tích hợp Kafka Core, Dispatcher & Quản lý Tiến trình

**Mục tiêu:** Nhận thông điệp chuẩn, định tuyến đến các kênh và đảm bảo hệ thống vận hành bền bỉ.

* [ ] **Bước 3.1: Phát triển Dispatcher Service (Core)**
  * [ ] Tạo file `internal/core/services/dispatcher.go`.
  * [ ] Implement `NotificationService`.
  * [ ] Viết logic duyệt mảng `Channels` và đẩy payload tương ứng sang các Adapter (WebSocket, Email).

* [ ] **Bước 3.2: Xây dựng Kafka Consumer Adapter**
  * [ ] Tạo file `internal/adapters/inbound/kafka/consumer.go` (`sarama` hoặc `kafka-go`).
  * [ ] Cấu hình Worker Pool / Consumer Group lắng nghe topic chuẩn `global-notify-events`.
  * [ ] Viết logic unmarshal JSON từ Kafka Message sang `NotificationEnvelope`.

* [ ] **Bước 3.3: Kết nối Pipeline**
  * [ ] Trong `cmd/quicknotify/main.go`, khởi tạo Hub, Dispatcher, Email Sender và Kafka Consumer.
  * [ ] Inject Dispatcher vào Kafka Consumer.

* [ ] **Bước 3.4: Liveness Probe & Graceful Shutdown (Quan trọng)**
  * [ ] Tạo endpoint HTTP `/healthz` (trả về 200 OK) để phục vụ Docker/K8s liveness/readiness probe.
  * [ ] Bổ sung channel lắng nghe tín hiệu `os.Interrupt`, `syscall.SIGTERM`.
  * [ ] Khi nhận tín hiệu tắt: 
    * [ ] Dừng Kafka consumer để đảm bảo commit offset an toàn.
    * [ ] Duyệt qua toàn bộ Hub, gửi `Close Frame` chuẩn cho toàn bộ WebSocket clients và đóng kết nối TCP.

## Giai đoạn 4: Mở rộng với Custom Translators (Inbound Adapters)

**Mục tiêu:** Linh hoạt consume các topic khác mà không sửa code Core.

* [ ] **Bước 4.1: Định nghĩa Translator Interface**
  * [ ] Tạo interface `MessageTranslator` (hàm biến đổi `[]byte` sang `NotificationEnvelope`).

* [ ] **Bước 4.2: Xây dựng các hàm Translate cụ thể**
  * [ ] Tạo thư mục `translators/` trong adapter kafka.
  * [ ] Viết các translator đặc thù (VD: `QuickBiteOrderTranslator`, `ShorterLinkAlertTranslator`).

* [ ] **Bước 4.3: Viết Registry Map Translator**
  * [ ] Cấu hình load từ `configs/consumers.yaml`.
  * [ ] Ánh xạ Tên Topic với Translator tương ứng.
  * [ ] Điều chỉnh Kafka Consumer để gọi Translator trước khi đẩy vào Dispatcher đối với các topic custom.

## Giai đoạn 5: Triển khai, Kiểm thử & Công cụ Demo

**Mục tiêu:** Đóng gói, đưa lên môi trường và trang bị công cụ để test/demo hiệu quả.

* [ ] **Bước 5.1: Đóng gói Docker**
  * [ ] Viết `Dockerfile` (Multi-stage build).
  * [ ] Cấu hình `docker-compose.yml` với build local và environment variables.

* [ ] **Bước 5.2: Cấu hình Ingress/Reverse Proxy**
  * [ ] Cấu hình Traefik labels trong compose file để map domain và route traffic `/ws`.

* [ ] **Bước 5.3: End-to-End Testing**
  * [ ] Kết nối nhiều clients.
  * [ ] Publish message test vào Kafka và kiểm tra hệ thống Multi-session, Scope Filtering, và Non-blocking send.

* [ ] **Bước 5.4: Xây dựng bộ công cụ Demo & Test nội bộ**
  * [ ] **Tool tạo Mock JWT Token:** Viết 1 script Go nhỏ (VD: `scripts/gen_token/main.go`) để sinh nhanh JWT với `project_id` và `user_id` tùy biến truyền qua CLI (phục vụ test handshake ngay không cần Auth Service).
  * [ ] **Trang Web Test Client (`test_client.html`):** Tạo một file HTML/JS thuần cực nhẹ. Chứa form nhập JWT, nút Connect WS, màn hình in log realtime và hiển thị popup (toast/alert) khi có Notification bắn về (trực quan hơn dùng `wscat`).