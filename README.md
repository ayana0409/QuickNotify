# 🚀 QuickNotify

> **Hệ thống thông báo thời gian thực tập trung (Centralized Real-Time Notification Service) đa nền tảng, hỗ trợ đa dự án (Multi-tenant), phân luồng đa cấp độ và tương thích đa cụm Apache Kafka.**

[![Go Version](https://img.shields.io/badge/Go-1.24+-00ADD8?style=flat&logo=go)](https://golang.org)
[![Architecture](https://img.shields.io/badge/Architecture-Hexagonal%20(Ports%20%26%20Adapters)-blueviolet)](#-kiến-trúc-hệ-thống)
[![Docker](https://img.shields.io/badge/Deployment-Docker%20%7C%20Compose-2496ED?logo=docker)](docker-compose.yml)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 📌 MỤC LỤC
- [1. Giới thiệu tổng quan](#1-giới-thiệu-tổng-quan)
- [2. Tính năng nổi bật](#2-tính-năng-nổi-bật)
- [3. Kiến trúc hệ thống](#3-kiến-trúc-hệ-thống)
- [4. Cấu trúc thư mục](#4-cấu-trúc-thư-mục)
- [5. Khởi động nhanh (Quick Start)](#5-khởi-động-nhanh-quick-start)
  - [Cách 1: Triển khai với Docker Compose (Khuyên dùng)](#cách-1-triển-khai-với-docker-compose-khuyên-dùng)
  - [Cách 2: Chạy trực tiếp từ mã nguồn Go](#cách-2-chạy-trực-tiếp-từ-mã-nguồn-go)
- [6. Hướng dẫn cấu hình (.env)](#6-hướng-dẫn-cấu-hình-env)
- [7. Cơ chế tiếp nhận sự kiện (Inbound Engine)](#7-cơ-chế-tiếp-nhận-sự-kiện-inbound-engine)
  - [7.1. Chuẩn Envelope tiếp nhận vào Kafka Core](#71-chuẩn-envelope-tiếp-nhận-vào-kafka-core)
  - [7.2. Tích hợp đa cụm Kafka (Multi-Cluster Mapping)](#72-tích-hợp-đa-cụm-kafka-multi-cluster-mapping)
  - [7.3. Gửi thông báo trực tiếp qua REST API](#73-gửi-thông-báo-trực-tiếp-qua-rest-api)
- [8. Kết nối WebSocket phía Client (Frontend)](#8-kết-nối-websocket-phía-client-frontend)
- [9. Bảng điều khiển quản trị (Admin Dashboard)](#9-bảng-điều-khiển-quản-trị-admin-dashboard)
- [10. Danh mục API Endpoints](#10-danh-mục-api-endpoints)
- [11. Kiểm thử và Công cụ hỗ trợ](#11-kiểm-thử-và-công-cụ-hỗ-trợ)
- [12. Tài liệu kỹ thuật liên quan](#12-tài-liệu-kỹ-thuật-liên-quan)

---

## 1. GIỚI THIỆU TỔNG QUAN

Trong các hệ thống phân tán và microservices, việc mỗi dự án tự cài đặt giải pháp WebSocket/Email riêng lẻ dẫn đến phân mảnh tài nguyên, khó kiểm soát kết nối và tốn công bảo trì.

**QuickNotify** đóng vai trò là một **Hub trung tâm**:
* Tiếp nhận sự kiện từ nhiều dự án khác nhau qua Kafka hoặc HTTP REST API.
* Quản lý hàng vạn kết nối WebSocket thời gian thực đồng thời.
* Phân phối thông báo chính xác đến người dùng theo dự án (`project_id`), tài khoản (`user_id`), toàn dự án hoặc toàn hệ thống.
* Tích hợp cơ chế Translator để tương thích ngược với các định dạng topic Kafka cũ mà không yêu cầu sửa đổi hệ thống nguồn.

---

## 2. TÍNH NĂNG NỔI BẬT

* 🌐 **Đa dự án (Multi-Tenant) & Đa phiên (Multi-Session):**
  * Tách biệt hoàn toàn luồng dữ liệu giữa các dự án (`shorterlink`, `quickbite`, `edutrack`,...).
  * Một người dùng có thể mở đồng thời nhiều tab trình duyệt, điện thoại, máy tính — tất cả đều nhận thông báo đồng bộ.
* ⚡ **Non-Blocking Send & Chống nghẽn (Backpressure Protection):**
  * Sử dụng Goroutines và Channels với cấu trúc `select - default` an toàn. Client bị lag mạng không làm treo luồng phát tin của hệ thống.
* 🎯 **Định tuyến thông báo 3 cấp độ (Multi-Scope Routing):**
  1. `user`: Gửi riêng đích danh cho 1 tài khoản người dùng.
  2. `project_global`: Phát sóng (broadcast) cho toàn bộ người dùng đang online thuộc một dự án cụ thể.
  3. `system_global`: Phát sóng khẩn cấp toàn hệ thống (bảo trì, nâng cấp server).
* 🔄 **Kafka Engine 2 Tầng (Dual-Tier Kafka):**
  * **Tier 1 (Core Standard):** Lắng nghe topic chuẩn `global-notify-events` nhận trực tiếp NotificationEnvelope (dùng cho ShorterLink và service chuẩn).
  * **Tier 2 (Multi-Cluster Mapping):** Cấu hình linh hoạt qua biến môi trường để lắng nghe nhiều cụm Kafka độc lập với topic và translator riêng.
* 🛡️ **Bảo mật Anti-IDOR, API Key & Scope Guard:**
  * Bóc tách danh tính người dùng từ JWT Token ký bằng thuật toán HMAC-SHA256 (`HS256`) theo từng dự án độc lập.
  * API Key riêng biệt cho từng tenant (ShorterLink, QuickBite) kèm cơ chế Anti-Spoofing và Scope Guard ngăn chặn vượt quyền.
* ⏰ **Lịch Trình Tự Động (Scheduled Workers - Multi-Action):**
  * Thiết lập kịch bản gửi thông báo tự động theo 3 kiểu: **Cron chuẩn 5 trường**, **Interval lặp lại**, hoặc **Hẹn giờ gửi 1 lần**.
  * Trong mỗi lịch hỗ trợ chuỗi **nhiều hành động (Multi-Action)** chạy tuần tự (có thời gian chờ `delay_before_sec`) hoặc đồng loạt.
  * Lưu trữ mỗi lịch là 1 file JSON riêng biệt tự băm tên không trùng (`data/schedules/sched_<hash>.json`), tự động khởi tạo khi chưa có.
* 📊 **Admin Dashboard tích hợp sẵn:**
  * Truy cập trực tiếp tại `/` hoặc `/admin` mà không cần cài đặt thêm frontend riêng.
  * Theo dõi số lượng kết nối online, danh sách tenant đang active, phát thông báo thủ công và xem luồng live feed realtime.
* 📦 **100% Cấu hình qua `.env` & Docker-Ready:**
  * Loại bỏ hoàn toàn các file cấu hình YAML phức tạp. Toàn bộ thông số quản lý tập trung qua file `.env`.

---

## 3. KIẾN TRÚC HỆ THỐNG

Dự án áp dụng mô hình **Hexagonal Architecture (Ports and Adapters)** phân tách nghiêm ngặt giữa Domain Logic lõi và các tầng giao tiếp bên ngoài:

```
                            ┌────────────────────────────────────────┐
                            │            INBOUND ADAPTERS            │
                            │  • Kafka Consumer Manager (Sarama/Go)  │
                            │  • HTTP REST Handler (/api/v1/notify)  │
                            └───────────────────┬────────────────────┘
                                                │
                                                ▼ (Inbound Ports)
                     ┌─────────────────────────────────────────────────────┐
                     │                 CORE APPLICATION                    │
                     │  • Domain Models (Envelope, Target, Scope)          │
                     │  • Dispatcher Service (Phân luồng Scope/Channels)   │
                     │  • Order Cache Service (Redis / In-memory)          │
                     └──────────────────────────┬──────────────────────────┘
                                                │
                                                ▼ (Outbound Ports)
                            ┌────────────────────────────────────────┐
                            │           OUTBOUND ADAPTERS            │
                            │  • WebSocket Hub Manager (Multi-tenant)│
                            │  • SMTP Email Sender                   │
                            │  • Redis Cache Adapter                 │
                            └────────────────────────────────────────┘
```

---

## 4. CẤU TRÚC THƯ MỤC

```text
QuickNotify/
├── cmd/
│   └── quicknotify/
│       └── main.go                 # Điểm khởi chạy ứng dụng (CLI entrypoint)
├── internal/
│   ├── core/                       # Lõi nghiệp vụ (Domain & Ports)
│   │   ├── domain/                 # Structs (NotificationEnvelope, Scope, Message)
│   │   ├── ports/                  # Interfaces giao tiếp (WSPort, EmailPort, Cache)
│   │   └── services/               # Dispatcher phân luồng thông báo
│   ├── adapters/                   # Các Adapter giao tiếp hạ tầng
│   │   ├── inbound/kafka/          # Kafka Consumer Manager & Topic Translators
│   │   │   ├── translators/        # Direct, QuickBite Order, EduTrack Exam
│   │   │   ├── consumer.go         # Worker đọc Kafka phân vùng độc lập
│   │   │   └── registry.go         # Quản lý danh mục Translator động
│   │   └── outbound/
│   │       ├── websocket/          # Hub quản lý kết nối, Client Pump, CORS, JWT Auth
│   │       ├── cache/              # Redis & In-memory Order Cache
│   │       └── email/              # SMTP Email Sender
│   ├── app/                        # Wire dependencies & HTTP Web Server
│   └── pkg/config/                 # Trình nạp cấu hình thuần .env & Docker Env
├── web/
│   └── admin.html                  # Giao diện Bảng điều khiển quản trị (Admin Dashboard)
├── docs/                           # Tài liệu kỹ thuật chi tiết
│   ├── kafka_multi_cluster_mapping_guide.md  # Hướng dẫn đa cụm Kafka
│   ├── websocket_connection_guide.md         # Hướng dẫn tích hợp Frontend ShorterLink
│   └── e2e_testing_guide.md                  # Hướng dẫn kiểm thử toàn trình
├── scripts/                        # Công cụ hỗ trợ phát triển & kiểm thử
│   ├── gen_token/                  # Tool tạo token JWT thử nghiệm
│   └── test_producer/              # Tool bắn message thử nghiệm vào Kafka / HTTP
├── Dockerfile                      # Multi-stage Docker build tối ưu
├── docker-compose.yml              # Cấu hình triển khai container
├── .env.example                    # File mẫu biến môi trường
└── README.md
```

---

## 5. KHỞI ĐỘNG NHANH (QUICK START)

### Cách 1: Triển khai với Docker Compose (Khuyên dùng)

1. **Chuẩn bị file môi trường:**
   ```bash
   cp .env.example .env
   ```
   *Chỉnh sửa các giá trị bí mật (`AUTH_JWT_SECRET`, `ADMIN_PASSWORD`) trong file `.env`.*

2. **Khởi động dịch vụ:**
   ```bash
   docker compose up -d --build
   ```

3. **Kiểm tra trạng thái hoạt động:**
   * Health check endpoint: `http://localhost:8080/healthz`
   * Admin Dashboard: `http://localhost:8080/admin` (Đăng nhập bằng tài khoản trong `.env`, mặc định: `admin` / `admin123`).

4. **Theo dõi logs:**
   ```bash
   docker compose logs -f quicknotify
   ```

---

### Cách 2: Chạy trực tiếp từ mã nguồn Go

**Yêu cầu môi trường:** Go 1.24 trở lên.

1. **Cài đặt thư viện dependencies:**
   ```bash
   go mod download
   ```

2. **Tạo file cấu hình:**
   ```bash
   cp .env.example .env
   ```

3. **Chạy ứng dụng:**
   ```bash
   go run ./cmd/quicknotify
   ```
   *(Mặc định app tự tìm file `.env` tại thư mục hiện tại. Nếu muốn chỉ định file khác: `go run ./cmd/quicknotify -env /path/to/.env`).*

---

## 6. HƯỚNG DẪN CẤU HÌNH (.ENV)

Toàn bộ hệ thống được kiểm soát bởi các biến môi trường sau:

```env
# ── HTTP Server ────────────────────────────────────────────────────────────────
SERVER_HTTP_PORT=8080
SERVER_READ_TIMEOUT_SEC=15
SERVER_WRITE_TIMEOUT_SEC=15
# Whitelist domain cho phép kết nối WebSocket/CORS (Hỗ trợ wildcard *)
SERVER_ALLOWED_ORIGINS=http://localhost:3000,http://localhost:5173,https://*.yourdomain.com
# Tiền tố đường dẫn nếu deploy sau reverse proxy (ví dụ: /quick-notify)
SERVER_BASE_PATH=

# ── Xác thực Client (WebSocket JWT Fallback) ──────────────────────────────────
# Secret key dự phòng khi project chưa được khai báo jwt_secret riêng
AUTH_JWT_SECRET=your-secret-key-must-match-client-backend

# ── Cấu hình API Key, JWT Secret & Scope Guard theo từng Tenant (JSON Array) ───
# Quản lý phân quyền chặt chẽ: Đúng API Key -> Đúng Project ID -> Đúng Scope cho phép
PROJECT_CREDENTIALS='[
  {
    "project_id": "shorterlink",
    "name": "ShorterLink Platform",
    "api_key": "abc",
    "jwt_secret": "abc",
    "allowed_scopes": ["user", "project_global"],
    "enabled": true
  },
  {
    "project_id": "quickbite",
    "name": "QuickBite Delivery",
    "api_key": "abc",
    "jwt_secret": "abc",
    "allowed_scopes": ["user", "project_global"],
    "enabled": true
  },
]'

# ── Admin Dashboard ────────────────────────────────────────────────────────────
ADMIN_ENABLED=true
ADMIN_USERNAME=admin
ADMIN_PASSWORD=your_secure_password
ADMIN_JWT_SECRET=your_admin_secret_key

# ── Kafka Core (Cluster chính của QuickNotify) ─────────────────────────────────
KAFKA_BROKERS=100.76.224.64:9092
KAFKA_CONSUMER_GROUP=quicknotify-core-group
# Topic tiếp nhận chuẩn NotificationEnvelope (dành cho ShorterLink)
KAFKA_STANDARD_TOPIC=global-notify-events

# ── Kafka Multi-Cluster Mapping (Tùy chọn: Dành cho các dự án ngoài) ───────────
# KAFKA_CLUSTERS='[{"id":"cluster_qb","brokers":["kafka.quickbite.local:9092"]}]'
# KAFKA_CONSUMERS='[{"topic":"order-events","cluster_id":"cluster_qb","translator":"quickbite_order"}]'

# ── Redis Caching ──────────────────────────────────────────────────────────────
REDIS_ENABLED=false
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=
REDIS_DB=0

# ── Email SMTP ─────────────────────────────────────────────────────────────────
EMAIL_ENABLED=false
EMAIL_HOST=smtp.example.com
EMAIL_PORT=587
EMAIL_USERNAME=user
EMAIL_PASSWORD=pass
EMAIL_FROM=no-reply@yourdomain.com

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_LEVEL=debug
LOG_DEVELOPMENT=true
```

---

## 7. CƠ CHẾ TIẾP NHẬN SỰ KIỆN (INBOUND ENGINE)

### 7.1. Chuẩn Envelope tiếp nhận vào Kafka Core

Khi microservice của bạn (như **ShorterLink**) phát sinh thông báo, hãy xuất bản (publish) một bản tin JSON vào topic `KAFKA_STANDARD_TOPIC` (`global-notify-events`):

#### ✉️ Gửi cho 1 người dùng cụ thể (`scope: "user"`)
```json
{
  "event_id": "evt_shorterlink_001",
  "project_id": "shorterlink",
  "scope": "user",
  "priority": "normal",
  "channels": ["websocket"],
  "target": {
    "user_id": "usr_9988aabb"
  },
  "content": {
    "type": "raw",
    "data": {
      "message": "Link ngắn 'my-campaign' của bạn vừa đạt mốc 1,000 clicks!"
    }
  }
}
```

#### 📢 Phát sóng toàn dự án (`scope: "project_global"`)
```json
{
  "event_id": "evt_shorterlink_002",
  "project_id": "shorterlink",
  "scope": "project_global",
  "channels": ["websocket"],
  "target": {},
  "content": {
    "type": "raw",
    "data": {
      "message": "ShorterLink vừa cập nhật tính năng tạo QR code động mới!"
    }
  }
}
```

#### 🚨 Phát sóng toàn hệ thống (`scope: "system_global"`)
```json
{
  "event_id": "evt_sys_001",
  "project_id": "system",
  "scope": "system_global",
  "channels": ["websocket"],
  "target": {},
  "content": {
    "type": "raw",
    "data": {
      "message": "Toàn bộ hệ thống sẽ tạm dừng để bảo trì cơ sở dữ liệu lúc 00:00."
    }
  }
}
```

---

### 7.2. Tích hợp đa cụm Kafka (Multi-Cluster Mapping)

Khi cần lắng nghe các cụm Kafka độc lập có schema tin nhắn riêng (ví dụ: `QuickBite`, `EduTrack`), bạn khai báo trực tiếp mảng JSON vào `.env`:

```env
# Định nghĩa các cụm Kafka độc lập
KAFKA_CLUSTERS='[
  {"id":"cluster_qb","brokers":["kafka.quickbite.local:9092"]},
  {"id":"cluster_edu","brokers":["10.0.2.15:9092","10.0.2.16:9092"]}
]'

# Mapping Topic của cụm sang Translator tương ứng
KAFKA_CONSUMERS='[
  {"topic":"order-events","cluster_id":"cluster_qb","translator":"quickbite_order"},
  {"topic":"edutrack-exam-results","cluster_id":"cluster_edu","translator":"edutrack_exam"}
]'
```

*Xem tài liệu chi tiết tại [`docs/kafka_multi_cluster_mapping_guide.md`](docs/kafka_multi_cluster_mapping_guide.md).*

---

### 7.3. Gửi thông báo trực tiếp qua REST API (Bảo vệ bằng API Key & Scope Guard)

Khi kiểm thử nội bộ hoặc khi microservice không dùng Kafka, bạn có thể gọi HTTP POST trực tiếp đến `/api/v1/notify`.
Endpoint này được bảo vệ bởi cơ chế **3-Tier Security Guard**:
1. **Xác thực API Key**: Truyền qua Header `X-API-Key` hoặc `Authorization: Bearer <API_KEY>`.
2. **Anti-Spoofing Guard**: API Key của dự án nào chỉ được quyền phát tin cho dự án đó (`project_id`).
3. **Scope Guard**: Chỉ được phép gửi các scope nằm trong danh sách `allowed_scopes` được cấu hình trong `PROJECT_CREDENTIALS` (chặn gửi `system_global` nếu không có quyền `*`).

```bash
curl -X POST http://localhost:8080/api/v1/notify \
  -H "Content-Type: application/json" \
  -H "X-API-Key: abc" \
  -d '{
    "event_id": "evt_curl_101",
    "project_id": "shorterlink",
    "scope": "user",
    "channels": ["websocket"],
    "target": { "user_id": "usr_9988aabb" },
    "content": { "type": "raw", "data": { "message": "Thông báo qua REST API!" } }
  }'
```

---

## 8. KẾT NỐI WEBSOCKET PHÍA CLIENT (FRONTEND)

### Yêu cầu về JWT Token (Per-Project Secret Isolation)
Mọi kết nối WebSocket đến `/ws` bắt buộc phải kèm một JWT Token hợp lệ ký bằng thuật toán `HS256`.
* **Bảo mật cô lập từng dự án (Multi-Tenant Isolation)**: Mỗi dự án được quản lý Secret Key riêng biệt (`jwt_secret` trong `PROJECT_CREDENTIALS`). ShorterLink không thể giả mạo token của QuickBite.
* **Cơ chế Fallback**: Nếu dự án chưa khai báo secret riêng, QuickNotify tự động dùng `AUTH_JWT_SECRET` làm khóa giải mã dự phòng. 

**Claims của Token:**
```json
{
  "project_id": "shorterlink",
  "user_id": "usr_9988aabb",
  "exp": 1793869650
}
```

### Mã nguồn kết nối mẫu (JavaScript / TypeScript)
```javascript
// 1. Lấy token xác thực từ backend dự án của bạn
const token = await getAuthTokenFromBackend();

// 2. Tự động nhận diện giao thức ws: (HTTP local) hoặc wss: (HTTPS production)
const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
const host = window.location.host; 
const wsUrl = `${protocol}//${host}/ws?token=${encodeURIComponent(token)}`;

// 3. Khởi tạo kết nối
const socket = new WebSocket(wsUrl);

socket.onopen = () => {
  console.log("Connected to QuickNotify WebSocket server!");
};

socket.onmessage = (event) => {
  const envelope = JSON.parse(event.data);
  console.log("Received notification:", envelope);
  
  // Hiển thị toast hoặc cập nhật UI
  showToastNotification(envelope.data.message);
};

socket.onerror = (error) => {
  console.error("WebSocket error:", error);
};

socket.onclose = () => {
  console.warn("WebSocket disconnected. Reconnecting in 3s...");
  setTimeout(connectWebSocket, 3000);
};
```

*Xem hướng dẫn tích hợp chi tiết tại [`docs/websocket_connection_guide.md`](docs/websocket_connection_guide.md).*

---

## 9. BẢNG ĐIỀU KHIỂN QUẢN TRỊ (ADMIN DASHBOARD)

QuickNotify tích hợp sẵn giao diện quản trị hiện đại (Glassmorphism UI) được phục vụ trực tiếp tại:
📍 **`http://localhost:8080/admin`** (hoặc `http://localhost:8080/`)

### Các tính năng chính của Admin Dashboard:
1. **Xác thực an toàn:** Đăng nhập bảo vệ bằng mật khẩu và sinh phiên làm việc JWT Admin riêng biệt.
2. **Theo dõi thời gian thực (Real-time Metrics):**
   * Số lượng kết nối WebSocket đang online đồng thời.
   * Danh sách các Tenant (Project) đang hoạt động và số lượng tab phiên làm việc của từng tenant.
3. **Bộ phát thông báo thủ công (Manual Dispatcher):**
   * Gửi thông báo trực tiếp đến bất kỳ `user`, `project_global`, hoặc `system_global`.
   * Hỗ trợ chèn Extra Data JSON tùy biến.
4. **Live Event Feed:** Xem trực tiếp các thông báo vừa được phát ra dưới dạng danh sách tương tác và Popup Toast thông báo tức thì.
5. **Hỗ trợ Subpath linh hoạt:** Tự động nhận diện đường dẫn khi deploy sau Reverse Proxy (như `https://yourdomain.com/quick-notify/`).

---

## 10. DANH MỤC API ENDPOINTS

| Phương thức | Đường dẫn (Endpoint) | Quyền truy cập | Mô tả chức năng |
|---|---|---|---|
| `GET` | `/healthz` | Public | Liveness probe kiểm tra sức khỏe hệ thống (cho Docker/K8s) |
| `GET` | `/` hoặc `/admin` | Public / Session | Giao diện web Bảng điều khiển quản trị Admin Dashboard |
| `POST` | `/api/v1/notify` | X-API-Key / Admin Bearer | Tiếp nhận sự kiện thông báo qua HTTP (Bảo vệ bởi Anti-Spoofing & Scope Guard) |
| `POST` | `/api/v1/admin/login` | Public | Đăng nhập tài khoản quản trị Admin và nhận Bearer Token |
| `GET` | `/api/v1/stats` | Admin Bearer | Thống kê số lượng kết nối, danh sách tenant và cluster status |
| `GET` | `/api/v1/schedules` | X-API-Key / Admin | Lấy danh sách lịch trình tự động (Lọc theo tenant hoặc xem toàn bộ) |
| `POST` | `/api/v1/schedules` | X-API-Key / Admin | Thiết lập lịch trình mới kèm danh sách đa hành động (Multi-Action) |
| `GET` | `/api/v1/schedules/{id}` | X-API-Key / Admin | Xem thông tin chi tiết của một lịch trình |
| `PUT` | `/api/v1/schedules/{id}` | X-API-Key / Admin | Cập nhật cấu hình lịch trình hoặc danh sách hành động |
| `POST` | `/api/v1/schedules/{id}/trigger` | X-API-Key / Admin | **⚡ Chạy thử ngay**: Kích hoạt phát toàn bộ hành động trong lịch tức thì |
| `POST` | `/api/v1/schedules/{id}/toggle` | X-API-Key / Admin | Bật lại hoặc Tạm dừng (`Active` / `Paused`) một lịch trình |
| `DELETE` | `/api/v1/schedules/{id}` | X-API-Key / Admin | Xóa bỏ file JSON lưu trữ của lịch trình |
| `GET` | `/ws` | Client JWT | Nâng cấp kết nối HTTP Handshake thành WebSocket duplex |

---

## 11. KIỂM THỬ VÀ CÔNG CỤ HỖ TRỢ

### Chạy toàn bộ Unit Tests
```bash
go test -v -count=1 ./...
```

### Công cụ sinh Token thử nghiệm (`scripts/gen_token`)
Sinh nhanh chuỗi JWT Token hợp lệ để test kết nối WebSocket:
```bash
go run ./scripts/gen_token/main.go -project shorterlink -user user_101
```

### Công cụ phát sự kiện thử nghiệm (`scripts/test_producer`)
Bắn thử một thông báo qua HTTP endpoint (có kèm API Key để xác thực):
```bash
go run ./scripts/test_producer/main.go \
  -http http://localhost:8080/api/v1/notify \
  -api-key abc \
  -type core_user \
  -project shorterlink \
  -user user_101 \
  -message "Xin chào từ kịch bản kiểm thử QuickNotify!"
```

*Xem hướng dẫn kiểm thử toàn trình tại [`docs/e2e_testing_guide.md`](docs/e2e_testing_guide.md).*

---

## 12. TÀI LIỆU KỸ THUẬT LIÊN QUAN

* 📖 [Hướng dẫn tích hợp WebSocket cho Frontend ShorterLink](docs/websocket_connection_guide.md)
* 📖 [Hướng dẫn cấu hình & Mapping đa cụm Kafka qua .env](docs/kafka_multi_cluster_mapping_guide.md)
* 📖 [Hướng dẫn kiểm thử toàn trình E2E từ Kafka đến WebSocket](docs/e2e_testing_guide.md)

