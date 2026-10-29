# TÀI LIỆU ĐẶC TẢ KỸ THUẬT: HỆ THỐNG QUICKNOTIFY

**Phiên bản:** 1.2.0 (Cập nhật Multi-session, Non-blocking Send & Security)
**Ngày cập nhật:** 29/10/2026
**Mô hình Kiến trúc:** Hexagonal Architecture (Ports and Adapters)
**Ngôn ngữ lập trình chính:** Golang (Khuyến nghị bản 1.22+)

## 1. TỔNG QUAN DỰ ÁN

**QuickNotify** là một dịch vụ thông báo tập trung (Centralized Notification Service) đa luồng, hỗ trợ đa dự án (Multi-tenant). Nhiệm vụ cốt lõi của hệ thống là tiếp nhận các sự kiện thông báo từ nhiều nguồn Kafka khác nhau và phân phối (route) chúng đến người dùng cuối thông qua đa kênh (Omnichannel: WebSocket, Email, v.v.).

Hệ thống được thiết kế với các chức năng chính:

1. **Core Standard:** Xử lý một chuẩn Payload chung qua topic `global-notify-events`.

2. **Custom Integration:** Lắng nghe và tự động chuyển đổi (translate) dữ liệu từ các topic Kafka cũ của các dự án hiện hữu về chuẩn Core.

3. **Multi-level Broadcasting:** Hỗ trợ định tuyến thông báo theo 3 cấp độ: Cá nhân (User), Toàn dự án (Project Global), và Toàn hệ thống (System Global).

## 2. CÔNG NGHỆ VÀ THƯ VIỆN SỬ DỤNG

### 2.1. Nền tảng hạ tầng

* **Ngôn ngữ:** Golang (Goroutines và Channels).

* **Message Broker:** Apache Kafka (Hỗ trợ Multi-cluster).

* **Triển khai:** Docker & Docker Compose (local), Kubernetes (Production).

* **Reverse Proxy / Ingress:** Traefik hoặc Nginx.

### 2.2. Thư viện Golang (Go Modules)

* **Kafka Client:** `github.com/IBM/sarama` hoặc `github.com/segmentio/kafka-go`.

* **WebSocket:** `github.com/gorilla/websocket`.

* **Configuration:** `github.com/spf13/viper`.

* **Logging:** `go.uber.org/zap`.

* **Validation:** `github.com/go-playground/validator/v10`.
* **JWT Auth:** `github.com/golang-jwt/jwt/v5`.

## 3. CẤU TRÚC THƯ MỤC DỰ ÁN

```
QuickNotify/
├── cmd/
│   └── quicknotify/
│       └── main.go                 
├── configs/
│   ├── config.yaml                 
│   └── consumers.yaml              
├── internal/
│   ├── core/                       
│   │   ├── domain/                 # Structs (NotificationEnvelope, Client, Message).
│   │   ├── ports/                  # Interfaces (WSPort, EmailPort).
│   │   └── services/               # Dispatcher xử lý Scope/Channels, Hub quản lý State.
│   │
│   ├── adapters/                   
│   │   ├── inbound/                
│   │   │   └── kafka/              
│   │   │       ├── consumer.go     
│   │   │       ├── registry.go     
│   │   │       └── translators/    
│   │   │
│   │   └── outbound/               
│   │       ├── websocket/          
│   │       │   ├── hub.go          
│   │       │   ├── client.go       
│   │       │   ├── handler.go      
│   │       │   └── middleware.go   # Xử lý xác thực JWT
│   │       │
│   │       └── email/              
│   │           └── smtp_sender.go  
│   │
│   └── pkg/                        
├── deployments/
│   ├── Dockerfile                  
│   └── docker-compose.yml          
├── go.mod
└── go.sum

```

## 4. PHẦN 1: CHUẨN CHUNG (CORE STANDARD)

### 4.1. Chuẩn Payload (The Envelope) và Cơ chế Scope

Mọi dự án muốn tận dụng sức mạnh của QuickNotify cần gửi vào topic `global-notify-events` theo định dạng JSON. Payload được thiết kế với trường `scope` để xác định mức độ phủ sóng của thông báo:

**Ví dụ 1: Gửi cho cá nhân (Scope: `user`)**

```json
{
  "event_id": "evt_001",
  "project_id": "quicktest",
  "scope": "user", 
  "channels": ["websocket", "email"], 
  "target": {
    "user_id": "usr_8899aabb",
    "email": "student@example.com"
  },
  "content": {
    "type": "template",
    "template_id": "EXAM_GRADED_01",
    "data": { "score": "9.5", "subject": "Golang Basics" }
  }
}
```

**Ví dụ 2: Gửi toàn dự án (Scope: `project_global`)**

```json
{
  "event_id": "evt_002",
  "project_id": "quickbite",
  "scope": "project_global", 
  "channels": ["websocket"], 
  "target": {}, // Không cần user_id
  "content": {
    "type": "raw",
    "data": { "message": "QuickBite mở voucher giảm giá 50% ngay lúc này!" }
  }
}
```

**Ví dụ 3: Gửi toàn hệ thống (Scope: `system_global`)**

```json
{
  "event_id": "evt_003",
  "project_id": "system",
  "scope": "system_global", 
  "channels": ["websocket"], 
  "target": {}, 
  "content": {
    "type": "raw",
    "data": { "message": "Toàn bộ hệ thống sẽ bảo trì trong 10 phút nữa." }
  }
}
```

### 4.2. Core Dispatcher (Service Lõi Định Tuyến Đa Cấp)

Dispatcher phân loại tin nhắn dựa trên trường `scope` và gọi các hàm tương ứng của Adapter.

```go
// internal/core/domain/models.go
type Scope string
const (
    ScopeUser          Scope = "user"
    ScopeProjectGlobal Scope = "project_global"
    ScopeSystemGlobal  Scope = "system_global"
)

// internal/core/ports/outbound.go
type WSPort interface {
    PushToClient(projectID, userID string, payload []byte) error
    PushToProject(projectID string, payload []byte) error
    PushToAll(payload []byte) error
}

// internal/core/services/dispatcher.go
type Dispatcher struct {
    wsAdapter ports.WSPort
    // ... emailAdapter, v.v.
}

func (d *Dispatcher) ProcessEvent(env domain.NotificationEnvelope) error {
    for _, channel := range env.Channels {
        if channel == "websocket" {
            payloadBytes, _ := json.Marshal(env.Content)
            switch env.Scope {
            case domain.ScopeUser:
                go d.wsAdapter.PushToClient(env.ProjectID, env.Target.UserID, payloadBytes)
            case domain.ScopeProjectGlobal:
                go d.wsAdapter.PushToProject(env.ProjectID, payloadBytes)
            case domain.ScopeSystemGlobal:
                go d.wsAdapter.PushToAll(payloadBytes)
            }
        }
        // Logic cho Email/SMS...
    }
    return nil
}
```

## 5. PHẦN 2: TÍCH HỢP TÙY BIẾN (CUSTOM INTEGRATION)

### 5.1. File Cấu Hình Consumers (`configs/consumers.yaml`)

```yaml
clusters:
  - id: "cluster_main"
    brokers: ["kafka.main.local:9092"]

consumers:
  - topic: "global-notify-events"
    cluster_id: "cluster_main"
    translator: "direct" 

  - topic: "quickbite-orders"
    cluster_id: "cluster_main"
    translator: "quickbite_order"
```

### 5.2. Translator Pattern

```go
// internal/adapters/inbound/kafka/translators/quickbite_order.go
func (t *QuickbiteOrderTranslator) Translate(raw []byte) (domain.NotificationEnvelope, error) {
    return domain.NotificationEnvelope{
        ProjectID: "quickbite",
        Scope:     domain.ScopeUser,
        Channels:  []string{"websocket"},
        Target:    domain.TargetInfo{UserID: "mapped_user_id"},
        Content:   domain.ContentInfo{ /* ... */ },
    }, nil
}
```

## 6. QUẢN LÝ WEBSOCKET ĐA LUỒNG & NON-BLOCKING

Để hỗ trợ một người dùng đăng nhập nhiều thiết bị (Multi-session) và tránh sập hệ thống khi có mạng lag (Backpressure), Hub được thiết kế lại như sau:

### 6.1. Cấu trúc lưu trữ đa kết nối (Multi-Session Map)

Thay vì 1-1, Hub sử dụng Map đa tầng trỏ tới danh sách các con trỏ kết nối:

```go
// Hub quản lý kết nối
type Hub struct {
    // map[ProjectID]map[UserID]map[*Client]bool
    // Giá trị bool (true) dùng để xác định client đang active nhanh chóng (tương tự tập hợp - set)
    clients map[string]map[string]map[*Client]bool 
    sync.RWMutex // Tránh Data Race
}
```
*Lợi ích:* Khi user mở tab ẩn danh, tab thứ 2, hoặc mở app điện thoại song song, tất cả các Client đều được lưu dưới chung một `UserID` và nhận thông báo đồng thời.

### 6.2. Cơ chế truyền tải Non-Blocking Send

Khi Dispatcher gọi lệnh Push thông báo, dữ liệu đẩy vào channel `send` của Client **BẮT BUỘC** phải sử dụng cấu trúc `select - default` để tránh tình trạng goroutine bị treo (block) do client lag làm đầy buffer.

```go
func (h *Hub) pushToClientSafe(client *Client, payload []byte) {
    select {
    case client.send <- payload:
        // Đẩy tin nhắn thành công
    default:
        // Buffer channel đã đầy (Client mạng lag không nhận kịp)
        // -> Đóng channel và ngắt kết nối để giải phóng Memory
        close(client.send)
        h.removeClient(client) // Gọi hàm xóa khỏi Hub
    }
}
```

* **`PushToClient` (ScopeUser):** Hub dùng Mutex Read Lock lấy Map của `ProjectID` -> `UserID`. Duyệt qua tất cả các `*Client` đang kết nối của user đó và gọi `pushToClientSafe`.
* **`PushToProject` (ScopeProjectGlobal):** Hub khóa nhánh `ProjectID`, duyệt toàn bộ User, trong mỗi User duyệt toàn bộ `*Client` và áp dụng cơ chế Non-Blocking Send.
* **`PushToAll` (ScopeSystemGlobal):** Hub khóa toàn bộ `clients` map và broadcast cho mọi connection bằng `pushToClientSafe`.

## 7. BẢO MẬT & XÁC THỰC KẾT NỐI (AUTHENTICATION)

Để ngăn chặn lỗ hổng IDOR (Insecure Direct Object Reference) nơi người dùng tự ý đổi `user_id` trên URL để nghe lén thông báo của người khác, kết nối WebSocket bị từ chối nhận các tham số định danh từ URL Query trần.

### 7.1. Handshake Security bằng JWT
Mọi Client khi khởi tạo kết nối WS (Handshake) bắt buộc phải truyền một chuỗi JWT hợp lệ thông qua một trong hai cách (khuyến nghị cách 1):
1. **Query Param Token:** `ws://notify.domain.com/ws?token=eyJh...`
2. **Subprotocol:** Truyền Header `Sec-WebSocket-Protocol: jwt_eyJh...`

### 7.2. Token Claims (Trích xuất danh tính)
Server sẽ có một Middleware (`middleware.go`) chạy trước lúc Upgrade HTTP lên WebSocket. Quá trình hoạt động:

1. Trích xuất token từ kết nối đến.
2. Validate chữ ký JWT (Sử dụng secret key nội bộ hoặc public key xác thực).
3. Lấy `project_id` và `user_id` trực tiếp từ **Claims** của JWT token.

```go
// internal/adapters/outbound/websocket/middleware.go
func WSAuthMiddleware(next http.HandlerFunc) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        tokenString := r.URL.Query().Get("token")
        
        // Parse và Validate Token bằng thư viện jwt-go
        claims, err := ValidateJWT(tokenString)
        if err != nil {
            http.Error(w, "Unauthorized", http.StatusUnauthorized)
            return
        }

        // Lấy ProjectID và UserID TỪ TOKEN, không phải từ người dùng nhập
        userID := claims["user_id"].(string)
        projectID := claims["project_id"].(string)

        // Inject thông tin an toàn vào Context để WS Handler sử dụng
        ctx := context.WithValue(r.Context(), "user_id", userID)
        ctx := context.WithValue(ctx, "project_id", projectID)
        
        next.ServeHTTP(w, r.WithContext(ctx))
    }
}
```
*Kết quả:* Thông tin định danh của người dùng hoàn toàn do Backend ký phát. Bất kỳ sự chỉnh sửa nào từ phía Frontend sẽ làm hỏng token và bị từ chối kết nối ngay lập tức ở bước Handshake.