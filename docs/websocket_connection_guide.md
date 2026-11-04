# TÀI LIỆU HƯỚNG DẪN TÍCH HỢP HỆ THỐNG QUICKNOTIFY (CHUẨN CHUNG)

Tài liệu này cung cấp hướng dẫn chuẩn hóa để bất kỳ dự án nào (ví dụ: **ShorterLink**, QuickBite, EduTrack,...) có thể dễ dàng kết nối và gửi thông báo thời gian thực đến người dùng cuối thông qua **QuickNotify**.

---

## 1. MÔ HÌNH TÍCH HỢP TỔNG QUAN

QuickNotify hỗ trợ 2 cơ chế tiếp nhận sự kiện (Inbound Adapters) từ backend của bạn:
1. **Qua Apache Kafka (Khuyến nghị cho Production/Event-driven):** Bắn JSON Envelope vào topic chung `global-notify-events` (hoặc topic riêng của dự án qua Custom Translator).
2. **Qua HTTP REST API (Tiện lợi cho microservice gọi trực tiếp hoặc chạy local):** Gọi `POST /api/v1/notify`.

```
[ Backend Dự án của bạn: ShorterLink ]
      │                               │
      │ (Cách 1: Kafka Message)       │ (Cách 2: HTTP POST /api/v1/notify)
      ▼                               ▼
 [ Kafka Topic ] ──────────────► [ QuickNotify Core Dispatcher ]
                                              │
                                              │ Định tuyến theo ProjectID & UserID
                                              ▼
                                 [ WebSocket Hub Manager ]
                                              │
                                              │ Push realtime (Non-blocking)
                                              ▼
                             [ Frontend Web / Mobile App ]
```

---

## 2. KẾT NỐI WEBSOCKET & XÁC THỰC DANH TÍNH (PHÍA FRONTEND)

### 2.1. Yêu cầu về JWT Token (Authentication)
Mọi kết nối WebSocket đến QuickNotify bắt buộc phải có một chuỗi **JWT Token** hợp lệ được ký bằng thuật toán `HS256` với **Secret Key** chung (`auth.jwt_secret` cấu hình trong QuickNotify).

Token phải chứa tối thiểu các trường Claims sau:
```json
{
  "project_id": "shorterlink",
  "user_id": "usr_9988aabb",
  "exp": 1793856000
}
```

* **`project_id` (string, bắt buộc):** Định danh dự án để phân luồng độc lập (Tenant Isolation), ví dụ: `"shorterlink"`.
* **`user_id` (string, bắt buộc):** Định danh của user trong dự án ShorterLink (Account ID, User UUID).
* **`exp` (int64, bắt buộc):** Thời gian hết hạn của token (Unix timestamp).

> [!IMPORTANT]
> **Chống giả mạo danh tính (Anti-IDOR):** QuickNotify bóc tách `user_id` và `project_id` trực tiếp từ Claims đã ký của JWT Token. Không nhận `user_id` từ URL query trần, ngăn chặn hoàn toàn việc người dùng đổi ID trên URL để nghe lén thông báo của người khác.

---

### 2.2. Các cách truyền JWT Token khi kết nối
QuickNotify hỗ trợ 3 cách truyền JWT linh hoạt:

1. **Cách 1: Query Parameter (Phổ biến nhất trên trình duyệt):**
   ```text
   ws://<QUICKNOTIFY_HOST>:<PORT>/ws?token=<CHUỖI_JWT_TOKEN>
   ```
2. **Cách 2: Subprotocol Header (`Sec-WebSocket-Protocol`):**
   ```text
   Sec-WebSocket-Protocol: <CHUỖI_JWT_TOKEN>
   ```
3. **Cách 3: Authorization Header (Dành cho Mobile App / HTTP Handshake client):**
   ```text
   Authorization: Bearer <CHUỖI_JWT_TOKEN>
   ```

* **Cổng kết nối:**
  * Nếu chạy trực tiếp (Go local): Port `8080` $\to$ `ws://localhost:8080/ws?token=...`
  * Nếu chạy qua Docker / Traefik Ingress: Port `80` $\to$ `ws://localhost/ws?token=...`

---

### 2.3. Cấu hình Nguồn Kết Nối Hợp Lệ (CORS & Allowed Origins)

Trước khi frontend của ShorterLink kết nối, hãy đảm bảo domain/port của frontend đã được thêm vào mục `SERVER_ALLOWED_ORIGINS` trong [`.env`]:

```env
SERVER_ALLOWED_ORIGINS=http://localhost:3000,http://localhost:5173,http://localhost:8080,http://127.0.0.1:*
```
*(Trong môi trường kiểm thử nội bộ, bạn có thể tạm đặt `["*"]` để cho phép mọi nguồn).*

---

### 2.4. Code JavaScript mẫu phía Frontend (React / Vue / Vanilla JS)

```javascript
// 1. Lấy token của người dùng đăng nhập trong ShorterLink
const jwtToken = localStorage.getItem("auth_token"); 

// 2. Mở kết nối WebSocket tới QuickNotify
const wsUrl = `ws://localhost:8080/ws?token=${encodeURIComponent(jwtToken)}`;
const socket = new WebSocket(wsUrl);

socket.onopen = () => {
    console.log("✅ Đã kết nối thành công tới QuickNotify!");
};

socket.onmessage = (event) => {
    // Lưu ý: Một frame có thể chứa nhiều tin nhắn phân tách bằng dấu xuống dòng '\n'
    const messages = event.data.split('\n');
    for (const msgStr of messages) {
        if (!msgStr.trim()) continue;
        const notification = JSON.parse(msgStr);
        console.log("🔔 Thông báo mới nhận:", notification);

        // notification cấu trúc:
        // {
        //   "event_id": "evt_...",
        //   "project_id": "shorterlink",
        //   "scope": "user",
        //   "content": { "type": "raw", "data": { "message": "..." } }
        // }
        const text = notification.content?.data?.message || "Bạn có thông báo mới";
        showToast(text);
    }
};

socket.onclose = (event) => {
    console.warn("⚠️ WebSocket đã đóng. Đang thử kết nối lại sau 3s...", event.code);
    setTimeout(reconnectWebSocket, 3000);
};

socket.onerror = (error) => {
    console.error("❌ Lỗi kết nối WebSocket:", error);
};
```

---

## 3. PHÁT THÔNG BÁO TỪ BACKEND DỰ ÁN SHORTERLINK

### Cách 1: Bắn qua Kafka (Chuẩn Event-Driven)
Gửi message vào Kafka topic chung `global-notify-events` với nội dung JSON chuẩn Envelope.

### Cách 2: Bắn qua HTTP REST API (Gọi trực tiếp)
Gửi HTTP POST request tới `http://<QUICKNOTIFY_HOST>:8080/api/v1/notify` với Header `Content-Type: application/json`.

---

### 3.1. Cấu trúc Payload Chuẩn (JSON Envelope)

```json
{
  "event_id": "evt_shorterlink_001",
  "project_id": "shorterlink",
  "scope": "user",
  "priority": "normal",
  "channels": ["websocket"],
  "target": {
    "user_id": "usr_9988aabb",
    "email": "owner@shorterlink.com"
  },
  "content": {
    "type": "raw",
    "data": {
      "message": "Link 'bit.ly/summer-sale' của bạn vừa đạt mốc 10,000 lượt click!",
      "link_id": "link_12345",
      "short_code": "summer-sale",
      "total_clicks": 10000
    }
  }
}
```

---

### 3.2. Bảng Mô Tả Chi Tiết Các Trường

| Trường | Kiểu | Bắt buộc | Mô tả |
| :--- | :--- | :--- | :--- |
| `event_id` | `string` | **Có** | ID duy nhất của sự kiện (UUID hoặc format `evt_timestamp`). |
| `project_id` | `string` | **Có** | Đặt là `"shorterlink"` (phải trùng khớp với `project_id` trong JWT của client). |
| `scope` | `string` | **Có** | Một trong 3 giá trị: `"user"`, `"project_global"`, `"system_global"`. |
| `channels` | `array` | **Có** | Mảng các kênh nhận: `["websocket"]` (hoặc kèm `["email"]`). |
| `target.user_id`| `string` | *Bắt buộc nếu scope=user* | ID người nhận (khớp với `user_id` trong JWT của user). |
| `target.email` | `string` | *Tùy chọn* | Email nhận thông báo nếu `channels` có `"email"`. |
| `content.type` | `string` | **Có** | `"raw"` (thông điệp trực tiếp) hoặc `"template"`. |
| `content.data` | `object` | **Có** | Object chứa dữ liệu thông báo. **Khuyến nghị luôn có field `message`** để client hiển thị nhanh. |

---

### 3.3. Ví dụ các kịch bản thực tế cho ShorterLink

#### Kịch bản 1: Thông báo cá nhân (`scope: "user"`)
*Dùng khi:* Link đạt mốc lượt click, link bị cảnh báo bản quyền/spam, tài khoản được nạp tiền.
```json
{
  "event_id": "evt_sl_click_milestone_10k",
  "project_id": "shorterlink",
  "scope": "user",
  "channels": ["websocket"],
  "target": {
    "user_id": "usr_9988aabb"
  },
  "content": {
    "type": "raw",
    "data": {
      "message": "Chúc mừng! Link ngắn 'shrt.link/campaign2026' đã vượt mốc 10.000 clicks!",
      "link_id": "lnk_abc",
      "clicks": 10000
    }
  }
}
```

#### Kịch bản 2: Broadcast toàn bộ người dùng ShorterLink (`scope: "project_global"`)
*Dùng khi:* ShorterLink ra mắt tính năng mới (ví dụ: Custom Domain, QR Code động), khuyến mãi gói Pro.
```json
{
  "event_id": "evt_sl_feature_qrcode",
  "project_id": "shorterlink",
  "scope": "project_global",
  "channels": ["websocket"],
  "target": {},
  "content": {
    "type": "raw",
    "data": {
      "message": "Tính năng mới: ShorterLink hiện đã hỗ trợ tạo Dynamic QR Code với logo tùy chỉnh!"
    }
  }
}
```

#### Kịch bản 3: Thông báo bảo trì toàn hệ thống (`scope: "system_global"`)
*Dùng khi:* Bảo trì cụm máy chủ chung.
```json
{
  "event_id": "evt_sys_maintenance",
  "project_id": "system",
  "scope": "system_global",
  "channels": ["websocket"],
  "target": {},
  "content": {
    "type": "raw",
    "data": {
      "message": "Hệ thống sẽ tiến hành bảo trì định kỳ trong 15 phút tới."
    }
  }
}
```

---

## 4. CÔNG CỤ HỖ TRỢ KIỂM THỬ NHANH DỰ ÁN SHORTERLINK

Trước khi viết code vào ShorterLink, bạn có thể kiểm thử toàn bộ luồng kết nối ngay lập tức:

### 1. Sinh token thử nghiệm cho ShorterLink
Chạy lệnh sau trên terminal để sinh JWT Token cho user của ShorterLink:
```bash
go run ./scripts/gen_token/main.go -project shorterlink -user usr_9988aabb
```

### 2. Kiểm tra nhận tin trên Web Client
1. Mở file [`test_client.html`] trên trình duyệt.
2. Dán token vừa sinh vào ô **JWT Authentication Token**.
3. Bấm **Connect**.
4. Mở terminal bắn thử một thông báo:
   ```bash
   go run ./scripts/test_producer/main.go -http http://localhost:8080/api/v1/notify -type core_user -project shorterlink -user usr_9988aabb -message "Link ngắn của bạn vừa nhận thêm 100 lượt click mới!"
   ```
5. Ngay lập tức trên trình duyệt sẽ nảy thông báo popup toast và lưu vào lịch sử realtime feed!

---

## 5. CHECKLIST TÍCH HỢP CHO DỰ ÁN SHORTERLINK

- [ ] **1. Secret Key:** Đảm bảo ShorterLink ký JWT token bằng cùng secret key với QuickNotify (`.env` $\to$ `AUTH_JWT_SECRET`).
- [ ] **2. Claims:** Token của ShorterLink khi người dùng đăng nhập phải chứa `project_id: "shorterlink"` và `user_id: "<id>"`.
- [ ] **3. CORS / Origin:** Thêm domain frontend của ShorterLink vào `SERVER_ALLOWED_ORIGINS` trong `.env`.
- [ ] **4. Backend ShorterLink:** Khi phát sinh sự kiện, đóng gói JSON theo chuẩn **Envelope** và gửi vào topic Kafka `global-notify-events` hoặc gọi HTTP `POST /api/v1/notify`.
- [ ] **5. Frontend ShorterLink:** Khởi tạo `new WebSocket("ws://<host>:<port>/ws?token=" + userToken)` và lắng nghe `onmessage`.
