# TÀI LIỆU HƯỚNG DẪN TÍCH HỢP HỆ THỐNG QUICKNOTIFY (CHUẨN CHUNG)

Tài liệu này cung cấp hướng dẫn chuẩn hóa để bất kỳ dự án hoặc vi dịch vụ (Microservice) nào trong hệ sinh thái có thể tích hợp và gửi thông báo thời gian thực đến người dùng cuối qua **QuickNotify**.

---

## 1. MÔ HÌNH TÍCH HỢP TỔNG QUAN

Hệ thống hoạt động theo mô hình phi tập trung:
* **Phía Frontend (Người dùng cuối):** Mở kết nối WebSocket tới QuickNotify kèm theo một **JWT Token** hợp lệ.
* **Phía Backend (Dịch vụ nghiệp vụ của bạn):** Khi phát sinh sự kiện cần gửi thông báo, chỉ cần **Publish một JSON Payload chuẩn** vào Kafka Topic chung.
* **QuickNotify:** Đóng vai trò là trung tâm phân phối, nhận sự kiện từ Kafka $\rightarrow$ bóc tách định danh $\rightarrow$ đẩy trực tiếp xuống thiết bị của người dùng qua WebSocket.

```
[ Dịch vụ Backend của bạn ]
           │
           │ (1) Publish Event chuẩn (JSON Envelope)
           ▼
    [ Kafka Topic ]
           │
           ▼
    [ QuickNotify ]
           │
           │ (2) Tự động định tuyến theo Scope & Target User
           ▼
[ Frontend / App của User ] (Nhận realtime qua WebSocket)
```

---

## 2. KẾT NỐI WEBSOCKET & XÁC THỰC DANH TÍNH (FRONTEND)

### 2.1. Yêu cầu về JWT Token
Để kết nối WebSocket, Client bắt buộc phải có một chuỗi **JWT Token** (thuật toán `HS256`) do hệ thống cấp khi người dùng đăng nhập. 

Token phải chứa tối thiểu các trường (Claims) bắt buộc sau:

```json
{
  "project_id": "<MÃ_DỰ_ÁN_CỦA_BẠN>", 
  "user_id": "<ID_NGƯỜI_DÙNG>",
  "exp": 1793400000
}
```

* **`project_id` (string, bắt buộc):** Mã định danh dự án của bạn (ví dụ: `"my_project"`, `"ecommerce"`, `"lms"`). Dùng để cô lập người dùng giữa các dự án khác nhau.
* **`user_id` (string, bắt buộc):** Mã định danh duy nhất của người dùng trong hệ thống của bạn (User ID, UUID).
* **`exp` (int64, bắt buộc):** Thời gian hết hạn của token (Unix timestamp).

> [!IMPORTANT]
> **Cơ chế bảo mật chống giả mạo:** QuickNotify tự động giải mã token bằng Secret Key nội bộ để lấy `user_id`. Không cho phép truyền `user_id` trực tiếp trên URL để loại bỏ hoàn toàn nguy cơ kẻ xấu tự đổi ID nghe lén thông báo của người khác.

---

### 2.2. Cách kết nối từ Client
Client kết nối tới endpoint `/ws` và truyền Token qua Query Parameter:

```
ws://<QUICKNOTIFY_HOST>:<PORT>/ws?token=<CHUỖI_JWT_TOKEN>
```

---

### 2.3. Cơ Chế Giới Hạn Nguồn Kết Nối (CORS & Origin Whitelist)

Để ngăn chặn việc bất kỳ trang web lạ nào cũng có thể mở kết nối tới QuickNotify, hệ thống trang bị bộ lọc **Origin Check**:

* Trong file cấu hình [`configs/config.yaml`](file:///d:/Workspace/QuickNotify/configs/config.yaml), người quản trị chỉ định danh sách tên miền được phép kết nối qua `allowed_origins`:
  ```yaml
  server:
    allowed_origins:
      - "http://localhost:3000"
      - "http://localhost:5173"
      - "https://app.yourdomain.com"
      - "https://*.yourdomain.com" # Hỗ trợ wildcard subdomain
  ```
* **Cơ chế hoạt động:** 
  * Khi trình duyệt gửi request nâng cấp WebSocket, Header `Origin` được kiểm tra đối chiếu với Whitelist.
  * Nếu Origin **hợp lệ**: Server cho phép bắt tay và cấp kết nối.
  * Nếu Origin **không nằm trong danh sách**: Server từ chối ngay lập tức (**HTTP 403 Forbidden**), bảo vệ tuyệt đối hệ thống trước các cuộc tấn công Cross-Site WebSocket Hijacking (CSWSH).
  * Đối với các client không phải trình duyệt (như Mobile App Native / Backend Service / Postman) không có Header `Origin`, kết nối vẫn được cho phép dựa trên xác thực JWT Token.

#### Code JavaScript mẫu phía Frontend:
```javascript
// 1. Lấy token của người dùng sau khi đăng nhập
const userToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...";

// 2. Khởi tạo kết nối WebSocket
const socket = new WebSocket(`ws://<QUICKNOTIFY_HOST>:<PORT>/ws?token=${userToken}`);

socket.onopen = () => {
    console.log("Đã kết nối thành công tới QuickNotify!");
};

socket.onmessage = (event) => {
    const notification = JSON.parse(event.data);
    console.log("Nhận thông báo mới:", notification);

    // Dữ liệu nội dung thông báo
    const content = notification.content;
    alert(content.data.message);
};

socket.onclose = (event) => {
    console.warn("Mất kết nối WebSocket. Mã lỗi:", event.code);
};

socket.onerror = (error) => {
    console.error("Lỗi WebSocket:", error);
};
```

---

## 3. CHUẨN PAYLOAD PHÁT SỰ KIỆN QUA KAFKA (BACKEND)

Mọi dịch vụ Backend khi muốn gửi thông báo bắt buộc phải publish dữ liệu vào Kafka theo cấu trúc **NotificationEnvelope (JSON)** chuẩn dưới đây.

### 3.1. Cấu trúc Payload Chuẩn (JSON Envelope)

```json
{
  "event_id": "evt_unique_123456",
  "project_id": "<MÃ_DỰ_ÁN_CỦA_BẠN>",
  "scope": "user",
  "priority": "normal",
  "channels": ["websocket"],
  "target": {
    "user_id": "<ID_NGƯỜI_NHẬN>",
    "email": "user@example.com"
  },
  "content": {
    "type": "raw",
    "template_id": "",
    "data": {
      "message": "Nội dung thông báo hiển thị cho người dùng",
      "extra_info": "Dữ liệu bổ sung tùy ý"
    }
  }
}
```

### 3.2. Bảng Mô Tả Các Trường Dữ Liệu

| Trường | Kiểu | Bắt buộc | Mô tả |
| :--- | :--- | :--- | :--- |
| `event_id` | `string` | **Có** | ID duy nhất của sự kiện (dùng để log và truy vết). |
| `project_id` | `string` | **Có** | Mã dự án gửi thông báo (phải khớp với `project_id` trong token của client). |
| `scope` | `string` | **Có** | Phạm vi phát thông báo: `"user"`, `"project_global"`, hoặc `"system_global"`. |
| `channels` | `array` | **Có** | Danh sách kênh nhận: `["websocket"]`, `["email"]` hoặc `["websocket", "email"]`. |
| `target.user_id`| `string` | *Tùy scope* | ID người nhận (bắt buộc khi `scope: "user"`). |
| `target.email` | `string` | *Không* | Email người nhận (cần thiết nếu `channels` chứa `"email"`). |
| `content.type` | `string` | **Có** | Định dạng thông báo: `"raw"` (thông điệp trực tiếp) hoặc `"template"`. |
| `content.template_id` | `string` | *Không* | Mã mẫu thông báo nếu `type: "template"`. |
| `content.data` | `object` | **Có** | Dữ liệu thông báo động. Trường `message` khuyến nghị luôn có để hiển thị câu chữ hoàn chỉnh. |

---

### 3.3. Các Cấp Độ Phạm Vi Thông Báo (`scope`)

Hệ thống hỗ trợ 3 cấp độ định tuyến thông báo:

#### Cấp độ 1: Gửi cho cá nhân (`scope: "user"`)
* **Mục đích:** Gửi thông báo riêng cho 1 người dùng cụ thể (ví dụ: thông báo đơn hàng của bạn, chấm điểm, nhắc việc cá nhân).
* **Yêu cầu:** Bắt buộc phải có `target.user_id`.
* **Cơ chế:** QuickNotify sẽ tìm trong Hub và đẩy đồng thời tới **toàn bộ các tab/thiết bị** mà người dùng này đang mở.

```json
{
  "event_id": "evt_personal_001",
  "project_id": "my_project",
  "scope": "user",
  "channels": ["websocket"],
  "target": {
    "user_id": "usr_9988aabb"
  },
  "content": {
    "type": "raw",
    "data": {
      "message": "Số dư tài khoản của bạn vừa thay đổi +50,000 VND."
    }
  }
}
```

#### Cấp độ 2: Gửi toàn dự án (`scope: "project_global"`)
* **Mục đích:** Gửi broadcast tới **tất cả người dùng đang online trong dự án của bạn** (ví dụ: tung voucher chung, thông báo nội bộ dự án).
* **Yêu cầu:** Để trống trường `target`. QuickNotify tự động phát cho mọi client có cùng `project_id`.

```json
{
  "event_id": "evt_project_promo_002",
  "project_id": "my_project",
  "scope": "project_global",
  "channels": ["websocket"],
  "target": {},
  "content": {
    "type": "raw",
    "data": {
      "message": "Sự kiện siêu sale vừa bắt đầu, hãy khám phá ngay!"
    }
  }
}
```

#### Cấp độ 3: Gửi toàn hệ thống (`scope: "system_global"`)
* **Mục đích:** Gửi broadcast khẩn cấp tới **toàn bộ người dùng đang online của tất cả các dự án** kết nối vào hệ thống (ví dụ: thông báo bảo trì máy chủ trung tâm).

```json
{
  "event_id": "evt_maintenance_003",
  "project_id": "system",
  "scope": "system_global",
  "channels": ["websocket"],
  "target": {},
  "content": {
    "type": "raw",
    "data": {
      "message": "Toàn bộ hệ thống sẽ bảo trì nâng cấp trong 15 phút tới."
    }
  }
}
```

---

## 4. THÔNG TIN HẠ TẦNG KAFKA (DÀNH CHO BACKEND TÍCH HỢP)

Các dịch vụ Backend của bạn gửi sự kiện vào thông tin kết nối dưới đây:

* **Kafka Brokers:**
  ```
  # Điền danh sách địa chỉ Kafka Broker tại đây (ví dụ: 192.168.1.10:9092)
  - <ĐIỀN_KAFKA_BROKER_HOST_1:PORT>
  - <ĐIỀN_KAFKA_BROKER_HOST_2:PORT>
  ```
* **Topic chuẩn tiếp nhận thông báo chung:**
  ```
  global-notify-events
  ```
* **Serialization:** Chuỗi định dạng **JSON** với mã hóa UTF-8.

---

## 5. TÓM TẮT CHECKLIST KHI TÍCH HỢP DỰ ÁN MỚI

1. [ ] **Phía Backend:** Khi có sự kiện, đóng gói JSON theo chuẩn **Envelope** (`event_id`, `project_id`, `scope`, `channels`, `target`, `content`) và bắn vào topic `global-notify-events`.
2. [ ] **Phía Auth:** API Login của bạn khi cấp token cho User cần bổ sung claims: `project_id` và `user_id`.
3. [ ] **Phía Frontend:** Lấy JWT Token từ Auth và mở WebSocket đến `ws://<QUICKNOTIFY_HOST>:<PORT>/ws?token=<JWT>`.
4. [ ] **Nhận thông báo:** Lắng nghe sự kiện `socket.onmessage` và xử lý giao diện cho người dùng cuối.
