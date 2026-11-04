# HƯỚNG DẪN CẤU HÌNH & MAPPING ĐA CỤM KAFKA (MULTI-CLUSTER) QUA `.ENV` VÀ DOCKER COMPOSE

Tài liệu này hướng dẫn chi tiết cách cấu hình **Kafka Core** và **Multi-Cluster Topic Mapping** trong QuickNotify chỉ thông qua file `.env` và Docker Compose, thay thế hoàn toàn các file YAML cũ.

---

## 1. TỔNG QUAN KIẾN TRÚC KAFKA TRONG QUICKNOTIFY

QuickNotify được thiết kế để phục vụ đồng thời 2 nhóm nhu cầu:

```
                  ┌─────────────────────────────────────────────────────────────┐
                  │                 QuickNotify Inbound Engine                  │
                  └──────────────────────────────┬──────────────────────────────┘
                                                 │
          ┌──────────────────────────────────────┴──────────────────────────────────────┐
          ▼                                                                             ▼
┌───────────────────────────────────────┐                     ┌───────────────────────────────────────────────┐
│ Tier 1: Core Standard Kafka Cluster   │                     │ Tier 2: Multi-Cluster Integration             │
├───────────────────────────────────────┤                     ├───────────────────────────────────────────────┤
│ • Dùng cho hệ thống chuẩn (ShorterLink)│                    │ • Dùng cho các hệ thống cũ / độc lập          │
│ • Bắn trực tiếp NotificationEnvelope  │                     │ • Mỗi dự án có cụm Kafka & schema topic riêng │
│ • Cấu hình: KAFKA_BROKERS, TOPIC      │                     │ • Cấu hình: KAFKA_CLUSTERS, KAFKA_CONSUMERS   │
└───────────────────────────────────────┘                     └───────────────────────────────────────────────┘
```

1. **Tier 1 (Core Cluster):** Cụm Kafka chính của QuickNotify. Lắng nghe topic chuẩn (`global-notify-events`) dùng `DirectTranslator`. Bất kỳ dự án nào (như ShorterLink) bắn đúng format JSON `NotificationEnvelope` vào topic này đều được chuyển thẳng đến người dùng.
2. **Tier 2 (Multi-Cluster / Multi-Topic Integration):** Dành cho các dự án độc lập (QuickBite, EduTrack,...) có cụm Kafka riêng hoặc topic riêng với schema dữ liệu cũ. QuickNotify sẽ tự động lắng nghe và nạp qua Translator để chuẩn hóa về envelope.

---

## 2. ĐẶC TẢ SCHEMA CẤU HÌNH TRONG `.ENV`

Toàn bộ cấu hình Multi-Cluster được nạp qua 2 biến JSON trong `.env`: `KAFKA_CLUSTERS` và `KAFKA_CONSUMERS`.

### 2.1. Cấu hình Cụm Kafka: `KAFKA_CLUSTERS`
Chuỗi JSON Array chứa danh sách các cụm Kafka độc lập:

```json
[
  {
    "id": "tên_định_danh_cluster",
    "brokers": ["host1:port", "host2:port"]
  }
]
```

* **`id`** *(string, bắt buộc)*: Tên định danh duy nhất cho cụm Kafka này (dùng để `KAFKA_CONSUMERS` tham chiếu tới).
* **`brokers`** *(string array, bắt buộc)*: Danh sách địa chỉ IP/Host:Port của các broker trong cụm đó.

### 2.2. Cấu hình Topic Mapping: `KAFKA_CONSUMERS`
Chuỗi JSON Array chứa mapping từ Topic của Cluster cụ thể vào Translator xử lý:

```json
[
  {
    "topic": "tên_topic_cần_lắng_nghe",
    "cluster_id": "id_cụm_kafka_đã_khai_báo_ở_trên",
    "translator": "tên_translator_xử_lý"
  }
]
```

* **`topic`** *(string, bắt buộc)*: Tên Kafka topic cần consume dữ liệu.
* **`cluster_id`** *(string, bắt buộc)*: Khớp với trường `id` trong `KAFKA_CLUSTERS`.
* **`translator`** *(string, bắt buộc)*: Tên Translator đã đăng ký trong Registry.

---

## 3. DANH SÁCH TRANSLATORS CÓ SẴN TRONG HỆ THỐNG

| Tên Translator | Mô tả chức năng | Định dạng Input |
|---|---|---|
| **`direct`** | Tiếp nhận trực tiếp `NotificationEnvelope` chuẩn, không cần biến đổi. Thích hợp cho **ShorterLink** hoặc các dịch vụ nội bộ mới. | JSON Envelope (`event_id`, `project_id`, `scope`, `channels`, `target`, `content`) |
| **`quickbite_order`** | Chuyển đổi toàn bộ vòng đời đơn hàng ẩm thực QuickBite (`submitted`, `preparing`, `delivering`, `completed`, `refunded`,...) thành thông báo thời gian thực + quản lý cache Order-User trên Redis. | JSON sự kiện đơn hàng QuickBite |
| **`edutrack_exam`** | Chuyển đổi sự kiện học tập / chấm điểm thi của hệ thống EduTrack sang thông báo kết quả thi gửi cho học sinh/phụ huynh. | JSON sự kiện thi EduTrack |

*(Nếu sau này bạn có thêm dự án mới với format riêng, chỉ cần tạo 1 struct implement interface `kafka.Translator` và đăng ký vào `internal/adapters/inbound/kafka/registry.go`).*

---

## 4. CÁC KỊCH BẢN CẤU HÌNH THỰC TẾ

> [!IMPORTANT]
> **Quy tắc quan trọng trong file `.env`:**
> Giá trị của chuỗi JSON phải nằm trên **1 dòng duy nhất**, không được ngắt dòng. Bạn nên bọc chuỗi JSON bằng dấu nháy đơn (`'...'`) hoặc nháy kép.

### Kịch bản 1: Chỉ chạy cụm Core chuẩn (Ví dụ dự án ShorterLink)
Nếu hệ thống chỉ cần nhận thông báo chuẩn từ ShorterLink và các microservice nội bộ:

```env
# QuickNotify Core Cluster
KAFKA_BROKERS=100.76.224.64:9092
KAFKA_CONSUMER_GROUP=quicknotify-core-group
KAFKA_STANDARD_TOPIC=global-notify-events

# Không cần khai báo KAFKA_CLUSTERS hoặc KAFKA_CONSUMERS
```

QuickNotify sẽ tự động khởi tạo 1 consumer lắng nghe topic `global-notify-events` trên broker `100.76.224.64:9092`.

---

### Kịch bản 2: 1 Cụm Kafka duy nhất nhưng có nhiều Topic của các dự án
Khi tất cả các dự án (ShorterLink, QuickBite, EduTrack) cùng trỏ về 1 Kafka cluster nhưng mỗi dự án dùng topic và schema riêng:

```env
KAFKA_BROKERS=100.76.224.64:9092
KAFKA_CONSUMER_GROUP=quicknotify-core-group
KAFKA_STANDARD_TOPIC=global-notify-events

KAFKA_CLUSTERS='[{"id":"shared_cluster","brokers":["100.76.224.64:9092"]}]'
KAFKA_CONSUMERS='[{"topic":"global-notify-events","cluster_id":"shared_cluster","translator":"direct"},{"topic":"order-events","cluster_id":"shared_cluster","translator":"quickbite_order"},{"topic":"edutrack-exam-results","cluster_id":"shared_cluster","translator":"edutrack_exam"}]'
```

---

### Kịch bản 3: Đa cụm Kafka độc lập (Kafka 1 có topic 1.1, 1.2; Kafka 2 có topic 2.1, 2.2)
Đây là kịch bản chuẩn doanh nghiệp:
* **Kafka 1 (`cluster_main`):** `100.76.224.64:9092` có topic `global-notify-events` và `shorterlink-audit`
* **Kafka 2 (`cluster_quickbite`):** `kafka.quickbite.internal:9092` có topic `order-events` và `restaurant-payouts`
* **Kafka 3 (`cluster_edutrack`):** `10.0.2.15:9092,10.0.2.16:9092` có topic `edutrack-exam-results`

Trong file `.env`:

```env
# 1. Cluster mặc định của QuickNotify
KAFKA_BROKERS=100.76.224.64:9092
KAFKA_CONSUMER_GROUP=quicknotify-core-group
KAFKA_STANDARD_TOPIC=global-notify-events

# 2. Khai báo danh sách các cụm Kafka độc lập
KAFKA_CLUSTERS='[{"id":"cluster_main","brokers":["100.76.224.64:9092"]},{"id":"cluster_quickbite","brokers":["kafka.quickbite.internal:9092"]},{"id":"cluster_edutrack","brokers":["10.0.2.15:9092","10.0.2.16:9092"]}]'

# 3. Mapping topic của từng cluster vào đúng translator
KAFKA_CONSUMERS='[{"topic":"global-notify-events","cluster_id":"cluster_main","translator":"direct"},{"topic":"shorterlink-audit","cluster_id":"cluster_main","translator":"direct"},{"topic":"order-events","cluster_id":"cluster_quickbite","translator":"quickbite_order"},{"topic":"restaurant-payouts","cluster_id":"cluster_quickbite","translator":"direct"},{"topic":"edutrack-exam-results","cluster_id":"cluster_edutrack","translator":"edutrack_exam"}]'
```

Khi chạy, QuickNotify sẽ mở các Consumer Goroutines độc lập kết nối song song đến cả 3 cụm Kafka trên.

---

## 5. TRIỂN KHAI VỚI DOCKER COMPOSE

### 5.1. File `docker-compose.yml` chuẩn
Bạn chỉ cần để file `docker-compose.yml` sử dụng `env_file: .env`:

```yaml
version: '3.8'

services:
  quicknotify:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: quicknotify
    ports:
      - "8080:8080"
    env_file:
      - .env
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://localhost:8080/healthz"]
      interval: 15s
      timeout: 5s
      retries: 3
      start_period: 5s
```

### 5.2. Hoặc cấu hình trực tiếp qua khối `environment:`
Nếu bạn không muốn dùng file `.env` mà muốn nhúng cấu hình thẳng vào `docker-compose.yml` (ví dụ khi kết hợp vào cụm compose của dự án khác):

```yaml
services:
  quicknotify:
    image: quicknotify:latest
    ports:
      - "8080:8080"
    environment:
      - SERVER_HTTP_PORT=8080
      - AUTH_JWT_SECRET=quicknotify_dev_secret_key_2026
      - KAFKA_BROKERS=100.76.224.64:9092
      - KAFKA_STANDARD_TOPIC=global-notify-events
      - KAFKA_CLUSTERS=[{"id":"main","brokers":["100.76.224.64:9092"]},{"id":"qb","brokers":["kafka.quickbite:9092"]}]
      - KAFKA_CONSUMERS=[{"topic":"global-notify-events","cluster_id":"main","translator":"direct"},{"topic":"order-events","cluster_id":"qb","translator":"quickbite_order"}]
```

---

## 6. QUY TRÌNH THÊM MỘT PROJECT / KAFKA MỚI TRONG TƯƠNG LAI

Khi bạn phát triển thêm 1 dự án mới (ví dụ: dự án `ShopOnline`):

1. **Bước 1: Xác định format dữ liệu:**
   * Nếu `ShopOnline` bắn đúng JSON chuẩn `NotificationEnvelope` $\to$ Dùng translator `direct`.
   * Nếu `ShopOnline` bắn JSON đặc thù riêng $\to$ Viết thêm 1 translator `shoponline_order.go` trong `internal/adapters/inbound/kafka/translators/` và đăng ký vào registry.
2. **Bước 2: Cập nhật `.env`:**
   * Thêm cluster của `ShopOnline` vào mảng `KAFKA_CLUSTERS`.
   * Thêm mapping topic vào mảng `KAFKA_CONSUMERS`.
3. **Bước 3: Khởi động lại dịch vụ:**
   ```bash
   docker compose restart
   ```

---

## 7. KIỂM TRA & TROUBLESHOOTING

Khi khởi động QuickNotify, kiểm tra log container để đảm bảo các consumer đã được gán thành công:

```bash
docker compose logs -f quicknotify
```

* **Log nạp thành công multi-cluster consumer:**
  ```text
  {"level":"info","msg":"Configured Kafka consumer for topic","topic":"order-events","cluster_id":"cluster_quickbite","translator":"quickbite_order"}
  {"level":"info","msg":"Configured Kafka consumer for topic","topic":"global-notify-events","cluster_id":"cluster_main","translator":"direct"}
  ```
* **Lỗi sai Cluster ID:**
  ```text
  {"level":"warn","msg":"Skipping consumer: cluster_id not found or has no brokers","topic":"order-events","cluster_id":"cluster_xyz"}
  ```
  $\to$ Kiểm tra lại xem `cluster_id` trong `KAFKA_CONSUMERS` có khớp với `id` trong `KAFKA_CLUSTERS` chưa.
* **Lỗi Translator không tồn tại:**
  ```text
  {"level":"warn","msg":"Skipping consumer: translator not found in registry","topic":"order-events","translator":"abc"}
  ```
  $\to$ Kiểm tra lại tên translator (chỉ dùng `direct`, `quickbite_order`, `edutrack_exam` hoặc translator bạn tự viết).

