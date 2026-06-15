# Hệ Thống Cào Dữ Liệu & Phân Tích Facebook Ads (Microservices)

Hệ thống thu thập dữ liệu từ Facebook Ads Library, xử lý luồng sự kiện qua Apache Kafka, chấm điểm tự động và giám sát hệ thống log tập trung bằng MongoDB.

---

## 🏗️ 1. Kiến Trúc Luồng Dữ Liệu & Khởi Chạy

Hệ thống được thiết kế theo mô hình Microservices, luân chuyển dữ liệu qua các bước sau:
1. **Crawler Service (`fb-ads`)** cào bài quảng cáo -> Đẩy vào Kafka Topic.
2. **Analyzer Service (`fb-ads-analyzer`)** nhặt dữ liệu từ Kafka để phân tích điểm số -> Đẩy log/kết quả về Kafka/MongoDB.
3. **Monitoring Service (`fb-ads-monitor`)** lắng nghe hàng đợi log để lưu trữ và hiển thị lên giao diện giám sát.

### 🚨 Cấu Hình MongoDB Trên Windows (Máy Thật) Trước Khi Chạy
Để tránh lỗi lệch múi giờ âm của Docker Desktop trên Windows (`TimeoutNegativeWarning`), toàn bộ các service trong Docker sẽ bắn dữ liệu trực tiếp về MongoDB Compass chạy ngoài máy thật.
1. Mở file cấu hình MongoDB trên Windows: `C:\Program Files\MongoDB\Server\6.0\bin\mongod.cfg` (hoặc đường dẫn bản 7.0 tương ứng).
2. Sửa dòng `bindIp: 127.0.0.1` thành `bindIp: 0.0.0.0`.
3. Mở **Task Manager** -> Chọn tab **Services** -> Chuột phải vào dịch vụ **MongoDB** và nhấn **Restart**.

---

## 🚀 2. Hướng Dẫn Khởi Chạy Hệ Thống (Đúng Thứ Tự)

Để tránh lỗi các Consumer tranh chấp phân vùng khi Topic chưa khởi tạo (`UNKNOWN_TOPIC_OR_PARTITION`), hệ thống cần được kích hoạt theo đúng thứ tự:

### Bước 1: Dọn dẹp và hạ toàn bộ cụm container cũ
```bash
docker compose down

docker compose up -d --build

###### Hoặc

docker compose up -d kafka zookeeper crawler-service --build

docker compose up -d analyzer-service monitoring-service --build

############

docker compose ps

# Xem log hệ thống cào, tiến trình điều khiển Chrome ẩn và trạng thái đẩy Ads
docker compose logs -f crawler-service

# Xem log hệ thống phân tích, luồng đọc tin nhắn Batch và chấm điểm Ads
docker compose logs -f analyzer-service

# Xem log hệ thống giám sát và API Server chạy tại cổng 4000
docker compose logs -f monitoring-service

docker exec -it fb-ads-kafka kafka-topics --bootstrap-server localhost:9092 --list

docker exec -it fb-ads-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list

docker exec -it fb-ads-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group analyzer-group

docker exec -it fb-ads-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic <tên_topic> --from-beginning
