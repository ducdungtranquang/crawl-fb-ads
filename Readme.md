# Hệ Thống Cào Dữ Liệu & Phân Tích Facebook Ads

Hệ thống thu thập dữ liệu từ Facebook Ads Library, xử lý luồng sự kiện qua Apache Kafka, và chấm điểm quảng cáo tự động.

---

## 🏗️ 1. Kiến Trúc và Luồng Dữ Liệu

Hệ thống bao gồm các thành phần chính:
1.  **Crawler Service (`crawler`)**: Chạy liên tục để cào dữ liệu quảng cáo mới và đẩy vào Apache Kafka.
2.  **Analyzer Service (`analyzer`)**: Được chạy thủ công khi cần. Dịch vụ này sẽ đọc tất cả dữ liệu từ Kafka, phân tích, chấm điểm, và lưu kết quả vào MongoDB. Sau khi xử lý xong, nó sẽ tự động kết thúc.
3.  **API Server (`searchApi`)**: Cung cấp các endpoint để truy vấn dữ liệu đã được phân tích.
4.  **Apache Kafka & Zookeeper**: Hệ thống message queue trung gian.
5.  **MongoDB**: Cơ sở dữ liệu để lưu trữ và truy vấn.

---

## 🚀 2. Hướng Dẫn Cài Đặt và Khởi Chạy Thủ Công

### Bước 1: Cài đặt các phần mềm cần thiết
1.  **Cài đặt Node.js**: Tải và cài đặt phiên bản LTS mới nhất từ trang chủ nodejs.org.
2.  **Cài đặt MongoDB**: Tải và cài đặt MongoDB Community Server từ mongodb.com. Đảm bảo dịch vụ MongoDB đang chạy.
3.  **Cài đặt và chạy Kafka**:
    *   Tải Kafka từ kafka.apache.org/downloads.
    *   Giải nén và làm theo hướng dẫn để khởi chạy Zookeeper và Kafka Server.
    *   **Lệnh chạy Zookeeper:** `bin\windows\zookeeper-server-start.bat config\zookeeper.properties`
    *   **Lệnh chạy Kafka:** `bin\windows\kafka-server-start.bat config\server.properties`

### Bước 2: Cài đặt các gói phụ thuộc của dự án
Mở terminal trong thư mục gốc của dự án và chạy các lệnh sau:
```bash
# Cài đặt cho crawler
cd fb-ads-system/crawler
npm install

# Cài đặt cho analyzer (bao gồm cả API)
cd ../analyzer
npm install
```

---

### Bước 3: Khởi chạy các dịch vụ
Bạn cần mở các cửa sổ terminal riêng biệt cho mỗi dịch vụ.

1.  **Chạy Crawler Service (Luôn chạy nền):**
    ```bash
    cd fb-ads-system/crawler
    node crawler.js
    ```
    Một cửa sổ trình duyệt Chrome sẽ mở ra. Hãy đăng nhập vào Facebook và để nó chạy để thu thập dữ liệu.

2.  **Chạy API Server (Luôn chạy nền):**
    ```bash
    cd fb-ads-system/analyzer
    node searchApi.js
    ```
    API sẽ có sẵn tại `http://localhost:5002`.

3.  **Chạy Analyzer Service (Chạy khi cần phân tích):**
    Bất cứ khi nào bạn muốn xử lý dữ liệu đã thu thập, hãy mở một terminal mới và chạy:
    ```bash
    cd fb-ads-system/analyzer
    node analyzer.js
    ```
    Tiến trình sẽ xử lý tất cả quảng cáo đang chờ và tự động thoát khi hoàn tất. Bạn có thể chạy lệnh này vài lần một ngày theo nhu-cầu.

---
### Các công cụ hữu ích
*   **Kafka UI (Tùy chọn):** Bạn có thể chạy Kafka UI qua Docker để theo dõi các topic và message một cách trực quan.
    ```bash
    docker run -p 8080:8080 -e KAFKA_CLUSTERS_0_NAME=local -e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=host.docker.internal:9092 provectuslabs/kafka-ui:latest
    ```
*   **MongoDB Compass:** Sử dụng để xem và quản lý cơ sở dữ liệu.
