const { MongoClient } = require('mongodb');
const { Kafka } = require('kafkajs');
const express = require('express');
const path = require('path');

process.removeAllListeners('warning');
process.on('warning', (warning) => {
    if (warning.name === 'TimeoutNegativeWarning') return;
    console.warn(warning.stack);
});

const originalSetTimeout = global.setTimeout;
global.setTimeout = function (callback, delay, ...args) {
    if (typeof delay === 'number' && delay < 0) {
        return originalSetTimeout(callback, 1000, ...args);
    }
    return originalSetTimeout(callback, delay, ...args);
};

process.env.TZ = 'UTC';
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';

// ===== CONFIG =====
const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2';
const DB_NAME = 'fb_ads_monitoring';
const PORT = 4000;

const KAFKA_BROKERS = process.env.KAFKA_BROKERS ? [process.env.KAFKA_BROKERS] : ['localhost:9092'];
const LOG_TOPIC = 'system-logs';

let db;
const kafka = new Kafka({ clientId: 'monitoring-service', brokers: KAFKA_BROKERS });
const consumer = kafka.consumer({ groupId: 'monitoring-log-group' });

let dashboardMetrics = {
    total_logs: 0,
    info_count: 0,
    warn_count: 0,
    error_count: 0,
    recent_errors: []
};

// ===== CONNECT DB & START KAFKA =====
async function startMonitoring() {
    const client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db(DB_NAME);
    console.log('✅ Monitoring MongoDB connected');

    // TỐI ƯU HÓA: Tạo chỉ mục phức hợp để lọc kết hợp đa dạng điều kiện (Service + Level + Time) cực nhanh
    const logsCol = db.collection('logs');
    await logsCol.createIndex({ service: 1, level: 1, timestamp: -1 });
    await logsCol.createIndex({ timestamp: -1 });

    await consumer.connect();
    await consumer.subscribe({ topic: LOG_TOPIC, fromBeginning: false });

    console.log('🖥️  Monitoring Service đang lắng nghe log hệ thống từ Kafka...');

    await consumer.run({
        eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
            const bulkLogs = [];

            for (const message of batch.messages) {
                try {
                    const logEntry = JSON.parse(message.value.toString());
                    // Đảm bảo cấu trúc timestamp luôn là dạng Number
                    if (logEntry.timestamp && typeof logEntry.timestamp === 'string') {
                        logEntry.timestamp = new Date(logEntry.timestamp).getTime();
                    } else if (!logEntry.timestamp) {
                        logEntry.timestamp = Date.now();
                    }

                    bulkLogs.push(logEntry);

                    dashboardMetrics.total_logs++;
                    if (logEntry.level === 'INFO') dashboardMetrics.info_count++;
                    if (logEntry.level === 'WARN') dashboardMetrics.warn_count++;
                    if (logEntry.level === 'ERROR') {
                        dashboardMetrics.error_count++;
                        dashboardMetrics.recent_errors.unshift(logEntry);
                        if (dashboardMetrics.recent_errors.length > 10) {
                            dashboardMetrics.recent_errors.pop();
                        }
                    }
                } catch (e) {
                    console.error('❌ Lỗi parse log message:', e.message);
                }
            }

            if (bulkLogs.length > 0) {
                await logsCol.insertMany(bulkLogs);
            }

            for (const message of batch.messages) { resolveOffset(message.offset); }
            await heartbeat();
        }
    });
}

// ===== API SERVER =====
const app = express();
app.use(express.json());

// Phục vụ thư mục UI tĩnh
app.use(express.static(path.join(__dirname, 'public')));

// 1. API lấy tổng quan số liệu thống kê nhanh
app.get('/api/monitor/summary', (req, res) => {
    res.json({ success: true, metrics: dashboardMetrics });
});

// 2. API ĐÃ ĐƯỢC NÂNG CẤP: Hỗ trợ lọc đa dạng theo Dịch vụ, Trạng thái (INFO/WARN/ERROR), Thời gian
app.get('/api/monitor/logs', async (req, res) => {
    try {
        const { service, level, startTime, endTime, limit = 50 } = req.query;

        // Khởi tạo Object query động
        const query = {};

        // Lọc theo Service cụ thể
        if (service) {
            query.service = service;
        }

        // Lọc theo Trạng thái / Cấp độ lỗi
        if (level) {
            query.level = level.toUpperCase();
        }

        // Lọc theo Khoảng thời gian (Kiểm tra mốc Timestamp dạng số)
        if (startTime || endTime) {
            query.timestamp = {};
            if (startTime) {
                query.timestamp.$gte = parseInt(startTime, 10);
            }
            if (endTime) {
                query.timestamp.$lte = parseInt(endTime, 10);
            }
        }

        // Thực thi tìm kiếm trên Database đã được Index
        const logs = await db.collection('logs')
            .find(query)
            .sort({ timestamp: -1 }) // Ưu tiên các dòng log mới nhất lên đầu
            .limit(Math.min(parseInt(limit, 10), 200)) // Đặt lằn ranh giới hạn để bảo vệ băng thông ứng dụng
            .toArray();

        res.json({ success: true, count: logs.length, data: logs });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

app.listen(PORT, () => {
    console.log(`🖥️  Monitoring API Server đang chạy tại cổng http://localhost:${PORT}`);
    startMonitoring().catch(console.error);
});