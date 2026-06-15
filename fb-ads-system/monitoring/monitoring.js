const { MongoClient } = require('mongodb');
const { Kafka } = require('kafkajs');
const express = require('express');

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
    // 1. Kết nối DB
    const client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db(DB_NAME);
    console.log('✅ Monitoring MongoDB connected');

    // Tạo Index cho log_time để sau này truy vấn log nhanh hơn
    await db.collection('logs').createIndex({ timestamp: -1 });

    // 2. Kết nối Kafka
    await consumer.connect();
    await consumer.subscribe({ topic: LOG_TOPIC, fromBeginning: false });

    console.log('🖥️  Monitoring Service đang lắng nghe log hệ thống từ Kafka...');

    // Xử lý gom Batch Log để lưu vào DB cho nhanh
    await consumer.run({
        eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
            const bulkLogs = [];

            for (const message of batch.messages) {
                try {
                    const logEntry = JSON.parse(message.value.toString());
                    bulkLogs.push(logEntry);

                    // Cập nhật số liệu Real-time metrics
                    dashboardMetrics.total_logs++;
                    if (logEntry.level === 'INFO') dashboardMetrics.info_count++;
                    if (logEntry.level === 'WARN') dashboardMetrics.warn_count++;
                    if (logEntry.level === 'ERROR') {
                        dashboardMetrics.error_count++;
                        // Gom lỗi mới nhất lên đầu mảng
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
                await db.collection('logs').insertMany(bulkLogs);
            }

            for (const message of batch.messages) { resolveOffset(message.offset); }
            await heartbeat();
        }
    });
}

// ===== API SERVER ĐỂ CHECK LOG & ERROR =====
const app = express();
app.use(express.json());

// 1. API lấy tổng quan số lượng lỗi/log (Dùng cho giao diện Dashboard)
app.get('/api/monitor/summary', (req, res) => {
    res.json({ success: true, metrics: dashboardMetrics });
});

// 2. API Tra cứu Log lịch sử, hỗ trợ filter theo Service hoặc Level (ERROR/INFO)
app.get('/api/monitor/logs', async (req, res) => {
    try {
        const { service, level, limit = 50 } = req.query;
        const query = {};
        if (service) query.service = service;
        if (level) query.level = level.toUpperCase();

        const logs = await db.collection('logs')
            .find(query)
            .sort({ timestamp: -1 })
            .limit(parseInt(limit))
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