const express = require('express');
const cors = require('cors');

const app = express();

const PORT = process.env.API_PORT || 5002;
const INTERNAL_SECRET_HEADER = 'x-fb-internal-token';
const INTERNAL_SECRET_VALUE = process.env.INTERNAL_API_TOKEN || 'fb-analyzer-secret-2026';
const KAFKA_TOPIC = 'fb-ads-events';

/**
 * Middleware kiểm tra Header đặc biệt
 */
function checkSpecialHeader(req, res, next) {
    if (req.method === 'OPTIONS') {
        return res.sendStatus(200);
    }

    const token = req.headers[INTERNAL_SECRET_HEADER];
    if (!token || token !== INTERNAL_SECRET_VALUE) {
        return res.status(403).json({
            success: false,
            message: 'Forbidden: Invalid or missing internal system sign.'
        });
    }
    next();
}

/**
 * Khởi tạo API Server 
 * @param {import('mongodb').Db} dbInstance 
 * @param {import('kafkajs').Kafka} kafkaInstance 
 */
function initSearchServer(dbInstance, kafkaInstance) {
    const adsCol = dbInstance.collection('analyzed_ads');

    // ===== ĐĂNG KÝ MIDDLEWARE THEO ĐÚNG THỨ TỰ TRONG NÀY =====
    app.use(express.json());

    // 1. Mở cổng CORS trước
    app.use(cors({
        origin: '*',
        methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
        allowedHeaders: ['Content-Type', 'Authorization', 'x-fb-internal-token'],
        credentials: true
    }));

    // 2. Chặn bộ lọc Header ngay sau CORS
    app.use(checkSpecialHeader);

    // 3. Khởi tạo Router xử lý dữ liệu
    app.get('/api/ads/search', async (req, res) => {
        try {
            const { text, country, date_from, date_to, min_score, max_score, level } = req.query;
            const query = {};

            // 1. Filter theo Tên / Nội dung quảng cáo
            if (text) {
                query.$text = { $search: `\"${text}\"` };
            }

            // 2. Filter theo Đất nước
            if (country) {
                query.$or = [
                    { publisher_platforms: { $regex: country, $options: 'i' } },
                    { text: { $regex: country, $options: 'i' } }
                ];
            }

            // 3. Filter theo Khoảng ngày
            if (date_from || date_to) {
                query.start_date = {};
                if (date_from) {
                    query.start_date.$gte = Math.floor(new Date(date_from).getTime() / 1000);
                }
                if (date_to) {
                    query.start_date.$lte = Math.floor(new Date(date_to).getTime() / 1000);
                }
            }

            // 4. Filter theo khoảng Điểm số (Score)
            if (min_score || max_score) {
                query.score = {};
                if (min_score) query.score.$gte = parseInt(min_score, 10);
                if (max_score) query.score.$lte = parseInt(max_score, 10);
            }

            // 5. Filter theo Sản phẩm Winning
            if (level) {
                query.level = level.toUpperCase().includes('WINNER') ? '🔥 WINNER' : level.toUpperCase();
            }

            const page = parseInt(req.query.page, 10) || 1;
            const limit = parseInt(req.query.limit, 10) || 100;
            const skip = (page - 1) * limit;

            const total = await adsCol.countDocuments(query);
            const results = await adsCol.find(query)
                .sort({ score: -1, analyzed_at: -1 })
                .skip(skip)
                .limit(limit)
                .toArray();

            return res.json({
                success: true,
                pagination: {
                    total,
                    page,
                    limit,
                    pages: Math.ceil(total / limit)
                },
                data: results
            });

        } catch (error) {
            return res.status(500).json({
                success: false,
                message: 'Internal Server Error',
                error: error.message
            });
        }
    });

    app.listen(PORT, async () => {
        console.log(`🖥️  API Search Server đang hoạt động tại cổng: http://localhost:${PORT}`);

        // Kích hoạt một chu kỳ Producer ngắn để báo trạng thái Online
        try {
            const initProducer = kafkaInstance.producer();
            await initProducer.connect();
            await initProducer.send({
                topic: KAFKA_TOPIC,
                messages: [{
                    value: JSON.stringify({
                        event: 'SYSTEM_STARTUP',
                        service: 'search-api',
                        timestamp: Date.now(),
                        message: `🟢 API Search Server đã khởi tạo thành công trên cổng ${PORT} và sẵn sàng kết nối!`
                    })
                }]
            });
            await initProducer.disconnect(); // Bắn xong đóng luôn, không chạy ngầm ngốn RAM
            console.log(`🔹 [Kafka Log] Đã gửi thông báo khởi tạo hệ thống thành công.`);
        } catch (kafkaErr) {
            console.error(`❌ [Kafka Log] Không thể gửi log khởi tạo:`, kafkaErr.message);
        }
    });
}

module.exports = { initSearchServer };


