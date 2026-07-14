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

    /**
         * 🟢 API 1: TÌM KIẾM QUẢNG CÁO (Đã bổ sung Media thu gọn để tăng tốc độ)
         */
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
                query.platforms = country.toUpperCase();
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
                query.level = level.toUpperCase().includes('WINNER') ? 'WINNER' : level.toUpperCase();
            }

            const page = parseInt(req.query.page, 10) || 1;
            const limit = parseInt(req.query.limit, 10) || 100;
            const skip = (page - 1) * limit;

            const total = await adsCol.countDocuments(query);

            // 💡 NÂNG CẤP PROJECTION: Lấy thêm Media đại diện (chỉ lấy 1 phần tử đầu tiên để tối ưu băng thông)
            const results = await adsCol.find(query)
                .project({
                    ad_archive_id: 1,
                    page_name: 1,
                    page_like_count: 1,
                    text: { $substrCP: ["$text", 0, 150] },
                    link: 1,
                    domain: 1,
                    start_date: 1,
                    seen_count: 1,
                    score: 1,
                    level: 1,
                    scaling_level: 1,
                    funnel: 1,
                    analyzed_at: 1,
                    // Chỉ bốc duy nhất 1 ảnh/video đầu tiên làm thumbnail hiển thị trên Card UI danh sách
                    images: { $slice: ["$images", 1] },
                    videos: { $slice: ["$videos", 1] }
                })
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

    /**
     * 🟢 API 2: LẤY CHI TIẾT QUẢNG CÁO (Trả ra toàn bộ Object gồm lịch sử tăng trưởng)
     */
    app.get('/api/ads/detail/:id', async (req, res) => {
        try {
            const adId = req.params.id;

            if (!adId) {
                return res.status(400).json({ success: false, message: 'Missing ad archive id parameters.' });
            }

            // Tìm kiếm bản ghi chi tiết đầy đủ thuộc tính trong MongoDB
            const adDetail = await adsCol.findOne({ ad_archive_id: adId });

            if (!adDetail) {
                return res.status(404).json({
                    success: false,
                    message: `Không tìm thấy thông tin chi tiết cho quảng cáo có ID: ${adId}`
                });
            }

            return res.json({
                success: true,
                data: adDetail
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
            await initProducer.disconnect();
            console.log(`🔹 [Kafka Log] Đã gửi thông báo khởi tạo hệ thống thành công.`);
        } catch (kafkaErr) {
            console.error(`❌ [Kafka Log] Không thể gửi log khởi tạo:`, kafkaErr.message);
        }
    });
}

module.exports = { initSearchServer };