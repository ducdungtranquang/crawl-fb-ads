const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');

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
    // ===== ELASTICSEARCH CONFIG =====
    const esClient = new Client({ node: process.env.ELASTICSEARCH_URL || 'http://localhost:9200' });
    const ELASTIC_INDEX = 'fb_ads';

    const adsCol = dbInstance.collection('analyzed_ads');
    const productsCol = dbInstance.collection('products');


    // Đường dẫn tới thư mục lưu trữ ảnh được chia sẻ qua Docker volume
    const imageStoragePath = path.join(__dirname, '..', '..', 'storage', 'images');
    if (!fs.existsSync(imageStoragePath)) {
        fs.mkdirSync(imageStoragePath, { recursive: true });
    }

    // ===== ĐĂNG KÝ MIDDLEWARE THEO ĐÚNG THỨ TỰ TRONG NÀY =====
    app.use(express.json());

    // 1. Mở cổng CORS trước
    app.use(cors({
        origin: '*',
        methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
        allowedHeaders: ['Content-Type', 'Authorization', 'x-fb-internal-token'],
        credentials: true
    }));

    // 1.5. Phục vụ các file ảnh tĩnh từ thư mục storage
    app.use('/static/images', express.static(imageStoragePath));

    // 2. Chặn bộ lọc Header ngay sau CORS
    app.use(checkSpecialHeader);

    // 3. Khởi tạo Router xử lý dữ liệu

    /**
         * 🟢 API 1: TÌM KIẾM QUẢNG CÁO (Đã bổ sung Media thu gọn để tăng tốc độ)
         */
    app.get('/api/ads/search', async (req, res) => {
        try {
            const {
                text,
                country,
                date_from, date_to,
                min_score, max_score,
                level,
                estimated_spend, // Mới: Lọc theo mức chi tiêu
                min_trending_score, // Mới: Lọc theo điểm trending
                funnel, // Mới: Lọc theo phễu
                scaling_level // Mới: Lọc theo mức độ scaling
            } = req.query;
            const query = {};

            const page = parseInt(req.query.page, 10) || 1;
            const limit = parseInt(req.query.limit, 10) || 100;
            const skip = (page - 1) * limit;

            // 1. TÍCH HỢP ELASTICSEARCH: Nếu có `text`, tìm kiếm ID trên ES trước
            if (text) {
                try {
                    const { body } = await esClient.search({
                        index: ELASTIC_INDEX,
                        body: {
                            query: {
                                multi_match: {
                                    query: text,
                                    fields: ['text', 'headline', 'description'],
                                    fuzziness: "AUTO" // Cho phép tìm kiếm mờ (gõ sai chính tả)
                                }
                            },
                            _source: false, // Chỉ lấy ID, không cần nội dung
                            size: 10000,    // Giới hạn số lượng ID trả về, có thể điều chỉnh
                            from: 0
                        }
                    });

                    const adIdsFromEs = body.hits.hits.map(hit => hit._id);

                    // Nếu ES không trả về kết quả nào, kết thúc sớm
                    if (adIdsFromEs.length === 0) {
                        return res.json({ success: true, pagination: { total: 0, page, limit, pages: 0 }, data: [] });
                    }
                    query.ad_archive_id = { $in: adIdsFromEs };
                } catch (esError) {
                    console.error("❌ [Elasticsearch Error]", esError.meta ? JSON.stringify(esError.meta.body) : esError);
                    return res.status(500).json({ success: false, message: 'Lỗi khi tìm kiếm trên Elasticsearch.', error: esError.message });
                }
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
            // Hỗ trợ lọc nhiều level, ví dụ: level=WINNER,LEGEND
            if (level) {
                const levels = level.split(',').map(l => l.trim().toUpperCase());
                query.level = { $in: levels };
            }

            // 6. Mới: Filter theo Mức chi tiêu ước tính (có thể chọn nhiều)
            if (estimated_spend) {
                const spendLevels = estimated_spend.split(',').map(s => s.trim().toUpperCase());
                query.estimated_spend = { $in: spendLevels };
            }

            // 7. Mới: Filter theo Điểm trending tối thiểu
            if (min_trending_score) {
                query.trending_score = { $gte: parseInt(min_trending_score, 10) };
            }

            // 8. Mới: Filter theo Phễu Marketing (có thể chọn nhiều)
            if (funnel) {
                const funnels = funnel.split(',').map(f => f.trim().toUpperCase());
                query.funnel = { $in: funnels };
            }

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
                    trending_score: 1, // Bổ sung các trường mới vào projection
                    estimated_spend: 1,
                    scaling_level: 1,
                    funnel: 1,
                    analyzed_at: 1,
                    // Chỉ bốc duy nhất 1 ảnh/video đầu tiên làm thumbnail hiển thị trên Card UI danh sách
                    thumbnail_local: 1, // Trả về đường dẫn ảnh đã lưu
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

    /**
     * 🟢 API 3: TÌM KIẾM SẢN PHẨM/DOMAIN (MỚI)
     * API này cho phép tìm kiếm và lọc các domain dựa trên điểm tổng hợp.
     */
    app.get('/api/products/search', async (req, res) => {
        try {
            const {
                domain,
                min_product_score,
                min_winning_ads,
                min_total_ads,
                sort_by = 'product_score', // Mặc định sắp xếp theo điểm sản phẩm
                sort_order = -1 // Mặc định giảm dần (cao nhất trước)
            } = req.query;

            const query = {};

            if (domain) {
                query.domain = { $regex: domain, $options: 'i' };
            }
            if (min_product_score) {
                query.product_score = { $gte: parseInt(min_product_score, 10) };
            }
            if (min_winning_ads) {
                query.winning_ads = { $gte: parseInt(min_winning_ads, 10) };
            }
            if (min_total_ads) {
                query.total_ads = { $gte: parseInt(min_total_ads, 10) };
            }

            const page = parseInt(req.query.page, 10) || 1;
            const limit = parseInt(req.query.limit, 10) || 50;
            const skip = (page - 1) * limit;

            const total = await productsCol.countDocuments(query);

            const results = await productsCol.find(query)
                .sort({ [sort_by]: parseInt(sort_order, 10) })
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
     * 🟢 API 4: LẤY CHI TIẾT SẢN PHẨM/DOMAIN (MỚI)
     * Trả về thông tin tổng hợp của domain và các quảng cáo tốt nhất liên quan.
     */
    app.get('/api/products/detail/:domain', async (req, res) => {
        try {
            const { domain } = req.params;
            if (!domain) {
                return res.status(400).json({ success: false, message: 'Domain is required.' });
            }

            // 1. Lấy thông tin tổng hợp của sản phẩm
            const productInfo = await productsCol.findOne({ domain });

            if (!productInfo) {
                return res.status(404).json({ success: false, message: `Product with domain '${domain}' not found.` });
            }

            // 2. Lấy 20 quảng cáo có điểm cao nhất thuộc domain này
            const topAds = await adsCol.find({ domain })
                .sort({ score: -1 })
                .limit(20)
                .project({ ad_archive_id: 1, text: { $substrCP: ["$text", 0, 150] }, score: 1, level: 1, images: { $slice: ["$images", 1] }, videos: { $slice: ["$videos", 1] } })
                .toArray();

            return res.json({
                success: true,
                data: {
                    ...productInfo,
                    top_ads: topAds
                }
            });

        } catch (error) {
            return res.status(500).json({ success: false, message: 'Internal Server Error', error: error.message });
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