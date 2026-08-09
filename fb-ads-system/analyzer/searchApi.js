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
    const ELASTIC_INDEX = 'fb_ads_analyzer';

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
                date_from,
                date_to,
                min_score,
                max_score,
                level,
                estimated_spend,
                min_trending_score,
                funnel,
                scaling_level,
                sort_by = 'score',
                sort_order = -1
            } = req.query;

            const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
            const limit = Math.min(
                Math.max(parseInt(req.query.limit, 10) || 100, 1),
                100
            );
            const skip = (page - 1) * limit;

            const query = {};

            // =========================================================
            // 1. SEARCH TEXT BẰNG ELASTICSEARCH
            // =========================================================
            if (text) {
                const keyword = Array.isArray(text)
                    ? text
                        .map(x => String(x).trim())
                        .filter(Boolean)
                        .join(" ")
                    : String(text).trim();

                if (keyword) {
                    try {
                        /*
                         * QUAN TRỌNG:
                         * Không lấy ES _id nữa.
                         *
                         * Lấy ad_archive_id từ _source để đảm bảo
                         * đúng field MongoDB đang query.
                         */
                        const esLimit = Math.min(
                            Math.max(page * limit * 5, 100),
                            5000
                        );

                        const searchQuery = {
                            bool: {
                                should: [
                                    // Field tổng hợp nếu index có field này
                                    {
                                        match: {
                                            full_text_search: {
                                                query: keyword,
                                                boost: 5
                                            }
                                        }
                                    },

                                    // Field thực tế đang có trong data
                                    {
                                        match: {
                                            normalized_text: {
                                                query: keyword,
                                                boost: 4
                                            }
                                        }
                                    },

                                    {
                                        match: {
                                            text: {
                                                query: keyword,
                                                boost: 3
                                            }
                                        }
                                    },

                                    {
                                        match: {
                                            page_name: {
                                                query: keyword,
                                                boost: 2
                                            }
                                        }
                                    }
                                ],
                                minimum_should_match: 1
                            }
                        };

                        const esResponse = await esClient.search({
                            index: ELASTIC_INDEX,
                            query: searchQuery,

                            // Không cần lấy toàn bộ document
                            _source: ['ad_archive_id'],

                            size: esLimit,

                            track_total_hits: false
                        });

                        const hits = esResponse.hits?.hits || [];

                        /*
                         * Lấy ad_archive_id từ _source
                         *
                         * KHÔNG:
                         * hits.map(x => x._id)
                         */
                        const adIdsFromEs = [
                            ...new Set(
                                hits
                                    .map(hit => hit?._source?.ad_archive_id)
                                    .filter(Boolean)
                                    .map(String)
                            )
                        ];

                        console.log(
                            `[ADS SEARCH] keyword="${keyword}" | ES hits=${hits.length} | Mongo IDs=${adIdsFromEs.length}`
                        );

                        /*
                         * ES có hit nhưng document không có
                         * ad_archive_id => index đang thiếu field này.
                         */
                        if (!adIdsFromEs.length) {
                            return res.json({
                                success: true,
                                pagination: {
                                    total: 0,
                                    page,
                                    limit,
                                    pages: 0
                                },
                                data: []
                            });
                        }

                        query.ad_archive_id = {
                            $in: adIdsFromEs
                        };

                    } catch (esError) {
                        console.error(
                            '[ADS SEARCH] Elasticsearch Error:',
                            esError
                        );

                        return res.status(500).json({
                            success: false,
                            message: 'Lỗi khi tìm kiếm trên Elasticsearch.',
                            error: esError.message
                        });
                    }
                }
            }

            // =========================================================
            // 2. FILTER COUNTRY
            // =========================================================
            if (country) {
                query.platforms = String(country).toUpperCase();
            }

            // =========================================================
            // 3. FILTER DATE
            // =========================================================
            if (date_from || date_to) {
                query.start_date = {};

                if (date_from) {
                    const from = new Date(date_from);

                    if (!Number.isNaN(from.getTime())) {
                        query.start_date.$gte = Math.floor(
                            from.getTime() / 1000
                        );
                    }
                }

                if (date_to) {
                    const to = new Date(date_to);

                    if (!Number.isNaN(to.getTime())) {
                        query.start_date.$lte = Math.floor(
                            to.getTime() / 1000
                        );
                    }
                }

                // Nếu date không hợp lệ thì bỏ filter
                if (Object.keys(query.start_date).length === 0) {
                    delete query.start_date;
                }
            }

            // =========================================================
            // 4. FILTER SCORE
            // =========================================================
            const minScore = Number(min_score);
            const maxScore = Number(max_score);

            if (
                Number.isFinite(minScore) ||
                Number.isFinite(maxScore)
            ) {
                query.score = {};

                if (Number.isFinite(minScore)) {
                    query.score.$gte = minScore;
                }

                if (Number.isFinite(maxScore)) {
                    query.score.$lte = maxScore;
                }
            }

            // =========================================================
            // 5. FILTER LEVEL
            // =========================================================
            if (level) {
                const levels = String(level)
                    .split(',')
                    .map(x => x.trim().toUpperCase())
                    .filter(Boolean);

                if (levels.length) {
                    query.level = {
                        $in: levels
                    };
                }
            }

            // =========================================================
            // 6. FILTER ESTIMATED SPEND
            // =========================================================
            if (estimated_spend) {
                const spendLevels = String(estimated_spend)
                    .split(',')
                    .map(x => x.trim().toUpperCase())
                    .filter(Boolean);

                if (spendLevels.length) {
                    query.estimated_spend = {
                        $in: spendLevels
                    };
                }
            }

            // =========================================================
            // 7. FILTER TRENDING SCORE
            // =========================================================
            const minTrendingScore = Number(min_trending_score);

            if (Number.isFinite(minTrendingScore)) {
                query.trending_score = {
                    $gte: minTrendingScore
                };
            }

            // =========================================================
            // 8. FILTER FUNNEL
            // =========================================================
            if (funnel) {
                const funnels = String(funnel)
                    .split(',')
                    .map(x => x.trim().toUpperCase())
                    .filter(Boolean);

                if (funnels.length) {
                    query.funnel = {
                        $in: funnels
                    };
                }
            }

            // =========================================================
            // 9. FILTER SCALING LEVEL
            // =========================================================
            if (scaling_level) {
                const scalingLevels = String(scaling_level)
                    .split(',')
                    .map(x => x.trim().toUpperCase())
                    .filter(Boolean);

                if (scalingLevels.length) {
                    query.scaling_level = {
                        $in: scalingLevels
                    };
                }
            }

            // =========================================================
            // 10. COUNT
            // =========================================================
            const total =
                Object.keys(query).length === 0
                    ? await adsCol.estimatedDocumentCount()
                    : await adsCol.countDocuments(query);

            // =========================================================
            // 11. SORT
            // =========================================================
            const sortDirection =
                Number(sort_order) === 1 ? 1 : -1;

            /*
             * Whitelist field sort để tránh nhận field linh tinh
             */
            const allowedSortFields = new Set([
                'score',
                'trending_score',
                'start_date',
                'seen_count',
                'analyzed_at',
                'first_seen',
                'last_seen',
                'scaling_score'
            ]);

            const safeSortField = allowedSortFields.has(String(sort_by))
                ? String(sort_by)
                : 'score';

            const sortOptions = {
                [safeSortField]: sortDirection
            };

            // =========================================================
            // 12. QUERY MONGODB
            // =========================================================
            const results = await adsCol
                .find(query)
                .project({
                    ad_archive_id: 1,
                    page_name: 1,
                    page_like_count: 1,

                    // Giữ nguyên text đầy đủ nếu Mongo driver hỗ trợ
                    text: 1,

                    link: 1,
                    domain: 1,
                    start_date: 1,
                    seen_count: 1,
                    score: 1,
                    level: 1,
                    trending_score: 1,
                    estimated_spend: 1,
                    scaling_level: 1,
                    funnel: 1,
                    analyzed_at: 1,
                    thumbnail_local: 1,

                    images: {
                        $slice: 1
                    },

                    videos: {
                        $slice: 1
                    }
                })
                .sort(sortOptions)
                .skip(skip)
                .limit(limit)
                .toArray();

            // =========================================================
            // 13. RESPONSE
            // =========================================================
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
            console.error('[ADS SEARCH] Internal Error:', error);

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