const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');

const app = express();

const PORT = process.env.API_PORT || 5002;
const INTERNAL_SECRET_HEADER = 'x-fb-internal-token';
const INTERNAL_SECRET_VALUE = process.env.INTERNAL_API_TOKEN || 'fb-analyzer-secret-2026';

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
 * Khởi tạo API Server (Đã gỡ bỏ Kafka)
 * @param {import('mongodb').Db} dbInstance 
 */
function initSearchServer(dbInstance) {
    const esClient = new Client({ node: process.env.ELASTICSEARCH_URL || 'http://localhost:9200' });
    const ELASTIC_INDEX = 'fb_ads_analyzer';

    const adsCol = dbInstance.collection('analyzed_ads');
    const productsCol = dbInstance.collection('products');

    const imageStoragePath = path.join(__dirname, '..', '..', 'storage', 'images');
    if (!fs.existsSync(imageStoragePath)) {
        fs.mkdirSync(imageStoragePath, { recursive: true });
    }

    app.use(express.json());
    app.use(cors({
        origin: '*',
        methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
        allowedHeaders: ['Content-Type', 'Authorization', 'x-fb-internal-token'],
        credentials: true
    }));
    app.use('/static/images', express.static(imageStoragePath));
    app.use(checkSpecialHeader);

    /**
     * 🟢 API 1: TÌM KIẾM QUẢNG CÁO (Nâng cấp bộ lọc chuẩn Minea)
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
                page_name,     // Lọc theo tên Page
                domain,        // Lọc theo tên miền
                is_active,     // Lọc theo trạng thái quảng cáo (true/false)
                sort_by = 'score',
                sort_order = -1
            } = req.query;

            const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
            const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 100, 1), 100);
            const skip = (page - 1) * limit;

            const query = {};

            // 1. SEARCH TEXT (Elasticsearch)
            if (text) {
                const keyword = Array.isArray(text) ? text.map(x => String(x).trim()).filter(Boolean).join(" ") : String(text).trim();
                if (keyword) {
                    try {
                        const esLimit = Math.min(Math.max(page * limit * 5, 100), 5000);
                        const searchQuery = {
                            bool: {
                                should: [
                                    { match: { full_text_search: { query: keyword, boost: 5 } } },
                                    { match: { normalized_text: { query: keyword, boost: 4 } } },
                                    { match: { text: { query: keyword, boost: 3 } } },
                                    { match: { page_name: { query: keyword, boost: 2 } } }
                                ],
                                minimum_should_match: 1
                            }
                        };

                        const esResponse = await esClient.search({
                            index: ELASTIC_INDEX,
                            query: searchQuery,
                            _source: ['ad_archive_id'],
                            size: esLimit,
                            track_total_hits: false
                        });

                        const hits = esResponse.hits?.hits || [];
                        const adIdsFromEs = [...new Set(hits.map(hit => hit?._source?.ad_archive_id).filter(Boolean).map(String))];

                        if (!adIdsFromEs.length) {
                            return res.json({ success: true, pagination: { total: 0, page, limit, pages: 0 }, data: [] });
                        }
                        query.ad_archive_id = { $in: adIdsFromEs };
                    } catch (esError) {
                        return res.status(500).json({ success: false, message: 'Elasticsearch Error', error: esError.message });
                    }
                }
            }

            // 2. CÁC BỘ LỌC CHUẨN MINEA
            if (country) query.platforms = String(country).toUpperCase();
            if (page_name) query.page_name = { $regex: String(page_name), $options: 'i' };
            if (domain) query.domain = { $regex: String(domain), $options: 'i' };
            if (is_active !== undefined) query.is_active = is_active === 'true';

            if (date_from || date_to) {
                query.start_date = {};
                if (date_from && !Number.isNaN(new Date(date_from).getTime())) query.start_date.$gte = Math.floor(new Date(date_from).getTime() / 1000);
                if (date_to && !Number.isNaN(new Date(date_to).getTime())) query.start_date.$lte = Math.floor(new Date(date_to).getTime() / 1000);
                if (Object.keys(query.start_date).length === 0) delete query.start_date;
            }

            const minScore = Number(min_score), maxScore = Number(max_score);
            if (Number.isFinite(minScore) || Number.isFinite(maxScore)) {
                query.score = {};
                if (Number.isFinite(minScore)) query.score.$gte = minScore;
                if (Number.isFinite(maxScore)) query.score.$lte = maxScore;
            }

            if (level) query.level = { $in: String(level).split(',').map(x => x.trim().toUpperCase()).filter(Boolean) };
            if (estimated_spend) query.estimated_spend = { $in: String(estimated_spend).split(',').map(x => x.trim().toUpperCase()).filter(Boolean) };
            if (Number.isFinite(Number(min_trending_score))) query.trending_score = { $gte: Number(min_trending_score) };
            if (funnel) query.funnel = { $in: String(funnel).split(',').map(x => x.trim().toUpperCase()).filter(Boolean) };
            if (scaling_level) query.scaling_level = { $in: String(scaling_level).split(',').map(x => x.trim().toUpperCase()).filter(Boolean) };

            const total = Object.keys(query).length === 0 ? await adsCol.estimatedDocumentCount() : await adsCol.countDocuments(query);

            const allowedSortFields = new Set(['score', 'trending_score', 'start_date', 'seen_count', 'analyzed_at', 'first_seen', 'last_seen', 'scaling_score', 'estimated_spend_usd']);
            const safeSortField = allowedSortFields.has(String(sort_by)) ? String(sort_by) : 'score';
            const sortOptions = { [safeSortField]: Number(sort_order) === 1 ? 1 : -1 };

            const results = await adsCol.find(query)
                .project({
                    ad_archive_id: 1, page_name: 1, page_like_count: 1, text: 1, link: 1, domain: 1,
                    start_date: 1, seen_count: 1, score: 1, level: 1, trending_score: 1, estimated_spend_usd: 1,
                    estimated_spend: 1, estimated_reach: 1, scaling_level: 1, funnel: 1, analyzed_at: 1,
                    thumbnail_local: 1, is_active: 1, images: { $slice: 1 }, videos: { $slice: 1 }
                })
                .sort(sortOptions).skip(skip).limit(limit).toArray();

            return res.json({ success: true, pagination: { total, page, limit, pages: Math.ceil(total / limit) }, data: results });
        } catch (error) {
            return res.status(500).json({ success: false, message: 'Internal Server Error', error: error.message });
        }
    });

    /**
     * 🟢 API 2: LẤY CHI TIẾT QUẢNG CÁO
     */
    app.get('/api/ads/detail/:id', async (req, res) => {
        try {
            const adId = req.params.id;
            if (!adId) return res.status(400).json({ success: false, message: 'Missing ad archive id parameters.' });

            const adDetail = await adsCol.findOne({ ad_archive_id: adId });
            if (!adDetail) return res.status(404).json({ success: false, message: `Không tìm thấy ID: ${adId}` });

            return res.json({ success: true, data: adDetail });
        } catch (error) {
            return res.status(500).json({ success: false, message: 'Internal Server Error', error: error.message });
        }
    });

    /**
     * 🟢 API 3: QUẢNG CÁO TƯƠNG TỰ (SIMILAR ADS) - Tính năng Minea
     * Tìm các quảng cáo chung Domain hoặc chung Page để gợi ý.
     */
    app.get('/api/ads/:id/similar', async (req, res) => {
        try {
            const adId = req.params.id;
            const limit = Math.min(parseInt(req.query.limit, 10) || 10, 50);

            const sourceAd = await adsCol.findOne({ ad_archive_id: adId });
            if (!sourceAd) return res.status(404).json({ success: false, message: 'Ad not found.' });

            const query = { ad_archive_id: { $ne: adId } };

            // Ưu tiên tìm cùng Domain, nếu không có domain thì tìm cùng Page
            if (sourceAd.domain) {
                query.domain = sourceAd.domain;
            } else if (sourceAd.page_name) {
                query.page_name = sourceAd.page_name;
            } else {
                return res.json({ success: true, data: [] }); // Không đủ cơ sở để tìm tương tự
            }

            const similarAds = await adsCol.find(query)
                .project({
                    ad_archive_id: 1, page_name: 1, text: { $substrCP: ["$text", 0, 150] }, score: 1,
                    estimated_spend_usd: 1, thumbnail_local: 1, images: { $slice: 1 }, videos: { $slice: 1 }
                })
                .sort({ score: -1 })
                .limit(limit)
                .toArray();

            return res.json({ success: true, data: similarAds });
        } catch (error) {
            return res.status(500).json({ success: false, message: 'Internal Server Error', error: error.message });
        }
    });

    /**
     * 🟢 API 4: THỐNG KÊ CHI TIẾT TỪ MỘT PAGE CỤ THỂ (PAGE PROFILE)
     */
    app.get('/api/pages/:page_name/ads', async (req, res) => {
        try {
            const pageName = req.params.page_name;
            const limit = Math.min(parseInt(req.query.limit, 10) || 20, 100);
            const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
            const skip = (page - 1) * limit;

            const query = { page_name: pageName };
            const total = await adsCol.countDocuments(query);

            const ads = await adsCol.find(query)
                .project({
                    ad_archive_id: 1, text: { $substrCP: ["$text", 0, 150] }, score: 1, level: 1,
                    is_active: 1, start_date: 1, estimated_spend_usd: 1, estimated_reach: 1,
                    images: { $slice: 1 }, videos: { $slice: 1 }
                })
                .sort({ score: -1 })
                .skip(skip)
                .limit(limit)
                .toArray();

            return res.json({
                success: true,
                pagination: { total, page, limit, pages: Math.ceil(total / limit) },
                data: ads
            });
        } catch (error) {
            return res.status(500).json({ success: false, message: 'Internal Server Error', error: error.message });
        }
    });

    /**
     * 🟢 API 5: TÌM KIẾM SẢN PHẨM/DOMAIN
     */
    app.get('/api/products/search', async (req, res) => {
        try {
            const { domain, min_product_score, min_winning_ads, min_total_ads, sort_by = 'product_score', sort_order = -1 } = req.query;
            const query = {};

            if (domain) query.domain = { $regex: domain, $options: 'i' };
            if (min_product_score) query.product_score = { $gte: parseInt(min_product_score, 10) };
            if (min_winning_ads) query.winning_ads = { $gte: parseInt(min_winning_ads, 10) };
            if (min_total_ads) query.total_ads = { $gte: parseInt(min_total_ads, 10) };

            const page = parseInt(req.query.page, 10) || 1;
            const limit = parseInt(req.query.limit, 10) || 50;
            const skip = (page - 1) * limit;

            const total = await productsCol.countDocuments(query);
            const results = await productsCol.find(query)
                .sort({ [sort_by]: parseInt(sort_order, 10) })
                .skip(skip).limit(limit).toArray();

            return res.json({ success: true, pagination: { total, page, limit, pages: Math.ceil(total / limit) }, data: results });
        } catch (error) {
            return res.status(500).json({ success: false, message: 'Internal Server Error', error: error.message });
        }
    });

    /**
     * 🟢 API 6: LẤY CHI TIẾT SẢN PHẨM/DOMAIN
     */
    app.get('/api/products/detail/:domain', async (req, res) => {
        try {
            const { domain } = req.params;
            if (!domain) return res.status(400).json({ success: false, message: 'Domain is required.' });

            const productInfo = await productsCol.findOne({ domain });
            if (!productInfo) return res.status(404).json({ success: false, message: `Product not found.` });

            const topAds = await adsCol.find({ domain })
                .sort({ score: -1 }).limit(20)
                .project({ ad_archive_id: 1, text: { $substrCP: ["$text", 0, 150] }, score: 1, level: 1, estimated_spend_usd: 1, images: { $slice: 1 }, videos: { $slice: 1 } })
                .toArray();

            return res.json({ success: true, data: { ...productInfo, top_ads: topAds } });
        } catch (error) {
            return res.status(500).json({ success: false, message: 'Internal Server Error', error: error.message });
        }
    });

    app.listen(PORT, () => {
        console.log(`🖥️  API Search Server đang hoạt động tại cổng: http://localhost:${PORT}`);
    });
}

module.exports = { initSearchServer };