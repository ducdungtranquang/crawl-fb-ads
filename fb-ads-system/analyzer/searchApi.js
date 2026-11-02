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

// ===== MINEA-STYLE FILTER PRESETS =====
// Các preset dùng field do analyzer tính sẵn (duration_days, duplicates_count,
// scaling_score, trending_score, filter_signals). Mọi ngưỡng đều chỉnh được
// qua query param — xem FILTER_DEFAULTS. Ngưỡng mặc định phải khớp
// PRESET_THRESHOLDS trong analyzer.js (nơi tính sẵn cờ filter_signals.*).
const FILTER_DEFAULTS = {
    new_max_days: 5,            // Ads mới lên: chạy <= 5 ngày (du di 3–5 ngày)
    scale_min_days: 3,          // Ready to Scale: chạy 3-7 ngày (giai đoạn trước winner)
    scale_max_days: 7,
    scale_min_duplicates: 2,    // ... + creative đã bị duplicate (đang nhân bản)
    scale_min_scaling_score: 0, // 0 = không ràng buộc scaling_score (chỉ sort theo nó)
    winner_min_days: 7,         // Weekly Winner: chạy >= 7 ngày không tắt
    winner_max_days: 0,         // 0 = không giới hạn trên (chạy càng lâu càng win)
    winner_min_duplicates: 2,   // ... + có duplicate (duplicates_count tính cả chính ad)
    winner_min_score: 20,       // ... + (score cao HOẶC trending cao)
    winner_min_trending: 8,
    winner_max_seen_age_days: 7, // ... + còn thấy trong 7 ngày qua (0 = không ràng buộc)
};

// ===== DANH MỤC NGÀNH NGHỀ =====
// value: mã chuẩn trong field `industry` do analyzer ghi (xem
// INDUSTRY_CATEGORIES trong analyzer.js). PHẢI KHỚP value hai bên — đổi ở
// đây thì đổi cả bên analyzer. FE map trực tiếp theo `value`, hiển thị theo
// `label`. Chỉ ad nào được analyzer bản mới (có phân loại ngành) chạy qua mới
// có field `industry`; ad cũ chưa có field sẽ không lọt khi lọc theo ngành.
const INDUSTRIES = [
    { value: 'beauty_health', label: 'Làm đẹp & Sức khỏe' },
    { value: 'electronics', label: 'Điện tử & Công nghệ' },
    { value: 'fashion', label: 'Thời trang' },
    { value: 'home_living', label: 'Nhà cửa & Đời sống' },
    { value: 'food_beverage', label: 'Ẩm thực & Đồ uống' },
    { value: 'real_estate', label: 'Bất động sản' },
    { value: 'finance', label: 'Tài chính & Đầu tư' },
    { value: 'education', label: 'Giáo dục & Khóa học' },
    { value: 'automotive', label: 'Ô tô & Xe cộ' },
    { value: 'pets', label: 'Thú cưng' },
    { value: 'travel', label: 'Du lịch & Nghỉ dưỡng' },
    { value: 'entertainment', label: 'Giải trí & Game' },
    { value: 'gambling', label: 'Cá cược & Casino' },
    { value: 'other', label: 'Khác' },
];
const INDUSTRY_VALUES = new Set(INDUSTRIES.map(i => i.value));

// Điều kiện Mongo cho param `industry` (nhiều giá trị cách nhau bằng dấu
// phẩy, không phân biệt hoa thường). Giá trị lạ bị bỏ qua trong im lặng như
// preset lạ; tất cả lạ thì trả null = không lọc.
function buildIndustryCondition(industryParam) {
    const values = String(industryParam || '').split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
    const valid = [...new Set(values)].filter(v => INDUSTRY_VALUES.has(v));
    if (!valid.length) return null;
    return { industry: { $in: valid } };
}

function numParam(value, fallback) {
    const n = Number(value);
    return Number.isFinite(n) ? n : fallback;
}

function escapeRegExp(s) {
    return String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// Giữ lại để tương thích, nhưng KHÔNG dùng cho lọc page_name/domain
// chính nữa vì regex 'i' không dùng được index (xem lowerPrefixRange).
function prefixRegex(s) {
    return new RegExp(`^${escapeRegExp(String(s).trim())}`, 'i');
}

// Range tiền tố trên field *_lower đã chuẩn hóa (xem analyzer.js):
// Mongo dùng được index cho $gte/$lt trên chuỗi thường, còn regex
// không phân biệt hoa thường thì KHÔNG dùng được index — explain ngày
// 02/11 chứng minh: regex /^Christabel/i + sort start_date, dù đã có
// index {page_name:1, start_date:-1}, vẫn quét 314168 docs trong 50.7s
// để trả 2 ad. Vì vậy page_name/domain KHÔNG lọc bằng regex nữa mà lọc
// bằng range trên page_name_lower/domain_lower (đã toLowerCase sẵn,
// index {page_name_lower:1, start_date:-1}). Chỉ khớp tên bắt đầu bằng
// chuỗi nhập (đã lower), không phân biệt hoa thường, không tìm chuỗi
// con ở giữa tên. Field *_lower phải được migration/analyzer ghi trước
// khi deploy searchApi bản này, nếu không query sẽ trả rỗng.
function lowerPrefixRange(s) {
    const v = String(s).trim().toLowerCase();
    return { $gte: v, $lt: v + '\uffff' };
}

// Bỏ dấu tiếng Việt để bắt thêm trường hợp text đã strip dấu lúc index.
function stripViDiacritics(s) {
    return String(s).normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/đ/g, 'd').replace(/Đ/g, 'D');
}

// Query text search cho ES. strict=true: match_phrase (cụm đúng thứ tự) +
// match operator AND (đủ mọi token) — loại kết quả trúng 1 token lẻ
// ("Đà Nẵng", "Đa năng" khi tìm "Thực phẩm chức năng").
// strict=false: OR như cũ, chỉ dùng fallback khi strict trả về 0 hit.
function buildTextSearchQuery(keyword, strict) {
    const kw = String(keyword).trim();
    const kwAscii = stripViDiacritics(kw);
    const variants = kwAscii !== kw ? [kw, kwAscii] : [kw];
    const should = [];
    for (const v of variants) {
        if (strict) {
            should.push(
                { match_phrase: { full_text_search: { query: v, boost: 10 } } },
                { match_phrase: { normalized_text: { query: v, boost: 8 } } },
                { match: { full_text_search: { query: v, operator: 'and', boost: 5 } } },
                { match: { normalized_text: { query: v, operator: 'and', boost: 4 } } },
                { match: { text: { query: v, operator: 'and', boost: 3 } } },
                { match: { page_name: { query: v, operator: 'and', boost: 2 } } },
            );
        } else {
            should.push(
                { match: { full_text_search: { query: v, boost: 5 } } },
                { match: { normalized_text: { query: v, boost: 4 } } },
                { match: { text: { query: v, boost: 3 } } },
                { match: { page_name: { query: v, boost: 2 } } },
            );
        }
    }
    return { bool: { should, minimum_should_match: 1 } };
}

// Trả về mảng điều kiện Mongo cho preset (kết hợp AND với các filter khác).
// presets: chuỗi 'new_ads,ready_to_scale' (chấp nhận alias new/scale/winner).
// Đường nhanh: dùng cờ filter_signals.* tính sẵn bởi analyzer (1 field
// boolean có index) khi user không tinh chỉnh ngưỡng — đếm/lọc nhanh hơn
// hàng chục giây so với chạy $or + nhiều range mỗi request. Khi có param
// tinh chỉnh thì fallback sang query tính trực tiếp (chậm hơn nhưng đúng
// ngưỡng mới). Cờ chỉ có trên document sau 1 vòng analyzer bản mới.
function hasAnyParam(q, keys) {
    return keys.some(k => q[k] !== undefined);
}

function buildPresetConditions(presets, q) {
    const conds = [];
    const list = String(presets || '').split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
    for (const p of list) {
        if (p === 'new_ads' || p === 'new') {
            if (hasAnyParam(q, ['new_max_days'])) {
                conds.push({
                    duration_days: { $lte: numParam(q.new_max_days, FILTER_DEFAULTS.new_max_days) },
                });
            } else {
                conds.push({ 'filter_signals.is_new_ad': true });
            }
        } else if (p === 'ready_to_scale' || p === 'scale') {
            if (hasAnyParam(q, ['scale_min_days', 'scale_max_days', 'scale_min_duplicates', 'scale_min_scaling_score'])) {
                // Dấu hiệu sắp scale: còn non (3-7 ngày) nhưng creative đã bị
                // duplicate — nhà quảng cáo đang nhân bản. scaling_score chỉ dùng
                // để sort, không ép ngưỡng vì growth_history mỏng thì điểm này
                // hiếm khi lên cao (trừ khi truyền scale_min_scaling_score > 0).
                const cond = {
                    duration_days: {
                        $gte: numParam(q.scale_min_days, FILTER_DEFAULTS.scale_min_days),
                        $lte: numParam(q.scale_max_days, FILTER_DEFAULTS.scale_max_days),
                    },
                    duplicates_count: { $gte: numParam(q.scale_min_duplicates, FILTER_DEFAULTS.scale_min_duplicates) },
                };
                const minScaling = numParam(q.scale_min_scaling_score, FILTER_DEFAULTS.scale_min_scaling_score);
                if (minScaling > 0) cond.scaling_score = { $gte: minScaling };
                conds.push(cond);
            } else {
                conds.push({ 'filter_signals.is_ready_to_scale': true });
            }
        } else if (p === 'weekly_winner' || p === 'winner') {
            if (hasAnyParam(q, ['winner_min_days', 'winner_max_days', 'winner_min_duplicates', 'winner_min_score', 'winner_min_trending', 'winner_max_seen_age_days'])) {
                // Winner kiểu Minea: sống lâu (>= 7 ngày không tắt) + còn chạy +
                // bị duplicate + (điểm tổng cao HOẶC đà tăng mạnh). Dùng OR cho
                // score/trending vì ad win theo kiểu bền hoặc kiểu bùng đều tính.
                const durationCond = { $gte: numParam(q.winner_min_days, FILTER_DEFAULTS.winner_min_days) };
                const maxDays = numParam(q.winner_max_days, FILTER_DEFAULTS.winner_max_days);
                if (maxDays > 0) durationCond.$lte = maxDays;
                const parts = [
                    { duration_days: durationCond },
                    { duplicates_count: { $gte: numParam(q.winner_min_duplicates, FILTER_DEFAULTS.winner_min_duplicates) } },
                    { $or: [
                        { score: { $gte: numParam(q.winner_min_score, FILTER_DEFAULTS.winner_min_score) } },
                        { trending_score: { $gte: numParam(q.winner_min_trending, FILTER_DEFAULTS.winner_min_trending) } },
                    ] },
                ];
                const maxSeenAge = numParam(q.winner_max_seen_age_days, FILTER_DEFAULTS.winner_max_seen_age_days);
                if (maxSeenAge > 0) {
                    // "Còn chạy": khớp 1 trong 3 cách ghi nhận recency (last_seen
                    // tính theo ms như analyzer, filter_signals, hoặc is_active).
                    parts.push({ $or: [
                        { last_seen: { $gte: Date.now() - maxSeenAge * 24 * 3600 * 1000 } },
                        { 'filter_signals.is_recently_seen': true },
                        { is_active: true },
                    ] });
                }
                conds.push({ $and: parts });
            } else {
                conds.push({ 'filter_signals.is_weekly_winner': true });
            }
        }
        // preset lạ: bỏ qua trong im lặng (không vỡ request)
    }
    return conds;
}

// Sort mặc định theo preset khi user không truyền sort_by tường minh.
function resolvePresetSort(presets, hasExplicitSortBy) {
    if (hasExplicitSortBy) return null;
    const list = String(presets || '').split(',').map(s => s.trim().toLowerCase());
    if (list.includes('weekly_winner') || list.includes('winner')) return { field: 'trending_score', order: -1 };
    if (list.includes('ready_to_scale') || list.includes('scale')) return { field: 'scaling_score', order: -1 };
    if (list.includes('new_ads') || list.includes('new')) return { field: 'start_date', order: -1 };
    return null;
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

    // ===== ADS TƯƠNG TỰ (dùng chung cho detail + endpoint /similar) =====
    // Ưu tiên cùng Domain (khớp chính xác, dùng được index domain_score),
    // nếu không có domain thì cùng Page (khớp chính xác, index page_score).
    // Không dùng regex ở đây để tránh quét toàn collection như vụ page_name.
    // LOẠI DUPLICATE: ad tương tự phải KHÁC text chuẩn hóa của ad gốc.
    // Đây đúng định nghĩa duplicate mà analyzer dùng để tính
    // duplicates_count: text = normalized_text || normalize(text)
    // (chữ thường, gộp khoảng trắng, giữ dấu), đếm trùng qua textMap,
    // ưu tiên collation_count của Meta; text rỗng không tính là duplicate
    // nên ad gốc text rỗng thì không loại trừ gì thêm. Nhóm collation của
    // Meta không có ID riêng được lưu, nên không lọc trực tiếp theo nhóm
    // đó được — ad cùng nhóm collation chia sẻ cùng creative text nên
    // trong cùng domain/page, lọc theo normalized_text sẽ loại được
    // chúng. Ad cùng hình nhưng khác text thì KHÔNG bị tính là duplicate
    // theo định nghĩa này nên vẫn được giữ lại.
    function duplicateTextOf(ad) {
        return String(ad.normalized_text || ad.text || '').toLowerCase().replace(/\s+/g, ' ').trim();
    }
    const SIMILAR_PROJECTION = {
        ad_archive_id: 1, ad_library_url: 1, page_name: 1, text: { $substrCP: ["$text", 0, 150] }, score: 1,
        trending_score: 1, estimated_spend_usd: 1, start_date: 1, duplicates_count: 1, duration_days: 1,
        industry: 1, thumbnail_r2: 1, thumbnail_local: 1, images: { $slice: 1 }, videos: { $slice: 1 }
    };

    function buildSimilarQuery(sourceAd, adId) {
        const query = { ad_archive_id: { $ne: adId } };
        if (sourceAd.domain) {
            query.domain = sourceAd.domain;
        } else if (sourceAd.page_name) {
            query.page_name = sourceAd.page_name;
        } else {
            return null; // Không đủ cơ sở để tìm tương tự
        }
        // Loại ad có cùng normalized_text với ad gốc (duplicate), trừ
        // chính ad gốc đã loại ở trên. Doc thiếu field normalized_text
        // sẽ lọt qua $ne — field này do crawler ghi và analyzer giữ qua
        // spread nên bình thường luôn có.
        const dupText = duplicateTextOf(sourceAd);
        if (dupText) query.normalized_text = { $ne: dupText };
        return query;
    }

    async function fetchSimilarAds(sourceAd, adId, page, limit) {
        const query = buildSimilarQuery(sourceAd, adId);
        if (!query) return { similarAds: [], total: 0, pages: 0, has_more: false };
        const total = await adsCol.countDocuments(query);
        const similarAds = await adsCol.find(query)
            .project(SIMILAR_PROJECTION)
            .sort({ score: -1 })
            .skip((page - 1) * limit)
            .limit(limit)
            .toArray();
        return { similarAds, total, pages: Math.ceil(total / limit), has_more: page * limit < total };
    }

    /**
     * 🟢 API 1: TÌM KIẾM QUẢNG CÁO (Nâng cấp bộ lọc chuẩn Minea)
     * Thêm param `filter` (preset): new_ads | ready_to_scale | weekly_winner
     * (nhiều preset cách nhau bằng dấu phẩy, kết hợp AND với các filter khác).
     * Thêm param `industry`: beauty_health,fashion,... (xem INDUSTRIES;
     * nhiều giá trị cách nhau bằng dấu phẩy, kết hợp AND với các filter khác).
     * Ngưỡng chỉnh qua: new_max_days, scale_min_days, scale_max_days,
     * scale_min_duplicates, scale_min_scaling_score, winner_min_score,
     * winner_min_trending, winner_min_duplicates, winner_min_days,
     * winner_max_days, winner_max_seen_age_days.
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
                filter,        // Preset kiểu Minea: new_ads | ready_to_scale | weekly_winner
                industry,      // Lọc theo ngành nghề: beauty_health,fashion,... (xem INDUSTRIES)
            } = req.query;

            const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
            const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 100, 1), 100);
            const skip = (page - 1) * limit;

            const query = {};
            let esRank = null; // thứ hạng ES, dùng sort theo relevance khi không có sort_by tường minh

            // 1. SEARCH TEXT (Elasticsearch)
            // Chất lượng: match_phrase (cụm đúng thứ tự) + match operator AND (đủ
            // mọi token) để "Thực phẩm chức năng" không trả về "Đà Nẵng"/"Đa năng"
            // (trước đây match OR + minimum_should_match 1 nên trúng 1 token "năng"
            // là đã lọt). Strict mà 0 hit thì fallback sang OR như cũ.
            if (text) {
                const keyword = Array.isArray(text) ? text.map(x => String(x).trim()).filter(Boolean).join(" ") : String(text).trim();
                if (keyword) {
                    try {
                        const esLimit = Math.min(Math.max(page * limit * 5, 100), 5000);
                        const esParams = {
                            index: ELASTIC_INDEX,
                            _source: ['ad_archive_id'],
                            size: esLimit,
                            track_total_hits: false
                        };

                        let esResponse = await esClient.search({ ...esParams, query: buildTextSearchQuery(keyword, true) });
                        let hits = esResponse.hits?.hits || [];
                        if (!hits.length) {
                            esResponse = await esClient.search({ ...esParams, query: buildTextSearchQuery(keyword, false) });
                            hits = esResponse.hits?.hits || [];
                        }

                        const adIdsFromEs = [...new Set(hits.map(hit => hit?._source?.ad_archive_id).filter(Boolean).map(String))];

                        if (!adIdsFromEs.length) {
                            return res.json({ success: true, pagination: { total: 0, page, limit, pages: 0 }, data: [] });
                        }
                        esRank = new Map(adIdsFromEs.map((id, i) => [id, i]));
                        query.ad_archive_id = { $in: adIdsFromEs };
                    } catch (esError) {
                        return res.status(500).json({ success: false, message: 'Elasticsearch Error', error: esError.message });
                    }
                }
            }

            // 2. CÁC BỘ LỌC CHUẨN MINEA
            // FIX: country trước đây map nhầm sang `platforms` (tên nền tảng) nên
            // lọc theo quốc gia luôn trả về rỗng. Giờ lọc theo country_mentions
            // (mảng) hoặc latest_detail_country do analyzer ghi.
            if (country) {
                const codeRe = new RegExp(`^${escapeRegExp(String(country).trim())}$`, 'i');
                query.$or = [{ country_mentions: codeRe }, { latest_detail_country: codeRe }];
            }
            if (page_name) query.page_name_lower = lowerPrefixRange(page_name);
            if (domain) query.domain_lower = lowerPrefixRange(domain);
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

            // FIX: level/scaling_level trong DB có prefix emoji (vd '🏆 LEGEND',
            // '🚀 SCALING HARD') nên match chính xác 'LEGEND'/'SCALING' không ra.
            // Đổi sang regex không phân biệt hoa thường.
            if (level) {
                const words = String(level).split(',').map(x => x.trim()).filter(Boolean);
                query.level = { $in: words.map(w => new RegExp(escapeRegExp(w), 'i')) };
            }
            if (estimated_spend) query.estimated_spend = { $in: String(estimated_spend).split(',').map(x => x.trim().toUpperCase()).filter(Boolean) };
            if (Number.isFinite(Number(min_trending_score))) query.trending_score = { $gte: Number(min_trending_score) };
            if (funnel) query.funnel = { $in: String(funnel).split(',').map(x => x.trim().toUpperCase()).filter(Boolean) };
            if (scaling_level) {
                const words = String(scaling_level).split(',').map(x => x.trim()).filter(Boolean);
                query.scaling_level = { $in: words.map(w => new RegExp(escapeRegExp(w), 'i')) };
            }

            // 3. PRESET KIỂU MINEA + LỌC NGÀNH NGHỀ (kết hợp AND với mọi filter trên)
            const presetList = String(filter || '').split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
            const presetConds = buildPresetConditions(filter, req.query);
            const industryCond = buildIndustryCondition(industry);
            const andConds = [...presetConds];
            if (industryCond) andConds.push(industryCond);
            if (andConds.length) query.$and = andConds;

            const total = Object.keys(query).length === 0 ? await adsCol.estimatedDocumentCount() : await adsCol.countDocuments(query);

            const presetSort = resolvePresetSort(filter, req.query.sort_by !== undefined);
            const requestedSort = presetSort ? presetSort.field : (req.query.sort_by || 'score');
            const requestedOrder = presetSort ? presetSort.order : (Number(req.query.sort_order) === 1 ? 1 : -1);
            const allowedSortFields = new Set(['score', 'trending_score', 'start_date', 'seen_count', 'analyzed_at', 'first_seen', 'last_seen', 'scaling_score', 'estimated_spend_usd', 'duplicates_count', 'duration_days', 'estimated_spend_max_usd', 'estimated_spend_min_usd', 'estimated_reach']);
            const safeSortField = allowedSortFields.has(String(requestedSort)) ? String(requestedSort) : 'score';
            const sortOptions = { [safeSortField]: requestedOrder };

            const results = await adsCol.find(query)
                .project({
                    ad_archive_id: 1, ad_library_url: 1, page_name: 1, page_like_count: 1, text: 1, link: 1, domain: 1,
                    start_date: 1, seen_count: 1, score: 1, level: 1, trending_score: 1, scaling_score: 1,
                    estimated_spend_usd: 1, estimated_spend_min_usd: 1, estimated_spend_max_usd: 1,
                    estimated_spend: 1, estimated_reach: 1, estimated_reach_min: 1, estimated_reach_max: 1,
                    duplicates_count: 1, duration_days: 1, cpm_used: 1, filter_signals: 1, industry: 1, thumbnail_r2: 1,
                    scaling_level: 1, funnel: 1, analyzed_at: 1,
                    thumbnail_local: 1, is_active: 1, images: { $slice: 1 }, videos: { $slice: 1 }
                })
                .sort(sortOptions).skip(skip).limit(limit).toArray();

            // Khi search text mà user không chọn sort_by: giữ thứ hạng relevance
            // của ES thay vì để analyzer score quyết định thứ tự.
            if (esRank && req.query.sort_by === undefined) {
                results.sort((a, b) => (esRank.get(String(a.ad_archive_id)) ?? Number.MAX_SAFE_INTEGER) - (esRank.get(String(b.ad_archive_id)) ?? Number.MAX_SAFE_INTEGER));
            }

            return res.json({ success: true, applied_presets: presetList, applied_industries: industryCond ? industryCond.industry.$in : [], pagination: { total, page, limit, pages: Math.ceil(total / limit) }, data: results });
        } catch (error) {
            return res.status(500).json({ success: false, message: 'Internal Server Error', error: error.message });
        }
    });

    /**
     * 🟢 API 1b: SỐ LƯỢNG THEO TỪNG PRESET (để frontend hiển thị badge/đánh giá)
     */
    app.get('/api/ads/filter-stats', async (req, res) => {
        try {
            const total = await adsCol.estimatedDocumentCount();
            const presets = {};
            for (const name of ['new_ads', 'ready_to_scale', 'weekly_winner']) {
                const conds = buildPresetConditions(name, {});
                presets[name] = await adsCol.countDocuments(conds.length ? { $and: conds } : {});
            }
            return res.json({ success: true, data: { total, presets, defaults: FILTER_DEFAULTS, industries: INDUSTRIES } });
        } catch (error) {
            return res.status(500).json({ success: false, message: 'Internal Server Error', error: error.message });
        }
    });

    /**
     * 🟢 API 1c: DANH MỤC NGÀNH NGHỀ (để FE map filter, không cần hardcode)
     */
    app.get('/api/ads/industries', async (req, res) => {
        return res.json({ success: true, data: INDUSTRIES });
    });

    /**
     * 🟢 API 2: LẤY CHI TIẾT QUẢNG CÁO (kèm ads tương tự)
     * Mặc định trả kèm 10 ads tương tự không phải duplicate (cùng domain,
     * nếu không có domain thì cùng page, khác normalized_text của ad gốc,
     * sort score giảm dần) trong `similar_ads` + phân trang
     * trong `similar_pagination`. Muốn lấy thêm: gọi lại detail với
     * ?similar_page=2 (và/hoặc ?similar_limit=N), hoặc gọi endpoint
     * /api/ads/:id/similar?page=2. Tắt kèm similar: ?similar_limit=0
     * hoặc ?include_similar=false. `data` giữ nguyên shape cũ.
     */
    app.get('/api/ads/detail/:id', async (req, res) => {
        try {
            const adId = req.params.id;
            if (!adId) return res.status(400).json({ success: false, message: 'Missing ad archive id parameters.' });

            const adDetail = await adsCol.findOne({ ad_archive_id: adId });
            if (!adDetail) return res.status(404).json({ success: false, message: `Không tìm thấy ID: ${adId}` });

            const includeSimilar = req.query.include_similar !== 'false' && req.query.include_similar !== '0';
            const similarLimit = req.query.similar_limit === undefined
                ? 10
                : Math.min(Math.max(parseInt(req.query.similar_limit, 10) || 0, 0), 20);
            const similarPage = Math.max(parseInt(req.query.similar_page, 10) || 1, 1);
            let similarAds = [];
            let similarPagination = { total: 0, page: similarPage, limit: similarLimit, pages: 0, has_more: false };
            if (includeSimilar && similarLimit > 0) {
                const result = await fetchSimilarAds(adDetail, adId, similarPage, similarLimit);
                similarAds = result.similarAds;
                similarPagination = { total: result.total, page: similarPage, limit: similarLimit, pages: result.pages, has_more: result.has_more };
            }

            return res.json({ success: true, data: adDetail, similar_ads: similarAds, similar_pagination: similarPagination });
        } catch (error) {
            return res.status(500).json({ success: false, message: 'Internal Server Error', error: error.message });
        }
    });

    /**
     * 🟢 API 3: QUẢNG CÁO TƯƠNG TỰ (SIMILAR ADS) - Tính năng Minea
     * Tìm các quảng cáo chung Domain hoặc chung Page để gợi ý, loại
     * ad có cùng normalized_text với ad gốc (duplicate).
     * Phân trang để lấy thêm: ?page=1&limit=10 (mặc định limit 10, tối đa
     * 50). Response giữ `data` là mảng như cũ, thêm `pagination`.
     */
    app.get('/api/ads/:id/similar', async (req, res) => {
        try {
            const adId = req.params.id;
            const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 10, 1), 50);
            const page = Math.max(parseInt(req.query.page, 10) || 1, 1);

            const sourceAd = await adsCol.findOne({ ad_archive_id: adId });
            if (!sourceAd) return res.status(404).json({ success: false, message: 'Ad not found.' });

            const result = await fetchSimilarAds(sourceAd, adId, page, limit);
            return res.json({
                success: true,
                pagination: { total: result.total, page, limit, pages: result.pages, has_more: result.has_more },
                data: result.similarAds
            });
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
                    ad_archive_id: 1, ad_library_url: 1, text: { $substrCP: ["$text", 0, 150] }, score: 1, level: 1,
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
                .project({ ad_archive_id: 1, ad_library_url: 1, text: { $substrCP: ["$text", 0, 150] }, score: 1, level: 1, estimated_spend_usd: 1, images: { $slice: 1 }, videos: { $slice: 1 } })
                .toArray();

            return res.json({ success: true, data: { ...productInfo, top_ads: topAds } });
        } catch (error) {
            return res.status(500).json({ success: false, message: 'Internal Server Error', error: error.message });
        }
    });

    const server = app.listen(PORT, () => {
        console.log(`🖥️  API Search Server đang hoạt động tại cổng: http://localhost:${PORT}`);
    });
    server.on('error', (err) => {
        console.error(`❌ [Search API] Không mở được cổng ${PORT}:`, err.message);
        process.exit(1);
    });
}

module.exports = { initSearchServer, buildPresetConditions, resolvePresetSort, FILTER_DEFAULTS, buildTextSearchQuery, stripViDiacritics, INDUSTRIES, buildIndustryCondition, prefixRegex, escapeRegExp, lowerPrefixRange };

// ===== CHẠY ĐỘC LẬP: node searchApi.js (server search thường trực, pm2) =====
// Analyzer KHÔNG gọi initSearchServer nữa — server tự kết nối Mongo, retry
// có giới hạn rồi thoát lỗi rõ ràng để pm2 restart, không lặp lặng lẽ.
// Chỉ chạy khi file được gọi trực tiếp; khi analyzer (hay test) require thì
// khối này không chạy.
if (require.main === module) {
    const { MongoClient } = require('mongodb');
    const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2';
    const ANALYZER_DB_NAME = 'fb_ads_analyzer';
    const MAX_ATTEMPTS = parseInt(process.env.SEARCH_START_RETRIES || '5', 10);
    const RETRY_MS = parseInt(process.env.SEARCH_START_RETRY_MS || '3000', 10);

    (async () => {
        let lastErr = null;
        for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                console.log(`🔄 [Search API] Đang kết nối MongoDB (lần ${attempt}/${MAX_ATTEMPTS})...`);
                const client = new MongoClient(MONGO_URI);
                await client.connect();
                console.log('💾 [Search API] Kết nối MongoDB thành công!');
                initSearchServer(client.db(ANALYZER_DB_NAME));
                return;
            } catch (err) {
                lastErr = err;
                console.error(`❌ [Search API] Kết nối thất bại (lần ${attempt}/${MAX_ATTEMPTS}):`, err.message);
                if (attempt < MAX_ATTEMPTS) await new Promise((r) => setTimeout(r, RETRY_MS));
            }
        }
        console.error(`❌ [Search API] Không kết nối được MongoDB sau ${MAX_ATTEMPTS} lần — thoát để pm2 restart. Lỗi cuối:`, lastErr && lastErr.message);
        process.exit(1);
    })();
}
