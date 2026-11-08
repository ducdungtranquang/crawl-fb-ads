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
    winner_min_days: 7,         // Weekly Winner: chạy 7-9 ngày không tắt (cửa sổ FE đang ép)
    winner_max_days: 9,         // 9 = khớp route FE (winner_min_days=7&winner_max_days=9); 0 = không giới hạn trên
    winner_min_duplicates: 2,   // ... + có duplicate (duplicates_count tính cả chính ad)
    winner_min_score: 15,       // ... + (score cao HOẶC trending cao) — nới từ 20/8
    winner_min_trending: 6,
    winner_max_seen_age_days: 14, // ... + còn thấy trong 14 ngày qua (nới từ 7; 0 = không ràng buộc)
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
    { value: 'toys', label: 'Đồ chơi' },
    { value: 'plants', label: 'Hạt giống & Cây giống' },
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

// ===== CHUẨN HÓA THAM SỐ ĐẦU VÀO (FIX 2026-11-03) =====
// Test UI 03/11 cho thấy nhiều bộ lọc đứng một mình trả 0 dù dữ liệu có:
// nguyên nhân ở searchApi gồm (1) FE gửi tên/giá trị tham số khác với tên
// searchApi đang đọc, (2) field mới (countries_lower, page_name_lower,
// industry, creative_rank, is_active) chưa có trên doc cũ nên điều kiện
// equality trả rỗng, (3) field is_active analyzer chưa từng ghi. Bản này
// chấp nhận alias + nhãn tiếng Việt + giá trị thay thế, và fallback về
// field gốc khi field mới chưa có (xem schema probe trong handler).
// Mọi điều kiện fallback đều chậm hơn đường nhanh, nhưng TRẢ ĐÚNG kết
// quả; sau khi migration/analyzer chạy xong, probe sẽ tự chuyển sang
// đường nhanh có index. Không thay đổi ngữ nghĩa của đường nhanh.

function normKey(s) {
    return stripViDiacritics(String(s || '')).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}

// Mã quốc gia từ tên nước FE có thể gửi (vd "Việt Nam", "Hoa Kỳ").
const COUNTRY_NAME_MAP = {
    'viet nam': 'vn', vietnam: 'vn', 'vn': 'vn',
    'hoa ky': 'us', 'united states': 'us', usa: 'us', 'us': 'us', 'my': 'us',
    'nhat ban': 'jp', japan: 'jp', 'jp': 'jp',
    'thai lan': 'th', thailand: 'th', 'th': 'th',
    'philippines': 'ph', 'phi lip pin': 'ph', 'ph': 'ph',
    'malaysia': 'my', 'ma lai xi a': 'my',
    'indonesia': 'id', 'in do ne xi a': 'id', 'id': 'id',
    'singapore': 'sg', 'xin ga po': 'sg', 'sg': 'sg',
    'han quoc': 'kr', 'korea': 'kr', 'south korea': 'kr', 'kr': 'kr',
    'anh': 'gb', 'united kingdom': 'gb', 'uk': 'gb', 'gb': 'gb',
    'duc': 'de', germany: 'de', 'de': 'de',
    'phap': 'fr', france: 'fr', 'fr': 'fr',
    'canada': 'ca', 'ca': 'ca',
    'uc': 'au', australia: 'au', 'au': 'au',
    'brazil': 'br', 'br': 'br',
    'mexico': 'mx', 'mx': 'mx',
    'uae': 'ae', 'cac tieu vuong quoc a rap': 'ae', 'ae': 'ae',
    'dai loan': 'tw', taiwan: 'tw', 'tw': 'tw',
    'hong kong': 'hk', 'hong kong': 'hk', 'hk': 'hk',
    'trung quoc': 'cn', china: 'cn', 'cn': 'cn',
    'an do': 'in', india: 'in', 'in': 'in',
    'tay ban nha': 'es', spain: 'es', 'es': 'es',
    'y': 'it', italy: 'it', 'it': 'it',
    'ha lan': 'nl', netherlands: 'nl', 'nl': 'nl',
    'nga': 'ru', russia: 'ru', 'ru': 'ru',
    'tho nhi ky': 'tr', turkey: 'tr', 'tr': 'tr',
};
const COUNTRY_CODE_TO_NAME = { vn: 'vietnam', us: 'united states', jp: 'japan', th: 'thailand', ph: 'philippines', my: 'malaysia', id: 'indonesia', sg: 'singapore', kr: 'korea', gb: 'united kingdom', de: 'germany', fr: 'france', ca: 'canada', au: 'australia', br: 'brazil', mx: 'mexico', ae: 'uae', tw: 'taiwan', hk: 'hong kong', cn: 'china', in: 'india', es: 'spain', it: 'italy', nl: 'netherlands', ru: 'russia', tr: 'turkey' };

// Các dạng tên/bí danh có thể đã lưu trong countries_lower của doc cũ
// (analyzer cũ ghi nguyên giá trị thô, vd 'usa', 'united states') — đảo
// bảng tên để đường lọc nhanh khớp được cả dạng bí danh, không chỉ mã
// ISO. Dạng có dấu ('việt nam') KHÔNG khớp ở đây; analyzer/migrate bản
// mới chuẩn hóa countries_lower về mã ISO nên doc mới không cần.
// Ngoại lệ: 'my' (từ 'Mỹ') KHÔNG đưa vào bí danh của 'us' — 'my' là mã
// của Malaysia, đưa vào sẽ làm lọc US khớp nhầm doc Malaysia.
const COUNTRY_ALIASES = {};
for (const [aliasName, aliasCode] of Object.entries(COUNTRY_NAME_MAP)) {
    if (aliasName === 'my' && aliasCode === 'us') continue;
    (COUNTRY_ALIASES[aliasCode] = COUNTRY_ALIASES[aliasCode] || []).push(aliasName);
}
for (const [aliasCode, aliasName] of Object.entries(COUNTRY_CODE_TO_NAME)) {
    (COUNTRY_ALIASES[aliasCode] = COUNTRY_ALIASES[aliasCode] || []).push(aliasName);
}
// Mọi giá trị countries_lower có thể tương ứng với 1 mã nước.
function countryMatchValues(code) {
    return [...new Set([code, ...(COUNTRY_ALIASES[code] || [])])];
}

function normalizeCountryCodes(countryParam) {
    const out = [];
    for (const raw of String(countryParam || '').split(',')) {
        const t = String(raw).trim();
        if (!t) continue;
        // Mã ISO 2 chữ đúng dạng (vd 'MY') được ưu tiên trước bảng tên:
        // 'MY' (Malaysia) mà đi qua bảng tên sẽ bị nuốt thành 'us' vì từ
        // 'Mỹ' strip dấu cũng ra 'my'. 'Mỹ' viết hoa thành 'MỸ' (không
        // khớp regex ASCII) nên vẫn rơi xuống bảng tên -> 'us' như cũ.
        const upper = t.toUpperCase();
        if (/^[A-Z]{2}$/.test(upper) && COUNTRY_CODE_TO_NAME[upper.toLowerCase()]) {
            const isoCode = upper.toLowerCase();
            if (!out.includes(isoCode)) out.push(isoCode);
            continue;
        }
        const key = normKey(t);
        // 'Các nước khác' = mọi ad không phải Việt Nam (gồm cả ad chưa có
        // nhãn nước): sentinel 'other', xử lý riêng ở buildCountryCondition.
        if (['other', 'others', 'other countries', 'khac', 'cac nuoc khac', 'non vn', 'not vn', 'notvn'].includes(key)) {
            if (!out.includes('other')) out.push('other');
            continue;
        }
        const code = COUNTRY_NAME_MAP[key] || (/^[a-z]{2}$/.test(key) ? key : null);
        if (!code || code === 'all') continue;
        if (!out.includes(code)) out.push(code);
    }
    return out;
}

// Bí danh tiếng Anh ngắn cho param industry gõ tay trên URL
// (?industry=beauty): FE gửi giá trị chuẩn nên chỉ ảnh hưởng param gõ tay.
const INDUSTRY_ALIASES = { beauty: 'beauty_health', home: 'home_living', food: 'food_beverage', realestate: 'real_estate', 'real estate': 'real_estate', auto: 'automotive', cars: 'automotive', pet: 'pets', game: 'entertainment', casino: 'gambling' };

// Nhãn ngành FE gửi (label hiển thị hoặc slug) -> value chuẩn.
function normalizeIndustryValues(industryParam) {
    const out = [];
    for (const raw of String(industryParam || '').split(',')) {
        const t = String(raw).trim();
        if (!t) continue;
        const lower = t.toLowerCase();
        if (INDUSTRY_VALUES.has(lower)) { if (!out.includes(lower)) out.push(lower); continue; }
        const key = normKey(t);
        const alias = INDUSTRY_ALIASES[lower] || INDUSTRY_ALIASES[key];
        if (alias && !out.includes(alias)) { out.push(alias); continue; }
        const hit = INDUSTRIES.find(i => {
            const lk = normKey(i.label);
            return lk === key || lk.startsWith(key) || key.startsWith(lk) || lk.includes(key);
        });
        if (hit && !out.includes(hit.value)) out.push(hit.value);
    }
    return out;
}

// Nhóm chi tiêu: FE gửi nhãn tiếng Việt ("Rất cao") hoặc tier tiếng Anh
// ("VERY HIGH") hoặc slug. Stored values: LOW/MEDIUM/HIGH/VERY HIGH,
// ngưỡng analyzer: LOW < $50, MEDIUM $50-299, HIGH $300-999, VERY HIGH >= $1000
// (estimated_spend_usd). Nhãn không map được thì bỏ qua (không lọc).
const SPEND_TIER_MAP = {
    'rat cao': 'VERY HIGH', 'very high': 'VERY HIGH', veryhigh: 'VERY HIGH', 'very_high': 'VERY HIGH',
    'cao': 'HIGH', high: 'HIGH',
    'trung binh': 'MEDIUM', medium: 'MEDIUM',
    'thap': 'LOW', low: 'LOW',
};
const SPEND_TIER_RANGES = { 'LOW': [0, 50], 'MEDIUM': [50, 300], 'HIGH': [300, 1000], 'VERY HIGH': [1000, Infinity] };

function normalizeSpendTiers(estimatedSpendParam) {
    const out = [];
    for (const raw of String(estimatedSpendParam || '').split(',')) {
        const key = normKey(raw);
        const tier = SPEND_TIER_MAP[key] || SPEND_TIER_MAP[String(raw).trim().toLowerCase()];
        if (tier && !out.includes(tier)) out.push(tier);
    }
    return out;
}

const FUNNEL_MAP = {
    leadgen: 'LEADGEN', lead: 'LEADGEN', 'khach hang tiem nang': 'LEADGEN', 'lead gen': 'LEADGEN',
    dropship: 'DROPSHIP', 'ban hang': 'DROPSHIP', 'drop ship': 'DROPSHIP',
    ecom: 'ECOM', ecommerce: 'ECOM', 'e commerce': 'ECOM', 'thuong mai dien tu': 'ECOM', 'ban le': 'ECOM',
    unknown: 'UNKNOWN', 'khac': 'UNKNOWN', 'chua ro': 'UNKNOWN', 'other': 'UNKNOWN',
};
function normalizeFunnelValues(funnelParam) {
    const out = [];
    for (const raw of String(funnelParam || '').split(',')) {
        const key = normKey(raw);
        const v = FUNNEL_MAP[key] || FUNNEL_MAP[String(raw).trim().toLowerCase()] || String(raw).trim().toUpperCase();
        if (['LEADGEN', 'DROPSHIP', 'ECOM', 'UNKNOWN'].includes(v) && !out.includes(v)) out.push(v);
    }
    return out;
}

// is_active: FE có thể gửi boolean, số, hoặc nhãn trạng thái.
function parseActiveParam(v) {
    if (v === undefined || v === null || v === '') return undefined;
    if (v === true || v === false) return v;
    const key = normKey(v);
    if (['true', '1', 'yes', 'active', 'running', 'dang chay', 'dang chay'].includes(key)) return true;
    if (['false', '0', 'no', 'inactive', 'paused', 'da tat', 'dung', 'ngung'].includes(key)) return false;
    return undefined;
}

// "Đang chạy" = còn thấy trong 7 ngày qua (last_seen, mili giây — đã kiểm
// chứng 01/11). Analyzer bản cũ KHÔNG ghi field is_active nên lọc bằng
// is_active trực tiếp luôn trả 0; dùng last_seen thì đúng cả trước và sau
// khi analyzer ghi field (cùng quy tắc 7 ngày, xem analyzer.js).
// Inactive CHỈ dùng last_seen < cutoff (không kèm nhánh $exists:false):
// nhánh đó bắt Mongo quét toàn collection để chứng minh field vắng mặt
// nên đếm inactive không bao giờ xong (~70s). Bất biến dựa vào: crawler
// + analyzer luôn ghi last_seen cho mọi doc (saveAdAndDetailDirectly ghi
// first_seen/last_seen khi insert; analyzer spread giữ lại). KIỂM CHỨNG
// trước khi deploy: countDocuments({last_seen: {$exists: false}}) phải
// bằng 0; nếu > 0 thì giữ nhánh $or cũ và backfill last_seen trước.
const ACTIVE_WINDOW_MS = 7 * 24 * 3600 * 1000;

// Ad chạy > 35 ngày thì KHÔNG tính là "đang vít mạnh" nữa (analyzer chặn
// điểm scaling lưu trữ ở 11; xem cappedScalingScore bên analyzer.js).
// Guard ở API cho filter có hiệu lực ngay trước lượt FULL: duration_days
// là số ngày chạy (PR #18 ghi run-length; doc cũ là tuổi stale — chặn
// thừa ad cũ cũng đúng vì ad cũ không còn "đang" vít nữa).
const SCALING_HARD_MAX_RUN_DAYS = 35;

// Ad chết cũ (không thấy > 30 ngày) là vô giá trị: ẩn khỏi mọi list
// (search, similar, page ads) + số đếm preset. Dùng last_seen thay vì cờ
// is_active vì cờ có thể cũ; last_seen mới là sự thật (analyzer/crawler
// luôn ghi last_seen cho mọi doc — đã kiểm chứng missing = 0).
const STALE_AD_MAX_AGE_DAYS = 30;
const STALE_AD_MAX_AGE_MS = STALE_AD_MAX_AGE_DAYS * 24 * 3600 * 1000;
function buildFreshAdCondition(nowMs = Date.now()) {
    return { last_seen: { $gte: nowMs - STALE_AD_MAX_AGE_MS } };
}
function buildActiveCondition(activeBool) {
    const cutoff = Date.now() - ACTIVE_WINDOW_MS;
    if (activeBool === true) return { last_seen: { $gte: cutoff } };
    return { last_seen: { $lt: cutoff } };
}

// Ngày: nhận ISO (2026-10-27), MM/DD/YYYY, hoặc epoch giây/ms (số thuần).
function parseDateParam(v) {
    if (v === undefined || v === null || v === '') return null;
    const s = String(v).trim();
    if (/^\d+$/.test(s)) {
        const n = Number(s);
        return n > 1e12 ? Math.floor(n / 1000) : n; // ms -> giây
    }
    const ms = new Date(s).getTime();
    return Number.isNaN(ms) ? null : Math.floor(ms / 1000);
}

// Sort: FE gửi nhiều dạng tên. Map về field thật trong analyzed_ads.
// 'scaling'/'điểm scaling' -> scaling_score (không phải score).
const SORT_FIELD_ALIASES = {
    scaling_score: 'scaling_score', scalingscore: 'scaling_score', scaling: 'scaling_score',
    'diem scaling': 'scaling_score', 'diemscaling': 'scaling_score',
    score: 'score', diem: 'score',
    trending_score: 'trending_score', trendingscore: 'trending_score', trending: 'trending_score',
    start_date: 'start_date', startdate: 'start_date', newest: 'start_date', 'moi nhat': 'start_date',
    published: 'start_date', 'moi xuat ban nhat': 'start_date',
    duplicates_count: 'duplicates_count', duplicates: 'duplicates_count', copies: 'duplicates_count',
    'nhieu ban sao nhat': 'duplicates_count', 'ban sao': 'duplicates_count',
    duration_days: 'duration_days', duration: 'duration_days', 'chay lau nhat': 'duration_days',
    estimated_spend_usd: 'estimated_spend_usd', spend: 'estimated_spend_usd',
    'chi tieu uoc tinh cao nhat': 'estimated_spend_usd', 'chi tieu': 'estimated_spend_usd',
    estimated_reach: 'estimated_reach', reach: 'estimated_reach',
    'luot tiep can uoc tinh': 'estimated_reach',
    last_seen: 'last_seen', seen_count: 'seen_count', 'luot quet thay': 'last_seen',
    analyzed_at: 'analyzed_at', first_seen: 'first_seen',
};
function normalizeSortField(sortBy) {
    const key = normKey(sortBy);
    if (ALLOWED_SORT_FIELDS.has(String(sortBy))) return String(sortBy);
    return SORT_FIELD_ALIASES[key] || null;
}
function normalizeSortOrder(sortOrder) {
    if (sortOrder === undefined || sortOrder === null || sortOrder === '') return -1;
    const raw = String(sortOrder).trim();
    if (raw === '-1' || raw === 'desc' || raw === 'descending') return -1;
    if (raw === '1' || raw === 'asc' || raw === 'ascending') return 1;
    if (typeof sortOrder === 'number') return sortOrder === 1 ? 1 : -1;
    const key = normKey(sortOrder);
    if (['asc', 'tang dan', 'tang', 'ascend'].includes(key)) return 1;
    if (['desc', 'giam dan', 'giam', 'descend'].includes(key)) return -1;
    return -1;
}

// Lấy tham số đầu tiên khác undefined trong danh sách alias.
function firstParam(q, names) {
    for (const n of names) if (q[n] !== undefined && q[n] !== '') return q[n];
    return undefined;
}

// Chuẩn hóa query: alias -> key chuẩn, giữ nguyên mọi key khác (ngưỡng
// preset, tuning params, product_first...). Handler chỉ đọc key chuẩn.
function normalizeQueryParams(raw) {
    const q = { ...raw };
    const text = firstParam(raw, ['text', 'q', 'keyword', 'search', 'search_text', 'query']);
    if (text !== undefined) q.text = text;
    const country = firstParam(raw, ['country', 'countries', 'country_code']);
    if (country !== undefined) q.country = country;
    // NOTE 2026-11-04: 'page' KHÔNG được làm alias của page_name — 'page'
    // là số trang phân trang, route FE luôn gửi page=1 nên alias cũ biến
    // mọi query thành lọc page_name tiền tố "1" (chỉ còn ad của page bắt
    // đầu bằng "1": vũ trụ 335 ad, VN 19 ad, preset ra 0).
    const pageName = firstParam(raw, ['page_name', 'fanpage', 'pageName']);
    if (pageName !== undefined) q.page_name = pageName;
    const domain = firstParam(raw, ['domain', 'landing_domain', 'site']);
    if (domain !== undefined) q.domain = domain;
    const active = firstParam(raw, ['is_active', 'active', 'status', 'trang_thai']);
    if (active !== undefined) q.is_active = active;
    const df = firstParam(raw, ['date_from', 'start_date_from', 'from_date', 'from', 'published_from']);
    if (df !== undefined) q.date_from = df;
    const dt = firstParam(raw, ['date_to', 'start_date_to', 'to_date', 'to', 'published_to']);
    if (dt !== undefined) q.date_to = dt;
    const minS = firstParam(raw, ['min_score', 'score_min', 'minScore']);
    if (minS !== undefined) q.min_score = minS;
    const maxS = firstParam(raw, ['max_score', 'score_max', 'maxScore']);
    if (maxS !== undefined) q.max_score = maxS;
    const mts = firstParam(raw, ['min_trending_score', 'trending_min', 'minTrendingScore']);
    if (mts !== undefined) q.min_trending_score = mts;
    const lvl = firstParam(raw, ['level', 'ad_level']);
    if (lvl !== undefined) q.level = lvl;
    const spend = firstParam(raw, ['estimated_spend', 'spend', 'spend_level', 'spendLevel']);
    if (spend !== undefined) q.estimated_spend = spend;
    const minSpend = firstParam(raw, ['min_spend_usd', 'min_spend', 'spend_min']);
    if (minSpend !== undefined) q.min_spend_usd = minSpend;
    const maxSpend = firstParam(raw, ['max_spend_usd', 'max_spend', 'spend_max']);
    if (maxSpend !== undefined) q.max_spend_usd = maxSpend;
    const funnel = firstParam(raw, ['funnel', 'funnel_stage']);
    if (funnel !== undefined) q.funnel = funnel;
    const scaling = firstParam(raw, ['scaling_level', 'scaling', 'scalingLevel']);
    if (scaling !== undefined) q.scaling_level = scaling;
    const filter = firstParam(raw, ['filter', 'preset', 'strategy', 'filter_type']);
    if (filter !== undefined) q.filter = filter;
    const industry = firstParam(raw, ['industry', 'category', 'nganh']);
    if (industry !== undefined) q.industry = industry;
    const sortBy = firstParam(raw, ['sort_by', 'sort', 'order_by', 'sortBy']);
    if (sortBy !== undefined) q.sort_by = sortBy;
    const sortOrder = firstParam(raw, ['sort_order', 'order', 'sortOrder', 'direction']);
    if (sortOrder !== undefined) q.sort_order = sortOrder;
    const page = firstParam(raw, ['page', 'page_number']);
    if (page !== undefined) q.page = page;
    const limit = firstParam(raw, ['limit', 'page_size', 'per_page', 'pageSize']);
    if (limit !== undefined) q.limit = limit;
    return q;
}

// Preset từ nhãn FE ("Ads mới lên", "Sẵn sàng Scale", "Weekly Winner",
// "Tất cả Ads") -> tên preset chuẩn. "Tất cả" = không preset.
function normalizePresetNames(filterParam) {
    const out = [];
    for (const raw of String(filterParam || '').split(',')) {
        const key = normKey(raw);
        if (['new_ads', 'new', 'ads moi len', 'moi len', 'ad moi', 'new ads'].includes(key)) out.push('new_ads');
        else if (['ready_to_scale', 'scale', 'san sang scale', 'san sang scale', 'ready to scale'].includes(key)) out.push('ready_to_scale');
        else if (['weekly_winner', 'winner', 'weekly winner'].includes(key)) out.push('weekly_winner');
        else if (['tat ca', 'tat ca ads', 'all', 'all ads'].includes(key)) { /* không preset */ }
        else if (String(raw).trim()) out.push(String(raw).trim().toLowerCase());
    }
    return [...new Set(out)];
}

// ===== SCHEMA PROBE (fallback khi field mới chưa có) =====
// Analyzer chạy FULL 1 lượt và ghi đè toàn bộ doc, nên field mới có hay
// không là "tất cả hoặc không": lấy 3 doc mới thấy nhất (last_seen desc,
// có index) là đủ biết. Probe tối đa 5 phút/lần, lỗi thì mặc định
// fallback (an toàn: trả đúng, chậm hơn). Handler truyền probe vào các
// builder; gọi trực tiếp (test/unit) không truyền schema thì mặc định
// đường nhanh như cũ để không vỡ test cũ.
const SCHEMA_PROBE_TTL_MS = 5 * 60 * 1000;
let schemaProbeCache = { at: 0, probe: { lower: true, country: true, industry: true, rank: true, flags: true } };
let schemaProbeInFlight = null;
async function refreshSchemaProbe(adsCol) {
    const now = Date.now();
    if (now - schemaProbeCache.at < SCHEMA_PROBE_TTL_MS) return schemaProbeCache.probe;
    if (schemaProbeInFlight) return schemaProbeInFlight;
    schemaProbeInFlight = (async () => {
        try {
            const docs = await adsCol.find({})
                .project({ page_name_lower: 1, domain_lower: 1, countries_lower: 1, industry: 1, creative_rank: 1, filter_signals: 1, is_group_rep: 1, preset_priority: 1 })
                .sort({ last_seen: -1 }).limit(3).toArray();
            const probe = { lower: true, country: true, industry: true, rank: true, flags: true, groupRep: true, priority: true };
            if (!docs.length) {
                Object.assign(probe, { lower: false, country: false, industry: false, rank: false, flags: false, groupRep: false, priority: false });
            } else {
                probe.lower = docs.every(d => typeof d.page_name_lower === 'string' && typeof d.domain_lower === 'string');
                probe.country = docs.every(d => Array.isArray(d.countries_lower));
                probe.industry = docs.every(d => typeof d.industry === 'string' && d.industry.length > 0);
                probe.rank = docs.every(d => typeof d.creative_rank === 'number');
                probe.flags = docs.every(d => d.filter_signals && typeof d.filter_signals.is_new_ad === 'boolean');
                probe.groupRep = docs.every(d => typeof d.is_group_rep === 'boolean');
                probe.priority = docs.every(d => typeof d.preset_priority === 'number');
            }
            schemaProbeCache = { at: Date.now(), probe };
            return probe;
        } catch (e) {
            console.warn('[Search API] Schema probe lỗi, dùng fallback an toàn:', e.message);
            const probe = { lower: false, country: false, industry: false, rank: false, flags: false, groupRep: false, priority: false };
            schemaProbeCache = { at: Date.now(), probe };
            return probe;
        } finally {
            schemaProbeInFlight = null;
        }
    })();
    return schemaProbeInFlight;
}

// Từ khóa phân loại ngành (bản sao gọn của INDUSTRY_CATEGORIES trong
// analyzer.js) — CHỈ dùng cho fallback khi doc chưa có field industry.
// Analyzer chạy lại thì fallback không dùng nữa. Từ khóa ngắn (<=3 ký
// tự) phải khớp nguyên từ, cụm dài khớp chuỗi con — cùng quy tắc với
// classifyIndustry sau fix boundary 03/11.
const INDUSTRY_FALLBACK_KEYWORDS = {
    beauty_health: ['mỹ phẩm', 'skincare', 'trị mụn', 'kem', 'serum', 'dược', 'thuốc', 'vitamin', 'beauty', 'health', 'thực phẩm chức năng', 'thực phẩm bảo vệ sức khỏe', 'viên uống', 'collagen', 'giảm cân', 'spa', 'làm đẹp', 'sữa rửa mặt', 'kem chống nắng', 'dầu gội', 'trị nám', 'mọc tóc', 'sữa tắm', 'nước hoa', 'son môi', 'kem dưỡng', 'mặt nạ', 'kem đánh răng', 'bàn chải đánh răng', 'đồ dùng cá nhân', 'ghế massage', 'máy massage', 'massage', 'máy chạy bộ', 'xe đạp tập', 'máy tập', 'yoga'],
    electronics: ['điện thoại', 'iphone', 'samsung', 'laptop', 'macbook', 'tai nghe', 'airpods', 'đồng hồ thông minh', 'smartwatch', 'apple watch', 'máy tính bảng', 'ipad', 'loa bluetooth', 'camera', 'flycam', 'sạc dự phòng', 'bàn phím', 'chuột gaming', 'phụ kiện điện thoại'],
    fashion: ['thời trang', 'quần áo', 'giày', 'túi', 'đồng hồ', 'fashion', 'wear', 'váy', 'đầm', 'áo dài', 'sneaker', 'dép', 'quần jean', 'áo khoác', 'túi xách', 'giày dép', 'mũ', 'khăn choàng', 'thắt lưng', 'kính râm', 'trang sức', 'vòng cổ', 'nhẫn', 'bông tai', 'đồ lót', 'đồ ngủ'],
    home_living: ['nội thất', 'sofa', 'bàn ghế', 'đèn', 'chăn ga', 'gối', 'rèm', 'vệ sinh', 'làm sạch', 'tẩy rửa', 'nước giặt', 'nước rửa chén', 'lau nhà', 'hút bụi', 'máy lọc nước', 'máy lọc không khí', 'đồ gia dụng', 'nhà bếp', 'xoong', 'chảo', 'gia dụng', 'đồ trang trí', 'trang trí', 'decor', 'tranh treo tường', 'gương trang trí', 'thảm trải', 'nến thơm', 'lọ hoa', 'nồi', 'bình giữ nhiệt', 'hộp đựng', 'dụng cụ nhà bếp', 'kệ', 'giá treo'],
    food_beverage: ['nhà hàng', 'quán ăn', 'đồ ăn', 'thực phẩm', 'bánh', 'kẹo', 'cà phê', 'trà sữa', 'đồ uống', 'yến sào', 'hải sản', 'đặc sản', 'ăn vặt', 'thịt bò', 'gạo', 'mật ong', 'hạt dinh dưỡng', 'gia vị', 'nước mắm', 'trà'],
    toys: ['đồ chơi', 'đồ chơi trẻ em', 'lego', 'mô hình', 'búp bê', 'gấu bông', 'xe đồ chơi', 'xếp hình', 'thú nhồi bông', 'robot đồ chơi', 'đồ chơi giáo dục', 'bộ xếp hình'],
    plants: ['hạt giống', 'cây giống', 'cây cảnh', 'cây ăn quả', 'giống cây', 'hạt rau', 'phân bón', 'chậu cây', 'cây trồng', 'rau giống', 'hoa giống'],
    real_estate: ['bất động sản', 'nhà đất', 'căn hộ', 'chung cư', 'đất nền', 'biệt thự', 'nhà phố', 'shophouse', 'cho thuê nhà', 'bán nhà', 'dự án căn hộ'],
    finance: ['tài chính', 'đầu tư', 'crypto', 'forex', 'bank', 'ngân hàng', 'chứng khoán', 'cổ phiếu', 'vay vốn', 'vay tiền', 'bảo hiểm', 'tín dụng', 'coin', 'bitcoin', 'trading', 'lãi suất', 'thẻ tín dụng'],
    education: ['khóa học', 'học tiếng anh', 'đào tạo', 'tuyển sinh', 'luyện thi', 'ielts', 'toeic', 'học online', 'gia sư', 'du học', 'kỹ năng'],
    automotive: ['ô tô', 'xe hơi', 'xe máy', 'xe điện', 'phụ kiện ô tô', 'độ xe', 'lốp xe', 'bảo dưỡng xe', 'mua bán xe', 'xe tải'],
    pets: ['thú cưng', 'thức ăn cho chó', 'thức ăn cho mèo', 'cát mèo', 'phụ kiện thú cưng', 'spa thú cưng', 'chó', 'mèo'],
    travel: ['du lịch', 'tour', 'vé máy bay', 'khách sạn', 'resort', 'nghỉ dưỡng', 'combo du lịch', 'visa', 'hộ chiếu', 'vé tham quan'],
    entertainment: ['game', 'trò chơi', 'phim', 'ca nhạc', 'concert', 'vé xem phim', 'truyện', 'livestream', 'giải trí', 'pubg', 'liên quân', 'nạp game', 'truyện tranh', 'tiểu thuyết', 'webtoon', 'manhwa', 'manga', 'anime', 'hoạt hình', 'truyền hình', 'gameshow', 'chương trình truyền hình', 'show truyền hình', 'phim hoạt hình', 'phim bộ', 'phim lẻ', 'esports', 'streamer', 'ngôn tình', 'xem phim', 'đọc truyện', 'thể thao điện tử', 'game thủ'],
    gambling: ['casino', 'cá cược', 'cá độ', 'nhà cái', 'tài xỉu', 'xóc đĩa', 'bắn cá', 'nổ hũ', 'lô đề', 'đánh bài', 'poker'],
};
function industryFallbackRegex(value) {
    const kws = (INDUSTRY_FALLBACK_KEYWORDS[value] || []).slice().sort((a, b) => b.length - a.length);
    if (!kws.length) return null;
    const parts = kws.map(k => {
        const e = escapeRegExp(k);
        return k.includes(' ') ? e : `(?:^|[^\\p{L}\\p{N}])${e}(?:$|[^\\p{L}\\p{N}])`;
    });
    return new RegExp(parts.join('|'), 'iu');
}
function buildIndustryTextCondition(values) {
    const branches = [];
    for (const v of values) {
        if (v === 'other') continue; // xử lý riêng bên dưới
        const re = industryFallbackRegex(v);
        if (re) branches.push({ $or: [{ normalized_text: re }, { text: re }] });
    }
    if (values.includes('other')) {
        // 'other' = không khớp ngành nào: NOR trên mọi regex ngành, chỉ
        // trên normalized_text để giữ query trong tầm kiểm soát.
        const allRes = Object.keys(INDUSTRY_FALLBACK_KEYWORDS).map(industryFallbackRegex).filter(Boolean);
        branches.push({ $nor: allRes.map(re => ({ normalized_text: re })) });
    }
    if (!branches.length) return null;
    return branches.length === 1 ? branches[0] : { $or: branches };
}


// Điều kiện Mongo cho param `industry` (nhiều giá trị cách nhau bằng dấu
// phẩy; chấp nhận slug lẫn nhãn tiếng Việt, vd "Làm đẹp & Sức khỏe").
// Đường nhanh: equality trên field `industry` do analyzer ghi. Fallback
// khi doc chưa có field industry (schema.industryField=false): regex từ
// khóa trên text — chậm hơn nhưng trả đúng; sau 1 lượt analyzer mới
// thì fallback không dùng nữa. Giá trị lạ bị bỏ qua trong im lặng;
// tất cả lạ thì trả null = không lọc.
function buildIndustryCondition(industryParam, schema) {
    const valid = normalizeIndustryValues(industryParam);
    if (!valid.length) return null;
    const industryField = !schema || schema.industryField !== false;
    if (industryField) return { industry: { $in: valid } };
    return buildIndustryTextCondition(valid);
}

// Điều kiện Mongo cho param `country` (mã quốc gia hoặc tên nước, vd VN,
// "Việt Nam"; nhiều giá trị cách nhau bằng dấu phẩy).
// Đường nhanh: equality trên field chuẩn hóa countries_lower do analyzer
// ghi (xem ghi chú cũ: regex 'i' + $or quét 314168 docs / 50.7s).
// Fallback khi doc chưa có countries_lower (schema.migrated=false):
// equality trên country_mentions / latest_detail_country /
// country_mentions_lower với đủ dạng hoa/thường + tên nước — dùng được
// index đơn field, không regex. 'ALL' không phải mã nước nên bỏ qua.
function buildCountryCondition(countryParam, schema) {
    const codes = normalizeCountryCodes(countryParam);
    if (!codes.length) return null;
    // 'other' = không phải Việt Nam: {countries_lower: {$ne: 'vn'}} khớp
    // cả doc có nhãn nước khác VN lẫn doc chưa có nhãn nước (field rỗng
    // hoặc thiếu — $ne khớp cả trường vắng mặt). Giả định: countries_lower
    // đã chuẩn hóa ISO (migrate PR #7 + analyzer ghi ISO).
    if (codes.includes('other')) {
        const otherCond = { countries_lower: { $ne: 'vn' } };
        const rest = codes.filter((c) => c !== 'other');
        if (!rest.length) return otherCond;
        return { $or: [otherCond, { countries_lower: { $in: rest.flatMap(countryMatchValues) } }] };
    }
    const migrated = !schema || schema.migrated !== false;
    if (migrated) {
        // Khớp cả bí danh đã lưu ở doc cũ ('usa', 'united states', ...):
        // vẫn dùng được index countries_lower, chỉ thêm giá trị $in.
        const values = codes.flatMap(countryMatchValues);
        return { countries_lower: { $in: values } };
    }
    const variants = [];
    for (const c of codes) {
        variants.push(c.toUpperCase(), c.toLowerCase());
        const name = COUNTRY_CODE_TO_NAME[c];
        if (name) variants.push(name);
    }
    const uniq = [...new Set(variants)];
    return {
        $or: [
            { country_mentions_lower: { $in: codes } },
            { country_mentions: { $in: uniq } },
            { latest_detail_country_lower: { $in: codes } },
            { latest_detail_country: { $in: uniq } },
        ],
    };
}

// Điều kiện level: DB lưu kèm prefix emoji ('🏆 LEGEND', '🔥 WINNER',
// '⚡ POTENTIAL', 'REGULAR'). Chấp nhận mã, tên có/không emoji, và nhãn
// tiếng Việt. Regex $in khớp chuỗi con nên 'LEGEND' bắt được '🏆 LEGEND'.
function buildLevelCondition(levelParam) {
    const LEVEL_MAP = {
        legend: 'LEGEND', 'huyen thoai': 'LEGEND',
        winner: 'WINNER', 'nguoi thang': 'WINNER', 'chien thang': 'WINNER',
        potential: 'POTENTIAL', 'tiem nang': 'POTENTIAL',
        regular: 'REGULAR', 'thuong': 'REGULAR', 'binh thuong': 'REGULAR',
    };
    const words = [];
    for (const raw of String(levelParam || '').split(',')) {
        const t = String(raw).trim();
        if (!t) continue;
        const key = normKey(t.replace(/[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}]/gu, ''));
        words.push(LEVEL_MAP[key] || LEVEL_MAP[String(raw).trim().toLowerCase()] || t);
    }
    const uniq = [...new Set(words)].filter(Boolean);
    if (!uniq.length) return null;
    return { level: { $in: uniq.map(w => new RegExp(escapeRegExp(w), 'i')) } };
}

// Điều kiện scaling_level: DB chỉ có 3 giá trị chuẩn ('🚀 SCALING HARD',
// '⚡ SCALING', 'NORMAL') do analyzer ghi. Map thẳng sang giá trị chuẩn
// (equality) thay vì regex 'i' — regex không phân biệt hoa thường KHÔNG
// dùng được index nên đếm/tìm scaling trước đây quét toàn collection
// (~50s, đếm không xong). Giá trị đã chuẩn hóa nên equality cho cùng tập
// kết quả với dữ liệu chuẩn; nhãn lạ chưa map vẫn giữ regex ở nhánh cuối.
// Nhiều giá trị -> $or.
function buildScalingCondition(scalingParam) {
    const branches = [];
    for (const raw of String(scalingParam || '').split(',')) {
        const t = String(raw).trim();
        if (!t) continue;
        const key = normKey(t.replace(/[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}]/gu, ''));
        if (key === 'scaling hard' || key === 'rat cao' || key === 'very high') {
            // Chỉ tính "đang vít mạnh" khi ad chạy <= 35 ngày (xem
            // SCALING_HARD_MAX_RUN_DAYS): ad chạy lâu có điểm scaling tích
            // lũy cao dù hiện tại chỉ chạy lai rai.
            branches.push({ $and: [{ scaling_level: '🚀 SCALING HARD' }, { duration_days: { $lte: SCALING_HARD_MAX_RUN_DAYS } }] });
        } else if (key === 'scaling') {
            branches.push({ scaling_level: '⚡ SCALING' });
        } else if (key === 'cao' || key === 'high') {
            branches.push({ scaling_score: { $gte: 6 } });
        } else if (key === 'trung binh' || key === 'medium' || key === 'trung' || key === 'binh thuong' || key === 'normal') {
            branches.push({ scaling_level: 'NORMAL' });
        } else if (key === 'thap' || key === 'low') {
            branches.push({ scaling_level: 'NORMAL', scaling_score: { $lte: 0 } });
        } else {
            branches.push({ scaling_level: new RegExp(escapeRegExp(t), 'i') });
        }
    }
    if (!branches.length) return null;
    return branches.length === 1 ? branches[0] : { $or: branches };
}

// Điều kiện chi tiêu: tier chuẩn (LOW/MEDIUM/HIGH/VERY HIGH) trên field
// estimated_spend + khoảng số trên estimated_spend_usd nếu FE gửi
// min/max bằng số. Hai điều kiện cùng lúc thì AND (cùng 1 số estimated
// spend nên không mâu thuẫn).
function buildSpendCondition(estimatedSpendParam, minSpendUsd, maxSpendUsd) {
    const conds = [];
    const tiers = normalizeSpendTiers(estimatedSpendParam);
    if (tiers.length) conds.push({ estimated_spend: { $in: tiers } });
    const lo = minSpendUsd !== undefined ? Number(minSpendUsd) : NaN;
    const hi = maxSpendUsd !== undefined ? Number(maxSpendUsd) : NaN;
    if (Number.isFinite(lo) || Number.isFinite(hi)) {
        const range = {};
        if (Number.isFinite(lo)) range.$gte = lo;
        if (Number.isFinite(hi)) range.$lte = hi;
        conds.push({ estimated_spend_usd: range });
    }
    if (!conds.length) return null;
    return conds.length === 1 ? conds[0] : { $and: conds };
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

// Lọc theo domain: khớp domain chính XÁC + mọi subdomain của nó.
// Prefix range một mình KHÔNG đủ: nó chỉ khớp domain BẮT ĐẦU bằng chuỗi
// nhập, nên tìm 'kingsport.vn' bỏ sót 'ghemassage.kingsport.vn' —
// domain thật của ad 1470373528353715 (detail báo domain_ads_count 14
// nhưng search domain=kingsport.vn trả total 0, retest 07/11).
// Nhánh 2 là regex hậu tố '.\<domain>$' (đã escape, chỉ khớp cuối
// chuỗi): bắt subdomain, không bắt 'fakekingsport.vn' hay
// 'kingsport.vn.evil.com'. Nhánh prefix dùng được index domain_lower;
// nhánh regex quét index (chấp nhận được cho query đã lọc). Chuẩn hóa
// input: bỏ scheme, path, 'www.' đầu.
function normalizeDomainInput(s) {
    let d = String(s || '').trim().toLowerCase();
    d = d.replace(/^https?:\/\//, '').split('/')[0];
    d = d.replace(/^\.+/, '');
    if (d.startsWith('www.')) d = d.slice(4);
    return d;
}

function buildDomainCondition(domainParam, schema) {
    const d = normalizeDomainInput(domainParam);
    if (!d) return null;
    const suffix = new RegExp(escapeRegExp('.' + d) + '$', 'i');
    if (schema && schema.lower) {
        return { $or: [{ domain_lower: lowerPrefixRange(d) }, { domain_lower: suffix }] };
    }
    return { $or: [{ domain: prefixRegex(d) }, { domain: suffix }] };
}

// Bỏ dấu tiếng Việt để bắt thêm trường hợp text đã strip dấu lúc index.
function stripViDiacritics(s) {
    return String(s).normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/đ/g, 'd').replace(/Đ/g, 'D');
}

// Giới hạn số ID lấy từ ES cho 1 text search. Sàn cũ 100 ID làm total
// search text bị trần đúng 100 ("ghế massage" trả total 100 ở cả UI lẫn
// API, retest 07/11) dù ES khớp nhiều hơn: total đếm từ countDocuments
// trên $in: esIds nên không bao giờ vượt số ID lấy được. Sàn mới 1000
// (trần 5000): đủ cho trang 1-10 ở limit 100, vẫn rẻ vì ES chỉ trả _id.
// total thật của ES được đọc qua track_total_hits (xem handler) và báo
// bằng cờ total_capped khi vẫn còn hit chưa lấy.
const TEXT_SEARCH_LIMIT_FLOOR = 1000;
const TEXT_SEARCH_LIMIT_MAX = 5000;
function textSearchLimit(page, limit, multiplier) {
    return Math.min(Math.max(page * limit * multiplier, TEXT_SEARCH_LIMIT_FLOOR), TEXT_SEARCH_LIMIT_MAX);
}

// Query text search cho ES. strict=true: match_phrase (cụm đúng thứ tự) +
// match operator AND (đủ mọi token) — loại kết quả trúng 1 token lẻ
// ("Đà Nẵng", "Đa năng" khi tìm "Thực phẩm chức năng").
// strict=false: OR như cũ, chỉ dùng fallback khi strict trả về 0 hit.
function buildTextSearchQuery(keyword, strict, dedupeEs) {
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
    if (dedupeEs) {
        // Lọc đại diện nhóm ngay trong ES: total + thứ hạng + phân trang
        // giữ chính xác. Chỉ bật khi probe mapping ES đã có field này.
        return { bool: { filter: [{ term: { is_group_rep: true } }], should, minimum_should_match: 1 } };
    }
    return { bool: { should, minimum_should_match: 1 } };
}

// Dedupe list: mặc định BẬT — list chỉ hiện 1 ad gốc mỗi nhóm duplicate
// (cùng page + cùng text, do analyzer gom sẵn). Tắt bằng ?dedupe=0 để
// xem tất cả bản sao. Áp dụng cho cả đường Mongo lẫn ES; khi field nhóm
// chưa có trên doc (analyzer cũ) thì bỏ lọc và báo fallback, không trả rỗng.
function dedupeEnabled(q) {
    const v = q.dedupe;
    if (v === undefined) return true;
    return !(v === '0' || v === 'false' || v === 'no' || v === 'off');
}

// Probe mapping ES (cache 5 phút): field is_group_rep chỉ có sau khi
// analyzer bản mới sync ES lần đầu. Chưa có thì KHÔNG lọc ở ES — doc cũ
// thiếu field sẽ bị term filter loại hết, làm text search mất kết quả.
// Khi đó lọc ở Mongo sau khi lấy ID từ ES (over-fetch lớn hơn) + cờ
// dedupe_fallback trong response.
const ES_DEDUPE_PROBE_TTL_MS = 5 * 60 * 1000;
let esDedupeProbeCache = { at: 0, available: false };
let esDedupeProbeInFlight = null;
async function refreshEsDedupeProbe(esClient) {
    const now = Date.now();
    if (now - esDedupeProbeCache.at < ES_DEDUPE_PROBE_TTL_MS) return esDedupeProbeCache.available;
    if (esDedupeProbeInFlight) return esDedupeProbeInFlight;
    esDedupeProbeInFlight = (async () => {
        try {
            const mapping = await esClient.indices.getMapping({ index: 'fb_ads_analyzer' });
            const first = Object.values(mapping || {})[0];
            const props = first?.mappings?.properties || mapping?.mappings?.properties || {};
            const available = Boolean(props.is_group_rep);
            esDedupeProbeCache = { at: Date.now(), available };
            return available;
        } catch (e) {
            console.warn('[Search API] ES mapping probe lỗi, không lọc dedupe ở ES:', e.message);
            esDedupeProbeCache = { at: Date.now(), available: false };
            return false;
        } finally {
            esDedupeProbeInFlight = null;
        }
    })();
    return esDedupeProbeInFlight;
}

// Các field sort được phép (dùng chung cho handler + resolveSearchSort).
const ALLOWED_SORT_FIELDS = new Set(['score', 'trending_score', 'start_date', 'seen_count', 'analyzed_at', 'first_seen', 'last_seen', 'scaling_score', 'estimated_spend_usd', 'duplicates_count', 'duration_days', 'estimated_spend_max_usd', 'estimated_spend_min_usd', 'estimated_reach']);

// Param tinh chỉnh theo từng preset. Một preset dùng đường nhanh cờ khi
// user không truyền param nào, HOẶC mọi param đã truyền đều BẰNG đúng giá
// trị mặc định — trường hợp này hay gặp nhất: route FE luôn ép
// winner_min_days=7&winner_max_days=9, trước đây 2 param đó làm hasAnyParam
// bật đường tính trực tiếp ($or + nhiều range, 5-8s), dù ngưỡng y hệt mặc
// định. So sánh bằng giá trị, không so sự hiện diện, thì URL của FE đi
// đúng đường cờ (1 field boolean có index).
const PRESET_TUNING_KEYS = {
    new_ads: ['new_max_days'],
    ready_to_scale: ['scale_min_days', 'scale_max_days', 'scale_min_duplicates', 'scale_min_scaling_score'],
    weekly_winner: ['winner_min_days', 'winner_max_days', 'winner_min_duplicates', 'winner_min_score', 'winner_min_trending', 'winner_max_seen_age_days'],
};
const PRESET_ALIASES = { new: 'new_ads', scale: 'ready_to_scale', winner: 'weekly_winner' };

function presetUsesFlag(presetName, q) {
    const canonical = PRESET_ALIASES[presetName] || presetName;
    const keys = PRESET_TUNING_KEYS[canonical] || [];
    return keys.every((k) => q[k] === undefined || Number(q[k]) === FILTER_DEFAULTS[k]);
}

// Ưu tiên ad mua sắm/bán sản phẩm (creative_rank 0) lên trước, ad thuần
// giải trí (rank 2) xuống sau, trong các preset. Mặc định BẬT; tắt bằng
// ?product_first=0. Sort tường minh của user vẫn được giữ làm sort thứ 2
// (trong cùng 1 rank thì xếp theo sort đó), không bị ghi đè.
function productFirstEnabled(q) {
    const v = q.product_first;
    if (v === undefined) return true;
    return !(v === '0' || v === 'false' || v === 'no' || v === 'off');
}

// Sort cho /api/ads/search: rank trước (khi có preset + product_first
// VÀ doc đã có creative_rank — probe nói không có rank thì bỏ rank để
// preset không trả rỗng vì điều kiện $exists), rồi tới sort của preset
// hoặc sort tường minh. opts.hasCreativeRank mặc định true để test cũ
// và gọi trực tiếp không vỡ; handler truyền giá trị từ schema probe.
// sort_by lạ -> null -> fallback 'score'; sort_order nhận cả 'asc'/'desc',
// 'tăng dần'/'giảm dần'.
function resolveSearchSort(presets, q, opts) {
    const hasCreativeRank = !opts || opts.hasCreativeRank !== false;
    const presetList = normalizePresetNames(presets);
    const hasPreset = presetList.length > 0;
    const hasPresetPriority = opts && opts.hasPresetPriority === true;
    const presetSort = resolvePresetSort(presets, q.sort_by !== undefined);
    const normalizedField = q.sort_by !== undefined ? normalizeSortField(q.sort_by) : null;
    const requestedSort = presetSort ? presetSort.field : (normalizedField || 'score');
    const requestedOrder = presetSort ? presetSort.order : normalizeSortOrder(q.sort_order);
    const safeSortField = ALLOWED_SORT_FIELDS.has(String(requestedSort)) ? String(requestedSort) : 'score';
    if (hasPreset && (hasCreativeRank || hasPresetPriority) && productFirstEnabled(q)) {
        // Ưu tiên preset trước: preset_priority (sản phẩm lên trước,
        // giải trí xuống sau), rồi creative_rank, rồi sort của preset.
        // Chỉ đưa key nào probe đã xác nhận có trên doc vào sort để
        // không làm preset trả rỗng khi analyzer chưa chạy lại.
        const sortOptions = {};
        if (hasPresetPriority) sortOptions.preset_priority = 1;
        if (hasCreativeRank) sortOptions.creative_rank = 1;
        sortOptions[safeSortField] = requestedOrder;
        return { sortOptions, rankFirst: true };
    }
    return { sortOptions: { [safeSortField]: requestedOrder }, rankFirst: false };
}

// Query ĐẾM: bỏ điều kiện $exists trên field rank (preset_priority /
// creative_rank) khỏi bản sao của query — $exists chặn COUNT_SCAN nên đếm
// phải FETCH toàn bộ doc khớp (đo thực tế trên VM: đếm new_ads có $exists
// 12.2s, bỏ $exists 0.15s, cùng total 82.861). Chỉ bỏ đúng 2 field rank và
// chỉ khi field đó có trong sortOptions (tức probe đã xác nhận field triển
// khai — đúng trạng thái khiến $exists được thêm vào query). Mọi điều kiện
// khác (kể cả $exists khác, $or, $and lồng) giữ nguyên; query gốc không
// đổi nên danh sách trả về giữ nguyên điều kiện $exists. Sau 1 lượt
// analyzer FULL mọi doc đều có cả 2 field nên total đếm == số dòng list;
// doc cũ thiếu field (nếu còn) chỉ làm total cao hơn chút, không ảnh
// hưởng danh sách. Không bao giờ trả về query rỗng: nếu lọc xong $and
// rỗng thì giữ nguyên query gốc.
function stripCountExistsConditions(query, sortOptions) {
    if (!query || typeof query !== 'object' || Array.isArray(query)) return query;
    const rankFields = [];
    if (sortOptions && 'preset_priority' in sortOptions) rankFields.push('preset_priority');
    if (sortOptions && 'creative_rank' in sortOptions) rankFields.push('creative_rank');
    if (!rankFields.length) return query;
    const isExistsTrue = (v) => !!v && typeof v === 'object' && !Array.isArray(v) && Object.keys(v).length === 1 && v.$exists === true;
    const isRankExists = (c) => {
        if (!c || typeof c !== 'object' || Array.isArray(c)) return false;
        const keys = Object.keys(c);
        return keys.length === 1 && rankFields.includes(keys[0]) && isExistsTrue(c[keys[0]]);
    };
    let changed = false;
    const out = { ...query };
    if (Array.isArray(query.$and)) {
        const rest = query.$and.filter((c) => !isRankExists(c));
        if (rest.length === 0) return query;
        if (rest.length !== query.$and.length) { out.$and = rest; changed = true; }
    }
    for (const f of rankFields) {
        if (isExistsTrue(query[f])) { delete out[f]; changed = true; }
    }
    return changed ? out : query;
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

function buildPresetConditions(presets, q, opts) {
    const useFlags = !opts || opts.useFlags !== false;
    const conds = [];
    const list = normalizePresetNames(presets);
    for (const p of list) {
        if (p === 'new_ads' || p === 'new') {
            if (useFlags && presetUsesFlag(p, q)) {
                conds.push({ 'filter_signals.is_new_ad': true });
            } else {
                conds.push({
                    duration_days: { $lte: numParam(q.new_max_days, FILTER_DEFAULTS.new_max_days) },
                });
            }
        } else if (p === 'ready_to_scale' || p === 'scale') {
            if (useFlags && presetUsesFlag(p, q)) {
                conds.push({ 'filter_signals.is_ready_to_scale': true });
            } else {
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
            }
        } else if (p === 'weekly_winner' || p === 'winner') {
            if (useFlags && presetUsesFlag(p, q)) {
                conds.push({ 'filter_signals.is_weekly_winner': true });
            } else {
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
            }
        }
        // preset lạ: bỏ qua trong im lặng (không vỡ request)
    }
    return conds;
}

// Sort mặc định theo preset khi user không truyền sort_by tường minh.
function resolvePresetSort(presets, hasExplicitSortBy) {
    if (hasExplicitSortBy) return null;
    const list = normalizePresetNames(presets);
    if (list.includes('weekly_winner') || list.includes('winner')) return { field: 'trending_score', order: -1 };
    if (list.includes('ready_to_scale') || list.includes('scale')) return { field: 'scaling_score', order: -1 };
    if (list.includes('new_ads') || list.includes('new')) return { field: 'start_date', order: -1 };
    return null;
}

/**
 * Khởi tạo API Server (Đã gỡ bỏ Kafka)
 * @param {import('mongodb').Db} dbInstance 
 */

// ===== ADS TƯƠNG TỰ: TẦNG FALLBACK (module-level để test được) =====
// Tầng theo thứ tự liên quan giảm dần: cùng domain -> cùng page -> cùng
// industry -> mọi ad khác. Mỗi tầng đều loại ad trùng text với ad gốc
// (duplicate theo normalized_text). Từ PR #16: similar thiếu thì trả ít
// hơn (hoặc rỗng) chứ KHÔNG lấp bằng ad trùng text nữa (quyết định
// 06/11). Từ PR #18: $ne còn yêu cầu $exists — $ne ở Mongo khớp cả doc
// THIẾU field, nên ad không có normalized_text từng lọt qua tầng lọc;
// và fillSimilarAds khử trùng thêm theo text/prefix (xem dưới) để cùng
// 1 creative chạy nhiều showroom (chỉ khác địa chỉ/SĐT cuối text) chỉ
// hiện 1 lần.
function similarDupTextOf(ad) {
    return String(ad.normalized_text || ad.text || '').toLowerCase().replace(/\s+/g, ' ').trim();
}

// Độ dài prefix text dùng khử trùng similar: 2 ad cùng 200 ký tự đầu
// thì coi là cùng creative (địa chỉ/SĐT/link showroom nằm ở cuối text).
// Chỉ áp dụng khi text dài hơn 200 ký tự; text ngắn hơn đã khử trùng
// bằng full text. 200 ký tự (~35 từ) đủ dài để 2 creative khác nhau
// khó trùng prefix ngẫu nhiên, nhưng đủ ngắn để bắt các bản showroom
// có body chung ~200+ ký tự rồi mới rẽ sang địa chỉ riêng. Không giới
// hạn theo page: cùng creative chạy ở nhiều page franchise vẫn phải gộp.
const SIMILAR_TEXT_PREFIX_LEN = 200;

// Đếm tổng similar có giới hạn thời gian: union $or trên toàn collection
// có thể quét rất lâu (tầng global khớp gần hết 388k doc) làm treo cả
// response detail/similar. Timeout thì trả null để caller dùng số đã lấy
// được, không treo request. Mặc định 5s: route FE abort sau 10s
// (AbortSignal.timeout(10000) trong app/api/ads/detail/[id]/route.ts),
// nên đếm phải xong sớm hơn để cả response kịp về trước khi FE ngắt.
// Query ĐẾM cho similar: chỉ giữ các điều kiện equality + is_group_rep
// (đều có index) — BỎ $ne trên normalized_text / duplicate_group_key /
// ad_archive_id. Các $ne đó không có index nên countDocuments phải FETCH
// từng doc ứng viên để kiểm tra (tầng global ~337k rep -> nhiều giây),
// trong khi total chỉ cần để phân trang. Đếm này hơi CAO hơn total thật
// (cộng thêm các bản sao cùng text + nhóm của ad gốc + chính ad gốc —
// thường chỉ vài doc); danh sách trả về vẫn khử trùng đúng bằng
// fillSimilarAds. Khi fill đã cạn mọi tầng thì total lấy từ số đã lấy
// (chính xác, xem fetchSimilarAds) nên lệch chỉ xảy ra ở trang còn đủ
// limit.
function buildSimilarCountQuery(tiers) {
    const branches = [];
    for (const t of tiers) {
        const b = {};
        if (t.domain !== undefined) b.domain = t.domain;
        if (t.page_name !== undefined) b.page_name = t.page_name;
        if (t.industry !== undefined) b.industry = t.industry;
        if (t.is_group_rep !== undefined) b.is_group_rep = t.is_group_rep;
        // Giữ cả điều kiện ad tươi (last_seen): total phải khớp list đã
        // loại ad chết cũ, không thì total phình (nhánh global đếm cả ad
        // chết). Nhánh industry có thể FETCH (không có compound index
        // industry+last_seen) nhưng count có timeout 5s + fallback về số
        // đã lấy nên không treo.
        if (t.last_seen !== undefined) b.last_seen = t.last_seen;
        if (Object.keys(b).length) branches.push(b);
    }
    if (!branches.length) return null;
    return branches.length === 1 ? branches[0] : { $or: branches };
}

async function countSimilarTotal(col, unionQuery, maxTimeMS) {
    try {
        return await col.countDocuments(unionQuery, { maxTimeMS: maxTimeMS || 5000 });
    } catch (err) {
        console.warn('[Search API] similar count timeout/lỗi, dùng số đã lấy:', err && err.message);
        return null;
    }
}

function buildSimilarTiers(sourceAd, adId, nowMs = Date.now()) {
    const base = { ad_archive_id: { $ne: adId } };
    const dupText = similarDupTextOf(sourceAd);
    // $exists: true là bắt buộc: $ne khớp cả doc THIẾU normalized_text.
    const noDup = dupText ? { normalized_text: { $ne: dupText, $exists: true } } : {};
    // Chỉ lấy 1 ad cho mỗi creative: is_group_rep != false (true, hoặc
    // thiếu field ở doc cũ) — doc is_group_rep=false là bản sao đã biết.
    // Nhóm của chính ad gốc cũng loại hẳn: xem detail của 1 bản sao thì
    // rep của nó cũng là cùng 1 creative, không được hiện lại.
    const repOnly = { is_group_rep: { $ne: false } };
    const noOwnGroup = sourceAd.duplicate_group_key ? { duplicate_group_key: { $ne: sourceAd.duplicate_group_key } } : {};
    // Ad chết cũ (last_seen > 30 ngày) không vào similar: ảnh hết hạn,
    // vô giá trị dù điểm cao.
    const freshOnly = buildFreshAdCondition(nowMs);
    const tiers = [];
    if (sourceAd.domain) tiers.push({ ...base, domain: sourceAd.domain, ...noDup, ...repOnly, ...noOwnGroup, ...freshOnly });
    if (sourceAd.page_name) tiers.push({ ...base, page_name: sourceAd.page_name, ...noDup, ...repOnly, ...noOwnGroup, ...freshOnly });
    if (sourceAd.industry) tiers.push({ ...base, industry: sourceAd.industry, ...noDup, ...repOnly, ...noOwnGroup, ...freshOnly });
    tiers.push({ ...base, ...noDup, ...repOnly, ...noOwnGroup, ...freshOnly });
    return tiers;
}

// Đếm số ad dùng chung ảnh/video đầu tiên của 1 ad (theo image_keys /
// video_keys đã chuẩn hóa ở analyzer — cùng file re-sign vẫn chung key).
// Đếm GỒM chính ad đó. Lỗi thì trả {} chứ không làm hỏng detail.
async function countMediaUsage(col, ad) {
    const out = {};
    try {
        const imgKey = Array.isArray(ad.image_keys) && ad.image_keys[0];
        const vidKey = Array.isArray(ad.video_keys) && ad.video_keys[0];
        // 2 count chạy song song (trước đây tuần tự): mỗi count khi chưa
        // có index là 1 lần quét collection, detail đỡ đợi gấp đôi.
        const [imgCount, vidCount] = await Promise.all([
            imgKey ? col.countDocuments({ image_keys: imgKey }) : Promise.resolve(null),
            vidKey ? col.countDocuments({ video_keys: vidKey }) : Promise.resolve(null),
        ]);
        if (imgCount !== null) out.image = { key: imgKey, ads_count: imgCount };
        if (vidCount !== null) out.video = { key: vidKey, ads_count: vidCount };
    } catch (err) {
        console.warn('[Search API] media usage lỗi:', err && err.message);
    }
    return out;
}

// Gom đủ (page * limit) ad từ các tầng theo thứ tự, khử trùng theo
// ad_archive_id VÀ duplicate_group_key, rồi cắt đúng cửa sổ trang. col là
// collection Mongo (hoặc fake collection trong test) có
// find().project().sort().limit().
// Khử trùng thêm theo TEXT (PR #18): tầng query chỉ loại text trùng
// VỚI AD GỐC, không loại trùng nhau TRONG list — cùng 1 creative chạy
// nhiều showroom chỉ khác địa chỉ/SĐT cuối text (khác group key vì
// group key dùng full text) nên similar hiện 6× cùng creative (retest
// 07/11: Kingsport "Tâm lý chung cha mẹ..." 6×/15). Vì vậy ở đây giữ
// thêm seenTexts (full text) + seenPrefixes (300 ký tự đầu, chỉ khi
// text dài hơn 300) và loại cả ad có text trùng ad gốc (phòng khi doc
// thiếu normalized_text lọt qua $ne ở tầng query).
async function fillSimilarAds(col, tiers, page, limit, projection, sourceDupText) {
    const seen = new Set();
    const seenGroups = new Set();
    const seenTexts = new Set();
    const seenPrefixes = new Set();
    const picked = [];
    const need = page * limit;
    // Mỗi tầng sau phải loại cả ID đã lấy ở tầng trước: nếu không, tầng
    // sau trả lại đúng những ad điểm cao đã thấy, limit bị tiêu hết mà
    // không thêm được ad mới (lỗi đã bắt bằng test mock).
    const sourceId = tiers[0]?.ad_archive_id?.$ne;
    const sourceText = sourceDupText || tiers[0]?.normalized_text?.$ne || '';
    for (const query of tiers) {
        if (picked.length >= need) break;
        const exclude = [...seen];
        if (sourceId !== undefined) exclude.push(String(sourceId));
        const baseEx = query.ad_archive_id && typeof query.ad_archive_id === 'object' ? query.ad_archive_id : {};
        const q = { ...query, ad_archive_id: { ...baseEx, $nin: exclude } };
        // Lay thua 5 doc moi tang: guard JS ben duoi (loai text trung /
        // prefix trung) co the vut doc da fetch; lay dung need thi tang
        // bi "lang phi" va fill roi xuong tang kem lien quan hon (vd.
        // limit=2 ma doc dau tang page bi loai thi similar toan ad
        // global). Thu them re (query da co index, chi them vai doc).
        const docs = await col.find(q)
            .project(projection)
            .sort({ score: -1 })
            .limit(need - picked.length + 5)
            .toArray();
        for (const d of docs) {
            const key = String(d.ad_archive_id || d._id);
            if (seen.has(key)) continue;
            // Phòng thủ tầng 2: 2 ad cùng duplicate_group_key là cùng 1
            // creative — chỉ giữ ad đầu tiên (điểm cao nhất do sort).
            const gk = d.duplicate_group_key;
            if (gk != null && gk !== '') {
                if (seenGroups.has(gk)) continue;
                seenGroups.add(gk);
            }
            // Khử trùng theo text: cùng full text (kể cả khác page), hoặc
            // cùng 300 ký tự đầu khi text dài (bản sao chỉ khác chi tiết
            // cuối: địa chỉ/SĐT/link showroom). Text của chính ad gốc
            // cũng loại ở đây (doc thiếu normalized_text lọt qua $ne).
            const normText = similarDupTextOf(d);
            const rawText = similarDupTextOf({ normalized_text: '', text: d.text });
            if ((normText && normText === sourceText) || (rawText && rawText === sourceText)) continue;
            if (normText) {
                if (seenTexts.has(normText)) continue;
                seenTexts.add(normText);
                if (normText.length > SIMILAR_TEXT_PREFIX_LEN) {
                    const prefixKey = normText.slice(0, SIMILAR_TEXT_PREFIX_LEN);
                    if (seenPrefixes.has(prefixKey)) continue;
                    seenPrefixes.add(prefixKey);
                }
            }
            seen.add(key);
            picked.push(d);
        }
    }
    // KHÔNG còn phương án cuối lấy ad trùng text: similar thiếu thì trả
    // ít hơn (hoặc rỗng) chứ không hiện duplicate (quyết định 06/11).
    return picked.slice((page - 1) * limit, page * limit);
}

function initSearchServer(dbInstance) {
    const esClient = new Client({ node: process.env.ELASTICSEARCH_URL || 'http://localhost:9200' });
    const ELASTIC_INDEX = 'fb_ads_analyzer';

    const adsCol = dbInstance.collection('analyzed_ads');
    const productsCol = dbInstance.collection('products');
    const winnersCol = dbInstance.collection('product_winners_daily');

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
    // Tầng similar (xem buildSimilarTiers ở module-level): cùng Domain
    // (khớp chính xác, index domain_score) -> cùng Page (index page_score)
    // -> cùng industry -> mọi ad khác, tầng nào cũng loại duplicate.
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
    const SIMILAR_PROJECTION = {
        ad_archive_id: 1, ad_library_url: 1, page_name: 1, text: { $substrCP: ["$text", 0, 150] }, score: 1,
        trending_score: 1, estimated_spend_usd: 1, start_date: 1, duplicates_count: 1, duration_days: 1,
        industry: 1, duplicate_group_size: 1, duplicate_group_key: 1, thumbnail_r2: 1, thumbnail_local: 1, images: { $slice: 1 }, videos: { $slice: 1 }
    };

    async function fetchSimilarAds(sourceAd, adId, page, limit) {
        const tiers = buildSimilarTiers(sourceAd, adId);
        const similarAds = await fillSimilarAds(adsCol, tiers, page, limit, SIMILAR_PROJECTION, similarDupTextOf(sourceAd));
        let total;
        if (similarAds.length < limit) {
            // Fill đã cạn mọi tầng (số đã lấy < need) -> total chính xác
            // bằng số ad tương tự thật, không cần đếm union (đắt).
            total = (page - 1) * limit + similarAds.length;
        } else {
            // Còn đủ limit -> có thể còn trang sau: đếm bằng query rẻ
            // (chỉ equality + is_group_rep, xem buildSimilarCountQuery).
            const countQuery = buildSimilarCountQuery(tiers);
            total = countQuery ? await countSimilarTotal(adsCol, countQuery) : null;
            if (total === null) total = similarAds.length;
        }
        const effectiveTotal = total === 0 && similarAds.length ? similarAds.length : total;
        return { similarAds, total: effectiveTotal, pages: Math.ceil(effectiveTotal / limit), has_more: page * limit < effectiveTotal };
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
            // Chuẩn hóa tham số: chấp nhận alias + nhãn FE (xem
            // normalizeQueryParams). Probe schema 1 lần để biết field mới
            // đã có trên doc chưa — chưa có thì dùng fallback field gốc.
            const q = normalizeQueryParams(req.query);
            const schema = await refreshSchemaProbe(adsCol);
            const dedupeOn = dedupeEnabled(q);
            const esDedupeAvailable = dedupeOn ? await refreshEsDedupeProbe(esClient) : false;
            let dedupeFallback = null; // 'schema' | 'mongo': dedupe bật nhưng chưa lọc được ở tầng đó
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
            } = q;

            const page = Math.max(parseInt(q.page, 10) || 1, 1);
            const limit = Math.min(Math.max(parseInt(q.limit, 10) || 100, 1), 100);
            const skip = (page - 1) * limit;

            const query = {};
            let esRank = null; // thứ hạng ES, dùng sort theo relevance khi không có sort_by tường minh
            let searchFallback = null; // 'mongo' khi ES lỗi/0-hit và phải lọc bằng Mongo
            let esIdsFetched = 0; // số ID ES trả (để phát hiện trang thiếu dòng khi dedupe qua Mongo)
            let esIdsCapped = false;
            let esTotalHits = null; // total thật ES khớp (track_total_hits), null khi không qua ES

            // 1. SEARCH TEXT (Elasticsearch, fallback Mongo khi ES lỗi/0-hit)
            // Chất lượng: match_phrase (cụm đúng thứ tự) + match operator AND (đủ
            // mọi token) để "Thực phẩm chức năng" không trả về "Đà Nẵng"/"Đa năng"
            // (trước đây match OR + minimum_should_match 1 nên trúng 1 token "năng"
            // là đã lọt). Strict mà 0 hit thì fallback sang OR như cũ. ES lỗi
            // hoặc cả 2 lượt đều 0 hit thì fallback sang regex Mongo trên
            // text/page_name thay vì trả 500/rỗng (ES có thể chưa index kịp
            // hoặc đang down — text search vẫn phải chạy).
            if (text) {
                const keyword = Array.isArray(text) ? text.map(x => String(x).trim()).filter(Boolean).join(" ") : String(text).trim();
                if (keyword) {
                    let esIds = null;
                    // NOTE 2026-11-04 (BUG-01): esLimit phải khai báo NGOÀI
                    // khối try — trước đây `const esLimit` nằm trong try nên
                    // dòng `esIds.length >= esLimit` ở ngoài try ném
                    // ReferenceError, mọi text search có hit ES đều HTTP 500.
                    let esLimit = 5000;
                    try {
                        // Dedupe qua Mongo (mapping ES chưa có field): over-fetch
                        // gấp đôi để trang ít bị thiếu dòng sau khi lọc đại diện.
                        const esMultiplier = (dedupeOn && !esDedupeAvailable) ? 10 : 5;
                        esLimit = textSearchLimit(page, limit, esMultiplier);
                        const esParams = {
                            index: ELASTIC_INDEX,
                            _source: ['ad_archive_id'],
                            size: esLimit,
                            track_total_hits: true
                        };

                        let esResponse = await esClient.search({ ...esParams, query: buildTextSearchQuery(keyword, true, esDedupeAvailable) });
                        let hits = esResponse.hits?.hits || [];
                        if (!hits.length) {
                            esResponse = await esClient.search({ ...esParams, query: buildTextSearchQuery(keyword, false, esDedupeAvailable) });
                            hits = esResponse.hits?.hits || [];
                        }
                        // Total thật ES khớp (để báo total_capped): hits.total
                        // là {value, relation} ở ES 8, số ở bản cũ.
                        const esTotal = esResponse.hits?.total;
                        esTotalHits = typeof esTotal === 'number' ? esTotal : (esTotal?.value ?? null);

                        // _source thiếu ad_archive_id thì dùng _id của ES doc.
                        esIds = [...new Set(hits.map(hit => hit?._source?.ad_archive_id || hit?._id).filter(Boolean).map(String))];
                    } catch (esError) {
                        console.warn('[Search API] ES lỗi, fallback Mongo cho text search:', esError.message);
                        searchFallback = 'mongo-after-es-error';
                    }
                    if (esIds && esIds.length) {
                        esRank = new Map(esIds.map((id, i) => [id, i]));
                        esIdsFetched = esIds.length;
                        esIdsCapped = esIds.length >= esLimit;
                        query.ad_archive_id = { $in: esIds };
                    } else {
                        // ES 0 hit hoặc lỗi: regex Mongo (chậm, nhưng đúng).
                        // Dùng cụm đã escape, khớp chuỗi con trên 3 field text.
                        searchFallback = searchFallback || 'mongo';
                        const re = new RegExp(escapeRegExp(keyword), 'i');
                        query.$or = [{ normalized_text: re }, { text: re }, { page_name: re }];
                    }
                }
            }

            // 2. CÁC BỘ LỌC CHUẨN MINEA
            // country: equality trên countries_lower khi field đã có,
            // fallback equality trên field gốc khi chưa (probe quyết định).
            // Giá trị lạ (không map được mã/tên nước) -> bỏ qua, không lọc.
            if (country) {
                const countryCond = buildCountryCondition(country, schema);
                if (countryCond) {
                    if (countryCond.$or && query.$or) {
                        // Text fallback cũng dùng $or: gộp cả hai bằng $and
                        // để không ghi đè nhau.
                        const textOr = query.$or;
                        delete query.$or;
                        query.$and = [{ $or: textOr }, countryCond, ...(query.$and || [])];
                    } else Object.assign(query, countryCond);
                }
            }
            // page_name/domain: range trên *_lower khi field đã có, regex
            // tiền tố trên field gốc khi chưa (probe quyết định).
            if (page_name) {
                if (schema.lower) query.page_name_lower = lowerPrefixRange(page_name);
                else query.page_name = prefixRegex(page_name);
            }
            if (domain) {
                const domainCond = buildDomainCondition(domain, schema);
                if (domainCond) {
                    if (domainCond.$or && query.$or) {
                        // Text fallback cũng dùng $or: gộp cả hai bằng $and
                        // để không ghi đè nhau (cùng mẫu với country).
                        const textOr = query.$or;
                        delete query.$or;
                        query.$and = [{ $or: textOr }, domainCond, ...(query.$and || [])];
                    } else Object.assign(query, domainCond);
                }
            }
            // is_active: analyzer cũ không ghi field này -> lọc bằng
            // last_seen trong 7 ngày (cùng quy tắc analyzer bản mới ghi).
            if (is_active !== undefined) {
                const activeBool = parseActiveParam(is_active);
                if (activeBool !== undefined) Object.assign(query, buildActiveCondition(activeBool));
            }

            if (date_from || date_to) {
                const fromSec = parseDateParam(date_from);
                const toSec = parseDateParam(date_to);
                query.start_date = {};
                if (fromSec !== null) query.start_date.$gte = fromSec;
                if (toSec !== null) query.start_date.$lte = toSec;
                if (Object.keys(query.start_date).length === 0) delete query.start_date;
            }

            const minScore = Number(min_score), maxScore = Number(max_score);
            if (Number.isFinite(minScore) || Number.isFinite(maxScore)) {
                query.score = {};
                if (Number.isFinite(minScore)) query.score.$gte = minScore;
                if (Number.isFinite(maxScore)) query.score.$lte = maxScore;
            }

            // level: regex $in trên giá trị có prefix emoji (xem
            // buildLevelCondition). scaling_level: map nhãn FE về 3 giá trị
            // thật của DB (xem buildScalingCondition).
            if (level) {
                const levelCond = buildLevelCondition(level);
                if (levelCond) Object.assign(query, levelCond);
            }
            const spendCond = buildSpendCondition(estimated_spend, q.min_spend_usd, q.max_spend_usd);
            if (spendCond) {
                if (spendCond.$and) query.$and = [...(query.$and || []), ...spendCond.$and];
                else Object.assign(query, spendCond);
            }
            if (Number.isFinite(Number(min_trending_score))) query.trending_score = { $gte: Number(min_trending_score) };
            if (funnel) {
                const funnelValues = normalizeFunnelValues(funnel);
                if (funnelValues.length) query.funnel = { $in: funnelValues };
            }

            // 3. PRESET KIỂU MINEA + LỌC NGÀNH NGHỀ (kết hợp AND với mọi filter trên)
            const presetList = normalizePresetNames(filter);
            const presetConds = buildPresetConditions(filter, q, { useFlags: schema.flags });
            const industryCond = buildIndustryCondition(industry, schema);
            const industryValues = normalizeIndustryValues(industry);
            const andConds = [...presetConds];
            // Dedupe: chỉ hiện ad đại diện mỗi nhóm (analyzer ghi sẵn).
            // Field nhóm chưa có (analyzer cũ) thì không lọc, báo
            // fallback — list vẫn chạy, chỉ chưa gom nhóm.
            if (dedupeOn) {
                if (schema.groupRep) andConds.push({ is_group_rep: true });
                else dedupeFallback = 'schema';
            }
            // Ad chết cũ (last_seen > 30 ngày) là vô giá trị: ẩn khỏi
            // mọi list search + đếm (total khớp list). Dùng last_seen thay
            // vì cờ is_active vì cờ có thể cũ.
            andConds.push(buildFreshAdCondition());
            if (dedupeOn && esRank && !esDedupeAvailable && schema.groupRep) dedupeFallback = 'mongo';
            if (industryCond) andConds.push(industryCond);
            if (scaling_level) {
                const scalingCond = buildScalingCondition(scaling_level);
                if (scalingCond) andConds.push(scalingCond);
            }
            // Sort theo rank chỉ có nghĩa khi doc đã có creative_rank
            // (probe rank=false -> bỏ rank, không thêm điều kiện $exists
            // làm preset trả rỗng như bản cũ khi analyzer chưa chạy lại).
            const sortInfo = resolveSearchSort(filter, q, { hasCreativeRank: schema.rank, hasPresetPriority: schema.priority });
            if (sortInfo.rankFirst) {
                if ('preset_priority' in sortInfo.sortOptions) andConds.push({ preset_priority: { $exists: true } });
                if ('creative_rank' in sortInfo.sortOptions) andConds.push({ creative_rank: { $exists: true } });
            }
            if (andConds.length) query.$and = [...(query.$and || []), ...andConds];

            // Đếm: countDocuments trên query chậm (regex/$or) có thể vượt
            // timeout của proxy FE. Giới hạn 30s; hết giờ thì vẫn trả data
            // với total=null để FE hiện kết quả thay vì lỗi 500 (FE phải
            // chịu được total=null, xem FE brief). Query đếm bỏ $exists
            // trên field rank (xem stripCountExistsConditions) để Mongo
            // dùng COUNT_SCAN thay vì FETCH toàn bộ doc.
            let total = null;
            let countTimeout = false;
            const countQuery = stripCountExistsConditions(query, sortInfo.sortOptions);
            try {
                total = Object.keys(countQuery).length === 0
                    ? await adsCol.estimatedDocumentCount()
                    : await adsCol.countDocuments(countQuery, { maxTimeMS: 30000 });
            } catch (countErr) {
                console.warn('[Search API] countDocuments timeout/lỗi:', countErr.message);
                countTimeout = true;
            }

            const sortOptions = sortInfo.sortOptions;

            let results = await adsCol.find(query)
                .project({
                    ad_archive_id: 1, ad_library_url: 1, page_name: 1, page_like_count: 1, text: 1, link: 1, domain: 1,
                    start_date: 1, seen_count: 1, score: 1, level: 1, trending_score: 1, scaling_score: 1,
                    estimated_spend_usd: 1, estimated_spend_min_usd: 1, estimated_spend_max_usd: 1,
                    estimated_spend: 1, estimated_spend_source: 1, estimated_spend_capped: 1, country_inferred: 1, estimated_reach: 1, estimated_reach_min: 1, estimated_reach_max: 1,
                    duplicates_count: 1, duration_days: 1, cpm_used: 1, filter_signals: 1, industry: 1, creative_kind: 1, creative_rank: 1, thumbnail_r2: 1, duplicate_group_key: 1, duplicate_group_size: 1, is_group_rep: 1, preset_priority: 1,
                    scaling_level: 1, funnel: 1, analyzed_at: 1,
                    thumbnail_local: 1, is_active: 1, images: { $slice: 1 }, videos: { $slice: 1 }
                })
                .sort(sortOptions).skip(skip).limit(limit).toArray();

            // Khi search text mà user không chọn sort_by: giữ thứ hạng relevance
            // của ES thay vì để analyzer score quyết định thứ tự.
            if (esRank && q.sort_by === undefined) {
                const priorityFirst = sortInfo.rankFirst && 'preset_priority' in sortInfo.sortOptions;
                if (sortInfo.rankFirst) {
                    results.sort((a, b) => (priorityFirst ? (a.preset_priority ?? 1) - (b.preset_priority ?? 1) : 0) || ((a.creative_rank ?? 1) - (b.creative_rank ?? 1)) || ((esRank.get(String(a.ad_archive_id)) ?? Number.MAX_SAFE_INTEGER) - (esRank.get(String(b.ad_archive_id)) ?? Number.MAX_SAFE_INTEGER)));
                } else {
                    results.sort((a, b) => (esRank.get(String(a.ad_archive_id)) ?? Number.MAX_SAFE_INTEGER) - (esRank.get(String(b.ad_archive_id)) ?? Number.MAX_SAFE_INTEGER));
                }
            }

            // Chốt chặn cuối: cùng ad_archive_id không được xuất hiện 2 lần
            // trong 1 trang (doc trùng ID khác kiểu số/chuỗi, hoặc ES +
            // Mongo hợp nhất lệch). Giữ bản đầu tiên theo thứ tự đã sort.
            if (dedupeOn && results.length > 1) {
                const seenIds = new Set();
                results = results.filter((d) => {
                    const rawId = d.ad_archive_id != null ? d.ad_archive_id : d._id;
                    if (rawId == null) return true;
                    const key = String(rawId);
                    if (seenIds.has(key)) return false;
                    seenIds.add(key);
                    return true;
                });
            }

            // Echo để FE debug: bộ lọc nào đã áp dụng thật, giá trị nào bị
            // bỏ (lạ), đường fallback nào đang chạy. Chỉ thêm field, không
            // đổi shape cũ (data/pagination giữ nguyên).
            return res.json({
                success: true,
                applied_presets: presetList,
                applied_industries: industryValues,
                applied_filters: {
                    text: text ? String(text) : null,
                    country: country ? normalizeCountryCodes(country) : [],
                    page_name: page_name ? String(page_name) : null,
                    domain: domain ? String(domain) : null,
                    is_active: is_active !== undefined ? parseActiveParam(is_active) ?? null : null,
                    industry: industryValues,
                    estimated_spend: normalizeSpendTiers(estimated_spend),
                    funnel: funnel ? normalizeFunnelValues(funnel) : [],
                    sort_by: sortInfo.sortOptions,
                    product_first: productFirstEnabled(q),
                    dedupe: dedupeOn,
                },
                schema_fallback: {
                    lower_fields: !schema.lower,
                    country_fields: !schema.country,
                    industry_field: !schema.industry,
                    preset_flags: !schema.flags,
                    creative_rank: !schema.rank,
                    dedupe_group_fields: !schema.groupRep,
                    dedupe_priority: !schema.priority,
                },
                ...(searchFallback ? { search_fallback: searchFallback } : {}),
                ...(dedupeFallback ? { dedupe_fallback: dedupeFallback } : {}),
                ...(dedupeOn && esRank && !esDedupeAvailable && esIdsCapped && results.length < limit ? { dedupe_partial: true } : {}),
                ...(esTotalHits !== null && esTotalHits > esIdsFetched ? { total_capped: true } : {}),
                ...(countTimeout ? { count_timeout: true } : {}),
                pagination: { total, page, limit, pages: total === null ? null : Math.ceil(total / limit) },
                data: results,
            });
        } catch (error) {
            return res.status(500).json({ success: false, message: 'Internal Server Error', error: error.message });
        }
    });

    /**
     * 🟢 API 1b: SỐ LƯỢNG THEO TỪNG PRESET (để frontend hiển thị badge/đánh giá)
     */
    app.get('/api/ads/filter-stats', async (req, res) => {
        try {
            const schema = await refreshSchemaProbe(adsCol);
            // Total + badge preset chỉ đếm ad tươi (last_seen <= 30 ngày)
            // để khớp với list đã ẩn ad chết cũ; total cũ dùng
            // estimatedDocumentCount đếm cả ad chết nên lệch với list.
            const freshCond = buildFreshAdCondition();
            const total = await adsCol.countDocuments(freshCond);
            const presets = {};
            for (const name of ['new_ads', 'ready_to_scale', 'weekly_winner']) {
                const conds = buildPresetConditions(name, {}, { useFlags: schema.flags });
                const countQuery = conds.length ? { $and: [...conds, freshCond] } : { ...freshCond };
                // Badge khớp list đã dedupe: chỉ đếm ad đại diện khi field nhóm đã có.
                if (schema.groupRep) countQuery.is_group_rep = true;
                presets[name] = await adsCol.countDocuments(countQuery);
            }
            return res.json({ success: true, data: { total, presets, defaults: FILTER_DEFAULTS, industries: INDUSTRIES, schema_fallback: { preset_flags: !schema.flags, industry_field: !schema.industry } } });
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

    // Tìm ad theo ad_archive_id (chuỗi, FE có thể gửi số) hoặc _id
    // ObjectId khi FE gửi _id của Mongo. Dùng chung cho detail/similar.
    async function findAdById(id) {
        const sid = String(id);
        const byArchive = await adsCol.findOne({ ad_archive_id: sid });
        if (byArchive) return byArchive;
        // Doc cũ có thể lưu ad_archive_id dạng số (analyzer cũ không ép
        // String) — thử cả dạng số trước khi kết luận không tìm thấy.
        if (/^\d+$/.test(sid)) {
            const byNumeric = await adsCol.findOne({ ad_archive_id: Number(sid) });
            if (byNumeric) return byNumeric;
        }
        let ObjectId = null;
        try { ({ ObjectId } = require('mongodb')); } catch (_) { /* test env không có mongodb thật */ }
        if (ObjectId && /^[0-9a-fA-F]{24}$/.test(sid)) {
            try { return await adsCol.findOne({ _id: new ObjectId(sid) }); } catch (_) { return null; }
        }
        return null;
    }

    /**
     * 🟢 API 2: LẤY CHI TIẾT QUẢNG CÁO (KHÔNG kèm similar mặc định)
     * TÁCH similar khỏi detail (PR #19): detail mặc định chỉ trả `data` +
     * `media_usage` + `duplicate_group` để mở nhanh; similar lấy riêng
     * qua endpoint /api/ads/:id/similar (bất đồng bộ, không chặn modal).
     * Muốn kèm similar trong 1 lần gọi: ?include_similar=true (limit
     * từ ?similar_limit, mặc định 10, tối đa 20) hoặc
     * ?similar_page=2 để lấy trang sau. `data` giữ nguyên shape cũ.
     */
    app.get('/api/ads/detail/:id', async (req, res) => {
        try {
            const adId = req.params.id;
            if (!adId) return res.status(400).json({ success: false, message: 'Missing ad archive id parameters.' });

            const adDetail = await findAdById(adId);
            if (!adDetail) return res.status(404).json({ success: false, message: `Không tìm thấy ID: ${adId}` });

            const mediaUsage = await countMediaUsage(adsCol, adDetail);
            // Mặc định KHÔNG kèm similar (tách riêng để detail mở nhanh);
            // chỉ kèm khi gọi tường minh ?include_similar=true (hoặc =1).
            const includeSimilar = req.query.include_similar === 'true' || req.query.include_similar === '1';
            const similarLimit = req.query.similar_limit === undefined
                ? 10
                : Math.min(Math.max(parseInt(req.query.similar_limit, 10) || 0, 0), 20);
            const similarPage = Math.max(parseInt(req.query.similar_page, 10) || 1, 1);
            let similarAds = [];
            let similarPagination = { total: 0, page: similarPage, limit: similarLimit, pages: 0, has_more: false };
            if (includeSimilar && similarLimit > 0) {
                const result = await fetchSimilarAds(adDetail, adDetail.ad_archive_id, similarPage, similarLimit);
                similarAds = result.similarAds;
                similarPagination = { total: result.total, page: similarPage, limit: similarLimit, pages: result.pages, has_more: result.has_more };
            }

            let duplicateGroup = null;
            if (adDetail.duplicate_group_key) {
                const groupAds = await adsCol.find({ duplicate_group_key: adDetail.duplicate_group_key, ad_archive_id: { $ne: adDetail.ad_archive_id } })
                    .project({ ad_archive_id: 1, ad_library_url: 1, page_name: 1, text: { $substrCP: ['$text', 0, 150] }, score: 1, start_date: 1, thumbnail_r2: 1, images: { $slice: 1 } })
                    .sort({ start_date: 1 }).limit(10).toArray();
                duplicateGroup = { key: adDetail.duplicate_group_key, size: adDetail.duplicate_group_size || groupAds.length + 1, ads: groupAds };
            }

            return res.json({ success: true, data: adDetail, media_usage: mediaUsage, similar_ads: similarAds, similar_pagination: similarPagination, duplicate_group: duplicateGroup });
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

            const sourceAd = await findAdById(adId);
            if (!sourceAd) return res.status(404).json({ success: false, message: 'Ad not found.' });

            const result = await fetchSimilarAds(sourceAd, sourceAd.ad_archive_id, page, limit);
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
            // Dedupe mặc định BẬT (như search): 1 creative chỉ hiện 1 lần
            // — trước đây 20 slot đầu của Kingsport có 7× "{{product.brand}}"
            // + 4× cùng creative, total 972 đếm cả bản sao nên số trang
            // không khớp số creative thật. Tắt bằng ?dedupe=0.
            const dedupeOn = req.query.dedupe !== '0' && req.query.dedupe !== 'false';
            // Loại ad đang xem khỏi chính danh sách của page nó (modal
            // detail truyền ?exclude=<ad_archive_id>).
            const excludeId = req.query.exclude ? String(req.query.exclude).trim() : '';
            const query = { page_name: pageName };
            if (dedupeOn) query.is_group_rep = true;
            if (excludeId) {
                query.ad_archive_id = /^\d+$/.test(excludeId)
                    ? { $nin: [excludeId, Number(excludeId)] }
                    : { $ne: excludeId };
            }
            // Ad chết cũ (last_seen > 30 ngày) không hiện trong list page
            // nữa (ảnh hết hạn, vô giá trị) — cả count lẫn list dùng chung
            // query này nên total khớp số creative tươi thật.
            Object.assign(query, buildFreshAdCondition());
            const total = await adsCol.countDocuments(query);

            const ads = await adsCol.find(query)
                .project({
                    ad_archive_id: 1, ad_library_url: 1, text: { $substrCP: ["$text", 0, 150] }, score: 1, level: 1,
                    is_active: 1, start_date: 1, estimated_spend_usd: 1, estimated_reach: 1,
                    images: { $slice: 1 }, videos: { $slice: 1 }
                })
                // Active lên trước (ad đã dừng/không còn trong Library
                // xuống sau — trước đây sort pure score nên 20 ad đầu của
                // Kingsport toàn is_active=false), rồi score, rồi id để
                // phân trang ổn định (score trùng nhau rất nhiều).
                .sort({ is_active: -1, score: -1, ad_archive_id: 1 })
                .skip(skip)
                .limit(limit)
                .toArray();

            return res.json({
                success: true,
                pagination: { total, page, limit, pages: Math.ceil(total / limit), dedupe: dedupeOn },
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
            const { sort_by = 'product_score', sort_order = -1 } = req.query;
            const query = buildProductsSearchQuery(req.query);

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
     * 🟢 API 5b: MONTHLY WINNERS — sản phẩm thắng của tháng (xếp theo số
     * ngày thắng rồi tổng product_score) + top 1 mỗi ngày trong tháng.
     * Snapshot hàng ngày do analyzer ghi vào product_winners_daily (top 30
     * product/ngày, chạy lần đầu ở lượt FULL sau deploy — trước đó trả rỗng).
     * ?month=YYYY-MM (mặc định tháng hiện tại) hoặc ?day=YYYY-MM-DD để lấy
     * winners của 1 ngày (theo rank).
     */
    app.get('/api/products/monthly-winners', async (req, res) => {
        try {
            const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
            const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 50, 1), 100);
            const skip = (page - 1) * limit;

            const day = String(req.query.day || '').trim();
            if (day) {
                if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) return res.status(400).json({ success: false, message: 'day phai co dang YYYY-MM-DD.' });
                const total = await winnersCol.countDocuments({ date: day, domain: { $not: excludedProductDomainRegex() } });
                const results = await winnersCol.find({ date: day, domain: { $not: excludedProductDomainRegex() } }).sort({ rank: 1 }).skip(skip).limit(limit).toArray();
                return res.json({ success: true, mode: 'day', day, pagination: { total, page, limit, pages: Math.ceil(total / limit) }, data: results });
            }

            const month = parseMonthParam(req.query.month);
            if (!month) return res.status(400).json({ success: false, message: 'month phai co dang YYYY-MM.' });
            const all = await winnersCol.aggregate(buildMonthlyWinnersPipeline(month)).toArray();
            const total = all.length;
            const data = all.slice(skip, skip + limit).map((d) => ({ product_key: d._id, domain: d.domain, landing_url: d.landing_url || null, category: d.category || null, title: d.title, days_won: d.days_won, total_product_score: Number(d.total_product_score.toFixed(1)), total_winning_ads: d.total_winning_ads, best_rank: d.best_rank, latest_score: d.latest_score }));
            // Top 1 mỗi ngày trong tháng (để FE vẽ "hàng win mỗi ngày").
            const { start, end } = monthRange(month);
            const dailyTop = await winnersCol.find({ date: { $gte: start, $lt: end }, domain: { $not: excludedProductDomainRegex() } }).sort({ date: 1, rank: 1 }).project({ date: 1, domain: 1, title: 1, product_score: 1, rank: 1 }).toArray();
            const daily_top = {};
            for (const d of dailyTop) if (!daily_top[d.date]) daily_top[d.date] = { product_key: d.product_key || d.domain, domain: d.domain, landing_url: d.landing_url || null, category: d.category || null, title: d.title, product_score: d.product_score };
            return res.json({ success: true, mode: 'month', month, pagination: { total, page, limit, pages: Math.ceil(total / limit) }, data, daily_top });
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
            // Tham so nay gio la product_key (landing URL da encode) hoac
            // domain cu (tuong thich nguoc): thu product_key truoc.
            let rawKey = String(domain).trim();
            try { rawKey = decodeURIComponent(rawKey); } catch { /* giu nguyen */ }
            if (isExcludedProductDomain(rawKey)) return res.status(404).json({ success: false, message: `Product not found.` });

            let productInfo = await productsCol.findOne({ product_key: rawKey });
            if (!productInfo) productInfo = await productsCol.findOne({ domain: new RegExp(`^${escapeRegExp(rawKey)}$`, 'i') });
            if (!productInfo) return res.status(404).json({ success: false, message: `Product not found.` });
            if (isExcludedProductDomain(productInfo.domain)) return res.status(404).json({ success: false, message: `Product not found.` });

            const topAdsQuery = (productInfo.product_key && productInfo.product_key.startsWith('http'))
                ? { product_key: productInfo.product_key }
                : { domain: productInfo.domain };
            const topAds = await adsCol.find(topAdsQuery)
                .sort({ score: -1 }).limit(20)
                .project({ ad_archive_id: 1, ad_library_url: 1, text: { $substrCP: ["$text", 0, 150] }, score: 1, level: 1, estimated_spend_usd: 1, thumbnail_r2: 1, start_date: 1, last_seen: 1, product_key: 1, images: { $slice: 1 }, videos: { $slice: 1 } })
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

// Domain san TMDT / mang xa hoi / video — khong tinh la "san pham" de dua
// len bang winning products. Gom theo domain thi ca san gom thanh 1 product
// (vd shopee.vn gom hang trieu mat hang) va diem tong de be dan, nen bang
// toan hien ten san thay vi san pham cu the. Muon them/bot: sua list nay.
// Giu dong bo voi EXCLUDED_PRODUCT_DOMAINS trong searchApi.js (cung list).
const EXCLUDED_PRODUCT_DOMAINS = [
  // San TMDT
  'shopee.vn', 'shopee.com', 'shopee.ph', 'shopee.sg', 'shopee.com.my', 'shopee.co.th', 'shopee.co.id', 'shopee.com.br', 'shopee.tw', 'shopee.com.mx', 'shopee.cl',
  'lazada.vn', 'lazada.com', 'lazada.com.my', 'lazada.sg', 'lazada.co.th', 'lazada.co.id', 'lazada.ph',
  'tiki.vn', 'sendo.vn',
  'alibaba.com', 'aliexpress.com', 'aliexpress.us', '1688.com', 'taobao.com', 'tmall.com', 'jd.com', 'dhgate.com',
  'amazon.com', 'amazon.ca', 'amazon.co.uk', 'amazon.de', 'amazon.fr', 'amazon.it', 'amazon.es', 'amazon.com.au', 'amazon.in', 'amazon.co.jp', 'amazon.com.mx', 'amazon.com.br',
  'ebay.com', 'etsy.com', 'walmart.com',
  // Mang xa hoi / nhan tin / video
  'facebook.com', 'fb.com', 'instagram.com', 'tiktok.com', 'youtube.com', 'messenger.com', 'whatsapp.com', 'zalo.me', 'telegram.org', 'twitter.com', 'x.com', 'pinterest.com', 'linkedin.com', 'snapchat.com', 'threads.net',
];
// true neu domain la (hoac la subdomain cua) 1 domain trong blocklist.
// Khong phan biet hoa thuong, bo 'www.' dau. 'notshopee.vn' hay
// 'shopee.vn.fake.com' -> false (chi khop dung domain hoac '.domain').
function isExcludedProductDomain(domain) {
  if (!domain || typeof domain !== 'string') return false;
  const d = domain.trim().toLowerCase().replace(/^www\./, '');
  if (!d) return false;
  for (const base of EXCLUDED_PRODUCT_DOMAINS) {
    if (d === base || d.endsWith('.' + base)) return true;
  }
  return false;
}
// Regex dung cho query Mongo ($not): khop dung isExcludedProductDomain.
function excludedProductDomainRegex() {
  const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp('(^|\\.)(' + EXCLUDED_PRODUCT_DOMAINS.map(esc).join('|') + ')$', 'i');
}

// Query cho /api/products/search: q khớp title hoặc domain (substring,
// không phân biệt hoa thường); domain khớp chính xác; các min_* lọc số.
// products chỉ ~50k doc nên regex substring ở đây chấp nhận được.
function buildProductsSearchQuery(params) {
    const query = {};
    const q = String(params.q || '').trim();
    if (q) {
        const re = new RegExp(escapeRegExp(q), 'i');
        query.$or = [{ title: re }, { domain: re }, { landing_url: re }];
    }
    if (params.domain) query.domain = new RegExp(`^${escapeRegExp(String(params.domain).trim())}$`, 'i');
    // Loc theo nganh hang: category tren doc product la industry code cua
    // ad top score (beauty_health, electronics, fashion...). Gia tri la
    // phai nam trong INDUSTRIES, neu khong thi bo qua (khong tra 0 oan).
    if (params.industry) {
        const ind = String(params.industry).trim().toLowerCase();
        if (INDUSTRY_VALUES.has(ind)) query.category = ind;
    }
    if (params.min_product_score) query.product_score = { $gte: parseInt(params.min_product_score, 10) };
    if (params.min_winning_ads) query.winning_ads = { $gte: parseInt(params.min_winning_ads, 10) };
    if (params.min_total_ads) query.total_ads = { $gte: parseInt(params.min_total_ads, 10) };
    // Loai san TMDT / mang xa hoi khoi ket qua (xem EXCLUDED_PRODUCT_DOMAINS).
    query.$and = [...(query.$and || []), { domain: { $not: excludedProductDomainRegex() } }];
    return query;
}

// Khoảng ngày của 1 tháng 'YYYY-MM': [ngày đầu tháng, ngày đầu tháng sau).
function monthRange(month) {
    const [y, mo] = month.split('-').map(Number);
    const start = month + '-01';
    const end = new Date(Date.UTC(y, mo, 1)).toISOString().slice(0, 10);
    return { start, end };
}

// 'YYYY-MM'; mặc định tháng hiện tại (+07). Trả null nếu sai định dạng.
function parseMonthParam(monthParam, nowMs = Date.now()) {
    if (!monthParam) return new Date(nowMs + 7 * 60 * 60 * 1000).toISOString().slice(0, 7);
    const m = String(monthParam).trim();
    if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(m)) return null;
    return m;
}

// Pipeline gom monthly winners: theo domain, xếp theo số ngày thắng rồi
// tổng product_score. $sort theo date trước $group để $last là snapshot
// mới nhất của domain trong tháng.
function buildMonthlyWinnersPipeline(month) {
    const { start, end } = monthRange(month);
    return [
        { $match: { date: { $gte: start, $lt: end }, domain: { $not: excludedProductDomainRegex() } } },
        { $sort: { date: 1, rank: 1 } },
        { $group: { _id: { $ifNull: ['$product_key', '$domain'] }, domain: { $last: '$domain' }, landing_url: { $last: '$landing_url' }, category: { $last: '$category' }, title: { $last: '$title' }, days_won: { $sum: 1 }, total_product_score: { $sum: '$product_score' }, total_winning_ads: { $sum: '$winning_ads' }, best_rank: { $min: '$rank' }, latest_score: { $last: '$product_score' } } },
        { $sort: { days_won: -1, total_product_score: -1 } },
    ];
}

module.exports = { initSearchServer, buildPresetConditions, resolvePresetSort, resolveSearchSort, presetUsesFlag, productFirstEnabled, FILTER_DEFAULTS, buildTextSearchQuery, textSearchLimit, stripViDiacritics, INDUSTRIES, buildIndustryCondition, buildCountryCondition, buildDomainCondition, prefixRegex, escapeRegExp, lowerPrefixRange, normalizeQueryParams, normalizeCountryCodes, countryMatchValues, normalizeIndustryValues, normalizeSpendTiers, normalizeFunnelValues, parseActiveParam, buildActiveCondition, parseDateParam, normalizeSortField, normalizeSortOrder, normalizePresetNames, buildLevelCondition, buildScalingCondition, buildSpendCondition, buildIndustryTextCondition, refreshSchemaProbe, dedupeEnabled, buildSimilarTiers, buildSimilarCountQuery, fillSimilarAds, SIMILAR_TEXT_PREFIX_LEN, countSimilarTotal, refreshEsDedupeProbe, stripCountExistsConditions, EXCLUDED_PRODUCT_DOMAINS, isExcludedProductDomain, excludedProductDomainRegex, buildProductsSearchQuery, monthRange, parseMonthParam, buildMonthlyWinnersPipeline, countMediaUsage, buildFreshAdCondition, SCALING_HARD_MAX_RUN_DAYS, STALE_AD_MAX_AGE_DAYS, STALE_AD_MAX_AGE_MS };

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
