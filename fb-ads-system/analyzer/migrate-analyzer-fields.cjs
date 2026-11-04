/**
 * Migration field mới cho fb_ads_analyzer.analyzed_ads — chạy 1 lần sau
 * khi deploy searchApi bản mới, TRƯỚC khi đo lại các bộ lọc, HOẶC bỏ qua
 * nếu sẽ chạy 1 lượt analyzer FULL ngay (analyzer ghi đè toàn bộ doc,
 * gồm các field này).
 *
 * Ghi các field searchApi đường nhanh cần mà doc cũ chưa có:
 *   page_name_lower, domain_lower,
 *   country_mentions_lower, latest_detail_country_lower, countries_lower,
 *   industry (phân loại lại bằng đúng bảng từ khóa của analyzer.js,
 *   với quy tắc khớp nguyên từ cho từ đơn — xem industryKeywordMatch),
 *   preset_priority (0 sản phẩm ưu tiên / 1 trung lập / 2 giải trí —
 *   cùng quy tắc presetPriority của analyzer.js, từ industry +
 *   creative_rank đã có trên doc),
 *   is_active (còn thấy trong 7 ngày qua, cùng quy tắc searchApi).
 * KHÔNG migrate field nhóm duplicate (duplicate_group_key/size,
 * is_group_rep): chúng cần đếm toàn cục ở pass 1 của analyzer —
 * chỉ 1 lượt analyzer FULL mới ghi đúng. Dedupe ở API ở chế độ
 * fallback (dedupe_fallback='schema') cho tới lượt đó.
 * Ngoài ra ghi lại countries_lower cho doc đã có field nhưng thiếu
 * 'all'/latest country? Không: chỉ ghi field còn thiếu hoặc doc được
 * chọn bởi filter bên dưới; doc đã đủ field thì bỏ qua.
 *
 * Chạy:
 *   node migrate-analyzer-fields.cjs --dry-run --limit 10
 *   node migrate-analyzer-fields.cjs --limit 1000
 *   node migrate-analyzer-fields.cjs
 * Env: MONGO_URI (mặc định như analyzer.js: localhost; trên VM DB đặt
 * MONGO_URI=mongodb://192.168.100.7:27017/... hoặc chạy trong docker
 * exec mongodb-core). An toàn chạy lại: chỉ update doc còn thiếu field,
 * ghi theo batch 1000, có log tiến độ. Không xóa/sửa field nào khác.
 */
'use strict';
const { MongoClient } = require('mongodb');

const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2';
const DB_NAME = 'fb_ads_analyzer';
const COL_NAME = 'analyzed_ads';
const ACTIVE_WINDOW_MS = 7 * 24 * 3600 * 1000;

// Bảng từ khóa PHẢI khớp INDUSTRY_CATEGORIES trong analyzer.js (copy tại
// thời điểm viết script 03/11). Analyzer chạy lại là nguồn chính xác
// nhất; script này chỉ để bộ lọc chạy đúng ngay trước lượt analyzer đó.
const INDUSTRY_CATEGORIES = [
  { value: 'beauty_health', keywords: ['mỹ phẩm', 'skincare', 'trị mụn', 'kem', 'serum', 'dược', 'thuốc', 'vitamin', 'beauty', 'health', 'thực phẩm chức năng', 'thực phẩm bảo vệ sức khỏe', 'viên uống', 'collagen', 'giảm cân', 'spa', 'làm đẹp', 'sữa rửa mặt', 'kem chống nắng', 'dầu gội', 'trị nám', 'mọc tóc', 'sữa tắm', 'nước hoa', 'son môi', 'kem dưỡng', 'mặt nạ', 'kem đánh răng', 'bàn chải đánh răng', 'đồ dùng cá nhân'] },
  { value: 'electronics', keywords: ['điện thoại', 'iphone', 'samsung', 'laptop', 'macbook', 'tai nghe', 'airpods', 'đồng hồ thông minh', 'smartwatch', 'apple watch', 'máy tính bảng', 'ipad', 'loa bluetooth', 'camera', 'flycam', 'sạc dự phòng', 'bàn phím', 'chuột gaming', 'phụ kiện điện thoại'] },
  { value: 'fashion', keywords: ['thời trang', 'quần áo', 'giày', 'túi', 'đồng hồ', 'fashion', 'wear', 'váy', 'đầm', 'áo dài', 'sneaker', 'dép', 'quần jean', 'áo khoác', 'túi xách', 'giày dép', 'mũ', 'khăn choàng', 'thắt lưng', 'kính râm', 'trang sức', 'vòng cổ', 'nhẫn', 'bông tai', 'đồ lót', 'đồ ngủ'] },
  { value: 'home_living', keywords: ['nội thất', 'sofa', 'bàn ghế', 'đèn', 'chăn ga', 'gối', 'rèm', 'vệ sinh', 'làm sạch', 'tẩy rửa', 'nước giặt', 'nước rửa chén', 'lau nhà', 'hút bụi', 'máy lọc nước', 'máy lọc không khí', 'đồ gia dụng', 'nhà bếp', 'xoong', 'chảo', 'gia dụng', 'đồ trang trí', 'trang trí', 'decor', 'tranh treo tường', 'gương trang trí', 'thảm trải', 'nến thơm', 'lọ hoa', 'nồi', 'bình giữ nhiệt', 'hộp đựng', 'dụng cụ nhà bếp', 'kệ', 'giá treo'] },
  { value: 'food_beverage', keywords: ['nhà hàng', 'quán ăn', 'đồ ăn', 'thực phẩm', 'bánh', 'kẹo', 'cà phê', 'trà sữa', 'đồ uống', 'yến sào', 'hải sản', 'đặc sản', 'ăn vặt', 'thịt bò', 'gạo', 'mật ong', 'hạt dinh dưỡng', 'gia vị', 'nước mắm', 'trà'] },
  { value: 'toys', keywords: ['đồ chơi', 'đồ chơi trẻ em', 'lego', 'mô hình', 'búp bê', 'gấu bông', 'xe đồ chơi', 'xếp hình', 'thú nhồi bông', 'robot đồ chơi', 'đồ chơi giáo dục', 'bộ xếp hình'] },
  { value: 'plants', keywords: ['hạt giống', 'cây giống', 'cây cảnh', 'cây ăn quả', 'giống cây', 'hạt rau', 'phân bón', 'chậu cây', 'cây trồng', 'rau giống', 'hoa giống'] },
  { value: 'real_estate', keywords: ['bất động sản', 'nhà đất', 'căn hộ', 'chung cư', 'đất nền', 'biệt thự', 'nhà phố', 'shophouse', 'cho thuê nhà', 'bán nhà', 'dự án căn hộ'] },
  { value: 'finance', keywords: ['tài chính', 'đầu tư', 'crypto', 'forex', 'bank', 'ngân hàng', 'chứng khoán', 'cổ phiếu', 'vay vốn', 'vay tiền', 'bảo hiểm', 'tín dụng', 'coin', 'bitcoin', 'trading', 'lãi suất', 'thẻ tín dụng'] },
  { value: 'education', keywords: ['khóa học', 'học tiếng anh', 'đào tạo', 'tuyển sinh', 'luyện thi', 'ielts', 'toeic', 'học online', 'gia sư', 'du học', 'kỹ năng'] },
  { value: 'automotive', keywords: ['ô tô', 'xe hơi', 'xe máy', 'xe điện', 'phụ kiện ô tô', 'độ xe', 'lốp xe', 'bảo dưỡng xe', 'mua bán xe', 'xe tải'] },
  { value: 'pets', keywords: ['thú cưng', 'thức ăn cho chó', 'thức ăn cho mèo', 'cát mèo', 'phụ kiện thú cưng', 'spa thú cưng', 'chó', 'mèo'] },
  { value: 'travel', keywords: ['du lịch', 'tour', 'vé máy bay', 'khách sạn', 'resort', 'nghỉ dưỡng', 'combo du lịch', 'visa', 'hộ chiếu', 'vé tham quan'] },
  { value: 'entertainment', keywords: ['game', 'trò chơi', 'phim', 'ca nhạc', 'concert', 'vé xem phim', 'truyện', 'livestream', 'giải trí', 'pubg', 'liên quân', 'nạp game', 'truyện tranh', 'tiểu thuyết', 'webtoon', 'manhwa', 'manga', 'anime', 'hoạt hình', 'truyền hình', 'gameshow', 'chương trình truyền hình', 'show truyền hình', 'phim hoạt hình', 'phim bộ', 'phim lẻ', 'esports', 'streamer', 'ngôn tình', 'xem phim', 'đọc truyện', 'thể thao điện tử', 'game thủ'] },
  { value: 'gambling', keywords: ['casino', 'cá cược', 'cá độ', 'nhà cái', 'tài xỉu', 'xóc đĩa', 'bắn cá', 'nổ hũ', 'lô đề', 'đánh bài', 'poker'] },
  { value: 'other', keywords: [] },
];

function normalizeText(s) {
  return String(s || '').toLowerCase().replace(/\s+/g, ' ').trim();
}
function industryKeywordMatch(fullText, keyword) {
  if (keyword.includes(' ')) return fullText.includes(keyword);
  const escaped = keyword.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`(^|[^\\p{L}\\p{N}])${escaped}($|[^\\p{L}\\p{N}])`, 'u').test(fullText);
}
function classifyIndustry(doc) {
  const fullText = normalizeText((doc.text || '') + ' ' + (doc.headline || ''));
  for (const cat of INDUSTRY_CATEGORIES) {
    if (cat.keywords.some(k => industryKeywordMatch(fullText, k))) return cat.value;
  }
  return 'other';
}
const PRESET_PRIORITY_INDUSTRIES = new Set(['beauty_health', 'fashion', 'home_living', 'food_beverage', 'toys', 'plants']);
function presetPriorityValue(industry, creativeRank) {
  if (PRESET_PRIORITY_INDUSTRIES.has(industry)) return 0;
  if (industry === 'entertainment' || industry === 'gambling') return 2;
  if (creativeRank === 2) return 2;
  return 1;
}

function extractDomain(link) {
  try {
    const u = new URL(String(link || ''));
    return u.hostname.replace(/^www\./, '').toLowerCase();
  } catch (_) { return ''; }
}

// Bỏ dấu tiếng Việt (để chuẩn hóa mã nước, không đụng text gốc).
function stripDiacritics(s) {
  return String(s || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/đ/g, 'd').replace(/Đ/g, 'D');
}

// Bảng tên nước -> mã ISO (đồng bộ COUNTRY_NAME_TO_CODE trong analyzer.js
// và COUNTRY_NAME_MAP trong searchApi.js; đổi ở đây thì đổi cả 2 bên kia).
// Khóa đã strip dấu + chữ thường.
const COUNTRY_NAME_TO_CODE = {
  'viet nam': 'vn', vietnam: 'vn', vn: 'vn',
  'hoa ky': 'us', 'united states': 'us', usa: 'us', us: 'us',
  my: 'us', // từ 'Mỹ' strip dấu — chỉ tới được đây khi raw KHÔNG phải mã 'MY' (mã ISO được ưu tiên ở dưới)
  'nhat ban': 'jp', japan: 'jp', jp: 'jp',
  'thai lan': 'th', thailand: 'th', th: 'th',
  philippines: 'ph', 'phi lip pin': 'ph', ph: 'ph',
  malaysia: 'my', 'ma lai xi a': 'my',
  indonesia: 'id', 'in do ne xi a': 'id', id: 'id',
  singapore: 'sg', 'xin ga po': 'sg', sg: 'sg',
  'han quoc': 'kr', korea: 'kr', 'south korea': 'kr', kr: 'kr',
  anh: 'gb', 'united kingdom': 'gb', uk: 'gb', gb: 'gb',
  duc: 'de', germany: 'de', de: 'de',
  phap: 'fr', france: 'fr', fr: 'fr',
  canada: 'ca', ca: 'ca',
  uc: 'au', australia: 'au', au: 'au',
  brazil: 'br', br: 'br',
  mexico: 'mx', mx: 'mx',
  uae: 'ae', 'cac tieu vuong quoc a rap': 'ae', ae: 'ae',
  'dai loan': 'tw', taiwan: 'tw', tw: 'tw',
  'hong kong': 'hk', hk: 'hk',
  'trung quoc': 'cn', china: 'cn', cn: 'cn',
  'an do': 'in', india: 'in', in: 'in',
  'tay ban nha': 'es', spain: 'es', es: 'es',
  y: 'it', italy: 'it', it: 'it',
  'ha lan': 'nl', netherlands: 'nl', nl: 'nl',
  nga: 'ru', russia: 'ru', ru: 'ru',
  'tho nhi ky': 'tr', turkey: 'tr', tr: 'tr',
};
const COUNTRY_CODES = new Set(Object.values(COUNTRY_NAME_TO_CODE));

// Giá trị country thô -> mã ISO chữ thường, hoặc null nếu không nhận ra /
// là 'ALL'. Mã ISO 2 chữ đúng dạng được ưu tiên trước bảng tên để 'MY'
// (Malaysia) không bị nuốt thành 'us' do 'Mỹ' strip dấu cũng ra 'my'.
function canonicalizeCountryValue(raw) {
  const t = String(raw == null ? '' : raw).trim();
  if (!t) return null;
  const upper = t.toUpperCase();
  if (/^[A-Z]{2}$/.test(upper) && COUNTRY_CODES.has(upper.toLowerCase())) return upper.toLowerCase();
  const key = stripDiacritics(t).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
  if (!key || key === 'all') return null;
  return COUNTRY_NAME_TO_CODE[key] || null;
}

// Tính các field mới cho 1 doc (pure — test được không cần DB).
function computeFields(doc, nowMs) {
  const now = nowMs || Date.now();
  const domain = doc.domain || extractDomain(doc.link);
  const countryMentionsLower = (Array.isArray(doc.country_mentions) ? doc.country_mentions : [])
    .map(c => String(c).trim().toLowerCase()).filter(Boolean);
  const latestCountry = doc.latest_detail_country || doc.last_country || 'ALL';
  const latestCountryLower = String(latestCountry).trim().toLowerCase();
  // countries_lower CHUẨN HÓA về mã ISO (đồng bộ analyzer.js): giá trị
  // thô dạng tên ('United States') không khớp lọc equality theo mã.
  // 'ALL' bị loại. country_mentions_lower/latest_detail_country_lower
  // giữ dạng thô cho đường fallback.
  const countriesLower = [...new Set([
    ...countryMentionsLower.map(canonicalizeCountryValue),
    canonicalizeCountryValue(latestCountry),
  ].filter(Boolean))];
  const lastSeen = typeof doc.last_seen === 'number' ? doc.last_seen : 0;
  const industry = classifyIndustry(doc);
  return {
    page_name_lower: String(doc.page_name || '').toLowerCase(),
    domain_lower: String(domain || '').toLowerCase(),
    country_mentions_lower: countryMentionsLower,
    latest_detail_country_lower: latestCountryLower,
    countries_lower: countriesLower,
    industry,
    preset_priority: presetPriorityValue(industry, doc.creative_rank),
    is_active: lastSeen > 0 && (now - lastSeen) <= ACTIVE_WINDOW_MS,
  };
}

function parseArgs(argv) {
  const args = { dryRun: false, limit: 0, batchSize: 1000 };
  for (const a of argv) {
    if (a === '--dry-run') args.dryRun = true;
    else if (a.startsWith('--limit=')) args.limit = parseInt(a.slice(8), 10) || 0;
    else if (a.startsWith('--limit')) { /* --limit N handled below */ }
    else if (a.startsWith('--batch-size=')) args.batchSize = parseInt(a.slice(13), 10) || 1000;
  }
  const li = argv.indexOf('--limit');
  if (li >= 0 && argv[li + 1]) args.limit = parseInt(argv[li + 1], 10) || 0;
  const bi = argv.indexOf('--batch-size');
  if (bi >= 0 && argv[bi + 1]) args.batchSize = parseInt(argv[bi + 1], 10) || 1000;
  return args;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  try {
    const col = client.db(DB_NAME).collection(COL_NAME);
    const total = await col.estimatedDocumentCount();
    // Doc cần migration: thiếu ÍT NHẤT 1 field mới.
    const filter = {
      $or: [
        { page_name_lower: { $exists: false } },
        { domain_lower: { $exists: false } },
        { countries_lower: { $exists: false } },
        { industry: { $exists: false } },
        { preset_priority: { $exists: false } },
        { is_active: { $exists: false } },
      ],
    };
    const needing = await col.countDocuments(filter);
    console.log(`Collection: ${DB_NAME}.${COL_NAME} — tổng ~${total} docs, cần migration: ${needing} docs${args.dryRun ? ' (DRY-RUN, không ghi)' : ''}${args.limit ? `, giới hạn ${args.limit}` : ''}.`);
    const cursor = col.find(filter, {
      projection: { ad_archive_id: 1, page_name: 1, domain: 1, link: 1, text: 1, headline: 1, country_mentions: 1, latest_detail_country: 1, last_country: 1, last_seen: 1, creative_rank: 1 },
      noCursorTimeout: true,
    }).batchSize(200);
    let scanned = 0, updated = 0, ops = [];
    const flush = async () => {
      if (!ops.length) return;
      if (args.dryRun) { console.log(`[dry-run] sẽ ghi ${ops.length} ops (bỏ qua)`); ops = []; return; }
      const r = await col.bulkWrite(ops, { ordered: false });
      updated += (r.modifiedCount || 0) + (r.upsertedCount || 0);
      ops = [];
    };
    try {
      for await (const doc of cursor) {
        if (args.limit && scanned >= args.limit) break;
        scanned++;
        const fields = computeFields(doc);
        // Chỉ $set field còn thiếu (doc có thể đã có 1 phần field từ
        // analyzer bản giữa): tránh ghi đè industry đã phân loại mới.
        const set = {};
        for (const [k, v] of Object.entries(fields)) {
          if (doc[k] === undefined) set[k] = v;
        }
        if (Object.keys(set).length) {
          ops.push({ updateOne: { filter: { _id: doc._id }, update: { $set: set } } });
        }
        if (ops.length >= args.batchSize) await flush();
        if (scanned % 10000 === 0) console.log(`... đã quét ${scanned}, đã ghi ${updated}`);
      }
      await flush();
    } finally {
      await cursor.close().catch(() => {});
    }
    console.log(`Xong: đã quét ${scanned} docs, đã ghi ${updated} docs${args.dryRun ? ' (dry-run: 0 ghi thật)' : ''}.`);
    if (!args.dryRun) {
      const remaining = await col.countDocuments(filter);
      console.log(`Còn thiếu field: ${remaining} docs. Kiểm chứng: db.analyzed_ads.countDocuments({countries_lower:{$exists:true}}) nên gần bằng tổng số docs.`);
    }
  } finally {
    await client.close().catch(() => {});
  }
}

if (require.main === module) {
  main().catch(e => { console.error(e); process.exit(1); });
}

module.exports = { computeFields, classifyIndustry, industryKeywordMatch, INDUSTRY_CATEGORIES, parseArgs, presetPriority: presetPriorityValue, canonicalizeCountryValue };
