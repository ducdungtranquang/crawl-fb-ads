const { MongoClient } = require('mongodb');
const { Client } = require('@elastic/elasticsearch');

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

// ===== CONFIG =====
const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2';
const CRAWLER_DB_NAME = 'fb_ads';          // Nguồn dữ liệu gốc từ crawler
const ANALYZER_DB_NAME = 'fb_ads_analyzer'; // Nơi lưu trữ dữ liệu đã phân tích

const ELASTICSEARCH_URL = process.env.ELASTICSEARCH_URL || 'http://localhost:9200';
const ELASTIC_INDEX = 'fb_ads_analyzer';


// ===== UTILS =====
function normalize(text) {
  return text?.toLowerCase().replace(/\s+/g, ' ').trim();
}

// Bỏ dấu tiếng Việt (để chuẩn hóa mã nước, không đụng text gốc).
function stripDiacritics(s) {
  return String(s || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/đ/g, 'd').replace(/Đ/g, 'D');
}

// Bảng tên nước -> mã ISO (đồng bộ COUNTRY_NAME_MAP trong searchApi.js;
// đổi ở đây thì đổi cả bên kia). Khóa đã strip dấu + chữ thường.
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

// Giá trị country thô (vd 'USA', 'United States', 'Việt Nam', 'MY') ->
// mã ISO chữ thường ('us', 'vn', 'my'), hoặc null nếu không nhận ra /
// là 'ALL' (mặc định "mọi nước" của crawler, không phải 1 nước).
// Mã ISO 2 chữ đúng dạng được ưu tiên trước bảng tên để 'MY' (Malaysia)
// không bị nuốt thành 'us' do 'Mỹ' strip dấu cũng ra 'my'.
function canonicalizeCountryValue(raw) {
  const t = String(raw == null ? '' : raw).trim();
  if (!t) return null;
  const upper = t.toUpperCase();
  if (/^[A-Z]{2}$/.test(upper) && COUNTRY_CODES.has(upper.toLowerCase())) return upper.toLowerCase();
  const key = stripDiacritics(t).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
  if (!key || key === 'all') return null;
  return COUNTRY_NAME_TO_CODE[key] || null;
}

// Suy luận nước dự phòng (2026-11-04): CHỈ dùng khi countries_lower RỖNG
// (không có dữ liệu nước thật từ Meta). KHÔNG BAO GIỜ ghi đè dữ liệu thật:
// ad đã có nhãn Meta (kể cả 'us' cho ad tiếng Việt nhắm kiều bào) giữ nguyên.
// Suy ra 'vn' khi có tín hiệu tiếng Việt đủ mạnh:
//   - domain .vn, hoặc giá tiền VND (số + ₫/đ/VNĐ),
//   - hoặc >= 3 ký tự dấu đặc trưng tiếng Việt (dấu mũ/nón/móc/nặng mà
//     tiếng Pháp/Tây Ban Nha không dùng: ắ ộ ự ả ...),
//   - hoặc >= 1 ký tự dấu đặc trưng + >= 1 cụm từ bán hàng tiếng Việt,
//   - hoặc >= 2 cụm từ bán hàng tiếng Việt khác nhau.
// Kết quả suy luận luôn đánh dấu country_inferred=true để phân biệt với
// nhãn Meta. migrate-analyzer-fields.cjs có bản copy giống hệt (dùng
// normalizeText thay cho normalize): đổi quy tắc thì đổi cả 2 chỗ.
const VIETNAMESE_MARK_CHARS = /[ăắằẳẵặấầẩẫậđếềểễệốồổỗộớờởỡợứừửữựảãạẻẽẹỉĩịỏõọủũụỷỹỵ]/g;
const VIETNAMESE_MARKERS = [
  'mua ngay', 'giam gia', 'freeship', 'mien phi van chuyen', 'giao hang',
  'toan quoc', 'dat hang', 'lien he', 'chinh hang', 'bao hanh', 'gia chi',
  'con hang', 'hotline', 'zalo', 'tiki', 'ship cod', 'kuyen mai', 'tu van',
  'mien phi', 'dat lich', 'xem them', 'mua sam', 'thanh toan', 'nhan hang',
  'uu dai', 'ho tro', 'cham soc', 'mua 1 tang 1', 'sale soc', 'tri mun',
  'noi that', 'thoi trang', 'gia dung', 'suc khoe', 'lam dep',
];
const VND_PRICE_RE = /\d[\d.,]*\s*(₫|đ|vnđ|vnd)(?!\p{L})/iu;

function inferCountryFromVietnamese(ad, domain) {
  const rawText = normalize(
    (ad.text || '') + ' ' + (ad.headline || '') + ' ' + (ad.description || '') + ' ' + (ad.page_name || '')
  ) || '';
  const dom = String(domain || ad.domain || extractDomain(ad.link) || '').toLowerCase();
  if (dom.endsWith('.vn')) return 'vn';
  if (VND_PRICE_RE.test(rawText)) return 'vn';
  const marks = (rawText.match(VIETNAMESE_MARK_CHARS) || []).length;
  if (marks >= 3) return 'vn';
  const stripped = stripDiacritics(rawText).toLowerCase();
  let hits = 0;
  for (const m of VIETNAMESE_MARKERS) {
    if (m.includes(' ')) {
      if (stripped.includes(m)) hits++;
    } else {
      const esc = m.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      if (new RegExp(`(^|[^\\p{L}\\p{N}])${esc}($|[^\\p{L}\\p{N}])`, 'u').test(stripped)) hits++;
    }
  }
  if ((marks >= 1 && hits >= 1) || hits >= 2) return 'vn';
  return null;
}
function extractDomain(url) {
  try {
    return new URL(url).hostname.replace('www.', '');
  } catch {
    return null;
  }
}

// ===== SCORING CALCULATIONS =====
function calcDelta(history) {
  if (!history || history.length < 2) return 0;
  return history[history.length - 1].c - history[history.length - 2].c;
}

function calcSmoothDelta(history) {
  if (!history || history.length < 3) return 0;
  let sum = 0;
  for (let i = 1; i < history.length; i++) {
    sum += (history[i].c - history[i - 1].c);
  }
  return sum / (history.length - 1);
}

function calcBurst(history, window = 5) {
  if (!history || history.length < window) return 0;
  const slice = history.slice(-window);
  return slice[slice.length - 1].c - slice[0].c;
}

function calcGrowthFallback(ad) {
  if (!ad.first_seen || !ad.seen_count) return 0;
  const ageHours = (Date.now() - ad.first_seen) / (1000 * 3600);
  if (ageHours <= 0) return 0;
  return ad.seen_count / ageHours;
}

// ===== PHÂN LOẠI NGÀNH NGHỀ =====
// value: mã chuẩn lưu trong field `industry` (API lọc theo đó, FE map theo
// value). PHẢI KHỚP với INDUSTRIES trong searchApi.js — đổi value ở đây thì
// đổi cả bên kia. keywords: cụm từ đã normalize (chữ thường, giữ dấu) để
// phân loại từ text + headline. Thứ tự mảng = thứ tự ưu tiên khi ad khớp
// nhiều ngành (khớp trước thắng). Bảng này KHÔNG dùng cho hệ số spend
// (mInd) — mInd giữ nguyên 3 nhánh cũ trong estimateSpendAndReach để không
// đổi số spend đã tính (vd: "thực phẩm chức năng" phân loại beauty_health
// nhưng mInd vẫn 1.0 như cũ).
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

function industryKeywordMatch(fullText, keyword) {
  // Cụm nhiều từ: khớp chuỗi con như cũ. Từ đơn: phải khớp NGUYÊN TỪ
  // (ranh giới không phải chữ/số, Unicode-aware) — trước đây includes()
  // làm 'kem'/'spa'/'tour'/'game' bắt nhầm cả từ khác chứa chúng, và
  // beauty_health đứng đầu nên gán sai hàng loạt (test UI 03/11: ad bất
  // động sản, sàn gym, đồ điện đều bị gán "Làm đẹp & Sức khỏe").
  if (keyword.includes(' ')) return fullText.includes(keyword);
  const escaped = keyword.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`(^|[^\\p{L}\\p{N}])${escaped}($|[^\\p{L}\\p{N}])`, 'u').test(fullText);
}

function classifyIndustry(ad) {
  const fullText = normalize((ad.text || '') + ' ' + (ad.headline || ''));
  for (const cat of INDUSTRY_CATEGORIES) {
    if (cat.keywords.some(k => industryKeywordMatch(fullText, k))) return cat.value;
  }
  return 'other';
}

// ===== PHÂN LOẠI KIỂU CREATIVE (ƯU TIÊN MUA SẮM) =====
// Heuristic DỰA TRÊN TEXT/HEADLINE/CTA/LINK — không phải phân tích hình ảnh.
// Meta/crawler không lưu kiểu hình (vẽ tay, AI, ảnh thật) nên ad nào ảnh
// truyện tranh/AI nhưng text viết như ad sản phẩm bình thường thì KHÔNG
// phân biệt được ở đây; muốn bắt đúng nhóm đó phải thêm phân loại ảnh
// (vision model) ở crawler, ngoài phạm vi bản này.
// - 'product' (rank 0): ad mua sắm/bán sản phẩm — CTA mua hàng, funnel
//   ECOM/DROPSHIP, hoặc có link/domain kèm từ giá/khuyến mãi/COD/freeship.
// - 'entertainment' (rank 2): ad thuần giải trí — game, truyện, phim,
//   nhạc, livestream giải trí, nội dung "tạo bởi AI". Từ "AI" đứng một
//   mình KHÔNG tính (sản phẩm công nghệ dùng AI thật vẫn là sản phẩm),
//   phải là cụm nội dung AI. 'anime'/'hoạt hình' chỉ tính khi kèm
//   xem/đọc/tập/phim.
// - 'other' (rank 1): còn lại — không loại, chỉ xếp giữa.
// Thứ tự: product thắng entertainment (ad bán hàng có nhắc game/truyện
// trong text nhưng có giá + COD vẫn là ad sản phẩm). Chỉ dùng để XẾP
// HẠNG trong preset, không lọc bỏ ad nào.
const CREATIVE_KINDS = {
  product: 0,
  other: 1,
  entertainment: 2,
};
const SHOP_CTAS = ['shop now', 'mua ngay', 'đặt hàng', 'mua hàng', 'order now', 'buy now', 'add to cart', 'thêm vào giỏ', 'đặt mua', 'mua sắm'];
const PRODUCT_TOKENS = ['cod', 'freeship', 'miễn phí vận chuyển', 'giảm giá', '% off', 'flash sale', 'chỉ còn', 'giá chỉ', 'mua 1 tặng 1', 'khuyến mãi', 'đặt hàng', 'sale'];
const ENTERTAINMENT_TOKENS = ['game', 'trò chơi', 'chơi game', 'phim', 'ca nhạc', 'concert', 'truyện', 'livestream', 'giải trí', 'anime', 'hoạt hình', 'webtoon', 'manhwa', 'manhua', 'manga', 'comic', 'tiểu thuyết', 'âm nhạc', 'xem phim', 'đọc truyện', 'truyền hình', 'gameshow', 'esports', 'streamer', 'phim hoạt hình', 'show truyền hình'];
const AI_CONTENT_TOKENS = ['tạo bởi ai', 'ai tạo', 'video ai', 'ảnh ai', 'giọng ai', 'nhân vật ai', 'ai vẽ', 'deepfake', 'ai-generated', 'generated by ai', 'made with ai', 'do ai tạo', 'bằng ai'];
const ENTERTAINMENT_CTAS = ['play game', 'chơi game', 'chơi ngay', 'xem ngay', 'watch now'];

function classifyCreativeKind(ad, funnel) {
  const fullText = normalize((ad.text || '') + ' ' + (ad.headline || ''));
  const cta = String(ad.cta || '').toLowerCase().trim();
  const hasDomain = Boolean(extractDomain(ad.link));
  const hasShopCta = SHOP_CTAS.some((c) => cta === c || cta.includes(c));
  const hasProductToken = PRODUCT_TOKENS.some((t) => fullText.includes(t));
  if (hasShopCta || funnel === 'ECOM' || funnel === 'DROPSHIP' || (hasDomain && hasProductToken)) {
    return 'product';
  }
  const hasEntertainmentCta = ENTERTAINMENT_CTAS.some((c) => cta.includes(c));
  const hasEntertainmentText = ENTERTAINMENT_TOKENS.some((t) => fullText.includes(t))
    || AI_CONTENT_TOKENS.some((t) => fullText.includes(t));
  if (hasEntertainmentCta || hasEntertainmentText) return 'entertainment';
  return 'other';
}


// ===== ƯU TIÊN PRESET: SẢN PHẨM LÊN TRƯỚC, GIẢI TRÍ XUỐNG SAU =====
// preset_priority: 0 = ngành sản phẩm ưu tiên (gia dụng, đồ chơi, thực
// phẩm chức năng, thời trang, đồ dùng cá nhân, đồ trang trí, hạt
// giống/cây giống), 1 = trung lập, 2 = giải trí/cá cược (game, phim,
// truyện, chương trình truyền hình...). Chỉ dùng để XẾP HẠNG trong 3
// preset (new_ads, ready_to_scale, weekly_winner), không lọc bỏ ad.
// Ngành quyết định trước; ad thuần giải trí nhưng không khớp ngành nào
// (industry 'other') mà creative_kind là entertainment cũng xuống 2.
const PRESET_PRIORITY_INDUSTRIES = new Set(['beauty_health', 'fashion', 'home_living', 'food_beverage', 'toys', 'plants']);
const PRESET_DEPRIORITIZED_INDUSTRIES = new Set(['entertainment', 'gambling']);

function presetPriority(industry, creativeKind) {
  if (PRESET_PRIORITY_INDUSTRIES.has(industry)) return 0;
  if (PRESET_DEPRIORITIZED_INDUSTRIES.has(industry)) return 2;
  if (creativeKind === 'entertainment') return 2;
  return 1;
}

function detectFunnel(ad) {
  const text = (ad.text || '').toLowerCase();
  const link = (ad.link || '').toLowerCase();

  if (text.includes('free') || text.includes('ebook') || text.includes('training') || link.includes('lead') || link.includes('form')) return 'LEADGEN';
  if (text.includes('shipping') || text.includes('cod') || text.includes('50% off')) return 'DROPSHIP';
  if (ad.cta === 'Shop Now' || link.includes('product') || link.includes('cart')) return 'ECOM';
  return 'UNKNOWN';
}

/**
 * 🎯 ƯỚC TÍNH CHI PHÍ (SPEND) VÀ SỐ LƯỢT TIẾP CẬN (REACH)
 * Áp dụng mô hình toán học Ads Spy tối ưu
 */
// Trần ước tính chi tiêu (2026-11-04, chống số phi lý):
// - D (bản sao) tối đa 30: vượt quá là re-upload theo thời gian, không
//   phải hàng chục nghìn bản sao chạy đồng thời cùng đốt ngân sách.
// - T (số ngày) tối đa 365: quá 1 năm thì start_date/first_seen đã cũ,
//   không còn là chi tiêu liên tục.
// - Trần tuyệt đối theo quy mô Page: min(500k, max(5k, 1k × số ad của Page)).
//   Page chỉ có vài ad không thể có 1 ad đốt hàng trăm nghìn đô.
// - Tuổi dùng số ngày THỰC (phân số, tối thiểu 0,05 ngày ≈ 1,2 giờ) thay vì
//   làm tròn lên 1: các ad mới lên trong cùng 1 ngày không còn ra cùng
//   một bộ ba $35 / 1,7k / 1 bản sao.
// Các hằng số điều chỉnh được tại SPEND_MODEL_CAPS. Công thức gốc
// (D × T × bBase × mInd × fT) và các tier giữ nguyên.
const SPEND_MODEL_CAPS = {
  maxCopies: 30,
  maxDays: 365,
  minAgeDays: 0.05,
  spendFloorUsd: 1,
  absoluteMaxUsd: 500000,
  perPageAdCapUsd: 1000,
  minPageCapUsd: 5000,
};

function estimateSpendAndReach(ad, durationDays = 1, duplicatesCount = 1, pageAdsCount = 1) {
  const T = Math.min(SPEND_MODEL_CAPS.maxDays, Math.max(SPEND_MODEL_CAPS.minAgeDays, durationDays));
  const D = Math.min(SPEND_MODEL_CAPS.maxCopies, Math.max(1, Math.round(duplicatesCount || ad.seen_count || 1)));

  // 1. Xác định CPM theo khu vực Địa lý
  const countries = Array.isArray(ad.country_mentions) && ad.country_mentions.length > 0
    ? ad.country_mentions
    : [ad.last_country || 'US'];

  const geoCpmMap = {
    US: 18.0, CA: 16.0,
    UK: 10.0, DE: 9.5, FR: 9.0, SG: 11.0, EU: 9.0,
    RO: 5.0, BR: 4.5, MX: 4.0, TH: 3.5,
    VN: 2.0, PH: 1.8, ID: 1.5
  };

  let sumCpm = 0;
  countries.forEach(c => {
    sumCpm += geoCpmMap[c?.toUpperCase()] || 8.0;
  });
  const cpm = sumCpm / countries.length;

  // 2. Định nghĩa Hệ số Ngành hàng
  const fullText = normalize((ad.text || '') + ' ' + (ad.headline || ''));
  let mInd = 1.0;
  if (fullText.match(/mỹ phẩm|skincare|trị mụn|kem|serum|dược|thuốc|vitamin|beauty|health/i)) {
    mInd = 1.2;
  } else if (fullText.match(/thời trang|quần áo|giày|túi|đồng hồ|fashion|wear/i)) {
    mInd = 0.9;
  } else if (fullText.match(/bất động sản|tài chính|đầu tư|crypto|forex|bank/i)) {
    mInd = 2.0;
  }

  // 3. Phân tầng Ngân sách Cơ sở (Tier Dynamic Allocation)
  const TIERS = [1.5, 8.0, 35.0]; // duy trì/nuôi page, Standard, Scale
  let bBase = 35.0;
  if (T > 30 && D <= 2) {
    bBase = 1.5;
  } else if (T > 10 && D <= 3) {
    bBase = 8.0;
  }

  const fT = 1.0 + 0.12 * Math.log(T);

  // 4. Tính toán Ngân sách Chi tiêu — trả về KHOẢNG (min–max) thay vì 1 số điểm.
  // Khoảng phản ánh độ bất định lớn nhất của mô hình: việc xếp tier (bBase).
  // min = tier dưới, max = tier trên; ở tier biên thì nhân 0.5 / 1.5.
  const spendFor = (b) => Math.max(D * T * b * mInd * fT, SPEND_MODEL_CAPS.spendFloorUsd); // Tối thiểu $1 tuyệt đối
  let estimatedSpend = spendFor(bBase);
  const tierIdx = TIERS.indexOf(bBase);
  const bLow = tierIdx > 0 ? TIERS[tierIdx - 1] : bBase * 0.5;
  const bHigh = tierIdx < TIERS.length - 1 ? TIERS[tierIdx + 1] : bBase * 1.5;
  let spendMin = Math.min(estimatedSpend, spendFor(bLow));
  let spendMax = Math.max(estimatedSpend, spendFor(bHigh));

  // Trần theo quy mô Page (áp TRƯỚC khi suy reach, để reach cũng bị chặn theo).
  const pageCap = Math.min(
    SPEND_MODEL_CAPS.absoluteMaxUsd,
    Math.max(SPEND_MODEL_CAPS.minPageCapUsd, SPEND_MODEL_CAPS.perPageAdCapUsd * Math.max(1, pageAdsCount || 1))
  );
  const spendCapped = spendMax > pageCap;
  if (spendCapped) {
    estimatedSpend = Math.min(estimatedSpend, pageCap);
    spendMin = Math.min(spendMin, pageCap);
    spendMax = Math.min(spendMax, pageCap);
  }

  // 5. Tính toán Số lượt tiếp cận (kéo theo khoảng của spend)
  const frequency = 1.15;
  const reachFor = (s) => Math.round((s / (cpm * frequency)) * 1000);
  const estimatedReach = reachFor(estimatedSpend);

  let spendLevel = 'LOW';
  if (estimatedSpend >= 1000) spendLevel = 'VERY HIGH';
  else if (estimatedSpend >= 300) spendLevel = 'HIGH';
  else if (estimatedSpend >= 50) spendLevel = 'MEDIUM';

  return {
    estimated_spend_usd: Number(estimatedSpend.toFixed(1)),
    estimated_spend_min_usd: Number(spendMin.toFixed(1)),
    estimated_spend_max_usd: Number(spendMax.toFixed(1)),
    estimated_reach: estimatedReach,
    estimated_reach_min: reachFor(spendMin),
    estimated_reach_max: reachFor(spendMax),
    estimated_spend: spendLevel,
    estimated_spend_source: 'model',
    estimated_spend_capped: spendCapped,
    cpm_used: cpm
  };
}

function summarizeDetailHistory(ad) {
  const history = Array.isArray(ad.detail_history) ? ad.detail_history : [];
  const latest = history[history.length - 1] || {};
  const firstSeen = ad.first_seen || Date.now();
  const totalDays = Math.max(1, ((Date.now() - firstSeen) / (1000 * 3600 * 24)));

  return {
    ad_lifecycle_days: Number(totalDays.toFixed(2)),
    text_change_count: ad.text_change_count || history.filter(item => item.text && item.text !== latest.text).length,
    headline_change_count: ad.headline_change_count || history.filter(item => item.headline && item.headline !== latest.headline).length,
    domain_change_count: ad.domain_change_count || (Array.isArray(ad.domain_history) ? ad.domain_history.length - 1 : 0),
    cta_change_count: ad.cta_change_count || (Array.isArray(ad.cta_history) ? ad.cta_history.length - 1 : 0),
    creative_change_count: ad.creative_change_count || (Array.isArray(ad.media_change_history) ? ad.media_change_history.length : 0),
    keyword_count: Array.isArray(ad.keyword_mentions) ? ad.keyword_mentions.length : 0,
    country_count: Array.isArray(ad.country_mentions) ? ad.country_mentions.length : 0,
    unique_keywords: Array.isArray(ad.keyword_mentions) ? [...new Set(ad.keyword_mentions)].length : 0,
    unique_countries: Array.isArray(ad.country_mentions) ? [...new Set(ad.country_mentions)].length : 0,
    detail_history_length: history.length,
    longest_creative_streak_days: history.length > 0 ? Math.max(1, history.length) : 1,
    latest_detail_country: latest.country || ad.last_country || 'ALL'
  };
}

// Hash 64-bit (2 x 32-bit: FNV-1a + djb2) cho text: key cố định ~13 ký tự
// thay vì giữ nguyên cả đoạn text trong textMap. Hàng triệu text unique mà
// giữ nguyên string làm key = hàng GB RAM -> tràn. Va chạm 64-bit với vài
// triệu text là không đáng kể (~1e-6); text va chạm chỉ làm D lệch nhẹ cho
// đúng text đó, không ảnh hưởng số liệu khác.
function hashTextKey(s) {
  let h1 = 0x811c9dc5, h2 = 5381;
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    h1 = Math.imul(h1 ^ c, 0x01000193);
    h2 = ((h2 << 5) + h2 + c) | 0;
  }
  return (h1 >>> 0).toString(36) + ':' + (h2 >>> 0).toString(36);
}


// ===== GOM NHÓM DUPLICATE (1 AD GỐC / NHÓM) =====
// Nhóm = cùng page_name (chữ thường) + cùng normalized_text. Text rỗng
// KHÔNG gom (mỗi ad là nhóm riêng) — cùng quy tắc duplicates_count.
// Khóa nhóm dùng chung hash 64-bit với textMap để không giữ text dài
// trong RAM. Ad đại diện = ad chạy sớm nhất (start_date, rồi
// first_seen); hòa thì ad_archive_id nhỏ hơn thắng để quyết định
// deterministic dù thứ tự cursor đổi. Crawler không lưu page_id nên
// dùng page_name làm khóa page (khớp với pageMap hiện có).
// Text quá ngắn/chung chung (chỉ emoji, "sale", "shop now", ...) không
// đủ định danh 1 creative: đếm trùng toàn cục theo text đó phình D lên
// hàng chục nghìn (vd 45240 ad cùng 1 text chung -> spend nổ theo D*T).
// Coi như không có text để đếm/gom nhóm: duplicates rơi về
// collation_count/seen_count. Ngưỡng 8 ký tự chữ/số (đã ghi nhận).
function identifiableTextLength(text) {
  return String(text || '').replace(/[^\p{L}\p{N}]/gu, '').length;
}
function isGenericCreativeText(text) {
  return identifiableTextLength(text) < 8;
}

function groupKeyFor(pageName, text) {
  if (!text || isGenericCreativeText(text)) return null;
  return hashTextKey(String(pageName || '').toLowerCase().trim() + '\n' + text);
}

function resolveGroupArchiveId(ad) {
  const rawId = ad.ad_archive_id || ad.ad_id || ad.id || null;
  return rawId ? String(rawId) : null;
}

function groupStartMsOf(ad) {
  const ms = (ad.start_date ? ad.start_date * 1000 : ad.first_seen) || 0;
  return ms > 0 ? ms : Number.MAX_SAFE_INTEGER;
}

// Cộng dồn 1 ad vào groupMap: { [key]: { repId, repStart, size } }.
function accumulateGroupMap(groupMap, ad, text) {
  const key = groupKeyFor(ad.page_name, text);
  if (!key) return null;
  const id = resolveGroupArchiveId(ad);
  const startMs = groupStartMsOf(ad);
  const entry = groupMap[key] || (groupMap[key] = { repId: null, repStart: Number.MAX_SAFE_INTEGER, size: 0 });
  entry.size += 1;
  if (id && (startMs < entry.repStart || (startMs === entry.repStart && (entry.repId === null || String(id) < String(entry.repId))))) {
    entry.repId = id;
    entry.repStart = startMs;
  }
  return key;
}

// Thông tin nhóm cho 1 ad ở pass 2 (đọc từ groupMap toàn cục của pass 1).
// Nhóm không có ID nào (repId null) hoặc ad thiếu ID: coi là đại diện để
// không ad nào bị ẩn khỏi list chỉ vì thiếu ID (doc thiếu ID vốn đã bị
// bỏ qua ở bước ghi, nhưng pass 2 vẫn cần cờ an toàn).
function selectGroupInfo(groupMap, ad, text, archiveId) {
  const key = groupKeyFor(ad.page_name, text);
  if (!key) return { key: null, size: 1, isRep: true };
  const entry = groupMap && groupMap[key];
  if (!entry) return { key, size: 1, isRep: true };
  const isRep = !entry.repId || !archiveId || String(entry.repId) === String(archiveId);
  return { key, size: entry.size || 1, isRep };
}

function logMemory(tag) {
  const m = process.memoryUsage();
  console.log(`${tag} — RSS ${(m.rss / 1048576).toFixed(0)}MB, heap ${(m.heapUsed / 1048576).toFixed(0)}MB`);
}

// Cộng dồn 1 ad vào các map đếm toàn cục (dùng chung cho pass 1 và fallback)
function countIntoMaps(maps, ad) {
  const text = ad.normalized_text || normalize(ad.text || '');
  const domain = extractDomain(ad.link);
  const pageName = ad.page_name || 'unknown';

  maps.pageMap[pageName] = (maps.pageMap[pageName] || 0) + 1;
  if (text && !isGenericCreativeText(text)) {
    const k = hashTextKey(text);
    if (maps.textMap[k] === undefined) maps.textKeys = (maps.textKeys || 0) + 1;
    maps.textMap[k] = (maps.textMap[k] || 0) + 1; // text rỗng không đếm (tránh phình D)
  }
  if (domain) maps.domainMap[domain] = (maps.domainMap[domain] || 0) + 1;
  // Nhóm duplicate cho dedupe: pass 1 xây groupMap toàn cục, pass 2 chỉ
  // đọc (xem accumulateGroupMap). Text rỗng -> null, không gom.
  if (maps.groupMap) accumulateGroupMap(maps.groupMap, ad, text);
}

function emptyMaps() {
  return { pageMap: {}, textMap: {}, domainMap: {}, textKeys: 0, groupMap: {} };
}

// ===== NGƯỠNG PRESET (MINEA-STYLE) — phải khớp FILTER_DEFAULTS trong searchApi.js =====
// Cờ filter_signals.is_* được tính sẵn ở đây để API chỉ lọc theo 1 field
// boolean có index (đếm/lọc nhanh), thay vì chạy $or + nhiều range mỗi request.
// Đổi ngưỡng ở đây thì đổi cả bên searchApi.js cho khớp.
const PRESET_THRESHOLDS = {
  new_max_days: 5,
  scale_min_days: 3,
  scale_max_days: 7,
  scale_min_duplicates: 2,
  winner_min_days: 7,
  winner_min_duplicates: 2,
  winner_min_score: 15,
  winner_min_trending: 6,
  winner_max_seen_age_days: 14,
  winner_max_days: 9, // 0 = không giới hạn trên; 9 = khớp cửa sổ 7-9 ngày mà route FE đang ép
};

// ===== CORE ANALYZE PROCESS =====
function analyzeAllAdsGlobally(ads, globalMaps) {
  const now = Date.now();
  // Có globalMaps (từ pass 1) thì dùng luôn — số liệu đúng toàn cục kể cả khi
  // xử lý theo batch. Không có thì tự đếm trong batch (hành vi cũ).
  const maps = globalMaps || emptyMaps();
  if (!globalMaps) {
    for (const ad of ads) countIntoMaps(maps, ad);
  }
  const { pageMap, textMap, domainMap } = maps;
  const groupMap = maps.groupMap || {};

  return ads.map(ad => {
    const text = ad.normalized_text || normalize(ad.text || '');
    const domain = extractDomain(ad.link);
    // Chuẩn hóa ID: crawler có thể lưu ad_id/id thay vì ad_archive_id
    const _rawArchiveId = ad.ad_archive_id || ad.ad_id || ad.id || null;
    const archiveId = _rawArchiveId ? String(_rawArchiveId) : null; // ép String: doc cũ có ID dạng số, unique index vẫn cho lọt 2 doc cùng ID khác kiểu
    const detailSummary = summarizeDetailHistory(ad);

    const startDateMs = (ad.start_date ? ad.start_date * 1000 : ad.first_seen) || now;
    const durationDays = Math.max(1, (now - startDateMs) / (1000 * 3600 * 24));
    // Tuổi thực (phân số) chỉ dùng cho ước tính spend: ad mới lên vài giờ
    // không bị làm tròn thành 1 ngày như durationDays (dùng cho score/preset).
    const ageDaysRaw = (now - startDateMs) / (1000 * 3600 * 24);
    // Duplicates: ưu tiên collation_count của Meta (nhóm creative do Meta gom),
    // fallback về đếm trùng text tuyệt đối, rồi seen_count.
    // GIẢ ĐỊNH: collation_count là kích thước nhóm TÍNH CẢ ad này.
    // Nếu crawler của bạn đếm collation_count chỉ tính sibling (không tính chính nó)
    // thì đổi thành (ad.collation_count + 1).
    const dupFromText = text && !isGenericCreativeText(text) ? (textMap[hashTextKey(text)] || 0) : 0;
    const duplicatesCount = Math.max(1, Math.round(ad.collation_count || dupFromText || ad.seen_count || 1));
    // Gom nhóm duplicate (page + text): ad đại diện = ad chạy sớm nhất.
    // groupMap xây ở pass 1; thiếu entry (doc mới ngoài pass 1) thì coi
    // là nhóm riêng để không ẩn nhầm.
    const groupInfo = selectGroupInfo(groupMap, ad, text, archiveId);

    const spendReachData = estimateSpendAndReach(ad, ageDaysRaw, duplicatesCount, pageMap[ad.page_name] || 0);

    let score = 0;
    const pageAds = pageMap[ad.page_name] || 0;
    const clones = duplicatesCount; // nhất quán với D dùng trong mô hình spend
    const domainAds = domainMap[domain] || 0;

    if (durationDays > 3) score += 2;
    if (durationDays > 7) score += 4;
    if (durationDays > 14) score += 6;
    if (durationDays > 30) score += 8;
    if (pageAds > 5) score += 2;
    if (pageAds > 10) score += 4;
    if (clones > 3) score += 3;
    if (clones > 5) score += 5;
    if (domainAds > 5) score += 2;

    if (ad.platforms?.length > 1) score += 2;
    if (ad.videos?.length > 0) score += 3;
    if (ad.cta === 'Shop Now') score += 2;
    if (ad.cta === 'Learn more') score += 1;

    const delta = calcDelta(ad.growth_history);
    const smooth = calcSmoothDelta(ad.growth_history);
    const burst = calcBurst(ad.growth_history);
    const fallback = calcGrowthFallback(ad);

    let scalingScore = 0;
    if (delta >= 1) scalingScore += 2;
    if (delta >= 2) scalingScore += 4;
    if (delta >= 3) scalingScore += 6;
    if (smooth >= 1) scalingScore += 3;
    if (smooth >= 2) scalingScore += 5;
    if (burst >= 2) scalingScore += 4;
    if (burst >= 4) scalingScore += 7;
    if (burst >= 6) scalingScore += 10;
    if (fallback > 1) scalingScore += 2;
    if (fallback > 3) scalingScore += 4;
    if (ad.seen_count > 3) scalingScore += 2;
    if (ad.seen_count > 6) scalingScore += 4;

    const recentMinutes = (Date.now() - (ad.last_seen || now)) / (1000 * 60);
    if (recentMinutes < 60) scalingScore += 3;
    if (recentMinutes < 15) scalingScore += 5;

    let trendingScore = 0;
    if (burst > 2) trendingScore += 5;
    if (burst > 4) trendingScore += 10;
    if (delta > 1) trendingScore += 5;
    if (spendReachData.estimated_spend_usd > 200 && durationDays <= 7) trendingScore += 8;

    const spendWeight = Math.min(25, (spendReachData.estimated_spend_usd / 100) * 2);
    const reachWeight = Math.min(15, Math.log10(spendReachData.estimated_reach + 1) * 3);

    score = scalingScore * 1.2 + score + spendWeight + reachWeight + trendingScore * 0.5 + (detailSummary.detail_history_length * 0.5);
    score = Number(score.toFixed(1));

    // Cờ preset tính sẵn (xem PRESET_THRESHOLDS): API lọc theo 1 field
    // boolean có index thay vì $or + nhiều range → đếm/lọc nhanh.
    // Dùng duration_days đã làm tròn để khớp đúng field API nhìn thấy.
    const durationRounded = Math.round(durationDays);
    const seenWithinWeek = recentMinutes <= PRESET_THRESHOLDS.winner_max_seen_age_days * 24 * 60;
    const isNewAd = durationRounded <= PRESET_THRESHOLDS.new_max_days;
    const isReadyToScale = durationRounded >= PRESET_THRESHOLDS.scale_min_days
      && durationRounded <= PRESET_THRESHOLDS.scale_max_days
      && duplicatesCount >= PRESET_THRESHOLDS.scale_min_duplicates;
    const isWeeklyWinner = durationRounded >= PRESET_THRESHOLDS.winner_min_days
      && (PRESET_THRESHOLDS.winner_max_days <= 0 || durationRounded <= PRESET_THRESHOLDS.winner_max_days)
      && duplicatesCount >= PRESET_THRESHOLDS.winner_min_duplicates
      && (score >= PRESET_THRESHOLDS.winner_min_score || trendingScore >= PRESET_THRESHOLDS.winner_min_trending)
      && seenWithinWeek;

    const funnelValue = detectFunnel(ad);
    const creativeKind = classifyCreativeKind(ad, funnelValue);
    // Quốc gia chuẩn hóa (chữ thường, trim) để API lọc bằng equality thay
    // vì regex 'i' + $or: regex không phân biệt hoa thường không dùng được
    // index (bài học page_name: 314168 docs / 50.7s). countries_lower là
    // hợp của 2 nguồn, giữ đúng ngữ nghĩa $or cũ (khớp 1 trong 2 là lọt).
    const countryMentionsLower = (Array.isArray(ad.country_mentions) ? ad.country_mentions : [])
      .map((c) => String(c).trim().toLowerCase()).filter(Boolean);
    const latestCountryLower = String(detailSummary.latest_detail_country || '').trim().toLowerCase();
    // countries_lower CHUẨN HÓA về mã ISO (vd 'USA'/'United States' ->
    // 'us'): API lọc equality theo mã nên giá trị thô dạng tên không bao
    // giờ khớp ('US' trả 0 dù doc có 'united states'). 'ALL' bị loại
    // (không phải 1 nước). country_mentions_lower/
    // latest_detail_country_lower giữ nguyên dạng thô cho đường fallback.
    const countriesLower = [...new Set([
      ...countryMentionsLower.map(canonicalizeCountryValue),
      canonicalizeCountryValue(detailSummary.latest_detail_country),
    ].filter(Boolean))];
    // Fallback suy luận (chỉ khi rỗng — không ghi đè nhãn Meta). Ghi dấu
    // country_inferred để phân biệt với nhãn thật (xem inferCountryFromVietnamese).
    let countryInferred = false;
    if (countriesLower.length === 0 && inferCountryFromVietnamese(ad, domain) === 'vn') {
      countriesLower.push('vn');
      countryInferred = true;
    }
    const industryValue = classifyIndustry(ad);
    const presetPriorityValue = presetPriority(industryValue, creativeKind);
    return {
      ...ad,
      ad_archive_id: archiveId,
      domain,
      ad_library_url: archiveId ? `https://www.facebook.com/ads/library/?id=${archiveId}` : null,
      score,
      duplicates_count: duplicatesCount,
      duplicate_group_key: groupInfo.key,
      duplicate_group_size: groupInfo.size,
      is_group_rep: groupInfo.isRep,
      image_keys: mediaKeys(ad.images),
      video_keys: mediaKeys(ad.videos),
      page_ads_count: pageAds,
      domain_ads_count: domainAds,
      clones_count: clones,
      level: score >= 50 ? '🏆 LEGEND' : score >= 30 ? '🔥 WINNER' : score >= 15 ? '⚡ POTENTIAL' : 'REGULAR',
      scaling_score: scalingScore,
      trending_score: trendingScore,
      estimated_spend_usd: spendReachData.estimated_spend_usd,
      estimated_spend_min_usd: spendReachData.estimated_spend_min_usd,
      estimated_spend_max_usd: spendReachData.estimated_spend_max_usd,
      estimated_reach: spendReachData.estimated_reach,
      estimated_reach_min: spendReachData.estimated_reach_min,
      estimated_reach_max: spendReachData.estimated_reach_max,
      estimated_spend: spendReachData.estimated_spend,
      estimated_spend_source: spendReachData.estimated_spend_source,
      estimated_spend_capped: spendReachData.estimated_spend_capped,
      cpm_used: spendReachData.cpm_used,
      // Tín hiệu thô để sau này build bộ lọc "Quảng cáo mới lên",
      // "Ready to Scale", "Đang chạy mạnh". Ngưỡng chỉ là gợi ý,
      // chỉnh khi build filter thật.
      filter_signals: {
        age_days: Math.round(durationDays),
        is_new: durationDays <= 7,
        recent_minutes: Math.round(recentMinutes),
        is_recently_seen: recentMinutes < 60,
        is_new_ad: isNewAd,
        is_ready_to_scale: isReadyToScale,
        is_weekly_winner: isWeeklyWinner,
        seen_within_week: seenWithinWeek,
        duplicates_count: duplicatesCount,
        page_ads_count: pageAds,
        domain_ads_count: domainAds,
        scaling_score: scalingScore,
        trending_score: trendingScore,
        burst,
        delta,
        smooth_delta: smooth,
        spend_min_usd: spendReachData.estimated_spend_min_usd,
        spend_max_usd: spendReachData.estimated_spend_max_usd
      },
      scaling_level: scalingScore >= 12 ? '🚀 SCALING HARD' : scalingScore >= 6 ? '⚡ SCALING' : 'NORMAL',
      delta,
      smooth_delta: smooth,
      burst,
      fallback_growth: fallback,
      funnel: funnelValue,
      industry: industryValue,
      preset_priority: presetPriorityValue,
      creative_kind: creativeKind,
      creative_rank: CREATIVE_KINDS[creativeKind],
      // "Đang chạy" = còn thấy trong 7 ngày qua (last_seen, ms). Quy tắc
      // này PHẢI khớp buildActiveCondition trong searchApi.js (cùng 7
      // ngày). Trước đây analyzer không ghi is_active nên lọc trạng thái
      // ở API luôn trả 0.
      is_active: recentMinutes <= 7 * 24 * 60,
      page_name_lower: String(ad.page_name || '').toLowerCase(),
      domain_lower: String(domain || '').toLowerCase(),
      country_mentions_lower: countryMentionsLower,
      latest_detail_country_lower: latestCountryLower,
      countries_lower: countriesLower,
      country_inferred: countryInferred,
      ...detailSummary,
      keyword_frequency: (Array.isArray(ad.keyword_mentions) ? ad.keyword_mentions : []).length,
      country_frequency: (Array.isArray(ad.country_mentions) ? ad.country_mentions : []).length
    };
  });
}

// ===== DATABASE BULK WRITES =====
async function updateAdsCollection(col, analyzedAds) {
  let written = 0;
  let skipped = 0;
  // Ghi theo lát 1000: dựng ops rồi ghi ngay từng lát, không giữ toàn bộ
  // ops của cả batch trong RAM (batch lớn + doc nặng = OOM). 1000 op/lần
  // vẫn dưới message 16MB của Mongo.
  for (let s = 0; s < analyzedAds.length; s += 1000) {
    const ops = [];
    for (const ad of analyzedAds.slice(s, s + 1000)) {
      const _rawArchiveId = ad.ad_archive_id || ad.ad_id || ad.id;
      const archiveId = _rawArchiveId ? String(_rawArchiveId) : null; // ép String (xem ghi chú ở pass 2)
      if (!archiveId) { skipped++; continue; } // thiếu ID thì bỏ qua (tránh gom nhầm khi upsert)
      const { _id, ...rest } = ad; // _id của DB crawler không mang sang (tránh lỗi immutable _id)
      ops.push({
        updateOne: {
          filter: { ad_archive_id: archiveId },
          update: {
            $set: {
              ...rest,
              ad_archive_id: archiveId, // chuẩn hóa ID, giữ ad_archive_id trong document
              analyzed_at: Date.now()
            }
          },
          upsert: true
        }
      });
    }
    if (ops.length) {
      await col.bulkWrite(ops);
      written += ops.length;
    }
  }
  if (skipped) console.log(`⚠️ [DB Ads] Bỏ qua ${skipped} ad thiếu ID (không có ad_archive_id/ad_id/id).`);
  if (written) {
    console.log(`💾 [DB Ads] Đã cập nhật (Bulk Write) ${written} bản ghi.`);
  }
}

// Key ổn định của 1 media item: bỏ query/fragment của URL (oe/oh hết hạn
// làm URL đổi nhưng file vẫn là 1) — cùng file re-sign vẫn chung 1 key.
// Item có thể là string URL hoặc object {original_image_url|video_url|url|
// image|src}. URL không parse được thì giữ nguyên chuỗi.
function mediaItemUrl(item) {
  if (!item) return null;
  if (typeof item === 'string') return item.trim() || null;
  return (item.original_image_url || item.video_url || item.url || item.image || item.src || null);
}
function mediaKey(url) {
  const u = (url || '').trim();
  if (!u) return null;
  try {
    const parsed = new URL(u);
    return (parsed.origin + parsed.pathname).toLowerCase();
  } catch { return u; }
}
function mediaKeys(media) {
  const out = [];
  for (const item of Array.isArray(media) ? media : []) {
    const k = mediaKey(mediaItemUrl(item));
    if (k && !out.includes(k)) out.push(k);
  }
  return out;
}

// Gom số liệu products theo domain — chỉ giữ TỔNG, không giữ ad (nhẹ RAM,
// gom xuyên suốt bao nhiêu batch cũng được, ghi 1 lần cuối vòng).
// Ghi thêm dữ liệu cho trang product detail: title + landing + category lấy
// từ ad có score cao nhất của domain, publication_date = start_date sớm
// nhất, active_ads = số ad còn thấy trong 7 ngày.
const PRODUCT_ACTIVE_WINDOW_MS = 7 * 24 * 60 * 60 * 1000;
function accumulateProductStats(map, analyzedAds, nowMs = Date.now()) {
  for (const ad of analyzedAds) {
    if (!ad.domain) continue;
    let s = map[ad.domain];
    if (!s) s = map[ad.domain] = { domain: ad.domain, totalAds: 0, pages: new Set(), totalScore: 0, winningAdsCount: 0, totalSpendUsd: 0, activeAds: 0, firstStartDate: null, topAdScore: -Infinity, topAdHeadline: '', topAdText: '', topAdId: null, topAdLandingUrl: '', topAdIndustry: '' };
    s.totalAds++;
    s.pages.add(ad.page_name);
    s.totalScore += ad.score || 0;
    if (ad.level === '🔥 WINNER' || ad.level === '🏆 LEGEND') s.winningAdsCount++;
    s.totalSpendUsd += ad.estimated_spend_usd || 0;
    if (ad.last_seen && (nowMs - ad.last_seen) <= PRODUCT_ACTIVE_WINDOW_MS) s.activeAds++;
    if (typeof ad.start_date === 'number' && (s.firstStartDate === null || ad.start_date < s.firstStartDate)) s.firstStartDate = ad.start_date;
    if ((ad.score || 0) > s.topAdScore) {
      s.topAdScore = ad.score || 0;
      s.topAdHeadline = (ad.headline || '').trim();
      s.topAdText = (ad.text || '').trim();
      s.topAdId = ad.ad_archive_id != null ? String(ad.ad_archive_id) : null;
      s.topAdLandingUrl = ad.landing_url || ad.ad_library_url || '';
      s.topAdIndustry = ad.industry || '';
    }
  }
  return map;
}

// Title hiển thị của product: headline của ad top score; nếu rỗng thì cắt
// text của ad đó (tối đa 120 ký tự). Không có cả hai -> '' (FE fallback
// sang domain).
function productTitleFor(s) {
  if (s.topAdHeadline) return s.topAdHeadline;
  if (s.topAdText) return s.topAdText.length > 120 ? s.topAdText.slice(0, 120) : s.topAdText;
  return '';
}

// product_score = tổng score các ad + số page x 2 + số ad thắng x 5.
function computeProductScore(s) {
  const pages = s.pages.size;
  return Number((s.totalScore + pages * 2 + s.winningAdsCount * 5).toFixed(1));
}

// Snapshot top 30 product thắng của ngày chạy (theo product_score, chỉ
// tính product có >=1 ad thắng). Ghi đè theo ngày nên chạy lại cùng ngày
// không tạo bản sao. Ngày tính theo +07 (giờ chạy analyzer 11h).
const DAILY_WINNERS_LIMIT = 30;
function dailyWinnersDateString(nowMs = Date.now()) {
  return new Date(nowMs + 7 * 60 * 60 * 1000).toISOString().slice(0, 10);
}
function buildDailyWinners(map, dateStr, limit = DAILY_WINNERS_LIMIT) {
  return Object.values(map)
    .filter((s) => s.winningAdsCount > 0)
    .map((s) => ({ domain: s.domain, title: productTitleFor(s), product_score: computeProductScore(s), winning_ads: s.winningAdsCount, total_ads: s.totalAds, total_pages: s.pages.size, total_spend_usd: Number(s.totalSpendUsd.toFixed(1)) }))
    .sort((a, b) => b.product_score - a.product_score || b.winning_ads - a.winning_ads || a.domain.localeCompare(b.domain))
    .slice(0, limit)
    .map((w, i) => ({ date: dateStr, rank: i + 1, ...w }));
}
async function writeDailyWinnersSnapshot(db, map, nowMs = Date.now()) {
  const dateStr = dailyWinnersDateString(nowMs);
  const winners = buildDailyWinners(map, dateStr);
  const col = db.collection('product_winners_daily');
  await col.deleteMany({ date: dateStr });
  if (winners.length) await col.insertMany(winners);
  console.log(`🏆 [DB Daily Winners] ${dateStr}: ${winners.length} sản phẩm thắng.`);
  return winners.length;
}

// Ghi map products đã gom đủ — $set toàn bộ (không $inc).
async function flushProductsCollection(db, map) {
  const col = db.collection('products');
  const bulk = Object.values(map).map(s => {
    const pages = s.pages.size;
    const productScore = computeProductScore(s);
    return {
      updateOne: {
        filter: { domain: s.domain },
        update: {
          $set: {
            updated_at: Date.now(),
            total_spend_usd: Number(s.totalSpendUsd.toFixed(1)),
            total_ads: s.totalAds,
            total_pages: pages,
            total_score: Number(s.totalScore.toFixed(1)),
            winning_ads: s.winningAdsCount,
            product_score: productScore,
            title: productTitleFor(s),
            top_ad_id: s.topAdId,
            landing_url: s.topAdLandingUrl || null,
            category: s.topAdIndustry || null,
            publication_date: s.firstStartDate,
            active_ads: s.activeAds
          }
        },
        upsert: true
      }
    };
  });

  if (bulk.length) {
    await col.bulkWrite(bulk);
    console.log(`🔥 [DB Products] Đã tổng hợp ${bulk.length} sản phẩm/domain.`);
  }
}

async function updateProductsCollection(db, analyzedAds) {
  // Wrapper giữ tương thích: gom rồi ghi ngay (dùng khi xử lý 1 cục, không batch).
  await flushProductsCollection(db, accumulateProductStats({}, analyzedAds));
}

// ===== ELASTICSEARCH SYNC =====
async function syncToElasticsearch(esClient, analyzedAds) {
  if (!analyzedAds || analyzedAds.length === 0) return;

  let allOk = true;
  // Ghi theo lát 1000 để không giữ toàn bộ docs ES của cả batch trong RAM.
  for (let s = 0; s < analyzedAds.length; s += 1000) {
    const docs = analyzedAds.slice(s, s + 1000)
      .filter(ad => ad.ad_archive_id || ad.ad_id || ad.id)
      .map(ad => ({
      ad_archive_id: String(ad.ad_archive_id || ad.ad_id || ad.id),
      ad_library_url: ad.ad_library_url || '',
      text: ad.text || '',
      headline: ad.headline || '',
      description: ad.description || '',
      page_name: ad.page_name || '',
      domain: ad.domain || '',
      start_date: ad.start_date,
      score: ad.score ?? 0,
      level: ad.level || '',
      estimated_spend_usd: ad.estimated_spend_usd || 0,
      estimated_spend_min_usd: ad.estimated_spend_min_usd || 0,
      estimated_spend_max_usd: ad.estimated_spend_max_usd || 0,
      estimated_reach: ad.estimated_reach || 0,
      estimated_reach_min: ad.estimated_reach_min || 0,
      estimated_reach_max: ad.estimated_reach_max || 0,
      estimated_spend: ad.estimated_spend || '',
      estimated_spend_source: ad.estimated_spend_source || 'model',
      estimated_spend_capped: ad.estimated_spend_capped === true,
      duplicates_count: ad.duplicates_count || 1,
      duplicate_group_size: ad.duplicate_group_size || 1,
      is_group_rep: ad.is_group_rep !== false,
      preset_priority: ad.preset_priority ?? 1,
      duration_days: ad.duration_days || 0,
      scaling_score: ad.scaling_score || 0,
      trending_score: ad.trending_score || 0,
      funnel: ad.funnel || '',
      industry: ad.industry || 'other',
      country_inferred: ad.country_inferred === true,
      scaling_level: ad.scaling_level || ''
    }));
    if (!docs.length) continue;
    const operations = docs.flatMap(ad => [
      { index: { _index: ELASTIC_INDEX, _id: ad.ad_archive_id } },
      ad
    ]);
    try {
      const response = await esClient.bulk({ refresh: false, operations });
      if (response.errors) {
        allOk = false;
        const failed = (response.items || []).filter(it => it.index?.error).slice(0, 5);
        console.error(`⚠️ [Elasticsearch Sync] Có document lỗi (hiện 5 đầu):`, JSON.stringify(failed).slice(0, 1000));
      }
    } catch (error) {
      console.error('❌ [Elasticsearch Sync] Lỗi nghiêm trọng:', error?.message || error);
      return false;
    }
  }
  return allOk;
}

async function ensureIndexExists(esClient) {
  const exists = await esClient.indices.exists({ index: ELASTIC_INDEX });
  if (exists) return false;

  await esClient.indices.create({
    index: ELASTIC_INDEX,
    settings: {
      analysis: {
        analyzer: {
          vietnamese_analyzer: {
            type: 'custom',
            tokenizer: 'standard',
            filter: ['lowercase', 'asciifolding']
          }
        }
      }
    },
    mappings: {
      properties: {
        ad_archive_id: { type: 'keyword' },
        text: { type: 'text', analyzer: 'vietnamese_analyzer', copy_to: 'full_text_search' },
        headline: { type: 'text', analyzer: 'vietnamese_analyzer', copy_to: 'full_text_search' },
        description: { type: 'text', analyzer: 'vietnamese_analyzer', copy_to: 'full_text_search' },
        page_name: { type: 'text', analyzer: 'vietnamese_analyzer', copy_to: 'full_text_search' },
        domain: { type: 'keyword' },
        score: { type: 'double' },
        level: { type: 'keyword' },
        estimated_spend_usd: { type: 'double' },
        estimated_spend_min_usd: { type: 'double' },
        estimated_spend_max_usd: { type: 'double' },
        estimated_reach: { type: 'long' },
        estimated_reach_min: { type: 'long' },
        estimated_reach_max: { type: 'long' },
        estimated_spend: { type: 'keyword' },
        duplicates_count: { type: 'long' },
        duplicate_group_size: { type: 'long' },
        is_group_rep: { type: 'boolean' },
        preset_priority: { type: 'long' },
        duration_days: { type: 'double' },
        scaling_score: { type: 'double' },
        trending_score: { type: 'double' },
        ad_library_url: { type: 'keyword' },
        funnel: { type: 'keyword' },
        scaling_level: { type: 'keyword' },
        start_date: { type: 'date', format: 'epoch_second' },
        full_text_search: { type: 'text', analyzer: 'vietnamese_analyzer' }
      }
    }
  });
  return true;
}

// ===== MAIN: BATCH CHẠY TAY 1 LẦN/NGÀY (KHÔNG VÒNG LẶP, KHÔNG START SERVER) =====
// Analyzer chỉ tính toán: chạy 1 lượt FULL duy nhất rồi thoát. API search chạy
// riêng bằng searchApi.js (pm2, tiến trình khác). Không còn vòng lặp 60s,
// watermark hay initSearchServer — chạy tay `node analyzer.js` mỗi ngày 1 lần.
async function main() {
  const startedAt = Date.now();
  console.log(`[System] Analyzer chạy batch thủ công — PID ${process.pid}.`);

  const client = new MongoClient(MONGO_URI);

  try {
    await client.connect();
    console.log("💾 [DB] Kết nối thành công tới MongoDB Cluster.");
  } catch (dbErr) {
    console.error("❌ [DB Error] Không thể kết nối MongoDB.", dbErr.message);
    process.exit(1);
  }

  // Kết nối 2 DB (Nguồn crawler & Đích analyzer)
  const crawlerDb = client.db(CRAWLER_DB_NAME);
  const analyzerDb = client.db(ANALYZER_DB_NAME);

  const esClient = new Client({ node: ELASTICSEARCH_URL });

  try {
    await ensureIndexExists(esClient);

    const rawAdsCol = crawlerDb.collection('ads');
    const analyzedAdsCol = analyzerDb.collection('analyzed_ads');
    const productsCol = analyzerDb.collection('products');

    // Tạo index nếu chưa có index nào cùng key (tránh trùng khi tên khác nhau).
    async function ensureIndexes(col, specs) {
      let existing = [];
      try { existing = await col.indexes(); } catch (_) { /* collection mới: tạo hết */ }
      const existingKeys = new Set(existing.map(i => JSON.stringify(i.key)));
      for (const s of specs) {
        if (existingKeys.has(JSON.stringify(s.key))) {
          console.log(`⏭️ [DB Index] Bỏ qua ${s.name} (đã có index cùng key).`);
          continue;
        }
        await col.createIndex(s.key, { name: s.name, ...(s.unique ? { unique: true } : {}) });
        console.log(`✅ [DB Index] Đã tạo ${s.name}.`);
      }
    }

    // Khởi tạo các Index phân tích (giữ nguyên — API lọc theo cờ preset cần chúng)
    try {
      await ensureIndexes(analyzedAdsCol, [
        { key: { ad_archive_id: 1 }, name: "ad_archive_id_unique", unique: true },
        { key: { score: -1, analyzed_at: -1 }, name: "score_analyzed_sort" },
        { key: { estimated_spend_usd: -1 }, name: "spend_usd_sort" },
        { key: { estimated_reach: -1 }, name: "reach_sort" },
        { key: { trending_score: -1 }, name: "trending_sort" },
        { key: { duration_days: 1 }, name: "idx_duration_days" },
        { key: { last_seen: -1 }, name: "idx_last_seen" },
        { key: { 'filter_signals.is_new_ad': 1, start_date: -1 }, name: "idx_new_ad_flag" },
        { key: { 'filter_signals.is_ready_to_scale': 1, scaling_score: -1 }, name: "idx_scale_flag" },
        { key: { 'filter_signals.is_weekly_winner': 1, trending_score: -1 }, name: "idx_winner_flag" },
        { key: { 'filter_signals.is_new_ad': 1, creative_rank: 1, start_date: -1 }, name: "idx_new_ad_rank" },
        { key: { 'filter_signals.is_ready_to_scale': 1, creative_rank: 1, scaling_score: -1 }, name: "idx_scale_rank" },
        { key: { 'filter_signals.is_ready_to_scale': 1, creative_rank: 1, start_date: -1 }, name: "idx_scale_rank_start" },
        { key: { 'filter_signals.is_weekly_winner': 1, creative_rank: 1, trending_score: -1 }, name: "idx_winner_rank" },
        { key: { 'filter_signals.is_weekly_winner': 1, creative_rank: 1, start_date: -1 }, name: "idx_winner_rank_start" },
        { key: { 'filter_signals.is_weekly_winner': 1, creative_rank: 1, score: -1 }, name: "idx_winner_rank_score" },
        { key: { page_name_lower: 1, start_date: -1 }, name: "idx_page_lower_start_date" },
        { key: { domain_lower: 1, start_date: -1 }, name: "idx_domain_lower_start_date" },
        { key: { countries_lower: 1, start_date: -1 }, name: "idx_countries_lower_start_date" },
        { key: { is_active: 1, start_date: -1 }, name: "idx_is_active_start_date" },
        { key: { industry: 1, start_date: -1 }, name: "idx_industry_start_date" },
        { key: { industry: 1, score: -1 }, name: "idx_industry_score" },
        { key: { countries_lower: 1, score: -1 }, name: "idx_countries_lower_score" },
        { key: { funnel: 1, start_date: -1 }, name: "idx_funnel_start_date" },
        { key: { scaling_level: 1, start_date: -1 }, name: "idx_scaling_start_date" },
        { key: { estimated_spend: 1, start_date: -1 }, name: "idx_spend_tier_start_date" },
        { key: { duplicate_group_key: 1 }, name: "idx_group_key" },
        { key: { is_group_rep: 1, start_date: -1 }, name: "idx_group_rep_start_date" },
        { key: { preset_priority: 1, creative_rank: 1, start_date: -1 }, name: "idx_preset_priority" },
        { key: { 'filter_signals.is_new_ad': 1, preset_priority: 1, start_date: -1 }, name: "idx_new_ad_priority" },
        { key: { 'filter_signals.is_ready_to_scale': 1, preset_priority: 1, scaling_score: -1 }, name: "idx_scale_priority" },
        { key: { 'filter_signals.is_weekly_winner': 1, preset_priority: 1, trending_score: -1 }, name: "idx_winner_priority" },
        // 3 index dem preset + dedupe (da tao truc tiep tren VM 04/11/2026, ghi vao code de build lai khong mat).
        { key: { 'filter_signals.is_new_ad': 1, is_group_rep: 1, preset_priority: 1, creative_rank: 1, start_date: -1 }, name: "idx_new_ad_rep_priority" },
        { key: { 'filter_signals.is_ready_to_scale': 1, is_group_rep: 1, preset_priority: 1, creative_rank: 1, scaling_score: -1 }, name: "idx_scale_rep_rank" },
        { key: { 'filter_signals.is_weekly_winner': 1, is_group_rep: 1, preset_priority: 1, creative_rank: 1, trending_score: -1 }, name: "idx_winner_rep_rank" },
        // Index dem/tim cham 05/11: filter + is_group_rep + sort — khong co
        // is_group_rep o giua, Mongo phai FETCH toan bo doc cua filter de
        // loc dai dien (nganh Khac 145k doc -> dem khong xong).
        { key: { industry: 1, is_group_rep: 1, start_date: -1 }, name: "idx_industry_rep_start" },
        { key: { estimated_spend: 1, is_group_rep: 1, start_date: -1 }, name: "idx_spend_tier_rep_start" },
        { key: { scaling_level: 1, is_group_rep: 1, start_date: -1 }, name: "idx_scaling_rep_start" },
        { key: { scaling_score: -1, is_group_rep: 1, start_date: -1 }, name: "idx_scaling_score_rep_start" },
        { key: { domain_lower: 1, is_group_rep: 1, start_date: -1 }, name: "idx_domain_rep_start" },
        { key: { page_name_lower: 1, is_group_rep: 1, start_date: -1 }, name: "idx_page_rep_start" },
        { key: { countries_lower: 1, is_group_rep: 1, start_date: -1 }, name: "idx_countries_rep_start" },
        // 2 index dem/sort khi dedupe bat (truoc day chi co trong
        // create-indexes.js, analyzer FULL khong tu tao).
        { key: { is_group_rep: 1, estimated_spend_usd: 1 }, name: "idx_group_rep_spend" },
        // Fix retest 06/11: Inactive find (rep + last_seen + sort start_date)
        // va combo new_ads+VN (flag + rep + country + sort) — thieu 2 index
        // nay thi find phai FETCH toan bo doc cua filter (UI Inactive 0 the,
        // combo 20-35s) du count da nhanh.
        { key: { is_group_rep: 1, last_seen: -1, start_date: -1 }, name: "idx_group_rep_last_seen_start" },
        { key: { 'filter_signals.is_new_ad': 1, is_group_rep: 1, countries_lower: 1, start_date: -1 }, name: "idx_new_ad_rep_country_start" },
        { key: { image_keys: 1 }, name: "idx_image_keys" },
        { key: { video_keys: 1 }, name: "idx_video_keys" },
        { key: { page_name_lower: 1, score: -1 }, name: "idx_page_lower_score" },
        { key: { domain_lower: 1, score: -1 }, name: "idx_domain_lower_score" }
      ]);

      await ensureIndexes(productsCol, [
        { key: { domain: 1 }, name: "domain_unique", unique: true },
        { key: { product_score: -1 }, name: "product_score_sort" }
      ]);

      await ensureIndexes(analyzerDb.collection('product_winners_daily'), [
        { key: { date: 1, rank: 1 }, name: "idx_winners_date_rank", unique: true },
        { key: { domain: 1, date: -1 }, name: "idx_winners_domain_date" }
      ]);

      await ensureIndexes(rawAdsCol, [
        { key: { last_seen: -1 }, name: "idx_crawler_last_seen" },
        { key: { first_seen: -1 }, name: "idx_crawler_first_seen" }
      ]);
    } catch (indexErr) {
      console.warn("⚠️ [DB Index] Lỗi tạo index, hệ thống vẫn tiếp tục hoạt động:", indexErr.message);
    }

    console.log("⚡ [Analyzer] Bắt đầu lượt quét FULL duy nhất...");

    // PASS 1: quét nhẹ 1 lượt để đếm toàn cục (chỉ 4 field, không giữ document)
    const maps = emptyMaps();
    const statsCursor = rawAdsCol.find({}, {
      projection: { normalized_text: 1, text: 1, page_name: 1, link: 1, ad_archive_id: 1, ad_id: 1, id: 1, start_date: 1, first_seen: 1 }
    });
    for await (const a of statsCursor) countIntoMaps(maps, a);
    logMemory(`[Scanner] Pass 1 xong — unique texts (hash): ${maps.textKeys}, pages: ${Object.keys(maps.pageMap).length}, domains: ${Object.keys(maps.domainMap).length}`);

    // PASS 2: xử lý theo batch, dùng map toàn cục của pass 1
    const BATCH_SIZE = 500; // hạ xuống 200 nếu RAM vẫn căng (doc nặng)
    let batch = [];
    let totalProcessed = 0;
    const productStats = {};

    const flushBatch = async () => {
      const processedAds = analyzeAllAdsGlobally(batch, maps);
      accumulateProductStats(productStats, processedAds);
      await updateAdsCollection(analyzedAdsCol, processedAds);
      await syncToElasticsearch(esClient, processedAds);
      totalProcessed += batch.length;
      batch = []; // giải phóng RAM cho mẻ tiếp theo
      logMemory('[Analyzer] Sau mẻ');
    };

    const cursor = rawAdsCol.find({}).batchSize(BATCH_SIZE);
    for await (const rawAd of cursor) {
      batch.push(rawAd);
      if (batch.length >= BATCH_SIZE) {
        console.log(`📊 [Analyzer] Đang xử lý mẻ ${BATCH_SIZE} quảng cáo... (Đã quét ${totalProcessed})`);
        await flushBatch();
      }
    }
    if (batch.length > 0) {
      console.log(`📊 [Analyzer] Đang xử lý mẻ cuối cùng gồm ${batch.length} quảng cáo...`);
      await flushBatch();
    }

    // Products: ghi 1 lần duy nhất, đủ toàn bộ lượt quét
    await flushProductsCollection(analyzerDb, productStats);
    // Snapshot top 30 product thắng của ngày (phục vụ monthly winner).
    // Phụ trợ: lỗi ở đây chỉ cảnh báo, không làm fail cả lượt chạy.
    try {
      await writeDailyWinnersSnapshot(analyzerDb, productStats);
    } catch (snapErr) {
      console.error('⚠️ [DB Daily Winners] Không ghi được snapshot (bỏ qua):', snapErr.message);
    }

    if (totalProcessed > 0) {
      console.log(`✅ [Analyzer] Hoàn thành! Đã phân tích an toàn tổng cộng ${totalProcessed} quảng cáo.`);
    } else {
      console.log(`💤 [Scanner] Chưa có dữ liệu quảng cáo nào từ Crawler.`);
    }
    console.log(`⏱️ [Analyzer] Tổng thời gian chạy: ${((Date.now() - startedAt) / 1000).toFixed(1)}s.`);
  } finally {
    await client.close().catch(() => {});
    try { if (typeof esClient.close === 'function') await esClient.close(); } catch (_) { /* bỏ qua */ }
  }
}

main().then(() => process.exit(0)).catch((err) => {
  console.error('❌ [Analyzer] Lỗi không xử lý được:', err);
  process.exit(1);
});


// Export để test (không đổi hành vi chạy standalone: main() đã chạy ở trên).
module.exports = { classifyIndustry, INDUSTRY_CATEGORIES, PRESET_THRESHOLDS, classifyCreativeKind, CREATIVE_KINDS, presetPriority, PRESET_PRIORITY_INDUSTRIES, groupKeyFor, accumulateGroupMap, selectGroupInfo, analyzeAllAdsGlobally, canonicalizeCountryValue, isGenericCreativeText, stripDiacritics, estimateSpendAndReach, SPEND_MODEL_CAPS, inferCountryFromVietnamese, accumulateProductStats, flushProductsCollection, productTitleFor, computeProductScore, buildDailyWinners, dailyWinnersDateString, writeDailyWinnersSnapshot, DAILY_WINNERS_LIMIT, PRODUCT_ACTIVE_WINDOW_MS, mediaItemUrl, mediaKey, mediaKeys };
