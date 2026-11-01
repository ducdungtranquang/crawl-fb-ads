const { MongoClient } = require('mongodb');
const { Client } = require('@elastic/elasticsearch');
const { initSearchServer } = require('./searchApi');

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

// Mức thời gian nghỉ giữa các vòng lặp phân tích (Millisecond)
const POLLING_INTERVAL = 60 * 1000; // 1 phút

// ===== UTILS =====
function normalize(text) {
  return text?.toLowerCase().replace(/\s+/g, ' ').trim();
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
function estimateSpendAndReach(ad, durationDays = 1, duplicatesCount = 1) {
  const T = Math.max(1, Math.round(durationDays));
  const D = Math.max(1, Math.round(duplicatesCount || ad.seen_count || 1));

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
  const spendFor = (b) => Math.max(D * T * b * mInd * fT, 1.0 * T); // Tối thiểu $1/ngày
  const estimatedSpend = spendFor(bBase);
  const tierIdx = TIERS.indexOf(bBase);
  const bLow = tierIdx > 0 ? TIERS[tierIdx - 1] : bBase * 0.5;
  const bHigh = tierIdx < TIERS.length - 1 ? TIERS[tierIdx + 1] : bBase * 1.5;
  const spendMin = Math.min(estimatedSpend, spendFor(bLow));
  const spendMax = Math.max(estimatedSpend, spendFor(bHigh));

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
  if (text) {
    const k = hashTextKey(text);
    if (maps.textMap[k] === undefined) maps.textKeys = (maps.textKeys || 0) + 1;
    maps.textMap[k] = (maps.textMap[k] || 0) + 1; // text rỗng không đếm (tránh phình D)
  }
  if (domain) maps.domainMap[domain] = (maps.domainMap[domain] || 0) + 1;
}

function emptyMaps() {
  return { pageMap: {}, textMap: {}, domainMap: {}, textKeys: 0 };
}

// ===== NGƯỠNG PRESET (MINEA-STYLE) — phải khớp FILTER_DEFAULTS trong searchApi.js =====
// Cờ filter_signals.is_* được tính sẵn ở đây để API chỉ lọc theo 1 field
// boolean có index (đếm/lọc nhanh), thay vì chạy $or + nhiều range mỗi request.
// Đổi ngưỡng ở đây thì đổi cả bên searchApi.js cho khớp.
const PRESET_THRESHOLDS = {
  new_max_days: 3,
  scale_min_days: 3,
  scale_max_days: 7,
  scale_min_duplicates: 2,
  winner_min_days: 7,
  winner_min_duplicates: 2,
  winner_min_score: 20,
  winner_min_trending: 8,
  winner_max_seen_age_days: 7,
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

  return ads.map(ad => {
    const text = ad.normalized_text || normalize(ad.text || '');
    const domain = extractDomain(ad.link);
    // Chuẩn hóa ID: crawler có thể lưu ad_id/id thay vì ad_archive_id
    const archiveId = ad.ad_archive_id || ad.ad_id || ad.id || null;
    const detailSummary = summarizeDetailHistory(ad);

    const startDateMs = (ad.start_date ? ad.start_date * 1000 : ad.first_seen) || now;
    const durationDays = Math.max(1, (now - startDateMs) / (1000 * 3600 * 24));
    // Duplicates: ưu tiên collation_count của Meta (nhóm creative do Meta gom),
    // fallback về đếm trùng text tuyệt đối, rồi seen_count.
    // GIẢ ĐỊNH: collation_count là kích thước nhóm TÍNH CẢ ad này.
    // Nếu crawler của bạn đếm collation_count chỉ tính sibling (không tính chính nó)
    // thì đổi thành (ad.collation_count + 1).
    const dupFromText = text ? (textMap[hashTextKey(text)] || 0) : 0;
    const duplicatesCount = Math.max(1, Math.round(ad.collation_count || dupFromText || ad.seen_count || 1));

    const spendReachData = estimateSpendAndReach(ad, durationDays, duplicatesCount);

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
      && duplicatesCount >= PRESET_THRESHOLDS.winner_min_duplicates
      && (score >= PRESET_THRESHOLDS.winner_min_score || trendingScore >= PRESET_THRESHOLDS.winner_min_trending)
      && seenWithinWeek;

    return {
      ...ad,
      ad_archive_id: archiveId,
      domain,
      ad_library_url: archiveId ? `https://www.facebook.com/ads/library/?id=${archiveId}` : null,
      score,
      duration_days: Math.round(durationDays),
      duplicates_count: duplicatesCount,
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
      funnel: detectFunnel(ad),
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
      const archiveId = ad.ad_archive_id || ad.ad_id || ad.id;
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

// Gom số liệu products theo domain — chỉ giữ TỔNG, không giữ ad (nhẹ RAM,
// gom xuyên suốt bao nhiêu batch cũng được, ghi 1 lần cuối vòng).
function accumulateProductStats(map, analyzedAds) {
  for (const ad of analyzedAds) {
    if (!ad.domain) continue;
    let s = map[ad.domain];
    if (!s) s = map[ad.domain] = { domain: ad.domain, totalAds: 0, pages: new Set(), totalScore: 0, winningAdsCount: 0, totalSpendUsd: 0 };
    s.totalAds++;
    s.pages.add(ad.page_name);
    s.totalScore += ad.score || 0;
    if (ad.level === '🔥 WINNER' || ad.level === '🏆 LEGEND') s.winningAdsCount++;
    s.totalSpendUsd += ad.estimated_spend_usd || 0;
  }
  return map;
}

// Ghi map products đã gom đủ — $set toàn bộ (không $inc).
async function flushProductsCollection(db, map) {
  const col = db.collection('products');
  const bulk = Object.values(map).map(s => {
    const pages = s.pages.size;
    const productScore = Number((s.totalScore + pages * 2 + s.winningAdsCount * 5).toFixed(1));
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
            product_score: productScore
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
      duplicates_count: ad.duplicates_count || 1,
      duration_days: ad.duration_days || 0,
      scaling_score: ad.scaling_score || 0,
      trending_score: ad.trending_score || 0,
      funnel: ad.funnel || '',
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

// Vòng lặp incremental: vòng FULL quét toàn bộ, vòng thường chỉ lấy doc
// mới/thay đổi theo watermark (last_seen/first_seen, đơn vị ms như analyzer
// vẫn dùng). Vòng đầu sau khi (re)start luôn FULL để build map + bắt kịp.
function isFullLoop(loopCount, watermark, every) {
  return watermark === 0 || loopCount % every === 0;
}

function buildLoopQuery(isFull, watermark) {
  if (isFull) return {};
  return { $or: [{ last_seen: { $gte: watermark } }, { first_seen: { $gte: watermark } }] };
}

// ===== MAIN POLLING PROCESS (THAY THẾ KAFKA) =====
async function main() {
  console.log(`[System] Khởi chạy tiến trình Analyzer (Chế độ Database Polling) — PID ${process.pid}. Chỉ chạy 1 bản analyzer duy nhất, bản trùng sẽ nhân đôi tải và nhân đôi ghi.`);

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
  await ensureIndexExists(esClient);

  // Kích hoạt API Server (Nếu có dùng, loại bỏ param kafka)
  try {
    initSearchServer(analyzerDb);
  } catch (apiErr) {
    console.error("⚠️ [API Error] Lỗi khi dựng API Server (Nếu không dùng có thể bỏ qua):", apiErr.message);
  }

  const rawAdsCol = crawlerDb.collection('ads');
  const analyzedAdsCol = analyzerDb.collection('analyzed_ads');
  const productsCol = analyzerDb.collection('products');

  // Tạo index nếu chưa có index nào cùng key (tránh trùng khi tên khác nhau).
  // Index trên collection crawler (last_seen/first_seen) là bắt buộc để vòng
  // incremental không phải quét toàn bộ collection mỗi 60s.
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

  // Khởi tạo các Index phân tích
  try {
    await ensureIndexes(analyzedAdsCol, [
      { key: { ad_archive_id: 1 }, name: "ad_archive_id_unique", unique: true },
      { key: { score: -1, analyzed_at: -1 }, name: "score_analyzed_sort" },
      { key: { estimated_spend_usd: -1 }, name: "spend_usd_sort" },
      { key: { estimated_reach: -1 }, name: "reach_sort" },
      { key: { domain: 1 }, name: "domain_filter" },
      { key: { level: 1 }, name: "level_filter" },
      { key: { trending_score: -1 }, name: "trending_sort" },
      { key: { duration_days: 1 }, name: "idx_duration_days" },
      { key: { last_seen: -1 }, name: "idx_last_seen" },
      { key: { 'filter_signals.is_new_ad': 1, start_date: -1 }, name: "idx_new_ad_flag" },
      { key: { 'filter_signals.is_ready_to_scale': 1, scaling_score: -1 }, name: "idx_scale_flag" },
      { key: { 'filter_signals.is_weekly_winner': 1, trending_score: -1 }, name: "idx_winner_flag" }
    ]);

    await ensureIndexes(productsCol, [
      { key: { domain: 1 }, name: "domain_unique", unique: true },
      { key: { product_score: -1 }, name: "product_score_sort" }
    ]);

    await ensureIndexes(rawAdsCol, [
      { key: { last_seen: -1 }, name: "idx_crawler_last_seen" },
      { key: { first_seen: -1 }, name: "idx_crawler_first_seen" }
    ]);
  } catch (indexErr) {
    console.warn("⚠️ [DB Index] Lỗi tạo index, hệ thống vẫn tiếp tục hoạt động:", indexErr.message);
  }

  console.log("⚡ [Analyzer] Hệ thống đã sẵn sàng. Bắt đầu vòng lặp quét dữ liệu...");

  // Trạng thái vòng lặp incremental: quét toàn bộ (FULL) mỗi FULL_RESCAN_EVERY
  // vòng, các vòng còn lại chỉ xử lý doc mới/thay đổi. Quét toàn bộ mỗi 60s
  // là nguyên nhân CPU 79% + sập VM khi collection lớn.
  const FULL_RESCAN_EVERY = 30; // 30 vòng x 60s ≈ 30 phút quét toàn bộ 1 lần
  const WATERMARK_OVERLAP_MS = 5 * 60 * 1000; // chồng 5 phút, tránh sót doc cập nhật muộn
  let loopCount = 0;
  let watermark = 0; // 0 = vòng đầu quét toàn bộ
  let cachedMaps = null;

  // Hàm quét định kỳ thay thế Consumer — 2 lượt, bảo vệ RAM mà số vẫn chuẩn:
  // lượt 1 chỉ đếm toàn cục (projection nhẹ), lượt 2 xử lý theo batch.
  async function runAnalyzeLoop() {
    while (true) {
      const loopStart = Date.now();
      loopCount++;
      const isFull = isFullLoop(loopCount, watermark, FULL_RESCAN_EVERY);
      try {
        console.log(`\n🔍 [Scanner] Vòng #${loopCount} (${isFull ? 'FULL' : 'incremental'}) — collection 'ads' trong db '${CRAWLER_DB_NAME}'...`);

        // Vòng FULL: pass 1 đếm toàn cục rồi xử lý hết. Vòng incremental:
        // chỉ doc mới/thay đổi (last_seen/first_seen >= watermark), dùng lại
        // map toàn cục của vòng FULL gần nhất — duplicate của text hoàn toàn
        // mới có thể đếm thiếu tới vòng FULL sau (collation_count/seen_count
        // vẫn fallback). Doc thiếu cả 2 field thời gian chỉ refresh ở vòng FULL.
        let maps;
        let loopQuery;
        if (isFull) {
          // PASS 1: quét nhẹ 1 lượt để đếm toàn cục (chỉ 4 field, không giữ document)
          maps = emptyMaps();
          const statsCursor = rawAdsCol.find({}, {
            projection: { normalized_text: 1, text: 1, page_name: 1, link: 1 }
          });
          for await (const a of statsCursor) countIntoMaps(maps, a);
          logMemory(`[Scanner] Pass 1 xong — unique texts (hash): ${maps.textKeys}, pages: ${Object.keys(maps.pageMap).length}, domains: ${Object.keys(maps.domainMap).length}`);
          cachedMaps = maps;
          loopQuery = {};
        } else {
          maps = cachedMaps || emptyMaps();
          loopQuery = buildLoopQuery(false, watermark);
        }

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

        const cursor = rawAdsCol.find(loopQuery).batchSize(BATCH_SIZE);
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

        // Products: CHỈ ghi ở vòng FULL. Vòng incremental chỉ thấy 1 lát doc
        // nên $set toàn bộ sẽ làm tụt tổng products về đúng lát đó.
        if (isFull) {
          // Products: ghi 1 lần duy nhất, đủ toàn bộ vòng
          await flushProductsCollection(analyzerDb, productStats);
        } else {
          console.log(`⏭️ [DB Products] Bỏ qua tổng hợp products ở vòng incremental (chỉ vòng FULL mới ghi).`);
        }

        if (totalProcessed > 0) {
          console.log(`✅ [Analyzer] Hoàn thành vòng này! Đã phân tích an toàn tổng cộng ${totalProcessed} quảng cáo.`);
        } else {
          console.log(`💤 [Scanner] Chưa có dữ liệu quảng cáo nào từ Crawler.`);
        }
        const loopMs = Date.now() - loopStart;
        console.log(`⏱️ [Scanner] Vòng #${loopCount} xong trong ${(loopMs / 1000).toFixed(1)}s, xử lý ${totalProcessed} ad.`);
        if (loopMs > POLLING_INTERVAL) console.warn(`⚠️ [Scanner] Vòng chạy lâu hơn POLLING_INTERVAL (${POLLING_INTERVAL / 1000}s) — CPU sẽ bị chiếm gần như liên tục, cân nhắc tăng interval.`);
        // Chỉ tiến watermark khi vòng thành công (lỗi thì giữ nguyên để vòng sau quét lại kèm chồng lấn).
        watermark = loopStart - WATERMARK_OVERLAP_MS;
      } catch (err) {
        console.error("❌ [Loop Error] Có lỗi xảy ra trong quá trình phân tích vòng lặp:", err.message);
      }

      // Nghỉ chờ POLLING_INTERVAL rồi chạy tiếp
      console.log(`⏳ Tạm dừng hệ thống ${POLLING_INTERVAL / 1000} giây trước khi quét vòng tiếp theo...`);
      await new Promise(resolve => setTimeout(resolve, POLLING_INTERVAL));
    }
  }

  // Chạy vòng lặp vô tận
  runAnalyzeLoop();
}

main().catch(console.error);
