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
  let bBase = 35.0; // Phân khúc Scale
  if (T > 30 && D <= 2) {
    bBase = 1.5;  // Phân khúc duy trì/nuôi page
  } else if (T > 10 && D <= 3) {
    bBase = 8.0;  // Phân khúc Standard Budget
  }

  const fT = 1.0 + 0.12 * Math.log(T);

  // 4. Tính toán Ngân sách Chi tiêu
  let estimatedSpend = D * T * bBase * mInd * fT;
  estimatedSpend = Math.max(estimatedSpend, 1.0 * T); // Tối thiểu $1/ngày

  // 5. Tính toán Số lượt tiếp cận
  const frequency = 1.15;
  const estimatedReach = Math.round((estimatedSpend / (cpm * frequency)) * 1000);

  let spendLevel = 'LOW';
  if (estimatedSpend >= 1000) spendLevel = 'VERY HIGH';
  else if (estimatedSpend >= 300) spendLevel = 'HIGH';
  else if (estimatedSpend >= 50) spendLevel = 'MEDIUM';

  return {
    estimated_spend_usd: Number(estimatedSpend.toFixed(1)),
    estimated_reach: estimatedReach,
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

// ===== CORE ANALYZE PROCESS =====
function analyzeAllAdsGlobally(ads) {
  const now = Date.now();
  const pageMap = {};
  const textMap = {};
  const domainMap = {};

  for (const ad of ads) {
    const text = ad.normalized_text || normalize(ad.text || '');
    const domain = extractDomain(ad.link);
    const pageName = ad.page_name || 'unknown';

    pageMap[pageName] = (pageMap[pageName] || 0) + 1;
    textMap[text] = (textMap[text] || 0) + 1;
    if (domain) domainMap[domain] = (domainMap[domain] || 0) + 1;
  }

  return ads.map(ad => {
    const text = ad.normalized_text || normalize(ad.text || '');
    const domain = extractDomain(ad.link);
    const detailSummary = summarizeDetailHistory(ad);

    const startDateMs = (ad.start_date ? ad.start_date * 1000 : ad.first_seen) || now;
    const durationDays = Math.max(1, (now - startDateMs) / (1000 * 3600 * 24));
    const duplicatesCount = textMap[text] || ad.seen_count || 1;

    const spendReachData = estimateSpendAndReach(ad, durationDays, duplicatesCount);

    let score = 0;
    const pageAds = pageMap[ad.page_name] || 0;
    const clones = textMap[text] || 0;
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

    return {
      ...ad,
      domain,
      score,
      duration_days: Math.round(durationDays),
      duplicates_count: duplicatesCount,
      level: score >= 50 ? '🏆 LEGEND' : score >= 30 ? '🔥 WINNER' : score >= 15 ? '⚡ POTENTIAL' : 'REGULAR',
      scaling_score: scalingScore,
      trending_score: trendingScore,
      estimated_spend_usd: spendReachData.estimated_spend_usd,
      estimated_reach: spendReachData.estimated_reach,
      estimated_spend: spendReachData.estimated_spend,
      cpm_used: spendReachData.cpm_used,
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
  const bulk = analyzedAds.map(ad => ({
    updateOne: {
      filter: { ad_archive_id: ad.ad_archive_id },
      update: {
        $set: {
          ...ad,
          analyzed_at: Date.now()
        }
      },
      upsert: true
    }
  }));

  if (bulk.length) {
    await col.bulkWrite(bulk);
    console.log(`💾 [DB Ads] Đã cập nhật (Bulk Write) ${bulk.length} bản ghi.`);
  }
}

async function updateProductsCollection(db, analyzedAds) {
  const col = db.collection('products');
  const map = {};

  for (const ad of analyzedAds) {
    if (!ad.domain) continue;
    if (!map[ad.domain]) {
      map[ad.domain] = { domain: ad.domain, ads: [], pages: new Set() };
    }
    map[ad.domain].ads.push(ad);
    map[ad.domain].pages.add(ad.page_name);
  }

  const bulk = Object.values(map).map(p => {
    const totalAds = p.ads.length;
    const pages = p.pages.size;
    const totalScore = p.ads.reduce((sum, ad) => sum + (ad.score || 0), 0);
    const winningAdsCount = p.ads.filter(ad => ad.level === '🔥 WINNER' || ad.level === '🏆 LEGEND').length;
    const totalSpendUsd = p.ads.reduce((sum, ad) => sum + (ad.estimated_spend_usd || 0), 0);

    const productScore = Number((totalScore + pages * 2 + winningAdsCount * 5).toFixed(1));

    return {
      updateOne: {
        filter: { domain: p.domain },
        update: {
          $set: {
            updated_at: Date.now(),
            total_spend_usd: Number(totalSpendUsd.toFixed(1))
          },
          $inc: {
            total_ads: totalAds,
            total_pages: pages,
            total_score: totalScore,
            winning_ads: winningAdsCount,
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

// ===== ELASTICSEARCH SYNC =====
async function syncToElasticsearch(esClient, analyzedAds) {
  if (!analyzedAds || analyzedAds.length === 0) return;

  const operations = analyzedAds.flatMap(ad => {
    const adId = String(ad.ad_archive_id);
    const document = {
      ad_archive_id: adId,
      text: ad.text || '',
      headline: ad.headline || '',
      description: ad.description || '',
      page_name: ad.page_name || '',
      domain: ad.domain || '',
      start_date: ad.start_date,
      score: ad.score ?? 0,
      level: ad.level || '',
      estimated_spend_usd: ad.estimated_spend_usd || 0,
      estimated_reach: ad.estimated_reach || 0,
      estimated_spend: ad.estimated_spend || '',
      funnel: ad.funnel || '',
      scaling_level: ad.scaling_level || ''
    };

    return [
      { index: { _index: ELASTIC_INDEX, _id: adId } },
      document
    ];
  });

  try {
    const response = await esClient.bulk({ refresh: false, operations });
    if (response.errors) {
      console.error(`⚠️ [Elasticsearch Sync] Có lỗi xảy ra ở một số document khi đồng bộ.`);
      return false;
    }
    return true;
  } catch (error) {
    console.error('❌ [Elasticsearch Sync] Lỗi nghiêm trọng:', error?.message || error);
    return false;
  }
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
        estimated_reach: { type: 'long' },
        estimated_spend: { type: 'keyword' },
        funnel: { type: 'keyword' },
        scaling_level: { type: 'keyword' },
        start_date: { type: 'date', format: 'epoch_second' },
        full_text_search: { type: 'text', analyzer: 'vietnamese_analyzer' }
      }
    }
  });
  return true;
}

// ===== MAIN POLLING PROCESS (THAY THẾ KAFKA) =====
async function main() {
  console.log("[System] Khởi chạy tiến trình Analyzer (Chế độ Database Polling)...");

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

  // Khởi tạo các Index phân tích
  try {
    await analyzedAdsCol.createIndexes([
      { key: { ad_archive_id: 1 }, name: "ad_archive_id_unique", unique: true },
      { key: { score: -1, analyzed_at: -1 }, name: "score_analyzed_sort" },
      { key: { estimated_spend_usd: -1 }, name: "spend_usd_sort" },
      { key: { estimated_reach: -1 }, name: "reach_sort" },
      { key: { domain: 1 }, name: "domain_filter" },
      { key: { level: 1 }, name: "level_filter" },
      { key: { trending_score: -1 }, name: "trending_sort" }
    ]);

    await productsCol.createIndexes([
      { key: { domain: 1 }, name: "domain_unique", unique: true },
      { key: { product_score: -1 }, name: "product_score_sort" }
    ]);
  } catch (indexErr) {
    console.warn("⚠️ [DB Index] Lỗi tạo index, hệ thống vẫn tiếp tục hoạt động:", indexErr.message);
  }

  console.log("⚡ [Analyzer] Hệ thống đã sẵn sàng. Bắt đầu vòng lặp quét dữ liệu...");

  // Hàm quét định kỳ thay thế Consumer
  async function runAnalyzeLoop() {
    while (true) {
      try {
        console.log(`\n🔍 [Scanner] Đang kiểm tra dữ liệu từ collection 'ads' trong db '${CRAWLER_DB_NAME}'...`);

        // Lấy toàn bộ ads hoặc có thể tuỳ chỉnh query lọc ads mới nhất dựa theo last_seen/updated_at
        const rawAds = await rawAdsCol.find({}).toArray();

        if (rawAds.length > 0) {
          console.log(`📊 [Analyzer] Đã nạp ${rawAds.length} quảng cáo gốc. Bắt đầu tính điểm...`);

          // Chạy thuật toán chấm điểm
          const processedAds = analyzeAllAdsGlobally(rawAds);

          // Cập nhật DB & Elasticsearch
          await updateAdsCollection(analyzedAdsCol, processedAds);
          await updateProductsCollection(analyzerDb, processedAds);
          await syncToElasticsearch(esClient, processedAds);

          console.log(`✅ [Analyzer] Hoàn thành phân tích vòng này!`);
        } else {
          console.log(`💤 [Scanner] Chưa có dữ liệu quảng cáo nào từ Crawler.`);
        }
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