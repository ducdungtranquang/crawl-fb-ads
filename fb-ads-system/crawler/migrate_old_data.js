const { MongoClient } = require('mongodb');

// ===== CONFIGURATION =====
const MONGO_URI = 'mongodb://127.0.0.1:27017/?directConnection=true';

const SRC_DB_NAME = 'fb_ads'; 
const SRC_COL_NAME = 'ads';        

const DEST_DB_NAME = 'fb_ads_analyzer';
const DEST_ADS_COL = 'analyzed_ads';
const DEST_PROD_COL = 'products';

// ===== ANALYZER LOGIC EMBEDDED =====
function normalize(text) {
  return text?.toLowerCase().replace(/\s+/g, ' ').trim();
}

function extractDomain(url) {
  try { return new URL(url).hostname.replace('www.', ''); } catch { return null; }
}

function calcDelta(history) {
  if (!history || history.length < 2) return 0;
  return history[history.length - 1].c - history[history.length - 2].c;
}

function calcSmoothDelta(history) {
  if (!history || history.length < 3) return 0;
  let sum = 0;
  for (let i = 1; i < history.length; i++) { sum += (history[i].c - history[i - 1].c); }
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
  return ageHours <= 0 ? 0 : ad.seen_count / ageHours;
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
 * Ước tính mức độ chi tiêu cho quảng cáo dựa trên các chỉ số gián tiếp.
 * @param {object} ad Đối tượng quảng cáo.
 * @returns {{level: 'LOW'|'MEDIUM'|'HIGH'|'VERY HIGH', score: number}}
 */
function estimateSpendLevel(ad) {
    let spendScore = 0;
    if (ad.seen_count > 5) spendScore += 1;
    if (ad.seen_count > 10) spendScore += 2;
    if (ad.platforms?.length > 1) spendScore += 1;
    if (ad.is_active) spendScore += 1;

    if (spendScore >= 4) return { level: 'VERY HIGH', score: 4 };
    if (spendScore >= 3) return { level: 'HIGH', score: 3 };
    if (spendScore >= 2) return { level: 'MEDIUM', score: 2 };
    return { level: 'LOW', score: 1 };
}

// ===== CORE ANALYZE PROCESS (Copied from analyzer.js) =====
function analyzeAdsBatch(ads) {
  const now = Date.now();
  const pageMap = {};
  const textMap = {};
  const domainMap = {};

  // Xây dựng ngữ cảnh tần suất xuất hiện theo cả cụm dữ liệu nhận về
  for (const ad of ads) {
    const text = ad.normalized_text || normalize(ad.text || '');
    const domain = extractDomain(ad.link);

    pageMap[ad.page_name] = (pageMap[ad.page_name] || 0) + 1;
    textMap[text] = (textMap[text] || 0) + 1;
    if (domain) domainMap[domain] = (domainMap[domain] || 0) + 1;
  }

  return ads.map(ad => {
    let score = 0;
    const text = ad.normalized_text || normalize(ad.text || '');
    const domain = extractDomain(ad.link);

    // 1. Ước tính mức chi tiêu
    const spend = estimateSpendLevel(ad);

    const days = (now - (ad.start_date || now) * 1000) / (1000 * 3600 * 24);
    if (days > 3) score += 2;
    if (days > 7) score += 4;
    if (days > 14) score += 6;

    const pageAds = pageMap[ad.page_name] || 0;
    if (pageAds > 5) score += 2;
    if (pageAds > 10) score += 4;

    const clones = textMap[text] || 0;
    if (clones > 3) score += 3;
    if (clones > 5) score += 5;

    const domainAds = domainMap[domain] || 0;
    if (domainAds > 5) score += 2;

    if (ad.platforms?.length > 1) score += 2;
    if (ad.videos?.length > 0) score += 2;
    if (ad.cta === 'Shop Now') score += 2;
    if (ad.cta === 'Learn more') score += 1;

    const like = ad.snapshot?.page_like_count || 0;
    if (like > 10000) score += 2;
    if (like > 50000) score += 3;

    // 2. Tính toán các chỉ số tăng trưởng
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

    const recentMinutes = (Date.now() - ad.last_seen) / (1000 * 60);
    if (recentMinutes < 60) scalingScore += 3;
    if (recentMinutes < 15) scalingScore += 5;

    // 3. Tính điểm Trending dựa trên sự đột biến
    let trendingScore = 0;
    if (burst > 2) trendingScore += 5;
    if (burst > 4) trendingScore += 10;
    if (delta > 1) trendingScore += 5;

    // 4. TÍNH ĐIỂM TỔNG HỢP CUỐI CÙNG
    // Trọng số: Tăng trưởng > Điểm cơ bản > Mức chi tiêu > Trending
    score = scalingScore * 1.5 + score + spend.score + trendingScore * 0.5;

    return {
      ...ad,
      domain,
      score,
      level: score >= 40 ? '🏆 LEGEND' : score >= 25 ? '🔥 WINNER' : score >= 15 ? '⚡ POTENTIAL' : 'REGULAR',
      scaling_score: scalingScore,
      trending_score: trendingScore,
      estimated_spend: spend.level,
      scaling_level: scalingScore >= 12 ? '🚀 SCALING HARD' : scalingScore >= 6 ? '⚡ SCALING' : 'NORMAL',
      delta,
      smooth_delta: smooth,
      burst,
      fallback_growth: fallback,
      funnel: detectFunnel(ad)
    };
  });
}

// ===== MAIN MIGRATION PROCESS =====
async function runDirectMigration() {
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  console.log('✅ Đã kết nối thành công tới MongoDB Local.');

  const srcDb = client.db(SRC_DB_NAME);
  const destDb = client.db(DEST_DB_NAME);

  const srcCol = srcDb.collection(SRC_COL_NAME);
  const destAdsCol = destDb.collection(DEST_ADS_COL);
  const destProdCol = destDb.collection(DEST_PROD_COL);

  // Tạo index trước để lát query bên Monitor UI cho mượt
  await destAdsCol.createIndex({ ad_archive_id: 1 }, { unique: true });
  await destAdsCol.createIndex({ score: -1, timestamp: -1 });

  const totalRecords = await srcCol.countDocuments();
  console.log(`📊 Tìm thấy tổng cộng ${totalRecords.toLocaleString()} bản ghi cần xử lý.`);

  const cursor = srcCol.find({});
  let buffer = [];
  let processedCount = 0;

  console.log('Bắt đầu quét phân rã và tính toán điểm trực tiếp...');

  while (await cursor.hasNext()) {
    const rawAd = await cursor.next();
    buffer.push(rawAd);

    // Gom cụm 1000 bản ghi xử lý một lần để tối ưu RAM và Bulk Write
    if (buffer.length >= 1000) {
      await processAndSave(buffer, destAdsCol, destProdCol);
      processedCount += buffer.length;
      console.log(`⏩ Tiến độ: Đã phân tích & lưu thành công ${processedCount.toLocaleString()} / ${totalRecords.toLocaleString()} bản ghi.`);
      buffer = []; // Clear buffer
    }
  }

  // Xử lý nốt số lượng dư còn lại trong mảng
  if (buffer.length > 0) {
    await processAndSave(buffer, destAdsCol, destProdCol);
    processedCount += buffer.length;
    console.log(`🏁 Hoàn thành xử lý mớ dư cuối cùng. Tổng số bản ghi thực tế: ${processedCount.toLocaleString()}`);
  }

  console.log('🎉 QUÁ TRÌNH MIGRATION TRỰC TIẾP HOÀN THÀNH ĐẸP ĐẼ!');
  await client.close();
}

async function processAndSave(rawBatch, adsCol, prodCol) {
  // 1. Chấm điểm trực tiếp bằng hàm chuyên dụng
  const analyzedBatch = analyzeAdsBatch(rawBatch);

  // 2. Chuẩn bị Bulk Write cho bảng dán nhãn Ads độc nhất
  const adsBulkOps = analyzedBatch
    .filter(ad => ad && ad.ad_archive_id) // Chống rỗng ID
    .map(ad => {
      const { _id, ...adDataWithoutId } = ad; 

      return {
        updateOne: {
          filter: { ad_archive_id: ad.ad_archive_id },
          update: { 
            $set: { 
              ...adDataWithoutId, // 🔥 Chỉ update những trường còn lại, KHÔNG CÓ _id
              analyzed_at: Date.now() 
            } 
          },
          upsert: true
        }
      };
    });

  if (adsBulkOps.length > 0) {
    await adsCol.bulkWrite(adsBulkOps, { ordered: false });
  }

  // 3. Chuẩn bị Bulk Write cho bảng tổng hợp Domain (Products)
  const productMap = {};
  for (const ad of analyzedBatch) {
    if (!ad.domain) continue;
    if (!productMap[ad.domain]) {
      productMap[ad.domain] = { domain: ad.domain, ads: [], pages: new Set() };
    }
    productMap[ad.domain].ads.push(ad);
    productMap[ad.domain].pages.add(ad.page_name);
  }

  const prodBulkOps = Object.values(productMap).map(p => {
    const totalAds = p.ads.length;
    const pages = p.pages.size;
    const totalScore = p.ads.reduce((sum, ad) => sum + (ad.score || 0), 0);
    const winningAdsCount = p.ads.filter(ad => ad.level === '🔥 WINNER' || ad.level === '🏆 LEGEND').length;

    // Điểm sản phẩm = Tổng điểm ads + (số page * 2) + (số ads winning * 5)
    const productScore = totalScore + pages * 2 + winningAdsCount * 5;

    return {
      updateOne: {
        filter: { domain: p.domain },
        update: {
          $set: { updated_at: Date.now() },
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

  if (prodBulkOps.length > 0) {
    await prodCol.bulkWrite(prodBulkOps, { ordered: false });
  }
}

// Kích hoạt tiến trình
runDirectMigration().catch(console.error);