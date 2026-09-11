const { MongoClient } = require('mongodb');

const MONGO_URI = 'mongodb://127.0.0.1:27017/?directConnection=true';

const SRC_DB_NAME = 'fb_ads';
const SRC_COL_NAME = 'ads';

const DEST_DB_NAME = 'fb_ads_analyzer';
const DEST_ADS_COL = 'analyzed_ads';
const DEST_PROD_COL = 'products';

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
    const spend = estimateSpendLevel(ad);
    const detailSummary = summarizeDetailHistory(ad);

    let score = 0;
    const pageAds = pageMap[ad.page_name] || 0;
    const clones = textMap[text] || 0;
    const domainAds = domainMap[domain] || 0;

    const days = (now - (ad.start_date || now) * 1000) / (1000 * 3600 * 24);
    if (days > 3) score += 2;
    if (days > 7) score += 4;
    if (days > 14) score += 6;
    if (pageAds > 5) score += 2;
    if (pageAds > 10) score += 4;
    if (clones > 3) score += 3;
    if (clones > 5) score += 5;
    if (domainAds > 5) score += 2;
    if (ad.platforms?.length > 1) score += 2;
    if (ad.videos?.length > 0) score += 2;
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

    const recentMinutes = (Date.now() - ad.last_seen) / (1000 * 60);
    if (recentMinutes < 60) scalingScore += 3;
    if (recentMinutes < 15) scalingScore += 5;

    let trendingScore = 0;
    if (burst > 2) trendingScore += 5;
    if (burst > 4) trendingScore += 10;
    if (delta > 1) trendingScore += 5;

    score = scalingScore * 1.5 + score + spend.score + trendingScore * 0.5 + (detailSummary.detail_history_length * 0.5);

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
      funnel: detectFunnel(ad),
      ...detailSummary
    };
  });
}

async function processAndSave(rawBatch, adsCol, prodCol) {
  const analyzedBatch = analyzeAllAdsGlobally(rawBatch);

  const adsBulkOps = analyzedBatch
    .filter(ad => ad && ad.ad_archive_id)
    .map(ad => {
      const { _id, ...adDataWithoutId } = ad;
      return {
        updateOne: {
          filter: { ad_archive_id: ad.ad_archive_id },
          update: {
            $set: {
              ...adDataWithoutId,
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

async function runDirectMigration() {
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  console.log('✅ Đã kết nối thành công tới MongoDB Local.');

  const srcDb = client.db(SRC_DB_NAME);
  const destDb = client.db(DEST_DB_NAME);

  const srcCol = srcDb.collection(SRC_COL_NAME);
  const destAdsCol = destDb.collection(DEST_ADS_COL);
  const destProdCol = destDb.collection(DEST_PROD_COL);

  await destAdsCol.createIndex({ ad_archive_id: 1 }, { unique: true });
  await destAdsCol.createIndex({ score: -1, analyzed_at: -1 });

  const totalRecords = await srcCol.countDocuments();
  console.log(`📊 Tìm thấy tổng cộng ${totalRecords.toLocaleString()} bản ghi cần xử lý.`);

  const rawAds = await srcCol.find({}).toArray();
  const chunks = [];
  for (let i = 0; i < rawAds.length; i += 1000) {
    chunks.push(rawAds.slice(i, i + 1000));
  }

  let processedCount = 0;
  for (const batch of chunks) {
    await processAndSave(batch, destAdsCol, destProdCol);
    processedCount += batch.length;
    console.log(`⏩ Tiến độ: Đã phân tích & lưu thành công ${processedCount.toLocaleString()} / ${totalRecords.toLocaleString()} bản ghi.`);
  }

  console.log('🎉 QUÁ TRÌNH MIGRATION TRỰC TIẾP HOÀN THÀNH ĐẸP ĐẼ!');
  await client.close();
}

runDirectMigration().catch(console.error);