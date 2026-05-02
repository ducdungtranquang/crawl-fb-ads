const { MongoClient } = require('mongodb');

// ===== CONFIG =====
const MONGO_URI = 'mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2';
const DB_NAME = 'fb_ads';

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

// ===== SCALING (REALISTIC) =====

// spike gần nhất
function calcDelta(history) {
  if (!history || history.length < 2) return 0;
  const last = history[history.length - 1];
  const prev = history[history.length - 2];
  return last.c - prev.c;
}

// trend trung bình
function calcSmoothDelta(history) {
  if (!history || history.length < 3) return 0;

  let sum = 0;
  for (let i = 1; i < history.length; i++) {
    sum += (history[i].c - history[i - 1].c);
  }

  return sum / (history.length - 1);
}

// burst window (QUAN TRỌNG)
function calcBurst(history, window = 5) {
  if (!history || history.length < window) return 0;

  const slice = history.slice(-window);
  return slice[slice.length - 1].c - slice[0].c;
}

// fallback nếu history yếu
function calcGrowthFallback(ad) {
  if (!ad.first_seen || !ad.seen_count) return 0;

  const ageHours =
    (Date.now() - ad.first_seen) / (1000 * 3600);

  if (ageHours <= 0) return 0;

  return ad.seen_count / ageHours;
}

// ===== FUNNEL =====
function detectFunnel(ad) {
  const text = (ad.text || '').toLowerCase();
  const link = (ad.link || '').toLowerCase();

  if (
    text.includes('free') ||
    text.includes('ebook') ||
    text.includes('training') ||
    link.includes('lead') ||
    link.includes('form')
  ) return 'LEADGEN';

  if (
    text.includes('shipping') ||
    text.includes('cod') ||
    text.includes('50% off')
  ) return 'DROPSHIP';

  if (
    ad.cta === 'Shop Now' ||
    link.includes('product') ||
    link.includes('cart')
  ) return 'ECOM';

  return 'UNKNOWN';
}

// ===== ANALYZE =====
function analyzeAds(ads) {
  const now = Date.now();

  const pageMap = {};
  const textMap = {};
  const domainMap = {};

  // ===== BUILD CONTEXT =====
  for (const ad of ads) {
    const text = ad.normalized_text || normalize(ad.text || '');
    const domain = extractDomain(ad.link);

    pageMap[ad.page_name] = (pageMap[ad.page_name] || 0) + 1;
    textMap[text] = (textMap[text] || 0) + 1;

    if (domain) {
      domainMap[domain] = (domainMap[domain] || 0) + 1;
    }
  }

  // ===== SCORING =====
  return ads.map(ad => {
    let score = 0;

    const text = ad.normalized_text || normalize(ad.text || '');
    const domain = extractDomain(ad.link);

    // ===== LONGEVITY =====
    const days =
      (now - (ad.start_date || now) * 1000) / (1000 * 3600 * 24);

    if (days > 3) score += 2;
    if (days > 7) score += 4;
    if (days > 14) score += 6;

    // ===== PAGE SCALE =====
    const pageAds = pageMap[ad.page_name] || 0;
    if (pageAds > 5) score += 2;
    if (pageAds > 10) score += 4;

    // ===== CLONE =====
    const clones = textMap[text] || 0;
    if (clones > 3) score += 3;
    if (clones > 5) score += 5;

    // ===== DOMAIN SCALE =====
    const domainAds = domainMap[domain] || 0;
    if (domainAds > 5) score += 2;

    // ===== PLATFORM =====
    if (ad.platforms?.length > 1) score += 2;

    // ===== MEDIA =====
    if (ad.videos?.length > 0) score += 2;

    // ===== CTA =====
    if (ad.cta === 'Shop Now') score += 2;
    if (ad.cta === 'Learn more') score += 1;

    // ===== PAGE SIZE =====
    const like = ad.snapshot?.page_like_count || 0;
    if (like > 10000) score += 2;
    if (like > 50000) score += 3;

    // ===== SCALING (FIXED) =====
    const delta = calcDelta(ad.growth_history);
    const smooth = calcSmoothDelta(ad.growth_history);
    const burst = calcBurst(ad.growth_history);
    const fallback = calcGrowthFallback(ad);

    let scalingScore = 0;

    // spike
    if (delta >= 1) scalingScore += 2;
    if (delta >= 2) scalingScore += 4;
    if (delta >= 3) scalingScore += 6;

    // trend
    if (smooth >= 1) scalingScore += 3;
    if (smooth >= 2) scalingScore += 5;

    // burst
    if (burst >= 2) scalingScore += 4;
    if (burst >= 4) scalingScore += 7;
    if (burst >= 6) scalingScore += 10;

    // fallback
    if (fallback > 1) scalingScore += 2;
    if (fallback > 3) scalingScore += 4;

    // activity
    if (ad.seen_count > 3) scalingScore += 2;
    if (ad.seen_count > 6) scalingScore += 4;

    // freshness
    const recentMinutes =
      (Date.now() - ad.last_seen) / (1000 * 60);

    if (recentMinutes < 60) scalingScore += 3;
    if (recentMinutes < 15) scalingScore += 5;

    score += scalingScore;

    return {
      ...ad,
      domain,

      // main score
      score,
      level:
        score >= 12 ? '🔥 WINNER' :
          score >= 7 ? '⚡ GOOD' :
            'LOW',

      // scaling
      scaling_score: scalingScore,
      scaling_level:
        scalingScore >= 12 ? '🚀 SCALING HARD' :
          scalingScore >= 6 ? '⚡ SCALING' :
            'NORMAL',

      // debug
      delta,
      smooth_delta: smooth,
      burst,
      fallback_growth: fallback,

      funnel: detectFunnel(ad)
    };
  });
}

// ===== UPDATE ADS =====
async function updateAds(col, ads) {
  const bulk = ads.map(ad => ({
    updateOne: {
      filter: { ad_archive_id: ad.ad_archive_id },
      update: {
        $set: {
          score: ad.score,
          level: ad.level,
          scaling_score: ad.scaling_score,
          scaling_level: ad.scaling_level,
          funnel: ad.funnel,
          domain: ad.domain,
          delta: ad.delta,
          smooth_delta: ad.smooth_delta,
          burst: ad.burst,
          fallback_growth: ad.fallback_growth,
          analyzed_at: Date.now()
        }
      }
    }
  }));

  if (bulk.length) {
    await col.bulkWrite(bulk);
    console.log('✅ Ads updated:', bulk.length);
  }
}

// ===== UPDATE PRODUCTS =====
async function updateProducts(db, ads) {
  const col = db.collection('products');

  const map = {};

  for (const ad of ads) {
    if (!ad.domain) continue;

    if (!map[ad.domain]) {
      map[ad.domain] = {
        domain: ad.domain,
        ads: [],
        pages: new Set()
      };
    }

    map[ad.domain].ads.push(ad);
    map[ad.domain].pages.add(ad.page_name);
  }

  const bulk = Object.values(map).map(p => {
    const totalAds = p.ads.length;
    const pages = p.pages.size;

    const score = totalAds + pages * 2;

    return {
      updateOne: {
        filter: { domain: p.domain },
        update: {
          $set: {
            domain: p.domain,
            total_ads: totalAds,
            pages,
            score,
            updated_at: Date.now()
          }
        },
        upsert: true
      }
    };
  });

  if (bulk.length) {
    await col.bulkWrite(bulk);
    console.log('🔥 Products updated:', bulk.length);
  }
}

// ===== MAIN =====
async function run() {
  const client = new MongoClient(MONGO_URI);
  await client.connect();

  const db = client.db(DB_NAME);
  const col = db.collection('ads');

  console.log('🚀 Start analyzing...');

  const ads = await col.find().toArray();

  console.log('📦 Loaded ads:', ads.length);

  const analyzed = analyzeAds(ads);

  await updateAds(col, analyzed);
  await updateProducts(db, analyzed);

  console.log('🏆 ANALYZE DONE');

  await client.close();
}

run();