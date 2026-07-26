const { MongoClient } = require('mongodb');
const { Kafka } = require('kafkajs');
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
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';

// ===== CONFIG =====
const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2';
const DB_NAME = 'fb_ads_analyzer'; // Database riêng biệt phân tích

const KAFKA_BROKERS = process.env.KAFKA_BROKERS ? [process.env.KAFKA_BROKERS] : ['localhost:9092'];
const KAFKA_TOPIC = 'fb-ads-events';
const CONSUMER_GROUP = 'analyzer-group';

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

// ===== CORE ANALYZE PROCESS =====
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
    console.log(`💾 [DB Ads] Đã Bulk Write ${bulk.length} bản ghi.`);
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

    // Điểm sản phẩm = Tổng điểm ads + (số page * 2) + (số ads winning * 5)
    const productScore = totalScore + pages * 2 + winningAdsCount * 5;

    return {
      updateOne: {
        filter: { domain: p.domain },
        update: {
          $set: {
            updated_at: Date.now(),
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
    console.log(`🔥 [DB Products] Đã cập nhật ${bulk.length} sản phẩm/domain.`);
  }
}

const logger = require('./logger');

// main().catch(err => {
//     logger.error('Sập dịch vụ Analyzer chính', err);
// });

// ===== MAIN CONSUMER PROCESS =====
// ===== MAIN CONSUMER PROCESS =====
async function main() {
  console.log("[System] Bắt đầu khởi chạy tiến trình Analyzer tổng hợp...");

  // 1. Khởi tạo thực thể Kafka tổng trước
  const kafka = new Kafka({ clientId: 'fb-analyzer-service', brokers: KAFKA_BROKERS });

  // 2. Kết nối MongoDB độc lập
  const client = new MongoClient(MONGO_URI);

  try {
    await client.connect();
    console.log("💾 [DB] Kết nối thành công tới MongoDB Cluster.");
  } catch (dbErr) {
    console.error("❌ [DB Error] Không thể kết nối MongoDB. Dừng tiến trình!", dbErr.message);
    process.exit(1); // Dừng nếu không có DB
  }

  const db = client.db(DB_NAME);

  // 3. 🔥 KÍCH HOẠT API SERVER NGAY (Bọc try-catch riêng để nếu lỗi Kafka cũ không làm sập cổng 5002)
  try {
    initSearchServer(db, kafka);
  } catch (apiErr) {
    console.error("❌ [API Error] Thất bại khi dựng cổng 5002:", apiErr.message);
  }

  // 4. KHỞI TẠO LOGGER & KAFKA CONSUMER
  try {
    await logger.initLogger('analyzer-service');
    logger.info('Dịch vụ phân tích điểm số đã Online và đang chờ Batch...');

    // Đảm bảo bóc tách chuỗi Broker chính xác tuyệt đối
    const brokers = process.env.KAFKA_BROKERS
      ? process.env.KAFKA_BROKERS.split(',')
      : ['kafka:9092'];

    const kafkaInstance = new Kafka({ clientId: 'fb-analyzer-service', brokers });

    const consumer = kafkaInstance.consumer({
      groupId: CONSUMER_GROUP,
      maxWaitTimeInMs: 75 * 1000,
      maxPollInterval: 120 * 1000,
      sessionTimeout: 30000,
      heartbeatInterval: 10000
    });

    let isConnected = false;
    let retryCount = 10;

    while (!isConnected && retryCount > 0) {
      try {
        console.log(`🔄 [Kafka Consumer] Đang kết nối tới Broker: ${brokers} (Nhóm: ${CONSUMER_GROUP})...`);
        await consumer.connect();

        // Trước khi subscribe, kiểm tra hoặc đăng ký để ép Broker tạo Topic nếu chưa có
        const admin = kafkaInstance.admin();
        await admin.connect();
        const existingTopics = await admin.listTopics();

        if (!existingTopics.includes(KAFKA_TOPIC)) {
          console.log(`📣 [Kafka] Topic ${KAFKA_TOPIC} chưa tồn tại, đang tiến hành khởi tạo tự động...`);
          await admin.createTopics({
            topics: [{ topic: KAFKA_TOPIC, numPartitions: 1, replicationFactor: 1 }]
          });
        }
        await admin.disconnect();

        // Tiến hành đăng ký lắng nghe
        await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
        isConnected = true;
        logger.info('⚡ [Kafka Consumer] Đã kết nối và đăng ký Topic thành công!');
      } catch (connErr) {
        retryCount--;
        console.warn(`⚠️ [Kafka Consumer Warning] Lỗi kết nối hoặc bầu Leader (${connErr.message}). Thử lại sau 5s...`);
        await new Promise(resolve => setTimeout(resolve, 5000));
      }
    }

    if (!isConnected) {
      throw new Error("Không thể kết nối tới Kafka Broker sau nhiều lần thử lại.");
    }

    const adsCol = db.collection('analyzed_ads');
    
    // TỐI ƯU HÓA: Đảm bảo các index quan trọng cho việc tìm kiếm đã được tạo
    console.log("🚀 [DB Index] Đang kiểm tra và khởi tạo các chỉ mục tối ưu hóa truy vấn...");
    await adsCol.createIndexes([
        { key: { ad_archive_id: 1 }, name: "ad_archive_id_unique", unique: true },
        { key: { score: -1, analyzed_at: -1 }, name: "score_analyzed_sort" },
        { key: { text: "text" }, name: "text_search" },
        { key: { domain: 1 }, name: "domain_filter" },
        { key: { level: 1 }, name: "level_filter" },
        { key: { estimated_spend: 1 }, name: "spend_filter" },
        { key: { trending_score: -1 }, name: "trending_sort" },
        { key: { funnel: 1 }, name: "funnel_filter" },
        { key: { start_date: -1 }, name: "date_sort" }
    ]);
    console.log("✅ [DB Index] Hoàn tất việc đảm bảo các chỉ mục đã sẵn sàng.");

    // TỐI ƯU HÓA: Tạo index cho collection 'products'
    const productsCol = db.collection('products');
    console.log("🚀 [DB Index] Đang kiểm tra và khởi tạo các chỉ mục cho collection 'products'...");
    await productsCol.createIndexes([
        { key: { domain: 1 }, name: "domain_unique", unique: true },
        { key: { product_score: -1 }, name: "product_score_sort" },
        { key: { winning_ads: -1 }, name: "winning_ads_sort" }
    ]);
    console.log("✅ [DB Index] Hoàn tất việc đảm bảo các chỉ mục 'products' đã sẵn sàng.");

    await consumer.run({
      eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
        const rawAdsList = [];

        logger.info(`Kafka vừa gom được ${batch.messages.length} ads. Kích hoạt analyze...`);

        for (const message of batch.messages) {
          if (!isRunning() || isStale()) break;

          try {
            const event = JSON.parse(message.value.toString());
            rawAdsList.push(event.data);
          } catch (e) {
            logger.error('❌ Thất bại khi phân rã Message dữ liệu:', e.message);
          }
        }

        if (rawAdsList.length > 0) {
          logger.info(`🚀 Bắt đầu chấm điểm tập trung cho ${rawAdsList.length} Ads cùng lúc...`);
          const processedAds = analyzeAdsBatch(rawAdsList);

          // Lưu đồng bộ trực tiếp xuống DB Analyzer
          await updateAdsCollection(adsCol, processedAds);
          await updateProductsCollection(db, processedAds);
        }

        // Xác nhận hoàn tất việc xử lý cả cụm để đẩy Offset của Kafka lên
        for (const message of batch.messages) {
          resolveOffset(message.offset);
        }
        await heartbeat();
      }
    }).then(() => {
        console.log('✅ Analyzer đã xử lý xong tất cả các tin nhắn đang chờ. Tiến trình sẽ tự động kết thúc.');
        process.exit(0);
    });

  } catch (servicesErr) {
    console.error("❌ [Critical Error] Luồng khởi tạo Consumer sập hoàn toàn:", servicesErr.message);
  }
}

// Chạy tiến trình duy nhất
main().catch(console.error);