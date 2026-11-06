/**
 * Tạo/cập nhật index cho fb_ads_analyzer.analyzed_ads (+ products) để API
 * search hết chậm. Chạy 1 lần sau mỗi lần thêm index mới:
 *   node create-indexes.js
 * MONGO_URI lấy từ env, mặc định trỏ localhost như analyzer.js (trên VM
 * DB thì chạy qua `docker exec mongodb-core` hoặc đặt MONGO_URI trỏ
 * 192.168.100.7:27017 như crawler dùng).
 *
 * Danh sách này là HỢP của mọi index analyzer.js tự tạo (ensureIndexes)
 * + index cho các đường lọc/sort mới của searchApi (country, industry,
 * is_active, funnel, scaling, spend, lower fields, rank). Chạy file này
 * thì không cần đợi analyzer chạy mới có index; analyzer chạy lại cũng
 * tự bỏ qua index đã có cùng key.
 *
 * Quy tắc thứ tự field: filter trước (equality) — sort sau, để Mongo
 * vừa lọc vừa trả đúng thứ tự, không sort lại trong RAM. Bài học đã
 * trả bằng số liệu: regex 'i' + $or không dùng được index
 * (page_name: 314168 docs / 50.7s; country=VN ~80s).
 *
 * An toàn: chỉ tạo thêm, KHÔNG drop index nào. Index cũ
 * idx_page_start_date / idx_domain_start_date (field gốc, case-sensitive)
 * nay thừa sau khi có *_lower, nhưng vẫn có thể phục vụ match chính xác
 * — muốn dọn thì drop tay bằng mongosh (xem cuối file), không tự động.
 * Build index trên collection lớn có thể mất vài phút, cứ để chạy nền.
 * Mỗi index tạo lỗi thì ghi lỗi và đi tiếp, không dừng cả lượt.
 */
const { MongoClient } = require('mongodb');

const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2';
const DB_NAME = 'fb_ads_analyzer';
const COL_NAME = 'analyzed_ads';
const PRODUCTS_COL = 'products';

const INDEXES = [
  // Tra cứu theo ID (ES trả list ad_archive_id -> query $in + count).
  { key: { ad_archive_id: 1 }, name: 'idx_ad_archive_id' },
  // Sort mặc định / không lọc.
  { key: { score: -1 }, name: 'idx_score' },
  { key: { score: -1, analyzed_at: -1 }, name: 'score_analyzed_sort' },
  { key: { start_date: -1 }, name: 'idx_start_date' },
  // Gom nhóm duplicate (dedupe) + ưu tiên preset (sản phẩm lên trước).
  { key: { duplicate_group_key: 1 }, name: 'idx_group_key' },
  { key: { is_group_rep: 1, start_date: -1 }, name: 'idx_group_rep_start_date' },
  { key: { preset_priority: 1, creative_rank: 1, start_date: -1 }, name: 'idx_preset_priority' },
  { key: { 'filter_signals.is_new_ad': 1, preset_priority: 1, start_date: -1 }, name: 'idx_new_ad_priority' },
  { key: { 'filter_signals.is_ready_to_scale': 1, preset_priority: 1, scaling_score: -1 }, name: 'idx_scale_priority' },
  { key: { 'filter_signals.is_weekly_winner': 1, preset_priority: 1, trending_score: -1 }, name: 'idx_winner_priority' },
  // Dem preset + dedupe (is_group_rep) trong 1 index - 3 index nay da
  // tao truc tiep tren VM ngay 04/11/2026, ghi vao code de build lai
  // tu dau khong mat.
  { key: { 'filter_signals.is_new_ad': 1, is_group_rep: 1, preset_priority: 1, creative_rank: 1, start_date: -1 }, name: 'idx_new_ad_rep_priority' },
  { key: { 'filter_signals.is_ready_to_scale': 1, is_group_rep: 1, preset_priority: 1, creative_rank: 1, scaling_score: -1 }, name: 'idx_scale_rep_rank' },
  { key: { 'filter_signals.is_weekly_winner': 1, is_group_rep: 1, preset_priority: 1, creative_rank: 1, trending_score: -1 }, name: 'idx_winner_rep_rank' },
  // Index dem/tim cham 05/11: filter + is_group_rep + sort — khong co
  // is_group_rep o giua, Mongo phai FETCH toan bo doc cua filter de loc
  // dai dien (nganh Khac 145k doc -> dem khong xong).
  { key: { industry: 1, is_group_rep: 1, start_date: -1 }, name: 'idx_industry_rep_start' },
  { key: { estimated_spend: 1, is_group_rep: 1, start_date: -1 }, name: 'idx_spend_tier_rep_start' },
  { key: { scaling_level: 1, is_group_rep: 1, start_date: -1 }, name: 'idx_scaling_rep_start' },
  { key: { scaling_score: -1, is_group_rep: 1, start_date: -1 }, name: 'idx_scaling_score_rep_start' },
  { key: { domain_lower: 1, is_group_rep: 1, start_date: -1 }, name: 'idx_domain_rep_start' },
  { key: { page_name_lower: 1, is_group_rep: 1, start_date: -1 }, name: 'idx_page_rep_start' },
  { key: { countries_lower: 1, is_group_rep: 1, start_date: -1 }, name: 'idx_countries_rep_start' },
  { key: { last_seen: -1 }, name: 'idx_last_seen' },
  { key: { duration_days: 1 }, name: 'idx_duration_days' },
  { key: { estimated_spend_usd: -1 }, name: 'spend_usd_sort' },
  { key: { estimated_reach: -1 }, name: 'reach_sort' },
  { key: { trending_score: -1 }, name: 'trending_sort' },
  // Preset cũ (đường tính trực tiếp khi tinh chỉnh ngưỡng).
  { key: { start_date: -1, duration_days: 1 }, name: 'idx_new_ads' },
  { key: { scaling_score: -1, duration_days: 1, duplicates_count: 1 }, name: 'idx_ready_to_scale' },
  { key: { trending_score: -1, score: 1, duplicates_count: 1, duration_days: 1 }, name: 'idx_weekly_winner' },
  // Cờ preset (đường nhanh) + sort.
  { key: { 'filter_signals.is_new_ad': 1, start_date: -1 }, name: 'idx_new_ad_flag' },
  { key: { 'filter_signals.is_ready_to_scale': 1, scaling_score: -1 }, name: 'idx_scale_flag' },
  { key: { 'filter_signals.is_weekly_winner': 1, trending_score: -1 }, name: 'idx_winner_flag' },
  // Cờ preset + ưu tiên mua sắm (creative_rank) + sort của preset/FE.
  { key: { 'filter_signals.is_new_ad': 1, creative_rank: 1, start_date: -1 }, name: 'idx_new_ad_rank' },
  { key: { 'filter_signals.is_ready_to_scale': 1, creative_rank: 1, scaling_score: -1 }, name: 'idx_scale_rank' },
  { key: { 'filter_signals.is_ready_to_scale': 1, creative_rank: 1, start_date: -1 }, name: 'idx_scale_rank_start' },
  { key: { 'filter_signals.is_weekly_winner': 1, creative_rank: 1, trending_score: -1 }, name: 'idx_winner_rank' },
  { key: { 'filter_signals.is_weekly_winner': 1, creative_rank: 1, start_date: -1 }, name: 'idx_winner_rank_start' },
  { key: { 'filter_signals.is_weekly_winner': 1, creative_rank: 1, score: -1 }, name: 'idx_winner_rank_score' },
  // Lọc page_name/domain KHÔNG phân biệt hoa thường: range trên *_lower.
  { key: { page_name_lower: 1, start_date: -1 }, name: 'idx_page_lower_start_date' },
  { key: { page_name_lower: 1, score: -1 }, name: 'idx_page_lower_score' },
  { key: { domain_lower: 1, start_date: -1 }, name: 'idx_domain_lower_start_date' },
  { key: { domain_lower: 1, score: -1 }, name: 'idx_domain_lower_score' },
  // Similar ads + product detail: cùng domain/page, sort score.
  { key: { domain: 1, score: -1 }, name: 'idx_domain_score' },
  { key: { domain: 1 }, name: 'domain_filter' },
  { key: { page_name: 1, score: -1 }, name: 'idx_page_score' },
  // Lọc page_name/domain theo field gốc (giữ lại cho match chính xác;
  // đường chính nay là *_lower ở trên).
  { key: { page_name: 1, start_date: -1 }, name: 'idx_page_start_date' },
  { key: { domain: 1, start_date: -1 }, name: 'idx_domain_start_date' },
  // Lọc country: equality trên countries_lower (đường nhanh) + các
  // field gốc cho fallback khi doc chưa migrate.
  { key: { countries_lower: 1, start_date: -1 }, name: 'idx_countries_lower_start_date' },
  { key: { countries_lower: 1, score: -1 }, name: 'idx_countries_lower_score' },
  { key: { country_mentions: 1 }, name: 'idx_country' },
  { key: { country_mentions_lower: 1, start_date: -1 }, name: 'idx_country_mentions_lower_start_date' },
  { key: { latest_detail_country_lower: 1, start_date: -1 }, name: 'idx_latest_country_lower_start_date' },
  // Lọc ngành nghề.
  { key: { industry: 1 }, name: 'idx_industry' },
  { key: { industry: 1, start_date: -1 }, name: 'idx_industry_start_date' },
  { key: { industry: 1, score: -1 }, name: 'idx_industry_score' },
  // Lọc trạng thái (is_active do analyzer ghi; API fallback last_seen).
  { key: { is_active: 1, start_date: -1 }, name: 'idx_is_active_start_date' },
  // Lọc level / funnel / scaling / nhóm chi tiêu.
  { key: { level: 1 }, name: 'level_filter' },
  { key: { level: 1, start_date: -1 }, name: 'idx_level_start_date' },
  { key: { funnel: 1, start_date: -1 }, name: 'idx_funnel_start_date' },
  { key: { scaling_level: 1, start_date: -1 }, name: 'idx_scaling_start_date' },
  { key: { estimated_spend: 1, start_date: -1 }, name: 'idx_spend_tier_start_date' },
  { key: { scaling_score: -1 }, name: 'idx_scaling_score' },
  // Đếm + sort khi dedupe bật: is_group_rep đứng trước để countDocuments
  // theo preset/trạng thái/spend không phải quét toàn collection.
  { key: { is_group_rep: 1, last_seen: -1 }, name: 'idx_group_rep_last_seen' },
  { key: { is_group_rep: 1, estimated_spend_usd: 1 }, name: 'idx_group_rep_spend' },
  // Fix retest 06/11: Inactive find (rep + last_seen + sort start_date)
  // va combo new_ads+VN (flag + rep + country + sort) — thieu 2 index
  // nay thi find phai FETCH toan bo doc cua filter (UI Inactive 0 the,
  // combo 20-35s) du count da nhanh.
  { key: { is_group_rep: 1, last_seen: -1, start_date: -1 }, name: 'idx_group_rep_last_seen_start' },
  { key: { 'filter_signals.is_new_ad': 1, is_group_rep: 1, countries_lower: 1, start_date: -1 }, name: 'idx_new_ad_rep_country_start' },
  // Đếm media usage: cùng file re-sign vẫn chung image_keys/video_keys.
  { key: { image_keys: 1 }, name: 'idx_image_keys' },
  { key: { video_keys: 1 }, name: 'idx_video_keys' },
];

const PRODUCT_INDEXES = [
  { key: { domain: 1 }, name: 'domain_unique', unique: true },
  { key: { product_score: -1 }, name: 'product_score_sort' },
];

// Snapshot winners hàng ngày (analyzer ghi top 30 product/ngày).
const WINNERS_COL = 'product_winners_daily';
const WINNERS_INDEXES = [
  { key: { date: 1, rank: 1 }, name: 'idx_winners_date_rank', unique: true },
  { key: { domain: 1, date: -1 }, name: 'idx_winners_domain_date' },
];

async function ensureIndexes(col, specs, label) {
  const existing = await col.indexes();
  const existingKeys = new Set(existing.map(i => JSON.stringify(i.key)));
  let created = 0, skipped = 0, failed = 0;
  for (const ix of specs) {
    if (existingKeys.has(JSON.stringify(ix.key))) {
      console.log(`SKIP ${label}.${ix.name} (đã có index cùng key)`);
      skipped++;
      continue;
    }
    const t = Date.now();
    try {
      await col.createIndex(ix.key, { name: ix.name, ...(ix.unique ? { unique: true } : {}) });
      console.log(`OK ${label}.${ix.name} (${Date.now() - t}ms)`);
      created++;
    } catch (e) {
      console.log(`FAIL ${label}.${ix.name}: ${e.message}`);
      failed++;
    }
  }
  return { created, skipped, failed };
}

(async () => {
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  const db = client.db(DB_NAME);
  const col = db.collection(COL_NAME);
  console.log(`Collection: ${DB_NAME}.${COL_NAME} — ${await col.estimatedDocumentCount()} docs (ước tính)`);
  const r1 = await ensureIndexes(col, INDEXES, COL_NAME);
  const r2 = await ensureIndexes(db.collection(PRODUCTS_COL), PRODUCT_INDEXES, PRODUCTS_COL);
  const r3 = await ensureIndexes(db.collection(WINNERS_COL), WINNERS_INDEXES, WINNERS_COL);
  await client.close();
  console.log(`DONE — analyzed_ads: tạo ${r1.created}, bỏ qua ${r1.skipped}, lỗi ${r1.failed}; products: tạo ${r2.created}, bỏ qua ${r2.skipped}, lỗi ${r2.failed}; winners: tạo ${r3.created}, bỏ qua ${r3.skipped}, lỗi ${r3.failed}.`);
  console.log('Tiếp theo: chạy migrate-analyzer-fields.cjs (hoặc 1 lượt analyzer) rồi pm2 restart ads-api, sau đó đo lại bằng explain.');
})().catch(e => { console.error(e); process.exit(1); });

// Dọn index thừa (CHỈ chạy tay khi đã chắc đường *_lower + migration xong
// và không còn query nào dùng field gốc):
//   db.analyzed_ads.dropIndex('idx_page_start_date')
//   db.analyzed_ads.dropIndex('idx_domain_start_date')
