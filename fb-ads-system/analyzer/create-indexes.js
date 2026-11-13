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
  // 2026-11-06: MongoDB tối đa 64 index/collection (tính cả _id) — DB đã
  // chạm trần nên 4 index mới FAIL. Dọn 7 index thừa dưới đây (đều là
  // tiền tố strict của 1 index dài hơn còn giữ — index dài phục vụ được
  // mọi query của index ngắn): idx_score -> score_analyzed_sort,
  // idx_start_date -> idx_new_ads, domain_filter -> idx_domain_score,
  // idx_industry -> idx_industry_start_date, level_filter ->
  // idx_level_start_date, idx_scaling_score -> idx_scaling_score_rep_start,
  // idx_group_rep_last_seen -> idx_group_rep_last_seen_start. Drop tay
  // bằng mongosh trước, rồi chạy lại file này để tạo 4 index mới.
  // Tra cứu theo ID (ES trả list ad_archive_id -> query $in + count).
  { key: { ad_archive_id: 1 }, name: 'idx_ad_archive_id' },
  // Sort mặc định / không lọc.
  { key: { score: -1, analyzed_at: -1 }, name: 'score_analyzed_sort' },
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
  // Combo preset + industry/country (PR #19): dem preset+nganh/nuoc
  // truoc day phai FETCH toan bo doc cua preset de kiem tra industry /
  // countries_lower (khong co trong index cu) — scale+beauty ~10k doc
  // beo, winner+VN thi planner de chon nham index country (144k doc).
  // Index rong hon giu nguyen thu tu sort cu (flag, rep, preset_priority,
  // creative_rank) de preset don le khong cham lai, them industry +
  // countries_lower + sort field cua preset/FE (start_date) de dem/loc/
  // sort deu doc duoc tu index. Thay the 3 index cu (drop tay tren DB).
  // 08/11/2026 (PR #20): dieu kien tuoi last_seen>=now-30d lam dem bi FETCH
  // (new_ads/VN/OTHER/beauty timeout) vi last_seen khong co trong index.
  // Chen last_seen ngay sau is_group_rep (truoc cac key sort/loc) de dem
  // lai la COUNT_SCAN: equality prefix + range last_seen, khong FETCH doc.
  // 12/11 (PR #41, loi 10c VN+preset): doi key — thiet ke cu dat last_seen
  // (range) ngay sau is_group_rep de toi uu COUNT, nhung sort keys
  // (preset_priority/creative_rank/start_date) nam SAU range nen index
  // khong cap duoc sort -> FIND sort trong bo nho -> timeout. Key moi dua
  // sort keys len truoc last_seen (range xuong cuoi); FE ep sort_by=
  // start_date nen start_date la key sort chinh. Cung ten, khac key ->
  // deploy PHAI dropIndex truoc. COUNT co the cham hon chut, FIND
  // (cai FE timeout) nhanh hon nhieu — uu tien dung.
  { key: { 'filter_signals.is_new_ad': 1, is_group_rep: 1, countries_lower: 1, preset_priority: 1, creative_rank: 1, start_date: -1, last_seen: -1, industry: 1 }, name: 'idx_new_ad_rep_combo' },
  { key: { 'filter_signals.is_ready_to_scale': 1, is_group_rep: 1, countries_lower: 1, preset_priority: 1, creative_rank: 1, start_date: -1, scaling_score: -1, last_seen: -1, industry: 1 }, name: 'idx_scale_rep_combo' },
  { key: { 'filter_signals.is_weekly_winner': 1, is_group_rep: 1, countries_lower: 1, preset_priority: 1, creative_rank: 1, start_date: -1, trending_score: -1, last_seen: -1, industry: 1 }, name: 'idx_winner_rep_combo' },
  // Index dem/tim cham 05/11: filter + is_group_rep + sort — khong co
  // is_group_rep o giua, Mongo phai FETCH toan bo doc cua filter de loc
  // dai dien (nganh Khac 145k doc -> dem khong xong).
  { key: { industry: 1, is_group_rep: 1, last_seen: -1, start_date: -1 }, name: 'idx_industry_rep_start' },
  { key: { estimated_spend: 1, is_group_rep: 1, last_seen: -1, start_date: -1 }, name: 'idx_spend_tier_rep_start' },
  { key: { scaling_level: 1, is_group_rep: 1, last_seen: -1, start_date: -1 }, name: 'idx_scaling_rep_start' },
  { key: { scaling_score: -1, is_group_rep: 1, start_date: -1 }, name: 'idx_scaling_score_rep_start' },
  { key: { domain_lower: 1, is_group_rep: 1, start_date: -1 }, name: 'idx_domain_rep_start' },
  { key: { page_name_lower: 1, is_group_rep: 1, start_date: -1 }, name: 'idx_page_rep_start' },
  // Loc rieng quoc gia (fix 13/11, loi VN=0): query VN mot minh
  // ({countries_lower, is_group_rep, last_seen} + sort start_date)
  // khong co index nao cap ca filter + sort — planner chon
  // idx_countries_lower_start_date (chi cap sort) roi FETCH tung doc
  // de loc rep/fresh -> 15-30s -> FE timeout 25s -> tra 0. Key moi:
  // equality (nuoc, rep) -> sort (start_date) -> range (last_seen),
  // them creative_rank cuoi key de dieu kien an phim ($ne creative_rank)
  // duoc COUNT_SCAN. Cung ten, khac key -> deploy PHAI dropIndex truoc.
  // Dong thoi XOA idx_countries_lower_start_date (index xau gay ra loi).
  { key: { countries_lower: 1, is_group_rep: 1, start_date: -1, last_seen: -1, creative_rank: 1 }, name: 'idx_countries_rep_start' },
  // 12/11 (PR #41, loi 10a VN+industry): khong index nao chua ca
  // countries_lower + industry — planner chon 1 chieu roi FETCH tung doc
  // kiem tra chieu con lai + sort trong bo nho. Them 1 index to hop:
  // equality (nuoc, nganh, rep) -> sort (start_date) -> range (last_seen).
  // Can 1 slot: DB dang 64/64 sau PR #40 nen deploy PHAI drop 1 index thua
  // truoc (idx_scale_rep_rank hoac idx_winner_rep_rank — khong con trong
  // list, drop xong khong bi tao lai), roi moi chay file nay.
  { key: { countries_lower: 1, industry: 1, is_group_rep: 1, start_date: -1, last_seen: -1, creative_rank: 1 }, name: 'idx_country_industry_rep_start' },
  // VN + spend (fix 12/11): FE gui estimated_spend la 1 tier
  // ('VERY HIGH'/'HIGH'/'MEDIUM'/'LOW', xem SPEND_TIER_MAP) qua param
  // estimated_spend; query VN+spend truoc day khong co index nao chua ca
  // countries_lower + estimated_spend nen planner phai FETCH tung doc de
  // loc chieu con lai roi sort start_date trong bo nho -> timeout 25s.
  // Thu tu key: equality (nuoc, tier, rep) -> sort (start_date) ->
  // range (last_seen) cuoi. Them 1 index, tran 64: danh sach co 60
  // entry + _id = 61 -> them index nay thanh 62, van du slot (chi them
  // 1 index nay, khong them index USD vi FE chi gui tier).
  { key: { countries_lower: 1, estimated_spend: 1, is_group_rep: 1, start_date: -1, last_seen: -1, creative_rank: 1 }, name: 'idx_country_spend_tier_rep_start' },
  // OTHER (fix 11/11): $ne 'vn' khong dung duoc index -> dem >25s -> FE
  // route timeout 25s roi tra 0. Analyzer ghi san co
  // filter_signals.is_other (khong phai VN, gom ca ad chua co nhan nuoc);
  // query dung equality boolean tren co (xem buildCountryCondition).
  // Giu countries_lower trong key de duong fallback $ne (truoc FULL)
  // van kiem tra tu index. Cung ten, khac key -> deploy PHAI drop index
  // cu truoc khi chay create-indexes.js (xem ghi chu deploy).
  // 12/11 (PR #41, loi 3 OTHER): doi key — last_seen (range) dung TRUOC
  // start_date (sort) nen index khong cap duoc sort -> Mongo sort ~468k
  // doc trong bo nho -> timeout 25s. Key moi: equality (rep, is_other) ->
  // sort (start_date) -> range (last_seen) cuoi; bo countries_lower (dead
  // weight, query OTHER dung co thi khong loc no). Cung ten, khac key ->
  // deploy PHAI dropIndex('idx_other_rep_start') truoc khi chay file nay.
  { key: { is_group_rep: 1, 'filter_signals.is_other': 1, start_date: -1, last_seen: -1 }, name: 'idx_other_rep_start' },
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
  // PR #23: detail top_ads tim theo product_key (landing URL chuan hoa).
  { key: { product_key: 1, score: -1 }, name: 'idx_product_key_score' },
  { key: { page_name: 1, score: -1 }, name: 'idx_page_score' },
  // Lọc page_name/domain theo field gốc (giữ lại cho match chính xác;
  // đường chính nay là *_lower ở trên).
  { key: { page_name: 1, start_date: -1 }, name: 'idx_page_start_date' },
  { key: { domain: 1, start_date: -1 }, name: 'idx_domain_start_date' },
  // Lọc country: equality trên countries_lower (đường nhanh).
  // Da xoa idx_country + idx_country_mentions_lower_start_date (chi phuc
  // vu fallback khi doc chua migrate — DB da migrate, schema_fallback
  // toan false): giai phong 2 slot tran 64 cho idx_other_rep_start va
  // idx_product_key_score.
  // 13/11: XOA idx_countries_lower_start_date (index xau vu VN=0, xem
  // idx_countries_rep_start) — giai phong them 1 slot (ve 63/64).
  { key: { countries_lower: 1, score: -1 }, name: 'idx_countries_lower_score' },
  { key: { latest_detail_country_lower: 1, start_date: -1 }, name: 'idx_latest_country_lower_start_date' },
  // Lọc ngành nghề.
  { key: { industry: 1, start_date: -1 }, name: 'idx_industry_start_date' },
  { key: { industry: 1, score: -1 }, name: 'idx_industry_score' },
  // Lọc trạng thái (is_active do analyzer ghi; API fallback last_seen).
  { key: { is_active: 1, start_date: -1 }, name: 'idx_is_active_start_date' },
  // Lọc level / funnel / scaling / nhóm chi tiêu.
  { key: { level: 1, start_date: -1 }, name: 'idx_level_start_date' },
  { key: { funnel: 1, start_date: -1 }, name: 'idx_funnel_start_date' },
  { key: { scaling_level: 1, start_date: -1 }, name: 'idx_scaling_start_date' },
  { key: { estimated_spend: 1, start_date: -1 }, name: 'idx_spend_tier_start_date' },
  // Đếm + sort khi dedupe bật: is_group_rep đứng trước để countDocuments
  // theo preset/trạng thái/spend không phải quét toàn collection.
  { key: { is_group_rep: 1, estimated_spend_usd: 1 }, name: 'idx_group_rep_spend' },
  // Fix retest 06/11: Inactive find (rep + last_seen + sort start_date)
  // va combo new_ads+VN (flag + rep + country + sort) — thieu 2 index
  // nay thi find phai FETCH toan bo doc cua filter (UI Inactive 0 the,
  // combo 20-35s) du count da nhanh.
  // 12/11 (PR #41, loi 7 inactive): doi key — last_seen (range, inactive
  // = last_seen trong (now-30d, now-7d)) dung truoc start_date (sort) nen
  // khong cap duoc sort. Key moi: equality (rep) -> sort (start_date) ->
  // range (last_seen). Cung ten, khac key -> deploy PHAI dropIndex truoc.
  // 13/11: them creative_rank cuoi key de dieu kien an phim ($ne
  // creative_rank) o list mac dinh duoc COUNT_SCAN, khong FETCH.
  { key: { is_group_rep: 1, start_date: -1, last_seen: -1, creative_rank: 1 }, name: 'idx_group_rep_last_seen_start' },
  { key: { 'filter_signals.is_new_ad': 1, is_group_rep: 1, countries_lower: 1, start_date: -1 }, name: 'idx_new_ad_rep_country_start' },
  // Đếm media usage: cùng file re-sign vẫn chung image_keys/video_keys.
  { key: { image_keys: 1 }, name: 'idx_image_keys' },
  { key: { video_keys: 1 }, name: 'idx_video_keys' },
];

// PR #23: gom theo landing URL nen 1 domain co nhieu product — unique key
// chuyen tu domain sang product_key. TRUOC KHI CHAY FILE NAY: drop index
// unique cu tren products (mongosh):
//   db.getSiblingDB('fb_ads_analyzer').getCollection('products').dropIndex('domain_unique');
const PRODUCT_INDEXES = [
  { key: { product_key: 1 }, name: 'product_key_unique', unique: true },
  { key: { domain: 1 }, name: 'idx_products_domain' },
  { key: { product_score: -1 }, name: 'product_score_sort' },
];

// Snapshot winners hàng ngày (analyzer ghi top 30 product/ngày).
const WINNERS_COL = 'product_winners_daily';
const WINNERS_INDEXES = [
  { key: { date: 1, rank: 1 }, name: 'idx_winners_date_rank', unique: true },
  { key: { domain: 1, date: -1 }, name: 'idx_winners_domain_date' },
];

async function ensureIndexes(col, specs, label) {
  // Collection chưa tồn tại (vd product_winners_daily trước lượt FULL đầu
  // tiên): listIndexes ném NamespaceNotFound (code 26). Coi như chưa có
  // index nào — createIndex sẽ tạo luôn collection. Không để crash cả lượt.
  let existing = [];
  try {
    existing = await col.indexes();
  } catch (e) {
    if (e && e.code === 26) {
      console.log(`... ${label} chưa có collection, sẽ tạo mới cùng index.`);
    } else {
      throw e;
    }
  }
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
