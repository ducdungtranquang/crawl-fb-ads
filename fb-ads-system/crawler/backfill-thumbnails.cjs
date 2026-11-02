'use strict';
// Backfill thumbnail CŨ lên R2 (chạy 1 lần, chạy lại an toàn — idempotent).
// Chạy trên máy crawl (nơi có thư mục storage/images + tới được Mongo),
// cùng thư mục với crawler.js và r2-upload.js:
//
//   node backfill-thumbnails.cjs --dry-run --limit 10   # thử: chỉ đếm/báo, không ghi gì
//   node backfill-thumbnails.cjs --limit 100            # chạy thật 100 ad
//   node backfill-thumbnails.cjs --all                  # chạy thật toàn bộ
//
// Cách hoạt động (mỗi ad chưa có thumbnail_r2, chỉ xét ảnh đầu tiên —
// giống hệt crawler):
//   1. Tính key = hash MD5 của URL ảnh. HEAD R2: đã có thì chỉ ghi
//      thumbnail_r2 vào doc, khỏi tải/upload (ad trùng ảnh dùng chung).
//   2. Chưa có: ưu tiên file local cũ (tên theo hash, rồi tên theo adId)
//      để upload — khỏi tải lại từ Meta.
//   3. Không có file local: chỉ tải từ URL ảnh còn hạn (tham số oe của
//      fbcdn chưa qua). URL đã hết hạn thì bỏ qua luôn, khỏi fetch để
//      khỏi ăn 403 — ad cũ (trước khi bạn bắt đầu crawl ảnh ~2-3 tuần
//      nay) thường rơi vào nhóm này.
//   4. Upload lên R2 theo key, ghi thumbnail_r2 vào doc. Chạy lại lần nữa
//      thì các ad đã có thumbnail_r2 được bỏ qua ngay từ query.
// Xử lý từ ad MỚI nhất trước (sort _id giảm dần), vì ad mới mới có file
// local / URL còn hạn. --limit N nghĩa là N ad mới nhất chưa có thumbnail_r2.
// Lưu ý: fetch cần Node 18+. --dry-run không ghi R2 cũng không ghi Mongo.
const fs = require('fs');
const path = require('path');
const { MongoClient } = require('mongodb');
const { uploadThumbnailToR2, r2ThumbnailExists, thumbnailKeyFor, r2PublicUrl, isR2Enabled } = require('./r2-upload');

const MONGO_URI = process.env.MONGO_URI || 'mongodb://192.168.100.8:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2';
const DB_NAME = 'fb_ads';
const STORAGE_PATH = path.join(__dirname, '..', '..', 'storage', 'images');
const DELAY_MS = 50;

const args = process.argv.slice(2);
if (args.includes('--help') || args.includes('-h')) {
  console.log('Usage: node backfill-thumbnails.cjs [--dry-run] [--limit N | --all]');
  console.log('  --dry-run : chỉ kiểm tra/báo cáo, không upload, không ghi Mongo');
  console.log('  --limit N : số ad tối đa xử lý (mặc định 10, để thử an toàn)');
  console.log('  --all     : xử lý toàn bộ ad chưa có thumbnail_r2');
  process.exit(0);
}
const DRY_RUN = args.includes('--dry-run');
const ALL = args.includes('--all');
const limitIdx = args.indexOf('--limit');
const LIMIT = limitIdx !== -1 ? parseInt(args[limitIdx + 1], 10) : 10;

function imageUrls(ad) {
  const img = ad.images && ad.images[0];
  if (!img) return [];
  if (typeof img === 'string') return [img];
  return [
    img.original_image_url || img.url,
    img.url,
    img.resized_image_url,
    img.watermarked_resized_image_url,
  ].filter(Boolean).filter((v, i, a) => a.indexOf(v) === i);
}

// fbcdn ký URL bằng tham số oe = timestamp hex (giây, UTC). Hết hạn thì
// Meta trả 403, fetch vô ích. Không có oe thì coi như chưa biết, vẫn thử.
function oeExpiryMs(url) {
  try {
    const oe = new URL(url).searchParams.get('oe');
    if (!oe) return null;
    const t = parseInt(oe, 16);
    return Number.isNaN(t) ? null : t * 1000;
  } catch { return null; }
}

function isUrlExpired(url) {
  const exp = oeExpiryMs(url);
  return exp !== null && exp <= Date.now();
}

async function fetchBuffer(url) {
  if (typeof fetch !== 'function') throw new Error('Node <18: không có fetch, chỉ backfill được ad có file local');
  const res = await fetch(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
      'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
      'Referer': 'https://www.facebook.com/',
    },
    signal: AbortSignal.timeout(10000),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return Buffer.from(await res.arrayBuffer());
}

async function main() {
  if (!isR2Enabled()) {
    console.error('[Backfill] R2 chưa bật (thiếu key/secret). Dán key vào r2-upload.js hoặc đặt biến môi trường rồi chạy lại.');
    process.exit(1);
  }
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  const col = client.db(DB_NAME).collection('ads');
  const query = {
    $or: [{ thumbnail_r2: { $exists: false } }, { thumbnail_r2: null }, { thumbnail_r2: '' }],
    images: { $exists: true, $ne: [] },
  };
  const cursor = col.find(query, { projection: { ad_archive_id: 1, images: { $slice: 1 }, thumbnail_local: 1 } }).sort({ _id: -1 });
  if (!ALL) cursor.limit(LIMIT);

  const stats = { total: 0, alreadyOnR2: 0, uploadedFromLocal: 0, uploadedFromFetch: 0, skippedExpired: 0, failed: 0, noImage: 0 };
  for await (const ad of cursor) {
    stats.total++;
    const urls = imageUrls(ad);
    if (urls.length === 0) { stats.noImage++; continue; }
    // Key luôn tính từ URL ảnh đầu tiên (original||url) để khớp crawler.
    const key = thumbnailKeyFor(urls[0]);
    const publicUrl = r2PublicUrl(key);
    try {
      if (await r2ThumbnailExists(key)) {
        stats.alreadyOnR2++;
        if (!DRY_RUN) await col.updateOne({ ad_archive_id: ad.ad_archive_id }, { $set: { thumbnail_r2: publicUrl } });
        continue;
      }
      // Ưu tiên file local cũ: tên theo hash (mới) rồi tên theo adId (cũ).
      const candidates = [
        path.join(STORAGE_PATH, path.basename(key)),
        path.join(STORAGE_PATH, `${ad.ad_archive_id}.jpg`),
      ];
      let buffer = null;
      let fromLocal = false;
      for (const p of candidates) {
        try { buffer = await fs.promises.readFile(p); fromLocal = true; break; } catch { /* thử file tiếp theo */ }
      }
      if (!buffer) {
        // Không có file local: chỉ fetch URL còn hạn, URL hết hạn bỏ qua luôn.
        const freshUrls = urls.filter((u) => !isUrlExpired(u));
        if (freshUrls.length === 0) { stats.skippedExpired++; continue; }
        let lastErr = null;
        for (const u of freshUrls) {
          try { buffer = await fetchBuffer(u); break; } catch (e) { lastErr = e; }
        }
        if (!buffer) throw lastErr || new Error('không tải được ảnh');
      }
      if (DRY_RUN) { stats[fromLocal ? 'uploadedFromLocal' : 'uploadedFromFetch']++; continue; }
      const url = await uploadThumbnailToR2(buffer, key);
      if (!url) { stats.failed++; console.error(`[Backfill] upload thất bại: ${ad.ad_archive_id}`); continue; }
      const set = { thumbnail_r2: url };
      if (!ad.thumbnail_local && fromLocal) set.thumbnail_local = `/images/${path.basename(key)}`;
      await col.updateOne({ ad_archive_id: ad.ad_archive_id }, { $set: set });
      stats[fromLocal ? 'uploadedFromLocal' : 'uploadedFromFetch']++;
    } catch (err) {
      stats.failed++;
      console.error(`[Backfill] lỗi ad ${ad.ad_archive_id}:`, err.message);
    }
    if (stats.total % 100 === 0) console.log(`... ${stats.total} ads: ${JSON.stringify(stats)}`);
    if (DELAY_MS) await new Promise((r) => setTimeout(r, DELAY_MS));
  }
  console.log(`[Backfill] ${DRY_RUN ? 'DRY-RUN ' : ''}Xong: ${JSON.stringify(stats)}`);
  await client.close();
}

if (require.main === module) {
  main().catch((err) => { console.error('[Backfill] Lỗi:', err); process.exit(1); });
}

module.exports = { imageUrls, oeExpiryMs, isUrlExpired };
