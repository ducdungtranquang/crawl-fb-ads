'use strict';
// recover-products.cjs — Khoi phuc products + daily winners sau khi FULL crash
// o flushProductsCollection (E11000 domain_unique).
//
// NGUYEN NHAN: chay FULL bang code PR #23 (gom theo product_key, 1 domain co
// nhieu product) nhung chua drop index `domain_unique` tren collection
// `products` -> upsert product thu 2 cung domain loi duplicate key, ca bulk
// write fail, snapshot ngay khong duoc ghi. analyzed_ads (641k) DA GHI DU
// va dung (co product_key), chi thieu 2 buoc cuoi.
//
// CACH DUNG (tren VM, trong ~/crawl-fb-ads/fb-ads-system/analyzer):
//   1. Drop index cu (neu chua drop):
//      docker exec -i mongodb-core mongosh --quiet --eval 'db.getSiblingDB("fb_ads_analyzer").getCollection("products").dropIndex("domain_unique")'
//   2. Tao index moi (neu chua chay):
//      node create-indexes.js
//   3. Chay script nay (can file analyzer-lib.cjs cung thu muc):
//      node recover-products.cjs
// Script tu kiem tra: neu `domain_unique` van con thi DUNG LAI va bao loi
// (khong ghi gi) de khong lap lai crash.
//
// Script doc analyzed_ads theo me 5000, gom map bang dung ham cua analyzer
// (accumulateProductStats), roi flushProductsCollection + ghi snapshot
// writeDailyWinnersSnapshot. Khong phan tich lai, khong ghi lai analyzed_ads.
const { MongoClient } = require('mongodb');
const analyzer = require('./analyzer-lib.cjs');

const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2';
const ANALYZER_DB_NAME = process.env.ANALYZER_DB_NAME || 'fb_ads_analyzer';
const BATCH = 5000;

async function main() {
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  console.log('[Recover] Da ket noi MongoDB.');
  const db = client.db(ANALYZER_DB_NAME);
  const productsCol = db.collection('products');

  // An toan: neu domain_unique van con thi dung lai, khong ghi gi.
  const idxs = await productsCol.indexes().catch(() => []);
  const hasDomainUnique = idxs.some((ix) => ix.name === 'domain_unique');
  if (hasDomainUnique) {
    console.error('[Recover] LOI: index `domain_unique` tren `products` van con.');
    console.error('[Recover] Hay drop no truoc roi chay lai script:');
    console.error('  docker exec -i mongodb-core mongosh --quiet --eval \'db.getSiblingDB("fb_ads_analyzer").getCollection("products").dropIndex("domain_unique")\'');
    await client.close();
    process.exit(1);
  }
  console.log('[Recover] OK: khong con `domain_unique` tren `products`.');

  const col = db.collection('analyzed_ads');
  const total = await col.countDocuments();
  console.log(`[Recover] analyzed_ads: ${total} docs. Dang gom product map...`);
  const map = {};
  const nowMs = Date.now();
  let processed = 0;
  const cursor = col.find({}).batchSize(BATCH);
  let batch = [];
  for await (const doc of cursor) {
    batch.push(doc);
    if (batch.length >= BATCH) {
      analyzer.accumulateProductStats(map, batch, nowMs);
      processed += batch.length;
      batch = [];
      if (processed % 100000 === 0) console.log(`[Recover] Da gom ${processed}/${total}...`);
    }
  }
  if (batch.length) {
    analyzer.accumulateProductStats(map, batch, nowMs);
    processed += batch.length;
  }
  console.log(`[Recover] Gom xong ${processed} ads -> ${Object.keys(map).length} products. Dang flush products...`);
  await analyzer.flushProductsCollection(db, map);
  console.log('[Recover] Flush products xong. Dang ghi daily winners snapshot...');
  const n = await analyzer.writeDailyWinnersSnapshot(db, map, nowMs);
  console.log(`[Recover] XONG. Snapshot: ${n} winners. Bang winning products da co du lieu moi.`);
  await client.close();
  process.exit(0);
}

main().catch((err) => {
  console.error('[Recover] LOI khong xu ly duoc:', err);
  process.exit(1);
});
