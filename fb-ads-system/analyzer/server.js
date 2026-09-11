const { MongoClient } = require('mongodb');
const { initSearchServer } = require('./searchApi');

const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000';
const ANALYZER_DB_NAME = 'fb_ads_analyzer';

async function main() {
    console.log("🔄 Đang kết nối cơ sở dữ liệu cho Search API...");
    const client = new MongoClient(MONGO_URI);

    await client.connect();
    console.log("💾 [DB] Kết nối MongoDB thành công!");

    const db = client.db(ANALYZER_DB_NAME);

    // Kích hoạt server API
    initSearchServer(db);
}

main().catch(err => {
    console.error("❌ Lỗi khởi chạy server:", err);
    process.exit(1);
});