const { chromium } = require('playwright');
const { MongoClient } = require('mongodb');

// ===== CONFIG =====
const MONGO_URI = 'mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2';
const DB_NAME = 'fb_ads';
const PAUSE_TIME = 25 * 60 * 1000; // 25 phút nghỉ nếu bị rate limit

let db;
let isPaused = false;
const seen = new Set();
const SEEN_THRESHOLD = 10 * 60 * 1000;

// ===== RANDOM DELAY =====
function randomDelay(min = 2000, max = 5000) {
    return new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * (max - min + 1)) + min));
}

// ===== NORMALIZE & DOMAIN =====
function normalize(text) {
    return text?.toLowerCase().replace(/\s+/g, ' ').trim();
}

function extractDomain(url) {
    try { return new URL(url).hostname.replace('www.', ''); } catch { return null; }
}

// ===== CONNECT DB =====
async function connectDB() {
    const client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db(DB_NAME);
    console.log('✅ Connected MongoDB');
}

// ===== SAVE TO MONGO =====
async function saveAd(ad) {
    const collection = db.collection('ads');
    const now = Date.now();
    const domain = extractDomain(ad.link);
    const existing = await collection.findOne({ ad_archive_id: ad.ad_archive_id });

    if (!existing) {
        await collection.insertOne({
            ...ad, domain, first_seen: now, last_seen: now, seen_count: 1,
            growth_history: [{ t: now, c: 1 }]
        });
        return;
    }

    const shouldIncrease = now - (existing.last_seen || 0) > SEEN_THRESHOLD;
    const newCount = shouldIncrease ? (existing.seen_count || 0) + 1 : existing.seen_count;

    await collection.updateOne(
        { ad_archive_id: ad.ad_archive_id },
        {
            $set: { ...ad, domain, last_seen: now },
            ...(shouldIncrease && {
                $inc: { seen_count: 1 },
                $push: { growth_history: { t: now, c: newCount } }
            })
        }
    );
}

// ===== EXTRACT ADS =====
function extractAds(json) {
    try {
        const edges = json?.data?.ad_library_main?.search_results_connection?.edges;
        if (!edges) return [];
        const output = [];
        for (const e of edges) {
            const list = e?.node?.collated_results || [];
            for (const ad of list) {
                const snap = ad.snapshot || {};
                output.push({
                    ad_archive_id: ad.ad_archive_id,
                    page_id: ad.page_id,
                    page_name: ad.page_name,
                    text: snap.body?.text || null,
                    headline: snap.title || null,
                    link: snap.link_url || null,
                    start_date: ad.start_date,
                    is_active: ad.is_active,
                    normalized_text: normalize(snap.body?.text || '')
                });
            }
        }
        return output;
    } catch (e) { return []; }
}

// ===== MAIN RUNNER =====
(async () => {
    await connectDB();

    const browser = await chromium.launch({
        headless: false,
        args: [
            '--disable-blink-features=AutomationControlled',
            '--no-sandbox'
        ]
    });

    // Giả lập fingerprint sạch hơn
    const context = await browser.newContext({
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        viewport: { width: 1920, height: 1080 },
        deviceScaleFactor: 1,
        locale: 'vi-VN',
        timezoneId: 'Asia/Ho_Chi_Minh'
    });

    // Chặn tài nguyên thừa
    await context.route('**/*', route => {
        const type = route.request().resourceType();
        if (['image', 'media', 'font'].includes(type)) route.abort();
        else route.continue();
    });

    // Xử lý chặn Rate Limit toàn cục
    context.on('response', async (res) => {
        try {
            const url = res.url();
            if (!url.includes('graphql')) return;

            const text = await res.text();

            // KIỂM TRA RATE LIMIT
            if (text.includes('Rate limit') || res.status() === 429) {
                if (!isPaused) {
                    isPaused = true;
                    console.log('🚨 RATE LIMIT DETECTED! Pausing for 25 mins...');
                    setTimeout(() => { isPaused = false; }, PAUSE_TIME);
                }
                return;
            }

            if (!text.includes('ad_library_main')) return;

            const json = JSON.parse(text);
            const ads = extractAds(json);

            for (const ad of ads) {
                if (!seen.has(ad.ad_archive_id)) {
                    seen.add(ad.ad_archive_id);
                    await saveAd(ad);
                }
            }
        } catch (e) { }
    });

    const keywords = ["điện thoại", "airpods", "iphone", "samsung", "skincare", "fitness"];

    for (const keyword of keywords) {
        // Nếu đang bị rate limit, chờ cho đến khi hết pause
        while (isPaused) {
            await new Promise(r => setTimeout(r, 10000));
        }

        console.log(`🔍 Searching: ${keyword}`);
        const page = await context.newPage();

        try {
            const url = `https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country=VN&q=${encodeURIComponent(keyword)}&sort_data[direction]=desc&sort_data[mode]=relevancy_monthly_grouped`;

            await page.goto(url, { waitUntil: 'networkidle', timeout: 60000 });
            await randomDelay(3000, 5000);

            let prevHeight = 0;
            let sameCount = 0;

            // Scroll loop
            for (let i = 0; i < 50; i++) { // Giới hạn scroll tránh vô tận
                if (isPaused) break;

                const currHeight = await page.evaluate(() => {
                    window.scrollBy(0, window.innerHeight * 0.8);
                    return document.body.scrollHeight;
                });

                await randomDelay(2500, 4500);

                if (currHeight === prevHeight) sameCount++;
                else sameCount = 0;

                prevHeight = currHeight;
                if (sameCount >= 4) break;
            }

        } catch (err) {
            console.log('❌ Error with keyword:', keyword, err.message);
        }

        await page.close();
        await randomDelay(5000, 10000); // Nghỉ giữa các keyword
    }

    console.log('✅ Job Finished');
    await browser.close();
})();