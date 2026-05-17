const { chromium } = require('playwright');
const { MongoClient } = require('mongodb');

// ===== CONFIG =====
const MONGO_URI = 'mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2';
const DB_NAME = 'fb_ads';

const USER_DATA_DIR = './chrome-profile';
const CHROME_PATH = 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe';
const PROFILE = 'Default';

// ===== LOGIC =====
const SEEN_THRESHOLD = 10 * 60 * 1000;
let db;
const seen = new Set();
let lastSavedAt = Date.now();

// ===== UTILS =====
function randomDelay(min = 2000, max = 4000) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

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

// ===== CONNECT DB =====
async function connectDB() {
    const client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db(DB_NAME);
    console.log('✅ Mongo connected');
}

// ===== SAVE =====
async function saveAd(ad) {
    const col = db.collection('ads');
    const now = Date.now();
    const domain = extractDomain(ad.link);

    const existing = await col.findOne({ ad_archive_id: ad.ad_archive_id });

    if (!existing) {
        await col.insertOne({
            ...ad,
            domain,
            first_seen: now,
            last_seen: now,
            seen_count: 1,
            growth_history: [{ t: now, c: 1 }]
        });
        return;
    }

    const shouldIncrease =
        now - (existing.last_seen || 0) > SEEN_THRESHOLD;

    const newCount = shouldIncrease
        ? (existing.seen_count || 0) + 1
        : existing.seen_count;

    await col.updateOne(
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

// ===== EXTRACT =====
function extractAds(json) {
    try {
        const edges =
            json?.data?.ad_library_main?.search_results_connection?.edges;

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
                    snapshot: snap,

                    text: snap.body?.text || null,
                    headline: snap.title || null,
                    description: snap.link_description || null,
                    cta: snap.cta_text || null,
                    link: snap.link_url || null,

                    images: snap.images || [],
                    videos: snap.videos || [],

                    start_date: ad.start_date,
                    end_date: ad.end_date,
                    is_active: ad.is_active,
                    platforms: ad.publisher_platform || [],

                    normalized_text: normalize(snap.body?.text || '')
                });
            }
        }

        return output;
    } catch {
        return [];
    }
}

// ===== FINGERPRINT PATCH =====
async function applyStealth(page) {
    await page.addInitScript(() => {

        Object.defineProperty(navigator, 'webdriver', {
            get: () => false,
        });

        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en'],
        });

        // fake chrome object
        window.chrome = {
            runtime: {},
        };

        // WebGL spoof
        const getParameter = WebGLRenderingContext.prototype.getParameter;

        WebGLRenderingContext.prototype.getParameter = function (param) {
            if (param === 37445) return 'Intel Inc.';
            if (param === 37446) return 'Intel Iris OpenGL Engine';

            return getParameter.call(this, param);
        };

    });
}

// ===== WORKER =====
async function worker(context, keywords, id) {

    context.on('response', async (res) => {
        try {
            const url = res.url();

            if (!url.includes('graphql')) return;

            const text = await res.text();

            if (!text.includes('ad_library_main')) return;

            const json = JSON.parse(text);

            const ads = extractAds(json);

            for (const ad of ads) {
                if (!seen.has(ad.ad_archive_id)) {

                    seen.add(ad.ad_archive_id);

                    await saveAd(ad);

                    lastSavedAt = Date.now();

                    console.log(`W${id} ✔`, ad.ad_archive_id);
                }
            }

        } catch { }
    });

    for (const keyword of keywords) {

        console.log(`W${id} 🔍`, keyword);

        // ❌ bỏ random user-agent
        const page = await context.newPage({
            viewport: {
                width: randomInt(1200, 1920),
                height: randomInt(700, 1080)
            }
        });

        await applyStealth(page);

        const url = `https://www.facebook.com/ads/library/?ad_type=all&country=ALL&q=${encodeURIComponent(keyword)}`;
        lastSavedAt = Date.now();

        try {

            await page.goto(url, {
                waitUntil: 'domcontentloaded',
                timeout: 60000
            });

            await page.waitForTimeout(randomDelay(3000, 6000));

            try {
                await page.click('text=OK', { timeout: 3000 });
            } catch { }

            let prev = 0;
            let same = 0;

            while (true) {

                if (Date.now() - lastSavedAt > 200000) {
                    console.log(`⏱️ W${id} NO DATA >200s → skip keyword`);
                    break;
                }

                // random mouse movement
                await page.mouse.move(
                    randomInt(0, 800),
                    randomInt(0, 600),
                    {
                        steps: randomInt(10, 25)
                    }
                );

                // random scroll
                await page.evaluate(() => {
                    const direction = Math.random() > 0.3 ? 1 : -1;
                    const amount = Math.random() * 800 + 200;

                    window.scrollBy(0, direction * amount);
                });

                await page.waitForTimeout(randomDelay());

                const curr = await page.evaluate(
                    () => document.body.scrollHeight
                );

                if (curr === prev) same++;
                else same = 0;

                prev = curr;

                // if (same >= 5) break;
            }

        } catch (e) {
            console.log(`W${id} ❌`, keyword);
        }

        await page.close();

        // random nghỉ giữa keyword
        await new Promise(r =>
            setTimeout(r, randomDelay(3000, 6000))
        );
    }
}

// ===== SPLIT =====
function chunkArray(arr, n) {
    const result = Array.from({ length: n }, () => []);

    arr.forEach((item, i) => {
        result[i % n].push(item);
    });

    return result;
}

// ===== MAIN =====
(async () => {

    await connectDB();

    const context = await chromium.launchPersistentContext(
        USER_DATA_DIR,
        {
            headless: false,
            executablePath: CHROME_PATH,
            args: [
                `--profile-directory=${PROFILE}`,
                '--start-maximized',
                '--disable-blink-features=AutomationControlled'
            ]
        }
    );

    console.log('🔥 Using REAL Chrome profile');

    const keywords = [

        // 'shopee',
        // 'lazada',
        // 'tiktok shop',
        // 'điện thoại',
        // 'iphone',
        // 'samsung',
        // 'xiaomi',
        // 'laptop',
        // 'máy tính',
        // 'gaming',

        // 'mỹ phẩm',
        // 'skincare',
        'serum',
        'kem chống nắng',
        'sữa rửa mặt',
        'nước hoa',
        'spa',
        'thẩm mỹ',
        'giảm cân',
        'gym',

        'quần áo',
        'thời trang',
        'giày dép',
        'túi xách',
        'đồng hồ',
        'phụ kiện',
        'local brand',
        'áo thun',
        'váy',
        'hoodie',

        'trà sữa',
        'cafe',
        'ăn vặt',
        'nhà hàng',
        'thực phẩm',
        'đồ uống',
        'du lịch',
        'khách sạn',
        'vé máy bay',
        'resort',

        'bất động sản',
        'chung cư',
        'vay tiền',
        'ngân hàng',
        'bảo hiểm',
        'đầu tư',
        'crypto',
        'bitcoin',
        'affiliate',
        'dropshipping'
    ];

    const chunks = chunkArray(keywords, 2);

    for (let i = 0; i < chunks.length; i++) {
        await worker(context, chunks[i], i + 1);
    }

    console.log('✅ DONE');

})();