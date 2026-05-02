const { chromium } = require('playwright');
const { MongoClient } = require('mongodb');

// ===== CONFIG =====
const MONGO_URI = 'mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2';
const DB_NAME = 'fb_ads';

const USER_DATA_DIR = './chrome-profile';
const CHROME_PATH = 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe';

// nếu bạn login FB ở profile khác thì đổi
const PROFILE = 'Default'; // hoặc 'Profile 1'

// ===== LOGIC =====
const SEEN_THRESHOLD = 10 * 60 * 1000;
let db;
const seen = new Set();

// ===== UTILS =====
function randomDelay(min = 1500, max = 3500) {
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
                    console.log(`W${id} ✔`, ad.ad_archive_id);
                }
            }
        } catch { }
    });

    for (const keyword of keywords) {
        console.log(`W${id} 🔍`, keyword);

        const page = await context.newPage();

        const url = `https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country=ALL&q=${encodeURIComponent(keyword)}`;

        try {
            await page.goto(url, {
                waitUntil: 'domcontentloaded',
                timeout: 60000
            });

            await page.waitForTimeout(3000);

            // auto close popup adblock nếu có
            try {
                await page.click('text=OK', { timeout: 3000 });
            } catch { }

            let prev = 0;
            let same = 0;

            while (true) {
                 await page.mouse.move(
                        Math.random() * 800,
                        Math.random() * 600
                    );
                const curr = await page.evaluate(() => {
                    const random = Math.floor(Math.random() * 50);
                    window.scrollBy(0, window.innerHeight + random);
                    // await page.evaluate(() => {
                    //     const random = Math.floor(Math.random() * 500) + 300;
                    //     window.scrollBy(0, random);
                    // });
                    return document.body.scrollHeight;
                });

                await page.waitForTimeout(randomDelay());

                if (curr === prev) same++;
                else same = 0;

                prev = curr;

                if (same >= 4) break;
            }

        } catch (e) {
            console.log(`W${id} ❌`, keyword);
        }

        await page.close();
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
        "decor",
        "nội thất",
        "furniture",
    ];

    const chunks = chunkArray(keywords, 2);

    // await Promise.all(
    //     chunks.map((chunk, i) =>
    //         worker(context, chunk, i + 1)
    //     )
    // );

    for (let i = 0; i < chunks.length; i++) {
        await worker(context, chunks[i], i + 1);
    }

    console.log('✅ DONE');

})();