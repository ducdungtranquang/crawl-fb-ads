const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');
const { MongoClient } = require('mongodb');
// [R2-THUMBNAIL] Thumbnail ad qua module ./thumbnail.js (tải ảnh -> upload
// R2, R2-only, không lưu file local). Trạng thái R2 được log 1 dòng khi
// khởi động ở main IIFE cuối file.
const { downloadThumbnailForAd, getR2Status } = require('./thumbnail');
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

const MONGO_URI = process.env.MONGO_URI || 'mongodb://192.168.100.7:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2';
const DB_NAME = 'fb_ads';

const USER_DATA_DIR = './chrome-profile-mac';

const SEEN_THRESHOLD = 10 * 60 * 1000;
const DETAIL_HISTORY_LIMIT = 200;
const MAX_ADS_PER_KEYWORD = parseInt(process.env.MAX_ADS_PER_KEYWORD || '700', 10);
const MAX_SCROLLS_PER_KEYWORD = parseInt(process.env.MAX_SCROLLS_PER_KEYWORD || '60', 10);
const KEYWORD_TIMEOUT = parseInt(process.env.KEYWORD_TIMEOUT || '180000', 10);
const IDLE_TIMEOUT = parseInt(process.env.IDLE_TIMEOUT || '40000', 10);
const MAX_RETRIES_PER_KEYWORD = 2;
// [RAM-FIX] Restart browser dinh ky de giai phong RAM Chromium tich luy
// qua nhieu keyword (1 browser chay het 374 keyword -> RAM phinh toi
// 99% roi OOM kill). Chinh qua env BROWSER_RESTART_EVERY_KEYWORDS.
const BROWSER_RESTART_EVERY_KEYWORDS = parseInt(process.env.BROWSER_RESTART_EVERY_KEYWORDS || '50', 10);

let db;
const seen = new Set();
let lastSavedAt = Date.now();
let isRateLimited = false;


const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
];

function randomDelay(min = 2000, max = 4000) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function getRandomUserAgent() {
    return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

function getTodayDateStr() {
    return new Date().toLocaleDateString('en-CA', { timeZone: 'Asia/Ho_Chi_Minh' });
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

function stableMediaSignature(media = []) {
    return (media || []).map(item => {
        if (!item) return '';
        if (typeof item === 'string') return item;
        return item.original_image_url || item.url || item.image || item.src || JSON.stringify(item);
    }).join('|');
}

function buildDetailSignature(ad) {
    return JSON.stringify({
        text: ad.text || '',
        headline: ad.headline || '',
        description: ad.description || '',
        cta: ad.cta || '',
        link: ad.link || '',
        images: stableMediaSignature(ad.images || []),
        videos: stableMediaSignature(ad.videos || []),
        page_name: ad.page_name || '',
        normalized_text: normalize(ad.text || '')
    });
}

function buildDetailEntry(ad, keyword = null, transparencyData = null) {
    return {
        t: Date.now(),
        keyword,
        country: 'ALL',
        page_name: ad.page_name || null,
        page_id: ad.page_id || null,
        text: ad.text || null,
        headline: ad.headline || null,
        description: ad.description || null,
        cta: ad.cta || null,
        link: ad.link || null,
        domain: extractDomain(ad.link),
        images: ad.images || [],
        videos: ad.videos || [],
        start_date: ad.start_date || null,
        end_date: ad.end_date || null,
        is_active: ad.is_active ?? null,
        collation_count: ad.collation_count || 1,
        normalized_text: normalize(ad.text || ''),
        signature: buildDetailSignature(ad),
        transparency: transparencyData || null
    };
}

async function initializeInfrastructure() {
    const client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db(DB_NAME);

    await db.collection('ads').createIndex({ ad_archive_id: 1 }, { unique: true });
    console.log('✅ Crawler MongoDB connected & Index created.');
}

// Thumbnail ad (download + upload R2) nằm trong ./thumbnail.js — tách riêng
// để test được mà không cần browser thật.
async function saveAdAndDetailDirectly(ad, page, keyword = null, transparencyData = null) {
    const col = db.collection('ads');
    const now = Date.now();
    const domain = extractDomain(ad.link);

    const existing = await col.findOne({ ad_archive_id: ad.ad_archive_id });
    let detailEntry = buildDetailEntry({ ...ad, domain }, keyword, transparencyData);

    if (!existing) {
        // [R2-THUMBNAIL] thumb = { local, r2 } — URL R2 (null nếu R2 tắt/lỗi).
        // Thu het anh roi den preview video, thu tung URL cho den khi co thumb.
        const thumb = await downloadThumbnailForAd(page, ad, ad.ad_archive_id);

        const newAdDoc = {
            ...ad,
            domain,
            thumbnail_local: thumb.local, thumbnail_r2: thumb.r2, // [R2-THUMBNAIL] 4/4: URL ảnh trên R2 (null nếu R2 tắt/lỗi)
            first_seen: now,
            last_seen: now,
            seen_count: 1,
            growth_history: [{ t: now, c: 1 }],
            detail_history: [detailEntry],
            keyword_mentions: keyword ? [keyword] : [],
            keyword_history: keyword ? [{ t: now, keyword }] : [],
            domain_history: domain ? [{ t: now, domain }] : [],
            cta_history: ad.cta ? [{ t: now, cta: ad.cta }] : [],
            headline_history: ad.headline ? [{ t: now, headline: ad.headline }] : [],
            media_change_history: [],
            text_change_count: 0,
            headline_change_count: 0,
            domain_change_count: 0,
            cta_change_count: 0,
            creative_change_count: 0,
            last_detail_seen_at: now,
            last_detail_signature: detailEntry.signature,
            last_keyword: keyword,
            current_creative_signature: buildDetailSignature(ad)
        };

        try {
            await col.insertOne(newAdDoc);
        } catch (err) {
            if (err.code !== 11000) {
                console.error(`Lỗi khi insert ad ${ad.ad_archive_id}:`, err.message);
            }
        }
    } else {
        const shouldIncrease = now - (existing.last_seen || 0) > SEEN_THRESHOLD;
        const newCount = shouldIncrease ? (existing.seen_count || 0) + 1 : existing.seen_count;
        const oldHistory = Array.isArray(existing.detail_history) ? existing.detail_history : [];
        const historyToKeep = [...oldHistory];

        if (transparencyData) {
            historyToKeep.push(detailEntry);
        }

        // [R2-THUMBNAIL] Ad cu thieu thumbnail (lan crawl dau URL het han /
        // la ad video-only truoc khi co fallback preview) thi thu lay lai o
        // lan crawl nay — khong de thieu vinh vien.
        let thumbUpdate = {};
        if (!existing.thumbnail_r2 && !existing.thumbnail_local) {
            const thumb = await downloadThumbnailForAd(page, ad, ad.ad_archive_id);
            if (thumb && (thumb.r2 || thumb.local)) {
                thumbUpdate = { thumbnail_r2: thumb.r2, thumbnail_local: thumb.local };
            }
        }

        await col.updateOne(
            { ad_archive_id: ad.ad_archive_id },
            {
                $set: {
                    last_seen: now,
                    seen_count: newCount,
                    collation_count: ad.collation_count || 1,
                    detail_history: historyToKeep.slice(-DETAIL_HISTORY_LIMIT),
                    ...thumbUpdate,
                    ...(transparencyData ? { last_detail_seen_at: now } : {})
                },
                $addToSet: keyword ? { keyword_mentions: keyword } : {}
            }
        );
    }
}

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
                    collation_count: ad.collation_count || 1,
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

async function applyStealth(page) {
    await page.addInitScript(() => {
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        window.chrome = { runtime: {} };
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function (param) {
            if (param === 37445) return 'Intel Inc.';
            if (param === 37446) return 'Intel Iris OpenGL Engine';
            return getParameter.call(this, param);
        };
    });
}

// [RAM-FIX] Dong browser cu va mo browser moi (cung USER_DATA_DIR nen
// giu nguyen session dang nhap Facebook). Dung cho restart dinh ky va
// retry rate-limit (thay cho doan close + launch lap lai o main loop).
async function restartBrowserContext(oldContext, isDocker, reason) {
    console.log(`[RAM] ${reason} — dong browser cu, mo browser moi...`);
    try { await oldContext.close(); } catch { }
    const ctx = await createBrowserContext(isDocker);
    const heapMB = Math.round(process.memoryUsage().heapUsed / 1048576);
    console.log(`[RAM] Browser moi da mo (Node heap hien tai: ${heapMB} MB).`);
    return ctx;
}

async function createBrowserContext(isDocker) {
    return await chromium.launchPersistentContext(USER_DATA_DIR, {
        headless: false,
        channel: 'chrome',
        executablePath: fs.existsSync('/usr/bin/google-chrome') ? '/usr/bin/google-chrome' : undefined,
        userAgent: getRandomUserAgent(),
        viewport: { width: randomInt(1366, 1920), height: randomInt(768, 1080) },
        locale: 'en-US',
        timezoneId: 'America/New_York',
        args: [
            '--start-maximized',
            '--disable-blink-features=AutomationControlled',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process',
            '--disable-dev-shm-usage',
            ...(isDocker ? ['--no-sandbox', '--disable-setuid-sandbox'] : [])
        ]
    });
}

async function dismissOverlays(page) {
    try {
        const selectors = [
            'button:has-text("OK")',
            'button:has-text("Allow all cookies")',
            'button:has-text("Accept All")',
            'button:has-text("Close")',
            '[aria-label="Close"]',
            '[aria-label="close"]'
        ];
        for (const selector of selectors) {
            const btn = await page.$(selector);
            if (btn && await btn.isVisible()) {
                await btn.click({ timeout: 1500 }).catch(() => { });
            }
        }
    } catch { }
}

async function scanKeyword(context, keyword, index, total) {
    const page = await context.newPage();
    
    await applyStealth(page);

    lastSavedAt = Date.now();
    isRateLimited = false;
    let keywordAdsCount = 0;
    let pendingSaves = 0;

    page.on('response', async (res) => {
        try {
            const url = res.url();
            if (!url.includes('graphql')) return;

            if (res.status() === 429) {
                isRateLimited = true;
                return;
            }

            const text = await res.text();

            if (text.includes('Rate limit exceeded') || text.includes('1675004') || text.includes('temporarily blocked')) {
                isRateLimited = true;
                return;
            }

            if (
                (text.includes('ad_library_main') || text.includes('search_results_connection') || text.includes('collated_results')) &&
                !text.includes('ad_details')
            ) {
                const json = JSON.parse(text);
                const ads = extractAds(json);
                for (const ad of ads) {
                    const key = `${ad.ad_archive_id}:${keyword}`;
                    if (!seen.has(key)) {
                        seen.add(key);
                        pendingSaves++;
                        try {
                            await saveAdAndDetailDirectly(ad, page, keyword, null);
                            keywordAdsCount++;
                            lastSavedAt = Date.now();
                            console.log(`[Keyword ${index}/${total}] "${keyword}" (#${keywordAdsCount}) -- Saved Ad ID: ${ad.ad_archive_id}`);
                        } finally {
                            pendingSaves--;
                        }
                    }
                }
            }
        } catch { }
    });

    const today = getTodayDateStr();
    const url = `https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&is_targeted_country=false&media_type=all&q=${encodeURIComponent(keyword)}&search_type=keyword_unordered&sort_data[mode]=relevancy_monthly_grouped&sort_data[direction]=desc&start_date[min]=${today}&start_date[max]=`;

    try {
        console.log(`\n🔍 [Tiến trình ${index}/${total}] Đang quét từ khóa: "${keyword}" (Bắt đầu từ: ${today})`);
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
        await page.waitForTimeout(randomDelay(4000, 7000));

        await dismissOverlays(page);

        let prev = 0;
        let same = 0;
        let scrollCount = 0;
        const startTime = Date.now();

        while (true) {
            if (isRateLimited) {
                console.log(`⚠️ Phát hiện Rate Limit khi đang quét từ khóa "${keyword}".`);
                break;
            }
            if (keywordAdsCount >= MAX_ADS_PER_KEYWORD) {
                console.log(`🎯 [${keyword}] Đã đạt giới hạn ${keywordAdsCount}/${MAX_ADS_PER_KEYWORD} ads.`);
                break;
            }
            if (scrollCount >= MAX_SCROLLS_PER_KEYWORD) {
                console.log(`⏹️ [${keyword}] Đã đạt tối đa ${scrollCount} lượt cuộn.`);
                break;
            }
            if (Date.now() - startTime > KEYWORD_TIMEOUT) {
                console.log(`⏱️ [${keyword}] Hết thời gian tối đa cho phép (${KEYWORD_TIMEOUT / 1000}s).`);
                break;
            }
            if (Date.now() - lastSavedAt > IDLE_TIMEOUT) {
                console.log(`⌛ [${keyword}] Không nhận được thêm quảng cáo mới trong ${IDLE_TIMEOUT / 1000}s.`);
                break;
            }

            scrollCount++;
            const scrollAmount = randomInt(500, 900);
            await page.evaluate((amount) => { window.scrollBy(0, amount); }, scrollAmount);
            await page.waitForTimeout(randomDelay(1500, 3000));

            await dismissOverlays(page);

            const curr = await page.evaluate(() => document.body.scrollHeight);
            if (curr === prev) {
                same++;
            } else {
                same = 0;
            }
            prev = curr;
            if (same >= 5) {
                console.log(`🏁 [${keyword}] Đã cuộn đến cuối kết quả tìm kiếm.`);
                break;
            }
        }
    } catch (error) {
        console.error(`❌ Lỗi khi quét từ khóa "${keyword}":`, error.message);
    }

    const waitPendingStart = Date.now();
    while (pendingSaves > 0 && Date.now() - waitPendingStart < 5000) {
        await new Promise(resolve => setTimeout(resolve, 200));
    }

    try { await page.close(); } catch { }

    if (isRateLimited) return false;
    await new Promise(resolve => setTimeout(resolve, randomDelay(5000, 8000)));
    return true;
}

const isDocker = process.env.RUNNING_IN_DOCKER === 'true';
(async () => {
    await initializeInfrastructure();
    // Log trạng thái R2 ngay khi khởi động (vụ 11/11/2026: R2 tắt lặng lẽ
    // làm ad mới mất thumbnail mà log không nói lý do).
    const r2 = getR2Status();
    if (r2.enabled) console.log(`[R2] BẬT — bucket ${r2.bucket}, public ${r2.publicBaseUrl} — thumbnail ad mới sẽ upload lên R2.`);
    else console.log(`[R2] TẮT (${r2.reason}) — ad mới sẽ KHÔNG có thumbnail. Kiểm tra key/secret R2 hoặc cài @aws-sdk/client-s3.`);
    let context = await createBrowserContext(isDocker);

    const keywords = [
        // --- Gia dung & Nha cua ---
        "kitchen", "cookware", "blender", "air fryer", "coffee maker", "kettle",
        "vacuum", "robot vacuum", "air purifier", "humidifier", "fan", "heater",
        "lamp", "lighting", "led lights", "furniture", "sofa", "chair",
        "table", "bed", "mattress", "bedding", "curtains", "storage",
        "organizer", "cleaning", "mop", "broom", "laundry", "detergent",
        "bakeware", "cutting board", "knife set", "pots", "pans", "water bottle",
        "thermos", "lunch box", "food container", "kitchen gadgets", "home appliances", "household",
        "smart home", "door lock", "security camera", "doorbell", "shower", "faucet",
        "bathroom", "mirror", "rug", "carpet", "decor", "wall art",
        "plant pot", "garden", "tools", "drill", "toolbox", "ladder",
        "power strip", "batteries", "charger", "cable",
        // --- Da nang & Tien ich ---
        "versatile", "multipurpose", "multi-purpose", "all-in-one", "foldable", "portable",
        "adjustable", "reusable", "waterproof", "cordless", "wireless", "rechargeable",
        "compact", "mini", "gadget", "holder", "stand", "mount",
        "case", "2-in-1", "3-in-1", "travel", "outdoor",
        // --- Hat giong & Lam vuon ---
        "seeds", "seed", "plant seeds", "vegetable seeds", "flower seeds", "herb seeds",
        "fruit seeds", "garden seeds", "organic seeds", "heirloom seeds", "planting", "gardening",
        "planter", "soil", "fertilizer", "greenhouse", "hydroponic", "sprout",
        "grow kit", "bonsai", "succulent", "indoor plants",
        // --- My pham & Lam dep ---
        "cosmetics", "makeup", "skincare", "serum", "moisturizer", "sunscreen",
        "lipstick", "mascara", "eyeliner", "foundation", "concealer", "blush",
        "eyeshadow", "nail polish", "perfume", "fragrance", "cologne", "shampoo",
        "conditioner", "hair care", "hair oil", "hair mask", "beard", "shaving",
        "razor", "teeth whitening", "toothpaste", "whitening", "anti-aging", "collagen",
        "vitamin c", "retinol", "face mask", "cleanser", "toner", "lotion",
        "body wash", "soap", "deodorant", "beauty", "nail", "lashes",
        "brows", "lip gloss", "hair dryer", "straightener", "curling iron", "essential oils",
        // --- Mua sam & Khuyen mai ---
        "shop now", "buy now", "free shipping", "freeship", "discount", "sale",
        "clearance", "flash sale", "best seller", "new arrival", "gift", "gifts",
        "gift ideas", "bundle", "deal", "offer", "promotion", "coupon",
        "affordable", "order now", "limited time", "exclusive", "premium", "trending",
        "viral", "must have", "top rated", "money back", "bundle deal", "50% off",
        "special offer",
        // --- Dien tu ---
        "smartphone", "phone", "headphones", "earbuds", "speaker", "smartwatch",
        "watch", "tablet", "laptop", "camera", "drone", "projector",
        "power bank", "keyboard", "mouse", "monitor", "tv", "soundbar",
        "phone case", "bluetooth", "earphone", "headset", "microphone", "webcam",
        "printer", "router", "wifi", "smart tv",
        // --- Thoi trang ---
        "clothing", "shoes", "sneakers", "sandals", "boots", "dress",
        "shirt", "pants", "jeans", "jacket", "coat", "hoodie",
        "t-shirt", "underwear", "socks", "hat", "cap", "scarf",
        "gloves", "belt", "wallet", "handbag", "backpack", "luggage",
        "sunglasses", "jewelry", "necklace", "ring", "earrings", "bracelet",
        "streetwear", "designer fashion", "activewear", "leggings", "swimwear", "pajamas",
        "slippers", "heels",
        // --- Suc khoe & The hinh ---
        "supplements", "vitamins", "protein", "probiotics", "fish oil", "omega",
        "magnesium", "zinc", "herbal", "tea", "honey", "detox",
        "weight loss", "diet", "keto", "vegan", "organic", "healthy",
        "fitness", "gym", "yoga", "dumbbell", "resistance bands", "treadmill",
        "exercise", "massage", "posture", "orthopedic", "pain relief", "sleep",
        "pillow", "blanket", "yoga mat", "massage gun", "posture corrector", "water filter",
        // --- Me & Be ---
        "baby", "baby clothes", "baby toys", "diapers", "stroller", "bottle",
        "maternity", "kids", "children", "baby food", "baby care", "kids shoes",
        "kids clothing", "school bag", "baby monitor", "crib", "baby carrier",
        // --- Thu cung ---
        "pet", "dog", "cat", "dog food", "cat food", "pet toys",
        "pet grooming", "leash", "collar", "litter", "bird", "fish",
        "pet bed", "pet shampoo", "pet treats", "aquarium", "dog leash", "cat litter",
        // --- Thuc pham ---
        "snacks", "coffee", "chocolate", "spices", "sauce", "noodles",
        "rice", "dried fruit", "nuts", "candy", "beverage", "juice",
        "energy drink", "protein bar", "granola", "oatmeal", "peanut butter", "olive oil",
        "coconut oil", "seasoning", "baking", "organic food", "healthy food", "gluten-free",
        // --- Xe co ---
        "car accessories", "dash cam", "car wash", "car seat", "tires", "car care",
        "motorcycle", "bike", "bicycle", "helmet", "cycling", "car charger",
        "car mount", "seat cover", "floor mat", "car wax",
        // --- The thao & Ngoai troi ---
        "sports", "running", "hiking", "camping", "fishing", "swimming",
        "tennis", "golf", "basketball", "soccer", "football", "skateboard",
        "surf", "ski", "snowboard", "workout", "fitness equipment", "tent",
        "sleeping bag", "raincoat", "umbrella", "cooler", "picnic",
        "barbecue", "grill",
    ];

    let keywordRetries = 0;

    for (let i = 0; i < keywords.length; i++) {
        const keyword = keywords[i];
        const success = await scanKeyword(context, keyword, i + 1, keywords.length);

        if (!success) {
            keywordRetries++;
            if (keywordRetries <= MAX_RETRIES_PER_KEYWORD) {
                console.log(`⏳ [${keyword}] Gặp lỗi / Rate Limit (thử lại lần ${keywordRetries}/${MAX_RETRIES_PER_KEYWORD}). Tạm dừng hệ thống 3 phút rồi thử lại...`);
                await new Promise(resolve => setTimeout(resolve, 3 * 60 * 1000));
                context = await restartBrowserContext(context, isDocker, `[${keyword}] Rate limit`);
                i--; 
            } else {
                console.warn(`⚠️ [${keyword}] Đã thử lại ${MAX_RETRIES_PER_KEYWORD} lần thất bại. Bỏ qua từ khóa này để tiếp tục!`);
                keywordRetries = 0;
                await new Promise(resolve => setTimeout(resolve, 15000));
            }
        } else {
            keywordRetries = 0;
            const doneCount = i + 1;
            if (BROWSER_RESTART_EVERY_KEYWORDS > 0
                && doneCount % BROWSER_RESTART_EVERY_KEYWORDS === 0
                && doneCount < keywords.length) {
                context = await restartBrowserContext(context, isDocker,
                    `Da xong ${doneCount}/${keywords.length} keyword`);
            }
        }
    }

    console.log('✅ ĐÃ HOÀN TẤT TOÀN BỘ DANH SÁCH TỪ KHÓA!');
    process.exit(0);
})();
