const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');
const { MongoClient } = require('mongodb');

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

const MONGO_URI = process.env.MONGO_URI || 'mongodb://192.168.0.125:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2';
const DB_NAME = 'fb_ads';

const USER_DATA_DIR = './chrome-profile-mac';
const CHROME_PATH = 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe';
const STORAGE_PATH = path.join(__dirname, '..', '..', 'storage', 'images');
const PROFILE = 'Default';

const SEEN_THRESHOLD = 10 * 60 * 1000;
const DETAIL_HISTORY_LIMIT = 200;
const MAX_ADS_PER_KEYWORD = parseInt(process.env.MAX_ADS_PER_KEYWORD || '300', 10);
const MAX_SCROLLS_PER_KEYWORD = parseInt(process.env.MAX_SCROLLS_PER_KEYWORD || '60', 10);
const KEYWORD_TIMEOUT = parseInt(process.env.KEYWORD_TIMEOUT || '180000', 10); // 3 phút
const IDLE_TIMEOUT = parseInt(process.env.IDLE_TIMEOUT || '40000', 10); // 40s
const MAX_RETRIES_PER_KEYWORD = 2;

let db;
const seen = new Set();
let lastSavedAt = Date.now();
let isRateLimited = false;

// Tập hợp User-Agents phổ biến, mới nhất
const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0'
];

function getRandomUA() {
    return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

fs.mkdirSync(STORAGE_PATH, { recursive: true });

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

async function downloadThumbnail(page, imageUrl, adId) {
    if (!imageUrl || !adId) return null;
    try {
        const response = await page.request.get(imageUrl, { timeout: 5000 });
        if (!response.ok()) return null;
        const buffer = await response.body();
        const fileName = `${adId}.jpg`;
        const filePath = path.join(STORAGE_PATH, fileName);
        await fs.promises.writeFile(filePath, buffer);
        return `/images/${fileName}`;
    } catch {
        return null;
    }
}

async function saveAdAndDetailDirectly(ad, page, keyword = null, transparencyData = null) {
    const col = db.collection('ads');
    const now = Date.now();
    const domain = extractDomain(ad.link);
    const existing = await col.findOne({ ad_archive_id: ad.ad_archive_id });
    let detailEntry = buildDetailEntry({ ...ad, domain }, keyword, transparencyData);

    if (!existing) {
        const firstImage = ad.images && ad.images.length > 0 ? ad.images[0] : null;
        const localThumb = firstImage ? await downloadThumbnail(page, firstImage.original_image_url || firstImage.url, ad.ad_archive_id) : null;

        const newAdDoc = {
            ...ad,
            domain,
            thumbnail_local: localThumb,
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

        await col.updateOne(
            { ad_archive_id: ad.ad_archive_id },
            {
                $set: {
                    last_seen: now,
                    seen_count: newCount,
                    detail_history: historyToKeep.slice(-DETAIL_HISTORY_LIMIT),
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

// Cải thiện phần vượt detection (stealth)
async function applyStealth(page) {
    await page.addInitScript(() => {
        const isMac = navigator.userAgent.includes('Mac');
        
        // 1. Pass WebDriver
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        
        // 2. Fake OS properties
        Object.defineProperty(navigator, 'platform', { get: () => isMac ? 'MacIntel' : 'Win32' });
        Object.defineProperty(navigator, 'languages', { get: () => ['vi-VN', 'vi', 'en-US', 'en'] });
        Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
        Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => randomInt(4, 12) });
        
        // 3. Fake Plugins
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4] });
        
        // 4. Fake Chrome Window attributes
        window.chrome = {
            runtime: {},
            app: {},
            csi: () => {}
        };

        // 5. Fake WebGL Fingerprint
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function (param) {
            if (param === 37445) return 'Intel Inc.';
            if (param === 37446) return isMac ? 'Intel Iris OpenGL Engine' : 'Intel(R) UHD Graphics 620';
            return getParameter.call(this, param);
        };

        // 6. Fix Permissions API detection
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );
        
        function randomInt(min, max) {
            return Math.floor(Math.random() * (max - min + 1)) + min;
        }
    });
}

async function createBrowserContext(isDocker) {
    return await chromium.launchPersistentContext(USER_DATA_DIR, {
        headless: true,
        userAgent: getRandomUA(), // Lấy UA ngẫu nhiên thay vì fix cứng
        viewport: { width: randomInt(1366, 1920), height: randomInt(768, 1080) },
        //...(isDocker ? {} : { executablePath: CHROME_PATH }),
        args: [
            `--profile-directory=${PROFILE}`,
            '--start-maximized',
            '--disable-blink-features=AutomationControlled',
            '--disable-infobars', 
            '--ignore-certificate-errors',
            ...(isDocker ? ['--no-sandbox', '--disable-setuid-sandbox'] : [])
        ]
    });
}

async function dismissOverlays(page) {
    try {
        const selectors = [
            'button:has-text("OK")',
            'button:has-text("Allow all cookies")',
            'button:has-text("Cho phép tất cả cookie")',
            'button:has-text("Chấp nhận tất cả")',
            'button:has-text("Đóng")',
            'button:has-text("Close")',
            '[aria-label="Đóng"]',
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
    const page = await context.newPage({
        viewport: { width: randomInt(1200, 1920), height: randomInt(700, 1080) }
    });

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

    const url = `https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country=VN&media_type=all&search_type=keyword_unordered&q=${encodeURIComponent(keyword)}`;

    try {
        console.log(`\n🔍 [Tiến trình ${index}/${total}] Đang quét từ khóa: "${keyword}"`);
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

            // Xử lý timeout chờ khi nghi ngờ đến cuối trang
            if (same >= 5) {
                console.log(`⏳ [${keyword}] Không thấy cuộn thêm được. Đang đợi 5 giây kiểm tra lazy load...`);
                await page.waitForTimeout(5000); // Timeout đợi thêm mạng
                
                const checkFinal = await page.evaluate(() => document.body.scrollHeight);
                if (checkFinal === curr) {
                    console.log(`🏁 [${keyword}] Đã cuộn đến cuối kết quả tìm kiếm thực sự.`);
                    break;
                } else {
                    console.log(`▶️ [${keyword}] Có dữ liệu mới load thêm, tiếp tục quét...`);
                    same = 0; // Reset và cuộn tiếp
                    prev = checkFinal;
                }
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
    let context = await createBrowserContext(isDocker);

    const keywords = [
        'thực phẩm chức năng', 'giảm cân', 'kem chống nắng',
        'trà sữa', 'cafe', 'bất động sản',
        'quần áo', 'giày dép', 'túi xách', 'đồng hồ', 'phụ kiện',
        'mua ngay', 'xem ngay', 'khuyến mãi', 'giảm giá', 'ưu đãi',
        'thương mại điện tử', 'điện thoại', 'laptop', 'máy tính bảng', 'thiết bị gia dụng',
        'xe máy', 'ô tô', 'du lịch', 'khách sạn', 'resort',
        'spa', 'làm đẹp', 'nội thất', 'trang trí nhà cửa', 'đồ gia dụng',
        'sức khỏe', 'thể thao', 'gym', 'yoga', 'chăm sóc da',
        'streetwear', 'thời trang thiết kế', 'dược', 'thuốc', 'vitamin',
        'thảo dược', 'sản phẩm chăm sóc sức khỏe', 'ăn vặt', 'nhà hàng',
        'buffet', 'đồ ăn healthy'
    ];

    let keywordRetries = 0;

    for (let i = 0; i < keywords.length; i++) {
        const keyword = keywords[i];
        const success = await scanKeyword(context, keyword, i + 1, keywords.length);

        if (!success) {
            keywordRetries++;
            if (keywordRetries <= MAX_RETRIES_PER_KEYWORD) {
                console.log(`⏳ [${keyword}] Gặp lỗi / Rate Limit (thử lại lần ${keywordRetries}/${MAX_RETRIES_PER_KEYWORD}). Tạm dừng hệ thống 3 phút rồi thử lại...`);
                try { await context.close(); } catch { }
                await new Promise(resolve => setTimeout(resolve, 3 * 60 * 1000));
                context = await createBrowserContext(isDocker);
                i--; 
            } else {
                console.warn(`⚠️ [${keyword}] Đã thử lại ${MAX_RETRIES_PER_KEYWORD} lần thất bại. Bỏ qua từ khóa này để tiếp tục từ khóa kế tiếp!`);
                keywordRetries = 0;
                await new Promise(resolve => setTimeout(resolve, 15000));
            }
        } else {
            keywordRetries = 0;
        }
    }

    console.log('✅ ĐÃ HOÀN TẤT TOÀN BỘ DANH SÁCH TỪ KHÓA!');
    process.exit(0);
})();
