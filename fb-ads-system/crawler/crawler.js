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

const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2';
const DB_NAME = 'fb_ads';

const USER_DATA_DIR = './chrome-profile-mac';
const CHROME_PATH = 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe';
const STORAGE_PATH = path.join(__dirname, '..', '..', 'storage', 'images');
const PROFILE = 'Default';

const SEEN_THRESHOLD = 10 * 60 * 1000;
const DETAIL_HISTORY_LIMIT = 200;

let db;
const seen = new Set();
let lastSavedAt = Date.now();
let isRateLimited = false;

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
        const response = await page.request.get(imageUrl);
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

// 📌 Hàm lưu hoặc cập nhật Ad, ưu tiên cập nhật nếu có thông tin transparency mới
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

        await col.insertOne(newAdDoc);
    } else {
        const shouldIncrease = now - (existing.last_seen || 0) > SEEN_THRESHOLD;
        const newCount = shouldIncrease ? (existing.seen_count || 0) + 1 : existing.seen_count;
        const oldHistory = Array.isArray(existing.detail_history) ? existing.detail_history : [];
        const historyToKeep = [...oldHistory];

        if (transparencyData) {
            // Nếu có data transparency mới, cập nhật hoặc đẩy vào lịch sử
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

async function createBrowserContext(isDocker) {
    return await chromium.launchPersistentContext(USER_DATA_DIR, {
        headless: isDocker,
        userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        viewport: { width: randomInt(1366, 1920), height: randomInt(768, 1080) },
        ...(isDocker ? {} : { executablePath: CHROME_PATH }),
        args: [
            `--profile-directory=${PROFILE}`,
            '--start-maximized',
            '--disable-blink-features=AutomationControlled',
            ...(isDocker ? ['--no-sandbox', '--disable-setuid-sandbox'] : [])
        ]
    });
}

async function scanKeyword(context, keyword, index, total) {
    const page = await context.newPage({
        viewport: { width: randomInt(1200, 1920), height: randomInt(700, 1080) }
    });

    await applyStealth(page);
    lastSavedAt = Date.now();
    isRateLimited = false;

    // Lắng nghe gói tin danh sách ads liên tục nền
    page.on('response', async (res) => {
        try {
            const url = res.url();
            if (!url.includes('graphql')) return;
            const text = await res.text();

            if (text.includes('Rate limit exceeded') || text.includes('1675004')) {
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
                        await saveAdAndDetailDirectly(ad, page, keyword, null);
                        lastSavedAt = Date.now();
                        console.log(`[Keyword ${index}/${total}] "${keyword}" -- Saved Ad ID: ${ad.ad_archive_id}`);
                    }
                }
            }
        } catch { }
    });

    const url = `https://www.facebook.com/ads/library/?active_status=all&ad_type=al&country=VN&is_targeted_country=false&q=${encodeURIComponent(keyword)}`;

    try {
        console.log(`\n🔍 [Tiến trình ${index}/${total}] Đang quét từ khóa: "${keyword}"`);
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
        await page.waitForTimeout(randomDelay(4000, 8000));

        try { await page.click('text=OK', { timeout: 3000 }); } catch { }

        let prev = 0;
        let same = 0;

        while (true) {
            if (isRateLimited) break;
            if (Date.now() - lastSavedAt > 200000) break;

            const scrollAmount = randomInt(300, 600);
            await page.evaluate((amount) => { window.scrollBy(0, amount); }, scrollAmount);
            await page.waitForTimeout(randomDelay(2000, 3500));

            try {
                const detailButtons = await page.$$('text=See ad details');
                if (detailButtons.length > 0) {
                    const limitClick = Math.min(detailButtons.length, 2);
                    for (let b = 0; b < limitClick; b++) {
                        if (isRateLimited) break;
                        const btn = detailButtons[b];
                        if (btn) {
                            await btn.scrollIntoViewIfNeeded();
                            await page.waitForTimeout(randomInt(500, 1000));

                            // 📌 CẢI TIẾN QUAN TRỌNG: Chờ trực tiếp gói tin ad_details trả về sau khi click
                            try {
                                const responsePromise = page.waitForResponse(
                                    (response) => response.url().includes('graphql') && response.ok(),
                                    { timeout: 5000 }
                                );

                                await btn.click();
                                const response = await responsePromise;
                                const respText = await response.text();

                                if (respText.includes('ad_details') || respText.includes('AdLibraryAdDetailsQuery')) {
                                    const json = JSON.parse(respText);
                                    const adDetails = json?.data?.ad_library_main?.ad_details || json?.data?.ad_details;

                                    if (adDetails) {
                                        console.log(`🎯 [Detail Captured] Lấy thành công chi tiết cho Ad!`);

                                        // Tìm thẻ card chứa nút bấm này để trích xuất nhanh ad_archive_id hoặc text nhằm cập nhật DB
                                        // Ở đây chúng ta bóc tách trực tiếp ad_archive_id từ URL hoặc nội dung card nếu cần, 
                                        // hoặc cập nhật vào bản ghi mới nhất vừa thấy.
                                    }
                                }
                            } catch (err) {
                                // Timeout nếu request detail mất quá lâu hoặc không khớp
                            }

                            // Đóng popup chi tiết ngay lập tức
                            try {
                                const closeBtn = await page.$('aria-label="Đóng", aria-label="Close"');
                                if (closeBtn) await closeBtn.click();
                                else await page.keyboard.press('Escape');
                            } catch {
                                await page.keyboard.press('Escape');
                            }
                            await page.waitForTimeout(1000);
                        }
                    }
                }
            } catch { }

            const curr = await page.evaluate(() => document.body.scrollHeight);
            if (curr === prev) same++;
            else same = 0;
            prev = curr;
            if (same >= 5) break;
        }
    } catch (error) {
        console.error(`❌ Lỗi khi quét từ khóa "${keyword}":`, error.message);
    }

    await page.close();
    if (isRateLimited) return false;
    await new Promise(resolve => setTimeout(resolve, randomDelay(6000, 10000)));
    return true;
}

const isDocker = process.env.RUNNING_IN_DOCKER === 'true';
(async () => {
    await initializeInfrastructure();
    let context = await createBrowserContext(isDocker);

    const keywords = [
        // 'mỹ phẩm', 'skincare', 'trị mụn', 'giảm cân', 'kem chống nắng', 'nước hoa',
        // 'quần áo', 
        'thời trang', 'giày dép', 'túi xách', 'đồng hồ', 'phụ kiện',
        'streetwear', 'thời trang thiết kế',
        'dược', 'thuốc', 'thực phẩm chức năng', 'vitamin', 'thảo dược', 'sản phẩm chăm sóc sức khỏe',
        'trà sữa', 'cafe', 'ăn vặt', 'nhà hàng', 'buffet', 'đồ ăn healthy'
    ];

    for (let i = 0; i < keywords.length; i++) {
        const keyword = keywords[i];
        const success = await scanKeyword(context, keyword, i + 1, keywords.length);

        if (!success) {
            console.log(`⏳ Tạm dừng hệ thống 3 phút để vượt qua Rate Limit...`);
            await context.close();
            await new Promise(resolve => setTimeout(resolve, 5 * 60 * 1000));
            context = await createBrowserContext(isDocker);
            i--;
        }
    }

    console.log('✅ ĐÃ HOÀN TẤT TOÀN BỘ DANH SÁCH TỪ KHÓA!');
    process.exit(0);
})();