const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');
const { MongoClient } = require('mongodb');
// [R2-THUMBNAIL] Thumbnail ad qua module ./thumbnail.js (tải ảnh -> upload
// R2, R2-only, không lưu file local). Trạng thái R2 được log 1 dòng khi
// khởi động ở main IIFE cuối file.
const { downloadThumbnailForAd, getR2Status, hasThumbnail, videoUrlForAd, isVideoThumbnailUrl } = require('./thumbnail');
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
// 99% roi OOM kill). Mac dinh 30 (khong phai 50): crawler-2 chi co 3.8GB
// RAM, ngay 11/11 process bi OOM kill o keyword 38 — restart phai xay ra
// TRUOC diem chet (38) thi moi cuu duoc, restart o 50 la qua muon.
// Chinh qua env BROWSER_RESTART_EVERY_KEYWORDS (neu van OOM thi ha 25).
const BROWSER_RESTART_EVERY_KEYWORDS = parseInt(process.env.BROWSER_RESTART_EVERY_KEYWORDS || '30', 10);

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

function getTodayString() {
    return new Intl.DateTimeFormat('en-CA', {
        timeZone: 'Asia/Ho_Chi_Minh',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
    }).format(new Date());
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
        let thumb = await downloadThumbnailForAd(page, ad, ad.ad_archive_id);

        // [VIDEO-FALLBACK] Ad moi khong co thumbnail (tai anh loi / khong
        // co URL anh) thi lay TAM video URL lam thumbnail_r2 de FE con hien
        // video, thay vi bo qua ad — logic cu (THUMB-ONLY-DB) lam mat ~44%
        // ad moi. Chi bo qua khi ad khong co ca anh lan video; ad bi bo qua
        // se duoc thu lai o lan crawl sau (gap lai trong list moi — `seen`
        // chi chan trong cung keyword/luot chay).
        if (!hasThumbnail(thumb)) {
            const videoUrl = videoUrlForAd(ad);
            if (videoUrl) {
                thumb = { local: null, r2: videoUrl };
                console.log(`🎬 [${keyword || '?'}] Ad ID: ${ad.ad_archive_id} không có thumbnail — dùng tạm video URL làm thumbnail_r2`);
            }
        }
        if (!hasThumbnail(thumb)) {
            console.log(`⏭️ [${keyword || '?'}] Bỏ qua Ad ID: ${ad.ad_archive_id} (không có ảnh lẫn video — sẽ thử lại lần crawl sau)`);
            return;
        }

        const newAdDoc = {
            ...ad,
            domain,
            thumbnail_local: thumb.local, thumbnail_r2: thumb.r2, // [R2-THUMBNAIL] URL ảnh trên R2, hoặc video URL tạm nếu ad không có ảnh
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
        // lan crawl nay — khong de thieu vinh vien. Ad dang dung video URL
        // tam lam thumbnail_r2 cung thu lai: URL video het han oe, neu tai
        // duoc anh that thi ghi de thanh URL R2.
        let thumbUpdate = {};
        if ((!existing.thumbnail_r2 && !existing.thumbnail_local) || isVideoThumbnailUrl(existing.thumbnail_r2)) {
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
        const isMac = navigator.userAgent.includes('Mac');
        
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'platform', { get: () => isMac ? 'MacIntel' : 'Win32' });
        Object.defineProperty(navigator, 'languages', { get: () => ['vi-VN', 'vi', 'en-US', 'en'] });
        Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
        Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
        
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
        
        window.chrome = {
            runtime: {},
            app: {},
            csi: () => {}
        };

        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function (param) {
            if (param === 37445) return 'Intel Inc.';
            if (param === 37446) return isMac ? 'Intel Iris OpenGL Engine' : 'Intel(R) UHD Graphics 620';
            return getParameter.call(this, param);
        };

        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );
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
        locale: 'vi-VN',
        timezoneId: 'Asia/Ho_Chi_Minh',
        args: [
            '--start-maximized',
            '--disable-blink-features=AutomationControlled',
            '--disable-infobars',
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

    const today = getTodayString();
    const url = `https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&is_targeted_country=false&media_type=all&q=${encodeURIComponent(keyword)}&search_type=keyword_unordered&sort_data[mode]=relevancy_monthly_grouped&sort_data[direction]=desc&start_date[min]=${today}&start_date[max]=`;

    try {
        console.log(`\n🔍 [Tiến trình ${index}/${total}] Đang quét từ khóa: "${keyword}" (Từ ngày: ${today})`);
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
                console.log(`⏳ [${keyword}] Không thấy cuộn thêm được. Đang đợi 5 giây kiểm tra lazy load...`);
                await page.waitForTimeout(5000); 
                
                const checkFinal = await page.evaluate(() => document.body.scrollHeight);
                if (checkFinal === curr) {
                    console.log(`🏁 [${keyword}] Đã cuộn đến cuối kết quả tìm kiếm thực sự.`);
                    break;
                } else {
                    console.log(`▶️ [${keyword}] Có dữ liệu mới load thêm, tiếp tục quét...`);
                    same = 0; 
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
    // Log trạng thái R2 ngay khi khởi động (vụ 11/11/2026: R2 tắt lặng lẽ
    // làm ad mới mất thumbnail mà log không nói lý do).
    const r2 = getR2Status();
    if (r2.enabled) console.log(`[R2] BẬT — bucket ${r2.bucket}, public ${r2.publicBaseUrl} — thumbnail ad mới sẽ upload lên R2.`);
    else console.log(`[R2] TẮT (${r2.reason}) — ad mới sẽ KHÔNG có thumbnail. Kiểm tra key/secret R2 hoặc cài @aws-sdk/client-s3.`);
    let context = await createBrowserContext(isDocker);

    const keywords = [
        // --- Gia dung & Nha cua ---
        'nhà bếp', 'đồ nấu ăn', 'máy xay sinh tố', 'nồi chiên không dầu', 'máy pha cà phê',
        'ấm siêu tốc', 'máy hút bụi', 'robot hút bụi', 'máy lọc không khí', 'máy tạo ẩm',
        'quạt', 'máy sưởi', 'đèn', 'đèn led', 'nội thất',
        'sofa', 'ghế', 'bàn', 'giường', 'nệm',
        'chăn ga gối', 'rèm cửa', 'hộp đựng đồ', 'khay đựng', 'dụng cụ vệ sinh',
        'cây lau nhà', 'chổi', 'nước giặt', 'khuôn làm bánh', 'thớt',
        'bộ dao', 'nồi', 'chảo', 'bình nước', 'bình giữ nhiệt',
        'hộp cơm', 'hộp đựng thức ăn', 'đồ dùng nhà bếp', 'đồ gia dụng', 'gia dụng',
        'nhà thông minh', 'khóa cửa', 'camera an ninh', 'chuông cửa', 'vòi sen',
        'vòi nước', 'phòng tắm', 'gương', 'thảm', 'thảm trải sàn',
        'đồ trang trí', 'tranh treo tường', 'chậu cây', 'vườn', 'dụng cụ',
        'máy khoan', 'hộp dụng cụ', 'thang', 'ổ cắm điện', 'pin',
        'cục sạc', 'dây cáp',
        // --- Da nang & Tien ich ---
        'đa năng', 'đa công dụng', 'tất cả trong một', 'gấp gọn', 'cầm tay',
        'di động', 'điều chỉnh được', 'tái sử dụng', 'chống nước', 'không dây',
        'sạc lại được', 'nhỏ gọn', 'mini', 'đồ tiện ích', 'giá đỡ',
        'chân đế', 'giá treo', 'ốp lưng', '2 trong 1', '3 trong 1',
        'du lịch', 'ngoài trời',
        // --- Hat giong & Lam vuon ---
        'hạt giống', 'hạt giống cây trồng', 'hạt giống rau', 'hạt giống hoa', 'hạt giống thảo mộc',
        'hạt giống trái cây', 'hạt giống làm vườn', 'hạt giống hữu cơ', 'hạt giống thuần chủng', 'trồng cây',
        'làm vườn', 'chậu trồng cây', 'đất trồng', 'phân bón', 'nhà kính',
        'thủy canh', 'mầm', 'bộ trồng cây', 'cây bonsai', 'sen đá',
        'cây trong nhà',
        // --- My pham & Lam dep ---
        'mỹ phẩm', 'trang điểm', 'chăm sóc da', 'serum', 'kem dưỡng ẩm',
        'kem chống nắng', 'son môi', 'mascara', 'kẻ mắt', 'kem nền',
        'kem che khuyết điểm', 'phấn má', 'phấn mắt', 'sơn móng tay', 'nước hoa',
        'hương thơm', 'dầu gội', 'dầu xả', 'chăm sóc tóc', 'dầu dưỡng tóc',
        'ủ tóc', 'râu', 'cạo râu', 'dao cạo', 'làm trắng răng',
        'kem đánh răng', 'làm trắng', 'chống lão hóa', 'collagen', 'vitamin c',
        'retinol', 'mặt nạ', 'sữa rửa mặt', 'toner', 'sữa dưỡng thể',
        'sữa tắm', 'xà phòng', 'lăn khử mùi', 'làm đẹp', 'làm nail',
        'nối mi', 'lông mày', 'son bóng', 'máy sấy tóc', 'máy duỗi tóc',
        'máy uốn tóc', 'tinh dầu',
        // --- Mua sam & Khuyen mai ---
        'mua ngay', 'miễn phí vận chuyển', 'freeship', 'giảm giá', 'sale',
        'khuyến mãi', 'xả kho', 'flash sale', 'bán chạy nhất', 'hàng mới về',
        'quà tặng', 'ý tưởng quà tặng', 'combo', 'deal', 'ưu đãi',
        'mã giảm giá', 'giá rẻ', 'đặt hàng ngay', 'giới hạn thời gian', 'độc quyền',
        'cao cấp', 'đang hot', 'xu hướng', 'viral', 'phải có',
        'đánh giá cao', 'hoàn tiền', 'mua 1 tặng 1', 'combo ưu đãi', 'hàng về lại',
        'giảm 50%', 'ưu đãi đặc biệt',
        // --- Dien tu ---
        'điện thoại', 'tai nghe', 'tai nghe không dây', 'loa', 'đồng hồ thông minh',
        'đồng hồ', 'máy tính bảng', 'laptop', 'máy ảnh', 'flycam',
        'máy chiếu', 'sạc dự phòng', 'bàn phím', 'chuột máy tính', 'màn hình',
        'tivi', 'loa thanh', 'bluetooth', 'micro',
        'webcam', 'máy in', 'router', 'wifi', 'tivi thông minh',
        // --- Thoi trang ---
        'quần áo', 'giày', 'giày sneaker', 'dép sandal', 'giày boot',
        'váy', 'đầm', 'áo sơ mi', 'quần', 'quần jean',
        'áo khoác', 'áo hoodie', 'áo thun', 'đồ lót', 'tất',
        'mũ', 'mũ lưỡi trai', 'khăn quàng', 'găng tay', 'thắt lưng',
        'ví', 'túi xách', 'balo', 'vali', 'kính râm',
        'trang sức', 'dây chuyền', 'nhẫn', 'bông tai', 'vòng tay',
        'thời trang đường phố', 'thời trang thiết kế', 'đồ thể thao', 'quần legging', 'đồ bơi',
        'đồ ngủ', 'dép đi trong nhà', 'giày cao gót',
        // --- Suc khoe & The hinh ---
        'thực phẩm chức năng', 'vitamin', 'protein', 'men vi sinh', 'dầu cá',
        'omega', 'magie', 'kẽm', 'thảo dược', 'trà',
        'mật ong', 'thải độc', 'giảm cân', 'ăn kiêng', 'keto',
        'thuần chay', 'hữu cơ', 'tốt cho sức khỏe', 'thể hình', 'phòng gym',
        'yoga', 'tạ tay', 'dây kháng lực', 'máy chạy bộ', 'tập thể dục',
        'massage', 'tư thế', 'chỉnh hình', 'giảm đau', 'giấc ngủ',
        'gối', 'chăn', 'thảm yoga', 'súng massage', 'đai chỉnh tư thế',
        'máy lọc nước',
        // --- Me & Be ---
        'em bé', 'quần áo trẻ em', 'đồ chơi trẻ em', 'bỉm', 'xe đẩy',
        'bình sữa', 'đồ bầu', 'trẻ em', 'đồ ăn dặm', 'chăm sóc bé',
        'giày trẻ em', 'cặp sách', 'máy theo dõi bé', 'cũi', 'địu em bé',
        // --- Thu cung ---
        'thú cưng', 'chó', 'mèo', 'thức ăn cho chó', 'thức ăn cho mèo',
        'đồ chơi thú cưng', 'chăm sóc thú cưng', 'dây dắt chó', 'vòng cổ thú cưng', 'cát vệ sinh mèo',
        'chim', 'cá', 'giường thú cưng', 'sữa tắm thú cưng', 'bánh thưởng thú cưng',
        'bể cá',
        // --- Thuc pham ---
        'đồ ăn vặt', 'cà phê', 'sô cô la', 'gia vị', 'nước sốt',
        'mì', 'gạo', 'trái cây sấy', 'các loại hạt', 'kẹo',
        'đồ uống', 'nước ép', 'nước tăng lực', 'thanh protein', 'granola',
        'yến mạch', 'bơ đậu phộng', 'dầu oliu', 'dầu dừa', 'làm bánh',
        'thực phẩm hữu cơ', 'thực phẩm lành mạnh', 'không gluten',
        // --- Xe co ---
        'phụ kiện ô tô', 'camera hành trình', 'rửa xe', 'ghế ô tô', 'lốp xe',
        'chăm sóc xe', 'xe máy', 'xe đạp', 'mũ bảo hiểm', 'đạp xe',
        'sạc ô tô', 'giá đỡ điện thoại trên xe', 'bọc ghế xe', 'thảm lót sàn xe', 'sáp đánh bóng xe',
        // --- The thao & Ngoai troi ---
        'thể thao', 'chạy bộ', 'leo núi', 'cắm trại', 'câu cá',
        'bơi lội', 'quần vợt', 'golf', 'bóng rổ', 'bóng đá',
        'ván trượt', 'lướt sóng', 'trượt tuyết', 'tập luyện', 'thiết bị tập gym',
        'lều', 'túi ngủ', 'áo mưa', 'ô', 'thùng giữ lạnh',
        'dã ngoại', 'nướng bbq', 'bếp nướng',
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
