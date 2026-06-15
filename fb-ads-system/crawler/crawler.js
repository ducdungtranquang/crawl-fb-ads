const { chromium } = require('playwright');
const { MongoClient } = require('mongodb');
const { Kafka } = require('kafkajs');
const logger = require('./logger');

// 1. Tắt hoàn toàn log cảnh báo hệ thống hiển thị ra màn hình Terminal
process.removeAllListeners('warning');
process.on('warning', (warning) => {
    if (warning.name === 'TimeoutNegativeWarning') return; // Nuốt chửng lỗi thời gian âm
    console.warn(warning.stack);
});

// 2. Ghi đè trực tiếp hàm setTimeout toàn cục trước khi bất kỳ thư viện nào kịp chạy
const originalSetTimeout = global.setTimeout;
global.setTimeout = function (callback, delay, ...args) {
    if (typeof delay === 'number' && delay < 0) {
        // Nếu phát hiện KafkaJS hay MongoDB tính toán ra số âm (-56 năm), ép nó về chờ 1 giây
        return originalSetTimeout(callback, 1000, ...args);
    }
    return originalSetTimeout(callback, delay, ...args);
};

// 3. Ép Node.js chạy theo giờ UTC đồng bộ với nhân Linux của Docker
process.env.TZ = 'UTC';
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';

// ===== CONFIG =====
const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2';
const DB_NAME = 'fb_ads';

const USER_DATA_DIR = './chrome-profile';
const CHROME_PATH = 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe';
const PROFILE = 'Default';

const KAFKA_BROKERS = process.env.KAFKA_BROKERS ? [process.env.KAFKA_BROKERS] : ['localhost:9092'];
const KAFKA_TOPIC = 'fb-ads-events';

// ===== LOGIC VARIABLES =====
const SEEN_THRESHOLD = 10 * 60 * 1000;
let db;
const seen = new Set();
let lastSavedAt = Date.now();
let isKafkaConnected = false;

// ===== KAFKA INITIALIZATION =====
const kafka = new Kafka({ clientId: 'fb-crawler-service', brokers: KAFKA_BROKERS });
const producer = kafka.producer();

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

// ===== CONNECT DB & KAFKA =====
async function initializeInfrastructure() {
    const client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db(DB_NAME);
    console.log('✅ Crawler MongoDB connected');

    // 2. Kết nối Kafka Producer & Tự động tạo Topic nếu chưa có
    try {
        await producer.connect();
        isKafkaConnected = true;
        console.log('🚀 Kafka Producer connected');

        console.log('💼 Đang kiểm tra hệ thống Kafka Topics...');
        const admin = kafka.admin(); // Sử dụng thực thể kafka đã khai báo ở đầu file của bạn
        await admin.connect();

        // Định nghĩa các tên Topic hệ thống của bạn (sửa lại cho đúng với code của bạn nếu cần)
        const targetTopics = [
            'fb-ads-events',
            'system-logs',
            'fb-crawler-logs',
            'fb-analyzer-logs',
            'logger-topic'
        ];

        // Lấy danh sách các topic hiện tại đang có trên Kafka Broker
        const existingTopics = await admin.listTopics();

        // Lọc ra những topic nào chưa tồn tại để tiến hành tạo mới
        const topicsToCreate = targetTopics
            .filter(topic => !existingTopics.includes(topic))
            .map(topic => ({
                topic,
                numPartitions: 1,     // Số lượng phân vùng
                replicationFactor: 1  // Hệ số sao lưu (Docker đơn lẻ để là 1)
            }));

        if (topicsToCreate.length > 0) {
            await admin.createTopics({ topics: topicsToCreate });
            console.log(`✨ Đã khởi tạo thành công các Topic mới: ${topicsToCreate.map(t => t.topic).join(', ')}`);
        } else {
            console.log('✅ Các Kafka Topics hệ thống đã tồn tại sẵn, bỏ qua bước tạo mới.');
        }

        await admin.disconnect();

    } catch (err) {
        console.error('❌ Thất bại khi cấu hình hạ tầng Kafka:', err.message);
    }
}

// ===== SAVE AD & SEND TO KAFKA =====
async function saveAdAndPublish(ad) {
    const col = db.collection('ads');
    const now = Date.now();
    const domain = extractDomain(ad.link);
    let eventType = 'AD_CREATED';

    const existing = await col.findOne({ ad_archive_id: ad.ad_archive_id });
    let updatedAd = null;

    if (!existing) {
        updatedAd = {
            ...ad,
            domain,
            first_seen: now,
            last_seen: now,
            seen_count: 1,
            growth_history: [{ t: now, c: 1 }]
        };
        await col.insertOne(updatedAd);
    } else {
        eventType = 'AD_UPDATED';
        const shouldIncrease = now - (existing.last_seen || 0) > SEEN_THRESHOLD;
        const newCount = shouldIncrease ? (existing.seen_count || 0) + 1 : existing.seen_count;

        updatedAd = {
            ...ad,
            domain,
            last_seen: now,
            seen_count: newCount,
            growth_history: [...(existing.growth_history || [])]
        };
        if (shouldIncrease) {
            updatedAd.growth_history.push({ t: now, c: newCount });
        }

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

    // Phát sự kiện sang Kafka cho Analyzer tính toán điểm số
    if (isKafkaConnected) {
        try {
            await producer.send({
                topic: KAFKA_TOPIC,
                messages: [
                    {
                        key: ad.ad_archive_id.toString(),
                        value: JSON.stringify({
                            event_type: eventType,
                            data: updatedAd,
                            timestamp: now
                        })
                    }
                ]
            });
        } catch (err) {
            logger.error('❌ Thất bại khi gửi event lên Kafka:', err.message);
        }
    }
    else {
        console.log(`⚠️ Lưu DB local xong, bỏ qua Kafka do không có kết nối cho Ad: ${ad.ad_archive_id}`);
    }
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
                    await saveAdAndPublish(ad);
                    lastSavedAt = Date.now();
                    logger.info(`W${id} ✔ Saved & Published:`, ad.ad_archive_id);
                }
            }
        } catch { }
    });

    for (const keyword of keywords) {
        logger.info(`W${id} 🔍 Scanning:`, keyword);
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
            await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
            await page.waitForTimeout(randomDelay(3000, 6000));

            try { await page.click('text=OK', { timeout: 3000 }); } catch { }

            let prev = 0;
            let same = 0;

            while (true) {
                if (Date.now() - lastSavedAt > 200000) {
                    logger.info(`⏱️ W${id} NO DATA >200s → skip keyword`);
                    break;
                }

                await page.mouse.move(randomInt(0, 800), randomInt(0, 600), { steps: randomInt(10, 25) });
                await page.evaluate(() => {
                    const direction = Math.random() > 0.3 ? 1 : -1;
                    const amount = Math.random() * 800 + 200;
                    window.scrollBy(0, direction * amount);
                });

                await page.waitForTimeout(randomDelay());
                const curr = await page.evaluate(() => document.body.scrollHeight);

                if (curr === prev) same++;
                else same = 0;

                prev = curr;
            }
        } catch (e) {
            logger.error(`W${id} ❌ Error context:`, keyword);
        }

        await page.close();
        await new Promise(r => setTimeout(r, randomDelay(3000, 6000)));
    }
}

// ===== SPLIT =====
function chunkArray(arr, n) {
    const result = Array.from({ length: n }, () => []);
    arr.forEach((item, i) => { result[i % n].push(item); });
    return result;
}

// ===== MAIN =====
const isDocker = process.env.MONGO_URI ? true : false;
(async () => {
    await initializeInfrastructure();
    await logger.initLogger('crawler-service'); // Khởi tạo logger cho Crawler

    logger.info('Hệ thống cào dữ liệu bắt đầu khởi động Chrome...');
    const context = await chromium.launchPersistentContext(USER_DATA_DIR, {
        headless: isDocker, // Nếu chạy trong Docker thì bắt buộc phải ẩn giao diện (headless: true)
        ...(isDocker ? {} : { executablePath: CHROME_PATH }), // Nếu không phải Docker (chạy ngoài máy thật Windows) thì mới dùng đường dẫn CHROME_PATH
        args: [
            `--profile-directory=${PROFILE}`,
            '--start-maximized',
            '--disable-blink-features=AutomationControlled',
            ...(isDocker ? ['--no-sandbox', '--disable-setuid-sandbox'] : []) // Bổ sung các cờ này để Chrome chạy được mượt mà trong quyền Root của Docker
        ]
    });

    logger.info('🔥 Using REAL Chrome profile');

    const keywords = [
        // 'du lịch',
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
        'dropshipping',
        'kiếm tiền online',
        'freelance',
        'giáo dục',
        'khóa học online',
        'shopee', 'shoping', 'ecommerce', 'mua sắm', 'thương mại điện tử', 'tiktok shop',
        'quần áo', 'thời trang', 'giày dép', 'túi xách', 'đồng hồ', 'phụ kiện',
        'trà sữa', 'cafe', 'ăn vặt', 'nhà hàng'

    ];

    const chunks = chunkArray(keywords, 2);

    for (let i = 0; i < chunks.length; i++) {
        await worker(context, chunks[i], i + 1);
    }

    logger.info('✅ ALL KEYWORDS DONE');
})();