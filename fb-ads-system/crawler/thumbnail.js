'use strict';
// Thumbnail ad: tai anh, upload len Cloudflare R2, tra ve URL cong khai.
// Tach rieng khoi crawler.js de test duoc ma khong can browser that.
//
// Luong (R2-only, tu 10/11/2026 — KHONG luu file local nua):
//   1. HEAD R2 theo key (MD5 cua URL anh): co roi thi dung luon URL, khoi
//      tai/upload lai.
//   2. Chua co thi tai anh qua page.request (timeout 5s), upload len R2.
//   3. Upload loi nhat thoi (mang) thi thu lai 1 lan sau 1s.
//   4. R2 tat (chua cai SDK / chua gan key) thi tra null ngay, KHONG tai
//      anh (truoc day van tai roi moi tra null, vua ton bang thong vua
//      lang le lam ad moi mat anh — vu 11/11/2026).
//   5. Tai anh loi (HTTP 403/404, timeout, ...) thi LOG ly do ra console
//      (them 11/11/2026 sau vu skip ~44% ad moi: truoc day tai loi tra
//      null lang le nen khong phan biet duoc voi loi upload R2).
//
// Ad cu thieu thumbnail duoc crawl lai thu lay (xem saveAdAndDetailDirectly
// trong crawler.js), nen khi R2 bat lai thi anh duoc lap dan.
const { uploadThumbnailToR2, r2ThumbnailExists, thumbnailKeyFor, r2PublicUrl, isR2Enabled, getR2Status } = require('./r2-upload');

// Rut gon URL anh de log: chi giu origin + pathname, bo query (chua token
// oe/... het han) de log gon va khong lo token ra log.
function shortUrl(u) {
    try { const x = new URL(u); return x.origin + x.pathname; } catch { return String(u).slice(0, 80); }
}

let r2DisabledWarned = false;
function warnR2DisabledOnce() {
    if (r2DisabledWarned) return;
    r2DisabledWarned = true;
    const st = getR2Status();
    console.warn(`[R2] TAT (${st.reason}) — bo qua download thumbnail, ad moi se KHONG co anh. Kiem tra key/secret R2 hoac cai @aws-sdk/client-s3.`);
}

// Tai 1 anh thumbnail len R2, tra ve { local: null, r2: <URL R2> }.
// R2 tat / tai loi / upload loi (ca sau 1 lan thu lai) -> { local: null, r2: null }.
async function downloadThumbnail(page, imageUrl, adId) {
    if (!imageUrl || !adId) return { local: null, r2: null };
    if (!isR2Enabled()) { warnR2DisabledOnce(); return { local: null, r2: null }; }
    try {
        const key = thumbnailKeyFor(imageUrl);
        if (await r2ThumbnailExists(key)) return { local: null, r2: r2PublicUrl(key) };
        const response = await page.request.get(imageUrl, { timeout: 5000 });
        if (!response.ok()) {
            console.warn(`[thumb] Ad ${adId}: tải ảnh lỗi HTTP ${response.status()} (${shortUrl(imageUrl)})`);
            return { local: null, r2: null };
        }
        const buffer = await response.body();
        let r2Url = await uploadThumbnailToR2(buffer, key);
        if (!r2Url) {
            // Upload loi nhat thoi (mang chap chon) — thu lai 1 lan sau 1s.
            await new Promise((resolve) => setTimeout(resolve, 1000));
            r2Url = await uploadThumbnailToR2(buffer, key);
        }
        if (r2Url) return { local: null, r2: r2Url };
        return { local: null, r2: null };
    } catch (err) {
        console.warn(`[thumb] Ad ${adId}: tải ảnh lỗi (${err && err.message ? err.message : 'unknown'}) (${shortUrl(imageUrl)})`);
        return { local: null, r2: null };
    }
}

// Danh sach URL anh ung vien cho thumbnail, theo thu tu uu tien:
// het anh (images) truoc, roi den anh preview cua video (video_preview_image_url).
// Ad video-only (khong co images) truoc day bi thumbnail null vi chi lay images[0].
function thumbnailCandidatesForAd(ad) {
    const out = [];
    const push = (u) => { if (u && typeof u === 'string' && !out.includes(u)) out.push(u); };
    for (const img of Array.isArray(ad?.images) ? ad.images : []) {
        push(img?.original_image_url || img?.url || (typeof img === 'string' ? img : null));
    }
    for (const v of Array.isArray(ad?.videos) ? ad.videos : []) {
        push(v?.video_preview_image_url || v?.video_preview_url || null);
    }
    return out;
}

// Ad co thumbnail R2 dung duoc khong. Chi tinh URL R2 (R2-only nen local
// luon null — ad chi co thumbnail_local thi van tinh la KHONG co anh).
function hasThumbnail(thumb) {
    return Boolean(thumb && thumb.r2);
}

// URL video tot nhat cua ad — cung thu tu uu tien nhu pickVideoUrl ben
// searchApi (hd truoc, sd sau, roi video_url). Dung lam fallback TAM cho
// thumbnail_r2 khi ad khong co anh (tai anh loi / khong co URL anh), de
// ad khong bi mat khoi DB va FE con hien duoc video. Tra '' neu ad khong
// co video.
function videoUrlForAd(ad) {
    if (!ad) return '';
    const vids = Array.isArray(ad.videos) ? ad.videos : [];
    const first = vids[0] || {};
    return first.video_hd_url || first.video_sd_url || first.video_url || ad.video_url || '';
}

// thumbnail_r2 co phai URL video tam (fallback) khong — nhan dang bang
// .mp4 (URL video fbcdn dang ...mp4?oe=...; URL anh R2 dang ...jpg).
// Dung de: ad cu co thumbnail video tam thi van duoc thu lay anh that o
// lan crawl sau (khong thi URL video het han oe se hong vinh vien).
function isVideoThumbnailUrl(url) {
    return /\.mp4(\?|$)/i.test(String(url || ''));
}

// Thu tung URL ung vien cho den khi co thumbnail.
// Chi can 1 URL het han / timeout thi thu URL ke tiep, khong bo cuoc ngay.
async function downloadThumbnailForAd(page, ad, adId) {
    const candidates = thumbnailCandidatesForAd(ad);
    if (candidates.length === 0) {
        console.warn(`[thumb] Ad ${adId}: không có URL ảnh ứng viên (không images, không video_preview_image_url)`);
    }
    for (const url of candidates) {
        const thumb = await downloadThumbnail(page, url, adId);
        if (thumb && (thumb.r2 || thumb.local)) return thumb;
    }
    return { local: null, r2: null };
}

module.exports = { downloadThumbnail, thumbnailCandidatesForAd, downloadThumbnailForAd, hasThumbnail, videoUrlForAd, isVideoThumbnailUrl, isR2Enabled, getR2Status };
