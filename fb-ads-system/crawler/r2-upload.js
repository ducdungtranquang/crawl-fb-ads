'use strict';
// Upload thumbnail lên Cloudflare R2 (S3-compatible).
// File này đứng cùng thư mục với crawler.js, được require từ crawler.js.
//
// CHỐNG TRÙNG (dedupe):
// - Key R2 là hash MD5 của URL ảnh: `thumbnails/<md5(url-anh)>.jpg`.
//   Ad trùng creative (cùng ảnh, khác ad_archive_id) và ad đã upload rồi
//   đều trỏ chung 1 object R2, 1 URL.
// - Trước khi tải/upload, code HEAD R2 theo key: đã có thì dùng luôn URL
//   đó, khỏi tải lại, khỏi upload lại. Check này dùng chung cho cả 2 máy
//   crawl vì cùng 1 bucket.
//
// CÁCH GẮN KEY (chọn 1 trong 2, chỉ làm TRÊN MÁY VM):
//   Cách 1: dán Access Key ID + Secret Access Key của token R2 vào 2 dòng
//     PASTE_HERE bên dưới. File đã gắn key thì KHÔNG commit, KHÔNG gửi
//     lại cho ai, chỉ nằm trên máy VM.
//   Cách 2: đặt biến môi trường R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY
//     (biến môi trường luôn thắng giá trị trong file).
// Ba giá trị không nhạy cảm (Account ID, bucket, URL công khai) đã gắn sẵn
// bên dưới; biến môi trường R2_ACCOUNT_ID / R2_BUCKET / R2_PUBLIC_BASE_URL
// (nếu có) sẽ thắng.
//
// Thiếu key/secret (còn nguyên chữ PASTE_HERE hoặc chưa đặt env) thì mọi
// hàm upload/exists trả về null/false — crawler KHÔNG crash vì R2, nhưng
// ad mới sẽ KHÔNG có thumbnail (R2-only từ 10/11/2026, không còn fallback
// ghi file local). Xem getR2Status() để biết R2 tắt vì lý do gì.

// ============================================================
//  DÁN KEY/SECRET VÀO 2 DÒNG DƯỚI (chỉ trên máy VM, không commit)
// ============================================================
const PASTED_ACCESS_KEY_ID = 'DÁN_ACCESS_KEY_ID_VÀO_ĐÂY';
const PASTED_SECRET_ACCESS_KEY = 'DÁN_SECRET_ACCESS_KEY_VÀO_ĐÂY';
// ============================================================

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

// Load crawler-local secrets from the git-ignored .env file when present.
try {
  const envFile = fs.readFileSync(path.join(__dirname, '.env'), 'utf8');
  for (const line of envFile.split(/\r?\n/)) {
    const match = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$/);
    if (match && process.env[match[1]] === undefined) process.env[match[1]] = match[2].replace(/^(['"])(.*)\1$/, '$2');
  }
} catch {}

let S3Client = null;
let PutObjectCommand = null;
let HeadObjectCommand = null;
try {
  ({ S3Client, PutObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3'));
} catch {
  console.warn('[R2] Chưa cài @aws-sdk/client-s3 — chạy: npm install @aws-sdk/client-s3');
}

const PLACEHOLDER_KEY = 'DÁN_ACCESS_KEY_ID_VÀO_ĐÂY';
const PLACEHOLDER_SECRET = 'DÁN_SECRET_ACCESS_KEY_VÀO_ĐÂY';

// Biến môi trường thắng; tiếp theo là giá trị dán trong file (trừ khi còn
// nguyên chữ PASTE_HERE thì coi như chưa gắn).
function pick(envVal, pastedVal, placeholder) {
  if (envVal) return envVal;
  if (pastedVal && pastedVal !== placeholder) return pastedVal;
  return '';
}

const ACCOUNT_ID = process.env.R2_ACCOUNT_ID || '2d776f11da5e8a6e5f2b616050653cc9';
const ACCESS_KEY_ID = pick(process.env.R2_ACCESS_KEY_ID, PASTED_ACCESS_KEY_ID, PLACEHOLDER_KEY);
const SECRET_ACCESS_KEY = pick(process.env.R2_SECRET_ACCESS_KEY, PASTED_SECRET_ACCESS_KEY, PLACEHOLDER_SECRET);
const BUCKET = process.env.R2_BUCKET || 'fb-ad-thumbnails';
const PUBLIC_BASE_URL = (process.env.R2_PUBLIC_BASE_URL || 'https://pub-0a8bd04b515a4259b608a0d2478ee892.r2.dev').replace(/\/+$/, '');

const ENABLED = Boolean(
  S3Client && PutObjectCommand && HeadObjectCommand &&
  ACCOUNT_ID && ACCESS_KEY_ID && SECRET_ACCESS_KEY && BUCKET && PUBLIC_BASE_URL
);

let s3 = null;
if (ENABLED) {
  s3 = new S3Client({
    region: 'auto',
    endpoint: `https://${ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: { accessKeyId: ACCESS_KEY_ID, secretAccessKey: SECRET_ACCESS_KEY },
  });
} else if (S3Client) {
  console.warn('[R2] Chưa gắn key/secret R2 (dán vào 2 dòng PASTE_HERE trong file hoặc đặt biến môi trường R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY) — upload R2 tắt, ad mới sẽ không có thumbnail.');
}

function isR2Enabled() {
  return ENABLED;
}

// Trạng thái R2 để crawler log rõ ngay khi khởi động (trước đây R2 tắt
// lặng lẽ trả null, ad mới mất thumbnail mà log không nói lý do — vụ
// 11/11/2026). reason: null khi bật; 'sdk-missing' khi chưa cài
// @aws-sdk/client-s3; 'keys-missing' khi thiếu key/secret;
// 'config-missing' khi thiếu cấu hình khác (account/bucket/public URL).
function getR2Status() {
  let reason = null;
  if (!S3Client || !PutObjectCommand || !HeadObjectCommand) reason = 'sdk-missing';
  else if (!ACCESS_KEY_ID || !SECRET_ACCESS_KEY) reason = 'keys-missing';
  else if (!ACCOUNT_ID || !BUCKET || !PUBLIC_BASE_URL) reason = 'config-missing';
  return { enabled: ENABLED, reason, bucket: BUCKET, publicBaseUrl: PUBLIC_BASE_URL };
}

// Key R2 theo hash của URL ảnh — ad trùng ảnh dùng chung 1 key.
function thumbnailKeyFor(imageUrl) {
  const hash = crypto.createHash('md5').update(String(imageUrl)).digest('hex');
  return `thumbnails/${hash}.jpg`;
}

function r2PublicUrl(key) {
  return `${PUBLIC_BASE_URL}/${key}`;
}

// Thumbnail đã có trên R2 chưa? false khi chưa có, khi R2 tắt, hoặc khi
// HEAD lỗi — phía gọi sẽ upload (ghi đè cùng key là an toàn, cùng nội dung).
async function r2ThumbnailExists(key) {
  if (!ENABLED || !key) return false;
  try {
    await s3.send(new HeadObjectCommand({ Bucket: BUCKET, Key: key }));
    return true;
  } catch {
    return false;
  }
}

// Upload buffer ảnh theo key đã tính, trả về URL công khai đầy đủ,
// lỗi trả về null. R2 tắt thì cảnh báo 1 lần duy nhất mỗi process
// (trước đây lặng lẽ trả null nên không ai biết R2 đang tắt).
let disabledWarned = false;
async function uploadThumbnailToR2(buffer, key) {
  if (!ENABLED || !buffer || !key) {
    if (!ENABLED && !disabledWarned) {
      disabledWarned = true;
      const st = getR2Status();
      console.warn(`[R2] Upload tắt (${st.reason}) — thumbnail sẽ là null, ad mới không có ảnh. Kiểm tra key/secret R2 hoặc cài @aws-sdk/client-s3.`);
    }
    return null;
  }
  try {
    await s3.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: key,
      Body: buffer,
      ContentType: 'image/jpeg',
      CacheControl: 'public, max-age=31536000, immutable',
    }));
    return r2PublicUrl(key);
  } catch (err) {
    console.error(`[R2] Upload thumbnail ${key} thất bại:`, err.message);
    return null;
  }
}

module.exports = { uploadThumbnailToR2, r2ThumbnailExists, thumbnailKeyFor, r2PublicUrl, isR2Enabled, getR2Status };
