const express = require('express');
const app = express();

app.use(express.json());

// ===== CONFIG =====
const PORT = process.env.API_PORT || 5002;
// Dấu hiệu đặc biệt ở Header để xác thực nội bộ (Thay đổi chuỗi này theo ý bạn)
const INTERNAL_SECRET_HEADER = 'x-fb-internal-token';
const INTERNAL_SECRET_VALUE = process.env.INTERNAL_API_TOKEN || 'fb-analyzer-secret-2026';

/**
 * Middleware kiểm tra Header đặc biệt (Không can thiệp sâu vào Auth)
 */
function checkSpecialHeader(req, res, next) {
    const token = req.headers[INTERNAL_SECRET_HEADER];
    if (!token || token !== INTERNAL_SECRET_VALUE) {
        return res.status(403).json({ 
            success: false, 
            message: 'Forbidden: Invalid or missing internal system sign.' 
        });
    }
    next();
}

/**
 * Khởi tạo API Server kiếm tìm dữ liệu quảng cáo quảng cáo đã phân tích
 * @param {import('mongodb').Db} dbInstance 
 */
function initSearchServer(dbInstance) {
    const adsCol = dbInstance.collection('analyzed_ads');

    // Áp dụng middleware kiểm tra Header cho toàn bộ các route tìm kiếm
    app.use(checkSpecialHeader);

    /**
     * API GET /api/ads/search
     * Các Query Filters hỗ trợ: text, country, date_from, date_to, min_score, max_score, level
     */
    app.get('/api/ads/search', async (req, res) => {
        try {
            const { text, country, date_from, date_to, min_score, max_score, level } = req.query;
            
            const query = {};

            // 1. Filter theo Tên / Nội dung quảng cáo (Tìm kiếm tương đối không phân biệt hoa thường)
            if (text) {
               query.$text = { $search: `\"${text}\"` };
            }

            // 2. Filter theo Đất nước
            if (country) {
                // Hỗ trợ nếu trường dữ liệu của bạn lưu dạng mảng hoặc chuỗi text đơn thuần
                query.$or = [
                    { publisher_platforms: { $regex: country, $options: 'i' } }, 
                    { text: { $regex: country, $options: 'i' } } // Fallback nếu lưu nước trong text
                ];
            }

            // 3. Filter theo Khoảng ngày (Dựa trên start_date của bài Ads - lưu dạng Timestamp giây)
            if (date_from || date_to) {
                query.start_date = {};
                if (date_from) {
                    query.start_date.$gte = Math.floor(new Date(date_from).getTime() / 1000);
                }
                if (date_to) {
                    query.start_date.$lte = Math.floor(new Date(date_to).getTime() / 1000);
                }
            }

            // 4. Filter theo khoảng Điểm số (Score)
            if (min_score || max_score) {
                query.score = {};
                if (min_score) query.score.$gte = parseInt(min_score, 10);
                if (max_score) query.score.$lte = parseInt(max_score, 10);
            }

            // 5. Filter theo Sản phẩm Winning (Dựa trên nhãn '🔥 WINNER' của hệ thống chấm điểm)
            if (level) {
                // level nhận vào có thể là: '🔥 WINNER', '⚡ GOOD', 'LOW'
                query.level = level.toUpperCase().includes('WINNER') ? '🔥 WINNER' : level.toUpperCase();
            }

            // Thực thi phân trang mặc định để tránh nghẽn băng thông hệ thống (100 record/lượt)
            const page = parseInt(req.query.page, 10) || 1;
            const limit = parseInt(req.query.limit, 10) || 100;
            const skip = (page - 1) * limit;

            const total = await adsCol.countDocuments(query);
            const results = await adsCol.find(query)
                                        .sort({ score: -1, analyzed_at: -1 }) // Ưu tiên hàng Winner và mới nhất lên đầu
                                        .skip(skip)
                                        .limit(limit)
                                        .toArray();

            return res.json({
                success: true,
                pagination: {
                    total,
                    page,
                    limit,
                    pages: Math.ceil(total / limit)
                },
                data: results
            });

        } catch (error) {
            return res.status(500).json({ 
                success: false, 
                message: 'Internal Server Error', 
                error: error.message 
            });
        }
    });

    // Lắng nghe cổng kết nối
    app.listen(PORT, () => {
        console.log(`🖥️  API Search Server đang hoạt động độc lập tại cổng: http://localhost:${PORT}`);
    });
}

module.exports = { initSearchServer };