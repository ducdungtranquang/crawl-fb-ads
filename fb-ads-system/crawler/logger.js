const { Kafka } = require('kafkajs');

let logProducer = null;
let currentServiceName = 'unknown';

// Khởi tạo Logger kết nối tới Kafka
async function initLogger(serviceName, brokers = process.env.KAFKA_BROKERS ? [process.env.KAFKA_BROKERS] : ['localhost:9092']) {
    currentServiceName = serviceName;
    try {
        const kafka = new Kafka({ clientId: `logger-${serviceName}`, brokers });
        logProducer = kafka.producer();
        await logProducer.connect();
        console.log(`[Logger] Hoàn tất kết nối hệ thống log cho dịch vụ: ${serviceName}`);
    } catch (err) {
        console.error('[Logger] Không thể kết nối Kafka cho log, log sẽ chỉ in ra console:', err.message);
    }
}

async function sendLog(level, message, errorObj = null) {
    const timestamp = Date.now();
    const logPayload = {
        service: currentServiceName,
        level: level.toUpperCase(),
        message: message,
        stack: errorObj ? errorObj.stack : null,
        timestamp: timestamp
    };

    const timeStr = new Date(timestamp).toLocaleTimeString();
    if (level === 'ERROR') {
        console.error(`❌ [${timeStr}] [${level}] ${message}`, errorObj || '');
    } else {
        console.log(`💡 [${timeStr}] [${level}] ${message}`);
    }

    if (logProducer) {
        try {
            await logProducer.send({
                topic: 'system-logs',
                messages: [{ value: JSON.stringify(logPayload) }]
            });
        } catch (err) {
            console.error('[Logger Error] Thất bại khi đẩy log lên Kafka:', err.message);
        }
    }
}

module.exports = {
    initLogger,
    info: (msg) => sendLog('INFO', msg),
    warn: (msg) => sendLog('WARN', msg),
    error: (msg, err) => sendLog('ERROR', msg, err)
};