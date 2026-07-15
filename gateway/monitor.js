import Redis from 'ioredis';

const redis = new Redis();
const counts = {};

// Subscribe to all book channels
redis.psubscribe('book:*', (err) => {
    if (err) console.error("Redis subscription failed:", err);
    console.log("Monitoring active Redis publications... Press Ctrl+C to stop.");
});

// Increment count per stock symbol whenever a message is published
redis.on('pmessage', (pattern, channel) => {
    const symbol = channel.split(':')[1] || 'UNKNOWN';
    counts[symbol] = (counts[symbol] || 0) + 1;
});

// Clear the screen and draw a clean table every 1 second
setInterval(() => {
    console.clear();
    console.log("=== Active Redis Publications (Event Counts) ===");
    
    const tableData = Object.entries(counts).map(([symbol, count]) => ({
        "Stock Symbol": symbol,
        "Total Events Published": count
    }));
    
    console.table(tableData);
}, 1000);