import express from 'express';
import { WebSocketServer } from 'ws';
import Redis from 'ioredis';

const app = express();
const server = app.listen(3001);
const wss = new WebSocketServer({ server });

// 2 Redis clients: one for sub, one for publishing/commands
const redisSub = new Redis();
const redisPub = new Redis();

const clients = new Map(); // ws -> { symbols: Set<string>, slowCount: number }

// Per-symbol sequence tracking for gap detection on server side
const lastSeq = new Map(); // symbol -> BigInt

// Subscribe to all book updates + snapshots
redisSub.psubscribe('book:*');
redisSub.psubscribe('snapshot:*');

// FIX: Changed 'pmessage' to 'pmessageBuffer' to receive the payload as a Node.js Buffer
redisSub.on('pmessageBuffer', (pattern, channelBuffer, messageBuffer) => {
    // Because channel is received as a Buffer now, convert it to a string for parsing names
    const channel = channelBuffer.toString();
    const parts = channel.split(':');
    const type = parts[0]; // 'book' or 'snapshot'
    const symbol = parts[1];

    if (type === 'book') {
        try {
            // FIX: Safely slice out the backing ArrayBuffer from the Node.js Buffer instance
            const arrayBuffer = messageBuffer.buffer.slice(
                messageBuffer.byteOffset,
                messageBuffer.byteOffset + messageBuffer.byteLength
            );
            
            const view = new DataView(arrayBuffer);
            const seq = view.getBigUint64(0, true);
            const prev = lastSeq.get(symbol) || 0n;
            
            if (prev !== 0n && seq !== prev + 1n) {
                console.warn(`Gap on ${symbol}: expected ${prev + 1n}, got ${seq}`);
            }
            lastSeq.set(symbol, seq);
        } catch (err) {
            console.error(`Error processing binary message for ${symbol}:`, err);
            return;
        }
    }

    // Broadcast to subscribed clients with backpressure
    wss.clients.forEach(ws => {
        const clientData = clients.get(ws);
        if (!clientData || !clientData.symbols.has(symbol)) return;

        // Backpressure: if >1MB queued, skip sends and count
        if (ws.bufferedAmount > 1024 * 1024) {
            clientData.slowCount++;
            // Kill if >5MB queued or 100+ skipped sends
            if (ws.bufferedAmount > 5 * 1024 * 1024 || clientData.slowCount > 100) {
                console.log(`Killing slow client, buffered=${ws.bufferedAmount}`);
                ws.close();
            }
            return;
        }
        clientData.slowCount = 0;
        
        // WebSocket can natively send Node.js Buffer objects straight to the browser
        ws.send(messageBuffer); 
    });
});

wss.on('connection', ws => {
    clients.set(ws, { symbols: new Set(), slowCount: 0 });

    ws.on('message', msg => {
        let data;
        try {
            data = JSON.parse(msg);
        } catch (e) {
            console.error('Bad JSON:', e);
            return;
        }

        const clientData = clients.get(ws);
        if (!clientData) return;

        if (data.type === 'sub') {
            clientData.symbols.add(data.symbol);
            // Request snapshot from C++ so client doesn't start blank
            redisPub.publish(`snapshot_req:${data.symbol}`, '');
        }

        if (data.type === 'unsub') {
            clientData.symbols.delete(data.symbol);
        }

        if (data.type === 'add_order') {
            // Push to Redis list consumed by C++ BLPOP thread
            const cmd = JSON.stringify({
                type: 'ADD',
                order_type: data.side === 'B' ? 0 : 1,
                quantity: data.qty,
                price: data.px,
                ts_ns: Date.now() * 1000000
            });
            redisPub.lpush(`cmd:${data.symbol}`, cmd).catch(err => {
                console.error('LPUSH failed:', err);
                ws.send(JSON.stringify({ type: 'error', msg: 'Order rejected' }));
            });
        }
    });

    ws.on('close', () => clients.delete(ws));
    ws.on('error', () => clients.delete(ws));
});

console.log('WS gateway on :3001');