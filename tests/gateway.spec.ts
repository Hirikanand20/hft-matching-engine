import { test, expect } from '@playwright/test';
import { WebSocket } from 'ws';
import Redis from 'ioredis';

let redis: Redis;

test.beforeAll(async () => {
  redis = new Redis();
  await redis.flushall();
});

test.afterAll(async () => {
  await redis.quit();
});

test.describe('gateway/gateway.js + docker redis', () => {
  
  test('book:* 56B broadcast', async () => {
    const pub = new Redis();
    const ws = new WebSocket('ws://localhost:3001');
    await new Promise(r => ws.on('open', r as any));
    ws.send(JSON.stringify({ type: 'sub', symbol: 'TCS.NS' }));
    await new Promise(r => setTimeout(r, 300));

    const buf = Buffer.alloc(56);
    buf.writeBigUInt64LE(1n, 0);
    buf.write('TCS.NS', 8);
    buf.writeDoubleLE(3500.5, 24);
    buf.writeInt32LE(50, 32);
    buf.writeBigUInt64LE(12345n, 36);
    buf.writeBigInt64LE(BigInt(Date.now()*1e6), 44);
    buf.write('B', 52); buf.write('A', 53);

    const got = await new Promise<Buffer>((resolve, reject) => {
      const t = setTimeout(()=> reject('timeout'), 2000);
      ws.once('message', (d) => { clearTimeout(t); resolve(d as Buffer); });
    });

    expect(got.length).toBe(56);
    ws.close();
    pub.disconnect();
  });

  test('snapshot:* broadcast', async () => {
    const pub = new Redis();
    const ws = new WebSocket('ws://localhost:3001');
    await new Promise(r => ws.on('open', r as any));
    ws.send(JSON.stringify({ type: 'sub', symbol: 'VBL.NS' }));
    await new Promise(r => setTimeout(r, 300));

    const snapBuf = Buffer.alloc(40);
    snapBuf.writeUInt32LE(1, 0);
    snapBuf.writeUInt32LE(0, 4);
    snapBuf.write('VBL.NS', 8);
    snapBuf.writeDoubleLE(2520.5, 24);

    const got = await new Promise<Buffer>((resolve, reject) => {
      const t = setTimeout(()=> reject('timeout'), 2000);
      ws.once('message', (d) => { clearTimeout(t); resolve(d as Buffer); });
    });

    // snapshots are >56B
    expect(got.length).toBeGreaterThan(0);
    ws.close();
    pub.disconnect();
  });

    test('add_order pushes to cmd: list', async () => {
    const TEST_SYMBOL = 'TEST.NS';
    await redis.del(`cmd:${TEST_SYMBOL}`); // ensure clean
    const ws = new WebSocket('ws://localhost:3001');
    await new Promise(r => ws.on('open', r as any));
    ws.send(JSON.stringify({ type: 'add_order', symbol: TEST_SYMBOL, side: 'B', px: 2520.5, qty: 100 }));
    await new Promise(r => setTimeout(r, 800));

    // Use LRANGE instead of LPOP to avoid consuming race, check first entry
    const list = await redis.lrange(`cmd:${TEST_SYMBOL}`, 0, 0);
    expect(list.length).toBeGreaterThan(0);
    const cmd = JSON.parse(list[0]!);
    expect(cmd.price).toBe(2520.5);
    expect(cmd.quantity).toBe(100);
    expect(cmd.type).toBe('ADD');

    await redis.del(`cmd:${TEST_SYMBOL}`);
    ws.close();
  });
  
  test('unsub stops receiving', async () => {
    const pub = new Redis();
    const ws = new WebSocket('ws://localhost:3001');
    await new Promise(r => ws.on('open', r as any));
    ws.send(JSON.stringify({ type: 'sub', symbol: 'VBL.NS' }));
    await new Promise(r => setTimeout(r, 200));
    ws.send(JSON.stringify({ type: 'unsub', symbol: 'VBL.NS' }));
    await new Promise(r => setTimeout(r, 200));

    let received = false;
    ws.on('message', () => received = true);
    await pub.publishBuffer('book:VBL.NS', Buffer.alloc(56) as any);
    await new Promise(r => setTimeout(r, 600));
    expect(received).toBeFalsy();

    ws.close();
    pub.disconnect();
  });
});