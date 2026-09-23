import { test, expect } from '@playwright/test';
import { WebSocketServer } from 'ws';

function buildOrderEvent(seq: bigint, symbol: string, price: number, qty: number, order_id: bigint, side: 'B'|'S', event: 'A'|'C'|'E') {
  const buf = new ArrayBuffer(56);
  const view = new DataView(buf);
  let off = 0;
  view.setBigUint64(off, seq, true); off+=8;
  new TextEncoder().encodeInto(symbol, new Uint8Array(buf, off, 16)); off+=16;
  view.setFloat64(off, price, true); off+=8;
  view.setInt32(off, qty, true); off+=4;
  view.setBigUint64(off, order_id, true); off+=8;
  view.setBigInt64(off, BigInt(Date.now()*1e6), true); off+=8;
  view.setUint8(off++, side.charCodeAt(0));
  view.setUint8(off++, event.charCodeAt(0));
  return Buffer.from(buf);
}

function buildSnapshot(symbol: string, bids: any[], asks: any[]) {
  const buf = new ArrayBuffer(24 + (bids.length+asks.length)*16);
  const view = new DataView(buf);
  view.setUint32(0, bids.length, true);
  view.setUint32(4, asks.length, true);
  new TextEncoder().encodeInto(symbol, new Uint8Array(buf, 8, 16));
  let off = 24;
  for(const l of bids){ view.setFloat64(off, l.price, true); off+=8; view.setInt32(off,l.qty,true); off+=4; view.setInt32(off,l.count,true); off+=4; }
  for(const l of asks){ view.setFloat64(off, l.price, true); off+=8; view.setInt32(off,l.qty,true); off+=4; view.setInt32(off,l.count,true); off+=4; }
  return Buffer.from(buf);
}

test.describe('frontend/app.tsx', () => {
  test('renders HFT Screener and order book from gateway', async ({ page }) => {
    await page.goto('/');
    await expect(page.getByText('HFT Screener')).toBeVisible();
    await page.waitForTimeout(1000);
    // gateway sends snapshot on sub, should see bid table header
    await expect(page.getByText('Bid Qty')).toBeVisible();
    await expect(page.getByText('Ask Qty')).toBeVisible();
  });

  test('symbol switch changes book title', async ({ page }) => {
    await page.goto('/');
    await page.getByRole('combobox').click();
    await page.getByRole('option', { name: 'TCS.NS' }).click();
    await expect(page.getByText('TCS.NS Order Book')).toBeVisible();
  });

  test('Buy/Sell sends add_order to gateway', async ({ page }) => {
    await page.goto('/');
    await page.locator('#px').fill('2525');
    await page.locator('#qty').fill('10');
    
    // Listen to WS outbound from browser
    await page.evaluate(() => {
      (window as any).sent = [];
      const orig = WebSocket.prototype.send;
      WebSocket.prototype.send = function(d){
        (window as any).sent.push(d);
        return orig.call(this, d as any);
      }
    });

    await page.getByRole('button', { name: 'Buy' }).click();
    await page.waitForTimeout(300);
    const sent = await page.evaluate(() => (window as any).sent.join(' '));
    expect(sent).toContain('add_order');
    expect(sent).toContain('RELIANCE.NS');
  });
});