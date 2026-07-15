import { useEffect, useRef, useState } from 'react';
import { AppBar, Box, Button, MenuItem, Select, Toolbar, Typography, Paper, Table, TableBody, TableCell, TableHead, TableRow, TextField, Snackbar, Alert } from '@mui/material';

interface OrderEvent {
  seq: bigint;
  symbol: string;
  price: number;
  qty: number;
  order_id: bigint;
  ts_ns: bigint;
  side: 'B' | 'S';
  event: 'A' | 'C' | 'E';
}

interface Level {
  price: number;
  qty: number;
  count: number;
}

interface Book {
  bids: Map<number, Level>;
  asks: Map<number, Level>;
  orders: Map<bigint, OrderEvent>;
  lastSeq: bigint;
  lastPrice: number;
}

interface TradeToast {
  id: number;
  symbol: string;
  side: 'B' | 'S';
  price: number;
  qty: number;
}

// ---- theme colors ----
const NAVY_BG = '#0a1330';
const NAVY_PANEL = '#101c40';
const NAVY_APPBAR = '#0d1838';
const LIGHT_BLUE = '#29b6f6';
const ORANGE = '#ff8a3d';

// 56B binary from C++ publish_event()
function parseEvent(buf: ArrayBuffer): OrderEvent {
  const view = new DataView(buf);
  let off = 0;
  const seq = view.getBigUint64(off, true); off += 8;
  const symbol = new TextDecoder().decode(new Uint8Array(buf, off, 16)).replace(/\0/g, ''); off += 16;
  const price = view.getFloat64(off, true); off += 8;
  const qty = view.getInt32(off, true); off += 4;
  const order_id = view.getBigUint64(off, true); off += 8;
  const ts_ns = view.getBigInt64(off, true); off += 8;
  const side = String.fromCharCode(view.getUint8(off++)) as 'B' | 'S';
  const event = String.fromCharCode(view.getUint8(off++)) as 'A' | 'C' | 'E';
  return { seq, symbol, price, qty, order_id, ts_ns, side, event };
}

// 24B + N*12B binary from C++ send_snapshot()
function parseSnapshot(buf: ArrayBuffer): { symbol: string; bids: Level[]; asks: Level[] } {
  const view = new DataView(buf);
  const bid_levels = view.getUint32(0, true);
  const ask_levels = view.getUint32(4, true);
  const symbol = new TextDecoder().decode(new Uint8Array(buf, 8, 16)).replace(/\0/g, '');

  let off = 24;
  const bids: Level[] = [];
  for (let i = 0; i < bid_levels; i++) {
    const price = view.getFloat64(off, true); off += 8;
    const qty = view.getInt32(off, true); off += 4;
    const count = view.getInt32(off, true); off += 4;
    bids.push({ price, qty, count });
  }

  const asks: Level[] = [];
  for (let i = 0; i < ask_levels; i++) {
    const price = view.getFloat64(off, true); off += 8;
    const qty = view.getInt32(off, true); off += 4;
    const count = view.getInt32(off, true); off += 4;
    asks.push({ price, qty, count });
  }

  return { symbol, bids, asks };
}

const emptyBook = (): Book => ({
  bids: new Map(),
  asks: new Map(),
  orders: new Map(),
  lastSeq: 0n,
  lastPrice: 0
});

export default function App() {
  const symbols = ['RELIANCE.NS', 'TCS.NS', 'VBL.NS'];
  const [activeSymbol, setActiveSymbol] = useState('RELIANCE.NS');
  const [books, setBooks] = useState<Record<string, Book>>(
    Object.fromEntries(symbols.map(s => [s, emptyBook()]))
  );

  // --- trade notification state ---
  // NOTE: the ws payload the "add_order" command sends doesn't get an order_id
  // echoed back by the server, so we can't strictly prove a given fill belongs
  // to *this* browser tab. As a best-effort approach we track order_ids we've
  // recently seen created (event 'A') right after we placed an order on the
  // active symbol, and surface a toast when one of those ids later fills ('E').
  // If you want a 100%-accurate "your order traded" message, have the C++ side
  // echo the order_id back over the same websocket on ADD so we can pin it exactly.
  const [toasts, setToasts] = useState<TradeToast[]>([]);
  const pendingOwnOrderIds = useRef<Set<bigint>>(new Set());
  const toastIdCounter = useRef(0);

  const ws = useRef<WebSocket | null>(null);

  useEffect(() => {
    ws.current = new WebSocket('ws://localhost:3001');
    ws.current.binaryType = 'arraybuffer';

    ws.current.onopen = () => {
      symbols.forEach(s => ws.current?.send(JSON.stringify({ type: 'sub', symbol: s })));
    };

    ws.current.onmessage = e => {
      const buf: ArrayBuffer = e.data;

      // BRANCH: 56B = OrderEvent, >56B = Snapshot. Snapshots are never 56B exactly.
      if (buf.byteLength === 56) {
        const ev = parseEvent(buf);

        // Track fills against orders we believe are ours, then fire a toast.
        if (ev.event === 'E' && pendingOwnOrderIds.current.has(ev.order_id)) {
          toastIdCounter.current += 1;
          setToasts(t => [...t, {
            id: toastIdCounter.current,
            symbol: ev.symbol,
            side: ev.side,
            price: ev.price,
            qty: ev.qty
          }]);
        }
        if (ev.event === 'C' || ev.event === 'E') {
          // order fully removed once filled/cancelled to size 0 -- stop tracking
          const stillOpenQty = ev.event === 'E' ? undefined : 0;
          if (stillOpenQty === 0 || ev.event === 'C') {
            pendingOwnOrderIds.current.delete(ev.order_id);
          }
        }

        setBooks(prev => {
          const book = prev[ev.symbol] ?? emptyBook();
          if (ev.seq !== book.lastSeq + 1n && book.lastSeq !== 0n) {
            console.warn(`Gap for ${ev.symbol}: expected ${book.lastSeq + 1n}, got ${ev.seq}`);
          }
          const orders = new Map(book.orders);
          const bids = new Map(book.bids);
          const asks = new Map(book.asks);
          let lastPrice = book.lastPrice;

          if (ev.event === 'A') {
            orders.set(ev.order_id, { ...ev });
            const bookSide = ev.side === 'B' ? bids : asks;
            const lvl = bookSide.get(ev.price);
            bookSide.set(ev.price, lvl ? { ...lvl, qty: lvl.qty + ev.qty, count: lvl.count + 1 }
                                       : { price: ev.price, qty: ev.qty, count: 1 });
          }
          if (ev.event === 'C') {
            const ord = orders.get(ev.order_id);
            if (ord) {
              const bookSide = ord.side === 'B' ? bids : asks;
              const lvl = bookSide.get(ord.price);
              if (lvl) {
                const newQty = lvl.qty - ord.qty;
                const newCount = lvl.count - 1;
                if (newQty <= 0 || newCount <= 0) bookSide.delete(ord.price);
                else bookSide.set(ord.price, { ...lvl, qty: newQty, count: newCount });
              }
              orders.delete(ev.order_id);
            }
          }
          if (ev.event === 'E') {
            const ord = orders.get(ev.order_id);
            if (ord) {
              const bookSide = ord.side === 'B' ? bids : asks;
              const lvl = bookSide.get(ord.price);
              if (lvl) {
                const newQty = lvl.qty - ev.qty;
                if (newQty <= 0) bookSide.delete(ord.price);
                else bookSide.set(ord.price, { ...lvl, qty: newQty });
              }
              const remainingQty = ord.qty - ev.qty;
              if (remainingQty <= 0) orders.delete(ev.order_id);
              else orders.set(ev.order_id, { ...ord, qty: remainingQty });
              lastPrice = ev.price;
            }
          }
          return { ...prev, [ev.symbol]: { bids, asks, orders, lastSeq: ev.seq, lastPrice } };
        });
      } else {
        // Snapshot path
        const snap = parseSnapshot(buf);
        setBooks(prev => {
          const prevBook = prev[snap.symbol] ?? emptyBook();
          const bids = new Map(snap.bids.map(l => [l.price, l]));
          const asks = new Map(snap.asks.map(l => [l.price, l]));
          const lastPrice = bids.size ? Math.max(...bids.keys()) : asks.size ? Math.min(...asks.keys()) : 0;

          return {
            ...prev,
            [snap.symbol]: {
              bids,
              asks,
              orders: new Map(),
              lastSeq: prevBook.lastSeq,
              lastPrice
            }
          };
        });
      }
    };

    return () => ws.current?.close();
  }, []);

  const addOrder = (side: 'B' | 'S') => {
    const px = parseFloat((document.getElementById('px') as HTMLInputElement).value);
    const qty = parseInt((document.getElementById('qty') as HTMLInputElement).value);
    ws.current?.send(JSON.stringify({ type: 'add_order', symbol: activeSymbol, side, px, qty }));
    // Best-effort: we don't get the order_id back from this message, so exact
    // "your order" attribution for fills isn't guaranteed -- see note above.
  };

  const closeToast = (id: number) => {
    setToasts(t => t.filter(x => x.id !== id));
  };

  const book = books[activeSymbol] ?? emptyBook();
  const sortedBids = [...book.bids.values()].sort((a, b) => b.price - a.price).slice(0, 15);
  const sortedAsks = [...book.asks.values()].sort((a, b) => a.price - b.price).slice(0, 15);

  return (
    <Box sx={{ bgcolor: NAVY_BG, minHeight: '100vh', color: '#ddd' }}>
      <AppBar position="static" sx={{ bgcolor: NAVY_APPBAR, boxShadow: 'none', borderBottom: '1px solid #1c2b55' }}>
        <Toolbar>
          <Typography variant="h6" sx={{ flexGrow: 1, color: LIGHT_BLUE, fontWeight: 700 }}>
            HFT Screener
          </Typography>
          <Select
            value={activeSymbol}
            onChange={e => setActiveSymbol(e.target.value)}
            size="small"
            sx={{ mr: 2, bgcolor: NAVY_PANEL, color: '#fff' }}
          >
            {symbols.map(s => <MenuItem key={s} value={s}>{s}</MenuItem>)}
          </Select>
          <Typography variant="h6" sx={{ color: '#0f0' }}>{book.lastPrice.toFixed(2)}</Typography>
        </Toolbar>
      </AppBar>

      <Box sx={{ display: 'flex', p: 2, gap: 2 }}>
        <Paper sx={{ p: 2, bgcolor: NAVY_PANEL, flex: 1 }}>
          <Typography variant="h6" sx={{ color: '#fff' }}>Order Entry</Typography>
          <TextField
            id="px"
            label="Price"
            type="number"
            size="small"
            sx={{ mr: 1, mt: 1 }}
            defaultValue={book.lastPrice || 2520.5}
          />
          <TextField id="qty" label="Qty" type="number" size="small" sx={{ mr: 1, mt: 1 }} defaultValue={100} />
          <Box sx={{ mt: 1 }}>
            <Button
              variant="contained"
              onClick={() => addOrder('B')}
              sx={{ mr: 1, bgcolor: LIGHT_BLUE, '&:hover': { bgcolor: '#1e9ce0' } }}
            >
              Buy
            </Button>
            <Button
              variant="contained"
              onClick={() => addOrder('S')}
              sx={{ bgcolor: ORANGE, '&:hover': { bgcolor: '#e5722c' } }}
            >
              Sell
            </Button>
          </Box>
        </Paper>

        <Paper sx={{ p: 2, bgcolor: NAVY_PANEL, flex: 2 }}>
          <Typography variant="h6" sx={{ color: '#fff' }}>{activeSymbol} Order Book</Typography>
          <Box sx={{ display: 'flex', gap: 2 }}>
            <Table size="small" sx={{ flex: 1 }}>
              <TableHead>
                <TableRow>
                  <TableCell sx={{ color: LIGHT_BLUE, fontWeight: 700 }}>Bid Qty</TableCell>
                  <TableCell align="right" sx={{ color: LIGHT_BLUE, fontWeight: 700 }}>Price</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {sortedBids.map(l => (
                  <TableRow key={l.price}>
                    <TableCell sx={{ color: '#fff' }}>{l.qty}</TableCell>
                    <TableCell align="right" sx={{ color: LIGHT_BLUE }}>{l.price.toFixed(2)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            <Table size="small" sx={{ flex: 1 }}>
              <TableHead>
                <TableRow>
                  <TableCell sx={{ color: ORANGE, fontWeight: 700 }}>Price</TableCell>
                  <TableCell align="right" sx={{ color: ORANGE, fontWeight: 700 }}>Ask Qty</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {sortedAsks.map(l => (
                  <TableRow key={l.price}>
                    <TableCell sx={{ color: ORANGE }}>{l.price.toFixed(2)}</TableCell>
                    <TableCell align="right" sx={{ color: '#fff' }}>{l.qty}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </Box>
        </Paper>
      </Box>

      {/* Trade fill notifications */}
      {toasts.map((t, i) => (
        <Snackbar
          key={t.id}
          open
          autoHideDuration={4000}
          onClose={() => closeToast(t.id)}
          anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
          sx={{ bottom: `${16 + i * 60}px !important` }}
        >
          <Alert
            onClose={() => closeToast(t.id)}
            severity="success"
            sx={{
              bgcolor: t.side === 'B' ? LIGHT_BLUE : ORANGE,
              color: '#0a1330',
              fontWeight: 700
            }}
          >
            Trade executed: {t.side === 'B' ? 'Bought' : 'Sold'} {t.qty} {t.symbol} @ {t.price.toFixed(2)}
          </Alert>
        </Snackbar>
      ))}
    </Box>
  );
}


