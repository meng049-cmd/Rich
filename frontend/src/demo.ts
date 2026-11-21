import type { DashboardState, Level } from './types';

const now = Date.now();
const book = (venue: 'bitopro' | 'okx', pair: string, mid: number, step: number, size: number) => {
  const levels = (side: 'bid' | 'ask'): Level[] => Array.from({ length: 12 }, (_, i) => ({
    price: (side === 'bid' ? mid - step * (i + 1) : mid + step * (i + 1)).toFixed(pair.includes('TWD') ? 0 : 2),
    amount: (size + i * size * 0.16).toFixed(4),
  }));
  return { venue, pair, source: 'demo', bids: levels('bid'), asks: levels('ask'), exchange_ts: now, received_ts: now, valid: true, age_ms: 0 };
};

const books = {
  'bitopro:BTC_USDT': book('bitopro', 'BTC_USDT', 68120, 8, 0.04),
  'okx:BTC-USDT': book('okx', 'BTC-USDT', 67860, 7, 0.05),
  'bitopro:BTC_TWD': book('bitopro', 'BTC_TWD', 2180500, 250, 0.03),
  'bitopro:USDT_TWD': book('bitopro', 'USDT_TWD', 32.02, 0.01, 5500),
};

export const demoState: DashboardState = {
  market: {
    mode: 'demo',
    generated_at: new Date(now).toISOString(),
    books,
    connections: {
      bitopro: { connected: true, last_message_ts: now, error: null },
      okx: { connected: true, last_message_ts: now, error: null },
    },
  },
  opportunities: [
    { id: 'cross_bito_buy_okx_sell', title: '幣託買入 → OKX 賣出', kind: 'cross', ready: true, depth_ready: true, raw_pct: '-0.38', net_pct: '-0.58', size_unit: 'BTC', reference_size: '0.001', estimate_pnl: '-0.39', estimate_asset: 'USDT', depth_capacity: '1.2', buy_venue: 'bitopro', sell_venue: 'okx', books: ['bitopro:BTC_USDT', 'okx:BTC-USDT'] },
    { id: 'cross_okx_buy_bito_sell', title: 'OKX 買入 → 幣託賣出', kind: 'cross', ready: true, depth_ready: true, raw_pct: '0.37', net_pct: '0.17', size_unit: 'BTC', reference_size: '0.001', estimate_pnl: '0.12', estimate_asset: 'USDT', depth_capacity: '1.2', buy_venue: 'okx', sell_venue: 'bitopro', books: ['okx:BTC-USDT', 'bitopro:BTC_USDT'] },
    { id: 'triangle_twd_btc_usdt_twd', title: 'TWD → BTC → USDT → TWD', kind: 'triangle', ready: true, depth_ready: true, raw_pct: '-0.14', net_pct: '-0.44', size_unit: 'TWD', reference_size: '10000', estimate_pnl: '-44', estimate_asset: 'TWD', buy_venue: 'bitopro', sell_venue: 'bitopro', books: ['bitopro:BTC_TWD', 'bitopro:BTC_USDT', 'bitopro:USDT_TWD'] },
    { id: 'triangle_twd_usdt_btc_twd', title: 'TWD → USDT → BTC → TWD', kind: 'triangle', ready: true, depth_ready: true, raw_pct: '0.08', net_pct: '-0.22', size_unit: 'TWD', reference_size: '10000', estimate_pnl: '-22', estimate_asset: 'TWD', buy_venue: 'bitopro', sell_venue: 'bitopro', books: ['bitopro:USDT_TWD', 'bitopro:BTC_USDT', 'bitopro:BTC_TWD'] },
  ],
  balances: [
    { venue: 'bitopro', asset: 'TWD', amount: '500000' }, { venue: 'bitopro', asset: 'BTC', amount: '0.05' }, { venue: 'bitopro', asset: 'USDT', amount: '30000' },
    { venue: 'okx', asset: 'BTC', amount: '0.05' }, { venue: 'okx', asset: 'USDT', amount: '30000' },
  ],
  trades: [],
  signals: [],
  settings: { bitopro_fee_bps: '10', okx_fee_bps: '10', net_threshold_bps: '30', cooldown_seconds: 60, max_book_age_ms: 5000, auto_simulation_enabled: false },
  summary: { trade_count: 0, realized_pnl_by_asset: {} },
};

