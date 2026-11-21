export type Venue = 'bitopro' | 'okx';
export type AppMode = 'demo' | 'live' | 'history';

export interface Level {
  price: string;
  amount: string;
}

export interface OrderBook {
  venue: Venue;
  pair: string;
  source?: 'websocket' | 'rest' | 'demo';
  bids: Level[];
  asks: Level[];
  exchange_ts: number | null;
  received_ts: number;
  sequence?: number | null;
  valid: boolean;
  age_ms: number;
}

export interface Opportunity {
  id: string;
  title: string;
  kind: 'cross' | 'triangle';
  ready: boolean;
  depth_ready: boolean;
  raw_pct: string;
  net_pct: string;
  size_unit: 'BTC' | 'TWD';
  reference_size: string;
  estimate_pnl: string;
  estimate_asset: string;
  depth_capacity?: string | null;
  buy_venue: Venue;
  sell_venue: Venue;
  books: string[];
  reason?: string | null;
}

export interface Balance {
  venue: Venue;
  asset: 'BTC' | 'USDT' | 'TWD' | string;
  amount: string;
}

export interface Trade {
  id: string;
  created_at: string;
  strategy_id: string;
  strategy_name: string;
  mode: AppMode;
  trigger_reason: string;
  start_asset: string;
  start_amount: string;
  end_asset: string;
  end_amount: string;
  pnl: string;
  pnl_percent: string;
  legs: Array<Record<string, unknown>>;
}

export interface Signal {
  id: string;
  observed_at: string;
  strategy_id: string;
  strategy_name: string;
  mode: AppMode;
  status: string;
  net_pct: string;
  reason: string;
}

export interface TradingSettings {
  bitopro_fee_bps: string;
  okx_fee_bps: string;
  net_threshold_bps: string;
  cooldown_seconds: number;
  max_book_age_ms: number;
  auto_simulation_enabled: boolean;
}

export interface DashboardState {
  hosting?: {
    market_transport: 'snapshot';
    poll_interval_ms: number;
    ledger_status: 'setup' | 'locked' | 'ready' | 'unavailable';
  };
  market: {
    mode: AppMode;
    generated_at: string;
    books: Record<string, OrderBook>;
    connections: Record<Venue, { connected: boolean; last_message_ts: number | null; error: string | null }>;
  };
  opportunities: Opportunity[];
  balances: Balance[];
  trades: Trade[];
  signals: Signal[];
  settings: TradingSettings;
  summary?: { trade_count: number; realized_pnl_by_asset: Record<string, string> };
}

