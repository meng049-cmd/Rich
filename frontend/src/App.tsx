import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Activity, ArrowDownUp, ArrowLeftRight, Bell, BookOpen, Check, ChevronDown,
  CircleHelp, Clock3, Gauge, History, KeyRound, LayoutDashboard, Menu,
  Play, RefreshCw, Settings2, ShieldCheck, SlidersHorizontal,
  WalletCards, X,
} from 'lucide-react';
import { demoState } from './demo';
import type { AppMode, DashboardState, Opportunity, OrderBook, TradingSettings, Trade, Signal, Venue } from './types';

type View = 'overview' | 'signals' | 'ledger' | 'depth';
const apiBase = import.meta.env.VITE_API_BASE ?? '';
const modeLabel: Record<AppMode, string> = { demo: '示範', live: '即時', history: '歷史' };

async function requestApi<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${apiBase}/api/${path}`, { cache: 'no-store', ...options });
  if (!response.headers.get('content-type')?.includes('application/json')) {
    throw new Error('Rich 後端暫無回應');
  }
  const result = await response.json();
  if (!response.ok) {
    throw new Error(typeof result.detail === 'string' ? result.detail : '無法完成操作，請檢查輸入');
  }
  return result as T;
}

function money(value: number | string, decimals = 2) {
  const n = Number(value);
  if (!Number.isFinite(n)) return '—';
  return new Intl.NumberFormat('zh-TW', { minimumFractionDigits: decimals, maximumFractionDigits: decimals }).format(n);
}

function venueLabel(venue: Venue) { return venue === 'bitopro' ? 'BitoPro' : 'OKX'; }
function pnlClass(value: string | number) { return Number(value) >= 0 ? 'positive' : 'negative'; }

function getBook(state: DashboardState, id?: string): OrderBook | undefined {
  return id ? state.market.books[id] : undefined;
}

function App() {
  const [state, setState] = useState<DashboardState>(demoState);
  const [apiOnline, setApiOnline] = useState(false);
  const [view, setView] = useState<View>('overview');
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [balancesOpen, setBalancesOpen] = useState(false);
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [toast, setToast] = useState('');
  const [busyTrade, setBusyTrade] = useState<string | null>(null);
  const [size, setSize] = useState<Record<string, string>>({});
  const [formSettings, setFormSettings] = useState<TradingSettings>(demoState.settings);
  const [savingSettings, setSavingSettings] = useState(false);
  const [balanceDraft, setBalanceDraft] = useState<Record<string, string>>({});
  const [savingBalances, setSavingBalances] = useState(false);
  const [signInOpen, setSignInOpen] = useState(false);
  const [accessKey, setAccessKey] = useState('');
  const [signingIn, setSigningIn] = useState(false);
  const [hostedMode, setHostedMode] = useState<AppMode>('live');
  const [feedAge, setFeedAge] = useState(0);
  const lastUpdate = useRef(performance.now());
  const lastHosting = useRef<DashboardState['hosting']>(undefined);

  const receiveState = useCallback((incoming: DashboardState) => {
    lastHosting.current = incoming.hosting;
    lastUpdate.current = performance.now();
    setFeedAge(0);
    setState(incoming);
    setApiOnline(true);
  }, []);

  const ledgerStatus = state.hosting?.ledger_status;
  const ledgerReady = apiOnline && (!state.hosting || (ledgerStatus === 'ready' && state.market.mode !== 'demo'));
  const ledgerMessage = ledgerStatus === 'setup' ? 'Rich 的 PostgreSQL 帳本尚未連接。'
    : ledgerStatus === 'unavailable' ? 'PostgreSQL 暫時無法連線。'
    : ledgerStatus === 'locked' ? '登入即可查看帳本與執行模擬。'
    : state.hosting && state.market.mode === 'demo' ? '切換到即時或歷史模式即可開啟帳本。' : '等待 Rich 後端連線。';

  function openSettings() {
    if (!ledgerReady) {
      if (ledgerStatus === 'locked') setSignInOpen(true);
      else setToast(ledgerMessage);
      return;
    }
    setFormSettings(state.settings);
    setSettingsOpen(true);
  }

  function openBalanceEditor() {
    if (!ledgerReady) {
      if (ledgerStatus === 'locked') setSignInOpen(true);
      else setToast(ledgerMessage);
      return;
    }
    setBalanceDraft(Object.fromEntries(state.balances.map(item => [`${item.venue}:${item.asset}`, item.amount])));
    setBalancesOpen(true);
  }

  const refreshState = useCallback(async () => {
    const next = await requestApi<DashboardState>(`state?mode=${hostedMode}`);
    receiveState(next);
  }, [hostedMode, receiveState]);

  useEffect(() => {
    let disposed = false;
    let socket: WebSocket | null = null;
    let timer: number | undefined;
    let pending = false;
    let controller: AbortController | undefined;
    const load = async () => {
      if (disposed || pending || document.visibilityState === 'hidden') return;
      window.clearTimeout(timer);
      pending = true;
      controller = new AbortController();
      const current = lastHosting.current;
      try {
        let next: DashboardState;
        if (current?.ledger_status === 'ready' && hostedMode === 'live') {
          try {
            next = await requestApi<DashboardState>('monitor', { method: 'POST', signal: controller.signal });
          } catch {
            next = await requestApi<DashboardState>('state?mode=live', { signal: controller.signal });
          }
        } else {
          next = await requestApi<DashboardState>(`state?mode=${hostedMode}`, { signal: controller.signal });
        }
        if (!disposed) {
          receiveState(next);
          if (!next.hosting && !socket) openSocket();
        }
      } catch {
        if (!disposed) setApiOnline(false);
      } finally {
        pending = false;
        if (!disposed && document.visibilityState === 'visible') {
          timer = window.setTimeout(load, lastHosting.current?.poll_interval_ms ?? 7000);
        }
      }
    };
    const openSocket = () => {
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const host = apiBase ? new URL(apiBase).host : window.location.host;
      socket = new WebSocket(`${protocol}//${host}/ws`);
      socket.onmessage = event => {
        if (!disposed && document.visibilityState === 'visible') {
          try { receiveState(JSON.parse(event.data) as DashboardState); } catch { /* Ignore a malformed feed frame. */ }
        }
      };
      socket.onclose = () => { socket = null; };
      socket.onerror = () => socket?.close();
    };
    void load();
    const onVisibilityChange = () => { void load(); };
    document.addEventListener('visibilitychange', onVisibilityChange);
    return () => {
      disposed = true;
      window.clearTimeout(timer);
      document.removeEventListener('visibilitychange', onVisibilityChange);
      controller?.abort();
      socket?.close();
    };
  }, [hostedMode, receiveState]);

  useEffect(() => {
    const timer = window.setInterval(() => setFeedAge(performance.now() - lastUpdate.current), 1000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    if (!toast) return;
    const timeout = window.setTimeout(() => setToast(''), 3200);
    return () => window.clearTimeout(timeout);
  }, [toast]);

  const latestPrice = useMemo(() => {
    const book = getBook(state, 'okx:BTC-USDT');
    if (!book?.bids[0] || !book?.asks[0]) return null;
    return (Number(book.bids[0].price) + Number(book.asks[0].price)) / 2;
  }, [state]);

  const staleCount = useMemo(() => Object.values(state.market.books).filter(book => !book.valid || book.age_ms + feedAge > state.settings.max_book_age_ms).length, [state, feedAge]);
  const noCurrentMarket = state.market.mode === 'history' || Object.keys(state.market.books).length === 0;
  const missingMarket = state.market.mode === 'live' && Object.keys(state.market.books).length < 4;
  const feedWarning = noCurrentMarket || staleCount > 0 || missingMarket || !apiOnline;
  const totalTrades = state.summary?.trade_count ?? state.trades.length;
  const realizedPnlByAsset = state.summary?.realized_pnl_by_asset ?? {};
  const pnlEntries = Object.entries(realizedPnlByAsset);
  const primaryPnlEntry = pnlEntries.find(([asset]) => asset === 'USDT') ?? pnlEntries[0];
  const realizedPnl = Number(primaryPnlEntry?.[1] ?? 0);
  const otherPnlAssets = pnlEntries.filter(([asset]) => asset !== primaryPnlEntry?.[0]).map(([asset, amount]) => `${asset} ${money(amount, 2)}`).join(' · ');

  async function changeMode(mode: AppMode) {
    if (!apiOnline) {
      setToast('等待 Rich 後端連線');
      return;
    }
    if (state.hosting) {
      setHostedMode(mode);
      return;
    }
    try {
      await requestApi('mode', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ mode }) });
      await refreshState();
      setToast(`已切換至${modeLabel[mode]}資料模式`);
    } catch { setToast('資料模式切換失敗，請確認後端服務狀態'); }
  }

  async function simulate(item: Opportunity) {
    if (!ledgerReady) { setToast(ledgerMessage); return; }
    const amount = size[item.id] || item.reference_size;
    setBusyTrade(item.id);
    try {
      const result = await requestApi<{ pnl: string; end_asset: string }>('simulations', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ strategy_id: item.id, size: amount }),
      });
      setToast(`模擬完成：${money(result.pnl, 4)} ${result.end_asset}`);
      await refreshState().catch(() => setApiOnline(false));
    } catch (error) {
      setToast(error instanceof Error ? error.message : '模擬成交失敗');
    } finally { setBusyTrade(null); }
  }

  async function saveSettings() {
    if (!ledgerReady) { setToast(ledgerMessage); return; }
    setSavingSettings(true);
    try {
      await requestApi('settings', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(formSettings) });
      setSettingsOpen(false);
      setToast('監控與模擬條件已儲存');
      await refreshState().catch(() => setApiOnline(false));
    } catch (error) { setToast(error instanceof Error ? error.message : '設定儲存失敗'); }
    finally { setSavingSettings(false); }
  }

  async function saveBalances() {
    if (!ledgerReady) { setToast(ledgerMessage); return; }
    setSavingBalances(true);
    try {
      const balances = Object.entries(balanceDraft).map(([key, amount]) => {
        const [venue, asset] = key.split(':');
        return { venue, asset, amount };
      });
      await requestApi('balances', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ balances }) });
      setBalancesOpen(false);
      setToast('虛擬資產已更新；既有帳本紀錄保留');
      await refreshState().catch(() => setApiOnline(false));
    } catch (error) { setToast(error instanceof Error ? error.message : '虛擬資產儲存失敗'); }
    finally { setSavingBalances(false); }
  }

  async function signIn() {
    setSigningIn(true);
    try {
      await requestApi('session', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ key: accessKey.trim() }) });
      await refreshState();
      setSignInOpen(false);
      setAccessKey('');
    } catch (error) {
      setToast(error instanceof Error ? error.message : '無法登入模擬帳本');
    } finally { setSigningIn(false); }
  }

  async function signOut() {
    try {
      await requestApi('session', { method: 'DELETE' });
      await refreshState();
    } catch (error) { setToast(error instanceof Error ? error.message : '無法登出模擬帳本'); }
  }

  const nav = [
    { id: 'overview' as View, label: '總覽', icon: LayoutDashboard },
    { id: 'signals' as View, label: '套利訊號', icon: Activity, count: state.opportunities.length },
    { id: 'ledger' as View, label: '模擬帳本', icon: History, count: totalTrades || undefined },
    { id: 'depth' as View, label: '委託簿深度', icon: BookOpen },
  ];

  return (
    <div className="app-shell">
      <aside className={`sidebar ${mobileNavOpen ? 'mobile-open' : ''}`}>
        <div className="brand-lockup">
          <div className="brand-mark"><ArrowLeftRight size={19} /></div>
          <div><strong>ARBITRAGE</strong><span>DESK · PAPER</span></div>
          <button className="icon-button close-mobile" aria-label="關閉選單" onClick={() => setMobileNavOpen(false)}><X size={18} /></button>
        </div>
        <div className="workspace-switcher">
          <div className="workspace-avatar">AD</div>
          <div><strong>個人模擬工作台</strong><span>單人虛擬帳戶</span></div>
          <ChevronDown size={15} className="muted-icon" />
        </div>
        <div className="nav-caption">工作區</div>
        <nav className="main-nav" aria-label="主選單">
          {nav.map(item => <button key={item.id} className={`nav-item ${view === item.id ? 'active' : ''}`} onClick={() => { setView(item.id); setMobileNavOpen(false); }}>
            <item.icon size={17} strokeWidth={1.8} /><span>{item.label}</span>{item.count ? <small>{item.count}</small> : null}
          </button>)}
        </nav>
        <div className="nav-caption market-caption">監控市場</div>
        <div className="market-nav-item"><span className="coin-icon btc">₿</span><span>BTC / USDT</span><span className="market-live-dot" /></div>
        <div className="market-nav-item"><span className="coin-icon twd">$</span><span>BTC / TWD</span><span className="market-live-dot" /></div>
        <div className="sidebar-bottom">
          <div className="safe-box"><ShieldCheck size={17} /><div><strong>僅限模擬交易</strong><span>不會送出真實訂單</span></div></div>
          <button className="nav-item" onClick={openSettings}><Settings2 size={17} /><span>設定</span></button>
          {ledgerStatus === 'ready' && <button className="nav-item" onClick={() => void signOut()}><KeyRound size={16} /><span>登出模擬帳本</span></button>}
          <div className="profile-row"><div className="profile-avatar">L</div><div><strong>{state.hosting ? 'Rich 工作台' : '本機操作者'}</strong><span>{state.hosting ? 'Vercel 行情與模擬' : '資料保存在本機資料庫'}</span></div><CircleHelp size={16} className="muted-icon" /></div>
        </div>
      </aside>

      {mobileNavOpen && <button className="mobile-scrim" aria-label="關閉導覽" onClick={() => setMobileNavOpen(false)} />}

      <main className="main-content">
        <header className="topbar">
          <div className="topbar-left">
            <button className="icon-button menu-toggle" aria-label="開啟選單" onClick={() => setMobileNavOpen(true)}><Menu size={19} /></button>
            <div className="breadcrumbs"><span>監控工作區</span><span className="crumb-slash">/</span><strong>{nav.find(item => item.id === view)?.label}</strong></div>
          </div>
          <div className="topbar-right">
            <div className="mode-select-wrap"><span className={`mode-dot ${state.market.mode}`} />
              <select aria-label="資料模式" value={state.market.mode} onChange={event => void changeMode(event.target.value as AppMode)}>
                <option value="demo">示範資料</option><option value="live">即時行情</option><option value="history">歷史帳本</option>
              </select><ChevronDown size={13} className="select-caret" />
            </div>
            <div className={`api-status ${apiOnline ? 'online' : 'offline'}`}><span />{apiOnline ? '後端已連線' : state.market.mode === 'demo' ? '前端示範畫面' : '後端暫未連線'}</div>
            <button className="icon-button bell-button" aria-label="通知"><Bell size={17} /><i /></button>
            <button className="icon-button settings-shortcut" aria-label="設定" onClick={openSettings}><SlidersHorizontal size={17} /></button>
          </div>
        </header>

        <div className="page-wrap">
          <section className="page-heading">
            <div>
              <div className="eyebrow"><span className="eyebrow-line" /> MARKET MONITOR <span className="eyebrow-date">{new Intl.DateTimeFormat('zh-TW', { dateStyle: 'medium' }).format(new Date())}</span></div>
              <h1>{view === 'overview' ? '套利監控總覽' : nav.find(item => item.id === view)?.label}</h1>
              <p>{view === 'overview' ? '追蹤跨所價差、三角路徑與模擬帳戶狀態。' : view === 'ledger' ? '檢視每次模擬的成交步驟、費用假設與餘額變化。' : view === 'depth' ? '使用可見委託簿估算成交量、VWAP 與滑價。' : '以扣除假設費率後的價差排序，深度不足時不建立模擬成交。'}</p>
            </div>
            <div className="heading-actions">
              <div className="last-updated"><span className="status-pulse" />更新於 {new Date(state.market.generated_at).toLocaleTimeString('zh-TW', { hour: '2-digit', minute: '2-digit', second: '2-digit' })}</div>
              <button className="button button-quiet" onClick={() => void refreshState().catch(() => setApiOnline(false))}><RefreshCw size={15} />重新整理</button>
            </div>
          </section>

          <section className="feed-strip">
            <div className="feed-strip-title"><span className="feed-icon"><Activity size={15} /></span><div><strong>行情來源</strong><span>{state.market.mode === 'live' ? state.hosting ? '公開委託簿快照' : '公開 WebSocket' : state.market.mode === 'demo' ? '示範行情' : '已保存帳本資料'}</span></div></div>
            <div className="feed-separator" />
            {(['bitopro', 'okx'] as Venue[]).map(venue => {
              const status = state.market.connections[venue];
              const restBooks = Object.values(state.market.books).filter(book => book.venue === venue && book.source === 'rest');
              const restReady = apiOnline && restBooks.some(book => book.valid && book.age_ms + feedAge <= state.settings.max_book_age_ms);
              const statusLabel = state.market.mode === 'history' ? '歷史模式' : state.market.mode === 'demo' ? '示範來源就緒'
                : status?.connected && apiOnline ? 'WebSocket 已連線' : restReady ? 'REST 快照可用' : '等待有效行情';
              const lastSeen = status?.last_message_ts ?? (restBooks.length ? Math.max(...restBooks.map(book => book.received_ts)) : null);
              return <div className="feed-source" key={venue}><span className={`source-dot ${apiOnline && (status?.connected || restReady) ? 'connected' : ''}`} /><div><strong>{venueLabel(venue)}</strong><span>{statusLabel}</span></div><span className="source-time">{lastSeen ? new Date(lastSeen).toLocaleTimeString('zh-TW', { hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '—'}</span></div>;
            })}
            <div className={`feed-health ${feedWarning ? 'warning' : ''}`}><span className="health-mark">{feedWarning ? '!' : <Check size={12} />}</span><div><strong>{state.market.mode === 'history' ? '歷史帳本模式' : noCurrentMarket ? '等待有效行情' : staleCount > 0 ? `${staleCount} 本行情過期` : '行情狀態正常'}</strong><span>{state.market.mode === 'live' ? '過期或不同步時暫停模擬' : state.market.mode === 'history' ? '不接受新增模擬成交' : '示範價格不可成交'}</span></div></div>
          </section>

          {state.market.mode === 'demo' && <div className="demo-warning"><span>DEMO</span><p>示範價格僅供介面與計算流程預覽，不能代表交易所即時報價或可成交價格。</p><button onClick={() => void changeMode('live')}>切換至即時行情 <ArrowDownUp size={13} /></button></div>}
          {state.market.mode === 'history' && <div className="history-notice"><Clock3 size={15} /><span>歷史模式只顯示本系統已保存的帳本與訊號紀錄；目前不提供歷史委託簿回放。</span></div>}
          {state.hosting && state.market.mode === 'live' && <div className="history-notice"><Clock3 size={15} /><span>約每 {state.hosting.poll_interval_ms / 1000} 秒取得公開快照。關閉或切離分頁後暫停更新與自動模擬。</span></div>}
          {state.hosting && !ledgerReady && <div className="history-notice"><KeyRound size={15} /><span>{ledgerMessage}</span>{ledgerStatus === 'locked' && <button className="text-button" onClick={() => setSignInOpen(true)}>登入模擬帳本</button>}</div>}

          <section className="metric-grid">
            <div className="metric-card"><div className="metric-head"><span>BTC / USDT 中間價</span><span className="metric-icon blue"><Gauge size={15} /></span></div><div className="metric-value">{latestPrice ? `$${money(latestPrice, 2)}` : '—'} <small>USDT</small></div><div className="metric-foot"><span className="metric-dot blue-dot" />{state.market.mode === 'live' ? 'OKX 公開委託簿' : '示範行情'}</div><div className="sparkline spark-blue"><i /><i /><i /><i /><i /><i /><i /><i /><i /><i /><i /><i /><i /><i /></div></div>
            <div className="metric-card"><div className="metric-head"><span>可執行套利路徑</span><span className="metric-icon green"><ArrowLeftRight size={15} /></span></div><div className="metric-value">{state.opportunities.filter(item => item.ready && Number(item.net_pct) > 0).length}<small> / {state.opportunities.length} 條</small></div><div className="metric-foot"><span className="metric-dot green-dot" />扣除設定費率後為正</div><div className="sparkline spark-green"><i /><i /><i /><i /><i /><i /><i /><i /><i /><i /><i /><i /><i /><i /></div></div>
            <div className="metric-card"><div className="metric-head"><span>模擬已實現損益</span><span className="metric-icon violet"><WalletCards size={15} /></span></div><div className={`metric-value ${pnlClass(realizedPnl)}`}>{primaryPnlEntry ? `${realizedPnl >= 0 ? '+' : ''}${money(realizedPnl, 4)}` : '—'} <small>{primaryPnlEntry?.[0] ?? '分幣別'}</small></div><div className="metric-foot"><span className="metric-dot violet-dot" />{totalTrades ? `${totalTrades} 筆${otherPnlAssets ? ` · ${otherPnlAssets}` : '模擬成交'}` : '尚無模擬成交'}</div><div className="sparkline spark-violet"><i /><i /><i /><i /><i /><i /><i /><i /><i /><i /><i /><i /><i /><i /></div></div>
            <div className="metric-card"><div className="metric-head"><span>行情新鮮度</span><span className={`metric-icon ${feedWarning ? 'amber' : 'green'}`}><Clock3 size={15} /></span></div><div className="metric-value">{state.market.mode === 'history' ? '歷史模式' : noCurrentMarket ? '等待中' : feedWarning ? '需檢查' : '正常'}<small>{state.market.mode === 'history' ? '' : staleCount ? ` · ${staleCount} 本過期` : ` · ≤ ${state.settings.max_book_age_ms} ms`}</small></div><div className="metric-foot"><span className={`metric-dot ${feedWarning ? 'amber-dot' : 'green-dot'}`} />{state.market.mode === 'history' ? '不接受新增成交' : '行情失效時停用模擬'}</div><div className="sparkline spark-amber"><i /><i /><i /><i /><i /><i /><i /><i /><i /><i /><i /><i /><i /><i /></div></div>
          </section>

          {(view === 'overview' || view === 'signals') && <section className="content-grid">
            <div className="panel opportunity-panel">
              <div className="panel-heading"><div><span className="panel-kicker">OPPORTUNITIES</span><h2>套利路徑</h2></div><div className="panel-tools"><span className="fee-note">已計入費率估算</span><button className="text-button" onClick={openSettings}>費率設定 <ChevronDown size={13} /></button></div></div>
              <div className="table-head opportunity-head"><span>策略 / 路徑</span><span>價差</span><span>淨利率</span><span>狀態</span><span></span></div>
              <div className="opportunity-list">{state.opportunities.map((item, index) => <OpportunityRow key={item.id} item={item} index={index} onSimulate={() => void simulate(item)} busy={busyTrade === item.id} apiOnline={ledgerReady && item.books.every(key => {
                const book = state.market.books[key];
                return book?.valid && book.age_ms + feedAge <= state.settings.max_book_age_ms;
              })} amount={size[item.id] ?? item.reference_size} onAmountChange={value => setSize(current => ({ ...current, [item.id]: value }))} mode={state.market.mode} />)}</div>
              <div className="panel-footnote"><ShieldCheck size={14} /><span>模擬按委託簿深度逐檔估算；深度不足、報價過期或路徑不同步時會拒絕整筆。</span></div>
            </div>

            <div className="panel account-panel">
              <div className="panel-heading"><div><span className="panel-kicker">PAPER ACCOUNTS</span><h2>虛擬資產</h2></div><button className="text-button" onClick={openBalanceEditor}>調整餘額 <ChevronDown size={13} /></button></div>
              <div className="account-total-row"><span>兩所資產</span><span className="account-value">{state.balances.length ? `${state.balances.length} 項` : '—'}</span></div>
              <div className="account-group">
                <div className="account-exchange"><span className="exchange-logo bito-logo">B</span><strong>BitoPro</strong><span>虛擬帳戶</span></div>
                {(['TWD', 'BTC', 'USDT'] as const).map(asset => <AssetRow key={`bitopro-${asset}`} state={state} venue="bitopro" asset={asset} />)}
              </div>
              <div className="account-group second">
                <div className="account-exchange"><span className="exchange-logo okx-logo">O</span><strong>OKX</strong><span>虛擬帳戶</span></div>
                {(['BTC', 'USDT'] as const).map(asset => <AssetRow key={`okx-${asset}`} state={state} venue="okx" asset={asset} />)}
              </div>
              <div className="account-note"><span className="mini-check"><Check size={11} /></span>交易前需在各交易所預先持有資產；不假設即時跨所轉帳。</div>
              <button className="button button-outline full-button" onClick={() => setView('ledger')}><History size={14} />查看交易帳本</button>
            </div>
          </section>}

          {view === 'overview' && <section className="bottom-grid">
            <DepthPanel state={state} />
            <RecentTrades trades={state.trades.slice(0, 4)} onViewAll={() => setView('ledger')} />
          </section>}
          {view === 'depth' && <section className="depth-page-grid">{['okx:BTC-USDT', 'bitopro:BTC_USDT', 'bitopro:BTC_TWD', 'bitopro:USDT_TWD'].map(key => <DepthPanel key={key} state={state} bookKey={key} expanded />)}</section>}
          {view === 'ledger' && <section className="ledger-stack"><div className="panel ledger-panel"><Ledger trades={state.trades} /></div><div className="panel signals-panel"><SignalsTable signals={state.signals ?? []} /></div></section>}

          <footer className="page-footer"><div><span className="footer-brand">ARBITRAGE DESK</span><span>第一版 · 模擬交易環境</span></div><div><span><i className="footer-dot" />公開行情</span><span>無真實下單 API</span><span>費率為估算值</span></div></footer>
        </div>
      </main>

      {settingsOpen && <SettingsDrawer settings={formSettings} onChange={setFormSettings} onClose={() => setSettingsOpen(false)} onSave={() => void saveSettings()} saving={savingSettings} hosted={Boolean(state.hosting)} />}
      {balancesOpen && <BalanceDrawer balances={balanceDraft} onChange={setBalanceDraft} onClose={() => setBalancesOpen(false)} onSave={() => void saveBalances()} saving={savingBalances} />}
      {signInOpen && <div className="drawer-scrim" onMouseDown={event => { if (event.target === event.currentTarget) { setSignInOpen(false); setAccessKey(''); } }}>
        <aside className="settings-drawer" aria-label="登入 Rich 模擬帳本">
          <div className="drawer-header"><h2>登入模擬帳本</h2><button className="icon-button" aria-label="關閉" onClick={() => { setSignInOpen(false); setAccessKey(''); }}><X size={18} /></button></div>
          <form className="drawer-section" onSubmit={event => { event.preventDefault(); void signIn(); }}>
            <label className="field-label">Rich 帳本存取金鑰<input type="password" autoComplete="current-password" required value={accessKey} onChange={event => setAccessKey(event.target.value)} /></label>
            <button className="button button-primary" type="submit" disabled={signingIn || !accessKey.trim()}><KeyRound size={14} />{signingIn ? '登入中' : '登入'}</button>
          </form>
        </aside>
      </div>}
      {toast && <div className="toast"><span className="toast-symbol"><Check size={14} /></span>{toast}</div>}
    </div>
  );
}

function OpportunityRow({ item, index, onSimulate, busy, apiOnline, amount, onAmountChange, mode }: { item: Opportunity; index: number; onSimulate: () => void; busy: boolean; apiOnline: boolean; amount: string; onAmountChange: (value: string) => void; mode: AppMode }) {
  const pct = Number(item.net_pct);
  const raw = Number(item.raw_pct);
  const estimatedPnl = Number(item.estimate_pnl);
  const depthHint = item.kind === 'cross' ? `可見深度 ${money(item.depth_capacity ?? 0, 3)} BTC` : '逐檔估算三腿';
  const estimateHint = `試算 ${estimatedPnl > 0 ? '+' : ''}${money(item.estimate_pnl, 3)} ${item.estimate_asset}`;
  return <div className="opportunity-row-wrap">
    <div className="opportunity-row">
      <div className="route-cell"><span className={`route-number route-${index + 1}`}>{String(index + 1).padStart(2, '0')}</span><div><strong>{item.title}</strong><span>{item.kind === 'cross' ? '跨交易所 · BTC/USDT' : '幣託三角路徑'}</span></div></div>
      <div className={`spread-cell ${pnlClass(raw)}`}>{raw > 0 ? '+' : ''}{money(raw, 2)}<small>%</small></div>
      <div className={`net-cell ${pnlClass(pct)}`}>{pct > 0 ? '+' : ''}{money(pct, 2)}<small>%</small></div>
      <div className="status-cell"><span className={`route-status ${item.ready && item.depth_ready ? 'ready' : item.ready ? 'depth-warning' : 'stale'}`}><i />{!item.ready ? '暫停' : item.depth_ready ? '可計算' : '深度不足'}</span></div>
      <div className="simulate-cell"><button className="button button-small button-simulate" disabled={!item.ready || !apiOnline || mode === 'history' || busy} onClick={onSimulate}>{busy ? <RefreshCw size={13} className="spin" /> : <Play size={13} fill="currentColor" />}{busy ? '計算中' : '模擬'}</button></div>
    </div>
    <div className="route-detail"><div className="route-flow">{item.kind === 'cross' ? <><span>{venueLabel(item.buy_venue)} 買</span><ArrowLeftRight size={12} /><span>{venueLabel(item.sell_venue)} 賣</span></> : <><span>BitoPro</span><span className="flow-chevron">›</span><span>BitoPro</span><span className="flow-chevron">›</span><span>BitoPro</span></>}</div><label className="size-input">{item.kind === 'cross' ? 'BTC' : 'TWD'}<input aria-label={`${item.title} 模擬投入`} inputMode="decimal" value={amount} onChange={event => onAmountChange(event.target.value)} /></label><span className={`route-hint ${estimatedPnl >= 0 ? 'positive' : 'negative'}`}>{estimateHint} · {depthHint}</span></div>
  </div>;
}

function AssetRow({ state, venue, asset }: { state: DashboardState; venue: Venue; asset: string }) {
  const balance = state.balances.find(row => row.venue === venue && row.asset === asset)?.amount;
  return <div className="asset-row"><div className={`asset-icon asset-${asset.toLowerCase()}`}>{asset === 'BTC' ? '₿' : asset === 'USDT' ? '₮' : '$'}</div><div className="asset-name"><strong>{asset}</strong><span>{asset === 'BTC' ? 'Bitcoin' : asset === 'USDT' ? 'Tether' : '新台幣'}</span></div><div className="asset-amount"><strong>{balance ? money(balance, asset === 'BTC' ? 6 : 2) : '—'}</strong><span>{asset}</span></div></div>;
}

function DepthPanel({ state, bookKey = 'okx:BTC-USDT', expanded = false }: { state: DashboardState; bookKey?: string; expanded?: boolean }) {
  const book = state.market.books[bookKey];
  const label = bookKey.startsWith('okx:') ? 'OKX' : 'BitoPro';
  const pair = bookKey.split(':')[1] ?? 'BTC-USDT';
  const rows = Math.min(expanded ? 10 : 5, Math.max(book?.bids.length ?? 0, book?.asks.length ?? 0));
  const maxTotal = Math.max(...(book?.bids ?? []).slice(0, rows).map(level => Number(level.amount) * Number(level.price)), ...(book?.asks ?? []).slice(0, rows).map(level => Number(level.amount) * Number(level.price)), 1);
  return <div className={`panel depth-panel ${expanded ? 'expanded' : ''}`}>
    <div className="panel-heading"><div><span className="panel-kicker">ORDER BOOK · {label.toUpperCase()}</span><h2>{pair.replace('_', ' / ')}</h2></div><div className="depth-live"><span className={book && book.age_ms <= state.settings.max_book_age_ms ? 'connected' : ''} />{book ? `${book.age_ms} ms` : '無行情'}</div></div>
    <div className="depth-table-head"><span>委託量</span><span>買入價格</span><span>賣出價格</span><span>委託量</span></div>
    {Array.from({ length: rows || 5 }, (_, i) => {
      const bid = book?.bids[i]; const ask = book?.asks[i];
      const bidValue = bid ? Number(bid.price) * Number(bid.amount) : 0;
      const askValue = ask ? Number(ask.price) * Number(ask.amount) : 0;
      return <div className="depth-row" key={i}>
        <div className="depth-qty bid-qty"><i style={{ width: `${Math.max(4, bidValue / maxTotal * 100)}%` }} />{bid ? money(bid.amount, pair.includes('TWD') ? 4 : 5) : '—'}</div>
        <span className="bid-price">{bid ? money(bid.price, pair.includes('TWD') ? 0 : 2) : '—'}</span>
        <span className="ask-price">{ask ? money(ask.price, pair.includes('TWD') ? 0 : 2) : '—'}</span>
        <div className="depth-qty ask-qty"><i style={{ width: `${Math.max(4, askValue / maxTotal * 100)}%` }} />{ask ? money(ask.amount, pair.includes('TWD') ? 4 : 5) : '—'}</div>
      </div>;
    })}
    {book && <div className="depth-spread"><span>買賣中間價</span><strong>{money((Number(book.bids[0]?.price) + Number(book.asks[0]?.price)) / 2, pair.includes('TWD') ? 0 : 2)}</strong><span className="spread-bps">價差 {money((Number(book.asks[0]?.price) - Number(book.bids[0]?.price)) / Number(book.bids[0]?.price) * 10000, 1)} bps</span></div>}
    {!expanded && <div className="depth-footer"><span><span className="legend-dot bid-legend" />買盤</span><span><span className="legend-dot ask-legend" />賣盤</span><span className="depth-disclaimer">只顯示可見深度</span></div>}
  </div>;
}

function RecentTrades({ trades, onViewAll }: { trades: Trade[]; onViewAll: () => void }) {
  return <div className="panel recent-panel"><div className="panel-heading"><div><span className="panel-kicker">PAPER LEDGER</span><h2>最近模擬</h2></div><button className="text-button" onClick={onViewAll}>查看全部 <ChevronDown size={13} /></button></div>
    {trades.length ? <div className="recent-list">{trades.map(trade => <div className="recent-row" key={trade.id}><span className="recent-icon"><ArrowLeftRight size={14} /></span><div><strong>{trade.strategy_name}</strong><span>{new Date(trade.created_at).toLocaleTimeString('zh-TW')} · {trade.mode === 'demo' ? '示範' : '即時'}</span></div><div className={`recent-pnl ${pnlClass(trade.pnl)}`}>{Number(trade.pnl) > 0 ? '+' : ''}{money(trade.pnl, 4)}<small> {trade.end_asset}</small></div></div>)}</div>
      : <div className="empty-state"><div className="empty-icon"><History size={19} /></div><strong>尚無模擬紀錄</strong><span>選擇一條有效路徑並輸入金額，開始建立虛擬帳本。</span><button className="button button-outline" onClick={onViewAll}>查看帳本欄位</button></div>}
    <div className="recent-footer"><ShieldCheck size={14} /><span>每筆紀錄都會保存行情快照與前後餘額</span></div>
  </div>;
}

function Ledger({ trades }: { trades: Trade[] }) {
  return <>
    <div className="panel-heading ledger-heading"><div><span className="panel-kicker">PERSISTENT PAPER RECORDS</span><h2>模擬交易帳本</h2></div><span className="ledger-count">{trades.length} 筆紀錄</span></div>
    {trades.length ? <div className="ledger-table-wrap"><table className="ledger-table"><thead><tr><th>時間</th><th>套利路徑</th><th>模式 / 觸發</th><th>投入</th><th>回收</th><th>損益</th><th>淨報酬</th></tr></thead><tbody>{trades.map(trade => <tr key={trade.id}><td>{new Date(trade.created_at).toLocaleString('zh-TW')}</td><td><strong>{trade.strategy_name}</strong></td><td><span className={`ledger-mode ${trade.mode}`}>{modeLabel[trade.mode]}</span><small>{trade.trigger_reason}</small></td><td>{money(trade.start_amount, 4)} {trade.start_asset}</td><td>{money(trade.end_amount, 4)} {trade.end_asset}</td><td className={pnlClass(trade.pnl)}>{Number(trade.pnl) > 0 ? '+' : ''}{money(trade.pnl, 4)} {trade.end_asset}</td><td className={pnlClass(trade.pnl_percent)}>{Number(trade.pnl_percent) > 0 ? '+' : ''}{money(trade.pnl_percent, 3)}%</td></tr>)}</tbody></table></div> : <div className="empty-state ledger-empty"><div className="empty-icon"><History size={19} /></div><strong>帳本會在第一筆模擬後出現紀錄</strong><span>目前不會載入外部歷史委託簿；每筆新模擬都會保存當時的深度、費用與資金變化。</span></div>}
  </>;
}

function SignalsTable({ signals }: { signals: Signal[] }) {
  return <>
    <div className="panel-heading"><div><span className="panel-kicker">SIGNAL HISTORY</span><h2>訊號狀態紀錄</h2></div><span className="ledger-count">{signals.length} 筆最近紀錄</span></div>
    {signals.length ? <div className="ledger-table-wrap"><table className="ledger-table signal-table"><thead><tr><th>時間</th><th>套利路徑</th><th>資料模式</th><th>狀態</th><th>淨利率</th></tr></thead><tbody>{signals.map(signal => <tr key={signal.id}><td>{new Date(signal.observed_at).toLocaleString('zh-TW')}</td><td><strong>{signal.strategy_name}</strong></td><td><span className={`ledger-mode ${signal.mode}`}>{modeLabel[signal.mode]}</span></td><td><span className={`signal-state ${signal.status === '淨利為正' ? 'positive' : ''}`}>{signal.status}</span><small>{signal.reason}</small></td><td className={pnlClass(signal.net_pct)}>{Number(signal.net_pct) > 0 ? '+' : ''}{money(signal.net_pct, 3)}%</td></tr>)}</tbody></table></div> : <div className="empty-state signal-empty"><div className="empty-icon"><Activity size={18} /></div><strong>訊號將從服務啟動後開始記錄</strong><span>系統在路徑狀態改變時保存訊號與委託簿快照，不會補造上線前的歷史深度。</span></div>}
  </>;
}

function BalanceDrawer({ balances, onChange, onClose, onSave, saving }: { balances: Record<string, string>; onChange: (next: Record<string, string>) => void; onClose: () => void; onSave: () => void; saving: boolean }) {
  const fields = [
    { venue: 'bitopro', asset: 'TWD', label: 'TWD · 新台幣' },
    { venue: 'bitopro', asset: 'BTC', label: 'BTC · Bitcoin' },
    { venue: 'bitopro', asset: 'USDT', label: 'USDT · Tether' },
    { venue: 'okx', asset: 'BTC', label: 'BTC · Bitcoin' },
    { venue: 'okx', asset: 'USDT', label: 'USDT · Tether' },
  ];
  return <div className="drawer-scrim" onMouseDown={event => { if (event.target === event.currentTarget) onClose(); }}>
    <aside className="settings-drawer balance-drawer" aria-label="調整虛擬資產">
      <div className="drawer-header"><div><span className="panel-kicker">PAPER ACCOUNT SETUP</span><h2>調整虛擬資產</h2></div><button className="icon-button" aria-label="關閉" onClick={onClose}><X size={18} /></button></div>
      <p className="balance-drawer-intro">設定 BitoPro 與 OKX 各自預先持有的虛擬餘額。模擬成交會使用這些餘額檢查資金，不會連到交易所帳戶。</p>
      {(['bitopro', 'okx'] as const).map(venue => <div className="balance-drawer-group" key={venue}><div className="account-exchange"><span className={`exchange-logo ${venue === 'bitopro' ? 'bito-logo' : 'okx-logo'}`}>{venue === 'bitopro' ? 'B' : 'O'}</span><strong>{venueLabel(venue)}</strong><span>虛擬帳戶</span></div>{fields.filter(field => field.venue === venue).map(field => <label className="balance-field" key={`${field.venue}:${field.asset}`}><span>{field.label}</span><input inputMode="decimal" type="number" min="0" step="any" value={balances[`${field.venue}:${field.asset}`] ?? '0'} onChange={event => onChange({ ...balances, [`${field.venue}:${field.asset}`]: event.target.value })} /></label>)}</div>)}
      <div className="settings-assumptions"><strong>帳本紀錄不會清除</strong><span>調整餘額只更新目前的虛擬帳戶；過去成交與訊號快照仍會保留。</span></div>
      <div className="drawer-actions"><button className="button button-quiet" onClick={onClose}>取消</button><button className="button button-primary" onClick={onSave} disabled={saving}>{saving ? <RefreshCw size={14} className="spin" /> : <Check size={14} />}{saving ? '儲存中' : '儲存餘額'}</button></div>
    </aside>
  </div>;
}

function SettingsDrawer({ settings, onChange, onClose, onSave, saving, hosted }: { settings: TradingSettings; onChange: (settings: TradingSettings) => void; onClose: () => void; onSave: () => void; saving: boolean; hosted: boolean }) {
  const update = (key: keyof TradingSettings, value: string | number | boolean) => onChange({ ...settings, [key]: value });
  return <div className="drawer-scrim" onMouseDown={event => { if (event.target === event.currentTarget) onClose(); }}>
    <aside className="settings-drawer" aria-label="監控與模擬設定">
      <div className="drawer-header"><div><span className="panel-kicker">CONFIGURATION</span><h2>監控與模擬設定</h2></div><button className="icon-button" aria-label="關閉設定" onClick={onClose}><X size={18} /></button></div>
      <div className="drawer-section"><div className="drawer-section-title"><SlidersHorizontal size={15} /><strong>套利計算假設</strong></div><p>費率套用於模擬計算。請依自己的交易等級調整，並將費用幣別視為報價幣。</p>
        <label className="field-label">BitoPro taker 費率 <span>bps</span><input type="number" min="0" max="200" step="0.1" value={settings.bitopro_fee_bps} onChange={event => update('bitopro_fee_bps', event.target.value)} /></label>
        <label className="field-label">OKX taker 費率 <span>bps</span><input type="number" min="0" max="200" step="0.1" value={settings.okx_fee_bps} onChange={event => update('okx_fee_bps', event.target.value)} /></label>
        <label className="field-label">最低淨利門檻 <span>bps</span><input type="number" min="0" step="1" value={settings.net_threshold_bps} onChange={event => update('net_threshold_bps', event.target.value)} /></label>
        <label className="field-label">觸發冷卻時間 <span>秒</span><input type="number" min="1" max="86400" value={settings.cooldown_seconds} onChange={event => update('cooldown_seconds', Number(event.target.value))} /></label>
        <label className="field-label">行情最長有效時間 <span>ms</span><input type="number" min="250" max="60000" step="250" value={settings.max_book_age_ms} onChange={event => update('max_book_age_ms', Number(event.target.value))} /></label>
      </div>
      <div className="drawer-section auto-section"><div className="auto-setting-row"><div className="auto-setting-icon"><Activity size={16} /></div><div><strong>條件觸發自動模擬</strong><span>{hosted ? '登入且即時行情分頁開啟時，達門檻就會寫入模擬帳本' : '達到最低淨利門檻時，後端自動寫入模擬帳本'}</span></div><button className={`toggle ${settings.auto_simulation_enabled ? 'on' : ''}`} aria-label="切換自動模擬" aria-checked={settings.auto_simulation_enabled} role="switch" onClick={() => update('auto_simulation_enabled', !settings.auto_simulation_enabled)}><i /></button></div><div className="auto-notice"><ShieldCheck size={14} /><span>初始關閉。成交只會更新模擬帳本中的虛擬餘額。</span></div></div>
      <div className="settings-assumptions"><strong>未納入費率的項目</strong><span>跨所轉帳、資產再平衡與額外換匯成本會另外影響實際淨利；本版尚未估算時不把它們當成零成本保證。</span></div>
      <div className="drawer-actions"><button className="button button-quiet" onClick={onClose}>取消</button><button className="button button-primary" onClick={onSave} disabled={saving}>{saving ? <RefreshCw size={14} className="spin" /> : <Check size={14} />}{saving ? '儲存中' : '儲存設定'}</button></div>
    </aside>
  </div>;
}

export default App;

