# Arbitrage Desk

BitoPro × OKX BTC/USDT monitoring and simulated arbitrage desk. The application only reads public market data. It has no exchange credentials, private API access, or real-order endpoints.

## Start

1. Copy `.env.example` to `.env` and adjust the paper-account starting balances if needed.
2. Run `docker compose up --build` from this directory.
3. Open `http://localhost:8080`.

The default is **demo mode**. Change `MARKET_DATA_MODE=live` in `.env` and restart the service to connect to the public BitoPro and OKX order books. Paper trades and settings are stored in PostgreSQL. All fees are configurable estimates and are shown as assumptions. The web port is bound to `127.0.0.1`; add authentication and HTTPS before exposing any instance to another network.

## Vercel deployment

The Rich Vercel project builds from the existing `frontend` directory. Vite serves the React dashboard, and `frontend/vercel.json` routes `/api/*` to the FastAPI function on the same domain.

1. Connect a dedicated PostgreSQL database to Rich's production environment. The Neon integration supplies `DATABASE_URL_UNPOOLED` and `DATABASE_URL`. A direct TLS connection string is preferred.
2. Set `RICH_LEDGER_ACCESS_KEY` to a random secret of at least 32 characters in Rich's production environment, then redeploy. The same key opens the paper ledger and its settings in the dashboard.
3. Open the production deployment. The default mode is **即時行情**. Choose **登入模擬帳本** to load balances, adjust settings, or execute a simulated trade.

The Vercel deployment fetches public order book snapshots on demand, about every five seconds while the dashboard is visible. Each refresh collects BitoPro books through its public WebSocket and an OKX book through its public REST API. Every manual simulation requests a fresh snapshot. The ledger rejects expired quotes, insufficient depth, and insufficient funds. Automatic paper simulation starts disabled. If enabled, it runs during signed-in updates in live mode. Closing or leaving the page pauses polling and automatic simulation.

The Docker Compose deployment runs the persistent WebSocket feed and monitor. The starting paper balances defined in `frontend/server/core.py` are illustrative. Adjust them from **虛擬資產 → 調整餘額** before making simulated trades. Existing ledger rows are retained when balances change.

## First-version scope

- BitoPro order books for `BTC_TWD`, `USDT_TWD`, and `BTC_USDT`.
- OKX `BTC-USDT` order book; sequence gaps trigger a reconnect and require a fresh snapshot.
- Two cross-exchange BTC/USDT directions and two BitoPro triangular routes.
- Depth-walk pricing, fee estimates, stale-book and time-skew checks, simulated balances, and a persistent paper ledger.
- Optional threshold-based paper simulation. It starts disabled and can be paused from the UI.
- Market signal history is stored with the source and observed prices. Vercel records observations during signed-in live updates, at most once per route per five minutes.
- A REST API. The persistent Docker Compose deployment also provides a WebSocket feed.

Historical depth replay, exchange lot-size metadata, multi-user accounts, external notifications, and live orders are outside this version. The history tab accumulates paper trades and signal-state changes from this deployment; it does not invent historical order-book data.

## Run without Docker

The frontend and backend are separate services. Start PostgreSQL, install `backend/requirements.txt` in a Python 3.11+ environment, then run `uvicorn backend.main:app --reload --port 8000`. In a second terminal, run `npm install && npm run dev` and set `VITE_API_BASE=http://localhost:8000` if the Vite proxy is not used.

## Safety and data handling

- The backend only uses public market-data endpoints.
- `POST /api/simulations` updates only the local PostgreSQL paper ledger.
- No exchange keys or personal credentials are read or stored.
- Demo prices are clearly marked and must never be interpreted as executable quotes.
- OKX public-market WebSocket connections use port 443.

