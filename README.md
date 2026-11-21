# Arbitrage Desk

BitoPro × OKX BTC/USDT monitoring and simulated arbitrage desk. The application only reads public market data. It has no exchange credentials, private API access, or real-order endpoints.

## Start

1. Copy `.env.example` to `.env` and adjust the paper-account starting balances if needed.
2. Run `docker compose up --build` from this directory.
3. Open `http://localhost:8080`.

The default is **demo mode**. Change `MARKET_DATA_MODE=live` in `.env` and restart the service to connect to the public BitoPro and OKX order books. Paper trades and settings are stored in PostgreSQL. All fees are configurable estimates and are shown as assumptions. The web port is bound to `127.0.0.1`; add authentication and HTTPS before exposing any instance to another network.

## Hosted deployment

The Vercel frontend project is configured to proxy `/api/*` to the persistent FastAPI service. Deploy the repository root as a Railway service using `railway.json`, add a Railway PostgreSQL service, and set the backend service variables:

- `DATABASE_URL` to Railway's private PostgreSQL connection URL.
- `MARKET_DATA_MODE=live`.
- `BACKEND_PROXY_SECRET` to a long random secret shared with the Vercel project.

Set the Vercel production variables `BACKEND_URL` to the Railway service's public HTTPS URL and `BACKEND_PROXY_SECRET` to the same secret. The frontend uses same-origin requests, so the backend remains protected by the injected secret and Vercel's project access protection. Keep `auto_simulation_enabled` off until you explicitly turn it on in the dashboard.

The starting paper balances in `backend/main.py` are illustrative only. Adjust them from **虛擬資產 → 調整餘額** before making simulated trades. Existing ledger rows are retained when balances change.

## First-version scope

- BitoPro order books for `BTC_TWD`, `USDT_TWD`, and `BTC_USDT`.
- OKX `BTC-USDT` order book; sequence gaps trigger a reconnect and require a fresh snapshot.
- Two cross-exchange BTC/USDT directions and two BitoPro triangular routes.
- Depth-walk pricing, fee estimates, stale-book and time-skew checks, simulated balances, and a persistent paper ledger.
- Optional threshold-based paper simulation. It starts disabled and can be paused from the UI.
- Signal-state history begins when the service is started and saves state changes with the associated market snapshot.
- A browser-friendly REST API and WebSocket feed.

Historical depth replay, exchange lot-size metadata, multi-user accounts, external notifications, and live orders are outside this version. The history tab accumulates paper trades and signal-state changes from this deployment; it does not invent historical order-book data.

## Run without Docker

The frontend and backend are separate services. Start PostgreSQL, install `backend/requirements.txt` in a Python 3.11+ environment, then run `uvicorn backend.main:app --reload --port 8000`. In a second terminal, run `npm install && npm run dev` and set `VITE_API_BASE=http://localhost:8000` if the Vite proxy is not used.

## Safety and data handling

- The backend calls public market-data WebSocket endpoints only.
- `POST /api/simulations` updates only the local PostgreSQL paper ledger.
- No exchange keys or personal credentials are read or stored.
- Demo prices are clearly marked and must never be interpreted as executable quotes.
- OKX public-market WebSocket connections use port 443.

