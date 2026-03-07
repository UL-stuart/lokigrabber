# lokigrabber

Jupyter notebooks for querying Loki logs and exporting session data to CSV/Excel.

## Environment

- Python version managed via pyenv — virtualenv: `uptimesessions`
- Dependencies loaded via `python-dotenv` from `.env` (never read this file)

## Project structure

- `loki_grabber.ipynb` — single-session log query and CSV export
- `loki_multi_session.ipynb` — multi-session batch query

## Configuration

Secrets live in `.env` (gitignored). See `.env.example` for required keys:
- `LOKI_URL` — Loki instance URL
- `KEYCLOAK_URL` — Keycloak instance for auth
- `CLIENT_ID` / `CLIENT_SECRET` — client credentials for token fetch
- `LOKI_ORG_ID` — Loki org/tenant ID

## Key patterns

- Auth: Keycloak client credentials flow → Bearer token
- Loki query: `query_range` endpoint with nanosecond timestamps, `X-Scope-OrgID` header
- SSL verification disabled (`verify=False`) — internal/self-signed certs
- Output: CSV with columns `datetime`, `player`, `channel`, `message`; emails redacted to `@PLAYER`
