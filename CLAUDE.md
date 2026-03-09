# lokigrabber

Python script for querying Loki logs and exporting session data to CSV/Excel.

## Environment

- Python version managed via pyenv — virtualenv: `uptimesessions`
- Dependencies loaded via `python-dotenv` from `.env` (never read this file)

## Project structure

- 'loki_grabber.py' - a script to pull single or multiple UL session transcripts from loki

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

## Version control

- Remote: `git@github.com:UL-stuart/lokigrabber.git`
- Never commit directly to `main` — always use a feature branch
- Branch naming: `feature/<short-description>` or `fix/<short-description>`
- Workflow: create branch → commit changes → push branch → open PR targeting `main`
- Ask before pushing or creating PRs
- **For new features:** Ask if you want to create a new feature branch (e.g., `feature/description`) or continue on the current branch
- **Before every commit:** Run the test suite (`pytest test_loki_grabber.py -v`) to ensure all tests pass
