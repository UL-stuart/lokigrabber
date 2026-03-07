# lokigrabber

Queries Loki for Slack session logs and exports them to CSV.

Logs are fetched using a Keycloak client credentials token. Player names not in the known list are redacted to `PLAYER`, and email addresses are redacted to `@PLAYER`.

## Requirements

- Python 3 (managed via pyenv, virtualenv `uptimesessions`)
- The following packages:

```bash
pip install requests urllib3 pandas python-dotenv
```

## Configuration

Copy `.env.example` to `.env` and fill in your values:

```
LOKI_URL=https://your-loki-instance.example.com
KEYCLOAK_URL=https://your-keycloak.example.com
CLIENT_ID=your-client-id
CLIENT_SECRET=your-client-secret
LOKI_ORG_ID=your-org-id
```

## Usage

### Single session

Fetch logs for one session ID over a full UTC day (midnight to midnight):

```bash
python loki_grabber.py single <session-id> <yyyy-mm-dd>
```

Output: `<session-id>.csv`

Example:

```bash
python loki_grabber.py single 7916 2026-03-07
```

### Multiple sessions (batch)

Fetch logs for multiple sessions defined in a CSV file:

```bash
python loki_grabber.py multi <sessions.csv>
```

The CSV must have a `session-id` column and a `session-time` column (any timezone-aware datetime format). An optional `name` column prefixes the output filename.

| Column | Required | Description |
|---|---|---|
| `session-id` | yes | Uptime session ID |
| `session-time` | yes | Timestamp of the session (used to compute a ±6-hour query window) |
| `name` | no | The player name: If present, output files are named `<name>-<session-id>.csv` |

Output: one CSV per session, with columns `datetime`, `player`, `channel`, `message`.
