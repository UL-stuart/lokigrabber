import os
import re
import argparse
from datetime import datetime, timedelta, timezone

import requests
import urllib3
import pandas as pd
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

parser = argparse.ArgumentParser(description="Fetch Loki logs for sessions.")
subparsers = parser.add_subparsers(dest="mode", required=True)

multi_parser = subparsers.add_parser("multi", help="Batch fetch sessions from a CSV file.")
multi_parser.add_argument("csv_file", help="Path to the sessions CSV file")

single_parser = subparsers.add_parser("single", help="Fetch a single session by ID and date.")
single_parser.add_argument("session_id", help="Session ID to fetch")
single_parser.add_argument("session_date", help="Date of the session (yyyy-mm-dd, UTC)")

args = parser.parse_args()

load_dotenv()
LOKI_URL      = os.environ["LOKI_URL"]
KEYCLOAK_URL  = os.environ["KEYCLOAK_URL"]
CLIENT_ID     = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]
LOKI_ORG_ID   = "uptimelabs"

print(f"Loki:     {LOKI_URL}")
print(f"Keycloak: {KEYCLOAK_URL}")
print(f"Org ID:   {LOKI_ORG_ID}")

resp = requests.post(
    f"{KEYCLOAK_URL}/realms/tenants/protocol/openid-connect/token",
    data={
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    },
    verify=False,
)
resp.raise_for_status()
TOKEN = resp.json()["access_token"]
print("Token acquired ✓")

KNOWN_PLAYERS = ["Bez", "Bob", "Daniel", "Hamed", "Shay", "Tanya", "Tinus", "UptimeLabs", "Maya"]
known_pattern = "^(" + "|".join(KNOWN_PLAYERS) + ")"


def fetch_and_export(session_id, start_ns, end_ns, out_file):
    start_dt = datetime.fromtimestamp(start_ns / 1e9, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ns / 1e9, tz=timezone.utc)
    print(f"Fetching session {session_id} ({start_dt} → {end_dt})...")

    response = requests.get(
        f"{LOKI_URL}/loki/api/v1/query_range",
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "X-Scope-OrgID": LOKI_ORG_ID,
        },
        params={
            "direction": "BACKWARD",
            "limit": 50000,
            "query": f'{{session_id="{session_id}",app="slack"}}',
            "start": start_ns,
            "end": end_ns,
            "step": 300,
        },
        verify=False,
    )
    response.raise_for_status()
    streams = response.json()["data"]["result"]
    print(f"  Streams returned: {len(streams)}")

    if not streams:
        print(f"  No logs found for session {session_id}, skipping.")
        return

    rows = []
    for stream in streams:
        labels = stream["stream"]
        for ts_ns, line in stream["values"]:
            rows.append({"timestamp_ns": ts_ns, "log_line": line, **labels})

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp_ns"].astype("int64"), unit="ns", utc=True)
    df.loc[~df["player_name"].str.match(known_pattern, case=False, na=False), "player_name"] = "PLAYER"

    export_df = df[["timestamp", "player_name", "channel", "log_line"]].copy()
    export_df = export_df.sort_values("timestamp").reset_index(drop=True)
    export_df["timestamp"] = export_df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    export_df["log_line"] = export_df["log_line"].str.replace(
        r'^\([^)]+\)\s+\S+:\s+', "", regex=True
    )
    export_df["log_line"] = export_df["log_line"].str.replace(
        r'@[a-zA-Z0-9._%+-]+[-@][a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', "@PLAYER", regex=True
    )
    export_df = export_df.rename(columns={
        "timestamp": "datetime",
        "player_name": "player",
        "log_line": "message",
    })

    export_df.to_csv(out_file, index=False)
    print(f"  Saved {len(export_df)} rows → {out_file}")


if args.mode == "multi":
    sessions_df = pd.read_csv(args.csv_file)
    print(f"Loaded {len(sessions_df)} sessions from: {args.csv_file}")

    WINDOW_HOURS = 6
    has_name_col = "name" in sessions_df.columns

    for _, row in sessions_df.iterrows():
        session_id   = str(row["session-id"])
        session_time = pd.to_datetime(row["session-time"], utc=True)
        start_ns     = int((session_time - timedelta(hours=WINDOW_HOURS)).timestamp() * 1e9)
        end_ns       = int((session_time + timedelta(hours=WINDOW_HOURS)).timestamp() * 1e9)
        out_file     = f"{row['name']}-{session_id}.csv" if has_name_col else f"{session_id}.csv"
        fetch_and_export(session_id, start_ns, end_ns, out_file)

elif args.mode == "single":
    start = datetime.strptime(args.session_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end   = start + timedelta(days=1)
    start_ns = int(start.timestamp() * 1e9)
    end_ns   = int(end.timestamp() * 1e9)
    fetch_and_export(args.session_id, start_ns, end_ns, f"{args.session_id}.csv")

print("All sessions complete ✓")
