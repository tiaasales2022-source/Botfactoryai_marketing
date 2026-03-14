from __future__ import annotations

import json
import os

from google_auth_oauthlib.flow import InstalledAppFlow


SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


def main() -> int:
    client_id = os.getenv("GMAIL_API_CLIENT_ID", "").strip()
    client_secret = os.getenv("GMAIL_API_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise SystemExit("Set GMAIL_API_CLIENT_ID and GMAIL_API_CLIENT_SECRET first.")

    client_config = {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    }
    flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
    credentials = flow.run_local_server(port=0, access_type="offline", prompt="consent")
    payload = {
        "GMAIL_API_CLIENT_ID": client_id,
        "GMAIL_API_CLIENT_SECRET": client_secret,
        "GMAIL_API_REFRESH_TOKEN": credentials.refresh_token,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
