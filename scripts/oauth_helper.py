# scripts/oauth_helper.py
# Helpers to build Drive & Sheets clients using a long-lived OAuth refresh token.
# Reads the following environment variables:
#   DRIVE_OAUTH_CLIENT_ID
#   DRIVE_OAUTH_CLIENT_SECRET
#   DRIVE_OAUTH_REFRESH_TOKEN

import os
import sys
from typing import Sequence

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build


# Scopes we rely on for the whole project
SCOPES: Sequence[str] = (
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
)


def _need(name: str) -> str:
    val = os.getenv(name, "").strip()
    if not val:
        print(f"ERROR[ENV_MISSING]: {name} is empty")
        sys.exit(2)
    return val


def _user_credentials() -> Credentials:
    """
    Create user credentials object from env refresh token.
    This does NOT hit the network yet; token is fetched lazily on first request.
    """
    client_id = _need("DRIVE_OAUTH_CLIENT_ID")
    client_secret = _need("DRIVE_OAUTH_CLIENT_SECRET")
    refresh_token = _need("DRIVE_OAUTH_REFRESH_TOKEN")

    creds = Credentials(
        token=None,  # access token will be obtained via refresh flow
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=list(SCOPES),
    )

    # Proactively refresh once so downstream code fails fast with a clear error.
    try:
        Request()  # ensure import side-effects
        creds.refresh(Request())
    except Exception as e:
        print(
            "ERROR[OAUTH_REFRESH]: invalid OAuth refresh: "
            f"client_id={client_id[:4]}...apps.googleusercontent.com, "
            f"secret={'*'*8}, token={refresh_token[:8]}...; "
            f"reason={e}"
        )
        print(" - check: OAuth consent screen is 'In production' (not Testing)")
        print(" - fix: re-issue refresh token in OAuth Playground with BOTH scopes: "
              "Drive (drive.file) + Sheets (spreadsheets)")
        print(" - check: refresh token must match CURRENT client secret")
        print(" - check: APIs enabled in this project (Drive + Sheets)")
        sys.exit(2)

    return creds


def build_drive_service():
    """
    v3 Drive client with supportsAllDrives enabled in requests where applicable.
    """
    creds = _user_credentials()
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def build_sheets_service():
    """
    v4 Sheets client using the same user credentials.
    """
    creds = _user_credentials()
    return build("sheets", "v4", credentials=creds, cache_discovery=False)
