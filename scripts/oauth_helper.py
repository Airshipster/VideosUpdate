# scripts/oauth_helper.py
import os
import sys
import json
import requests
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials

TOKEN_URL = "https://oauth2.googleapis.com/token"

def _need(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        print(f"ERROR[SECRET_MISSING]: {name} is empty")
        sys.exit(2)
    return v

def refresh_access_token():
    """
    Делаем ЧИСТЫЙ refresh: grant_type=refresh_token, БЕЗ scope.
    Если Google вернёт ошибку — печатаем детальный JSON и падаем.
    """
    cid  = _need("DRIVE_OAUTH_CLIENT_ID")
    csec = _need("DRIVE_OAUTH_CLIENT_SECRET")
    rtok = _need("DRIVE_OAUTH_REFRESH_TOKEN")

    data = {
        "client_id": cid,
        "client_secret": csec,
        "refresh_token": rtok,
        "grant_type": "refresh_token",
    }

    r = requests.post(TOKEN_URL, data=data, timeout=30)
    try:
        j = r.json()
    except Exception:
        print("ERROR[OAUTH_REFRESH_RAW]:", r.text[:4000])
        sys.exit(2)

    if r.status_code != 200:
        print("ERROR[OAUTH_REFRESH]: status", r.status_code, "body:", json.dumps(j, ensure_ascii=False))
        err = j.get("error")
        if err == "invalid_scope":
            print(
                "HINT: refresh-токен выписан под другими scope ИЛИ клиент/секрет не совпадает.\n"
                "      Перевыпусти в OAuth Playground сразу с ДВУМЯ scope:\n"
                "      https://www.googleapis.com/auth/drive.file и https://www.googleapis.com/auth/spreadsheets\n"
                "      и обязательно используй Web Client с редиректом https://developers.google.com/oauthplayground"
            )
        sys.exit(2)

    return j["access_token"], j.get("scope", ""), int(j.get("expires_in", 0))

def build_drive_service():
    at, sc, exp = refresh_access_token()
    # В googleapiclient достаточно передать одноразовый токен — авторизации хватит на ~1 час
    creds = Credentials(token=at)  # без auto-refresh, но нам этого достаточно для шага
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def build_sheets_service():
    at, sc, exp = refresh_access_token()
    creds = Credentials(token=at)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)
