# scripts/drive_debug.py
import os, json, sys, traceback
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

CLIENT_ID = os.environ.get("DRIVE_OAUTH_CLIENT_ID")
CLIENT_SECRET = os.environ.get("DRIVE_OAUTH_CLIENT_SECRET")
REFRESH_TOKEN = os.environ.get("DRIVE_OAUTH_REFRESH_TOKEN")
FOLDER_ID = os.environ.get("CHUNKS_FOLDER_ID") or os.environ.get("DRIVE_FOLDER_ID")

def build_drive():
    creds = Credentials(
        None,
        refresh_token=REFRESH_TOKEN,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token",
        scopes=["https://www.googleapis.com/auth/drive.file",
                "https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def main():
    print(f"DEBUG: CHUNKS_FOLDER_ID = {FOLDER_ID!r}")
    if not FOLDER_ID:
        print("DEBUG: Нет CHUNKS_FOLDER_ID в окружении. Проверь секреты/имя переменной.")
        sys.exit(2)

    drive = build_drive()

    # Кто мы такие (те же данные, что в probe)
    about = drive.about().get(fields="user(emailAddress,displayName),storageQuota").execute()
    print("DEBUG: AUTH USER =", json.dumps(about.get("user"), ensure_ascii=False, indent=2))

    # Прямая проверка папки с флагом supportsAllDrives
    try:
        meta = drive.files().get(
            fileId=FOLDER_ID,
            fields="id,name,mimeType,parents,driveId,trashed",
            supportsAllDrives=True,
        ).execute()
        print("DEBUG: FOLDER META =", json.dumps(meta, ensure_ascii=False, indent=2))
        if meta.get("mimeType") != "application/vnd.google-apps.folder":
            print("DEBUG: Этот ID не папка (mimeType=", meta.get("mimeType"), ")")
            sys.exit(2)
        if meta.get("trashed"):
            print("DEBUG: Папка в корзине (trashed=True). В таком состоянии get иногда бьёт 404 в разных вызовах.")
        print("DEBUG: OK: Папка доступна по этому токену.")
        sys.exit(0)
    except Exception as e:
        print("DEBUG: EXCEPTION while files.get()")
        traceback.print_exc()
        # Дополнительно пробуем list() / parents
        try:
            resp = drive.files().list(
                q="mimeType='application/vnd.google-apps.folder' and trashed=false",
                fields="files(id,name)",
                pageSize=10,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                corpora="allDrives",
            ).execute()
            print("DEBUG: Пример видимых папок:", json.dumps(resp, ensure_ascii=False, indent=2))
        except Exception as e2:
            print("DEBUG: Доп. list() тоже упал:")
            traceback.print_exc()
        sys.exit(2)

if __name__ == "__main__":
    main()
