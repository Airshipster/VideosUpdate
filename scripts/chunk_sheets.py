# scripts/chunk_sheets.py
import os
import sys
from oauth_helper import build_drive_service, build_sheets_service

def need(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        print(f"ERROR[ENV_MISSING]: {name} is empty")
        sys.exit(2)
    return v

def main():
    CHUNKS_FOLDER_ID = need("CHUNKS_FOLDER_ID")
    SOURCE_SHEET_ID  = need("SOURCE_SHEET_ID")
    SOURCE_SHEET_TAB = need("SOURCE_SHEET_TAB")
    MAP_SHEET_TAB    = need("MAP_SHEET_TAB")

    try:
        drive = build_drive_service()
    except SystemExit:
        raise
    except Exception as e:
        print(f"ERROR[DRIVE_CLIENT]: {e}")
        sys.exit(2)

    try:
        sheets = build_sheets_service()
    except SystemExit:
        raise
    except Exception as e:
        print(f"ERROR[SHEETS_CLIENT]: {e}")
        sys.exit(2)

    try:
        meta = drive.files().get(
            fileId=CHUNKS_FOLDER_ID,
            fields="id,name,mimeType,driveId,parents,trashed",
            supportsAllDrives=True,
        ).execute()
    except Exception as e:
        print(f"ERROR[DRIVE_FOLDER_FETCH]: cannot read CHUNKS_FOLDER_ID='{CHUNKS_FOLDER_ID}'; ex:{e}")
        print("HINT: проверь, что это ID папки и у текущего аккаунта есть доступ (Editor).")
        sys.exit(2)

    if meta.get("mimeType") != "application/vnd.google-apps.folder":
        print(f"ERROR[DRIVE_FOLDER_TYPE]: ID '{CHUNKS_FOLDER_ID}' не папка, mimeType={meta.get('mimeType')}")
        sys.exit(2)

    if meta.get("trashed"):
        print("WARN[DRIVE_FOLDER_TRASHED]: папка помечена как удалённая (trashed=True).")

    print(f"OK[DRIVE]: folder '{meta.get('name')}' ({meta.get('id')}) доступна")

    try:
        _ = sheets.spreadsheets().values().get(
            spreadsheetId=SOURCE_SHEET_ID, range=f"{SOURCE_SHEET_TAB}!A1:Z5"
        ).execute()
        print(f"OK[SHEETS]: читается {SOURCE_SHEET_ID} лист '{SOURCE_SHEET_TAB}'")
    except Exception as e:
        print(f"ERROR[SHEETS_READ]: cannot read spreadsheet '{SOURCE_SHEET_ID}' range '{SOURCE_SHEET_TAB}!A1:Z5'; ex:{e}")
        print("HINT: добавь текущую учётку в доступ к таблице (Viewer/Editor).")
        sys.exit(2)

    print("AUTH_OK: доступ к Drive и Sheets подтверждён. Продолжаем дальнейшие шаги…")

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        print(f"ERROR[UNHANDLED]: {e}")
        sys.exit(3)
