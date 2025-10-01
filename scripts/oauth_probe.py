# scripts/oauth_probe.py
# Small diagnostic to confirm the OAuth refresh token works and Drive/Sheets are reachable.

import sys, json
from googleapiclient.errors import HttpError
from oauth_helper import build_drive_service, build_sheets_service

def main():
    # 1) Try exchanging refresh->access implicitly by building clients
    print("OAUTH_PROBE: requesting access_token via refresh_token (no scope)...")
    try:
        # Build services (this performs a refresh under the hood)
        drive = build_drive_service()
        sheets = build_sheets_service()
        print("OAUTH_PROBE: status = 200")
    except HttpError as e:
        try:
            body = e.content.decode() if isinstance(e.content, bytes) else (e.content or "")
        except Exception:
            body = str(e)
        print("OAUTH_PROBE: ERROR", getattr(e.resp, "status", None), body[:400])
        sys.exit(2)
    except Exception as e:
        print("OAUTH_PROBE: ERROR", type(e).__name__, str(e)[:400])
        sys.exit(2)

    # 2) Call a harmless Drive endpoint to verify scope drive.file or drive
    try:
        about = drive.about().get(fields="user,storageQuota").execute()
        print("DRIVE about.get status = 200")
        # Trim noisy fields before printing
        safe = {
            "user": {
                "kind": about.get("user", {}).get("kind"),
                "displayName": about.get("user", {}).get("displayName"),
                "photoLink": about.get("user", {}).get("photoLink"),
                "me": about.get("user", {}).get("me"),
                "permissionId": about.get("user", {}).get("permissionId"),
                "emailAddress": about.get("user", {}).get("emailAddress"),
            },
            "storageQuota": about.get("storageQuota"),
        }
        print("DRIVE about.get =", json.dumps(safe, ensure_ascii=False, indent=2))
    except HttpError as e:
        print("DRIVE about.get ERROR", getattr(e.resp, "status", None))
        sys.exit(2)

    print("OAUTH_PROBE: OK")

if __name__ == "__main__":
    main()
