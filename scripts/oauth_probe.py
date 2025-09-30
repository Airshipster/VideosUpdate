import os, json, sys, requests

def getenv(name):
    v = os.getenv(name, "").strip()
    if not v:
        print(f"ERROR[SECRET_MISSING]: {name} is empty")
        sys.exit(2)
    return v

cid = getenv("DRIVE_OAUTH_CLIENT_ID")
csec = getenv("DRIVE_OAUTH_CLIENT_SECRET")
rtok = getenv("DRIVE_OAUTH_REFRESH_TOKEN")

token_url = "https://oauth2.googleapis.com/token"
data = {
    "client_id": cid,
    "client_secret": csec,
    "refresh_token": rtok,
    "grant_type": "refresh_token",
}

print("OAUTH_PROBE: requesting access_token via refresh_token (no scope)...")
r = requests.post(token_url, data=data, timeout=30)

def mask(s, keep=6):
    if not s: return s
    if len(s) <= keep: return "*"*len(s)
    return s[:keep] + "*"*(len(s)-keep)

print("OAUTH_PROBE: status =", r.status_code)
try:
    j = r.json()
except Exception:
    print("OAUTH_PROBE: raw =", r.text[:4000])
    sys.exit(2)

safe = dict(j)
if "access_token" in safe: safe["access_token"] = mask(safe["access_token"], 12)
if "refresh_token" in safe: safe["refresh_token"] = mask(safe["refresh_token"], 8)
print("OAUTH_PROBE: body =", json.dumps(safe, ensure_ascii=False, indent=2))

if r.status_code != 200:
    err = safe.get("error")
    desc = safe.get("error_description")
    if err == "invalid_scope":
        print("HINT: refresh-токен выписан с другими scope или клиент/секрет не совпадает с тем, чем он был выдан.")
        print("HINT: перевыпусти токен в OAuth Playground c ОДНИМИ запроса́ми одновременно:")
        print("      https://www.googleapis.com/auth/drive.file  И  https://www.googleapis.com/auth/spreadsheets")
        print("      и обязательно используй Web Client с редиректом https://developers.google.com/oauthplayground")
    sys.exit(2)

atk = j.get("access_token")
sc = j.get("scope")
print("OAUTH_PROBE: granted scopes =", sc)

headers = {"Authorization": f"Bearer {atk}"}
about = requests.get("https://www.googleapis.com/drive/v3/about?fields=user,storageQuota", headers=headers, timeout=30)
print("DRIVE about.get status =", about.status_code)
try:
    ab = about.json()
except Exception:
    print("DRIVE about.get raw =", about.text[:2000])
    sys.exit(2)

print("DRIVE about.get =", json.dumps(ab, ensure_ascii=False, indent=2))
print("OAUTH_PROBE: OK")
