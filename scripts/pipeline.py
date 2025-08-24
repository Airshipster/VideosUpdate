import os, io, sys, json, time, re, hashlib
import datetime as dt
from dateutil import tz, parser as dtparser
from typing import Dict, List, Tuple, Optional

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError

API_KEYS_RAW = os.getenv("YOUTUBE_API_KEYS","").strip()
API_KEYS = [x.strip() for x in API_KEYS_RAW.splitlines() if x.strip()]
DAILY_UNIT_BUDGET = int(os.getenv("DAILY_UNIT_BUDGET","9500") or "9500")
SOURCE_SHEET_ID = os.getenv("SOURCE_SHEET_ID","").strip()
SOURCE_SHEET_TAB = os.getenv("SOURCE_SHEET_TAB","").strip()
MAP_SHEET_TAB = os.getenv("MAP_SHEET_TAB","").strip()
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID","").strip()
RESCAN_INTERVAL_DAYS = int(os.getenv("RESCAN_INTERVAL_DAYS","1") or "1")
RUN_ANCHOR_LOCAL = os.getenv("RUN_ANCHOR_LOCAL","").strip()
RUN_TIME_LOCAL = os.getenv("RUN_TIME_LOCAL","13:00").strip()
PLAYLIST_LIMIT = int(os.getenv("PLAYLIST_LIMIT","0") or "0")

BAKU_TZ = tz.gettz("Asia/Baku")
YOUTUBE_ENDPOINT = "https://www.googleapis.com/youtube/v3"
SESSION = requests.Session()
KEY_IDX = 0
UNITS_USED = 0

LOCAL_OUT = "out_parquet"
LOCAL_TMP = "tmp"
STATE_NAME = "state.json"
TOMBSTONE_NAME = "tombstones.parquet"
DELTA_PREFIX = "videos_delta"

SHORTS_LIMIT_SEC = 182
WINDOW_DAYS = 365
WINDOW_MONTHS = 12
BUFFER_MONTHS = 2

os.makedirs(LOCAL_OUT, exist_ok=True)
os.makedirs(LOCAL_TMP, exist_ok=True)

def fail(code: str, detail: str, exit_code: int = 2):
    print(f"ERROR[{code}]: {detail}")
    sys.exit(exit_code)

def g_creds():
    try:
        path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        scopes = [
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive.readonly",
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/documents.readonly"
        ]
        return service_account.Credentials.from_service_account_file(path, scopes=scopes)
    except Exception:
        fail("GCP_SA", "service account json not loaded")

def build_svc(name, version):
    try:
        return build(name, version, credentials=g_creds(), cache_discovery=False)
    except Exception:
        fail("GOOGLE_SVC", f"{name} api init failed")

def parse_http_error(e: HttpError) -> Tuple[Optional[int], str]:
    try:
        status = getattr(e.resp, "status", None)
    except Exception:
        status = None
    msg = ""
    try:
        if hasattr(e, "content") and e.content:
            c = e.content
            if isinstance(c, bytes):
                c = c.decode("utf-8", "ignore")
            j = json.loads(c)
            msg = j.get("error", {}).get("message", "") or c[:200]
        else:
            msg = str(e)
    except Exception:
        msg = str(e)[:200]
    return status, msg

def sheets_get_range(spreadsheet_id: str, rng: str):
    try:
        svc = build_svc("sheets","v4")
        return svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    except HttpError as e:
        status, msg = parse_http_error(e)
        fail("SHEETS_ACCESS", f"{status} {rng} {msg}")
    except Exception:
        fail("SHEETS_ACCESS", "unexpected")

def drive_meta(file_id: str) -> Dict:
    try:
        svc = build_svc("drive","v3")
        return svc.files().get(fileId=file_id, fields="id,name,mimeType,driveId", supportsAllDrives=True).execute()
    except HttpError as e:
        status, msg = parse_http_error(e)
        if status == 404:
            fail("DRIVE_ID_NOT_FOUND", f"id={file_id}")
        fail("DRIVE_META", f"{status} {msg}")
    except Exception:
        fail("DRIVE_META", "unexpected")

def drive_upload(filepath: str, name: str, folder_id: str):
    try:
        svc = build_svc("drive","v3")
        media = MediaFileUpload(filepath, resumable=True)
        file_meta = {"name": name, "parents": [folder_id]}
        return svc.files().create(body=file_meta, media_body=media, fields="id", supportsAllDrives=True).execute()["id"]
    except HttpError as e:
        status, msg = parse_http_error(e)
        if status == 404:
            fail("DRIVE_ID_NOT_FOUND", f"id={folder_id}")
        if status == 403:
            fail("DRIVE_FORBIDDEN", msg or "forbidden")
        fail("DRIVE_UPLOAD", f"{status} {msg}")
    except Exception:
        fail("DRIVE_UPLOAD", "unexpected")

def drive_find_one_by_name(name: str, folder_id: str) -> Optional[str]:
    try:
        svc = build_svc("drive","v3")
        q = f"'{folder_id}' in parents and name = '{name}' and trashed = false"
        rsp = svc.files().list(q=q, spaces='drive', fields="files(id,name)", pageSize=1, includeItemsFromAllDrives=True, supportsAllDrives=True).execute()
        files = rsp.get("files",[])
        return files[0]["id"] if files else None
    except HttpError as e:
        status, msg = parse_http_error(e)
        if status == 404:
            fail("DRIVE_ID_NOT_FOUND", f"id={folder_id}")
        if status == 403:
            fail("DRIVE_FORBIDDEN", msg or "forbidden")
        fail("DRIVE_ACCESS", f"{status} {msg}")
    except Exception:
        fail("DRIVE_ACCESS", "unexpected")

def drive_download_to_file(file_id: str, dest_path: str):
    try:
        svc = build_svc("drive","v3")
        req = svc.files().get_media(fileId=file_id, supportsAllDrives=True)
        fh = io.FileIO(dest_path, 'wb')
        downloader = MediaIoBaseDownload(fh, req)
        done = False
        while not done:
            status, done = downloader.next_chunk()
    except HttpError as e:
        status, msg = parse_http_error(e)
        if status == 404:
            fail("DRIVE_STATE_NOT_FOUND", "state.json not found")
        fail("DRIVE_READ", f"{status} {msg}")
    except Exception:
        fail("DRIVE_READ", "unexpected")

def drive_delete(file_id: str):
    try:
        svc = build_svc("drive","v3")
        svc.files().delete(fileId=file_id, supportsAllDrives=True).execute()
    except Exception:
        pass

def drive_overwrite(name: str, folder_id: str, local_path: str):
    old_id = drive_find_one_by_name(name, folder_id)
    if old_id:
        try:
            drive_delete(old_id)
        except Exception:
            pass
    return drive_upload(local_path, name, folder_id)

def obf(s: str) -> str:
    if not s: return ""
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:8]

def log(msg: str):
    print(f"[{dt.datetime.utcnow().isoformat()}Z] {msg}")

def key() -> str:
    global KEY_IDX
    return API_KEYS[KEY_IDX % len(API_KEYS)]

def rotate_key():
    global KEY_IDX
    KEY_IDX += 1

def budget_left() -> int:
    return max(0, DAILY_UNIT_BUDGET - UNITS_USED)

def yt_get(path, params, cost_units=1):
    global UNITS_USED
    if budget_left() < cost_units:
        fail("QUOTA", "daily unit budget reached")
    for attempt in range(6):
        try:
            params2 = dict(params)
            params2["key"] = key()
            r = SESSION.get(f"{YOUTUBE_ENDPOINT}/{path}", params=params2, timeout=30)
            if r.status_code == 200:
                UNITS_USED += cost_units
                return r.json()
            if r.status_code in (403, 429, 503):
                rotate_key()
                time.sleep(min(60, 2**attempt))
                continue
            r.raise_for_status()
        except requests.RequestException:
            time.sleep(min(60, 2**attempt))
            continue
    fail("YOUTUBE_API", f"{path} request failed")

def iso8601_to_seconds(s: str) -> Optional[int]:
    if not s: return None
    m = re.fullmatch(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', s)
    if not m: return None
    h = int(m.group(1) or 0)
    mnt = int(m.group(2) or 0)
    sec = int(m.group(3) or 0)
    return h*3600 + mnt*60 + sec

def load_state() -> Dict:
    fid = drive_find_one_by_name(STATE_NAME, DRIVE_FOLDER_ID)
    if not fid:
        return {"last_gc_at": None, "playlists": {}}
    dest = os.path.join(LOCAL_TMP, STATE_NAME)
    drive_download_to_file(fid, dest)
    try:
        with open(dest,"r",encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        fail("STATE_PARSE", "state.json invalid")

def save_state(st: Dict):
    tmp = os.path.join(LOCAL_TMP, STATE_NAME)
    with open(tmp,"w",encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)
    drive_overwrite(STATE_NAME, DRIVE_FOLDER_ID, tmp)

def get_helper_maps() -> Tuple[Dict[str,str], Dict[str,str]]:
    rng = f"{MAP_SHEET_TAB}!A:H"
    rs = sheets_get_range(SOURCE_SHEET_ID, rng)
    values = rs.get("values", [])
    if not values:
        fail("HELPER_EMPTY", "helper sheet has no data")
    header_map = {}
    topic_ru_map = {}
    for row in values:
        if len(row) >= 2 and row[0] and row[1]:
            k = row[0].strip()
            v = row[1].strip()
            if k and v:
                header_map[k] = v
        if len(row) >= 8:
            url = row[6].strip() if len(row)>6 else ""
            ru  = row[7].strip() if len(row)>7 else ""
            if url and ru:
                topic_ru_map[url] = ru
    needed = ["relatedPlaylists.uploads","videoCount","topicCategories[]"]
    miss = [k for k in needed if k not in header_map]
    if miss:
        fail("HELPER_KEYS", f"missing keys in helper A:B: {','.join(miss)}")
    return header_map, topic_ru_map

def column_index_to_letter(i: int) -> str:
    s = ""
    i += 1
    while i > 0:
        i, r = divmod(i-1, 26)
        s = chr(65+r) + s
    return s

def read_baza_columns(header_map: Dict[str,str]) -> Tuple[List[str], List[Optional[str]], List[str]]:
    rs = sheets_get_range(SOURCE_SHEET_ID, f"{SOURCE_SHEET_TAB}!1:1")
    header = rs.get("values", [[]])[0]
    if not header:
        fail("BAZA_EMPTY", "baza header row is empty")
    key_uploads = header_map.get("relatedPlaylists.uploads","relatedPlaylists.uploads")
    key_vcount  = header_map.get("videoCount","videoCount")
    key_topics  = header_map.get("topicCategories[]","topicCategories[]")
    norm = {h.lower().strip():i for i,h in enumerate(header)}
    for k in [key_uploads, key_vcount, key_topics]:
        if k.lower().strip() not in norm:
            fail("HEADER_NOT_FOUND", f"'{k}' not found in Baza header")
    iu = norm[key_uploads.lower().strip()]
    iv = norm[key_vcount.lower().strip()]
    it = norm[key_topics.lower().strip()]
    lu = column_index_to_letter(iu)
    lv = column_index_to_letter(iv)
    lt = column_index_to_letter(it)
    try:
        col_u = sheets_get_range(SOURCE_SHEET_ID, f"{SOURCE_SHEET_TAB}!{lu}2:{lu}").get("values",[])
        col_v = sheets_get_range(SOURCE_SHEET_ID, f"{SOURCE_SHEET_TAB}!{lv}2:{lv}").get("values",[])
        col_t = sheets_get_range(SOURCE_SHEET_ID, f"{SOURCE_SHEET_TAB}!{lt}2:{lt}").get("values",[])
    except Exception:
        fail("SHEETS_READ", "failed reading Baza columns")
    n = max(len(col_u), len(col_v), len(col_t))
    uploads=[]; vcounts=[]; topics=[]
    for i in range(n):
        u = col_u[i][0].strip() if i<len(col_u) and col_u[i] else ""
        pid = u.split()[0] if u else ""
        uploads.append(pid if pid else "")
        vc = col_v[i][0].strip() if i<len(col_v) and col_v[i] else ""
        vcounts.append(vc if vc else None)
        tc = col_t[i][0].strip() if i<len(col_t) and col_t[i] else ""
        topics.append(tc)
    return uploads, vcounts, topics

def is_tv_channel(topic_cell_text: str) -> bool:
    if not topic_cell_text: return False
    return "телевизионные программы" in topic_cell_text.lower()

def over_10k_videos(vc: Optional[str]) -> bool:
    if not vc: return False
    try:
        return int(re.sub(r"[^\d]","",vc)) > 10000
    except Exception:
        return False

def list_playlist_video_ids_since(playlist_id: str, since_iso: str, stop_after: Optional[int]=None) -> List[Tuple[str,str]]:
    out=[]
    page=None
    while True:
        if budget_left() < 1: break
        js = yt_get("playlistItems",{
            "part":"contentDetails",
            "maxResults":50,
            "playlistId": playlist_id,
            **({"pageToken":page} if page else {})
        }, cost_units=1)
        items = js.get("items",[])
        if not items: break
        stop_here=False
        for it in items:
            vd = it["contentDetails"]["videoId"]
            vpa = it["contentDetails"].get("videoPublishedAt")
            if not vpa:
                continue
            if vpa >= since_iso:
                out.append((vd, vpa))
                if stop_after and len(out)>=stop_after:
                    return out
            else:
                stop_here=True
        if stop_here: break
        page = js.get("nextPageToken")
        if not page: break
    return out

FIELDS = ",".join([
  "items("
  "id,"
  "snippet(publishedAt,title,tags,categoryId,defaultLanguage,defaultAudioLanguage),"
  "contentDetails(duration,licensedContent),"
  "status(madeForKids,selfDeclaredMadeForKids),"
  "statistics(viewCount,likeCount,commentCount),"
  "topicDetails(topicCategories),"
  "paidProductPlacementDetails(hasPaidProductPlacement)"
  ")"
])

def fetch_videos(video_ids: List[str]) -> List[Dict]:
    out=[]
    for i in range(0,len(video_ids),50):
        if budget_left() < 1: break
        batch = ",".join(video_ids[i:i+50])
        js = yt_get("videos",{
            "part":"snippet,contentDetails,statistics,status,topicDetails,paidProductPlacementDetails",
            "id": batch,
            "fields": FIELDS
        }, cost_units=1)
        for it in js.get("items",[]):
            sn = it.get("snippet",{})
            cd = it.get("contentDetails",{})
            st = it.get("status",{})
            stat = it.get("statistics",{})
            td = it.get("topicDetails",{})
            pp = it.get("paidProductPlacementDetails",{})
            dur_sec = iso8601_to_seconds(cd.get("duration"))
            rec = {
              "videoId": it.get("id"),
              "publishedAt": sn.get("publishedAt"),
              "title": sn.get("title"),
              "tags": sn.get("tags",[]),
              "categoryId": sn.get("categoryId"),
              "defaultLanguage": sn.get("defaultLanguage"),
              "defaultAudioLanguage": sn.get("defaultAudioLanguage"),
              "duration": dur_sec,
              "licensedContent": cd.get("licensedContent"),
              "madeForKids": st.get("madeForKids"),
              "selfDeclaredMadeForKids": st.get("selfDeclaredMadeForKids"),
              "viewCount": stat.get("viewCount"),
              "likeCount": stat.get("likeCount"),
              "commentCount": stat.get("commentCount"),
              "topicCategories": td.get("topicCategories",[]),
              "hasPaidProductPlacement": pp.get("hasPaidProductPlacement", False),
            }
            out.append(rec)
    return out

def write_delta_records(records: List[Dict], playlist_id: str, topic_ru_map: Dict[str,str]):
    if not records: return []
    df = pd.DataFrame.from_records(records)
    df["isShorts"] = df["duration"].apply(lambda x: bool(x is not None and x <= SHORTS_LIMIT_SEC))
    df["isTombstoned"] = False
    df["tombstoneReason"] = None
    df["firstSeenAt"] = pd.Timestamp.utcnow()
    df["lastUpdatedAt"] = pd.Timestamp.utcnow()
    df["playlistId"] = playlist_id
    def map_topics(urls):
        if not isinstance(urls,list): return []
        ru=[]
        for u in urls:
            if u in topic_ru_map:
                ru.append(topic_ru_map[u])
        return ru
    df["topicCategories_ru"] = df["topicCategories"].apply(map_topics)
    df = df[~df["isShorts"].astype(bool)]
    if df.empty: return []
    df["publishedAt"] = pd.to_datetime(df["publishedAt"], utc=True, errors="coerce")
    df = df[df["publishedAt"].notna()]
    written=[]
    for key, part in df.groupby([df["publishedAt"].dt.year, df["publishedAt"].dt.month]):
        y=int(key[0]); m=int(key[1])
        part_dir = os.path.join(LOCAL_OUT, f"year={y:04d}", f"month={m:02d}")
        os.makedirs(part_dir, exist_ok=True)
        fname = f"{DELTA_PREFIX}_{y:04d}_{m:02d}_{int(time.time())}.parquet"
        dest = os.path.join(part_dir, fname)
        table = pa.Table.from_pandas(part, preserve_index=False)
        pq.write_table(table, dest, compression="zstd")
        written.append(dest)
    return written

def upload_folder_recursive(local_root: str, drive_folder_id: str):
    for root, _, files in os.walk(local_root):
        for f in files:
            full = os.path.join(root,f)
            rel = os.path.relpath(full, local_root).replace(os.sep,"__")
            name = f"{rel}"
            try:
                drive_upload(full, name, drive_folder_id)
            except SystemExit:
                raise
            except Exception:
                fail("DRIVE_UPLOAD", "upload failed")

def compact_month(year: int, month: int):
    part_dir = os.path.join(LOCAL_OUT, f"year={year:04d}", f"month={month:02d}")
    if not os.path.isdir(part_dir):
        return None
    files = [os.path.join(part_dir, f) for f in os.listdir(part_dir) if f.endswith(".parquet")]
    if not files: return None
    con = duckdb.connect()
    files_sql = ", ".join([f"read_parquet('{p}')" for p in files])
    q = f"""
    select * from (
        select *,
               row_number() over (partition by videoId order by lastUpdatedAt desc) as rn
        from ({files_sql})
    ) where rn=1
    """
    df = con.execute(q).df()
    con.close()
    compact_name = os.path.join(part_dir, f"videos_{year:04d}_{month:02d}_compact.parquet")
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), compact_name, compression="zstd")
    for p in files:
        try:
            if not p.endswith("_compact.parquet"):
                os.remove(p)
        except Exception:
            pass
    return compact_name

def append_tombstones(rows: List[Dict]):
    if not rows: return
    df = pd.DataFrame(rows)
    path = os.path.join(LOCAL_OUT, TOMBSTONE_NAME)
    if os.path.exists(path):
        old = pq.read_table(path).to_pandas()
        df = pd.concat([old, df], ignore_index=True)
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path, compression="zstd")

def recent_months_list(n: int):
    now = dt.datetime.utcnow()
    y = now.year
    m = now.month
    out=[]
    for _ in range(n):
        out.append((y,m))
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return out

def check_secrets():
    miss=[]
    if not API_KEYS: miss.append("YOUTUBE_API_KEYS")
    if not SOURCE_SHEET_ID: miss.append("SOURCE_SHEET_ID")
    if not SOURCE_SHEET_TAB: miss.append("SOURCE_SHEET_TAB")
    if not MAP_SHEET_TAB: miss.append("MAP_SHEET_TAB")
    if not DRIVE_FOLDER_ID: miss.append("DRIVE_FOLDER_ID")
    if miss: fail("MISSING_SECRET", ",".join(miss))

def check_drive_probe():
    meta = drive_meta(DRIVE_FOLDER_ID)
    mt = meta.get("mimeType","")
    if mt != "application/vnd.google-apps.folder":
        fail("DRIVE_ID_NOT_FOLDER", f"id={DRIVE_FOLDER_ID} mimeType={mt}")
    p = os.path.join(LOCAL_TMP, f"probe_{int(time.time())}.txt")
    with open(p,"w",encoding="utf-8") as f: f.write("ok")
    fid=None
    try:
        fid = drive_upload(p, os.path.basename(p), DRIVE_FOLDER_ID)
    except SystemExit:
        raise
    except Exception as e:
        fail("DRIVE_PROBE", "cannot write to DRIVE_FOLDER_ID")
    try:
        if fid: drive_delete(fid)
    except Exception:
        pass

def main():
    check_secrets()
    check_drive_probe()

    now_utc = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    since = now_utc - dt.timedelta(days=WINDOW_DAYS)
    since_iso = since.isoformat().replace("+00:00","Z")

    header_map, topic_ru_map = get_helper_maps()
    uploads, vcounts, topics = read_baza_columns(header_map)

    allowed=[]
    for pid, vc, tc in zip(uploads, vcounts, topics):
        if not pid: continue
        if over_10k_videos(vc): 
            continue
        if is_tv_channel(tc):
            continue
        allowed.append(pid)
    if PLAYLIST_LIMIT and PLAYLIST_LIMIT>0:
        allowed = allowed[:PLAYLIST_LIMIT]

    state = load_state()
    if "playlists" not in state: state["playlists"] = {}
    pl_state = state["playlists"]

    prev_pids = set(pl_state.keys())
    curr_pids = set(allowed)
    removed = list(prev_pids - curr_pids)
    trows=[]
    for rp in removed:
        trows.append({
            "videoId": None,
            "playlistId": rp,
            "tombstoneReason": "playlist_removed",
            "tombstonedAt": dt.datetime.utcnow().isoformat()+"Z"
        })
        st = pl_state.get(rp,{})
        st["present"] = False
        pl_state[rp]=st
    if trows:
        append_tombstones(trows)

    for pid in allowed:
        st = pl_state.get(pid, {})
        st["present"] = True
        pl_state[pid] = st

    for pid in allowed:
        if budget_left() < 2: break
        try:
            lst = list_playlist_video_ids_since(pid, since_iso)
        except Exception:
            fail("YOUTUBE_LIST", "playlistItems.list failed")
        if not lst: 
            st = pl_state.get(pid, {})
            st["last_scan_at"] = dt.datetime.utcnow().isoformat()+"Z"
            pl_state[pid] = st
            continue
        last_seen = pl_state.get(pid,{}).get("last_seen_publishedAt")
        if last_seen:
            lst = [x for x in lst if x[1] > last_seen]
        if not lst:
            st = pl_state.get(pid, {})
            st["last_scan_at"] = dt.datetime.utcnow().isoformat()+"Z"
            pl_state[pid] = st
            continue
        video_ids = [v for v,_ in lst]
        try:
            recs = fetch_videos(video_ids)
        except Exception:
            fail("YOUTUBE_VIDEOS", "videos.list failed")
        write_delta_records(recs, pid, topic_ru_map)
        max_vpa = max([vpa for _,vpa in lst])
        st = pl_state.get(pid, {})
        st["last_seen_publishedAt"] = max_vpa
        st["last_scan_at"] = dt.datetime.utcnow().isoformat()+"Z"
        pl_state[pid] = st
        if budget_left() < 2: break

    append_tombstones([{
        "videoId": None,
        "playlistId": None,
        "tombstoneReason": "out_of_window",
        "tombstonedAt": dt.datetime.utcnow().isoformat()+"Z"
    }])

    for pid in sorted(allowed, key=lambda x: pl_state.get(x,{}).get("last_update_scan_at") or "1970-01-01T00:00:00Z"):
        if budget_left() < 2: break
        try:
            lst = list_playlist_video_ids_since(pid, since_iso)
        except Exception:
            fail("YOUTUBE_LIST", "playlistItems.list failed")
        if not lst:
            st = pl_state.get(pid,{})
            st["last_update_scan_at"] = dt.datetime.utcnow().isoformat()+"Z"
            pl_state[pid]=st
            continue
        lst.sort(key=lambda tup: tup[1])
        vids=[]
        target_batches = max(1, min(10, budget_left()//2))
        for (vid, vpa) in lst:
            vids.append(vid)
            if len(vids)>=target_batches*50:
                break
        try:
            recs = fetch_videos(vids)
        except Exception:
            fail("YOUTUBE_VIDEOS", "videos.list failed")
        write_delta_records(recs, pid, topic_ru_map)
        st = pl_state.get(pid,{})
        st["last_update_scan_at"] = dt.datetime.utcnow().isoformat()+"Z"
        pl_state[pid]=st

    need_gc = False
    last_gc = state.get("last_gc_at")
    if last_gc:
        try:
            last_gc_dt = dtparser.isoparse(last_gc)
        except Exception:
            last_gc_dt = None
    else:
        last_gc_dt = None
    if not last_gc_dt:
        need_gc = True
    else:
        need_gc = (dt.datetime.utcnow() - last_gc_dt).days >= RESCAN_INTERVAL_DAYS
    if need_gc:
        now = dt.datetime.utcnow()
        months = []
        y = now.year; m = now.month
        for _ in range(WINDOW_MONTHS + BUFFER_MONTHS):
            months.append((y,m))
            m -= 1
            if m==0:
                m=12; y-=1
        seen=set()
        for y,m in months:
            if (y,m) in seen: continue
            seen.add((y,m))
            try:
                compact_month(int(y), int(m))
            except Exception:
                fail("COMPACT", "month compaction failed")
        state["last_gc_at"] = dt.datetime.utcnow().isoformat()+"Z"

    state["playlists"] = pl_state
    save_state(state)
    upload_folder_recursive(LOCAL_OUT, DRIVE_FOLDER_ID)
    log(f"audit: quota≈{UNITS_USED}, daily_budget={DAILY_UNIT_BUDGET}")
    log("finish")

if __name__ == "__main__":
    try:
        miss=[]
        if not API_KEYS: miss.append("YOUTUBE_API_KEYS")
        if not SOURCE_SHEET_ID: miss.append("SOURCE_SHEET_ID")
        if not SOURCE_SHEET_TAB: miss.append("SOURCE_SHEET_TAB")
        if not MAP_SHEET_TAB: miss.append("MAP_SHEET_TAB")
        if not DRIVE_FOLDER_ID: miss.append("DRIVE_FOLDER_ID")
        if miss: fail("MISSING_SECRET", ",".join(miss))
        main()
    except SystemExit:
        raise
    except Exception as e:
        t = type(e).__name__
        msg = str(e)[:200] if str(e) else ""
        print(f"ERROR[UNHANDLED]: {t} {msg}")
        sys.exit(3)
