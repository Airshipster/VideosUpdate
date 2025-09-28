import os, sys, json, time, re, io
import datetime as dt
from dateutil import tz
from typing import Dict, List
import requests
import pandas as pd
from google.oauth2.service_account import Credentials as SACreds
from google.oauth2.credentials import Credentials as UserCreds
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

API_KEYS=[x.strip() for x in os.getenv("YOUTUBE_API_KEYS","").splitlines() if x.strip()]
SOURCE_SHEET_ID=os.getenv("SOURCE_SHEET_ID","").strip()
SOURCE_SHEET_TAB=os.getenv("SOURCE_SHEET_TAB","").strip()
MAP_SHEET_TAB=os.getenv("MAP_SHEET_TAB","").strip()
CHUNKS_FOLDER_ID=os.getenv("CHUNKS_FOLDER_ID","").strip()
PLAYLIST_LIMIT=int(os.getenv("PLAYLIST_LIMIT","5") or "5")
ROWS_PER_DOC=int(os.getenv("ROWS_PER_DOC","20000") or "20000")

DRIVE_OAUTH_CLIENT_ID=os.getenv("DRIVE_OAUTH_CLIENT_ID","").strip()
DRIVE_OAUTH_CLIENT_SECRET=os.getenv("DRIVE_OAUTH_CLIENT_SECRET","").strip()
DRIVE_OAUTH_REFRESH_TOKEN=os.getenv("DRIVE_OAUTH_REFRESH_TOKEN","").strip()

BAKU_TZ=tz.gettz("Asia/Baku")
WINDOW_DAYS=365
SHORTS_LIMIT=182
SESSION=requests.Session()
YOUTUBE_ENDPOINT="https://www.googleapis.com/youtube/v3"
KEY_IDX=0
SCOPES_USER=["https://www.googleapis.com/auth/drive","https://www.googleapis.com/auth/spreadsheets"]

def fail(code,msg,ec=2):
    print(f"ERROR[{code}]: {msg}")
    sys.exit(ec)

def mask(s):
    if not s: return ""
    if len(s)<=8: return s[0:2]+"*"*(len(s)-4)+s[-2:]
    return s[0:4]+"*"*(len(s)-8)+s[-4:]

def user_creds():
    if not (DRIVE_OAUTH_CLIENT_ID and DRIVE_OAUTH_CLIENT_SECRET and DRIVE_OAUTH_REFRESH_TOKEN):
        fail("MISSING_OAUTH","one of DRIVE_OAUTH_CLIENT_ID/SECRET/REFRESH_TOKEN is empty")
    creds=UserCreds(token=None,refresh_token=DRIVE_OAUTH_REFRESH_TOKEN,token_uri="https://oauth2.googleapis.com/token",
                    client_id=DRIVE_OAUTH_CLIENT_ID,client_secret=DRIVE_OAUTH_CLIENT_SECRET,scopes=SCOPES_USER)
    try:
        creds.refresh(Request())
    except RefreshError as e:
        msg=str(e)
        hints=[]
        hints.append("check: OAuth consent screen status — if Testing, refresh tokens expire in 7 days")
        hints.append("fix: re-issue refresh token in OAuth Playground with BOTH scopes: drive + spreadsheets")
        hints.append("check: refresh token must match CURRENT client secret (if secret was reset, old token breaks)")
        hints.append("check: same OAuth project as your Client ID in secrets; APIs enabled (Drive + Sheets)")
        diag=f"invalid OAuth refresh: client_id={mask(DRIVE_OAUTH_CLIENT_ID)}, secret={mask(DRIVE_OAUTH_CLIENT_SECRET)}, token={mask(DRIVE_OAUTH_REFRESH_TOKEN)}; reason={msg}"
        fail("OAUTH_REFRESH", diag+"\n"+"\n".join(" - "+h for h in hints))
    except Exception as e:
        fail("OAUTH_PRECHECK", f"{type(e).__name__}: {str(e)[:200]}")
    return creds

def sa_creds(scopes):
    path=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not path: fail("MISSING_SA","GOOGLE_APPLICATION_CREDENTIALS not set")
    try:
        return SACreds.from_service_account_file(path,scopes=scopes)
    except Exception as e:
        fail("SA_PARSE", f"{type(e).__name__}: {str(e)[:200]}")

def build_sheets_user(): return build("sheets","v4",credentials=user_creds(),cache_discovery=False)
def build_sheets_sa(): return build("sheets","v4",credentials=sa_creds(["https://www.googleapis.com/auth/spreadsheets.readonly"]),cache_discovery=False)
def build_drive_user(): return build("drive","v3",credentials=user_creds(),cache_discovery=False)

def parse_http(e:HttpError):
    try: s=getattr(e.resp,"status",None)
    except: s=None
    try:
        c=e.content.decode("utf-8","ignore") if isinstance(e.content,bytes) else (e.content or "")
        j=json.loads(c) if c else {}
        m=j.get("error",{}).get("message","") or (str(e)[:200] if str(e) else "")
    except: m=str(e)[:200]
    return s,m

def a1(sheet,rng):
    if not (sheet.startswith("'") and sheet.endswith("'")): sheet=f"'{sheet}'"
    return f"{sheet}!{rng}"

def sheets_get_values_sa(rng):
    try:
        svc=build_sheets_sa()
        return svc.spreadsheets().values().get(spreadsheetId=SOURCE_SHEET_ID,range=rng).execute().get("values",[])
    except HttpError as e:
        s,m=parse_http(e); fail("SHEETS_SA",f"{s} {rng} {m}")
    except Exception as e: fail("SHEETS_SA",f"{type(e).__name__}: {str(e)[:200]}")

def sheets_values_get_user(spreadsheet_id,rng):
    try:
        svc=build_sheets_user()
        return svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id,range=rng).execute().get("values",[])
    except HttpError as e:
        s,m=parse_http(e); fail("SHEETS_USER_GET",f"{s} {spreadsheet_id} {rng} {m}")
    except Exception as e: fail("SHEETS_USER_GET",f"{type(e).__name__}: {str(e)[:200]}")

def sheets_values_batch_update_user(spreadsheet_id,data,value_input_option="RAW"):
    try:
        svc=build_sheets_user()
        body={"valueInputOption":value_input_option,"data":data}
        return svc.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id,body=body).execute()
    except HttpError as e:
        s,m=parse_http(e); fail("SHEETS_USER_WRITE",f"{s} {spreadsheet_id} {m}")
    except Exception as e: fail("SHEETS_USER_WRITE",f"{type(e).__name__}: {str(e)[:200]}")

def sheets_values_append_user(spreadsheet_id,rng,values):
    try:
        svc=build_sheets_user()
        body={"values":values}
        return svc.spreadsheets().values().append(spreadsheetId=spreadsheet_id,range=rng,valueInputOption="RAW",insertDataOption="INSERT_ROWS",body=body).execute()
    except HttpError as e:
        s,m=parse_http(e); fail("SHEETS_USER_APPEND",f"{s} {spreadsheet_id} {m}")
    except Exception as e: fail("SHEETS_USER_APPEND",f"{type(e).__name__}: {str(e)[:200]}")

def drive_create_sheet_in_folder(name,folder_id):
    try:
        svc=build_drive_user()
        meta={"name":name,"mimeType":"application/vnd.google-apps.spreadsheet","parents":[folder_id]}
        f=svc.files().create(body=meta,fields="id",supportsAllDrives=True).execute()
        return f["id"]
    except HttpError as e:
        s,m=parse_http(e); fail("DRIVE_CREATE",f"{s} {m}")
    except Exception as e: fail("DRIVE_CREATE",f"{type(e).__name__}: {str(e)[:200]}")

def drive_find_file_by_name(name,folder_id):
    try:
        svc=build_drive_user()
        q=f"'{folder_id}' in parents and name = '{name}' and trashed = false"
        r=svc.files().list(q=q,spaces="drive",fields="files(id,name)",pageSize=1,includeItemsFromAllDrives=True,supportsAllDrives=True).execute()
        files=r.get("files",[])
        return files[0]["id"] if files else None
    except HttpError as e:
        s,m=parse_http(e); fail("DRIVE_SEARCH",f"{s} {m}")
    except Exception as e:
        fail("DRIVE_SEARCH",f"{type(e).__name__}: {str(e)[:200]}")

def drive_download_text(file_id):
    try:
        svc=build_drive_user()
        req=svc.files().get_media(fileId=file_id,supportsAllDrives=True)
        buf=io.BytesIO()
        from googleapiclient.http import MediaIoBaseDownload
        dl=MediaIoBaseDownload(buf,req)
        done=False
        while not done:
            status,done=dl.next_chunk()
        buf.seek(0)
        return buf.read().decode("utf-8")
    except HttpError as e:
        s,m=parse_http(e); fail("DRIVE_READ",f"{s} {m}")
    except Exception as e: fail("DRIVE_READ",f"{type(e).__name__}: {str(e)[:200]}")

def drive_overwrite_text(name,folder_id,text):
    try:
        svc=build_drive_user()
        old_id=drive_find_file_by_name(name,folder_id)
        from googleapiclient.http import MediaIoBaseUpload
        media=MediaIoBaseUpload(io.BytesIO(text.encode("utf-8")),mimetype="text/plain",resumable=True)
        meta={"name":name,"parents":[folder_id]}
        if old_id: svc.files().delete(fileId=old_id,supportsAllDrives=True).execute()
        f=svc.files().create(body=meta,media_body=media,fields="id",supportsAllDrives=True).execute()
        return f["id"]
    except HttpError as e:
        s,m=parse_http(e); fail("DRIVE_WRITE",f"{s} {m}")
    except Exception as e: fail("DRIVE_WRITE",f"{type(e).__name__}: {str(e)[:200]}")

def drive_validate_folder(folder_id):
    try:
        svc=build_drive_user()
        meta=svc.files().get(fileId=folder_id,fields="id,name,mimeType,trashed,driveId",supportsAllDrives=True).execute()
        if meta.get("trashed"): fail("DRIVE_FOLDER","folder is trashed")
        if meta.get("mimeType")!="application/vnd.google-apps.folder": fail("DRIVE_FOLDER","not a folder id")
        return meta.get("name","")
    except HttpError as e:
        s,m=parse_http(e); fail("DRIVE_FOLDER",f"{s} {m}")
    except Exception as e:
        fail("DRIVE_FOLDER",f"{type(e).__name__}: {str(e)[:200]}")

def a1(sheet,rng):
    if not (sheet.startswith("'") and sheet.endswith("'")): sheet=f"'{sheet}'"
    return f"{sheet}!{rng}"

def get_helper_maps():
    vals=sheets_get_values_sa(a1(MAP_SHEET_TAB,"A:H"))
    header_map={}
    topic_ru={}
    for row in vals:
        k=(row[0] if len(row)>=1 else "").strip().lower()
        v=(row[1] if len(row)>=2 else "")
        if k: header_map[k]=v
        if len(row)>=8:
            url=(row[6] or "").strip()
            ru=(row[7] or "").strip()
            if url and ru: topic_ru[url]=ru
    need=["relatedplaylists.uploads","videocount","topiccategories[]","title"]
    miss=[x for x in need if x not in header_map]
    if miss: fail("HELPER_KEYS",",".join(miss))
    return header_map,topic_ru

def find_header_indices(header,names):
    idx={}; low={h.strip().lower():i for i,h in enumerate(header)}
    for n in names:
        k=n.strip().lower()
        if k not in low: fail("HEADER_NOT_FOUND",n)
        idx[n]=low[k]
    return idx

def get_baza_columns(header_map):
    hdr=sheets_get_values_sa(a1(SOURCE_SHEET_TAB,"1:1"))
    header=hdr[0] if hdr else []
    if not header: fail("BAZA_EMPTY","no header")
    pl_name=header_map["relatedplaylists.uploads"]
    vc_name=header_map["videocount"]
    tc_name=header_map["topiccategories[]"]
    tt_name=header_map["title"]
    names=[pl_name,vc_name,tc_name,tt_name]
    idx=find_header_indices(header,names)
    def col_letter(i):
        s=""; i+=1
        while i>0:
            i,r=divmod(i-1,26)
            s=chr(65+r)+s
        return s
    iu=idx[pl_name]; iv=idx[vc_name]; it=idx[tc_name]; ititle=idx[tt_name]
    lu=col_letter(iu); lv=col_letter(iv); lt=col_letter(it); ltitle=col_letter(ititle)
    cu=sheets_get_values_sa(a1(SOURCE_SHEET_TAB,f"{lu}2:{lu}"))
    cv=sheets_get_values_sa(a1(SOURCE_SHEET_TAB,f"{lv}2:{lv}"))
    ct=sheets_get_values_sa(a1(SOURCE_SHEET_TAB,f"{lt}2:{lt}"))
    ctitle=sheets_get_values_sa(a1(SOURCE_SHEET_TAB,f"{ltitle}2:{ltitle}"))
    n=max(len(cu),len(cv),len(ct),len(ctitle))
    uploads=[]; vcounts=[]; topics=[]; titles=[]
    for i in range(n):
        u=cu[i][0].strip() if i<len(cu) and cu[i] else ""
        pid=u.split()[0] if u else ""
        uploads.append(pid)
        vv=cv[i][0].strip() if i<len(cv) and cv[i] else ""
        vcounts.append(vv if vv else None)
        tt=ct[i][0].strip() if i<len(ct) and ct[i] else ""
        topics.append(tt)
        tval=ctitle[i][0].strip() if i<len(ctitle) and ctitle[i] else ""
        titles.append(tval)
    return uploads,vcounts,topics,titles

def is_tv(topic_cell_text): return "телевизионные программы" in (topic_cell_text or "").strip().lower()

def over_10k(vc):
    if not vc: return False
    try:
        x=int(re.sub(r"[^\d]","",vc)); return x>10000
    except: return False

def key():
    global KEY_IDX
    return API_KEYS[KEY_IDX%len(API_KEYS)]

def rotate_key():
    global KEY_IDX
    KEY_IDX+=1

def yt_get(path,params,cost=1):
    for attempt in range(6):
        try:
            p=dict(params); p["key"]=key()
            r=SESSION.get(f"{YOUTUBE_ENDPOINT}/{path}",params=p,timeout=30)
            if r.status_code==200: return r.json()
            if r.status_code in (403,429,503):
                rotate_key(); time.sleep(min(60,2**attempt)); continue
            r.raise_for_status()
        except requests.RequestException:
            time.sleep(min(60,2**attempt)); continue
    fail("YOUTUBE_API",path)

def iso_to_sec(s):
    if not s: return None
    m=re.fullmatch(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?',s)
    if not m: return None
    h=int(m.group(1) or 0); mnt=int(m.group(2) or 0); sec=int(m.group(3) or 0)
    return h*3600+mnt*60+sec

def list_since(playlist_id,since_iso,limit_annual=1001):
    out=[]; page=None
    while True:
        js=yt_get("playlistItems",{"part":"contentDetails","maxResults":50,"playlistId":playlist_id,**({"pageToken":page} if page else {})},1)
        items=js.get("items",[])
        if not items: break
        stop=False
        for it in items:
            v=it["contentDetails"]["videoId"]
            pa=it["contentDetails"].get("videoPublishedAt")
            if not pa: continue
            if pa>=since_iso:
                out.append(v)
                if len(out)>=limit_annual: return []
            else:
                stop=True
        if stop: break
        page=js.get("nextPageToken")
        if not page: break
    return out

FIELDS=",".join(["items(id,","snippet(publishedAt,title,tags,categoryId,defaultLanguage,defaultAudioLanguage),","contentDetails(duration,licensedContent),","status(madeForKids,selfDeclaredMadeForKids),","statistics(viewCount,likeCount,commentCount),","topicDetails(topicCategories),","paidProductPlacementDetails(hasPaidProductPlacement))"])

def fetch_videos(ids):
    out=[]
    for i in range(0,len(ids),50):
        batch=",".join(ids[i:i+50])
        js=yt_get("videos",{"part":"snippet,contentDetails,statistics,status,topicDetails,paidProductPlacementDetails","id":batch,"fields":FIELDS},1)
        for it in js.get("items",[]):
            sn=it.get("snippet",{}); cd=it.get("contentDetails",{}); st=it.get("status",{}); stt=it.get("statistics",{}); td=it.get("topicDetails",{}); pp=it.get("paidProductPlacementDetails",{})
            dur=iso_to_sec(cd.get("duration"))
            rec={"videoId":it.get("id"),"publishedAt":sn.get("publishedAt"),"title":sn.get("title"),"tags":sn.get("tags",[]),"categoryId":sn.get("categoryId"),"defaultLanguage":sn.get("defaultLanguage"),"defaultAudioLanguage":sn.get("defaultAudioLanguage"),"duration_s":dur,"licensedContent":cd.get("licensedContent"),"madeForKids":st.get("madeForKids"),"selfDeclaredMadeForKids":st.get("selfDeclaredMadeForKids"),"viewCount":stt.get("viewCount"),"likeCount":stt.get("likeCount"),"commentCount":stt.get("commentCount"),"topicCategories":td.get("topicCategories",[]),"hasPaidProductPlacement":pp.get("hasPaidProductPlacement",False)}
            out.append(rec)
    return out

def fmt_baku(iso_str):
    if not iso_str: return ""
    dt_utc=dt.datetime.fromisoformat(iso_str.replace("Z","+00:00"))
    dt_loc=dt_utc.astimezone(BAKU_TZ)
    return dt_loc.strftime("%d.%m.%Y %H:%M:%S")

HEADERS=["videoId","playlistId","channelTitle","publishedAt","title","duration_s","isShorts","viewCount","likeCount","commentCount","categoryId","defaultLanguage","topicCategories_ru","hasPaidProductPlacement","firstSeenAt","lastUpdatedAt","isTombstoned","tombstoneReason"]
INDEX_HEADERS=["playlistId","docId","docName","lastScanAt","rowsInDoc"]

def ensure_tab(spreadsheet_id,tab_name):
    svc=build_sheets_user()
    meta=svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets=meta.get("sheets",[])
    ids={sh["properties"]["title"]:sh["properties"]["sheetId"] for sh in sheets}
    if tab_name in ids: return
    if len(ids)==1 and "Sheet1" in ids:
        req={"requests":[{"updateSheetProperties":{"properties":{"sheetId":ids["Sheet1"],"title":tab_name},"fields":"title"}}]}
        svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,body=req).execute()
    else:
        req={"requests":[{"addSheet":{"properties":{"title":tab_name}}}]}
        svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,body=req).execute()

def ensure_header(spreadsheet_id,tab_name="videos"):
    ensure_tab(spreadsheet_id,tab_name)
    vals=sheets_values_get_user(spreadsheet_id,a1(tab_name,"1:1"))
    if not vals or not vals[0]:
        sheets_values_batch_update_user(spreadsheet_id,[{"range":a1(tab_name,"A1:R1"),"values":[HEADERS]}]); return
    cur=vals[0]; need=HEADERS
    if len(cur)<len(need) or cur[:len(need)]!=need:
        sheets_values_batch_update_user(spreadsheet_id,[{"range":a1(tab_name,"A1:R1"),"values":[need]}])

def ensure_index_sheet():
    name="VideosIndex"
    fid=drive_find_file_by_name(name,CHUNKS_FOLDER_ID)
    if not fid:
        fid=drive_create_sheet_in_folder(name,CHUNKS_FOLDER_ID)
    ensure_tab(fid,"index")
    vals=sheets_values_get_user(fid,a1("index","1:1"))
    if not vals or not vals[0]:
        sheets_values_batch_update_user(fid,[{"range":a1("index","A1:E1"),"values":[INDEX_HEADERS]}])
    return fid

def read_existing_video_ids(spreadsheet_id,tab_name="videos"):
    out={}
    col=sheets_values_get_user(spreadsheet_id,a1(tab_name,"A2:A"))
    for i,row in enumerate(col,start=2):
        if row and row[0]: out[row[0]]=i
    return out

def read_index_map(index_id):
    out={}
    col=sheets_values_get_user(index_id,a1("index","A2:A"))
    for i,row in enumerate(col,start=2):
        if row and row[0]: out[row[0]]=i
    return out

def batch_update_rows(spreadsheet_id,updates):
    data=[]
    for row_idx,vals in updates:
        rng=f"A{row_idx}:R{row_idx}"
        data.append({"range":a1("videos",rng),"values":[vals]})
    if data: sheets_values_batch_update_user(spreadsheet_id,data)

def append_rows(spreadsheet_id,rows):
    if rows: sheets_values_append_user(spreadsheet_id,a1("videos","A1"),rows)

def update_index(index_id,items):
    now=dt.datetime.now(BAKU_TZ).strftime("%d.%m.%Y %H:%M:%S")
    existing=read_index_map(index_id)
    updates=[]; appends=[]
    for it in items:
        row=[it["playlistId"],it["docId"],it["docName"],now,str(it.get("rowsInDoc",""))]
        if it["playlistId"] in existing:
            idx=existing[it["playlistId"]]
            updates.append((idx,row))
        else:
            appends.append(row)
    data=[]
    for row_idx,vals in updates:
        data.append({"range":a1("index",f"A{row_idx}:E{row_idx}"),"values":[vals]})
    if data: sheets_values_batch_update_user(index_id,data)
    if appends: sheets_values_append_user(index_id,a1("index","A1"),appends)

def load_state():
    name="chunks_state.json"
    fid=drive_find_file_by_name(name,CHUNKS_FOLDER_ID)
    if not fid:
        st={"docs":[],"playlist_to_doc":{}}
        drive_overwrite_text(name,CHUNKS_FOLDER_ID,json.dumps(st,ensure_ascii=False))
        return st
    text=drive_download_text(fid)
    try: return json.loads(text)
    except: fail("STATE_PARSE","invalid json")

def save_state(st): drive_overwrite_text("chunks_state.json",CHUNKS_FOLDER_ID,json.dumps(st,ensure_ascii=False,indent=2))

def pick_doc_for_playlist(st,playlist_id):
    if playlist_id in st["playlist_to_doc"]: return st["playlist_to_doc"][playlist_id]
    docs=sorted(st["docs"],key=lambda x:x.get("rows",0))
    for d in docs:
        if d.get("rows",0)<ROWS_PER_DOC:
            st["playlist_to_doc"][playlist_id]=d["id"]; return d["id"]
    name=f"VideosChunk_{len(st['docs'])+1:04d}"
    sid=drive_create_sheet_in_folder(name,CHUNKS_FOLDER_ID)
    st["docs"].append({"id":sid,"name":name,"rows":0})
    ensure_header(sid)
    st["playlist_to_doc"][playlist_id]=sid
    return sid

def process_playlist(st,playlist_id,channel_title,since_iso,topic_ru_map,index_id):
    vid_ids=list_since(playlist_id,since_iso,1001)
    if not vid_ids:
        print(f"SKIP[ANNUAL_LIMIT_OR_EMPTY]: {playlist_id}")
        return
    recs=fetch_videos(vid_ids)
    rows=[]
    now_loc=dt.datetime.now(BAKU_TZ).strftime("%d.%m.%Y %H:%M:%S")
    for r in recs:
        d=r.get("duration_s")
        is_short=bool(d is not None and d<=SHORTS_LIMIT)
        if is_short: continue
        tlist=r.get("topicCategories",[])
        ru=[]
        for u in tlist:
            if u in topic_ru_map: ru.append(topic_ru_map[u])
        row=[r.get("videoId") or "",playlist_id,channel_title or "",fmt_baku(r.get("publishedAt") or ""),r.get("title") or "",d if d is not None else "","TRUE" if is_short else "FALSE",r.get("viewCount") or "",r.get("likeCount") or "",r.get("commentCount") or "",r.get("categoryId") or "",r.get("defaultLanguage") or "",", ".join(ru),"TRUE" if r.get("hasPaidProductPlacement") else "FALSE",now_loc,now_loc,"FALSE",""]
        rows.append(row)
    if not rows:
        print(f"INFO[NONE_ROWS_AFTER_FILTER]: {playlist_id}")
        return
    doc_id=pick_doc_for_playlist(st,playlist_id)
    ensure_header(doc_id)
    existing=read_existing_video_ids(doc_id)
    updates=[]; appends=[]
    for row in rows:
        vid=row[0]
        if vid in existing: updates.append((existing[vid],row))
        else: appends.append(row)
    if updates: batch_update_rows(doc_id,updates)
    if appends:
        append_rows(doc_id,appends)
        for _ in appends:
            for d in st["docs"]:
                if d["id"]==doc_id:
                    d["rows"]=d.get("rows",0)+1
                    break
    doc_name=""
    for d in st["docs"]:
        if d["id"]==doc_id: doc_name=d.get("name",""); break
    update_index(index_id,[{"playlistId":playlist_id,"docId":doc_id,"docName":doc_name,"rowsInDoc":next((d.get("rows",0) for d in st["docs"] if d["id"]==doc_id),0)}])
    print(f"DONE[PLAYLIST]: {playlist_id} up={len(updates)} add={len(appends)}")

def drive_validate_folder(folder_id):
    svc=build_drive_user()
    try:
        meta=svc.files().get(fileId=folder_id,fields="id,name,mimeType,trashed,driveId",supportsAllDrives=True).execute()
    except HttpError as e:
        s,m=parse_http(e); fail("DRIVE_FOLDER",f"{s} {m}")
    except Exception as e:
        fail("DRIVE_FOLDER",f"{type(e).__name__}: {str(e)[:200]}")
    if meta.get("trashed"): fail("DRIVE_FOLDER","folder is trashed")
    if meta.get("mimeType")!="application/vnd.google-apps.folder": fail("DRIVE_FOLDER","not a folder id")
    return meta.get("name","")

def main():
    if not API_KEYS: fail("MISSING","YOUTUBE_API_KEYS")
    if not SOURCE_SHEET_ID or not SOURCE_SHEET_TAB or not MAP_SHEET_TAB: fail("MISSING","SOURCE_SHEET_*")
    if not CHUNKS_FOLDER_ID: fail("MISSING","CHUNKS_FOLDER_ID")
    drive_validate_folder(CHUNKS_FOLDER_ID)
    header_map,topic_ru_map=get_helper_maps()
    uploads,vcounts,topics,titles=get_baza_columns(header_map)
    allowed=[]
    for pid,vc,tc in zip(uploads,vcounts,topics):
        if not pid: continue
        if over_10k(vc): continue
        if is_tv(tc): continue
        allowed.append(pid)
    allowed=[x for x in allowed if x][:PLAYLIST_LIMIT]
    if not allowed: fail("NO_INPUT","no playlists after filter")
    since_iso=(dt.datetime.utcnow()-dt.timedelta(days=WINDOW_DAYS)).replace(microsecond=0).isoformat()+"Z"
    st=load_state()
    index_id=ensure_index_sheet()
    title_map={}
    for pid,t in zip(uploads,titles):
        if pid: title_map[pid]=t
    for pid in allowed:
        process_playlist(st,pid,title_map.get(pid,""),since_iso,topic_ru_map,index_id)
        time.sleep(0.2)
    save_state(st)

if __name__=="__main__":
    try: main()
    except SystemExit: raise
    except Exception as e:
        print(f"ERROR[UNHANDLED]: {type(e).__name__} {str(e)[:200]}")
        sys.exit(3)
