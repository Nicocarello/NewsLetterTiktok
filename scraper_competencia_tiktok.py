from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from googleapiclient.discovery import build
from google.oauth2 import service_account
from apify_client import ApifyClient
import pandas as pd
import numpy as np
import pytz
import json
import logging
import os
import time
import re
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# --- ENV ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
GOOGLE_CREDENTIALS_ENV = os.getenv("GOOGLE_CREDENTIALS")
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1du5Cx3pK1LnxoVeBXTzP-nY-OSvflKXjJZw2Lq-AE14")
ACTOR_ID = os.getenv("ACTOR_ID", "easyapi/google-news-scraper")

if not GOOGLE_CREDENTIALS_ENV or not APIFY_TOKEN:
    logging.error("Missing required env vars.")
    sys.exit(1)

COUNTRIES = os.getenv("COUNTRIES", "ar,cl,pe").split(",")
QUERIES = os.getenv("QUERIES", "youtube,google,instagram,facebook").split(",")

MAX_ITEMS = int(os.getenv("MAX_ITEMS", "500"))
TIME_PERIOD = os.getenv("TIME_PERIOD", "last_day")
TZ_ARGENTINA = pytz.timezone("America/Argentina/Buenos_Aires")

# --- CLIENTS ---
creds = service_account.Credentials.from_service_account_info(
    json.loads(GOOGLE_CREDENTIALS_ENV),
    scopes=SCOPES
)
sheet_service = build('sheets', 'v4', credentials=creds).spreadsheets()
apify_client = ApifyClient(APIFY_TOKEN)

# --- HELPERS ---
def normalize_link(url):
    if not url:
        return ''
    url = str(url).strip()
    url = re.sub(r'[?#].*$', '', url)
    return url.rstrip('/')

def retry(fn, max_attempts=5):
    for i in range(max_attempts):
        try:
            return fn()
        except Exception:
            time.sleep(2 ** i)
    raise Exception("Retry failed")

# --- RUN ACTORS ---
tasks = [
    {"query": q.strip(), "country": c.strip()}
    for q in QUERIES for c in COUNTRIES
]

def run_actor(task):
    run_input = {
        "cr": task["country"],
        "gl": task["country"],
        "lr": "lang_es",
        "maxItems": MAX_ITEMS,
        "query": task["query"],
        "time_period": TIME_PERIOD
    }
    run = retry(lambda: apify_client.actor(ACTOR_ID).call(run_input=run_input))
    return {
        "dataset_id": run.get("defaultDatasetId"),
        "country": task["country"]
    }

results = []
with ThreadPoolExecutor(max_workers=4) as ex:
    futures = [ex.submit(run_actor, t) for t in tasks]
    for f in as_completed(futures):
        r = f.result()
        if r["dataset_id"]:
            results.append(r)

# --- FETCH DATA ---
def fetch_dataset(entry):
    items = apify_client.dataset(entry["dataset_id"]).list_items().items
    df = pd.DataFrame(items)
    df["country"] = entry["country"]
    return df

dfs = []
with ThreadPoolExecutor(max_workers=6) as ex:
    futures = [ex.submit(fetch_dataset, r) for r in results]
    for f in as_completed(futures):
        dfs.append(f.result())

final_df = pd.concat(dfs, ignore_index=True)

# --- CLEAN ---
final_df.drop_duplicates(subset="link", inplace=True)
final_df['country'] = final_df['country'].replace({'ar': 'Argentina', 'cl': 'Chile', 'pe': 'Peru'})

# --- FILTER ---
pattern = re.compile(r"(youtube|google|instagram|facebook|roblox)", re.IGNORECASE)

title_col = final_df['title'].astype(str) if 'title' in final_df.columns else pd.Series([''] * len(final_df))
snippet_col = final_df['snippet'].astype(str) if 'snippet' in final_df.columns else pd.Series([''] * len(final_df))

mask = title_col.str.contains(pattern, na=False) | snippet_col.str.contains(pattern, na=False)
final_df = final_df[mask].copy()

# --- FORMAT FIXES ---
def format_date_utc(value):
    dt = pd.to_datetime(value, utc=True, errors='coerce')
    if pd.isna(dt):
        return ''
    return dt.tz_convert(TZ_ARGENTINA).strftime('%d/%m/%Y')

if 'date_utc' in final_df.columns:
    final_df['date_utc'] = final_df['date_utc'].apply(format_date_utc)
else:
    final_df['date_utc'] = ''

# forzar siempre scraped_at al final
final_df['scraped_at'] = datetime.now(TZ_ARGENTINA).strftime('%d/%m/%Y %H:%M')

# --- SHEET ---
HEADER = ['date_utc', 'country', 'title', 'link', 'domain', 'source', 'snippet', 'scraped_at', 'tier', 'enviar']
final_df = final_df.reindex(columns=HEADER, fill_value='')

# --- READ EXISTING ---
values = sheet_service.values().get(
    spreadsheetId=SPREADSHEET_ID,
    range="Competencia!A:J"
).execute().get("values", [])

existing_links = set()
if values:
    for r in values[1:]:
        if len(r) > 3:
            existing_links.add(normalize_link(r[3]))

# --- PREPARE ROWS ---
rows = []
for _, row in final_df.iterrows():
    link = normalize_link(row['link'])
    if not link or link in existing_links:
        continue
    rows.append(['' if pd.isna(row[c]) else str(row[c]) for c in HEADER])
    existing_links.add(link)

if not rows:
    logging.info("No new rows.")
    sys.exit(0)

# --- APPEND ---
sheet_service.values().append(
    spreadsheetId=SPREADSHEET_ID,
    range="Competencia!A1",
    valueInputOption="RAW",
    insertDataOption="INSERT_ROWS",
    body={"values": rows}
).execute()

logging.info(f"✅ {len(rows)} rows added.")
