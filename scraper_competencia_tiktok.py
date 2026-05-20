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
QUERIES = os.getenv("QUERIES", "youtube,instagram,facebook").split(",")

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
        except Exception as e:
            logging.warning("Attempt %d/%d failed: %s", i + 1, max_attempts, e)
            if i == max_attempts - 1:
                raise
            time.sleep(2 ** i)

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
    try:
        run = retry(lambda: apify_client.actor(ACTOR_ID).call(run_input=run_input))
        # Fix: apify-client now returns a Pydantic object (snake_case), not a dict
        dataset_id = (
            getattr(run, "default_dataset_id", None) or
            getattr(run, "defaultDatasetId", None)
        )
        if not dataset_id:
            logging.warning("No dataset_id for %s - %s", task["country"], task["query"])
        return {
            "dataset_id": dataset_id,
            "country": task["country"],
            "query": task["query"]
        }
    except Exception as e:
        logging.error("Actor failed for %s - %s: %s", task["country"], task["query"], e)
        return {"dataset_id": None, "country": task["country"], "query": task["query"]}

results = []
with ThreadPoolExecutor(max_workers=4) as ex:
    futures = [ex.submit(run_actor, t) for t in tasks]
    for f in as_completed(futures):
        r = f.result()
        if r["dataset_id"]:
            results.append(r)

logging.info("Actor runs completed: %d successful / %d total", len(results), len(tasks))

if not results:
    logging.error("No results from any actor. Exiting.")
    sys.exit(0)

# --- FETCH DATA ---
def fetch_dataset(entry):
    try:
        items = apify_client.dataset(entry["dataset_id"]).list_items().items
        if not items:
            logging.info("Empty dataset for %s - %s", entry["country"], entry.get("query", ""))
            return None
        df = pd.DataFrame(items)
        df["country"] = entry["country"]
        return df
    except Exception as e:
        logging.warning("Failed fetching dataset %s: %s", entry["dataset_id"], e)
        return None

dfs = []
with ThreadPoolExecutor(max_workers=6) as ex:
    futures = [ex.submit(fetch_dataset, r) for r in results]
    for f in as_completed(futures):
        df = f.result()
        if df is not None and not df.empty:
            dfs.append(df)

if not dfs:
    logging.error("No data fetched from datasets. Exiting.")
    sys.exit(0)

final_df = pd.concat(dfs, ignore_index=True)

# --- CLEAN ---
if 'link' in final_df.columns:
    final_df['link'] = final_df['link'].astype(str).apply(normalize_link)
    final_df.drop_duplicates(subset="link", inplace=True)
else:
    logging.warning("No 'link' column found.")

final_df['country'] = final_df['country'].replace({'ar': 'Argentina', 'cl': 'Chile', 'pe': 'Peru'})

# --- FILTER ---
pattern = re.compile(r"(youtube|instagram|facebook|roblox)", re.IGNORECASE)

title_col = final_df['title'].astype(str) if 'title' in final_df.columns else pd.Series([''] * len(final_df))
snippet_col = final_df['snippet'].astype(str) if 'snippet' in final_df.columns else pd.Series([''] * len(final_df))

mask = title_col.str.contains(pattern, na=False) | snippet_col.str.contains(pattern, na=False)
before = len(final_df)
final_df = final_df[mask].copy()
logging.info("Filter: %d -> %d rows", before, len(final_df))

if final_df.empty:
    logging.info("No rows after filter. Exiting.")
    sys.exit(0)

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

final_df['scraped_at'] = datetime.now(TZ_ARGENTINA).strftime('%d/%m/%Y %H:%M')

# --- SHEET ---
HEADER = ['date_utc', 'country', 'title', 'link', 'domain', 'source', 'snippet', 'tag', 'scraped_at', 'tier', 'enviar']
final_df = final_df.reindex(columns=HEADER, fill_value='')

# --- READ EXISTING ---
try:
    values = sheet_service.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="Competencia!A:L"
    ).execute().get("values", [])
except Exception as e:
    logging.error("Failed reading sheet: %s", e)
    values = []

existing_links = set()
if values:
    for r in values[1:]:
        if len(r) > 3:
            existing_links.add(normalize_link(r[3]))

logging.info("Existing links in sheet: %d", len(existing_links))

# --- PREPARE ROWS ---
rows = []
for _, row in final_df.iterrows():
    link = normalize_link(str(row.get('link', '')))
    if not link or link in existing_links:
        continue
    sanitized = []
    for c in HEADER:
        val = row[c]
        if val is None or (isinstance(val, float) and np.isnan(val)):
            sanitized.append('')
        else:
            sanitized.append(str(val))
    rows.append(sanitized)
    existing_links.add(link)

if not rows:
    logging.info("No new rows to add.")
    sys.exit(0)

# --- APPEND ---
try:
    sheet_service.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="Competencia!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows}
    ).execute()
    logging.info("✅ %d rows added.", len(rows))
except Exception as e:
    logging.error("Failed appending to sheet: %s", e)
    sys.exit(1)
