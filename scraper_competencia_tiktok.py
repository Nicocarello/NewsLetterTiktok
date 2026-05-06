from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.errors import HttpError
from apify_client import ApifyClient
from newspaper import Article
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import pytz
import json
import logging
import os
import time
import random
import re
import sys
import math

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
GOOGLE_CREDENTIALS_ENV = os.getenv("GOOGLE_CREDENTIALS")
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
ACTOR_ID = os.getenv("ACTOR_ID", "easyapi/google-news-scraper")

if not GOOGLE_CREDENTIALS_ENV or not APIFY_TOKEN:
    logging.error("Missing required env vars.")
    sys.exit(1)

COUNTRIES = os.getenv("COUNTRIES", "ar,cl,pe").split(",")
QUERIES = os.getenv("QUERIES", "youtube,google,instagram,facebook").split(",")

MAX_ITEMS = int(os.getenv("MAX_ITEMS", "500"))
TIME_PERIOD = os.getenv("TIME_PERIOD", "last_day")
TZ_ARGENTINA = pytz.timezone("America/Argentina/Buenos_Aires")

MAX_CONCURRENT_ACTORS = 4
MAX_CONCURRENT_DATASET_FETCH = 6

creds = service_account.Credentials.from_service_account_info(json.loads(GOOGLE_CREDENTIALS_ENV), scopes=SCOPES)
sheet_service = build('sheets', 'v4', credentials=creds).spreadsheets()
apify_client = ApifyClient(APIFY_TOKEN)

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
with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_ACTORS) as ex:
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
    df["scraped_at"] = datetime.now(TZ_ARGENTINA).strftime('%d/%m/%Y %H:%M')
    return df

dfs = []
with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DATASET_FETCH) as ex:
    futures = [ex.submit(fetch_dataset, r) for r in results]
    for f in as_completed(futures):
        dfs.append(f.result())

final_df = pd.concat(dfs, ignore_index=True)

# --- CLEAN ---
final_df.drop_duplicates(subset="link", inplace=True)
final_df['country'] = final_df['country'].replace({'ar':'Argentina','cl':'Chile','pe':'Peru'})

# --- FILTER ---
pattern = re.compile(r"(youtube|google|instagram|facebook|roblox)", re.IGNORECASE)

title_col = final_df['title'].astype(str) if 'title' in final_df else ""
snippet_col = final_df['snippet'].astype(str) if 'snippet' in final_df else ""

mask = title_col.str.contains(pattern) | snippet_col.str.contains(pattern)
final_df = final_df[mask]

# --- SHEET ---
HEADER = ['date_utc','country','title','link','domain','source','snippet','scraped_at','tier','enviar']
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
    rows.append([str(row[c]) for c in HEADER])
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
