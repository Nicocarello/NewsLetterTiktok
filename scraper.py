import os
import json
import time
import math
import random
import logging
import pytz
import re
from datetime import datetime
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
import pandas as pd

from googleapiclient.discovery import build
from google.oauth2 import service_account
from apify_client import ApifyClient


# --------------------------- Config --------------------------- #
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1du5Cx3pK1LnxoVeBXTzP-nY-OSvflKXjJZw2Lq-AE14")
SHEET_RANGE = os.getenv("SHEET_RANGE", "Data!A:J")

# Comma-separated envs are supported; fall back to your hardcoded lists
COUNTRIES = [c.strip() for c in os.getenv("COUNTRIES", "ar,cl,pe").split(",") if c.strip()]
QUERIES = [q.strip() for q in os.getenv(
    "QUERIES",
    "tik-tok,tiktok,tiktok suicidio,tiktok grooming,tiktok armas,tiktok drogas,tiktok violacion"
).split(",") if q.strip()]

# Apify Google News actor + window
ACTOR_ID = os.getenv("APIFY_ACTOR_ID", "easyapi/google-news-scraper")
TIME_PERIOD = os.getenv("TIME_PERIOD", "last_hour")   # e.g., last_hour, last_day
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "5000"))

# Concurrency
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "12"))

# Argentina timezone
TZ_ARG = pytz.timezone("America/Argentina/Buenos_Aires")

# Logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(message)s",
)
log = logging.getLogger("google_news_pipeline")

# Google + Apify credentials
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
APIFY_TOKEN = os.getenv("APIFY_TOKEN")

if not GOOGLE_CREDENTIALS or not APIFY_TOKEN:
    raise RuntimeError("Missing GOOGLE_CREDENTIALS or APIFY_TOKEN envs.")

CREDS = service_account.Credentials.from_service_account_info(
    json.loads(GOOGLE_CREDENTIALS), scopes=SCOPES
)

sheets = build("sheets", "v4", credentials=CREDS).spreadsheets()
apify = ApifyClient(APIFY_TOKEN)

COUNTRY_NAMES = {"ar": "Argentina", "cl": "Chile", "pe": "Peru"}

# ----------------------- Retry Helpers ------------------------ #
def backoff_sleep(attempt: int, base: float = 0.5, cap: float = 8.0) -> None:
    # exponential backoff with jitter
    sleep = min(cap, base * (2 ** attempt)) * (0.5 + random.random() / 2)
    time.sleep(sleep)

def with_retries(fn, *, tries=5, on=(Exception,), desc="op"):
    for attempt in range(tries):
        try:
            return fn()
        except on as e:
            if attempt == tries - 1:
                log.error(json.dumps({"event": "retry_exhausted", "op": desc, "error": str(e)}))
                raise
            log.warning(json.dumps({"event": "retry", "op": desc, "attempt": attempt + 1, "error": str(e)}))
            backoff_sleep(attempt)

# ---------------------- Utility Functions --------------------- #
def now_arg_fmt():
    return datetime.now(TZ_ARG).strftime("%d/%m/%Y %H:%M")

def to_arg_date(date_str):
    try:
        # Apify returns ISO string (UTC). Normalize -> Buenos Aires -> dd/mm/YYYY
        dt = pd.to_datetime(date_str, utc=True, errors="coerce")
        if pd.isna(dt):
            return ""
        return dt.tz_convert(TZ_ARG).strftime("%d/%m/%Y")
    except Exception:
        return ""

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

TITLE_META_KEYS = [
    ("meta", {"property": "og:title"}),
    ("meta", {"name": "twitter:title"}),
    ("title", {}),
]
DESC_META_KEYS = [
    ("meta", {"property": "og:description"}),
    ("meta", {"name": "description"}),
    ("meta", {"name": "twitter:description"}),
]

TIKTOK_REGEX = re.compile(r"\btik\s*-?\s*tok\b", re.IGNORECASE)

def extract_text_from_html(html: bytes) -> str:
    soup = BeautifulSoup(html, "html.parser")
    texts = []
    # titles/descriptions can carry the keyword even if body is lazy-loaded
    for tag, attrs in TITLE_META_KEYS + DESC_META_KEYS:
        for el in soup.find_all(tag, attrs=attrs):
            val = el.get("content") if tag == "meta" else el.get_text(separator=" ", strip=True)
            if val:
                texts.append(val)
    for tag in ("h1", "h2", "h3", "p"):
        texts.extend(el.get_text(separator=" ", strip=True) for el in soup.find_all(tag))
    text = " ".join(texts).lower()
    # normalize “weird dashes” and whitespace
    text = re.sub(r"[\u2010-\u2015\u2212\uFE58\uFE63\uFF0D]", "-", text)
    text = re.sub(r"\s+", " ", text)
    return text

def url_contains_tiktok(url: str, timeout=10) -> bool:
    headers = {"User-Agent": UA, "Accept-Encoding": "gzip, deflate, br"}
    def _fetch():
        return requests.get(url, timeout=timeout, headers=headers, allow_redirects=True)
    try:
        resp = with_retries(_fetch, tries=3, on=(requests.RequestException,), desc="http_get")
        if resp.status_code != 200 or not resp.content:
            return False
        text = extract_text_from_html(resp.content)
        return bool(TIKTOK_REGEX.search(text))
    except Exception as e:
        log.warning(json.dumps({"event": "fetch_failed", "url": url, "error": str(e)}))
        return False

def fetch_apify_items(query: str, country: str) -> list[dict]:
    run_input = {
        "cr": country,
        "gl": country,
        "hl": "es-419",
        "lr": "lang_es",
        "maxItems": MAX_ITEMS,
        "query": query,
        "time_period": TIME_PERIOD,
    }

    def _run_actor():
        return apify.actor(ACTOR_ID).call(run_input=run_input)

    log.info(json.dumps({"event": "apify_run_start", "actor": ACTOR_ID, "country": country, "query": query}))
    run = with_retries(_run_actor, tries=4, desc="apify_actor_call")
    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        return []

    # paginate through dataset in chunks
    items = []
    offset = 0
    page = 0
    PAGE_SIZE = 500  # Apify limit is typically up to 1000; use conservative size
    while True:
        def _list():
            return apify.dataset(dataset_id).list_items(limit=PAGE_SIZE, offset=offset)
        res = with_retries(_list, tries=4, desc="apify_list_items")
        batch = res.items or []
        items.extend(batch)
        got = len(batch)
        log.info(json.dumps({"event": "apify_page", "page": page, "got": got}))
        if got < PAGE_SIZE:
            break
        page += 1
        offset += PAGE_SIZE
    return items

def read_sheet_dataframe() -> pd.DataFrame:
    def _get():
        return sheets.values().get(spreadsheetId=SPREADSHEET_ID, range=SHEET_RANGE).execute()
    result = with_retries(_get, tries=4, desc="sheets_get")
    values = result.get("values", [])
    if not values:
        return pd.DataFrame(columns=["fecha_envio","date_utc","country","title","link","domain","snippet","tag","sentiment","scraped_at"])
    header, rows = values[0], values[1:]
    return pd.DataFrame(rows, columns=header)

def ensure_header():
    existing = read_sheet_dataframe()
    if existing.empty:
        header = ['fecha_envio','date_utc','country','title','link','domain','snippet','tag','sentiment','scraped_at']
        body = {"values": [header]}
        def _update():
            return sheets.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=SHEET_RANGE.split("!")[0] + "!A1",
                valueInputOption="RAW",
                body=body
            ).execute()
        with_retries(_update, tries=4, desc="sheets_init_header")

def append_rows(rows: list[list[str]]):
    if not rows:
        return
    body = {"values": rows}
    def _append():
        return sheets.values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=SHEET_RANGE,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body=body
        ).execute()
    with_retries(_append, tries=4, desc="sheets_append")

# ------------------------- Main Flow -------------------------- #
def main():
    ensure_header()
    existing_df = read_sheet_dataframe()
    existing_links = set(existing_df["link"].tolist()) if "link" in existing_df.columns else set()

    all_rows = []
    scraped_at = now_arg_fmt()

    # Pull from Apify (serial by query/country to avoid rate spikes)
    for query in QUERIES:
        for country in COUNTRIES:
            try:
                items = fetch_apify_items(query, country)
                if not items:
                    continue
                df = pd.DataFrame(items)

                # Normalize & add columns
                df["country"] = COUNTRY_NAMES.get(country, country)
                df["scraped_at"] = scraped_at
                df["tag"] = query
                # Ensure columns exist
                for col in ["title","link","domain","snippet","date_utc"]:
                    if col not in df.columns:
                        df[col] = ""

                # Fill domain if missing
                df["domain"] = df["domain"].fillna("").astype(str)
                df.loc[df["domain"].eq(""), "domain"] = df["link"].apply(
                    lambda u: urlparse(u).netloc if isinstance(u, str) and u else ""
                )

                # Convert/format dates
                df["date_utc"] = df["date_utc"].apply(to_arg_date)

                # Filter to entries we haven't already stored (avoid re-fetching pages)
                df = df[~df["link"].isin(existing_links)].copy()
                if df.empty:
                    continue

                # Body-text verification (concurrent)
                urls = df["link"].tolist()
                results = []
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                    future_map = {pool.submit(url_contains_tiktok, u): u for u in urls}
                    for fut in as_completed(future_map):
                        u = future_map[fut]
                        ok = False
                        try:
                            ok = fut.result()
                        except Exception as e:
                            log.warning(json.dumps({"event": "bodycheck_error", "url": u, "error": str(e)}))
                        results.append((u, ok))

                ok_urls = {u for u, ok in results if ok}
                df = df[df["link"].isin(ok_urls)].copy()
                if df.empty:
                    continue

                # Final shape / ensure required columns
                df["sentiment"] = ""
                df["fecha_envio"] = ""

                header = ['fecha_envio','date_utc','country','title','link','domain','snippet','tag','sentiment','scraped_at']
                df = df.reindex(columns=header, fill_value="")

                # Deduplicate within this batch by link
                df = df.drop_duplicates(subset=["link"])

                # Convert to rows and collect
                rows = df.astype(str).values.tolist()
                all_rows.extend(rows)

            except Exception as e:
                log.error(json.dumps({"event": "query_country_failed", "query": query, "country": country, "error": str(e)}))

    # Append to sheet
    append_rows(all_rows)
    log.info(json.dumps({"event": "done", "appended_rows": len(all_rows)}))


if __name__ == "__main__":
    main()
