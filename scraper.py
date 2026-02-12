from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.errors import HttpError
import pandas as pd
import numpy as np
import os
from apify_client import ApifyClient
from datetime import datetime
import json
import pytz
import time
import random
import sys
import logging
import math

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# --- Additional imports for body extraction ---
from concurrent.futures import ThreadPoolExecutor, as_completed
from newspaper import Article
import requests
import re

# --- Config / Env checks ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

GOOGLE_CREDENTIALS_ENV = os.getenv("GOOGLE_CREDENTIALS")
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1du5Cx3pK1LnxoVeBXTzP-nY-OSvflKXjJZw2Lq-AE14")
ACTOR_ID = os.getenv("ACTOR_ID", "easyapi/google-news-scraper")

if not GOOGLE_CREDENTIALS_ENV:
    logging.error("Missing GOOGLE_CREDENTIALS environment variable. Exiting.")
    sys.exit(1)

if not APIFY_TOKEN:
    logging.error("Missing APIFY_TOKEN environment variable. Exiting.")
    sys.exit(1)

# Optional tunables via env
COUNTRIES = [c.strip() for c in os.getenv("COUNTRIES", "ar,cl,pe").split(",") if c.strip()]
QUERIES = [q.strip() for q in os.getenv(
    "QUERIES",
    "tik-tok,tiktok,tiktok suicidio,tiktok grooming,tiktok armas,tiktok drogas,tiktok violacion,tiktok delincuentes,tiktok ladrones,tiktok narcos,tiktok estafa"
).split(",") if q.strip()]

try:
    MAX_ITEMS = int(os.getenv("MAX_ITEMS", "500"))
    if MAX_ITEMS <= 0:
        logging.warning("MAX_ITEMS <= 0; usando 500.")
        MAX_ITEMS = 500
except Exception:
    logging.warning("MAX_ITEMS no es int válido; usando 500.")
    MAX_ITEMS = 500

TIME_PERIOD = os.getenv("TIME_PERIOD", "last_day")  # mantener por defecto
TZ_ARGENTINA = pytz.timezone("America/Argentina/Buenos_Aires")

# --- Google Sheets client setup ---
try:
    creds_dict = json.loads(GOOGLE_CREDENTIALS_ENV)
    creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
except Exception as e:
    logging.exception("Failed loading Google credentials from env: %s", e)
    sys.exit(1)

service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

# --- Apify client ---
apify_client = ApifyClient(APIFY_TOKEN)

# --- Helper: retry with exponential backoff ---
def retry(fn, max_attempts=5, base_delay=2, max_delay=60, jitter=0.5, *args, **kwargs):
    attempt = 0
    while True:
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            attempt += 1
            if attempt >= max_attempts:
                logging.exception("Max retries reached calling %s", getattr(fn, "__name__", str(fn)))
                raise
            # exponential backoff with capped jitter
            backoff = min(max_delay, base_delay * (2 ** (attempt - 1)))
            sleep_for = backoff + (random.random() * jitter)
            logging.warning("Call to %s failed (attempt %d/%d): %s — retrying in %.1fs",
                            getattr(fn, "__name__", str(fn)), attempt, max_attempts, e, sleep_for)
            time.sleep(sleep_for)

# --- Utility: safe date conversion ---
def safe_convert_date_col(df, col='date_utc'):
    if col not in df.columns:
        df[col] = ''
        return df
    try:
        # Coerce to datetime (works with strings, numeric timestamps, pandas Timestamps)
        dtseries = pd.to_datetime(df[col], utc=True, errors='coerce')
        # If any are NaT, leave as blank string
        dtseries = dtseries.dt.tz_convert(TZ_ARGENTINA)
        df[col] = dtseries.dt.strftime('%d/%m/%Y').fillna('')
    except Exception:
        # fallback: coerce to string and fill blanks
        df[col] = df[col].astype(str).fillna('')
    return df

# Optional: week formatting (disponible si querés usarla)
import calendar
def format_week_range(date_str):
    if not date_str or pd.isna(date_str):
        return ''
    try:
        dt = datetime.strptime(date_str, '%d/%m/%Y')
        monday = dt - pd.Timedelta(days=dt.weekday())
        sunday = monday + pd.Timedelta(days=6)
        month_abbr = calendar.month_abbr[monday.month].upper()
        return f"{monday.day:02d}–{sunday.day:02d} {month_abbr} {monday.year}"
    except Exception:
        return ''

# --- Main scraping loop ---
all_dfs = []

for query in QUERIES:
    for country in COUNTRIES:
        run_input = {
            "cr": country,
            "gl": country,
            "lr": "lang_es",
            "maxItems": MAX_ITEMS,
            "query": query,
            "time_period": TIME_PERIOD
        }

        logging.info("Ejecutando actor %s para %s con query '%s'...", ACTOR_ID, country, query)
        try:
            run = retry(lambda: apify_client.actor(ACTOR_ID).call(run_input=run_input), max_attempts=4)
        except Exception as e:
            logging.error("Error al ejecutar actor para %s con query '%s': %s", country, query, e)
            continue

        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            logging.warning("No dataset generado para %s - '%s' (run: %s)", country, query, str(run)[:200])
            continue

        try:
            items = retry(lambda: apify_client.dataset(dataset_id).list_items().items, max_attempts=4)
        except Exception as e:
            logging.exception("Error al listar items del dataset %s: %s", dataset_id, e)
            continue

        if not items:
            logging.info("No hay resultados para %s - '%s'", country, query)
            continue

        df = pd.DataFrame(items)
        df["country"] = country
        # Añadir trazabilidad: qué query generó este item
        df["query"] = query
        df["scraped_at"] = datetime.now(TZ_ARGENTINA).isoformat()
        all_dfs.append(df)

if not all_dfs:
    logging.error("No se obtuvieron resultados de ningún país. Saliendo sin actualizar hoja.")
    sys.exit(0)

# --- Build final dataframe and normalize columns ---
final_df = pd.concat(all_dfs, ignore_index=True)

if 'link' in final_df.columns:
    final_df.drop_duplicates(subset=["link"], inplace=True)
else:
    logging.warning("No 'link' column present in scraped items; duplicates won't be removed by link.")

final_df = safe_convert_date_col(final_df, 'date_utc')

# Additional columns
final_df['sentiment'] = ''
final_df['semana'] = ''
final_df['tag'] = ''
# Map short country codes to names (defensivo)
final_df['country'] = final_df['country'].replace({'ar': 'Argentina', 'cl': 'Chile', 'pe': 'Peru'})

# Format scraped_at (legible)
try:
    final_df['scraped_at'] = pd.to_datetime(final_df['scraped_at'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M').fillna('')
except Exception:
    final_df['scraped_at'] = final_df['scraped_at'].astype(str).fillna('')

# --- EXTRA: fetch article bodies with newspaper3k + requests (ThreadPool + simple cache) ---
# Configurables (puedes moverlos a envvars si querés)
CACHE_PATH = os.getenv("ARTICLE_CACHE_PATH", "article_cache.json")
MAX_FETCH_WORKERS = int(os.getenv("MAX_FETCH_WORKERS", "6"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "15"))  # seconds
REQUEST_RETRIES = int(os.getenv("REQUEST_RETRIES", "2"))
REQUEST_SLEEP_BETWEEN = float(os.getenv("REQUEST_SLEEP_BETWEEN", "0.2"))  # polite small delay

# Regex para detectar 'tiktok' y variantes
TIKTOK_PATTERN = re.compile(r"tik\s*-?\s*tok", flags=re.IGNORECASE)

# --- Simple cache helper (link -> body) ---
def load_cache(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            return {}
    return {}

def save_cache(path, data):
    try:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.warning("No se pudo guardar cache en %s: %s", path, e)

article_cache = load_cache(CACHE_PATH)

# Normalizar URL como key (puede ser la URL completa)
def url_key(url):
    return url.strip()

# Fetch HTML with requests + retries
def fetch_html(url):
    headers = {
        "User-Agent": os.getenv("FETCH_USER_AGENT", "Mozilla/5.0 (compatible; PublicBot/1.0; +https://publicalatam.com)"),
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8"
    }
    for attempt in range(REQUEST_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            # follow redirects by default; check status
            if resp.status_code == 200 and resp.text:
                return resp.text
            else:
                logging.debug("fetch_html: %s returned status %d", url, resp.status_code)
        except requests.RequestException as e:
            logging.debug("fetch_html attempt %d for %s failed: %s", attempt+1, url, e)
        # small sleep before retry
        time.sleep(0.5 + REQUEST_SLEEP_BETWEEN * attempt)
    return None

# Extract article text using newspaper3k but passing HTML we fetched
def extract_body_from_html(url, html):
    try:
        a = Article(url, language='es')
        a.set_html(html)
        a.parse()
        text = a.text or ''
        return text.strip()
    except Exception as e:
        logging.debug("newspaper parse failed for %s: %s", url, e)
        return ''

# Combined fetch+parse function for executor
def fetch_and_parse(url):
    key = url_key(url)
    # check cache
    if key in article_cache:
        return key, article_cache[key]
    html = fetch_html(url)
    if not html:
        # fallback: empty string
        article_cache[key] = ''
        return key, ''
    body = extract_body_from_html(url, html)
    article_cache[key] = body
    # polite small delay
    time.sleep(REQUEST_SLEEP_BETWEEN)
    return key, body

# --- Prepare unique links to fetch ---
links_series = final_df['link'].dropna().astype(str)
unique_links = links_series.unique().tolist()
logging.info("Starting article fetch: %d unique links (cache hits: %d)", len(unique_links),
             sum(1 for l in unique_links if url_key(l) in article_cache))

# Run threaded fetch/parsing
link_to_body = {}
with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as ex:
    futures = {ex.submit(fetch_and_parse, url): url for url in unique_links}
    for fut in as_completed(futures):
        url = futures[fut]
        try:
            key, body = fut.result()
            link_to_body[key] = body or ''
        except Exception as e:
            logging.warning("Error fetching/parsing %s: %s", url, e)
            link_to_body[url] = ''

# Persist cache (best-effort)
save_cache(CACHE_PATH, article_cache)

# Map bodies back into final_df
final_df['link'] = final_df['link'].astype(str)
final_df['article_body'] = final_df['link'].map(lambda u: link_to_body.get(url_key(u), '')).fillna('')

# --- Filtering: keep rows that mention the pattern in title OR snippet OR article_body ---
mask = (
    final_df['title'].str.contains(TIKTOK_PATTERN, na=False) |
    final_df['snippet'].str.contains(TIKTOK_PATTERN, na=False) |
    final_df['article_body'].str.contains(TIKTOK_PATTERN, na=False)
)
before_tot = len(final_df)
final_df = final_df[mask].copy()
after_tot = len(final_df)
logging.info("After body verification filter: %d -> %d rows (removed %d)", before_tot, after_tot, before_tot - after_tot)

# Ensure column order and presence
header = ['semana','date_utc','country','title','link','domain','source','snippet','tag','sentiment','scraped_at']
final_df = final_df.reindex(columns=header, fill_value='')
final_df = final_df.drop_duplicates(subset='link')

# --- Read existing sheet and combine ---
SHEET_RANGE = "2026!A:J"  # cambia si corresponde
try:
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=SHEET_RANGE).execute()
    values = result.get("values", [])
    logging.info("Leídas %d filas desde la hoja (incl header si existía).", len(values))
except HttpError as e:
    logging.exception("Failed to read existing sheet: %s", e)
    values = []
except Exception as e:
    logging.exception("Failed to read existing sheet: %s", e)
    values = []

if values:
    try:
        existing_df = pd.DataFrame(values[1:], columns=values[0])
        existing_df = existing_df.reindex(columns=header, fill_value='')
    except Exception as e:
        logging.exception("Error parsing existing sheet values into DataFrame: %s", e)
        existing_df = pd.DataFrame(columns=header)
else:
    existing_df = pd.DataFrame(columns=header)

combined_df = pd.concat([existing_df, final_df], ignore_index=True)
if 'link' in combined_df.columns:
    combined_df.drop_duplicates(subset=["link"], inplace=True)
else:
    logging.warning("No 'link' column in combined_df; duplicates not removed by link.")
combined_df = combined_df.reset_index(drop=True)

# --- SANITIZE data before writing to Sheets ---
combined_df = combined_df.replace([np.nan, pd.NaT, None], '')
combined_df = combined_df.replace([np.inf, -np.inf], '')

def sanitize_cell(cell):
    # Numpy scalar numeric types
    if isinstance(cell, (np.integer,)):
        return int(cell)
    if isinstance(cell, (np.floating,)):
        fv = float(cell)
        if math.isnan(fv) or math.isinf(fv):
            return ''
        return fv
    if isinstance(cell, (np.bool_, bool)):
        return bool(cell)
    # pandas Timestamp
    try:
        import pandas as _pd
        if isinstance(cell, _pd.Timestamp):
            if pd.isna(cell):
                return ''
            return cell.isoformat()
    except Exception:
        pass
    # default
    s = '' if cell is None else str(cell)
    if s.lower() in ('nan', 'nat', 'none'):
        return ''
    return s

values_rows = []
for row in combined_df[header].values.tolist():
    sanitized_row = [sanitize_cell(cell) for cell in row]
    # Convert to strings for Sheets
    values_rows.append([str(cell) for cell in sanitized_row])

body_values = [header] + values_rows

# Pre-flight JSON serializable check (rápida)
def is_json_serializable(obj):
    try:
        json.dumps(obj)
        return True
    except Exception:
        return False

bad_cells = []
for r_idx, row in enumerate(body_values):
    for c_idx, cell in enumerate(row):
        if not is_json_serializable(cell):
            bad_cells.append((r_idx, c_idx, type(cell).__name__, repr(cell)))
if bad_cells:
    logging.warning("Found unserializable cells (row_index, col_index, type, repr). Showing up to 20 entries:")
    for entry in bad_cells[:20]:
        logging.warning(entry)

# --- Write to sheet with diagnostics on HttpError ---
try:
    logging.info("Clearing target range %s ...", SHEET_RANGE)
    sheet.values().clear(spreadsheetId=SPREADSHEET_ID, range=SHEET_RANGE).execute()
    logging.info("Updating sheet con %d filas (incl header)...", len(body_values))
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range="2026!A1",
        valueInputOption="RAW",
        body={"values": body_values}
    ).execute()
    logging.info("✅ Hoja actualizada sin duplicados. Filas escritas: %d", len(body_values)-1)
except HttpError as e:
    logging.exception("HttpError escribiendo en Sheets: %s", e)
    sample_preview = {
        "first_rows": body_values[:5],
        "rows_count": len(body_values),
        "bad_cells_count": len(bad_cells),
    }
    logging.error("Payload preview (first 5 rows): %s", json.dumps(sample_preview, ensure_ascii=False, indent=2))
    raise
except Exception as e:
    logging.exception("Unexpected error writing to Sheets: %s", e)
    raise

logging.info("Script finished correctamente.")
