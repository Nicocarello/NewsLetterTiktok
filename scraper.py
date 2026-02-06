#!/usr/bin/env python3
"""
robust_scraper.py

Robust version of the Google News -> Google Sheets scraper.
Features:
 - retries with exponential backoff for Apify and dataset calls
 - defensive handling of missing secrets
 - sanitization of DataFrame values before sending to Google Sheets
 - clear logging and helpful diagnostics on HttpError
"""
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

# --- Config / Env checks ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

GOOGLE_CREDENTIALS_ENV = os.getenv("GOOGLE_CREDENTIALS")
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1du5Cx3pK1LnxoVeBXTzP-nY-OSvflKXjJZw2Lq-AE14")

if not GOOGLE_CREDENTIALS_ENV:
    logging.error("Missing GOOGLE_CREDENTIALS environment variable. Exiting.")
    sys.exit(1)

if not APIFY_TOKEN:
    logging.error("Missing APIFY_TOKEN environment variable. Exiting.")
    sys.exit(1)

# Optional tunables via env
COUNTRIES = os.getenv("COUNTRIES", "ar,cl,pe").split(",")
QUERIES = os.getenv(
    "QUERIES",
    "tik-tok,tiktok,tiktok suicidio,tiktok grooming,tiktok armas,tiktok drogas,tiktok violacion,tiktok delincuentes,tiktok ladrones,tiktok narcos"
).split(",")
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "500"))   # keep reasonable for CI; tune later if needed
TIME_PERIOD = os.getenv("TIME_PERIOD", "last_day")  # default as your original script
ACTOR_ID = os.getenv("ACTOR_ID", "easyapi/google-news-scraper")

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
def retry(fn, max_attempts=5, base_delay=2, max_delay=60, jitter=1.0, *args, **kwargs):
    attempt = 0
    while True:
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            attempt += 1
            if attempt >= max_attempts:
                logging.exception("Max retries reached calling %s", getattr(fn, "__name__", str(fn)))
                raise
            sleep_for = min(max_delay, base_delay * (2 ** (attempt - 1))) + random.random() * jitter
            logging.warning("Call to %s failed (attempt %d/%d): %s — retrying in %.1fs",
                            getattr(fn, "__name__", str(fn)), attempt, max_attempts, e, sleep_for)
            time.sleep(sleep_for)

# --- Utility: safe date conversion ---
def safe_convert_date_col(df, col='date_utc'):
    if col not in df.columns:
        df[col] = ''
        return df
    try:
        df[col] = pd.to_datetime(df[col], utc=True, errors='coerce').dt.tz_convert(TZ_ARGENTINA)
        df[col] = df[col].dt.strftime('%d/%m/%Y').fillna('')
    except Exception:
        # fallback: coerce to string and fill blanks
        df[col] = df[col].astype(str).fillna('')
    return df

# Optional: week formatting (kept from your original, currently unused but available)
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
            "hl": "es-419",
            "lr": "lang_es",
            "maxItems": MAX_ITEMS,
            "query": query,
            "time_period": TIME_PERIOD,
        }
        logging.info("Ejecutando %s para %s con query '%s'...", ACTOR_ID, country, query)
        try:
            run = retry(lambda: apify_client.actor(ACTOR_ID).call(run_input=run_input), max_attempts=4)
        except Exception as e:
            logging.error("❌ Error al ejecutar actor para %s con query '%s': %s", country, query, e)
            continue

        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            logging.warning("⚠️ No dataset generado para %s - '%s'", country, query)
            continue

        try:
            items = retry(lambda: apify_client.dataset(dataset_id).list_items().items, max_attempts=4)
        except Exception as e:
            logging.exception("Error al listar items del dataset %s: %s", dataset_id, e)
            continue

        if not items:
            logging.info("⚠️ No hay resultados para %s - '%s'", country, query)
            continue

        df = pd.DataFrame(items)
        df["country"] = country
        df["scraped_at"] = datetime.now(TZ_ARGENTINA).isoformat()
        all_dfs.append(df)

if not all_dfs:
    logging.error("❌ No se obtuvieron resultados de ningún país. Saliendo sin actualizar hoja.")
    sys.exit(0)

# --- Build final dataframe and normalize columns ---
final_df = pd.concat(all_dfs, ignore_index=True)
# Ensure link column exists before dropping duplicates; if not present, keep all rows.
if 'link' in final_df.columns:
    final_df.drop_duplicates(subset=["link"], inplace=True)
else:
    logging.warning("No 'link' column present in scraped items; duplicates won't be removed by link.")

final_df = safe_convert_date_col(final_df, 'date_utc')

# Additional columns as in original
final_df['sentiment'] = ''
final_df['semana'] = ''   # you can optionally enable format_week_range(final_df['date_utc']) if you prefer
final_df['tag'] = ''
final_df['country'] = final_df['country'].replace({'ar': 'Argentina', 'cl': 'Chile', 'pe': 'Peru'})

# Format scraped_at (make it readable)
try:
    final_df['scraped_at'] = pd.to_datetime(final_df['scraped_at'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M').fillna('')
except Exception:
    final_df['scraped_at'] = final_df['scraped_at'].astype(str).fillna('')

# Ensure column order and presence (adds missing columns as empty strings)
header = ['semana','date_utc','country','title','link','domain','snippet','tag','sentiment','scraped_at']
final_df = final_df.reindex(columns=header, fill_value='')

# --- Read existing sheet and combine ---
try:
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range="Data!A:J").execute()
    values = result.get("values", [])
except HttpError as e:
    logging.exception("Failed to read existing sheet: %s", e)
    values = []
except Exception as e:
    logging.exception("Failed to read existing sheet: %s", e)
    values = []

if values:
    # values[0] are headers from the sheet; convert to DataFrame (all strings)
    try:
        existing_df = pd.DataFrame(values[1:], columns=values[0])
        # Normalize columns to match our header set
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
# Replace problematic values: NaN, NaT, None -> ''
combined_df = combined_df.replace([np.nan, pd.NaT, None], '')
combined_df = combined_df.replace([np.inf, -np.inf], '')

# Convert numpy types & other types to plain Python types and strings.
def sanitize_cell(cell):
    # Replace floats that are NaN/Inf
    if isinstance(cell, float):
        if math.isnan(cell) or math.isinf(cell):
            return ''
        # otherwise use the float as-is but cast to python float
        return float(cell)
    # numpy scalar types
    if isinstance(cell, (np.integer, np.int64, np.int32, np.int16)):
        return int(cell)
    if isinstance(cell, (np.floating, np.float64, np.float32)):
        fv = float(cell)
        if math.isnan(fv) or math.isinf(fv):
            return ''
        return fv
    if isinstance(cell, (np.bool_)):
        return bool(cell)
    # For Timestamp / datetime / pandas types:
    if hasattr(cell, 'to_pydatetime'):
        try:
            return cell.to_pydatetime().isoformat()
        except Exception:
            return str(cell)
    # default: convert to string but avoid "nan"/"NaT"
    s = str(cell)
    if s.lower() in ('nan', 'nat', 'none', 'nan.0'):
        return ''
    return s

values_rows = []
for row in combined_df[header].values.tolist():
    sanitized_row = [sanitize_cell(cell) for cell in row]
    values_rows.append(sanitized_row)

# Convert all to strings (Sheets accepts strings; numbers as strings are OK, or you can leave numeric types)
values_rows = [[str(cell) for cell in row] for row in values_rows]

# Build body - header row + data rows
body_values = [header] + values_rows

# --- Quick check for serializability (pre-flight) ---
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
    # Do not raise immediately; we'll still attempt to write — but it's safer to fail fast in CI:
    # raise RuntimeError("Found unserializable cells; see logs above.")

# --- Write to sheet with diagnostics on HttpError ---
try:
    logging.info("Clearing target range Data!A:J ...")
    sheet.values().clear(spreadsheetId=SPREADSHEET_ID, range="Data!A:J").execute()
    logging.info("Updating sheet with %d rows (incl header)...", len(body_values))
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range="Data!A1",
        valueInputOption="RAW",
        body={"values": body_values}
    ).execute()
    logging.info("✅ Hoja actualizada sin duplicados.")
except HttpError as e:
    logging.exception("❌ HttpError writing to Sheets: %s", e)
    # small payload preview to help debugging (don't dump entire payload if large)
    sample_preview = {
        "first_rows": body_values[:5],
        "rows_count": len(body_values),
        "bad_cells_count": len(bad_cells),
    }
    logging.error("Payload preview (first 5 rows): %s", json.dumps(sample_preview, ensure_ascii=False, indent=2))
    # re-raise so CI detects failure
    raise
except Exception as e:
    logging.exception("Unexpected error writing to Sheets: %s", e)
    raise

# --- Done ---
logging.info("Script finished successfully.")
