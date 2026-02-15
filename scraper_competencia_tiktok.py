#!/usr/bin/env python3
"""
robust_scraper_fixed.py

Versión revisada y con defensas adicionales del scraper Google News -> Google Sheets.
"""
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.errors import HttpError
import google.generativeai as genai
from bs4 import BeautifulSoup
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
ACTOR_ID = os.getenv("ACTOR_ID", "easyapi/google-news-scraper")

# --- Gemini config ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    logging.error("Missing GEMINI_API_KEY environment variable. Exiting.")
    sys.exit(1)

genai.configure(api_key=GEMINI_API_KEY)
GEMINI_MODEL_NAME = os.getenv("GEMINI_MODEL_NAME", "gemini-2.0-flash")
model = genai.GenerativeModel(GEMINI_MODEL_NAME)

if not GOOGLE_CREDENTIALS_ENV:
    logging.error("Missing GOOGLE_CREDENTIALS environment variable. Exiting.")
    sys.exit(1)

if not APIFY_TOKEN:
    logging.error("Missing APIFY_TOKEN environment variable. Exiting.")
    sys.exit(1)

# Optional tunables via env
COUNTRIES = [c.strip() for c in os.getenv("COUNTRIES", "ar,cl,pe").split(",") if c.strip()]
QUERIES = [q.strip() for q in (os.getenv("QUERIES") or "youtube,google,instagram,facebook,snapchat,twitter,twitch").split(",") if q.strip()]


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

# Map short country codes to names (defensivo)
final_df['country'] = final_df['country'].replace({'ar': 'Argentina', 'cl': 'Chile', 'pe': 'Peru'})

# Format scraped_at (legible)
try:
    final_df['scraped_at'] = pd.to_datetime(final_df['scraped_at'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M').fillna('')
except Exception:
    final_df['scraped_at'] = final_df['scraped_at'].astype(str).fillna('')

# Ensure column order and presence
header = ['date_utc','country','title','link','domain','source','snippet','tag','sentiment','scraped_at']
final_df = final_df.reindex(columns=header)
final_df = final_df.fillna('')

#keep rows that contain youtube, facebook, instagram, snapchat, twitter, twitch, google in the title or snippet (case-insensitive)
keywords = ['youtube', 'facebook', 'instagram', 'snapchat', 'twitter', 'twitch', 'google']
pattern = '|'.join(keywords)
final_df = final_df[final_df['title'].str.contains(pattern, case=False, na=False) | final_df['snippet'].str.contains(pattern, case=False, na=False)]

# --- Read existing sheet and combine ---
SHEET_RANGE = "Competencia!A:J"  # cambia si corresponde
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
        existing_df = existing_df.reindex(columns=header)
        existing_df = existing_df.fillna('')

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

def clasificar_sentiment_noticia(url):
    try:
        # Descargar la página
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")

        # Extraer solo el texto visible
        paragraphs = [p.get_text() for p in soup.find_all("p")]
        texto = " ".join(paragraphs)  

        # Prompt claro y forzado a solo una palabra
        prompt = f"""
         ROL

Actúa como Analista Senior de PR/Reputación. Tu única tarea es determinar si la noticia

es POSITIVA, NEGATIVA o NEUTRA respecto a la reputación del medio/red social como empresa/plataforma.

INSTRUCCIONES (leer atentamente)

- Analiza SOLO el texto provisto.

- Responde únicamente con UNA de las tres palabras EXACTAS (en mayúsculas): POSITIVO, NEGATIVO o NEUTRO.

- No añadas puntuación, explicaciones ni ningún otro texto.

- Si no puedes clasificar por falta de información, responde EXACTAMENTE: NEUTRO

- Respuestas aceptadas: ['POSITIVO','NEGATIVO','NEUTRO']
        {texto}
        """

        # Usar el modelo que ya inicializaste afuera
        response = model.generate_content(prompt)
        resultado = response.text.strip().upper()

        # Validación por seguridad
        if resultado not in ["POSITIVO", "NEGATIVO", "NEUTRO"]:
            return "NEUTRO"
        return resultado

    except Exception as e:
        print(f"Error procesando {url}: {e}")
        return "NEUTRO"

# Crear columna con sentimiento
combined_df["sentiment"] = combined_df["link"].apply(clasificar_sentiment_noticia)

# clasificar tag

def clasificar_tag_noticia(url):
    try:
        # Descargar la página
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")

        # Extraer solo el texto visible
        paragraphs = [p.get_text() for p in soup.find_all("p")]
        texto = " ".join(paragraphs)  

        # Prompt claro y forzado a solo una palabra
        prompt = f"""
        ROL
Actúa como un Analista de Datos Senior especializado en PR y Reputación Corporativa de empresas de redes sociales.
Tu única misión es clasificar la noticia en UNA sola categoría estratégica.

OBJETIVO
Determinar cuál es el eje principal de la noticia en relación con TikTok como empresa.

CATEGORÍAS DISPONIBLES (elige SOLO UNA)
- Consumer & Brand
- Music
- B2B
- SMB
- Creator
- Product
- TnS
- Corporate Reputation

REGLA DE PRIORIDAD (OBLIGATORIA)
Si la noticia impacta la imagen institucional, legal o regulatoria de la empresa,
la categoría SIEMPRE es: Corporate Reputation.

INSTRUCCIONES CRÍTICAS (LEER ATENTAMENTE)
1) ANALIZA la noticia provista abajo.
2) RESPONDE EXACTAMENTE con UNA de las siguientes cadenas (sin comillas, sin punto final, sin texto extra, sin explicación): 
   {allowed_line}
3) RESPONDE SOLO con la cadena EXACTA: por ejemplo: Product  (sin comillas)
4) Si por alguna razón NO PUEDES CLASIFICAR (texto ausente o incompleto), RESPONDE EXACTAMENTE: Corporate Reputation
5) NO agregues ninguna otra palabra, puntuación ni carácter.
6) Respuestas aceptadas: [Consumer & Brand, Music, B2B, SMB, Creator, Product, TnS, Corporate Reputation]

NOTICIA:
        {texto}
        """

        # Usar el modelo que ya inicializaste afuera
        response = model.generate_content(prompt)
        resultado = response.text.strip().upper()

    except Exception as e:
        print(f"Error procesando {url}: {e}")
        return "Corporate Reputation"

# Crear columna con sentimiento
combined_df["tag"] = df["link"].apply(clasificar_tag_noticia)


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
        range="Competencia!A1",
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
