from googleapiclient.discovery import build
from google.oauth2 import service_account
import pandas as pd
import os
from apify_client import ApifyClient
from datetime import datetime
import json
import pytz
import sys
import time
import logging

# Config logging simple
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1du5Cx3pK1LnxoVeBXTzP-nY-OSvflKXjJZw2Lq-AE14")

# Validate GOOGLE_CREDENTIALS present
creds_json = os.getenv("GOOGLE_CREDENTIALS")
if not creds_json:
    logging.error("Falta GOOGLE_CREDENTIALS en las variables de entorno.")
    sys.exit(1)

try:
    creds_dict = json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
except Exception as e:
    logging.exception("Error parsing GOOGLE_CREDENTIALS: %s", e)
    sys.exit(1)

service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

# Apify token desde secret
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
if not APIFY_TOKEN:
    logging.error("Falta APIFY_TOKEN en las variables de entorno.")
    sys.exit(1)

apify_client = ApifyClient(APIFY_TOKEN)

# Actor de Google News
ACTOR_ID = "easyapi/google-news-scraper"

# Lista de países y queries
COUNTRIES = ["ar", "cl", "pe"]
QUERIES = [
    "tik-tok", "tiktok", "tiktok suicidio", "tiktok grooming", "tiktok armas",
    "tiktok drogas", "tiktok violacion", "tiktok delincuentes", "tiktok ladrones",
    "tiktok narcos"
]

# Zona horaria
TZ_ARGENTINA = pytz.timezone("America/Argentina/Buenos_Aires")

# helper: simple retry decorator
def retry(fn, tries=3, delay=5):
    for attempt in range(1, tries + 1):
        try:
            return fn()
        except Exception as e:
            logging.warning("Intento %d/%d falló: %s", attempt, tries, e)
            if attempt == tries:
                raise
            time.sleep(delay)

# === Scraping con Apify ===
all_dfs = []

for query in QUERIES:
    for country in COUNTRIES:
        run_input = {
            "cr": country,
            "gl": country,
            "hl": "es-419",
            "lr": "lang_es",
            "maxItems": 5000,
            "query": query,
            "time_period": "last_hour",
        }
        logging.info("Ejecutando actor %s para %s con query '%s'", ACTOR_ID, country, query)
        try:
            # Llamada con retry
            run = retry(lambda: apify_client.actor(ACTOR_ID).call(run_input=run_input), tries=3, delay=5)
        except Exception as e:
            logging.error("Error al ejecutar actor para %s con query '%s': %s", country, query, e)
            continue

        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            logging.warning("No dataset generado para %s - '%s'", country, query)
            continue

        # Obtener items (paginado interno del cliente)
        try:
            items = retry(lambda: apify_client.dataset(dataset_id).list_items().items, tries=3, delay=3)
        except Exception as e:
            logging.error("Error list_items para dataset %s: %s", dataset_id, e)
            continue

        if not items:
            logging.info("No hay resultados para %s - '%s'", country, query)
            continue

        df = pd.DataFrame(items)
        # Añadir columnas básicas si faltan
        if 'link' not in df.columns:
            df['link'] = ''
        df["country"] = country
        # Fecha scraped_at timezone-aware
        df["scraped_at"] = datetime.now(TZ_ARGENTINA).isoformat()
        all_dfs.append(df)

if not all_dfs:
    logging.error("No se obtuvieron resultados de ningún país.")
    sys.exit(0)

# === Funciones de formateo ===
def safe_parse_date_series(s):
    # intenta parsear distintos formatos y devuelve fechas timezone-converted
    parsed = pd.to_datetime(s, utc=True, errors='coerce')  # si ya viene en UTC
    return parsed

def format_week_range_from_dt(dt):
    # dt: Timestamp (naive o tz-aware)
    if pd.isna(dt):
        return ''
    if dt.tzinfo is None:
        try:
            dt = TZ_ARGENTINA.localize(dt)
        except Exception:
            dt = dt
    monday = dt - pd.Timedelta(days=int(dt.weekday()))
    sunday = monday + pd.Timedelta(days=6)
    month_abbr = calendar.month_abbr[monday.month].upper()
    return f"{monday.day:02d}–{sunday.day:02d} {month_abbr} {monday.year}"

# === DataFrame final de todo lo nuevo ===
final_df = pd.concat(all_dfs, ignore_index=True)
final_df.drop_duplicates(subset=["link"], inplace=True)

# Convertir date_utc (si existe) a timezone Argentina y formatear
if 'date_utc' in final_df.columns:
    final_df['date_utc_parsed'] = safe_parse_date_series(final_df['date_utc'])
    # convertimos a TZ local si viene con tz
    final_df['date_local'] = final_df['date_utc_parsed'].dt.tz_convert(TZ_ARGENTINA)
    # Formatear dd/mm/YYYY
    final_df['date_utc'] = final_df['date_local'].dt.strftime('%d/%m/%Y')
else:
    final_df['date_utc'] = ''

# columnas adicionales
final_df['sentiment'] = ''
final_df['semana'] = final_df['date_local'].apply(lambda x: format_week_range_from_dt(x) if not pd.isna(x) else '')
final_df['tag'] = ''
final_df['country'] = final_df['country'].replace({'ar': 'Argentina', 'cl': 'Chile', 'pe': 'Peru'})

# scraped_at: convertir a formato legible
final_df['scraped_at'] = pd.to_datetime(final_df['scraped_at'])
final_df['scraped_at'] = final_df['scraped_at'].dt.tz_localize(None).dt.strftime('%d/%m/%Y %H:%M')

# Orden de columnas garantizado
header = ['semana','date_utc','country','title','link','domain','snippet','tag','sentiment','scraped_at']
final_df = final_df.reindex(columns=header, fill_value='')

# === Leer registros existentes en la hoja con manejo robusto ===
try:
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range="Data!A:J").execute()
    values = result.get("values", [])
except Exception as e:
    logging.exception("Error leyendo la hoja: %s", e)
    values = []

if values:
    # la hoja trae todo como strings: construimos dataframe respetando header si existe
    current_header = values[0]
    rows = values[1:]
    try:
        existing_df = pd.DataFrame(rows, columns=current_header)
    except Exception:
        # fallback si columnas distintas: crear DF vacío con header
        existing_df = pd.DataFrame(columns=header)
else:
    existing_df = pd.DataFrame(columns=header)

# Asegurar las mismas columnas y tipos (convertir a string para concatenar de forma estable)
existing_df = existing_df.reindex(columns=header, fill_value='')
existing_df = existing_df.astype(str)
final_to_write = final_df.astype(str)

# === Concatenar y limpiar duplicados ===
combined_df = pd.concat([existing_df, final_to_write], ignore_index=True)
combined_df.drop_duplicates(subset=["link"], inplace=True)

# Preparar payload para Sheets (limitar tamaño si es muy grande)
values_payload = [header] + combined_df.values.tolist()

# === Escribir en la hoja con clear + update con manejo de errores ===
try:
    sheet.values().clear(spreadsheetId=SPREADSHEET_ID, range="Data!A:J").execute()
    # Hacer la actualización en bloques si es muy grande (aquí simple)
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range="Data!A1",
        valueInputOption="RAW",
        body={"values": values_payload}
    ).execute()
    logging.info("Hoja actualizada sin duplicados. Filas totales: %d", len(combined_df))
except Exception as e:
    logging.exception("Error al escribir en la hoja: %s", e)
    sys.exit(1)
