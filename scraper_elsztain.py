# -*- coding: utf-8 -*-
# Pipeline Google News -> filtro contenido -> Google Sheets
# Caso: "Eduardo Elsztain" solo en Argentina
#
# Requiere variables de entorno:
# - GOOGLE_CREDENTIALS (JSON del service account con acceso a la Sheet)
# - APIFY_TOKEN (token de Apify)
#
# Google Sheet columnas: date_utc, title, link, source, snippet, sentiment, scraped_at

from googleapiclient.discovery import build
from google.oauth2 import service_account
import pandas as pd
import os
from apify_client import ApifyClient
from datetime import datetime
import json
import pytz
import requests
from bs4 import BeautifulSoup
import re
import unicodedata
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter, Retry
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

# ---------------------------
# Configuración general
# ---------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("elsztain-news")

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Credenciales Google desde env GOOGLE_CREDENTIALS (JSON)
creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)

# ID de la hoja (mismo que usabas; cambiá si corresponde)
SPREADSHEET_ID = '1du5Cx3pK1LnxoVeBXTzP-nY-OSvflKXjJZw2Lq-AE14'

service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

# Token Apify
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
apify_client = ApifyClient(APIFY_TOKEN)

# Actor de Google News
ACTOR_ID = "easyapi/google-news-scraper"

# Solo Argentina + queries sobre Eduardo Elsztain
COUNTRIES = ["ar"]
QUERIES = ['"eduardo elsztain"', "eduardo elsztain"]  # con y sin comillas

# Zona horaria de Argentina
TZ_ARGENTINA = pytz.timezone("America/Argentina/Buenos_Aires")

# Columnas de la nueva Sheet
HEADER = ['date_utc', 'title', 'link', 'source', 'snippet', 'sentiment', 'scraped_at']

# ---------------------------
# Helpers
# ---------------------------
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

def canonical_url(u: str) -> str:
    """Remueve parámetros de tracking para mejorar el dedupe."""
    try:
        p = urlparse(u)
        q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
             if not k.lower().startswith(('utm_', 'fbclid', 'gclid'))]
        return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(q, doseq=True), ''))
    except Exception:
        return u

def normalize_text(s: str) -> str:
    """Normaliza acentos/espacios/guiones para robustecer matching."""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r'[\u2010-\u2015\u2212\uFE58\uFE63\uFF0D-]+', '-', s)  # guiones raros -> '-'
    s = re.sub(r'\s+', ' ', s)
    return s.lower()

# Coincide con "eduardo elsztain" y también "elsztain" solo
PATRON_NOMBRE = re.compile(r'\beduardo\s+elsztain\b|\belsztain\b', re.IGNORECASE)

# Session con retries/backoff
session = requests.Session()
retries = Retry(total=3, backoff_factor=0.6, status_forcelist=(429, 500, 502, 503, 504))
session.mount("http://", HTTPAdapter(max_retries=retries))
session.mount("https://", HTTPAdapter(max_retries=retries))

def contiene_elsztain(url: str) -> bool:
    """Verifica que el cuerpo/título mencione a Eduardo Elsztain."""
    try:
        resp = session.get(url, timeout=10, headers={"User-Agent": UA})
        if resp.status_code != 200 or not resp.content:
            return False
        soup = BeautifulSoup(resp.content, "html.parser")
        textos = []
        for tag in ("title", "h1", "h2", "h3", "p"):
            textos.extend(el.get_text(separator=" ", strip=True) for el in soup.find_all(tag))
        texto = normalize_text(" ".join(textos))
        return bool(PATRON_NOMBRE.search(texto))
    except Exception as e:
        log.warning(f"Error al procesar {url}: {e}")
        return False

def ensure_source_column(df: pd.DataFrame) -> pd.DataFrame:
    """Asegura columna 'source': usa 'source' si viene del actor; si no, renombra 'domain'; sino, parsea del link."""
    if 'source' in df.columns and df['source'].notna().any():
        return df
    if 'domain' in df.columns:
        df = df.rename(columns={'domain': 'source'})
    else:
        df['source'] = df.get('link', pd.Series(dtype=str)).apply(
            lambda x: urlparse(x).netloc if isinstance(x, str) and x else ''
        )
    return df

# ---------------------------
# Scrape con Apify
# ---------------------------
all_dfs = []
for query in QUERIES:
    for country in COUNTRIES:
        run_input = {
            "cr": country,           # Country restrict
            "gl": country,           # Geolocalización
            "hl": "es-419",          # UI language
            "lr": "lang_es",         # Solo resultados en español
            "maxItems": 300,         # suficiente para last_hour
            "query": query,
            "time_period": "last_hour",
        }
        log.info(f"Ejecutando {ACTOR_ID} para {country} con query '{query}'...")
        try:
            run = apify_client.actor(ACTOR_ID).call(run_input=run_input)
        except Exception as e:
            log.error(f"No se pudo ejecutar actor para {country} con '{query}': {e}")
            continue

        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            log.warning(f"Sin dataset para {country} - '{query}'")
            continue

        items = apify_client.dataset(dataset_id).list_items().items
        if not items:
            log.warning(f"Sin resultados para {country} - '{query}'")
            continue

        df = pd.DataFrame(items)

        # Normalizaciones tempranas + dedupe
        if 'link' not in df.columns:
            log.warning("Dataset sin columna 'link'; se omite.")
            continue

        df['link'] = df['link'].astype(str).map(canonical_url)
        df.drop_duplicates(subset=["link"], inplace=True)

        # Aseguramos 'source'
        df = ensure_source_column(df)

        # Timestamp de scrape (AR) para salida final
        df["scraped_at"] = datetime.now(TZ_ARGENTINA).isoformat()

        all_dfs.append(df)

if not all_dfs:
    log.error("No se obtuvieron resultados de ningún país/query.")
    raise SystemExit(0)

# ---------------------------
# DataFrame combinado y fechas
# ---------------------------
final_df = pd.concat(all_dfs, ignore_index=True)
final_df.drop_duplicates(subset=["link"], inplace=True)

# Parseo robusto de date_utc -> AR -> string dd/mm/YYYY
final_df['date_utc'] = pd.to_datetime(final_df.get('date_utc', pd.Series(dtype=str)), utc=True, errors='coerce')
mask = final_df['date_utc'].notna()
final_df.loc[mask, 'date_utc'] = final_df.loc[mask, 'date_utc'].dt.tz_convert(TZ_ARGENTINA)
final_df['date_utc'] = final_df['date_utc'].dt.strftime('%d/%m/%Y')

# scraped_at uniforme (string local AR dd/mm/YYYY HH:MM)
final_df['scraped_at'] = pd.to_datetime(final_df['scraped_at'], errors='coerce')
final_df['scraped_at'] = final_df['scraped_at'].dt.tz_convert(TZ_ARGENTINA).dt.strftime('%d/%m/%Y %H:%M')

# ---------------------------
# Filtro por contenido (paralelo)
# ---------------------------
log.info("Filtrando noticias que realmente mencionan 'Eduardo Elsztain'...")
links = final_df["link"].tolist()
results = {}

max_workers = min(16, max(4, len(links)//5)) if len(links) > 10 else min(8, len(links) or 1)

with ThreadPoolExecutor(max_workers=max_workers or 1) as ex:
    future_to_url = {ex.submit(contiene_elsztain, u): u for u in links}
    for fut in as_completed(future_to_url):
        u = future_to_url[fut]
        ok = False
        try:
            ok = fut.result()
        except Exception as e:
            log.warning(f"Future error {u}: {e}")
        results[u] = ok

final_df = final_df[final_df["link"].map(results)].copy()

if final_df.empty:
    log.warning("Tras el filtro de contenido, no quedaron resultados relevantes.")

# ---------------------------
# Asegurar columnas y tipos
# ---------------------------
# Si Apify trae 'title'/'snippet' con NaN, convertir a str; si faltan columnas, crearlas vacías
for col in HEADER:
    if col not in final_df.columns:
        final_df[col] = ''

# Renombrar a 'source' si viniera como 'domain' (por si quedó algún df sin normalizar)
if 'domain' in final_df.columns and 'source' not in final_df.columns:
    final_df.rename(columns={'domain': 'source'}, inplace=True)

final_df = final_df.astype({c: str for c in final_df.columns})
final_df = final_df.reindex(columns=HEADER, fill_value='')

# ---------------------------
# Leer hoja existente
# ---------------------------
log.info("Leyendo hoja existente...")
result = sheet.values().get(
    spreadsheetId=SPREADSHEET_ID,
    range="Data!A:G"  # 7 columnas: A..G
).execute()
values = result.get("values", [])

if values:
    existing_df = pd.DataFrame(values[1:], columns=values[0])
else:
    existing_df = pd.DataFrame(columns=HEADER)

# ---------------------------
# Concatenar y deduplicar por link
# ---------------------------
combined_df = pd.concat([existing_df, final_df], ignore_index=True)
combined_df.drop_duplicates(subset=["link"], inplace=True)

# ---------------------------
# Escribir a Google Sheets
# ---------------------------
log.info("Escribiendo datos en Google Sheets...")
sheet.values().clear(
    spreadsheetId=SPREADSHEET_ID,
    range="NOTICIAS!A:G"
).execute()

sheet.values().update(
    spreadsheetId=SPREADSHEET_ID,
    range="NOTICIAS!A1",
    valueInputOption="RAW",
    body={"values": [HEADER] + combined_df.astype(str).values.tolist()}
).execute()

log.info("✅ Hoja actualizada sin duplicados.")
