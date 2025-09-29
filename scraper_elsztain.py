# -*- coding: utf-8 -*-
"""
Pipeline Google News -> filtro contenido -> Google Sheets
Caso: "Eduardo Elsztain" solo en Argentina

Env vars requeridas:
- GOOGLE_CREDENTIALS (JSON del service account con acceso a la Sheet)
- APIFY_TOKEN (token de Apify)

Google Sheet columnas (exactas):
date_utc, title, link, source, snippet, sentiment, scraped_at
"""
from __future__ import annotations

from typing import List, Dict
import os
import json
import logging
import re
import unicodedata
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry

from apify_client import ApifyClient
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ---------------------------
# Configuración general
# ---------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("elsztain-news")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
if not GOOGLE_CREDENTIALS:
    raise RuntimeError("Falta GOOGLE_CREDENTIALS en variables de entorno.")
try:
    CREDS_DICT = json.loads(GOOGLE_CREDENTIALS)
except json.JSONDecodeError as e:
    raise RuntimeError("GOOGLE_CREDENTIALS no es JSON válido.") from e

APIFY_TOKEN = os.getenv("APIFY_TOKEN")
if not APIFY_TOKEN:
    raise RuntimeError("Falta APIFY_TOKEN en variables de entorno.")

creds = service_account.Credentials.from_service_account_info(CREDS_DICT, scopes=SCOPES)

# IDs y constantes
SPREADSHEET_ID = "1DTMBII9byTfx9KU6M1QghhlU8abCRh8rKThcnaTbzpE"
SHEET_TAB = "NOTICIAS"
HEADER = ["date_utc", "title", "link", "source", "snippet", "sentiment", "scraped_at"]

# Google Sheets API
service = build("sheets", "v4", credentials=creds, cache_discovery=False)
sheet = service.spreadsheets()

# Apify
apify_client = ApifyClient(APIFY_TOKEN)
ACTOR_ID = "easyapi/google-news-scraper"

# Filtros
COUNTRIES = ["ar"]
QUERIES = ['"eduardo elsztain"', "eduardo elsztain"]

# Zona horaria de Argentina
TZ_ARG = pytz.timezone("America/Argentina/Buenos_Aires")

# HTTP cliente con retries
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
session = requests.Session()
retries = Retry(
    total=3,
    backoff_factor=0.6,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "HEAD"]),
)
session.mount("http://", HTTPAdapter(max_retries=retries))
session.mount("https://", HTTPAdapter(max_retries=retries))
DEFAULT_TIMEOUT = 12  # s
MIN_HTML_BYTES = 256  # rechaza respuestas vacías

# ---------------------------
# Helpers
# ---------------------------
def canonical_url(u: str) -> str:
    """Remueve parámetros de tracking para mejorar el dedupe."""
    try:
        p = urlparse(u)
        q = [
            (k, v)
            for k, v in parse_qsl(p.query, keep_blank_values=True)
            if not k.lower().startswith(("utm_", "fbclid", "gclid"))
        ]
        netloc = p.netloc.replace(":80", "").replace(":443", "")
        return urlunparse((p.scheme, netloc, p.path or "/", p.params, urlencode(q, doseq=True), ""))
    except Exception:
        return u

def normalize_text(s: str) -> str:
    """Normaliza acentos/espacios/guiones para robustecer matching."""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[\u2010-\u2015\u2212\uFE58\uFE63\uFF0D-]+", "-", s)  # guiones raros -> '-'
    s = re.sub(r"\s+", " ", s)
    return s.lower().strip()

PATRON_NOMBRE = re.compile(r"\beduardo\s+elsztain\b|\belsztain\b", re.IGNORECASE)

def ensure_source_column(df: pd.DataFrame) -> pd.DataFrame:
    """Asegura columna 'source'."""
    if "source" in df.columns and df["source"].notna().any():
        return df
    if "domain" in df.columns:
        df = df.rename(columns={"domain": "source"})
    else:
        df["source"] = df.get("link", pd.Series(dtype=str)).apply(
            lambda x: urlparse(x).netloc if isinstance(x, str) and x else ""
        )
    return df

def is_probably_html(url: str) -> bool:
    """Evita PDFs/imagenes antes de descargar cuerpo completo."""
    try:
        head = session.head(url, timeout=DEFAULT_TIMEOUT, headers={"User-Agent": UA}, allow_redirects=True)
        ctype = head.headers.get("Content-Type", "").lower()
        if "text/html" in ctype or ctype == "":
            return True
        return False
    except Exception:
        # si falla HEAD, seguimos y dejamos que GET lo determine
        return True

def page_mentions_elsztain(url: str) -> bool:
    """Verifica que el cuerpo/título mencione a Eduardo Elsztain."""
    try:
        if not is_probably_html(url):
            return False

        resp = session.get(url, timeout=DEFAULT_TIMEOUT, headers={"User-Agent": UA})
        if resp.status_code != 200:
            return False
        if not resp.content or len(resp.content) < MIN_HTML_BYTES:
            return False

        soup = BeautifulSoup(resp.content, "html.parser")

        textos: List[str] = []
        for tag in ("title", "h1", "h2", "h3", "p"):
            for el in soup.find_all(tag):
                textos.append(el.get_text(separator=" ", strip=True))

        text = normalize_text(" ".join(textos))
        return bool(PATRON_NOMBRE.search(text))
    except Exception as e:
        log.warning(f"Error al procesar {url}: {e}")
        return False

def prefilter_row_mentions(row: pd.Series) -> bool:
    """Prefiltro barato usando los campos del actor (sin salir a la web)."""
    for col in ("title", "snippet"):
        if col in row and isinstance(row[col], str) and row[col]:
            if PATRON_NOMBRE.search(row[col]):
                return True
    return False

def list_all_items(dataset_id: str, batch: int = 1000) -> List[dict]:
    """Paginar datasets grandes de Apify."""
    items: List[dict] = []
    offset = 0
    while True:
        page = apify_client.dataset(dataset_id).list_items(limit=batch, offset=offset)
        part = page.items or []
        items.extend(part)
        if len(part) < batch:
            break
        offset += batch
    return items

# ---------------------------
# Scrape con Apify
# ---------------------------
all_dfs: List[pd.DataFrame] = []
for query in QUERIES:
    for country in COUNTRIES:
        run_input = {
            "cr": country,
            "gl": country,
            "hl": "es-419",
            "lr": "lang_es",
            "maxItems": 300,
            "query": query,
            "time_period": "last_year",
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

        try:
            items = list_all_items(dataset_id)
        except Exception as e:
            log.error(f"No se pudo listar dataset {dataset_id}: {e}")
            continue

        if not items:
            log.info(f"Sin resultados para {country} - '{query}'")
            continue

        df = pd.DataFrame(items)

        if "link" not in df.columns:
            log.warning("Dataset sin columna 'link'; se omite.")
            continue

        # Normalizaciones + dedupe
        df["link"] = df["link"].astype(str).map(canonical_url)
        df.drop_duplicates(subset=["link"], inplace=True)

        # Asegurar "source"
        df = ensure_source_column(df)

        # Timestamps
        now_utc = datetime.now(timezone.utc)
        # date_utc -> ISO UTC (si viene nulo, queda cadena vacía luego)
        if "date_utc" in df.columns:
            dt = pd.to_datetime(df["date_utc"], utc=True, errors="coerce")
            df["date_utc"] = dt.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            df["date_utc"] = ""

        # scraped_at -> local AR dd/mm/YYYY HH:MM
        df["scraped_at"] = now_utc.astimezone(TZ_ARG).strftime("%d/%m/%Y %H:%M")

        # sentiment placeholder si no viene
        if "sentiment" not in df.columns:
            df["sentiment"] = ""

        all_dfs.append(df)

if not all_dfs:
    log.error("No se obtuvieron resultados de ningún país/query.")
    raise SystemExit(0)

# ---------------------------
# DataFrame combinado y prefiltro
# ---------------------------
final_df = pd.concat(all_dfs, ignore_index=True)
final_df.drop_duplicates(subset=["link"], inplace=True)

# Prefiltro por título/snippet
if not final_df.empty:
    mask_pref = final_df.apply(prefilter_row_mentions, axis=1)
    prefiltered = final_df[mask_pref].copy()
    if prefiltered.empty:
        prefiltered = final_df.copy()
else:
    prefiltered = final_df

# ---------------------------
# Filtro por contenido (HTTP en paralelo)
# ---------------------------
from concurrent.futures import ThreadPoolExecutor, as_completed

links = prefiltered["link"].tolist()
n = len(links)
if n == 0:
    log.warning("No hay links para verificar en sitio.")
    filtered = prefiltered.copy()
else:
    max_workers = max(4, min(16, (n // 6) + 1))
    log.info(f"Verificando contenido en sitio (threads={max_workers}, {n} urls)...")

    results: Dict[str, bool] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(page_mentions_elsztain, u): u for u in links}
        for fut in as_completed(futures):
            u = futures[fut]
            ok = False
            try:
                ok = fut.result()
            except Exception as e:
                log.warning(f"Future error {u}: {e}")
            results[u] = ok

    filtered = prefiltered[prefiltered["link"].map(results).fillna(False)].copy()

if filtered.empty:
    log.warning("Tras el filtro de contenido, no quedaron resultados relevantes.")

# ---------------------------
# Asegurar columnas, tipos y orden final (exactamente 7)
# ---------------------------
for col in HEADER:
    if col not in filtered.columns:
        filtered[col] = ""
final_out = filtered.astype({c: str for c in filtered.columns}).reindex(columns=HEADER, fill_value="")

# ---------------------------
# Leer hoja existente y crear headers si falta
# ---------------------------
read_range = f"{SHEET_TAB}!A:G"
log.info(f"Leyendo hoja existente: {read_range} ...")
try:
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=read_range).execute()
    values = result.get("values", [])
except Exception as e:
    log.warning(f"No se pudo leer la hoja (se asumirá vacía): {e}")
    values = []

if not values:
    log.info("Hoja vacía, creando encabezados...")
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_TAB}!A1",
        valueInputOption="RAW",
        body={"values": [HEADER]},
    ).execute()
    existing_df = pd.DataFrame(columns=HEADER)
else:
    existing_df = pd.DataFrame(values[1:], columns=values[0])

# ---------------------------
# Append SOLO filas nuevas (por link)
# ---------------------------
if existing_df.empty:
    new_rows = final_out
else:
    new_rows = final_out.loc[~final_out["link"].isin(existing_df["link"])]

if not new_rows.empty:
    log.info(f"Agregando {len(new_rows)} filas nuevas...")
    sheet.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_TAB}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": new_rows.astype(str).values.tolist()},
    ).execute()
else:
    log.info("No hay filas nuevas para agregar.")
