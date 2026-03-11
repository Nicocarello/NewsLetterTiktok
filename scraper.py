# -*- coding: utf-8 -*-
"""
Pipeline Google News -> TikTok queries -> Google Sheets
Añade columna 'tag' (categoría) usando Gemini (Google Generative AI)

Requerimientos (env vars):
- GOOGLE_CREDENTIALS (JSON del service account)
- APIFY_TOKEN
- GEMINI_API_KEY
"""
from __future__ import annotations

import os
import json
import time
import random
import logging
import re
import unicodedata
from datetime import datetime
from typing import List, Dict, Callable, Any
import threading
from threading import Semaphore, Lock

import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry

from apify_client import ApifyClient
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Gemini
import google.generativeai as genai

# ---------- Config / Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tiktok-news")

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Env vars
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
if not GOOGLE_CREDENTIALS:
    raise RuntimeError("Falta GOOGLE_CREDENTIALS en variables de entorno.")
try:
    creds_dict = json.loads(GOOGLE_CREDENTIALS)
except Exception as e:
    raise RuntimeError("GOOGLE_CREDENTIALS no es JSON válido.") from e

APIFY_TOKEN = os.getenv("APIFY_TOKEN")
if not APIFY_TOKEN:
    raise RuntimeError("Falta APIFY_TOKEN en variables de entorno.")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise RuntimeError("Falta GEMINI_API_KEY en variables de entorno.")

# ---------- Google Sheets init ----------
creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
SPREADSHEET_ID = '1du5Cx3pK1LnxoVeBXTzP-nY-OSvflKXjJZw2Lq-AE14'
service = build('sheets', 'v4', credentials=creds, cache_discovery=False)
sheet = service.spreadsheets()

# ---------- Apify init ----------
apify_client = ApifyClient(APIFY_TOKEN)
ACTOR_ID = "easyapi/google-news-scraper"

# ---------- Queries / Countries ----------
COUNTRIES = ["ar", "cl", "pe"]
QUERIES = ["tik-tok", "tiktok", "tiktok suicidio", "tiktok grooming", "tiktok armas",
           "tiktok drogas", "tiktok violacion", "tiktok delincuentes", "tiktok ladrones", "tiktok narcos"]

# TZ
TZ_ARGENTINA = pytz.timezone("America/Argentina/Buenos_Aires")

# HTTP / requests session (thread-local pattern)
_tls = threading.local()
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
DEFAULT_TIMEOUT = 12
MIN_HTML_BYTES = 256

def get_session() -> requests.Session:
    sess = getattr(_tls, "session", None)
    if sess is None:
        sess = requests.Session()
        retries = Retry(total=3, backoff_factor=0.6,
                        status_forcelist=(429, 500, 502, 503, 504),
                        allowed_methods=frozenset(["GET", "HEAD"]))
        adapter = HTTPAdapter(max_retries=retries)
        sess.mount("http://", adapter)
        sess.mount("https://", adapter)
        _tls.session = sess
    return sess

# ---------- Utilidades URL / HTML ----------
def canonical_url(u: str) -> str:
    try:
        from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
        p = urlparse(u.strip())
        q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if not k.lower().startswith(("utm_", "fbclid", "gclid"))]
        netloc = p.netloc.replace(":80", "").replace(":443", "")
        if netloc.startswith("www."):
            netloc = netloc[4:]
        path = p.path or "/"
        path = re.sub(r"/{2,}", "/", path)
        query = urlencode(q, doseq=True)
        return urlunparse((p.scheme or "https", netloc, path, "", query, ""))
    except Exception:
        return u or ""

_HEAD_CACHE: Dict[str, dict] = {}
def head_cached(url: str, timeout=DEFAULT_TIMEOUT) -> dict:
    if not url:
        return {"ok": False, "content_type": "", "status": None}
    if url in _HEAD_CACHE:
        return _HEAD_CACHE[url]
    try:
        h = get_session().head(url, timeout=timeout, headers={"User-Agent": UA}, allow_redirects=True)
        ctype = h.headers.get("Content-Type", "").lower()
        ok = h.status_code == 200
        info = {"ok": ok, "content_type": ctype, "status": h.status_code}
    except Exception as e:
        log.debug(f"HEAD fallo {url}: {e}")
        info = {"ok": False, "content_type": "", "status": None}
    _HEAD_CACHE[url] = info
    return info

def is_probably_html(url: str) -> bool:
    res = head_cached(url)
    ctype = res.get("content_type", "")
    return ("text/html" in ctype) or (ctype == "")

def download_html(url: str) -> str:
    try:
        head = head_cached(url)
        if not head.get("ok", False) and head.get("status") is not None:
            return ""
        ctype = head.get("content_type", "")
        if ctype and "html" not in ctype:
            return ""
        resp = get_session().get(url, timeout=DEFAULT_TIMEOUT, headers={"User-Agent": UA})
        if resp.status_code != 200:
            return ""
        if not resp.content or len(resp.content) < MIN_HTML_BYTES:
            return ""
        resp.encoding = resp.encoding or "utf-8"
        return resp.text or resp.content.decode("utf-8", errors="ignore")
    except Exception:
        return ""

VISIBLE_TAGS = {"p", "h1", "h2", "h3", "li"}
def extract_visible_text(html: str, max_chars: int = 5000) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    parts = []
    total = 0
    for tag in soup.find_all(VISIBLE_TAGS):
        t = tag.get_text(separator=" ", strip=True)
        if t:
            parts.append(t)
            total += len(t)
        if total > max_chars * 1.2:
            break
    text = " ".join(parts)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_chars]

# ---------- Gemini (categorías) setup ----------
genai.configure(api_key=GEMINI_API_KEY)
GEMINI_MODEL_NAME = "gemini-2.0-flash"
model = genai.GenerativeModel(GEMINI_MODEL_NAME)

CANONICAL_CATEGORIES = [
    "Consumer & Brand",
    "Music",
    "B2B",
    "SMB",
    "Creator",
    "Product",
    "TnS",
    "Corporate Reputation",
]

NORMALIZATION_MAP = {
    "CONSUMER & BRAND": "Consumer & Brand",
    "CONSUMER AND BRAND": "Consumer & Brand",
    "CONSUMER": "Consumer & Brand",
    "BRAND": "Consumer & Brand",
    "MUSIC": "Music",
    "B2B": "B2B",
    "SMB": "SMB",
    "CREATOR": "Creator",
    "CREATORS": "Creator",
    "PRODUCT": "Product",
    "TNS": "TnS",
    "T & S": "TnS",
    "TRUST AND SAFETY": "TnS",
    "TRUST & SAFETY": "TnS",
    "MODERATION": "TnS",
    "CORPORATE REPUTATION": "Corporate Reputation",
    "CORPORATE": "Corporate Reputation",
    "REPUTATION": "Corporate Reputation",
    "LEGAL": "Corporate Reputation",
    "REGULATORY": "Corporate Reputation",
    "REGULATION": "Corporate Reputation",
    "GOVERNMENT": "Corporate Reputation",
}

# concurrency + cache locks
llm_semaphore = Semaphore(3)  # ajustable
tag_cache_lock = Lock()
GEMINI_MIN_INTERVAL = 0.6
_last_gemini_call = 0.0

CATEGORY_CACHE_PATH = os.getenv("CATEGORY_CACHE_PATH", "category_cache.json")
try:
    if os.path.exists(CATEGORY_CACHE_PATH):
        with open(CATEGORY_CACHE_PATH, "r", encoding="utf-8") as fh:
            tag_cache = json.load(fh) or {}
    else:
        tag_cache = {}
except Exception:
    tag_cache = {}

def _atomic_write_json(path: str, data: dict) -> None:
    tmp = f"{path}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        log.debug(f"Atomic write failed for {path}: {e}")
        try:
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(data, fh, ensure_ascii=False, indent=2)
        except Exception as e2:
            log.debug(f"Fallback write also failed for {path}: {e2}")

def _sanitize_model_raw(raw: str) -> str:
    r = (raw or "").strip()
    r = r.strip(" \n\r\t\"'`.")
    r = re.sub(r"\s+", " ", r)
    return r

def _gemini_rate_limit():
    global _last_gemini_call
    now = time.time()
    wait = GEMINI_MIN_INTERVAL - (now - _last_gemini_call)
    if wait > 0:
        time.sleep(wait * (0.7 + 0.6 * random.random()))
    _last_gemini_call = time.time()

def normalize_category_from_model_output(raw_text: str) -> str:
    if not raw_text:
        return "Corporate Reputation"
    r = raw_text.strip()
    if not r:
        return "Corporate Reputation"
    # exact canonical
    for can in CANONICAL_CATEGORIES:
        if r.strip().lower() == can.lower():
            return can
    r_up = re.sub(r"[\"'\.\,]+", " ", r).upper()
    r_up = re.sub(r"\s+", " ", r_up).strip()
    keys_sorted = sorted(NORMALIZATION_MAP.keys(), key=lambda x: -len(x))
    for key in keys_sorted:
        if key in r_up:
            return NORMALIZATION_MAP[key]
    tokens = re.split(r"[\s,;:()\[\]\"'./\\-]+", r_up)
    for tok in tokens:
        tok = tok.strip()
        if not tok:
            continue
        if tok in NORMALIZATION_MAP:
            return NORMALIZATION_MAP[tok]
    for can in CANONICAL_CATEGORIES:
        if can.upper() in r_up:
            return can
    log.warning("Model output not mappable to a category (sanitized). Raw: %s", raw_text[:200])
    return "Corporate Reputation"

def build_prompt_from_text(texto: str, max_chars: int = 6000) -> str:
    t = (texto or "").strip()
    if len(t) > max_chars:
        t = t[:max_chars].rsplit(" ", 1)[0]
    allowed = ", ".join(CANONICAL_CATEGORIES)
    prompt = (
        "Eres un clasificador. Clasifica la noticia en UNA y SOLO UNA de las siguientes categorías EXACTAS:\n"
        f"{allowed}\n\n"
        "INSTRUCCIONES (OBLIGATORIO):\n"
        " - Responde EXACTAMENTE con UNA de las cadenas de la lista anterior.\n"
        " - No agregues notas, puntuación, explicación, ni texto adicional.\n"
        " - Si la noticia afecta la imagen institucional, regulatoria o legal de la empresa, devuelve EXACTAMENTE: Corporate Reputation\n"
        " - Si no puedes decidir, responde EXACTAMENTE: Corporate Reputation\n\n"
        "NOTICIA:\n"
        f"{t}\n"
        "\nRESPUESTA (UNA SÓLO LÍNEA, la categoría EXACTA):"
    )
    return prompt

def _call_model_generate(prompt: str, *, temperature: float = 0.0, max_output_tokens: int = 12):
    _gemini_rate_limit()
    try:
        resp = model.generate_content(prompt, temperature=temperature, max_output_tokens=max_output_tokens)
        return resp
    except TypeError:
        resp = model.generate_content(prompt)
        return resp

def retry(fn: Callable[..., Any], max_attempts: int = 3, base_wait: float = 0.8):
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as e:
            last_exc = e
            if attempt == max_attempts:
                raise
            wait = base_wait * (2 ** (attempt - 1)) * (0.8 + 0.4 * random.random())
            log.warning(f"Retry {attempt}/{max_attempts} after {wait:.2f}s due to: {e}")
            time.sleep(wait)
    raise last_exc

def call_model_with_retry(prompt: str, max_attempts: int = 3):
    if model is None:
        log.debug("Model not initialized")
        return None
    def _inner_call():
        with llm_semaphore:
            return _call_model_generate(prompt, temperature=0.0, max_output_tokens=12)
    try:
        resp = retry(_inner_call, max_attempts=max_attempts, base_wait=0.8)
        return resp
    except Exception as e:
        log.warning("Model call failed after retries: %s", e)
        return None

def categorize_text_with_model(texto: str) -> str:
    try:
        t = (texto or "").strip()
        if not t:
            return "Corporate Reputation"
        prompt = build_prompt_from_text(t, max_chars=6000)
        resp = call_model_with_retry(prompt, max_attempts=3)
        raw = ""
        if resp is None:
            return "Corporate Reputation"
        try:
            raw = getattr(resp, "text", "") or ""
        except Exception:
            raw = ""
        if not raw:
            try:
                cand = getattr(resp, "candidates", None)
                if cand and len(cand) > 0:
                    c0 = cand[0]
                    raw = getattr(c0, "content", None) or (c0.get("content") if isinstance(c0, dict) else str(c0))
            except Exception:
                raw = ""
        raw = _sanitize_model_raw(raw)
        lr = raw.lower()
        if not raw or raw.startswith("(") or lr.startswith("por favor") or "no puedo" in lr or "necesito" in lr:
            log.warning("Model returned non-categorical text; fallback to Corporate Reputation. Raw: %s", raw[:200])
            return "Corporate Reputation"
        cat = normalize_category_from_model_output(raw)
        return cat
    except Exception as e:
        log.warning("Error in categorize_text_with_model: %s", e)
        return "Corporate Reputation"

def url_key(url: str) -> str:
    if not url:
        return ""
    try:
        return canonical_url(url).lower()
    except Exception:
        return url.strip().lower()

def categorize_row_obtaining_text(row: Dict[str, Any]) -> str:
    try:
        url = (row.get("link") or "").strip()
        k = url_key(url)
        with tag_cache_lock:
            if k and k in tag_cache:
                return tag_cache[k]
        # preferimos article_body/snippet/title
        body = ""
        if isinstance(row, dict):
            body = (row.get("article_body") or "").strip() or (row.get("snippet") or "").strip() or (row.get("title") or "").strip()
        # si no hay suficiente texto, intentar fetch
        if not body and url:
            try:
                html = download_html(url)
                if html:
                    body = extract_visible_text(html, max_chars=5000)
            except Exception:
                body = ""
        if not body:
            return "Corporate Reputation"
        category = categorize_text_with_model(body)
        if k:
            try:
                with tag_cache_lock:
                    tag_cache[k] = category
                    try:
                        _atomic_write_json(CATEGORY_CACHE_PATH, tag_cache)
                    except Exception as e:
                        log.debug("No se pudo persistir tag_cache: %s", e)
            except Exception:
                pass
        return category
    except Exception as e:
        log.warning("Error categorizing row: %s", e)
        return "Corporate Reputation"

# ---------- Scraping con Apify ----------
all_dfs: List[pd.DataFrame] = []
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
        log.info(f"[{datetime.now()}] Ejecutando {ACTOR_ID} para {country} con query '{query}'...")
        try:
            run = apify_client.actor(ACTOR_ID).call(run_input=run_input)
        except Exception as e:
            log.warning(f"Error al ejecutar actor para {country} con query '{query}': {e}")
            continue

        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            log.warning(f"No dataset generado para {country} - '{query}'")
            continue

        try:
            items = apify_client.dataset(dataset_id).list_items().items
        except Exception as e:
            log.warning(f"Error listando dataset {dataset_id}: {e}")
            continue

        if not items:
            log.info(f"No hay resultados para {country} - '{query}'")
            continue

        df = pd.DataFrame(items)
        df["country"] = country
        df["scraped_at"] = datetime.now(TZ_ARGENTINA).isoformat()
        all_dfs.append(df)

if not all_dfs:
    log.error("No se obtuvieron resultados de ningún país.")
    raise SystemExit(0)

# ---------- DataFrame y limpieza ----------
final_df = pd.concat(all_dfs, ignore_index=True)
final_df.drop_duplicates(subset=["link"], inplace=True)

# Convertir date_utc si existe
if "date_utc" in final_df.columns:
    try:
        final_df['date_utc'] = pd.to_datetime(final_df['date_utc'], utc=True).dt.tz_convert(TZ_ARGENTINA)
        final_df['date_utc'] = final_df['date_utc'].dt.strftime('%d/%m/%Y')
    except Exception:
        final_df['date_utc'] = final_df.get('date_utc', '').astype(str).fillna('')

# Columnas adicionales
final_df['sentiment'] = ''
final_df['semana'] = ''
final_df['tag'] = ''
final_df['country'] = final_df['country'].replace({'ar': 'Argentina', 'cl': 'Chile', 'pe': 'Peru'})

# scraped_at format
try:
    final_df['scraped_at'] = pd.to_datetime(final_df['scraped_at'])
    final_df['scraped_at'] = final_df['scraped_at'].dt.strftime('%d/%m/%Y %H:%M')
except Exception:
    final_df['scraped_at'] = final_df.get('scraped_at', '').astype(str).fillna('')

# Column ordering / header
header = ['semana','date_utc','country','title','link','domain','source','snippet','tag','sentiment','scraped_at']
final_df = final_df.reindex(columns=header, fill_value='')

# ---------- Leer hoja existente ----------
SHEET_RANGE = "2026!A:K"
result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=SHEET_RANGE).execute()
values = result.get("values", [])
if values:
    existing_df = pd.DataFrame(values[1:], columns=values[0])
else:
    existing_df = pd.DataFrame(columns=header)

# ---------- Combinar y dedupe (pero antes aplicar 'tag' a filas nuevas) ----------
# Determinar links ya presentes en existing_df
existing_links = set(existing_df.get("link", pd.Series(dtype=str)).astype(str).tolist())

# Filas nuevas a clasificar (en final_df y no en existing)
to_classify_mask = ~final_df['link'].astype(str).isin(existing_links)
to_classify_df = final_df.loc[to_classify_mask].copy()

if not to_classify_df.empty:
    log.info(f"Clasificando 'tag' para {len(to_classify_df)} filas nuevas...")
    # Aplicamos clasificación secuencialmente (respetando semáforos internos)
    # Convertimos cada fila a dict y pasamos a categorize_row_obtaining_text
    def _apply_tag(row):
        try:
            rdict = row.to_dict()
            return categorize_row_obtaining_text(rdict)
        except Exception as e:
            log.debug("Error aplicando tag a fila: %s", e)
            return "Corporate Reputation"

    to_classify_df['tag'] = to_classify_df.apply(_apply_tag, axis=1)
    # Volcamos tags de vuelta a final_df
    final_df.loc[to_classify_df.index, 'tag'] = to_classify_df['tag']

else:
    log.info("No hay filas nuevas para clasificar.")

# ---------- Concatenar y limpiar duplicados ----------
combined_df = pd.concat([existing_df, final_df], ignore_index=True)
combined_df.drop_duplicates(subset=["link"], inplace=True)

# ---------- Escribir hoja (sobrescribir rango) ----------
log.info("Escribiendo hoja: limpiando rango y subiendo datos...")
sheet.values().clear(spreadsheetId=SPREADSHEET_ID, range=SHEET_RANGE).execute()

# Asegurar que combined_df tiene las columnas en el orden del header
combined_df = combined_df.reindex(columns=header, fill_value='')

sheet.values().update(
    spreadsheetId=SPREADSHEET_ID,
    range="2026!A1",
    valueInputOption="RAW",
    body={"values": [header] + combined_df.astype(str).values.tolist()}
).execute()

log.info("✅ Hoja actualizada con tags y sin duplicados.")
