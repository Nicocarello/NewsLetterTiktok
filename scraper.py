# -*- coding: utf-8 -*-
"""
Pipeline Google News -> filtro contenido -> Google Sheets + Sentimiento (Gemini) + Categoría (Gemini)
Caso: "Eduardo Elsztain" solo en Argentina

Env vars requeridas:
- GOOGLE_CREDENTIALS (JSON del service account con acceso a la Sheet)
- APIFY_TOKEN (token de Apify)
- GEMINI_API_KEY (API key de Google AI Studio)

Google Sheet columnas (exactas):
date_utc, title, link, source, snippet, sentiment, scraped_at
"""
from __future__ import annotations

from typing import List, Dict, Callable, Optional, Any
import os
import json
import logging
import re
import unicodedata
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
import random
import threading
from threading import Semaphore, Lock
from functools import wraps

import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry

from apify_client import ApifyClient
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --- Gemini ---
import google.generativeai as genai

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

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise RuntimeError("Falta GEMINI_API_KEY en variables de entorno.")

# Inicializar Google Sheets
creds = service_account.Credentials.from_service_account_info(CREDS_DICT, scopes=SCOPES)
service = build("sheets", "v4", credentials=creds, cache_discovery=False)
sheet = service.spreadsheets()

# Inicializar Apify
apify_client = ApifyClient(APIFY_TOKEN)
ACTOR_ID = "easyapi/google-news-scraper"

# Inicializar Gemini
genai.configure(api_key=GEMINI_API_KEY)
GEMINI_MODEL_NAME = "gemini-2.0-flash"
model = genai.GenerativeModel(GEMINI_MODEL_NAME)

# IDs y constantes
SPREADSHEET_ID = "1DTMBII9byTfx9KU6M1QghhlU8abCRh8rKThcnaTbzpE"  # ajustar si corresponde
SHEET_TAB = "NOTICIAS"
HEADER = ["date_utc", "title", "link", "source", "snippet", "sentiment", "scraped_at"]

# Filtros
QUERIES = ['"eduardo elsztain"', "eduardo elsztain"]

# Zona horaria de Argentina
TZ_ARG = pytz.timezone("America/Argentina/Buenos_Aires")

# HTTP base
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
DEFAULT_TIMEOUT = 12  # s
MIN_HTML_BYTES = 256  # rechaza respuestas vacías
MAX_THREADS_HARD = 16  # techo superior de threads

# Thread-local session para seguridad en paralelo
_tls = threading.local()

def get_session() -> requests.Session:
    sess = getattr(_tls, "session", None)
    if sess is None:
        sess = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.6,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "HEAD"]),
        )
        adapter = HTTPAdapter(max_retries=retries)
        sess.mount("http://", adapter)
        sess.mount("https://", adapter)
        _tls.session = sess
    return sess

# ---------------------------
# Helpers
# ---------------------------
def canonical_url(u: str) -> str:
    """Remueve parámetros de tracking para mejorar dedupe y normaliza host/path."""
    try:
        p = urlparse(u.strip())
        q = [
            (k, v)
            for k, v in parse_qsl(p.query, keep_blank_values=True)
            if not k.lower().startswith(("utm_", "fbclid", "gclid"))
        ]
        netloc = p.netloc.replace(":80", "").replace(":443", "")
        if netloc.startswith("www."):
            netloc = netloc[4:]
        path = p.path or "/"
        path = re.sub(r"/{2,}", "/", path)  # compactar múltiples slash
        query = urlencode(q, doseq=True)
        return urlunparse((p.scheme or "https", netloc, path, "", query, ""))
    except Exception:
        return u

def normalize_text(s: str) -> str:
    """Normaliza acentos/espacios/guiones para robustecer matching."""
    if not isinstance(s, str):
        return ""
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
        return df.rename(columns={"domain": "source"})
    def _host(x: str) -> str:
        try:
            host = urlparse(str(x)).netloc
            host = host.replace(":80", "").replace(":443", "")
            return host[4:] if host.startswith("www.") else host
        except Exception:
            return ""
    df["source"] = df.get("link", pd.Series([""] * len(df))).astype(str).apply(_host)
    return df

# Cache para HEAD en una ejecución
_HEAD_CACHE: Dict[str, dict] = {}

def head_cached(url: str, timeout=DEFAULT_TIMEOUT) -> dict:
    """Devuelve dict con keys: ok(bool), content_type(str), status(int). Cachea por ejecución."""
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
    """Evita PDFs/imagenes antes de descargar cuerpo completo."""
    res = head_cached(url)
    ctype = res.get("content_type", "")
    if "text/html" in ctype or ctype == "":
        return True
    return False

def download_html(url: str) -> str:
    """Descarga HTML de manera tolerante; devuelve '' si falla o no es HTML."""
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

def page_mentions_elsztain(url: str) -> bool:
    """Verifica que el cuerpo/título mencione a Eduardo Elsztain."""
    try:
        if not is_probably_html(url):
            return False

        resp = get_session().get(url, timeout=DEFAULT_TIMEOUT, headers={"User-Agent": UA})
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
        log.debug(f"Error al procesar {url}: {e}")
        return False

def prefilter_row_mentions(row: pd.Series) -> bool:
    """Prefiltro barato usando los campos del actor (sin salir a la web)."""
    for col in ("title", "snippet"):
        if col in row and isinstance(row[col], str) and row[col]:
            s = normalize_text(row[col])
            if PATRON_NOMBRE.search(s):
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
# Utilidades de texto/HTML para sentimiento (Gemini)
# ---------------------------
VISIBLE_TAGS = {"p", "h1", "h2", "h3", "li"}

def extract_visible_text(html: str, max_chars: int = 5000) -> str:
    """Extrae texto visible básico para análisis de sentimiento."""
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

from time import sleep
SENTIMENT_LABELS = {"POSITIVO", "NEGATIVO", "NEUTRO"}
SENT_PROMPT_TMPL = (
    "Eres un clasificador de sentimiento estricto. "
    "Clasifica el sentimiento hacia la figura de 'Eduardo Elsztain' o a la empresa IRSA o los proyectos de la empresa en el texto."
    "Devuelve SOLO una de estas palabras EXACTAS: POSITIVO, NEGATIVO, NEUTRO.\n\n"
    "Criterios:\n"
    "- POSITIVO: logros, apoyo, impacto favorable, mejoras atribuidas a él.\n"
    "- NEGATIVO: críticas, controversias, pérdidas, impacto desfavorable a él, antisemitismo.\n"
    "- NEUTRO: informativo/descriptivo sin carga valorativa clara.\n\n"
    "Texto:\n{texto}"
)

# simple retry wrapper for the model in this script
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

def analizar_noticia(url: str, *, retries: int = 3) -> str:
    """Devuelve POSITIVO/NEGATIVO/NEUTRO. Falla segura a NEUTRO."""
    try:
        html = download_html(url)
        if not html:
            return "NEUTRO"
        texto = extract_visible_text(html, max_chars=5000)
        if not texto or len(texto) < 120:
            return "NEUTRO"

        prompt = SENT_PROMPT_TMPL.format(texto=texto)

        # rate-limit + llamada robusta
        for attempt in range(retries):
            try:
                # breve pausa para evitar picos
                time.sleep(0.2 * (1 + attempt))
                resp = model.generate_content(prompt, temperature=0, max_output_tokens=40)
                out = (resp.text or "").strip().upper()
                m = re.search(r"[A-ZÁÉÍÓÚÜÑ]+", out)  # tomar la primera palabra alfabética
                label = m.group(0) if m else out
                return label if label in SENTIMENT_LABELS else "NEUTRO"
            except Exception as e:
                if attempt < retries - 1:
                    sleep(1.5 * (2 ** attempt))
                    continue
                log.debug(f"Gemini error en {url}: {e}")
                return "NEUTRO"

    except Exception as e:
        log.debug(f"Error procesando {url}: {e}")
        return "NEUTRO"

# ---------------------------
# Utilitarios Google Sheets (con backoff)
# ---------------------------
def with_backoff(fn: Callable, *, retries: int = 5, base_wait: float = 1.0, on_retry: Optional[str] = None):
    for attempt in range(retries):
        try:
            return fn()
        except HttpError as e:
            status = getattr(e.resp, "status", None)
            if status in (429, 500, 502, 503, 504) and attempt < retries - 1:
                wait = base_wait * (2 ** attempt)
                if on_retry:
                    log.warning(f"{on_retry} -> retry {attempt+1}/{retries} en {wait:.1f}s (HTTP {status})")
                time.sleep(wait)
                continue
            raise

# ---------------------------
# Scrape con Apify -> DataFrame
# ---------------------------
def run_apify_queries(queries: List[str]) -> List[pd.DataFrame]:
    """
    Ejecuta el actor de Google News por cada query (sin restricción geográfica).
    """
    all_dfs: List[pd.DataFrame] = []
    for query in queries:
        run_input = {
            "hl": "es-419",       # interfaz en español latino
            "lr": "lang_es",      # resultados en español
            "maxItems": 300,
            "query": query,
            "time_period": "last_hour",  # podés cambiar a "last_24_hours" si querés mayor ventana
        }

        log.info(f"Ejecutando {ACTOR_ID} con query '{query}' (sin filtro de país)...")
        try:
            run = apify_client.actor(ACTOR_ID).call(run_input=run_input)
        except Exception as e:
            log.error(f"No se pudo ejecutar actor con '{query}': {e}")
            continue

        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            log.warning(f"Sin dataset para '{query}'")
            continue

        try:
            items = list_all_items(dataset_id)
        except Exception as e:
            log.error(f"No se pudo listar dataset {dataset_id}: {e}")
            continue

        if not items:
            log.info(f"Sin resultados - '{query}'")
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
        if "date_utc" in df.columns:
            dt = pd.to_datetime(df["date_utc"], utc=True, errors="coerce")
            df["date_utc"] = dt.dt.strftime("%d/%m/%Y")
        else:
            df["date_utc"] = ""

        # scraped_at -> local AR dd/mm/YYYY HH:MM
        df["scraped_at"] = now_utc.astimezone(TZ_ARG).strftime("%d/%m/%Y %H:%M")

        # sentiment placeholder si no viene
        if "sentiment" not in df.columns:
            df["sentiment"] = ""

        all_dfs.append(df)

    return all_dfs

# ---------------------------
# Filtro por contenido (HTTP en paralelo)
# ---------------------------
from concurrent.futures import ThreadPoolExecutor, as_completed

def filter_by_content(prefiltered: pd.DataFrame) -> pd.DataFrame:
    links = prefiltered["link"].dropna().astype(str).tolist()
    n = len(links)
    if n == 0:
        log.warning("No hay links para verificar en sitio.")
        return prefiltered.copy()

    max_workers = max(4, min(MAX_THREADS_HARD, (n // 6) + 1))
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
                log.debug(f"Future error {u}: {e}")
            results[u] = ok

    return prefiltered[prefiltered["link"].map(results).fillna(False)].copy()

# ---------------------------
# Google Sheets IO
# ---------------------------
def ensure_headers() -> pd.DataFrame:
    read_range = f"{SHEET_TAB}!A:G"
    log.info(f"Leyendo hoja existente: {read_range} ...")

    try:
        result = with_backoff(
            lambda: sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=read_range).execute(),
            on_retry="Lectura de Google Sheets",
        )
        values = result.get("values", [])
    except Exception as e:
        log.warning(f"No se pudo leer la hoja (se asumirá vacía): {e}")
        values = []

    if not values:
        log.info("Hoja vacía, creando encabezados...")
        with_backoff(
            lambda: sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_TAB}!A1",
                valueInputOption="RAW",
                body={"values": [HEADER]},
            ).execute(),
            on_retry="Creación de encabezados",
        )
        return pd.DataFrame(columns=HEADER)

    header_in_sheet = values[0]
    rows = values[1:] if len(values) > 1 else []
    norm_rows = [row + [""] * (len(header_in_sheet) - len(row)) if len(row) < len(header_in_sheet) else row[:len(header_in_sheet)] for row in rows]
    existing_df = pd.DataFrame(norm_rows, columns=header_in_sheet)
    for col in HEADER:
        if col not in existing_df.columns:
            existing_df[col] = ""
    existing_df = existing_df.reindex(columns=HEADER, fill_value="")
    return existing_df

def append_new_rows(new_rows: pd.DataFrame) -> None:
    if new_rows.empty:
        log.info("No hay filas nuevas para agregar.")
        return
    log.info(f"Agregando {len(new_rows)} filas nuevas...")

    with_backoff(
        lambda: sheet.values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_TAB}!A1",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": new_rows.astype(str).values.tolist()},
        ).execute(),
        on_retry="Append a Google Sheets",
    )

# ---------------------------
# Clasificación con Gemini (categorías)
# ---------------------------
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
llm_semaphore = Semaphore(3)
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

    # 1) exact canonical match
    for can in CANONICAL_CATEGORIES:
        if r.strip().lower() == can.lower():
            return can

    # sanitize
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

    logging.warning("Model output not mappable to a category (sanitized). Raw: %s", raw_text[:200])
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

def categorize_row_obtaining_text(row: dict) -> str:
    try:
        url = (row.get("link") or "").strip()
        k = url_key(url)

        with tag_cache_lock:
            if k and k in tag_cache:
                return tag_cache[k]

        body = (row.get("article_body") or "").strip() if isinstance(row, dict) else ""

        if not body:
            body = (row.get("snippet") or "").strip() if isinstance(row, dict) else ""

        if not body and url:
            try:
                html = download_html(url)
                if html:
                    body = extract_visible_text(html, max_chars=5000)
            except Exception:
                body = ""

        if not body:
            body = (row.get("title") or "").strip() if isinstance(row, dict) else ""

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

# ---------------------------
# Main pipeline
# ---------------------------
def run_pipeline() -> None:
    # 1) Scrape
    all_dfs = run_apify_queries(QUERIES)
    if not all_dfs:
        log.error("No se obtuvieron resultados de ningún país/query.")
        return

    # 2) Combine + dedupe
    final_df = pd.concat(all_dfs, ignore_index=True)
    final_df.drop_duplicates(subset=["link"], inplace=True)

    # 3) Prefiltro por título/snippet
    if not final_df.empty:
        mask_pref = final_df.apply(prefilter_row_mentions, axis=1)
        prefiltered = final_df[mask_pref].copy()
        if prefiltered.empty:
            prefiltered = final_df.copy()
    else:
        prefiltered = final_df

    # 4) Filtro por contenido (requests en paralelo)
    filtered = filter_by_content(prefiltered)
    if filtered.empty:
        log.warning("Tras el filtro de contenido, no quedaron resultados relevantes.")

    # 5) Sentimiento con Gemini (solo si la columna existe/viene vacía)
    if "sentiment" not in filtered.columns:
        filtered["sentiment"] = ""
    mask_to_score = filtered["sentiment"].astype(str).str.strip().eq("")
    if mask_to_score.any():
        log.info(f"Calculando sentimiento Gemini para {mask_to_score.sum()} notas...")
        filtered.loc[mask_to_score, "sentiment"] = (
            filtered.loc[mask_to_score, "link"].astype(str).apply(analizar_noticia)
        )

    # 5b) Categoría con Gemini (añadir columna 'category')
    if "category" not in filtered.columns:
        filtered["category"] = ""
    mask_to_cat = filtered["category"].astype(str).str.strip().eq("")
    if mask_to_cat.any():
        log.info(f"Calculando categoría Gemini para {mask_to_cat.sum()} notas...")
        # secuencial o ligero paralelismo: uso apply (puede hacerse con ThreadPoolExecutor si querés)
        # aquí usamos apply para respetar semáforo/ratelimit dentro de la función
        filtered.loc[mask_to_cat, "category"] = filtered.loc[mask_to_cat].apply(lambda r: categorize_row_obtaining_text(r.to_dict()), axis=1)

    # 6) Asegurar columnas, tipos y orden final (si querés incluir 'category' en sheet, añadila al HEADER o manejar aparte)
    # mantenemos HEADER original para compatibilidad con la hoja; si querés almacenar 'category' en otra hoja/columna, ajustá.
    for col in HEADER:
        if col not in filtered.columns:
            filtered[col] = ""
    final_out = filtered.astype({c: str for c in filtered.columns}).reindex(columns=HEADER, fill_value="")

    # 7) Leer hoja/encabezados
    existing_df = ensure_headers()

    # 8) Append SOLO filas nuevas (por link)
    if existing_df.empty:
        new_rows = final_out
    else:
        new_rows = final_out.loc[~final_out["link"].isin(existing_df["link"])]

    # 9) Append
    append_new_rows(new_rows)

    # Resumen
    try:
        counts = filtered["sentiment"].value_counts().to_dict()
        cat_counts = (filtered.get("category") or pd.Series(dtype=str)).value_counts().to_dict()
        log.info(f"Resumen sentimiento: {counts}")
        log.info(f"Resumen categorías (top): {dict(list(cat_counts.items())[:10])}")
    except Exception:
        pass

if __name__ == "__main__":
    run_pipeline()
