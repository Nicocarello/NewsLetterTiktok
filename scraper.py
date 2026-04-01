from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.errors import HttpError
import google.generativeai as genai
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
import threading
import tempfile
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# --- Config / env ---
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

# Validate other required envs
if not GOOGLE_CREDENTIALS_ENV:
    logging.error("Missing GOOGLE_CREDENTIALS environment variable. Exiting.")
    sys.exit(1)
if not APIFY_TOKEN:
    logging.error("Missing APIFY_TOKEN environment variable. Exiting.")
    sys.exit(1)

COUNTRIES = [c.strip() for c in os.getenv("COUNTRIES", "ar,cl,pe").split(",") if c.strip()]
QUERIES = [q.strip() for q in os.getenv(
    "QUERIES",
    "tik-tok,tiktok,tiktok suicidio,tiktok grooming,tiktok armas,tiktok drogas,tiktok violacion,tiktok delincuentes,tiktok ladrones,tiktok narcos,tiktok estafa"
).split(",") if q.strip()]

try:
    MAX_ITEMS = int(os.getenv("MAX_ITEMS", "500"))
    if MAX_ITEMS <= 0:
        logging.warning("MAX_ITEMS <= 0; using 500.")
        MAX_ITEMS = 500
except Exception:
    logging.warning("MAX_ITEMS is not a valid int; using 500.")
    MAX_ITEMS = 500

TIME_PERIOD = os.getenv("TIME_PERIOD", "last_day")
TZ_ARGENTINA = pytz.timezone("America/Argentina/Buenos_Aires")

# Concurrency tunables (env)
MAX_CONCURRENT_ACTORS = int(os.getenv("MAX_CONCURRENT_ACTORS", "4"))
MAX_CONCURRENT_DATASET_FETCH = int(os.getenv("MAX_CONCURRENT_DATASET_FETCH", "6"))

# LLM concurrency guard (code-default; can be overridden via env if needed)
LLM_MAX_CONCURRENT = int(os.getenv("LLM_MAX_CONCURRENT", "2"))

# --- Google Sheets client ---
# Defensive loading of service account info from env (avoid logging full JSON)
try:
    sa_info = json.loads(GOOGLE_CREDENTIALS_ENV)
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    sheet_service = build('sheets', 'v4', credentials=creds).spreadsheets()
except Exception as e:
    logging.exception("Failed loading Google credentials (sanitized). Exiting.")
    sys.exit(1)

# --- Apify client ---
apify_client = ApifyClient(APIFY_TOKEN)

# --- Thread-safety primitives and utilities ---
article_cache_lock = threading.Lock()
tag_cache_lock = threading.Lock()
llm_semaphore = threading.Semaphore(LLM_MAX_CONCURRENT)

def atomic_write_json(path, data):
    # Write to temp and replace atomically
    dirpath = os.path.dirname(path) or '.'
    try:
        with tempfile.NamedTemporaryFile('w', dir=dirpath, delete=False, encoding='utf-8') as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
            tempname = fh.name
        os.replace(tempname, path)
        try:
            os.chmod(path, 0o600)
        except Exception:
            pass
    except Exception as e:
        logging.warning("atomic_write_json failed for %s: %s", path, e)

# --- Helpers: backoff retry (reusable) ---
def retry(fn, max_attempts=5, base_delay=1.5, max_delay=60, jitter=0.4, *args, **kwargs):
    attempt = 0
    while True:
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            attempt += 1
            if attempt >= max_attempts:
                logging.exception("Max retries reached calling %s", getattr(fn, "__name__", str(fn)))
                raise
            backoff = min(max_delay, base_delay * (2 ** (attempt - 1)))
            sleep_for = backoff + random.random() * jitter
            logging.warning("Call to %s failed (attempt %d/%d): %s — retrying in %.1fs",
                            getattr(fn, "__name__", str(fn)), attempt, max_attempts, e, sleep_for)
            time.sleep(sleep_for)

def safe_convert_date_col(df, col='date_utc'):
    if col not in df.columns:
        df[col] = ''
        return df
    try:
        dt = pd.to_datetime(df[col], utc=True, errors='coerce')
        dt = dt.dt.tz_convert(TZ_ARGENTINA)
        df[col] = dt.dt.strftime('%d/%m/%Y').fillna('')
    except Exception:
        df[col] = df[col].astype(str).fillna('')
    return df

# Optional helper
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

# --- Parallel actor execution (build task list: query x country) ---
tasks = []
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
        tasks.append({"query": query, "country": country, "run_input": run_input})

logging.info("Launching %d actor runs (concurrency=%d)...", len(tasks), MAX_CONCURRENT_ACTORS)

def run_actor_task(task):
    query = task["query"]
    country = task["country"]
    run_input = task["run_input"]
    try:
        logging.info("Executing actor %s for %s with query '%s'...", ACTOR_ID, country, query)
        run = retry(lambda: apify_client.actor(ACTOR_ID).call(run_input=run_input), max_attempts=4)
        dataset_id = run.get("defaultDatasetId")
        return {"query": query, "country": country, "run": run, "dataset_id": dataset_id, "error": None}
    except Exception as e:
        logging.exception("Error running actor for %s with query '%s'.", country, query)
        return {"query": query, "country": country, "run": None, "dataset_id": None, "error": str(e)}

actor_results = []
with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_ACTORS) as ex:
    futures = {ex.submit(run_actor_task, t): t for t in tasks}
    for fut in as_completed(futures):
        res = fut.result()
        if res["error"]:
            logging.warning("Run failed for %s - %s: %s", res["country"], res["query"], res["error"])
            continue
        if not res["dataset_id"]:
            # Log only limited part of run to avoid leaking secrets
            logging.warning("No dataset generated for %s - %s", res["country"], res["query"])
            continue
        actor_results.append(res)

logging.info("Actor executions completed: %d successful / %d total", len(actor_results), len(tasks))

# --- Descarga datasets en paralelo ---
def fetch_dataset_items(entry):
    dataset_id = entry["dataset_id"]
    country = entry["country"]
    query = entry["query"]
    try:
        items = retry(lambda: apify_client.dataset(dataset_id).list_items().items, max_attempts=4)
        if not items:
            logging.info("No items for dataset %s (%s - %s)", dataset_id, country, query)
            return None
        df = pd.DataFrame(items)
        df["country"] = country
        df["query"] = query
        df["scraped_at"] = datetime.now(TZ_ARGENTINA).isoformat()
        return df
    except Exception as e:
        logging.exception("Error listing items for dataset %s.", dataset_id)
        return None

all_dfs = []
with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DATASET_FETCH) as ex:
    futures = {ex.submit(fetch_dataset_items, r): r for r in actor_results}
    for fut in as_completed(futures):
        df = fut.result()
        if df is not None and not df.empty:
            all_dfs.append(df)

if not all_dfs:
    logging.error("No results obtained from any country. Exiting without updating sheet.")
    sys.exit(0)

# --- Build final dataframe and normalize columns ---
final_df = pd.concat(all_dfs, ignore_index=True)

if 'link' in final_df.columns:
    final_df.drop_duplicates(subset=["link"], inplace=True)
else:
    logging.warning("No 'link' column present in scraped items; duplicates won't be removed by link.")

final_df = safe_convert_date_col(final_df, 'date_utc')

# Ensure additional columns exist (we'll store classification in 'tag')
for col in ('tag', 'semana', 'article_body', 'sentiment'):
    if col not in final_df.columns:
        final_df[col] = ''

final_df['country'] = final_df['country'].replace({'ar': 'Argentina', 'cl': 'Chile', 'pe': 'Peru'})

try:
    final_df['scraped_at'] = pd.to_datetime(final_df['scraped_at'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M').fillna('')
except Exception:
    final_df['scraped_at'] = final_df['scraped_at'].astype(str).fillna('')

# --- Article fetch + parse w/ Session + ThreadPool (cache simple) ---
CACHE_PATH = os.getenv("ARTICLE_CACHE_PATH", "article_cache.json")
MAX_FETCH_WORKERS = int(os.getenv("MAX_FETCH_WORKERS", "3"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "15"))
REQUEST_RETRIES = int(os.getenv("REQUEST_RETRIES", "2"))
REQUEST_SLEEP_BETWEEN = float(os.getenv("REQUEST_SLEEP_BETWEEN", "0.2"))
TIKTOK_PATTERN = re.compile(r"tik\s*-?\s*tok", flags=re.IGNORECASE)

def load_cache(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                return json.load(fh) or {}
        except Exception:
            return {}
    return {}

def save_cache(path, data):
    try:
        atomic_write_json(path, data)
    except Exception as e:
        logging.warning("Could not save cache to %s: %s", path, e)

article_cache = load_cache(CACHE_PATH)

def url_key(u): return u.strip() if u else ''

# Configure session with adapter and moderate pool sizes
session = requests.Session()
adapter = HTTPAdapter(pool_connections=20, pool_maxsize=50)
session.mount("http://", adapter)
session.mount("https://", adapter)
session.headers.update({
    "User-Agent": os.getenv("FETCH_USER_AGENT", "Mozilla/5.0 (compatible; PublicBot/1.0; +https://publicalatam.com)"),
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8"
})

def fetch_html_with_retries(url):
    for attempt in range(REQUEST_RETRIES + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200 and resp.text:
                return resp.text
            logging.debug("fetch_html: %s returned status %d", url, getattr(resp, "status_code", None))
        except requests.RequestException as e:
            logging.debug("fetch_html attempt %d for %s failed: %s", attempt+1, url, e)
        time.sleep(0.5 + REQUEST_SLEEP_BETWEEN * attempt)
    return None

def extract_body_from_html(url, html):
    try:
        art = Article(url, language='es')
        art.set_html(html)
        art.parse()
        return (art.text or '').strip()
    except Exception as e:
        logging.debug("newspaper parse failed for %s: %s", url, e)
        return ''

def fetch_and_parse(url):
    k = url_key(url)
    if not k:
        return k, ''
    with article_cache_lock:
        if k in article_cache:
            return k, article_cache[k]
    html = fetch_html_with_retries(url)
    body = extract_body_from_html(url, html) if html else ''
    with article_cache_lock:
        article_cache[k] = body
    # small polite sleep
    time.sleep(REQUEST_SLEEP_BETWEEN)
    return k, body

links = final_df.get('link', pd.Series([], dtype=str)).dropna().astype(str).unique().tolist()
logging.info("Starting article fetch: %d unique links (cache hits: %d)", len(links), sum(1 for l in links if url_key(l) in article_cache))

link_to_body = {}
with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as ex:
    futures = {ex.submit(fetch_and_parse, url): url for url in links}
    for fut in as_completed(futures):
        url = futures[fut]
        try:
            k, body = fut.result()
            link_to_body[k] = body or ''
        except Exception as e:
            logging.warning("Error fetching/parsing %s: %s", url, e)
            link_to_body[url] = ''

save_cache(CACHE_PATH, article_cache)

final_df['link'] = final_df['link'].astype(str)
final_df['article_body'] = final_df['link'].map(lambda u: link_to_body.get(url_key(u), '')).fillna('')

# ---------------------------
# Filtro robusto (keep only rows mentioning TikTok)
# ---------------------------
mask = (
    # final_df.get('title', '').astype(str).str.contains(TIKTOK_PATTERN, na=False) |
    # final_df.get('snippet', '').astype(str).str.contains(TIKTOK_PATTERN, na=False) |
    final_df.get('article_body', '').astype(str).str.contains(TIKTOK_PATTERN, na=False)
)
before_tot = len(final_df)
final_df = final_df[mask].copy()
after_tot = len(final_df)
logging.info("After body verification filter: %d -> %d rows (removed %d)", before_tot, after_tot, before_tot - after_tot)

# ---------------------------
# CATEGORIZACIÓN POST-FILTER
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
    "TRUST AND SAFETY": "TnS",
    "MODERATION": "TnS",
    "CORPORATE REPUTATION": "Corporate Reputation",
    "CORPORATE": "Corporate Reputation",
    "REPUTATION": "Corporate Reputation",
    "LEGAL": "Corporate Reputation",
    "REGULATORY": "Corporate Reputation",
    "REGULATION": "Corporate Reputation",
    "GOVERNMENT": "Corporate Reputation",
}

def normalize_category_from_model_output(raw_text):
    if not raw_text:
        return "Corporate Reputation"
    r = raw_text.strip().upper()
    r_clean = re.sub(r"[\"'\.\,]", " ", r)
    for key, canonical in NORMALIZATION_MAP.items():
        if key in r_clean:
            return canonical
    for token in re.split(r"[\s,;:()\[\]\"']+", r_clean):
        token = token.strip()
        if not token:
            continue
        if token in NORMALIZATION_MAP:
            return NORMALIZATION_MAP[token]
    for can in CANONICAL_CATEGORIES:
        if can.upper() in r:
            return can
    logging.warning("Model output not mappable to a category (sanitized).")
    return "Corporate Reputation"

def build_prompt_from_text(texto):
    max_chars = 12000
    t = (texto or "").strip()
    if len(t) > max_chars:
        t = t[:max_chars]
    allowed = [
        "Consumer & Brand",
        "Music",
        "B2B",
        "SMB",
        "Creator",
        "Product",
        "TnS",
        "Corporate Reputation",
    ]
    allowed_line = ", ".join(allowed)
    prompt = f"""
ROL
Actúa como un Analista de Datos Senior especializado en PR y Reputación Corporativa de TikTok.
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

NOTICIA:
{t}
"""
    return prompt

# --- Limpieza variable obsoleta si estaba presente ---
try:
    del VALID_SENTIMENTS
except NameError:
    pass

# --- Category cache (tag_cache) ---
CATEGORY_CACHE_PATH = os.getenv("CATEGORY_CACHE_PATH", "category_cache.json")
try:
    if os.path.exists(CATEGORY_CACHE_PATH):
        with open(CATEGORY_CACHE_PATH, "r", encoding="utf-8") as fh:
            tag_cache = json.load(fh) or {}
    else:
        tag_cache = {}
except Exception:
    tag_cache = {}

# Wrapper para llamar al modelo con retry y parsing defensivo
def _call_model_with_retry(prompt, max_attempts=3):
    return retry(lambda: model.generate_content(prompt), max_attempts=max_attempts)

def categorize_text_with_model(texto):
    """
    Calls the LLM under a concurrency semaphore and parses defensively.
    """
    try:
        if model is None:
            logging.debug("Model not initialized — returning fallback category")
            return "Corporate Reputation"

        prompt = build_prompt_from_text(texto)

        # ensure concurrency limit for LLM
        with llm_semaphore:
            def call():
                try:
                    return model.generate_content(prompt, temperature=0, max_output_tokens=20)
                except TypeError:
                    return model.generate_content(prompt)
            try:
                resp = retry(call, max_attempts=3)
            except Exception as e:
                logging.warning("Model call failed after retries (sanitized): %s", e)
                return "Corporate Reputation"

        raw = ""
        try:
            raw = getattr(resp, "text", None) or ""
        except Exception:
            raw = ""

        if not raw:
            try:
                cand = getattr(resp, "candidates", None)
                if cand and len(cand) > 0:
                    raw = getattr(cand[0], "content", "") or str(cand[0])
            except Exception:
                raw = str(resp)

        raw = (raw or "").strip()

        # Quick sanity checks for system-style replies; fallback if suspicious
        lower_raw = raw.lower()
        if raw.startswith("(") or lower_raw.startswith("por favor") or "proporciona la noticia" in lower_raw:
            logging.warning("Model returned a system/clarification message; using fallback category.")
            return "Corporate Reputation"

        cat = normalize_category_from_model_output(raw)
        if cat == "Corporate Reputation" and raw.upper() not in [c.upper() for c in CANONICAL_CATEGORIES]:
            logging.debug("Model returned unmapped raw output (sanitized).")
        return cat

    except Exception as e:
        logging.warning("Error categorizing text with model: %s", e)
        return "Corporate Reputation"

def categorize_row_obtaining_text(row):
    url = (row.get("link") or "").strip()
    k = url_key(url)

    # Cache hit
    with tag_cache_lock:
        if k and k in tag_cache:
            return tag_cache[k]

    body = (row.get("article_body") or "").strip()
    if not body and url:
        try:
            html = fetch_html_with_retries(url)
            if html:
                body = extract_body_from_html(url, html)
        except Exception:
            body = ""

    if not body and url:
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200 and resp.text:
                soup = BeautifulSoup(resp.text, "html.parser")
                paragraphs = [p.get_text(separator=" ", strip=True) for p in soup.find_all("p")]
                body = " ".join(paragraphs)
        except Exception:
            body = ""

    category = categorize_text_with_model(body)

    if k:
        try:
            with tag_cache_lock:
                tag_cache[k] = category
        except Exception:
            pass

    return category

# Ejecutar clasificación en paralelo
rows_to_categorize = final_df.reset_index()[["index","link","article_body"]].to_dict(orient="records")
logging.info("Starting category classification for %d rows (workers=%d)...", len(rows_to_categorize), MAX_FETCH_WORKERS)

categories_map = {}
with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as ex:
    futures = {ex.submit(categorize_row_obtaining_text, r): r for r in rows_to_categorize}
    for fut in as_completed(futures):
        r = futures[fut]
        try:
            category = fut.result()
        except Exception as e:
            logging.warning("Error classifying row (link=%s): %s", r.get("link"), e)
            category = "Corporate Reputation"
        categories_map[r["index"]] = category

# Asignar resultado en 'tag'
final_df = final_df.reset_index()
final_df["tag"] = final_df["index"].map(lambda i: categories_map.get(i, "Corporate Reputation"))
final_df = final_df.drop(columns=["index"]).reset_index(drop=True)

logging.info("Category classification completed. Distribution: %s", final_df["tag"].value_counts().to_dict())

# Persistir tag cache atomically
try:
    atomic_write_json(CATEGORY_CACHE_PATH, tag_cache)
except Exception as e:
    logging.warning("Could not save category cache: %s", e)

# ---------------------------
# SENTIMENT CLASSIFICATION (POSITIVO / NEGATIVO / NEUTRO) - using Gemini
# ---------------------------

def analizar_noticia(url):
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200 or not response.text:
            return "NEUTRO"
        soup = BeautifulSoup(response.text, "html.parser")
        paragraphs = [p.get_text() for p in soup.find_all("p")]
        texto = " ".join(paragraphs)

        prompt = f"""
        ROL
Actúa como Analista Senior de PR/Reputación. Tu única tarea es determinar si la noticia
es POSITIVA, NEGATIVA o NEUTRA respecto a la reputación de TikTok como empresa/plataforma.

INSTRUCCIONES (leer atentamente)
- Analiza SOLO el texto provisto.
- Responde únicamente con UNA de las tres palabras EXACTAS (en mayúsculas): POSITIVO, NEGATIVO o NEUTRO.
- No añadas puntuación, explicaciones ni ningún otro texto.
- Si no puedes clasificar por falta de información, responde EXACTAMENTE: NEUTRO
- Respuestas aceptadas: ['POSITIVO','NEGATIVO','NEUTRO']

NOTICIA:
        {texto}
        """

        # Use semaphore to control concurrency
        with llm_semaphore:
            resp = model.generate_content(prompt)
        resultado = getattr(resp, "text", "") or ""
        resultado = resultado.strip().upper()
        if resultado not in ["POSITIVO", "NEGATIVO", "NEUTRO"]:
            return "NEUTRO"
        return resultado

    except Exception as e:
        logging.warning("Error processing %s: %s", url, e)
        return "NEUTRO"

#final_df['sentiment'] = final_df['link'].apply(analizar_noticia)

# Ensure column order and presence (header keeps 'tag' and 'sentiment' if you want both)
header = ['semana','date_utc','country','title','link','domain','source','snippet','tag','sentiment','scraped_at']
final_df = final_df.reindex(columns=header, fill_value='')
final_df = final_df.drop_duplicates(subset='link')
final_df = final_df.drop_duplicates(subset=["title", "snippet"])

# --- Read existing sheet and combine (incremental append instead of full rewrite) ---
SHEET_RANGE = "2026!A:K"
HEADER = ['semana','date_utc','country','title','link','domain','source','snippet','tag','sentiment','scraped_at']

# 1) Read current sheet values (if any)
try:
    result = sheet_service.values().get(spreadsheetId=SPREADSHEET_ID, range=SHEET_RANGE).execute()
    values = result.get("values", [])
    logging.info("Read %d rows from sheet (including header if present).", len(values))
except HttpError as e:
    logging.exception("Failed to read existing sheet (sanitized): %s", e)
    values = []
except Exception as e:
    logging.exception("Failed to read existing sheet (sanitized): %s", e)
    values = []

# 2) Build set of existing links from the sheet to avoid duplicate appends
existing_links_set = set()
sheet_has_header = False
if values and len(values) >= 1:
    header_row = values[0]
    link_idx = None
    try:
        link_idx = header_row.index('link')
        sheet_has_header = True
    except ValueError:
        row_lower = [c.lower() for c in header_row]
        if 'link' in row_lower:
            link_idx = row_lower.index('link')
            sheet_has_header = True
        else:
            if len(header_row) == len(HEADER):
                try:
                    link_idx = HEADER.index('link')
                    sheet_has_header = True
                except Exception:
                    link_idx = None

    if link_idx is not None:
        for r in values[1:]:
            try:
                if len(r) > link_idx:
                    v = r[link_idx].strip()
                    if v:
                        existing_links_set.add(v)
            except Exception:
                continue

sheet_empty = len(values) == 0

# 3) Ensure final_df has correct columns and sanitized values
for col in HEADER:
    if col not in final_df.columns:
        final_df[col] = ''

final_df = final_df.replace([np.nan, pd.NaT, None], '').replace([np.inf, -np.inf], '')

# Build list of candidate rows (in correct order), and filter out ones with link already in sheet
rows_to_add = []
new_links_count = 0
for row in final_df[HEADER].values.tolist():
    row_map = dict(zip(HEADER, row))
    link = str(row_map.get('link', '')).strip()
    if not link:
        continue
    if link in existing_links_set:
        continue
    sanitized_cells = []
    for cell in row:
        if isinstance(cell, (np.integer,)):
            val = int(cell)
        elif isinstance(cell, (np.floating,)):
            fv = float(cell)
            if math.isnan(fv) or math.isinf(fv):
                val = ''
            else:
                val = fv
        elif isinstance(cell, (np.bool_, bool)):
            val = bool(cell)
        else:
            try:
                import pandas as _pd
                if isinstance(cell, _pd.Timestamp):
                    if pd.isna(cell):
                        val = ''
                    else:
                        val = cell.isoformat()
                else:
                    val = '' if cell is None else str(cell)
            except Exception:
                val = '' if cell is None else str(cell)
        if isinstance(val, str) and val.lower() in ('nan', 'nat', 'none'):
            val = ''
        sanitized_cells.append(val)
    rows_to_add.append([str(c) for c in sanitized_cells])
    existing_links_set.add(link)
    new_links_count += 1

if new_links_count == 0 and not sheet_empty:
    logging.info("No new rows to add. Exiting without touching the sheet.")
    logging.info("Script finished successfully.")
    sys.exit(0)

# 4) Prepare batches and append with retries/backoff
BATCH_SIZE = int(os.getenv("SHEET_BATCH_SIZE", "500"))  # adjustable
def sheets_append_batch(values_batch):
    body = {"values": values_batch}
    return sheet_service.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="2026!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()

def append_with_retry(batch, max_attempts=5, base_delay=1.5):
    attempt = 0
    while True:
        try:
            return sheets_append_batch(batch)
        except HttpError as e:
            attempt += 1
            if attempt >= max_attempts:
                logging.exception("Failed to append batch to Sheets after %d attempts (sanitized).", attempt)
                raise
            sleep_for = min(60, base_delay * (2 ** (attempt - 1))) + random.random() * 0.5
            logging.warning("HttpError appending to Sheets (attempt %d/%d) — retrying in %.1fs", attempt, max_attempts, sleep_for)
            time.sleep(sleep_for)
        except Exception as e:
            attempt += 1
            if attempt >= max_attempts:
                logging.exception("Failed to append batch to Sheets after %d attempts (sanitized).", attempt)
                raise
            sleep_for = min(60, base_delay * (2 ** (attempt - 1))) + random.random() * 0.5
            logging.warning("Error appending to Sheets (attempt %d/%d) — retrying in %.1fs", attempt, max_attempts, sleep_for)
            time.sleep(sleep_for)

# if sheet empty, write header first (one-time)
if sheet_empty:
    logging.info("Sheet empty: writing header first.")
    try:
        append_with_retry([HEADER])
    except Exception as e:
        logging.exception("Could not write header to sheet (sanitized).")
        raise

# Append in batches
total_added = 0
for i in range(0, len(rows_to_add), BATCH_SIZE):
    batch = rows_to_add[i:i + BATCH_SIZE]
    try:
        append_with_retry(batch)
        total_added += len(batch)
        logging.info("Appended batch %d..%d (rows=%d) to sheet.", i, i+len(batch)-1, len(batch))
    except Exception as e:
        logging.exception("Failed appending batch starting at %d (sanitized).", i)
        continue

logging.info("✅ Sheet updated. New rows appended: %d", total_added)
logging.info("Script finished successfully.")
