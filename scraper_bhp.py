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
import calendar
import unicodedata
from requests.adapters import HTTPAdapter

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# =========================
# CONFIG GENERAL
# =========================
COMPANY_NAME = os.getenv("COMPANY_NAME", "BHP")

# --- Config / env ---
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
GOOGLE_CREDENTIALS_ENV = os.getenv("GOOGLE_CREDENTIALS")
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
SPREADSHEET_ID = "1bl5WRNlLNrzUFRrQ0G3zPMkFWd0FExTh6KjwNgzwDcs"
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

# =========================
# SOLO ARGENTINA
# =========================
COUNTRIES = ["ar"]

QUERIES = [q.strip() for q in os.getenv(

    "QUERIES",
    "BHP Argentina,BHP Group Argentina,BHP Billiton Argentina,Proyecto Vicuña,"
    "Filo del Sol Argentina,Josemaría Argentina,Lundin Mining,Vicuña bhp"
    "cobre Argentina,litio Argentina,minería Argentina,RIGI minería,Vicuña corp,"
    "San Juan minería,Mendoza minería,CAEM Argentina,Arminera Argentina,BHP,NGEx Minerals,Filo Corp"

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

# Concurrency tunables
MAX_CONCURRENT_ACTORS = int(os.getenv("MAX_CONCURRENT_ACTORS", "4"))
MAX_CONCURRENT_DATASET_FETCH = int(os.getenv("MAX_CONCURRENT_DATASET_FETCH", "6"))
LLM_MAX_CONCURRENT = int(os.getenv("LLM_MAX_CONCURRENT", "2"))

# --- Google Sheets client ---
try:
    sa_info = json.loads(GOOGLE_CREDENTIALS_ENV)
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    sheet_service = build("sheets", "v4", credentials=creds).spreadsheets()
except Exception:
    logging.exception("Failed loading Google credentials (sanitized). Exiting.")
    sys.exit(1)

# --- Apify client ---
apify_client = ApifyClient(APIFY_TOKEN)

# --- Thread-safety primitives and utilities ---
article_cache_lock = threading.Lock()
tag_cache_lock = threading.Lock()
llm_semaphore = threading.Semaphore(LLM_MAX_CONCURRENT)

def atomic_write_json(path, data):
    dirpath = os.path.dirname(path) or "."
    try:
        with tempfile.NamedTemporaryFile("w", dir=dirpath, delete=False, encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
            tempname = fh.name
        os.replace(tempname, path)
        try:
            os.chmod(path, 0o600)
        except Exception:
            pass
    except Exception as e:
        logging.warning("atomic_write_json failed for %s: %s", path, e)

# --- Helpers: backoff retry ---
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
            logging.warning(
                "Call to %s failed (attempt %d/%d): %s — retrying in %.1fs",
                getattr(fn, "__name__", str(fn)), attempt, max_attempts, e, sleep_for
            )
            time.sleep(sleep_for)

def safe_convert_date_col(df, col="date_utc"):
    if col not in df.columns:
        df[col] = ""
        return df
    try:
        dt = pd.to_datetime(df[col], utc=True, errors="coerce")
        dt = dt.dt.tz_convert(TZ_ARGENTINA)
        df[col] = dt.dt.strftime("%d/%m/%Y").fillna("")
    except Exception:
        df[col] = df[col].astype(str).fillna("")
    return df

def format_week_range(date_str):
    if not date_str or pd.isna(date_str):
        return ""
    try:
        dt = datetime.strptime(date_str, "%d/%m/%Y")
        monday = dt - pd.Timedelta(days=dt.weekday())
        sunday = monday + pd.Timedelta(days=6)
        month_abbr = calendar.month_abbr[monday.month].upper()
        return f"{monday.day:02d}–{sunday.day:02d} {month_abbr} {monday.year}"
    except Exception:
        return ""

def normalize_for_match(text):
    if text is None:
        return ""
    text = str(text)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"\s+", " ", text)
    return text.upper().strip()

def safe_series(df, col):
    if col in df.columns:
        return df[col].fillna("").astype(str)
    return pd.Series([""] * len(df), index=df.index, dtype="object")

# -------------------------------------------------
# KEYWORDS / FILTERS DEL CLIPPING
# -------------------------------------------------
MONITORING_TERMS = [
    # BHP corporativo
    "BHP", "BHP Group", "BHP Billiton", "Broken Hill Proprietary", "Mike Henry",
    "Ragnar Udd", "Brandon Craig", "BHP CEO", "BHP resultados", "BHP results",
    "BHP earnings", "BHP producción", "BHP production", "BHP guidance",
    "BHP inversión", "BHP investment", "BHP dividendo", "BHP dividend",
    "BHP sustainability report", "BHP ESG",

    # Vicuña / Lundin
    "Vicuña", "Vicuña Corp", "proyecto Vicuña", "Distrito Vicuña",
    "Filo del Sol", "Filo Corp",
    "Lundin Mining", "Jack Lundin", "Adam Lundin", "Juan Andrés Morel",
    "Juan Andres Morel", "Integrated Technical Study", "Tratado de Integración Minera",
    "Tratado de Integracion Minera", "cobre San Juan",
    "RIGI Vicuña", "RIGI Josemaría", "RIGI Josemaria",
    "Fluor", "Vicuña proveedores", "Vicuña empleo", "Proyecto Vicuña",

    # Contexto argentino
    "Javier Milei", "Luis Caputo", "Daniel González", "Daniel Gonzalez",
    "Luis Lucero", "Pablo Quirno", "RIGI", "Ley Bases", "Secretaría de Minería",
    "Secretaria de Mineria", "COFEMIN", "OFEMI", "retenciones minería",
    "retenciones mineria", "cepo", "dólar exportador", "dolar exportador",
    "minería exportador", "mineria exportador", "ley de glaciares"

    # Minería en general
    "minería Argentina", "mineria Argentina", "cobre Argentina", "litio Argentina",
    "oro Argentina", "proyectos cobre", "inversión minera", "inversion minera",
    "exportaciones mineras", "precio del cobre", "LME copper", "CAEM",
    "Panorama Minero", "Arminera", "PDAC", "Argentina Cobre", "Argentina Mining",
    "Glencore", "First Quantum", "Rio Tinto", "Newmont", "Barrick",
    "McEwen Copper", "Los Azules", "Michael Meding", "Ganfeng", "Zijin",
    "Rio Tinto Lithium",

    # Zoom San Juan
    "Marcelo Orrego", "Juan Pablo Perea", "Roberto Moreno", "Fernando Perea",
    "Roberto Gutiérrez", "Roberto Gutierrez", "Federico Ríos", "Federico Rios",
    "Cámara Minera San Juan", "Camara Minera San Juan", "Iván Grgic", "Ivan Grgic",
    "IPEEM", "Ministerio de Minería San Juan", "Ministerio de Mineria San Juan",
    "Iglesia", "Calingasta", "Jáchal", "Jachal", "Rodeo", "Las Flores",
    "Veladero", "Gualcamayo", "Altar", "Hualilán", "Hualilan", "Pachón", "Pachon",
    "Casposo", "derrame Jáchal", "derrame Jachal",

    # Zoom Mendoza
    "Alfredo Cornejo", "Ministerio de Energía y Ambiente Mendoza",
    "Ministerio de Energia y Ambiente Mendoza", "Ley 7722", "7722", "DIA Malargüe",
    "DIA Malargue", "PSI Malargüe", "PSI Malargue", "Malargüe Distrito Minero Occidental",
    "Malargue Distrito Minero Occidental", "San Jorge", "Hierro Indio",
    "Potasio Río Colorado", "Potasio Rio Colorado", "PRC", "Cerro Amarillo",
    "Don Sixto", "asambleas mendocinas", "agua pura Mendoza", "no a la mina Mendoza"
]

MONITORING_TERMS_NORMALIZED = [normalize_for_match(t) for t in MONITORING_TERMS]

def contains_any_monitoring_keyword(text):
    t = normalize_for_match(text)
    if not t:
        return False
    return any(term in t for term in MONITORING_TERMS_NORMALIZED)

# -------------------------------------------------
# CATEGORIES DEL CLIPPING
# -------------------------------------------------
CANONICAL_CATEGORIES = [
    "BHP Corporativo",
    "BHP Vicuña Lundin",
    "Contexto argentino",
    "Minería en general",
    "Zoom San Juan",
    "Zoom Mendoza",
]

CATEGORY_KEYWORDS = {
    "BHP Vicuña Lundin": [
        "Vicuña", "Vicuña Corp", "proyecto Vicuña", "Distrito Vicuña",
        "Filo del Sol", "Filo Corp", "Josemaría", "Josemaria", "FDS",
        "Lundin Mining", "Jack Lundin", "Adam Lundin", "Juan Andrés Morel",
        "Juan Andres Morel", "Integrated Technical Study", "Tratado de Integración Minera",
        "Tratado de Integracion Minera", "RIGI Vicuña", "RIGI Josemaría", "RIGI Josemaria",
        "Fluor", "Vicuña proveedores", "Vicuña empleo"
    ],
    "Zoom San Juan": [
        "Marcelo Orrego", "Juan Pablo Perea", "Roberto Moreno", "Fernando Perea",
        "Roberto Gutiérrez", "Roberto Gutierrez", "Federico Ríos", "Federico Rios",
        "Cámara Minera San Juan", "Camara Minera San Juan", "Iván Grgic", "Ivan Grgic",
        "IPEEM", "Ministerio de Minería San Juan", "Ministerio de Mineria San Juan",
        "Iglesia", "Calingasta", "Jáchal", "Jachal", "Rodeo", "Las Flores",
        "Veladero", "Gualcamayo", "Altar", "Hualilán", "Hualilan", "Pachón", "Pachon",
        "Casposo", "derrame Jáchal", "derrame Jachal"
    ],
    "Zoom Mendoza": [
        "Alfredo Cornejo", "Ministerio de Energía y Ambiente Mendoza",
        "Ministerio de Energia y Ambiente Mendoza", "Ley 7722", "7722", "DIA Malargüe",
        "DIA Malargue", "PSI Malargüe", "PSI Malargue", "Malargüe Distrito Minero Occidental",
        "Malargue Distrito Minero Occidental", "San Jorge", "Hierro Indio",
        "Potasio Río Colorado", "Potasio Rio Colorado", "PRC", "Cerro Amarillo",
        "Don Sixto", "asambleas mendocinas", "agua pura Mendoza", "no a la mina Mendoza"
    ],
    "Contexto argentino": [
        "Javier Milei", "Luis Caputo", "Daniel González", "Daniel Gonzalez",
        "Luis Lucero", "Pablo Quirno", "RIGI", "Ley Bases", "Secretaría de Minería",
        "Secretaria de Mineria", "COFEMIN", "OFEMI", "retenciones minería",
        "retenciones mineria", "cepo", "dólar exportador", "dolar exportador"
    ],
    "Minería en general": [
        "minería Argentina", "mineria Argentina", "cobre Argentina", "litio Argentina",
        "oro Argentina", "proyectos cobre", "inversión minera", "inversion minera",
        "exportaciones mineras", "precio del cobre", "LME copper", "CAEM",
        "Panorama Minero", "Arminera", "PDAC", "Argentina Cobre", "Argentina Mining",
        "Glencore", "First Quantum", "Rio Tinto", "Newmont", "Barrick",
        "McEwen Copper", "Los Azules", "Michael Meding", "Ganfeng", "Zijin",
        "Rio Tinto Lithium"
    ],
    "BHP Corporativo": [
        "BHP", "BHP Group", "BHP Billiton", "Broken Hill Proprietary", "Mike Henry",
        "Ragnar Udd", "Brandon Craig", "BHP CEO", "BHP resultados", "BHP results",
        "BHP earnings", "BHP producción", "BHP production", "BHP guidance",
        "BHP inversión", "BHP investment", "BHP dividendo", "BHP dividend",
        "BHP sustainability report", "BHP ESG"
    ]
}

CATEGORY_KEYWORDS_NORMALIZED = {
    cat: [normalize_for_match(k) for k in kws]
    for cat, kws in CATEGORY_KEYWORDS.items()
}

CATEGORY_PRIORITY = [
    "BHP Vicuña Lundin",
    "Zoom San Juan",
    "Zoom Mendoza",
    "Contexto argentino",
    "Minería en general",
    "BHP Corporativo",
]

def categorize_text_with_rules(text):
    t = normalize_for_match(text)
    if not t:
        return None
    for cat in CATEGORY_PRIORITY:
        if any(k in t for k in CATEGORY_KEYWORDS_NORMALIZED[cat]):
            return cat
    return None

def normalize_category_from_model_output(raw_text):
    if not raw_text:
        return "Minería en general"
    t = normalize_for_match(raw_text)
    for cat in CANONICAL_CATEGORIES:
        cat_norm = normalize_for_match(cat)
        if cat_norm == t:
            return cat
    for cat in CANONICAL_CATEGORIES:
        if any(k in t for k in CATEGORY_KEYWORDS_NORMALIZED[cat]):
            return cat
    return "Minería en general"

def build_prompt_from_text(texto):
    max_chars = 12000
    t = (texto or "").strip()
    if len(t) > max_chars:
        t = t[:max_chars]

    allowed_line = ", ".join(CANONICAL_CATEGORIES)

    prompt = f"""
ROL
Actúa como un Analista Senior de PR y Asuntos Públicos especializado en minería en Argentina.

OBJETIVO
Clasificar la noticia en UNA sola categoría estratégica según su tema principal.

CATEGORÍAS DISPONIBLES (elige SOLO UNA)
- BHP Corporativo
- BHP Vicuña Lundin
- Contexto argentino
- Minería en general
- Zoom San Juan
- Zoom Mendoza

CRITERIOS
- BHP Corporativo: resultados, estrategia, ejecutivos, ESG, inversión, dividendos.
- BHP Vicuña Lundin: proyectos Vicuña, Josemaría, Filo del Sol, Lundin y temas del JV.
- Contexto argentino: política, regulación, economía, RIGI, Ley Bases, minería nacional.
- Minería en general: sector, mercado, competidores, precios, cámaras, eventos.
- Zoom San Juan: actores, política, regulación o minería en San Juan.
- Zoom Mendoza: actores, regulación o minería en Mendoza.

INSTRUCCIONES
1) Analiza la noticia.
2) Responde SOLO con UNA categoría EXACTA:
{allowed_line}
3) Sin explicación, sin texto extra, sin puntuación adicional.
4) Si no puedes clasificar por falta de información, responde con la categoría que mejor encaje con el tema dominante.

NOTICIA:
{t}
"""
    return prompt

# -------------------------------------------------
# CACHES
# -------------------------------------------------
CATEGORY_CACHE_PATH = os.getenv("CATEGORY_CACHE_PATH", "category_cache.json")
try:
    if os.path.exists(CATEGORY_CACHE_PATH):
        with open(CATEGORY_CACHE_PATH, "r", encoding="utf-8") as fh:
            tag_cache = json.load(fh) or {}
    else:
        tag_cache = {}
except Exception:
    tag_cache = {}

CACHE_PATH = os.getenv("ARTICLE_CACHE_PATH", "article_cache.json")
MAX_FETCH_WORKERS = int(os.getenv("MAX_FETCH_WORKERS", "3"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "15"))
REQUEST_RETRIES = int(os.getenv("REQUEST_RETRIES", "2"))
REQUEST_SLEEP_BETWEEN = float(os.getenv("REQUEST_SLEEP_BETWEEN", "0.2"))

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

def url_key(u):
    return u.strip() if u else ""

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
            logging.debug("fetch_html attempt %d for %s failed: %s", attempt + 1, url, e)
        time.sleep(0.5 + REQUEST_SLEEP_BETWEEN * attempt)
    return None

def extract_body_from_html(url, html):
    try:
        art = Article(url, language="es")
        art.set_html(html)
        art.parse()
        return (art.text or "").strip()
    except Exception as e:
        logging.debug("newspaper parse failed for %s: %s", url, e)
        return ""

def fetch_and_parse(url):
    k = url_key(url)
    if not k:
        return k, ""
    with article_cache_lock:
        if k in article_cache:
            return k, article_cache[k]
    html = fetch_html_with_retries(url)
    body = extract_body_from_html(url, html) if html else ""
    with article_cache_lock:
        article_cache[k] = body
    time.sleep(REQUEST_SLEEP_BETWEEN)
    return k, body

def _call_model_with_retry(prompt, max_attempts=3):
    return retry(lambda: model.generate_content(prompt), max_attempts=max_attempts)

def categorize_text_with_model(texto):
    """
    Calls the LLM under a concurrency semaphore and parses defensively.
    """
    try:
        if model is None:
            logging.debug("Model not initialized — returning fallback category")
            return "Minería en general"

        prompt = build_prompt_from_text(texto)

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
                return "Minería en general"

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

        lower_raw = raw.lower()
        if raw.startswith("(") or lower_raw.startswith("por favor") or "proporciona la noticia" in lower_raw:
            logging.warning("Model returned a system/clarification message; using fallback category.")
            return "Minería en general"

        cat = normalize_category_from_model_output(raw)
        return cat

    except Exception as e:
        logging.warning("Error categorizing text with model: %s", e)
        return "Minería en general"

def categorize_row_obtaining_text(row):
    url = (row.get("link") or "").strip()
    k = url_key(url)

    # Cache hit
    with tag_cache_lock:
        if k and k in tag_cache:
            return tag_cache[k]

    combined = " ".join([
        (row.get("title") or "").strip(),
        (row.get("snippet") or "").strip(),
        (row.get("article_body") or "").strip(),
    ])

    rule_cat = categorize_text_with_rules(combined)
    if rule_cat:
        if k:
            with tag_cache_lock:
                tag_cache[k] = rule_cat
        return rule_cat

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

    category = categorize_text_with_model(body or combined)

    if k:
        try:
            with tag_cache_lock:
                tag_cache[k] = category
        except Exception:
            pass

    return category

# -------------------------------------------------
# PARALLEL ACTOR EXECUTION
# -------------------------------------------------
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
            logging.warning("No dataset generated for %s - %s", res["country"], res["query"])
            continue
        actor_results.append(res)

logging.info("Actor executions completed: %d successful / %d total", len(actor_results), len(tasks))

# -------------------------------------------------
# DESCARGA DATASETS EN PARALELO
# -------------------------------------------------
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
    except Exception:
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

# -------------------------------------------------
# BUILD FINAL DATAFRAME
# -------------------------------------------------
final_df = pd.concat(all_dfs, ignore_index=True)

if "link" in final_df.columns:
    final_df.drop_duplicates(subset=["link"], inplace=True)
else:
    logging.warning("No 'link' column present in scraped items; duplicates won't be removed by link.")

final_df = safe_convert_date_col(final_df, "date_utc")

for col in ("tag", "article_body"):
    if col not in final_df.columns:
        final_df[col] = ""

# Solo Argentina
final_df["country"] = final_df["country"].replace({"ar": "Argentina"})

try:
    final_df["scraped_at"] = pd.to_datetime(final_df["scraped_at"], errors="coerce").dt.strftime("%d/%m/%Y %H:%M").fillna("")
except Exception:
    final_df["scraped_at"] = final_df["scraped_at"].astype(str).fillna("")

# set semana from date_utc if available
#if "date_utc" in final_df.columns:
#    final_df["semana"] = final_df["date_utc"].apply(format_week_range)

# -------------------------------------------------
# ARTICLE FETCH + PARSE
# -------------------------------------------------
links = final_df.get("link", pd.Series([], dtype=str)).dropna().astype(str).unique().tolist()
logging.info(
    "Starting article fetch: %d unique links (cache hits: %d)",
    len(links),
    sum(1 for l in links if url_key(l) in article_cache)
)

link_to_body = {}
with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as ex:
    futures = {ex.submit(fetch_and_parse, url): url for url in links}
    for fut in as_completed(futures):
        url = futures[fut]
        try:
            k, body = fut.result()
            link_to_body[k] = body or ""
        except Exception as e:
            logging.warning("Error fetching/parsing %s: %s", url, e)
            link_to_body[url] = ""

save_cache(CACHE_PATH, article_cache)

final_df["link"] = final_df["link"].astype(str)
final_df["article_body"] = final_df["link"].map(lambda u: link_to_body.get(url_key(u), "")).fillna("")

# -------------------------------------------------
# FILTRO DE RELEVANCIA DEL CLIPPING
# -------------------------------------------------
combined_text = (
    safe_series(final_df, "title") + " " +
    safe_series(final_df, "snippet") + " " +
    safe_series(final_df, "article_body")
)

mask = combined_text.apply(contains_any_monitoring_keyword)

before_tot = len(final_df)
final_df = final_df[mask].copy()
after_tot = len(final_df)
logging.info("After clipping keyword filter: %d -> %d rows (removed %d)", before_tot, after_tot, before_tot - after_tot)

if final_df.empty:
    logging.info("No rows matched the clipping keywords. Exiting without updating sheet.")
    sys.exit(0)

# -------------------------------------------------
# CLASIFICACIÓN
# -------------------------------------------------
def categorize_text_with_model_safe(texto):
    try:
        return categorize_text_with_model(texto)
    except Exception:
        return "Minería en general"

def categorize_row_for_sheet(row):
    url = (row.get("link") or "").strip()
    k = url_key(url)

    with tag_cache_lock:
        if k and k in tag_cache:
            return tag_cache[k]

    combined = " ".join([
        (row.get("title") or "").strip(),
        (row.get("snippet") or "").strip(),
        (row.get("article_body") or "").strip(),
    ])

    rule_cat = categorize_text_with_rules(combined)
    if rule_cat:
        if k:
            with tag_cache_lock:
                tag_cache[k] = rule_cat
        return rule_cat

    cat = categorize_text_with_model_safe(combined)

    if cat not in CANONICAL_CATEGORIES:
        cat = "Minería en general"

    if k:
        try:
            with tag_cache_lock:
                tag_cache[k] = cat
        except Exception:
            pass

    return cat

rows_to_categorize = final_df.reset_index()[["index", "link", "title", "snippet", "article_body"]].to_dict(orient="records")
logging.info("Starting category classification for %d rows (workers=%d)...", len(rows_to_categorize), MAX_FETCH_WORKERS)

categories_map = {}
with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as ex:
    futures = {ex.submit(categorize_row_for_sheet, r): r for r in rows_to_categorize}
    for fut in as_completed(futures):
        r = futures[fut]
        try:
            category = fut.result()
        except Exception as e:
            logging.warning("Error classifying row (link=%s): %s", r.get("link"), e)
            category = "Minería en general"
        categories_map[r["index"]] = category

final_df = final_df.reset_index()
final_df["tag"] = final_df["index"].map(lambda i: categories_map.get(i, "Minería en general"))
final_df = final_df.drop(columns=["index"]).reset_index(drop=True)

logging.info("Category classification completed. Distribution: %s", final_df["tag"].value_counts().to_dict())

try:
    atomic_write_json(CATEGORY_CACHE_PATH, tag_cache)
except Exception as e:
    logging.warning("Could not save category cache: %s", e)

# -------------------------------------------------
# SENTIMENT (OPCIONAL)
# -------------------------------------------------
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
es POSITIVA, NEGATIVA o NEUTRA respecto a la reputación de {COMPANY_NAME} como empresa/minera.

INSTRUCCIONES
- Analiza SOLO el texto provisto.
- Responde únicamente con UNA de las tres palabras EXACTAS (en mayúsculas): POSITIVO, NEGATIVO o NEUTRO.
- No añadas puntuación, explicaciones ni ningún otro texto.
- Si no puedes clasificar por falta de información, responde EXACTAMENTE: NEUTRO

NOTICIA:
{texto}
"""

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

# final_df["sentiment"] = final_df["link"].apply(analizar_noticia)

# -------------------------------------------------
# SHEET WRITE
# -------------------------------------------------
header = ["date_utc", "country", "title", "link", "domain", "source", "snippet", "tag", "scraped_at"]
final_df = final_df.reindex(columns=header, fill_value="")
final_df = final_df.drop_duplicates(subset="link")
final_df = final_df.drop_duplicates(subset=["title", "snippet"])

SHEET_RANGE = "2026!A:K"
HEADER = header

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
        link_idx = header_row.index("link")
        sheet_has_header = True
    except ValueError:
        row_lower = [c.lower() for c in header_row]
        if "link" in row_lower:
            link_idx = row_lower.index("link")
            sheet_has_header = True
        else:
            if len(header_row) == len(HEADER):
                try:
                    link_idx = HEADER.index("link")
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
        final_df[col] = ""

final_df = final_df.replace([np.nan, pd.NaT, None], "").replace([np.inf, -np.inf], "")

rows_to_add = []
new_links_count = 0
for row in final_df[HEADER].values.tolist():
    row_map = dict(zip(HEADER, row))
    link = str(row_map.get("link", "")).strip()
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
                val = ""
            else:
                val = fv
        elif isinstance(cell, (np.bool_, bool)):
            val = bool(cell)
        else:
            try:
                import pandas as _pd
                if isinstance(cell, _pd.Timestamp):
                    if pd.isna(cell):
                        val = ""
                    else:
                        val = cell.isoformat()
                else:
                    val = "" if cell is None else str(cell)
            except Exception:
                val = "" if cell is None else str(cell)

        if isinstance(val, str) and val.lower() in ("nan", "nat", "none"):
            val = ""
        sanitized_cells.append(val)

    rows_to_add.append([str(c) for c in sanitized_cells])
    existing_links_set.add(link)
    new_links_count += 1

if new_links_count == 0 and not sheet_empty:
    logging.info("No new rows to add. Exiting without touching the sheet.")
    logging.info("Script finished successfully.")
    sys.exit(0)

# 4) Prepare batches and append with retries/backoff
BATCH_SIZE = int(os.getenv("SHEET_BATCH_SIZE", "500"))

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
        except HttpError:
            attempt += 1
            if attempt >= max_attempts:
                logging.exception("Failed to append batch to Sheets after %d attempts (sanitized).", attempt)
                raise
            sleep_for = min(60, base_delay * (2 ** (attempt - 1))) + random.random() * 0.5
            logging.warning(
                "HttpError appending to Sheets (attempt %d/%d) — retrying in %.1fs",
                attempt, max_attempts, sleep_for
            )
            time.sleep(sleep_for)
        except Exception:
            attempt += 1
            if attempt >= max_attempts:
                logging.exception("Failed to append batch to Sheets after %d attempts (sanitized).", attempt)
                raise
            sleep_for = min(60, base_delay * (2 ** (attempt - 1))) + random.random() * 0.5
            logging.warning(
                "Error appending to Sheets (attempt %d/%d) — retrying in %.1fs",
                attempt, max_attempts, sleep_for
            )
            time.sleep(sleep_for)

# if sheet empty, write header first
if sheet_empty:
    logging.info("Sheet empty: writing header first.")
    try:
        append_with_retry([HEADER])
    except Exception:
        logging.exception("Could not write header to sheet (sanitized).")
        raise

# Append in batches
total_added = 0
for i in range(0, len(rows_to_add), BATCH_SIZE):
    batch = rows_to_add[i:i + BATCH_SIZE]
    try:
        append_with_retry(batch)
        total_added += len(batch)
        logging.info("Appended batch %d..%d (rows=%d) to sheet.", i, i + len(batch) - 1, len(batch))
    except Exception:
        logging.exception("Failed appending batch starting at %d (sanitized).", i)
        continue

logging.info("✅ Sheet updated. New rows appended: %d", total_added)
logging.info("Script finished successfully.")
