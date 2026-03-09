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
        logging.warning("MAX_ITEMS <= 0; usando 500.")
        MAX_ITEMS = 500
except Exception:
    logging.warning("MAX_ITEMS no es int válido; usando 500.")
    MAX_ITEMS = 500

TIME_PERIOD = os.getenv("TIME_PERIOD", "last_hour")
TZ_ARGENTINA = pytz.timezone("America/Argentina/Buenos_Aires")

# Concurrency tunables (env)
MAX_CONCURRENT_ACTORS = int(os.getenv("MAX_CONCURRENT_ACTORS", "4"))
MAX_CONCURRENT_DATASET_FETCH = int(os.getenv("MAX_CONCURRENT_DATASET_FETCH", "6"))

# --- Google Sheets client ---
try:
    creds = service_account.Credentials.from_service_account_info(json.loads(GOOGLE_CREDENTIALS_ENV), scopes=SCOPES)
    sheet_service = build('sheets', 'v4', credentials=creds).spreadsheets()
except Exception as e:
    logging.exception("Failed loading Google credentials: %s", e)
    sys.exit(1)

# --- Apify client ---
apify_client = ApifyClient(APIFY_TOKEN)

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

logging.info("Lanzando %d ejecuciones de actor (concurrency=%d)...", len(tasks), MAX_CONCURRENT_ACTORS)

def run_actor_task(task):
    query = task["query"]
    country = task["country"]
    run_input = task["run_input"]
    try:
        logging.info("Ejecutando actor %s para %s con query '%s'...", ACTOR_ID, country, query)
        run = retry(lambda: apify_client.actor(ACTOR_ID).call(run_input=run_input), max_attempts=4)
        dataset_id = run.get("defaultDatasetId")
        return {"query": query, "country": country, "run": run, "dataset_id": dataset_id, "error": None}
    except Exception as e:
        logging.exception("Error al ejecutar actor para %s con query '%s': %s", country, query, e)
        return {"query": query, "country": country, "run": None, "dataset_id": None, "error": str(e)}

actor_results = []
with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_ACTORS) as ex:
    futures = {ex.submit(run_actor_task, t): t for t in tasks}
    for fut in as_completed(futures):
        res = fut.result()
        if res["error"]:
            logging.warning("Run falló para %s - %s: %s", res["country"], res["query"], res["error"])
            continue
        if not res["dataset_id"]:
            logging.warning("No dataset generado para %s - %s (run: %s)", res["country"], res["query"], str(res["run"])[:200])
            continue
        actor_results.append(res)

logging.info("Ejecuciones completadas: %d exitosas / %d totales", len(actor_results), len(tasks))

# --- Descarga datasets en paralelo ---
def fetch_dataset_items(entry):
    dataset_id = entry["dataset_id"]
    country = entry["country"]
    query = entry["query"]
    try:
        items = retry(lambda: apify_client.dataset(dataset_id).list_items().items, max_attempts=4)
        if not items:
            logging.info("No items para dataset %s (%s - %s)", dataset_id, country, query)
            return None
        df = pd.DataFrame(items)
        df["country"] = country
        df["query"] = query
        df["scraped_at"] = datetime.now(TZ_ARGENTINA).isoformat()
        return df
    except Exception as e:
        logging.exception("Error listando items del dataset %s: %s", dataset_id, e)
        return None

all_dfs = []
with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DATASET_FETCH) as ex:
    futures = {ex.submit(fetch_dataset_items, r): r for r in actor_results}
    for fut in as_completed(futures):
        df = fut.result()
        if df is not None and not df.empty:
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

def url_key(u): return u.strip() if u else ''

session = requests.Session()
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
            logging.debug("fetch_html: %s returned status %d", url, resp.status_code)
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
    if k in article_cache:
        return k, article_cache[k]
    html = fetch_html_with_retries(url)
    body = extract_body_from_html(url, html) if html else ''
    article_cache[k] = body
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
    final_df.get('title', '').astype(str).str.contains(TIKTOK_PATTERN, na=False) |
    final_df.get('snippet', '').astype(str).str.contains(TIKTOK_PATTERN, na=False) |
    final_df.get('article_body', '').astype(str).str.contains(TIKTOK_PATTERN, na=False)
)
before_tot = len(final_df)
final_df = final_df[mask].copy()
after_tot = len(final_df)
logging.info("After body verification filter: %d -> %d rows (removed %d)", before_tot, after_tot, before_tot - after_tot)

# ---------------------------
# CATEGORIZACIÓN POST-FILTER (devuelve una de las etiquetas y la guarda en 'tag')
# ---------------------------

# Canonical categories (exact output strings expected in the sheet)
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

# Normalization map: posibles tokens/respuestas del modelo -> categoría canónica
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
    """
    Convierte la respuesta libre del modelo a UNA categoría canónica.
    Si no se puede mapear, devuelve fallback 'Corporate Reputation'.
    """
    if not raw_text:
        return "Corporate Reputation"
    r = raw_text.strip().upper()
    # Elimina puntuación común que pueda acompañar la respuesta
    r_clean = re.sub(r"[\"'\.\,]", " ", r)
    # 1) Match por presencia de frases completas (búsqueda prioritaria)
    for key, canonical in NORMALIZATION_MAP.items():
        if key in r_clean:
            return canonical
    # 2) Token match: dividir y buscar tokens mapeables
    for token in re.split(r"[\s,;:()\[\]\"']+", r_clean):
        token = token.strip()
        if not token:
            continue
        if token in NORMALIZATION_MAP:
            return NORMALIZATION_MAP[token]
    # 3) Intentar buscar las categorías canónicas textualmente (safety)
    for can in CANONICAL_CATEGORIES:
        if can.upper() in r:
            return can
    # 4) Fallback estratégico
    logging.warning("Salida de modelo no mapeable a categoría: %s", raw_text)
    return "Corporate Reputation"

def build_prompt_from_text(texto):
    max_chars = 12000
    t = (texto or "").strip()
    if len(t) > max_chars:
        t = t[:max_chars]

    # Lista exacta de salidas permitidas (cópiala exactamente)
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
            tag_cache = json.load(fh)
    else:
        tag_cache = {}
except Exception:
    tag_cache = {}

# Wrapper para llamar al modelo con retry y parsing defensivo
def _call_model_with_retry(prompt, max_attempts=3):
    return retry(lambda: model.generate_content(prompt), max_attempts=max_attempts)

def categorize_text_with_model(texto):
    """
    Llama al LLM con parámetros controlados (temperature=0) y parseo defensivo.
    """
    try:
        # Si el modelo no está inicializado (opción B), devolver fallback
        if model is None:
            logging.debug("Model not initialized — returning fallback category")
            return "Corporate Reputation"

        prompt = build_prompt_from_text(texto)

        # Llamada determinista y corta: temperatura=0, tope de tokens de salida pequeño
        # Nota: la firma exacta depende del SDK; usamos kwargs comunes.
        def call():
            try:
                return model.generate_content(prompt, temperature=0, max_output_tokens=20)
            except TypeError:
                # si SDK no acepta esos nombres, intentar sin kwargs
                return model.generate_content(prompt)

        # usar tu retry wrapper para tolerar 429/errores temporales
        try:
            resp = retry(call, max_attempts=3)
        except Exception as e:
            logging.warning("Model call failed after retries: %s", e)
            return "Corporate Reputation"

        # parsing defensivo
        raw = ""
        try:
            raw = getattr(resp, "text", None) or ""
        except Exception:
            raw = ""

        if not raw:
            try:
                # algunos SDK devuelven candidates -> content
                cand = getattr(resp, "candidates", None)
                if cand and len(cand) > 0:
                    raw = getattr(cand[0], "content", "") or str(cand[0])
            except Exception:
                raw = str(resp)

        raw = (raw or "").strip()

        # DEBUG: log breve de respuestas inesperadas (puedes bajar a DEBUG level después)
        if raw.startswith("(") or raw.lower().startswith("por favor") or "proporciona la noticia" in raw.lower():
            logging.warning("Modelo retornó mensaje de sistema/clarificación: %s", raw)
            return "Corporate Reputation"

        # Normalizar y mapear a categoría
        cat = normalize_category_from_model_output(raw)
        if cat == "Corporate Reputation" and raw.upper() not in [c.upper() for c in CANONICAL_CATEGORIES]:
            # si normalizador cae en fallback, loguear raw para debugging (no inunda logs)
            logging.warning("Salida de modelo no mapeable a categoría: %s", raw)
        return cat

    except Exception as e:
        logging.warning("Error categorizando texto con model: %s", e)
        return "Corporate Reputation"


def categorize_row_obtaining_text(row):
    url = (row.get("link") or "").strip()
    k = url_key(url)

    # Cache hit
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

# Persistir tag cache
try:
    with open(CATEGORY_CACHE_PATH, "w", encoding="utf-8") as fh:
        json.dump(tag_cache, fh, ensure_ascii=False, indent=2)
except Exception as e:
    logging.warning("No se pudo guardar category cache: %s", e)

# ---------------------------
# SENTIMENT CLASSIFICATION (POSITIVO / NEGATIVO / NEUTRO) - usando Gemini
# ---------------------------

def analizar_noticia(url):
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
    logging.info("Leídas %d filas desde la hoja (incl header si existía).", len(values))
except HttpError as e:
    logging.exception("Failed to read existing sheet: %s", e)
    values = []
except Exception as e:
    logging.exception("Failed to read existing sheet: %s", e)
    values = []

# 2) Build set of existing links from the sheet to avoid duplicate appends
existing_links_set = set()
sheet_has_header = False
if values and len(values) >= 1:
    # assume first row is header if it matches at least some of our header names
    header_row = values[0]
    # find link index robustly
    link_idx = None
    try:
        link_idx = header_row.index('link')
        sheet_has_header = True
    except ValueError:
        # try case-insensitive match
        row_lower = [c.lower() for c in header_row]
        if 'link' in row_lower:
            link_idx = row_lower.index('link')
            sheet_has_header = True
        else:
            # fallback: if header length matches our HEADER, assume it's our header
            if len(header_row) == len(HEADER):
                # assume the header order matches
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

# If sheet was empty (no values) we will write header first
sheet_empty = len(values) == 0

# 3) Ensure final_df has correct columns and sanitized values (you already prepared final_df above)
# Re-use your sanitization logic to produce the rows to append (but only for new links)

# Ensure final_df contains header columns
for col in HEADER:
    if col not in final_df.columns:
        final_df[col] = ''

# drop duplicates by link/title/snippet as you already did
final_df = final_df.replace([np.nan, pd.NaT, None], '').replace([np.inf, -np.inf], '')

# Build list of candidate rows (in correct order), and filter out ones with link already in sheet
rows_to_add = []
new_links_count = 0
for row in final_df[HEADER].values.tolist():
    # row is list ordered as HEADER
    # sanitize each cell similarly to your sanitize_cell
    sanitized_row = []
    # create a dict-like mapping for convenience
    row_map = dict(zip(HEADER, row))
    link = str(row_map.get('link', '')).strip()
    # Skip if link missing
    if not link:
        continue
    if link in existing_links_set:
        continue
    # Sanitize each cell (reuse your sanitize_cell behavior)
    sanitized_cells = []
    for cell in row:
        # simple sanitize: convert to str, guard special pandas types
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
    logging.info("No hay filas nuevas para agregar. Saliendo sin tocar la hoja.")
    logging.info("Script finished correctamente.")
    sys.exit(0)

# 4) Prepare batches and append with retries/backoff
BATCH_SIZE = int(os.getenv("SHEET_BATCH_SIZE", "500"))  # ajustable
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
                logging.exception("Failed to append batch to Sheets after %d attempts: %s", attempt, e)
                raise
            # exponential backoff with jitter
            sleep_for = min(60, base_delay * (2 ** (attempt - 1))) + random.random() * 0.5
            logging.warning("HttpError appending to Sheets (attempt %d/%d): %s — retrying in %.1fs", attempt, max_attempts, e, sleep_for)
            time.sleep(sleep_for)
        except Exception as e:
            attempt += 1
            if attempt >= max_attempts:
                logging.exception("Failed to append batch to Sheets after %d attempts: %s", attempt, e)
                raise
            sleep_for = min(60, base_delay * (2 ** (attempt - 1))) + random.random() * 0.5
            logging.warning("Error appending to Sheets (attempt %d/%d): %s — retrying in %.1fs", attempt, max_attempts, e, sleep_for)
            time.sleep(sleep_for)

# if sheet empty, write header first (one-time)
if sheet_empty:
    logging.info("Hoja vacía: escribiendo header primero.")
    try:
        append_with_retry([HEADER])
    except Exception as e:
        logging.exception("No se pudo escribir el header en la sheet: %s", e)
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
        logging.exception("Failed appending batch starting at %d: %s", i, e)
        # continue attempting next batches (or you may prefer to abort)
        continue

logging.info("✅ Sheet updated. New rows appended: %d", total_added)
logging.info("Script finished correctamente.")
