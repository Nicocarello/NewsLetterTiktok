"""
apify_to_gsheets.py
-------------------
Ejecuta el actor `scrapestorm/google-news-scraper-fast-cheap-pay-per-results`
para Argentina, Peru y Chile, y AGREGA las filas al Google Sheet existente.

Dependencias:
    pip install apify-client gspread google-auth

Configuración:
    1. Crea credentials.json con tu Service Account de Google Cloud.
    2. Compartí el Sheet con el email del Service Account.
    3. Completá las 4 variables de CONFIG más abajo.

─────────────────────────────────────────────────────────────────────────────
Campos que devuelve el actor  →  columnas del Sheet
─────────────────────────────────────────────────────────────────────────────
Campo Apify       | Ejemplo                              | Col | Sheet
------------------|--------------------------------------|-----|-------------------------
"Date"            | "2026-05-15 14:44:24"                |  A  | semana  (calculado)
"Date"            | "2026-05-15 14:44:24"                |  B  | date_utc
(loop)            | "Argentina"                          |  C  | country
"Title"           | "Caso Karla Robles..."               |  D  | title
"Link"            | "https://news.google.com/rss/..."    |  E  | link
"Source Name"     | "Enterate Noticias"  (como domain)   |  F  | domain
"Source Name"     | "Enterate Noticias"                  |  G  | source
—                 | ""                                   |  H  | tier       (manual)
"Description"     | "Caso Karla Robles..."               |  I  | snippet
—                 | ""                                   |  J  | tag        (manual)
—                 | ""                                   |  K  | sentiment  (manual)
(ejecución)       | "15/05/2026 14:44"                   |  L  | scraped_at
—                 | ""                                   |  M  | enviar     (manual)
—                 | ""                                   |  N  | tema       (manual)
—                 | ""                                   |  O  | prioridad  (manual)
—                 | ""                                   |  P  | alerta_enviada (manual)
—                 | ""                                   |  Q  | justificacion_sentiment (manual)
─────────────────────────────────────────────────────────────────────────────
"""

import re
import time
from datetime import datetime, timedelta, timezone

from apify_client import ApifyClient
import gspread
from google.oauth2.service_account import Credentials


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS")
APIFY_API_TOKEN = os.getenv("APIFY_TOKEN")
GOOGLE_SHEET_ID  = "1du5Cx3pK1LnxoVeBXTzP-nY-OSvflKXjJZw2Lq-AE14"   # ID entre /d/ y /edit en la URL
SHEET_TAB_NAME   = "2026"                # Pestaña exacta del Sheet

# ─────────────────────────────────────────────
# Parámetros del scraper
# ─────────────────────────────────────────────

SEARCH_QUERY = "tiktok"

# (country_label exacto del actor, nombre que va en col C del Sheet)
COUNTRIES = [
    ("Argentina AR", "Argentina"),
    ("Peru PE",      "Peru"),
    ("Chile CL",     "Chile"),
]

ACTOR_BASE = {
    "keywords":             [SEARCH_QUERY],
    "timeFilter":           "Less than a day",
    "language":             "Spanish",
    "maxResultsPerKeyword": 30000,
}

# ─────────────────────────────────────────────
# Helpers de transformación
# ─────────────────────────────────────────────

MESES = ["ENE","FEB","MAR","ABR","MAY","JUN",
         "JUL","AGO","SEP","OCT","NOV","DIC"]


def _semana(date_val: str) -> str:
    """
    "2026-05-15 14:44:24"  →  "11-17 MAY"
    """
    try:
        dt     = datetime.fromisoformat(str(date_val)[:10])
        monday = dt - timedelta(days=dt.weekday())
        sunday = monday + timedelta(days=6)
        return f"{monday.day}-{sunday.day} {MESES[monday.month - 1]}"
    except Exception:
        return ""


def _clean_description(text: str) -> str:
    """Elimina los &nbsp; que trae el campo Description."""
    return re.sub(r"&nbsp;", " ", text or "").strip()


def build_row(item: dict, country_name: str, scraped_ts: str) -> list:
    """
    Mapea los campos exactos del actor a las columnas A→Q del Sheet.

    Campos del actor:
        "Id", "Keyword", "Title", "Description",
        "Source Name", "Published_time", "Date", "Link", "Image"
    """
    date_str    = item.get("Date", "")            # "2026-05-15 14:44:24"
    title       = item.get("Title", "")
    description = _clean_description(item.get("Description", ""))
    source_name = item.get("Source Name", "")
    link        = item.get("Link", "")

    return [
        _semana(date_str),   # A  semana
        date_str,            # B  date_utc
        country_name,        # C  country
        title,               # D  title
        link,                # E  link
        source_name,         # F  domain  (usamos Source Name como proxy del dominio)
        source_name,         # G  source
        "",                  # H  tier              ← manual
        description,         # I  snippet
        "",                  # J  tag               ← manual
        "",                  # K  sentiment         ← manual
        scraped_ts,          # L  scraped_at
        "",                  # M  enviar            ← manual
        "",                  # N  tema              ← manual
        "",                  # O  prioridad         ← manual
        "",                  # P  alerta_enviada    ← manual
        "",                  # Q  justificacion_sentiment ← manual
    ]


# ─────────────────────────────────────────────
# Apify
# ─────────────────────────────────────────────

def run_actor_for_country(client: ApifyClient,
                           country_label: str, country_name: str) -> list[dict]:
    actor_input = {**ACTOR_BASE, "country": country_label}
    print(f"\n  ▶ [{country_name}]  country='{country_label}'")
    run        = client.actor(
                     "scrapestorm/google-news-scraper-fast-cheap-pay-per-results"
                 ).call(run_input=actor_input)
    dataset_id = run["defaultDatasetId"]
    print(f"    Run ID: {run['id']} | Dataset: {dataset_id}")
    items = list(client.dataset(dataset_id).iterate_items())
    print(f"    Items recibidos: {len(items)}")
    return items


# ─────────────────────────────────────────────
# Google Sheets
# ─────────────────────────────────────────────

def open_existing_sheet(gc: gspread.Client) -> gspread.Worksheet:
    """Abre la pestaña existente sin tocar encabezados ni datos previos."""
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    ws = sh.worksheet(SHEET_TAB_NAME)
    print(f"  Sheet abierto: '{SHEET_TAB_NAME}' | filas actuales: {len(ws.get_all_values())}")
    return ws


def append_rows_batched(ws: gspread.Worksheet,
                         rows: list[list], batch: int = 500):
    total = len(rows)
    for i in range(0, total, batch):
        chunk = rows[i : i + batch]
        ws.append_rows(chunk,
                       value_input_option="USER_ENTERED",
                       insert_data_option="INSERT_ROWS")
        end = min(i + batch, total)
        print(f"    ✔ Filas {i+1}–{end} de {total} escritas")
        if end < total:
            time.sleep(1)


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    scraped_ts   = datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M")
    apify_client = ApifyClient(APIFY_API_TOKEN)

    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds  = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    gc     = gspread.authorize(creds)

    print(f"\n🔍  Query     : '{SEARCH_QUERY}'")
    print(f"🌎  Países    : {[c[1] for c in COUNTRIES]}")
    print(f"📅  scraped_at: {scraped_ts}\n")

    ws            = open_existing_sheet(gc)
    total_written = 0

    for country_label, country_name in COUNTRIES:
        items = run_actor_for_country(apify_client, country_label, country_name)

        if not items:
            print(f"    ⚠  Sin resultados para {country_name}.")
            continue

        rows = [build_row(item, country_name, scraped_ts) for item in items]
        print(f"    Escribiendo {len(rows)} filas ...")
        append_rows_batched(ws, rows)
        total_written += len(rows)

        time.sleep(2)

    print(f"\n✅  ¡Listo!  {total_written} filas agregadas.")
    print(f"   https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}")


if __name__ == "__main__":
    main()