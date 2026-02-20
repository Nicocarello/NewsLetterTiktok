from googleapiclient.discovery import build
from google.oauth2 import service_account
import pandas as pd
import os
from apify_client import ApifyClient
from datetime import datetime
import json
import pytz
import time
import sys
import math
import traceback

# --- Config ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1lhd_yh9yGNZNsAPOqZZYYsCvdUT_g6ysQImAlpQPSWA'  # tu hoja
creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

APIFY_TOKEN = os.getenv("APIFY_TOKEN")
apify_client = ApifyClient(APIFY_TOKEN)

TWITTER_ACTOR_ID = "kaitoeasyapi/twitter-x-data-tweet-scraper-pay-per-result-cheapest"
TZ_ARGENTINA = pytz.timezone("America/Argentina/Buenos_Aires")

# columnas/orden objetivo (13 columnas -> A:M)
HEADER = ['text', 'fecha', 'cuenta', 'seguidores', 'link', 'impresiones',
          'interacciones', 'compartidos', 'likes', 'comentarios', 'retweets', 'citas', 'guardados']

# --- Utilidades ---
def safe_print(*args, **kwargs):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}]", *args, **kwargs)

def is_truthy_active(v):
    if pd.isna(v):
        return False
    s = str(v).strip().lower()
    return s in ['true', '1', 'si', 'sí', 'yes', 'y', 'activado', 'activo', 'on']

# --- Lectura de usuarios ---
def load_users_from_sheet(sheet_obj, spreadsheet_id, range_name="USUARIOS!A:B"):
    try:
        resp = sheet_obj.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        values = resp.get("values", [])
    except Exception as e:
        safe_print(f"⚠️ Error leyendo USUARIOS sheet ({range_name}): {e}")
        return []

    if not values:
        return []

    header_row = [h.strip().lower() for h in values[0]] if values and any(values[0]) else []
    rows = values[1:] if len(values) > 1 else []

    # Caso: tabla con encabezados usuario/activo
    if 'usuario' in header_row:
        # crear df con rows y columnas = header original
        try:
            df_users = pd.DataFrame(rows, columns=values[0])
            df_users.columns = [c.strip().lower() for c in df_users.columns]
        except Exception:
            # fallback: construir con la primera columna
            users = [str(r[0]).strip() for r in rows if r and r[0]]
            return users

        if 'activo' in df_users.columns:
            df_users['activo_bool'] = df_users['activo'].apply(is_truthy_active)
            users = df_users[df_users['activo_bool']]['usuario'].dropna().astype(str).str.strip().tolist()
            return [u for u in users if u]
        else:
            users = df_users['usuario'].dropna().astype(str).str.strip().tolist()
            return [u for u in users if u]

    # Caso: sin cabecera -> tomar primera columna
    users = []
    for row in values:
        if not row:
            continue
        candidate = str(row[0]).strip()
        if candidate:
            users.append(candidate)
    return users

def load_users_with_fallback():
    users = load_users_from_sheet(sheet, SPREADSHEET_ID)
    if users:
        safe_print(f"🔎 Usuarios cargados desde Google Sheet: {len(users)}")
        return users
    env = os.getenv("TWITTER_USERS", "")
    if env:
        users = [u.strip() for u in env.split(",") if u.strip()]
        if users:
            safe_print(f"🔁 No había usuarios en la sheet; fallback a TWITTER_USERS env var ({len(users)} usuarios).")
            return users
    default = ["Mau_Albornoz", "fedeaikawa"]
    safe_print(f"⚠️ No se encontraron usuarios en sheet ni en TWITTER_USERS. Usando default ({len(default)}).")
    return default

# --- Apify: ejecutar actor y recuperar items (robusto) ---
def call_actor_and_get_items(actor_id, run_input, max_retries=3, backoff_base=2):
    """
    Ejecuta actor y devuelve lista de items del dataset. Maneja:
     - reintentos con backoff
     - fallback a defaultDatasetId
     - protección ante respuestas vacías
    """
    for attempt in range(1, max_retries + 1):
        try:
            safe_print(f"Ejecutando actor (intento {attempt})...")
            run = apify_client.actor(actor_id).call(run_input=run_input)
            run_id = run.get('id')
            # preferir defaultDatasetId si existe
            dataset_items = []
            if not run_id and run.get('defaultDatasetId'):
                dataset_id = run.get('defaultDatasetId')
                safe_print(f"No hay run_id; intentando dataset {dataset_id} directamente.")
                ds = apify_client.dataset(dataset_id).list_items()
                dataset_items = getattr(ds, 'items', []) or ds.get('items', []) or []
            else:
                # si hay run_id intentar leer dataset a través de run
                try:
                    run_obj = apify_client.run(run_id)
                    ds = run_obj.dataset().list_items()
                    dataset_items = getattr(ds, 'items', []) or ds.get('items', []) or []
                except Exception:
                    # fallback a defaultDatasetId del run
                    dataset_id = run.get("defaultDatasetId")
                    if dataset_id:
                        try:
                            ds = apify_client.dataset(dataset_id).list_items()
                            dataset_items = getattr(ds, 'items', []) or ds.get('items', []) or []
                        except Exception as e:
                            safe_print(f"⚠️ Error leyendo dataset {dataset_id}: {e}")
                            dataset_items = []
                    else:
                        safe_print(f"⚠️ No se encontró dataset para run {run_id}")
                        dataset_items = []

            # Normalmente dataset_items será una lista
            if isinstance(dataset_items, list):
                safe_print(f"Actor ejecutado. Items obtenidos: {len(dataset_items)}")
                return dataset_items
            else:
                # en algunos SDKs puede devolver dict paginado; intentar extraer "items" si es dict
                if isinstance(dataset_items, dict) and 'items' in dataset_items:
                    return dataset_items['items']
                safe_print("⚠️ El response del dataset no es una lista; intentando siguiente reintento.")
        except Exception as e:
            safe_print(f"❌ Error al ejecutar actor (intento {attempt}): {e}")
            # imprimir traceback en debug
            traceback.print_exc()
        # backoff antes del siguiente intento
        sleep_for = backoff_base ** attempt
        safe_print(f"Esperando {sleep_for}s antes del siguiente intento...")
        time.sleep(sleep_for)
    safe_print("❌ No se pudo obtener datos del actor tras varios intentos.")
    return []

# --- Main ---
user_list = load_users_with_fallback()
dfs = []

# parámetros base para actor (dejar within_time configurable si lo deseas)
DEFAULT_RUN_INPUT = {
    "-min_faves": 0,
    "-min_replies": 0,
    "-min_retweets": 0,
    "filter:blue_verified": False,
    "filter:consumer_video": False,
    "filter:has_engagement": False,
    "filter:hashtags": False,
    "filter:images": False,
    "filter:links": False,
    "filter:media": False,
    "filter:mentions": False,
    "filter:native_video": False,
    "filter:nativeretweets": False,
    "filter:news": False,
    "filter:pro_video": False,
    "filter:quote": False,
    "filter:replies": False,
    "filter:safe": False,
    "filter:spaces": False,
    "filter:twimg": False,
    "filter:videos": False,
    "filter:vine": False,
    "from": None,
    "include:nativeretweets": False,
    "maxItems": 10000,         # reducir límite muy alto para no pedir millones
    "min_faves": 0,
    "min_replies": 0,
    "min_retweets": 0,
    "within_time": "1d",
    "queryType": "Latest"
}

for user in user_list:
    run_input = DEFAULT_RUN_INPUT.copy()
    run_input['from'] = user
    safe_print(f"[{datetime.now().isoformat()}] Ejecutando Actor para usuario: {user}...")
    items = call_actor_and_get_items(TWITTER_ACTOR_ID, run_input)
    if not items:
        safe_print(f"⚠️ No se encontraron resultados para {user}")
    else:
        try:
            df_user = pd.DataFrame(items)
            dfs.append(df_user)
            safe_print(f"✅ Agregados {len(df_user)} items para {user}")
        except Exception as e:
            safe_print(f"⚠️ Error convirtiendo items a DataFrame para {user}: {e}")
    time.sleep(1)  # pausa pequeña, ajustar si da rate limits

# Si no hay datos, salimos
if not dfs:
    safe_print("❌ No se obtuvieron datos de ningún usuario. Terminando.")
    sys.exit(0)

# Concatenar
df = pd.concat(dfs, ignore_index=True, sort=False)
# Excluir mock_tweet si existe
if 'type' in df.columns:
    df = df[df['type'] != 'mock_tweet']
safe_print(f"DataFrame concatenado con {len(df)} filas (post-filter).")

# --- Normalizaciones y cálculos ---
# Extraer followers / username del sub-dict 'author' si existe
if 'author' in df.columns:
    df['seguidores'] = df['author'].apply(lambda x: x.get('followers') if isinstance(x, dict) else None)
    df['cuenta'] = df['author'].apply(lambda x: x.get('userName') if isinstance(x, dict) else None)
else:
    if 'followers' in df.columns:
        df['seguidores'] = df['followers']
    else:
        df['seguidores'] = 0
    if 'username' in df.columns:
        df['cuenta'] = df['username']
    else:
        df['cuenta'] = ''

# Asegurar columnas numéricas
num_cols_map = {
    'likeCount': 'likes',
    'replyCount': 'comentarios',
    'retweetCount': 'retweets',
    'quoteCount': 'citas',
    'bookmarkCount': 'guardados',
    'viewCount': 'impresiones'
}
for col in list(num_cols_map.keys()) + ['followers']:
    if col not in df.columns:
        df[col] = 0

for col in list(num_cols_map.keys()) + ['followers']:
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

df['interacciones'] = (df.get('likeCount', 0)
                       + df.get('replyCount', 0)
                       + df.get('retweetCount', 0)
                       + df.get('quoteCount', 0)
                       + df.get('bookmarkCount', 0))
df['compartidos'] = (df.get('retweetCount', 0) + df.get('quoteCount', 0))

# rename seguro sólo si existe la clave
rename_map = {
    'createdAt': 'fecha',
    'url': 'link',
    'viewCount': 'impresiones',
    'likeCount': 'likes',
    'replyCount': 'comentarios',
    'retweetCount': 'retweets',
    'quoteCount': 'citas',
    'bookmarkCount': 'guardados',
}
existing_rename = {k: v for k, v in rename_map.items() if k in df.columns}
df = df.rename(columns=existing_rename)

# text fallback
if 'text' not in df.columns:
    for alt in ['content', 'title', 'html']:
        if alt in df.columns:
            df['text'] = df[alt].astype(str)
            break
    else:
        df['text'] = ''

# eliminar menciones al inicio (@usuario)
df = df[~df['text'].str.startswith('@', na=False)]

# normalizar columnas finales (asegurar que existan)
for col in ['impresiones', 'interacciones', 'compartidos', 'likes', 'comentarios', 'retweets', 'citas', 'guardados', 'seguidores']:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    else:
        df[col] = 0

# manejar fecha: intentar parsear, convertir a TZ_ARGENTINA y formatear; si falla dejar vacio
if 'fecha' in df.columns and df['fecha'].notna().any():
    df['fecha_parsed'] = pd.to_datetime(df['fecha'], errors='coerce', utc=True)
    mask_valid = df['fecha_parsed'].notna()
    if mask_valid.any():
        df.loc[mask_valid, 'fecha'] = df.loc[mask_valid, 'fecha_parsed'].dt.tz_convert(TZ_ARGENTINA).dt.strftime('%Y-%m-%d %H:%M:%S')
    df.loc[~mask_valid, 'fecha'] = ''
    df.drop(columns=['fecha_parsed'], inplace=True, errors='ignore')
else:
    df['fecha'] = ''

# Asegurar columnas header
for col in HEADER:
    if col not in df.columns:
        df[col] = ''

final_df = df[HEADER].copy()

# === Leer registros existentes en la hoja TWEETS!A:M ===
try:
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range="TWEETS!A:M").execute()
    values = result.get("values", [])
except Exception as e:
    safe_print(f"⚠️ Error leyendo la hoja: {e}")
    values = []

if values:
    existing_df = pd.DataFrame(values[1:], columns=values[0])
else:
    existing_df = pd.DataFrame(columns=HEADER)

# Normalizar 'link' en existing_df
if 'link' not in existing_df.columns:
    existing_df['link'] = ''
existing_df['link'] = existing_df['link'].astype(str).fillna('').str.strip()
final_df['link'] = final_df['link'].astype(str).fillna('').str.strip()

# Concatenar y deduplicar — preferir filas más recientes si tenemos 'fecha'
combined_df = pd.concat([existing_df, final_df], ignore_index=True, sort=False)
combined_df = combined_df.astype(str).fillna('')

# Si hay links no vacíos, dedupe por link (mantener la fila con fecha más reciente si la fecha existe)
if combined_df['link'].str.strip().ne('').any():
    # Si 'fecha' parseable, convertir a datetime para ordenar; si no, mantener primer encontrado
    def parse_fecha_safe(x):
        try:
            return pd.to_datetime(x)
        except Exception:
            return pd.NaT
    combined_df['_fecha_dt'] = combined_df['fecha'].apply(parse_fecha_safe)
    # Ordenar por link + fecha desc (NaT al final), y quedarnos con la primera aparición (la más reciente)
    combined_df = combined_df.sort_values(by=['link', '_fecha_dt'], ascending=[True, False])
    combined_df = combined_df.drop_duplicates(subset=['link'], keep='first')
    combined_df.drop(columns=['_fecha_dt'], inplace=True, errors='ignore')
else:
    # no hay links: dedupe completo conservando la fila con fecha más reciente si hay fecha
    combined_df = combined_df.drop_duplicates(keep='first')

# Alineo columnas exactas del header y relleno vacíos
for col in HEADER:
    if col not in combined_df.columns:
        combined_df[col] = ''
combined_df = combined_df[HEADER].astype(str).fillna('')

# Intentar limpiar el rango antes de escribir
try:
    sheet.values().clear(spreadsheetId=SPREADSHEET_ID, range="TWEETS!A:M").execute()
except Exception as e:
    safe_print(f"Warning: no pude limpiar el rango antes de escribir: {e}")

values_to_write = [HEADER] + combined_df.values.tolist()

try:
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range="TWEETS!A1",
        valueInputOption="RAW",
        body={"values": values_to_write}
    ).execute()
    safe_print("✅ Hoja actualizada sin duplicados.")
except Exception as e:
    safe_print(f"❌ Error al escribir en la hoja: {e}")
    traceback.print_exc()
