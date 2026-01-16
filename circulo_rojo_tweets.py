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
header = ['text', 'fecha', 'cuenta', 'seguidores', 'link', 'impresiones',
          'interacciones', 'compartidos', 'likes', 'comentarios', 'retweets', 'citas', 'guardados']

def load_users_from_sheet(sheet_obj, spreadsheet_id, range_name="USUARIOS!A:B"):
    """
    Intenta leer la hoja 'USUARIOS' y devuelve una lista de usuarios activos.
    Soporta:
      - Cabecera ['usuario','activo'] -> toma sólo los que tienen activo truthy.
      - Sólo una columna (usuario) -> toma todos los no vacíos.
    Si falla o no hay datos, devuelve lista vacía.
    """
    try:
        resp = sheet_obj.values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()
        values = resp.get("values", [])
    except Exception as e:
        print(f"⚠️ Error leyendo USUARIOS sheet ({range_name}): {e}")
        return []

    if not values:
        return []

    # Si hay cabecera detectarla
    header_row = [h.strip().lower() for h in values[0]]
    rows = values[1:] if len(values) > 1 else []

    # Caso: tabla con encabezados usuario/activo
    if 'usuario' in header_row:
        df_users = pd.DataFrame(rows, columns=values[0])
        # Normalizar columnas si existieran en diferentes mayúsculas
        df_users.columns = [c.strip().lower() for c in df_users.columns]
        # Si hay columna 'activo', filtramos
        if 'activo' in df_users.columns:
            def is_active(v):
                if pd.isna(v): 
                    return False
                s = str(v).strip().lower()
                return s in ['true', '1', 'si', 'sí', 'yes', 'y', 'activado', 'activo']
            df_users['activo_bool'] = df_users['activo'].apply(is_active)
            users = df_users[df_users['activo_bool']]['usuario'].dropna().astype(str).str.strip().tolist()
            return [u for u in users if u]
        else:
            # No hay columna activo: tomar todos los usuarios no vacíos
            users = df_users['usuario'].dropna().astype(str).str.strip().tolist()
            return [u for u in users if u]

    # Caso: sin cabecera (o cabecera no incluye 'usuario'): tratar la primera columna como lista de usuarios
    # Flatten values: cada fila puede ser [usuario] o [usuario, activo]
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
        print(f"🔎 Usuarios cargados desde Google Sheet: {len(users)}")
        return users
    # fallback: variable de entorno
    env = os.getenv("TWITTER_USERS", "")
    if env:
        users = [u.strip() for u in env.split(",") if u.strip()]
        if users:
            print(f"🔁 No había usuarios en la sheet; fallback a TWITTER_USERS env var ({len(users)} usuarios).")
            return users
    # último recurso: lista pequeña por defecto (opcional)
    default = ["Mau_Albornoz", "fedeaikawa"]
    print(f"⚠️ No se encontraron usuarios en sheet ni en TWITTER_USERS. Usando default ({len(default)}).")
    return default

# --- Cargar lista de usuarios dinámicamente ---
user_list = load_users_with_fallback()

# --- Scraping con Apify ---
dfs = []
for user in user_list:
    run_input = {
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
        "from": user,
        "include:nativeretweets": False,
        "maxItems": 1000000,
        "min_faves": 0,
        "min_replies": 0,
        "min_retweets": 0,
        "within_time": "1d",
        "queryType": "Latest"
    }

    print(f"[{datetime.now()}] Ejecutando Actor para usuario: {user}...")
    try:
        run = apify_client.actor(TWITTER_ACTOR_ID).call(run_input=run_input)
    except Exception as e:
        print(f"❌ Error al ejecutar actor para {user}: {e}")
        continue

    run_id = run.get('id')
    print(f"Actor ejecutado. ID: {run_id}, estado: {run.get('status')}")

    # Obtener items del dataset (intentar por run luego por defaultDatasetId)
    dataset_items = []
    try:
        if run_id:
            run_obj = apify_client.run(run_id)
            try:
                dataset_items = run_obj.dataset().list_items().items or []
            except Exception:
                dataset_id = run.get("defaultDatasetId")
                if dataset_id:
                    try:
                        dataset_items = apify_client.dataset(dataset_id).list_items().items or []
                    except Exception as e:
                        print(f"⚠️ Error leyendo dataset {dataset_id}: {e}")
                        dataset_items = []
                else:
                    print(f"⚠️ No se encontró dataset para run {run_id}")
                    dataset_items = []
    except Exception as e:
        print(f"⚠️ Error general al obtener dataset para run {run_id}: {e}")
        dataset_items = []

    if dataset_items:
        try:
            dfs.append(pd.DataFrame(dataset_items))
            print(f"✅ Agregados {len(dataset_items)} items para {user}")
        except Exception as e:
            print(f"⚠️ Error convirtiendo items a DataFrame para {user}: {e}")
    else:
        print(f"⚠️ No se encontraron resultados para {user}")

    time.sleep(1)  # pequeña pausa

# Si no hay datos, cortamos
if not dfs:
    print("❌ No se obtuvieron datos de ningún usuario. Terminando.")
    sys.exit(0)

# (El resto del script continúa exactamente igual que ya lo tenías:)
# Concatenar y limpiar 'mock_tweet' si existe
df = pd.concat(dfs, ignore_index=True)
if 'type' in df.columns:
    df = df[df['type'] != 'mock_tweet']
print(f"DataFrame concatenado con {len(df)} filas (post-filter).")

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

for col in ['likeCount', 'replyCount', 'retweetCount', 'quoteCount', 'bookmarkCount', 'viewCount']:
    if col not in df.columns:
        df[col] = 0

for col in ['likeCount', 'replyCount', 'retweetCount', 'quoteCount', 'bookmarkCount', 'viewCount']:
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

df['interacciones'] = (df['likeCount']
                       + df['replyCount']
                       + df['retweetCount']
                       + df['quoteCount']
                       + df['bookmarkCount'])
df['compartidos'] = (df['retweetCount'] + df['quoteCount'])

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

if 'text' not in df.columns:
    for alt in ['content', 'title', 'html']:
        if alt in df.columns:
            df['text'] = df[alt].astype(str)
            break
    else:
        df['text'] = ''

df = df[~df['text'].str.startswith('@', na=False)]

for col in ['impresiones', 'interacciones', 'compartidos', 'likes', 'comentarios', 'retweets', 'citas', 'guardados', 'seguidores']:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    else:
        df[col] = 0

if 'fecha' in df.columns and df['fecha'].notna().any():
    df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce', utc=True)
    mask_valid = df['fecha'].notna()
    df.loc[mask_valid, 'fecha'] = df.loc[mask_valid, 'fecha'].dt.tz_convert(TZ_ARGENTINA).dt.strftime('%Y-%m-%d %H:%M:%S')
    df.loc[~mask_valid, 'fecha'] = ''
else:
    df['fecha'] = ''

for col in header:
    if col not in df.columns:
        df[col] = ''

final_df = df[header].copy()

# === Leer registros existentes en la hoja TWEETS!A:M ===
try:
    result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="TWEETS!A:M"
    ).execute()
    values = result.get("values", [])
except Exception as e:
    print(f"⚠️ Error leyendo la hoja: {e}")
    values = []

if values:
    existing_df = pd.DataFrame(values[1:], columns=values[0])
else:
    existing_df = pd.DataFrame(columns=header)

if 'link' not in existing_df.columns:
    existing_df['link'] = ''

combined_df = pd.concat([existing_df, final_df], ignore_index=True, sort=False)
combined_df['link'] = combined_df['link'].astype(str).fillna('').str.strip()

if combined_df['link'].str.strip().ne('').any():
    combined_df = combined_df.drop_duplicates(subset=["link"], keep='first')
else:
    combined_df = combined_df.drop_duplicates(keep='first')

combined_df = combined_df.astype(str).fillna('')

for col in header:
    if col not in combined_df.columns:
        combined_df[col] = ''

combined_df = combined_df[header]

try:
    sheet.values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range="TWEETS!A:M"
    ).execute()
except Exception as e:
    print(f"Warning: no pude limpiar el rango antes de escribir: {e}")

values_to_write = [header] + combined_df.values.tolist()

try:
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range="TWEETS!A1",
        valueInputOption="RAW",
        body={"values": values_to_write}
    ).execute()
    print("✅ Hoja actualizada sin duplicados.")
except Exception as e:
    print(f"❌ Error al escribir en la hoja: {e}")
