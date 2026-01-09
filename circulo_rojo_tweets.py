from googleapiclient.discovery import build
from google.oauth2 import service_account
import pandas as pd
import os
from apify_client import ApifyClient
from datetime import datetime
import json
import pytz
import time

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
user_list = ["Mau_Albornoz", "fedeaikawa"]
TZ_ARGENTINA = pytz.timezone("America/Argentina/Buenos_Aires")

# columnas/orden objetivo (13 columnas -> A:M)
header = ['text', 'fecha', 'cuenta', 'seguidores', 'link', 'impresiones',
          'interacciones', 'compartidos', 'likes', 'comentarios', 'retweets', 'citas', 'guardados']

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
    try:
        dataset_items = apify_client.run(run_id).dataset().list_items().items
    except Exception:
        dataset_id = run.get("defaultDatasetId")
        if dataset_id:
            try:
                dataset_items = apify_client.dataset(dataset_id).list_items().items
            except Exception as e:
                print(f"⚠️ Error leyendo dataset {dataset_id}: {e}")
                dataset_items = []
        else:
            print(f"⚠️ No se encontró dataset para run {run_id}")
            dataset_items = []

    if dataset_items:
        dfs.append(pd.DataFrame(dataset_items))
    else:
        print(f"⚠️ No se encontraron resultados para {user}")

    time.sleep(1)  # pequeña pausa

# Si no hay datos, cortamos
if not dfs:
    print("❌ No se obtuvieron datos de ningún usuario. Terminando.")
    exit(0)

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
    df['seguidores'] = df.get('followers', None)
    df['cuenta'] = df.get('username', None)

# Asegurarse columnas de conteos existan; si no, crear con 0
for col in ['likeCount', 'replyCount', 'retweetCount', 'quoteCount', 'bookmarkCount', 'viewCount']:
    if col not in df.columns:
        df[col] = 0

# métricas
df['interacciones'] = (df['likeCount'].fillna(0).astype(int)
                       + df['replyCount'].fillna(0).astype(int)
                       + df['retweetCount'].fillna(0).astype(int)
                       + df['quoteCount'].fillna(0).astype(int)
                       + df['bookmarkCount'].fillna(0).astype(int))
df['compartidos'] = (df['retweetCount'].fillna(0).astype(int)
                     + df['quoteCount'].fillna(0).astype(int))

# renombrar columnas al mapa deseado (si existen)
rename_map = {
    'createdAt': 'fecha',
    'url': 'link',
    'viewCount': 'impresiones',
    'likeCount': 'likes',
    'replyCount': 'comentarios',
    'retweetCount': 'retweets',
    'quoteCount': 'citas',
    'bookmarkCount': 'guardados',
    'text': 'text'
}
existing_rename = {k: v for k, v in rename_map.items() if k in df.columns}
df = df.rename(columns=existing_rename)

# si 'text' no existe, intentar otros campos
if 'text' not in df.columns:
    for alt in ['content', 'title', 'html']:
        if alt in df.columns:
            df['text'] = df[alt].astype(str)
            break
    else:
        df['text'] = ''

# filtrar replies que comienzan con '@'
df = df[~df['text'].str.startswith('@', na=False)]

# formatear 'fecha' como YYYY-MM-DD HH:MM:SS
if 'fecha' in df.columns:
    df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce', utc=True).dt.tz_convert(TZ_ARGENTINA)
    df['fecha'] = df['fecha'].dt.strftime('%Y-%m-%d %H:%M:%S')
else:
    df['fecha'] = ''

# Asegurarse columnas objetivo existen; crear vacías donde hagan falta
for col in header:
    if col not in df.columns:
        df[col] = ''

# Seleccionar sólo las columnas deseadas en el orden correcto
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

# Asegurar columna 'link' en existing_df
if 'link' not in existing_df.columns:
    existing_df['link'] = ''

# === Concatenar y limpiar duplicados por 'link' ===
combined_df = pd.concat([existing_df, final_df], ignore_index=True, sort=False)

if 'link' in combined_df.columns and combined_df['link'].notna().any():
    combined_df = combined_df.drop_duplicates(subset=["link"])
else:
    combined_df = combined_df.drop_duplicates()

# Convertir todo a string y rellenar NaN
combined_df = combined_df.astype(str).fillna('')

# === Sobrescribir hoja: limpiar rango TWEETS!A:M y escribir ===
try:
    sheet.values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range="TWEETS!A:M"
    ).execute()
except Exception as e:
    print(f"Warning: no pude limpiar el rango antes de escribir: {e}")

values_to_write = [header] + combined_df[header].values.tolist()

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
