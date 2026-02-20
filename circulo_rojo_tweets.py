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
import traceback
import math

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

# --- Opciones de ejecución ---
# Si querés correr histórico, setear en el entorno HISTORICAL_SINCE="2026-01-01"
# opcional HISTORICAL_UNTIL="2026-02-20" (si no está, se toma hoy)
HISTORICAL_SINCE = os.getenv("HISTORICAL_SINCE", "2026-01-01").strip()  # e.g. "2026-01-01"
HISTORICAL_UNTIL = os.getenv("HISTORICAL_UNTIL", "").strip()  # optional, format YYYY-MM-DD
# Tamaño de chunk para append (500 suele ser seguro)
APPEND_CHUNK = int(os.getenv("APPEND_CHUNK", "500"))

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

    if 'usuario' in header_row:
        try:
            df_users = pd.DataFrame(rows, columns=values[0])
            df_users.columns = [c.strip().lower() for c in df_users.columns]
        except Exception:
            users = [str(r[0]).strip() for r in rows if r and r[0]]
            return users

        if 'activo' in df_users.columns:
            df_users['activo_bool'] = df_users['activo'].apply(is_truthy_active)
            users = df_users[df_users['activo_bool']]['usuario'].dropna().astype(str).str.strip().tolist()
            return [u for u in users if u]
        else:
            users = df_users['usuario'].dropna().astype(str).str.strip().tolist()
            return [u for u in users if u]

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
    for attempt in range(1, max_retries + 1):
        try:
            safe_print(f"Ejecutando actor (intento {attempt})...")
            run = apify_client.actor(actor_id).call(run_input=run_input)
            run_id = run.get('id')
            dataset_items = []
            if not run_id and run.get('defaultDatasetId'):
                dataset_id = run.get('defaultDatasetId')
                safe_print(f"No hay run_id; intentando dataset {dataset_id} directamente.")
                ds = apify_client.dataset(dataset_id).list_items()
                dataset_items = getattr(ds, 'items', []) or ds.get('items', []) or []
            else:
                try:
                    run_obj = apify_client.run(run_id)
                    ds = run_obj.dataset().list_items()
                    dataset_items = getattr(ds, 'items', []) or ds.get('items', []) or []
                except Exception:
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

            if isinstance(dataset_items, list):
                safe_print(f"Actor ejecutado. Items obtenidos: {len(dataset_items)}")
                return dataset_items
            else:
                if isinstance(dataset_items, dict) and 'items' in dataset_items:
                    return dataset_items['items']
                safe_print("⚠️ El response del dataset no es una lista; intentando siguiente reintento.")
        except Exception as e:
            safe_print(f"❌ Error al ejecutar actor (intento {attempt}): {e}")
            traceback.print_exc()
        sleep_for = backoff_base ** attempt
        safe_print(f"Esperando {sleep_for}s antes del siguiente intento...")
        time.sleep(sleep_for)
    safe_print("❌ No se pudo obtener datos del actor tras varios intentos.")
    return []

# --- Helper para escribir en Google Sheets en chunks ---
def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# --- Main ---
def main():
    user_list = load_users_with_fallback()
    dfs = []

    # Construir run_input base; si HISTORICAL_SINCE está definido usamos query con since/until
    for user in user_list:
        if HISTORICAL_SINCE:
            today_str = HISTORICAL_UNTIL if HISTORICAL_UNTIL else datetime.now().strftime("%Y-%m-%d")
            query = f"from:{user} since:{HISTORICAL_SINCE} until:{today_str}"
            run_input = {
                "query": query,
                "maxItems": 10000,
                "queryType": "Latest"
            }
        else:
            # modo incremental (default) usando within_time
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
                "maxItems": 10000,
                "min_faves": 0,
                "min_replies": 0,
                "min_retweets": 0,
                "within_time": "1d",
                "queryType": "Top"
            }

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
        time.sleep(1)

    if not dfs:
        safe_print("❌ No se obtuvieron datos de ningún usuario. Terminando.")
        sys.exit(0)

    # Concatenar
    df = pd.concat(dfs, ignore_index=True, sort=False)
    if 'type' in df.columns:
        df = df[df['type'] != 'mock_tweet']
    safe_print(f"DataFrame concatenado con {len(df)} filas (post-filter).")

    # --- Normalizaciones y cálculos ---
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

    for col in ['likeCount', 'replyCount', 'retweetCount', 'quoteCount', 'bookmarkCount', 'viewCount', 'followers']:
        if col not in df.columns:
            df[col] = 0

    for col in ['likeCount', 'replyCount', 'retweetCount', 'quoteCount', 'bookmarkCount', 'viewCount', 'followers']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    df['interacciones'] = (df.get('likeCount', 0)
                           + df.get('replyCount', 0)
                           + df.get('retweetCount', 0)
                           + df.get('quoteCount', 0)
                           + df.get('bookmarkCount', 0))
    df['compartidos'] = (df.get('retweetCount', 0) + df.get('quoteCount', 0))

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

    # manejar fecha
    if 'fecha' in df.columns and df['fecha'].notna().any():
        df['fecha_parsed'] = pd.to_datetime(df['fecha'], errors='coerce', utc=True)
        mask_valid = df['fecha_parsed'].notna()
        if mask_valid.any():
            df.loc[mask_valid, 'fecha'] = df.loc[mask_valid, 'fecha_parsed'].dt.tz_convert(TZ_ARGENTINA).dt.strftime('%Y-%m-%d %H:%M:%S')
        df.loc[~mask_valid, 'fecha'] = ''
        df.drop(columns=['fecha_parsed'], inplace=True, errors='ignore')
    else:
        df['fecha'] = ''

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

    # --- BACKUP antes de tocar la hoja ---
    backup_title = f"TWEETS_BACKUP_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    try:
        add_sheet_req = {
            "requests": [
                {"addSheet": {"properties": {"title": backup_title}}}
            ]
        }
        try:
            service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=add_sheet_req).execute()
        except Exception:
            # si falla, puede ser porque la sheet ya existe o permisos; no cortamos ejecución
            pass

        if values:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{backup_title}!A1",
                valueInputOption="RAW",
                body={"values": values}
            ).execute()
            safe_print(f"📦 Backup creado en pestaña: {backup_title}")
        else:
            safe_print("📦 No había filas para backupear (values vacío).")
    except Exception as e:
        safe_print(f"⚠️ No pude crear backup: {e}")
        traceback.print_exc()

    # --- Lógica de append seguro ---
    if 'link' not in existing_df.columns:
        existing_df['link'] = ''
    existing_df['link'] = existing_df['link'].astype(str).fillna('').str.strip()
    existing_links = set(existing_df['link'].tolist())

    def make_key_row(r):
        cuenta = str(r.get('cuenta','')).strip()
        fecha = str(r.get('fecha','')).strip()
        text = str(r.get('text','')).strip()[:300]
        return f"{cuenta}||{fecha}||{text}"

    existing_keys = set()
    for _, row in existing_df.iterrows():
        existing_keys.add(make_key_row(row))

    final_df['link'] = final_df['link'].astype(str).fillna('').str.strip()
    final_df['cuenta'] = final_df['cuenta'].astype(str).fillna('').str.strip()
    final_df['fecha'] = final_df['fecha'].astype(str).fillna('').str.strip()
    final_df['text'] = final_df['text'].astype(str).fillna('').str.strip()

    mask_with_link = final_df['link'].str.strip() != ''
    df_with_link = final_df[mask_with_link].copy()
    df_no_link = final_df[~mask_with_link].copy()

    if not df_with_link.empty:
        df_with_link_new = df_with_link[~df_with_link['link'].isin(existing_links)].copy()
    else:
        df_with_link_new = pd.DataFrame(columns=final_df.columns)

    new_rows_no_link = []
    for _, row in df_no_link.iterrows():
        k = make_key_row(row)
        if k not in existing_keys:
            new_rows_no_link.append(row)
            existing_keys.add(k)

    if new_rows_no_link:
        df_no_link_new = pd.DataFrame(new_rows_no_link)
    else:
        df_no_link_new = pd.DataFrame(columns=final_df.columns)

    to_append_df = pd.concat([df_with_link_new, df_no_link_new], ignore_index=True, sort=False)

    if to_append_df.empty:
        safe_print("ℹ️ No hay tweets nuevos para agregar. No se modifica la hoja.")
        return

    # Preparar valores (sin cabecera) para append
    values_to_append = to_append_df[HEADER].values.tolist()

    # Si la hoja estaba vacía (values == []), escribir header + rows
    try:
        if not values:
            all_values = [HEADER] + values_to_append
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range="TWEETS!A1",
                valueInputOption="RAW",
                body={"values": all_values}
            ).execute()
            safe_print(f"✅ Hoja vacía: escrito header + {len(values_to_append)} filas.")
        else:
            # Append por chunks
            total = len(values_to_append)
            appended = 0
            for chunk in chunks(values_to_append, APPEND_CHUNK):
                sheet.values().append(
                    spreadsheetId=SPREADSHEET_ID,
                    range="TWEETS!A:M",
                    valueInputOption="RAW",
                    insertDataOption="INSERT_ROWS",
                    body={"values": chunk}
                ).execute()
                appended += len(chunk)
                safe_print(f"✅ Agregadas {len(chunk)} filas (total agregado: {appended}/{total}).")
            safe_print(f"🎉 Append completo: {appended} filas agregadas.")
    except Exception as e:
        safe_print(f"❌ Error al hacer append en la hoja: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
