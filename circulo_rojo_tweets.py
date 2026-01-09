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
# Sheet destino (el que nos pasaste en el ejemplo del segundo script)
SPREADSHEET_ID = '1lhd_yh9yGNZNsAPOqZZYYsCvdUT_g6ysQImAlpQPSWA'

# Entorno / credenciales Google (igual que tu segundo script)
creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

# Apify
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
apify_client = ApifyClient(APIFY_TOKEN)

# Actor de Twitter (del primer script)
TWITTER_ACTOR_ID = "kaitoeasyapi/twitter-x-data-tweet-scraper-pay-per-result-cheapest"

# Lista de usuarios a scrapear (ejemplo del primer script)
user_list = ["Mau_Albornoz", "fedeaikawa"]

# Zona horaria Argentina
TZ_ARGENTINA = pytz.timezone("America/Argentina/Buenos_Aires")

# Initialize an empty list to store DataFrames
dfs = []

# Asegúrate de que el Actor que uses realmente exista y tenga un ID válido
for user in user_list:
  actor_id = "kaitoeasyapi/twitter-x-data-tweet-scraper-pay-per-result-cheapest"
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

  print(f"Ejecutando Actor para usuario: {user}...")
  run = apify_client.actor(actor_id).call(run_input=run_input)
  
  print(f"Actor ejecutado. ID de la ejecución: {run['id']}")
  print(f"Estado de la ejecución: {run['status']}")

  # Obtener los items del dataset
  dataset_items = apify_client.run(run['id']).dataset().list_items().items

  # Crear el DataFrame y agregarlo a la lista
  if dataset_items:
    dfs.append(pd.DataFrame(dataset_items))
  else:
    print(f"No se encontraron resultados para el usuario {user}")

# Concatenar todos los DataFrames
df = pd.concat(dfs, ignore_index=True)
# drop rows that 'type' == 'mock_tweet'
df = df[df['type'] != 'mock_tweet']
print(f"\nDataFrame concatenado exitosamente con {len(df)} tweets")

if not dfs:
    print("❌ No se obtuvieron resultados de ningún país.")
    exit(0)

header = ['text', 'fecha', 'cuenta', 'seguidores', 'link','impresiones', 'interacciones', 'compartidos', 'likes', 'comentarios', 'retweets', 'citas', 'guardados']

# === Leer registros existentes en la hoja ===
result = sheet.values().get(
    spreadsheetId=SPREADSHEET_ID,
    range="TWEETS!A:M"
).execute()
values = result.get("values", [])

if values:
    existing_df = pd.DataFrame(values[1:], columns=values[0])
else:
    existing_df = pd.DataFrame(columns=header)

# === Concatenar y limpiar duplicados ===
combined_df = pd.concat([existing_df, df], ignore_index=True)
combined_df.drop_duplicates(subset=["link"], inplace=True)

# === Sobrescribir hoja con datos limpios ===
sheet.values().clear(
    spreadsheetId=SPREADSHEET_ID,
    range="Data!A:J"
).execute()

sheet.values().update(
    spreadsheetId=SPREADSHEET_ID,
    range="Data!A1",
    valueInputOption="RAW",
    body={"values": [header] + combined_df.astype(str).values.tolist()}
).execute()

print("✅ Hoja actualizada sin duplicados.")
