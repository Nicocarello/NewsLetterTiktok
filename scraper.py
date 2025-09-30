from googleapiclient.discovery import build
from google.oauth2 import service_account
import pandas as pd
import os
from apify_client import ApifyClient
from datetime import datetime
import json
import pytz
import requests
from bs4 import BeautifulSoup
import re

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Google credentials desde secret
creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)

SPREADSHEET_ID = '1du5Cx3pK1LnxoVeBXTzP-nY-OSvflKXjJZw2Lq-AE14'

service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

# Apify token desde secret
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
apify_client = ApifyClient(APIFY_TOKEN)

# Actor de Google News
ACTOR_ID = "easyapi/google-news-scraper"

# Lista de países
COUNTRIES = ["ar", "cl", "pe"]
QUERIES = ["tik-tok", "tiktok", "tiktok suicidio", "tiktok grooming", "tiktok armas", "tiktok drogas", "tiktok violacion"]

# Definimos la zona horaria de Argentina
TZ_ARGENTINA = pytz.timezone("America/Argentina/Buenos_Aires")

# === Scraping con Apify ===
all_dfs = []
for query in QUERIES:
    for country in COUNTRIES:
        run_input = {
            "cr": country,
            "gl": country,
            "hl": "es-419",
            "lr": "lang_es",
            "maxItems": 5000,
            "query": query,
            "time_period": "last_hour",
        }
        print(f"[{datetime.now()}] Ejecutando {ACTOR_ID} para {country} con query '{query}'...")
        try:
            run = apify_client.actor(ACTOR_ID).call(run_input=run_input)
        except Exception as e:
            print(f"❌ Error al ejecutar actor para {country} con query '{query}': {e}")
            continue

        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            print(f"⚠️ No dataset generado para {country} - '{query}'")
            continue

        items = apify_client.dataset(dataset_id).list_items().items
        if not items:
            print(f"⚠️ No hay resultados para {country} - '{query}'")
            continue

        df = pd.DataFrame(items)
        df["country"] = country
        df["scraped_at"] = datetime.now(TZ_ARGENTINA).isoformat()
        all_dfs.append(df)

if not all_dfs:
    print("❌ No se obtuvieron resultados de ningún país.")
    exit(0)

# === DataFrame con lo nuevo ===
final_df = pd.concat(all_dfs, ignore_index=True)
final_df.drop_duplicates(subset=["link"], inplace=True)

# Convertir fechas
final_df['date_utc'] = pd.to_datetime(final_df['date_utc'], utc=True).dt.tz_convert(TZ_ARGENTINA)
final_df['date_utc'] = final_df['date_utc'].dt.strftime('%d/%m/%Y')

# Columnas adicionales
final_df['sentiment'] = ''
final_df['fecha_envio'] = ''
final_df['tag'] = ''
final_df['country'] = final_df['country'].replace({'ar': 'Argentina', 'cl': 'Chile', 'pe': 'Peru'})

# Formato scraped_at
final_df['scraped_at'] = pd.to_datetime(final_df['scraped_at'])
final_df['scraped_at'] = final_df['scraped_at'].dt.strftime('%d/%m/%Y %H:%M')

# Orden de columnas (Forma segura y robusta)
header = ['fecha_envio','date_utc','country','title','link','domain','snippet','tag','sentiment','scraped_at']

# Reindexamos el DataFrame. Esto asegura que todas las columnas del 'header' existan.
# Si una columna no existe en los datos originales, se creará y se rellenará con ''.
final_df = final_df.reindex(columns=header, fill_value='')

# === Leer registros existentes en la hoja ===
result = sheet.values().get(
    spreadsheetId=SPREADSHEET_ID,
    range="Data!A:J"
).execute()
values = result.get("values", [])

if values:
    existing_df = pd.DataFrame(values[1:], columns=values[0])
else:
    existing_df = pd.DataFrame(columns=header)

# === Concatenar y limpiar duplicados ===
combined_df = pd.concat([existing_df, final_df], ignore_index=True)
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
