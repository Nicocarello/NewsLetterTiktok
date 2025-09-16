from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import pandas as pd
import os
from apify_client import ApifyClient
from datetime import datetime, timedelta
import json
import pytz # <-- Importamos la nueva biblioteca

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# Google credentials desde secret
creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)

SPREADSHEET_ID = '1du5Cx3pK1LnxoVeBXTzP-nY-OSvflKXjJZw2Lq-AE14'


service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()
result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range="Data!A:J").execute()
values = result.get('values', [])

# Apify token desde secret
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
apify_client = ApifyClient(APIFY_TOKEN)

# Actor de Google News (definido como secret en GitHub Actions)
ACTOR_ID = "easyapi/google-news-scraper"

# Lista de países
COUNTRIES = ["ar", "cl", "pe"]
QUERY = "tiktok"


# Definimos la zona horaria de Argentina
TZ_ARGENTINA = pytz.timezone("America/Argentina/Buenos_Aires")



all_dfs = []
for country in COUNTRIES:
    run_input = {
        "cr": country,
        "gl": country,
        "hl": "es-419",
        "lr": "lang_es",
        "maxItems": 5000,
        "query": QUERY,
        "time_period": "last_hour",
    }
    print(f"[{datetime.now()}] Ejecutando {ACTOR_ID} para {country}...")
    run = apify_client.actor(ACTOR_ID).call(run_input=run_input)
    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        print(f"⚠️ No dataset generado para {country}")
        continue
    items = apify_client.dataset(dataset_id).list_items().items
    if not items:
        print(f"⚠️ No hay resultados para {country}")
        continue
    df = pd.DataFrame(items)
    df["country"] = country
    df["scraped_at"] = datetime.now(TZ_ARGENTINA).isoformat()
    all_dfs.append(df)
if not all_dfs:
    print("❌ No se obtuvieron resultados de ningún país.")
    
# DataFrame con lo nuevo
final_df = pd.concat(all_dfs, ignore_index=True)
final_df.drop_duplicates(subset=["link"], inplace=True)


# Convert date_utc to ART timezone
final_df['date_utc'] = pd.to_datetime(final_df['date_utc'], utc=True).dt.tz_convert(TZ_ARGENTINA)


# 'date_utc' to format dd/mm/yyyy
final_df['date_utc'] = final_df['date_utc'].dt.strftime('%d/%m/%Y')
# Prepare data to append (convert DataFrame to list of lists, matching Google Sheets columns)
final_df['sentiment'] = ''
final_df['fecha_envio'] = ''
final_df['tag'] = ''
final_df['country'] = final_df['country'].replace({'ar': 'Argentina', 'cl': 'Chile', 'pe': 'Peru'})

#scraped_at to dd/mm/yyyy hh
final_df['scraped_at'] = pd.to_datetime(final_df['scraped_at'])
final_df['scraped_at'] = final_df['scraped_at'].dt.strftime('%d/%m/%Y %H:%M')

desired_columns = final_df[['fecha_envio', 'date_utc', 'country','title','link','source','snippet','tag','sentiment','scraped_at']].astype(str).values.tolist()
rows_to_append = [row for row in desired_columns if row not in values]

# Append rows to Google Sheet
sheet.values().append(
    spreadsheetId=SPREADSHEET_ID,
    range="Data!A1",
    valueInputOption="RAW",
    insertDataOption="INSERT_ROWS",
    body={"values": rows_to_append}
).execute()

