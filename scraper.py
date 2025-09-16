import os
import pandas as pd
from apify_client import ApifyClient
from datetime import datetime, timedelta
import pytz # <-- Importamos la nueva biblioteca

# Inicializa cliente con tu token
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
apify_client = ApifyClient(APIFY_TOKEN)

# Actor de Google News (definido como secret en GitHub Actions)
ACTOR_ID = os.getenv("ACTOR_ID", "easyapi/google-news-scraper")

# Lista de países
COUNTRIES = ["ar", "cl", "pe"]
QUERY = os.getenv("NEWS_QUERY", "tiktok")

CSV_FILE = "news_results.csv"

# Parámetros de limpieza
DAYS_TO_KEEP = 7
MAX_ROWS = 10000

# Definimos la zona horaria de Argentina
TZ_ARGENTINA = pytz.timezone("America/Argentina/Buenos_Aires")

def run_scraper():
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
        return

    # DataFrame con lo nuevo
    final_df = pd.concat(all_dfs, ignore_index=True)
    final_df.drop_duplicates(subset=["link"], inplace=True)

    # Si ya existe el CSV, concatenar con lo previo
    if os.path.exists(CSV_FILE):
        existing_df = pd.read_csv(CSV_FILE)
        combined_df = pd.concat([existing_df, final_df], ignore_index=True)
    else:
        combined_df = final_df

    # Convertir scraped_at a datetime para filtrar
    combined_df["scraped_at"] = pd.to_datetime(combined_df["scraped_at"], errors="coerce")

    # AHORA convertimos todas las fechas a la zona horaria de Argentina
    # Esto soluciona el error de comparación
    combined_df["scraped_at"] = combined_df["scraped_at"].dt.tz_convert(TZ_ARGENTINA)

    # ✅ Mantener solo los últimos N días
    cutoff = datetime.now(TZ_ARGENTINA) - timedelta(days=DAYS_TO_KEEP)
    combined_df = combined_df[combined_df["scraped_at"] >= cutoff]

    # ✅ Mantener un máximo de filas (las más recientes)
    if len(combined_df) > MAX_ROWS:
        combined_df = combined_df.sort_values("scraped_at").tail(MAX_ROWS)

    # Guardar limpio
    combined_df.to_csv(CSV_FILE, index=False)

    print(f"✅ Datos guardados en {CSV_FILE} ({len(combined_df)} filas después de limpieza)")

if __name__ == "__main__":
    run_scraper()
