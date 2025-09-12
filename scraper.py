import os
import pandas as pd
from apify_client import ApifyClient
from datetime import datetime

# Inicializa cliente con tu token
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
apify_client = ApifyClient(APIFY_TOKEN)

# Actor de Google News (definido como secret en GitHub Actions)
ACTOR_ID = os.getenv("ACTOR_ID", "easyapi/google-news-scraper")

# Lista de países
COUNTRIES = ["ar", "cl", "pe"]
QUERY = os.getenv("NEWS_QUERY", "tiktok")

CSV_FILE = "news_results.csv"

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
        df["scraped_at"] = datetime.now().isoformat()
        all_dfs.append(df)

    if not all_dfs:
        print("❌ No se obtuvieron resultados de ningún país.")
        return

    # ...
    final_df = pd.concat(all_dfs, ignore_index=True)
    
    # ✅ Evitar repetir noticias ya vistas
    final_df.drop_duplicates(subset=["link"], inplace=True)


    # Guardar CSV (append si existe)
    if os.path.exists(CSV_FILE):
        final_df.to_csv(CSV_FILE, mode="a", header=False, index=False)
    else:
        final_df.to_csv(CSV_FILE, index=False)

    print(f"✅ Datos guardados en {CSV_FILE}")


if __name__ == "__main__":
    run_scraper()
