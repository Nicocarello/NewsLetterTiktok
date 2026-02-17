import os
import json
import pandas as pd
from datetime import datetime, timedelta
import pytz
from googleapiclient.discovery import build
from google.oauth2 import service_account
import smtplib
from email.mime.text import MIMEText
import re

# === Configuración Google Sheets ===
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = "1du5Cx3pK1LnxoVeBXTzP-nY-OSvflKXjJZw2Lq-AE14"

creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

# === Configuración Email ===
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
RECIPIENTS = os.getenv("EMAIL_TO", "").split(",")

# Zona horaria
TZ_ARG = pytz.timezone("America/Argentina/Buenos_Aires")

# === Funciones ===
def get_sheet_data():
    """Descarga los datos de la hoja de Google Sheets"""
    result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="2026!A:L"
    ).execute()
    values = result.get("values", [])
    if not values:
        return pd.DataFrame()
    header = values[0]
    return pd.DataFrame(values[1:], columns=header)

def get_competencia_data():
    """Descarga los datos de la hoja 'Competencia'"""
    result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="Competencia!A:K"
    ).execute()
    values = result.get("values", [])
    if not values:
        return pd.DataFrame()
    header = values[0]
    return pd.DataFrame(values[1:], columns=header)


def sentiment_badge(label: str) -> str:
    lab = (label or "").strip().upper()
    color = "#9e9e9e"  # neutro default
    if lab == "POSITIVO":
        color = "#2e7d32"  # verde
    elif lab == "NEGATIVO":
        color = "#c62828"  # rojo
    elif lab == "NEUTRO":
        color = "#616161"  # gris
    return (
        f"<span style='display:inline-block;padding:2px 8px;border-radius:12px;"
        f"font-size:12px;color:#fff;background:{color};'>{lab or 'NEUTRO'}</span>"
    )

def filter_by_window(df, now):
    """
    - Lun 09:00: ventana desde Vie 09:00 -> Lun 09:00 (3 días hacia atrás)
    - Mar-Vie 09:00: ventana desde ayer 09:00 -> hoy 09:00 (1 día hacia atrás)
    - Sáb y Dom: no se envía (se corta en main)
    """
    # Parse y localiza scraped_at en ART
    df["scraped_at_dt"] = pd.to_datetime(
        df["scraped_at"], format="%d/%m/%Y %H:%M", errors="coerce"
    ).dt.tz_localize(TZ_ARG)

    weekday = now.weekday()  # Mon=0 ... Sun=6
    days_back = 3 if weekday == 0 else 1  # lunes 3, resto 1 (sábado/domingo no se ejecuta)

    start = (now - timedelta(days=days_back)).replace(hour=9, minute=0, second=0, microsecond=0)
    end = now.replace(hour=9, minute=0, second=0, microsecond=0)
    label = f"{start.strftime('%d/%m/%Y 09:00')} - {end.strftime('%d/%m/%Y 09:00')}"

    return df[(df["scraped_at_dt"] >= start) & (df["scraped_at_dt"] < end)], label

# Diccionario de imágenes de país
#COUNTRY_IMAGES = {
#    "Argentina": "https://raw.githubusercontent.com/vickyarrudi/newsletter-banderas/main/ARG.png",
#    "Chile": "https://raw.githubusercontent.com/vickyarrudi/newsletter-banderas/main/CHILE.png",
#    "Peru": "https://raw.githubusercontent.com/vickyarrudi/newsletter-banderas/main/PERU.png"
#}

def filter_tiktok_mentions(df):
    """
    Keep only rows where title (D) or snippet (H)
    contains 'tiktok' or its variations.
    """
    pattern = r"\btik[\s\-]?tok\w*\b"

    title_col = "D" if "D" in df.columns else "title"
    snippet_col = "H" if "H" in df.columns else "snippet"

    title_match = df[title_col].fillna("").str.contains(pattern, case=False, regex=True)
    snippet_match = df[snippet_col].fillna("").str.contains(pattern, case=False, regex=True)

    return df[title_match | snippet_match]



def clean_value(val):
    """Limpia valores nulos o placeholders."""
    if val is None or pd.isna(val):
        return ""
    s_val = str(val).strip()
    # Regex para detectar placeholders tipo {Title}
    if re.match(r'^\s*\{.+?\}\s*$', s_val):
        return ""
    return s_val

def format_email_html(df, window_label, competencia_df=None):
    if df.empty and (competencia_df is None or competencia_df.empty):
        return f"<p>No news found for {window_label}.</p>"

    orderTags = ["PROACTIVAS", "ISSUES", "GROOMING", "GENERALES", "VIRALES", "COMPETENCIA"]

    df = df.copy()
    if "tag" not in df.columns:
        df["tag"] = "generales"
    df["tag_norm"] = df["tag"].fillna("generales").astype(str).str.strip().str.upper()

    # Normalizar sentiment
    if "sentiment" not in df.columns:
        df["sentiment"] = "NEUTRO"
    df["sentiment_norm"] = (
        df["sentiment"].fillna("NEUTRO").astype(str).str.strip().str.upper()
    )

    body = [
        "<div style='margin-bottom:10px; text-align:center;'>"
        "<img src='https://mcusercontent.com/624d462ddab9885481536fb77/images/f6eec52f-27c8-ee63-94dc-7a050407d770.png' "
        "alt='Header' style='max-width:70%; height:auto;'>"
        "</div>",

        # "<div style='width:70%;"
        # "margin:0 auto 30px auto;"
        # "background-color:#000000;"
        # "padding:10px 0;"
        # "text-align:center;'>"
        # "<span style='font-family:Arial, Helvetica, sans-serif;"
        # "font-size:42px;"
        # "font-weight:800;"
        # "letter-spacing:-0.5px;'>"
        # "<span style='color:#FFFFFF;'>TikTok</span>"
        # "<span style='color:#00F2EA;'> / </span>"
        # "<span style='color:#fe2c55;'>Institutional</span>"
        # "</span>"
        # "</div>"
    ]

    def sort_news(dfpart):
        sort_key = pd.to_datetime(dfpart.get("date_utc", pd.NaT), errors="coerce", utc=True)
        if "scraped_at_dt" in dfpart.columns:
            sort_key = sort_key.fillna(dfpart["scraped_at_dt"])
        return dfpart.assign(_k=sort_key).sort_values("_k", ascending=False)

    # render_card (idéntica a la que ya tenías)
    def render_card(row):
        title = ""
        snippet = ""
        for col in ["title", "Title", "titulo", "headline", "D"]:
            cand = row.get(col)
            cleaned = clean_value(cand)
            if cleaned:
                title = cleaned
                break
        if not title and len(row) > 3:
            title = clean_value(row.iloc[3])

        for col in ["snippet", "Snippet", "resumen", "body", "H"]:
            cand = row.get(col)
            cleaned = clean_value(cand)
            if cleaned:
                snippet = cleaned
                break
        if not snippet and len(row) > 7:
            snippet = clean_value(row.iloc[7])

        source = clean_value(row.get("source") or row.get("domain") or row.get("G"))
        tier = clean_value(row.get("tier") or row.get("L"))
        link = clean_value(row.get("link") or row.get("url") or row.get("E"))

        raw_sentiment = clean_value(row.get("sentiment_norm") or row.get("sentiment") or "NEUTRO")
        sentiment_html = sentiment_badge(raw_sentiment)

        tag = clean_value(row.get("tag"))
        tag_html = ""
        if tag:
            tag_html = (
                f"<div style='display:inline-block;padding:3px 8px;border-radius:1px;"
                f"background:#fe3355;color:#fff;font-weight:bold;font-size:12px;margin-bottom:8px;"
                f"font-family:Helvetica,sans-serif;text-transform:uppercase'>{tag}</div>"
            )

        return (
            f"<div style='background:#fff;border:1px solid #e0e0e0;border-radius:8px;"
            f"padding:15px;margin:0 auto 15px auto;width:65%;"
            f"box-shadow:0 1px 2px rgba(0,0,0,0.05);'>"
            f"{tag_html}"
            f"<h3 style='margin:5px 0 12px;"
            f"font-size:20px;"
            f"font-weight:800;"
            f"letter-spacing:-0.4px;"
            f"color:#000000;"
            f"font-family:Arial, Helvetica, sans-serif;"
            f"line-height:1.15;'>"
            f"<a href='{link}' style='text-decoration:none;color:#000000;'>"
            f"{title}</a></h3>"
            f"<p style='margin:0 0 15px;font-size:12px;color:#000000;font-family:Helvetica,sans-serif;"
            f"line-height:1.5'>{snippet}</p>"
            f"<div style='border-top:1px solid #f1f3f4;padding-top:12px;font-size:12px;color:#444;font-family:Helvetica,sans-serif;line-height:1.6;'>"
            f"<div style='margin-bottom:4px;'>"
            f"<strong style='color:#000000'>Media:</strong> "
            f"<span style='color:#000000'>{source or '—'}</span>"
            f"</div>"
            f"<div style='margin-bottom:4px;'>"
            f"<strong style='color:#000000'>{tier or '—'}</strong>"
            f"</div>"
            f"<div style='margin-bottom:4px;'>"
            f"<strong style='color:#000000'>Sentiment:</strong> {sentiment_html}"
            f"</div>"
            f"<div>"
            f"<strong style='color:#000000'>Artículo:</strong> "
            f"<a href='{link}' target='_blank' style='color:#1a73e8;text-decoration:none;font-weight:bold'>Leer nota →</a>"
            f"</div>"
            f"</div>"
            f"</div>"
        )

    # País emojis
    COUNTRY_EMOJIS = {
        "Argentina": "🇦🇷",
        "Chile": "🇨🇱",
        "Peru": "🇵🇪"
    }

    # Desired country order
    countries_order = ["Argentina", "Chile", "Peru"]

    # --- Iterate by country and render Institutional then Competencia per country ---
    for country in countries_order:
        emoji = COUNTRY_EMOJIS.get(country, "")
        # --- Institutional section for this country (from 2026 sheet / df) ---
        inst_group = df[df.get("country") == country] if not df.empty else pd.DataFrame()
        if not inst_group.empty:
            body.append(
                "<div style='width:70%;"
                "margin:20px auto 10px auto;"
                "background-color:#000000;"
                "padding:10px 0;"
                "text-align:center;'>"
                "<span style='font-family:Arial, Helvetica, sans-serif;"
                "font-size:36px;"
                "font-weight:800;"
                "letter-spacing:-0.5px;'>"
                "<span style='color:#FFFFFF;'>TikTok</span>"
                "<span style='color:#00F2EA;'> / </span>"
                f"<span style='color:#fe2c55;'>Institutional — {country} {emoji}</span>"
                "</span>"
                "</div>"
            )
            # render known tags first
            known = inst_group[inst_group["tag_norm"].isin(orderTags)]
            unknown = inst_group[~inst_group["tag_norm"].isin(orderTags)]

            for t in orderTags:
                block = known[known["tag_norm"] == t]
                if block.empty:
                    continue
                for _, row in sort_news(block).iterrows():
                    body.append(render_card(row))

            if not unknown.empty:
                for t in sorted(unknown["tag_norm"].unique()):
                    block = unknown[unknown["tag_norm"] == t]
                    for _, row in sort_news(block).iterrows():
                        body.append(render_card(row))

        # --- SECCIÓN COMPETENCIA (Aquí aplicamos el límite de 3) ---
        comp_group = (
            competencia_df[competencia_df.get("country") == country]
            if (competencia_df is not None and not competencia_df.empty)
            else pd.DataFrame()
        )
        
        if not comp_group.empty:
            body.append(
                f"<div style='width:70%;margin:20px auto 10px auto;background-color:#000000;"
                f"padding:10px 0;text-align:center;'>"
                f"<span style='font-family:Arial, Helvetica, sans-serif;font-size:36px;font-weight:800;letter-spacing:-0.5px;'>"
                f"<span style='color:#FFFFFF;'>TikTok</span><span style='color:#00F2EA;'> / </span>"
                f"<span style='color:#fe2c55;'>Competencia — {country} {emoji}</span></span></div>"
            )
            
            # 1. Ordenamos las noticias por fecha (más recientes primero)
            # 2. Limitamos a las primeras 3 usando .head(3)
            comp_sorted_limited = sort_news(comp_group).head(3)
            
            for _, row in comp_sorted_limited.iterrows():
                body.append(render_card(row))

    return "\n".join(body)



def send_email(subject, body):
    """Envía el correo usando SMTP"""
    #recipients = [r.strip() for r in RECIPIENTS if r.strip()]
    recipients = ["nicolas.carello@publicalatam.com"]
    if not recipients:
        print("⚠️ No hay destinatarios en EMAIL_TO.")
        return

    msg = MIMEText(body, "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = EMAIL_USER
    msg["To"] = ", ".join(recipients)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, recipients, msg.as_string())
        

# === Ejecución ===
if __name__ == "__main__":
    now = datetime.now(TZ_ARG)

    # Sábado (5) o domingo (6): no se envía
    if now.weekday() in (5, 6):
        print("ℹ️ Fin de semana: no se envía newsletter.")
        raise SystemExit(0)

    df = get_sheet_data()
    if df.empty:
        print("⚠️ No hay datos en la hoja.")
        raise SystemExit(0)

    filtered, window_label = filter_by_window(df, now)
    if filtered.empty:
        print(f"⚠️ No hay noticias en la ventana {window_label}.")
        raise SystemExit(0)

    # 🔎 Filtrar solo noticias Tier 1 (como ya lo tenías)
    filtered = filtered[
        filtered["tier"].fillna("").str.strip().str.upper() == "TIER 1"
    ]

    if filtered.empty:
        print(f"⚠️ No hay noticias Tier 1 en la ventana {window_label}.")
        raise SystemExit(0)

    # 🔎 Filter only TikTok mentions in title or snippet
    filtered = filter_tiktok_mentions(filtered)


    # === Competencia ===
    competencia_df = get_competencia_data()
    competencia_filtered = pd.DataFrame()
    if not competencia_df.empty:
        competencia_filtered, _ = filter_by_window(competencia_df, now)
        # (opcional) si quisieras filtrar competencia por Tier 1 también:
        competencia_filtered = competencia_filtered[
        competencia_filtered["tier"].fillna("").str.strip().str.upper().str.contains("TIER 1")
        ]

    body = format_email_html(filtered, window_label, competencia_df=competencia_filtered)
    subject = f"Newsletter TikTok ({window_label})"

    send_email(subject, body)
    print("✅ Email enviado correctamente.")
