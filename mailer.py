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
COUNTRY_IMAGES = {
    "Argentina": "https://raw.githubusercontent.com/vickyarrudi/newsletter-banderas/main/ARG.png",
    "Chile": "https://raw.githubusercontent.com/vickyarrudi/newsletter-banderas/main/CHILE.png",
    "Peru": "https://raw.githubusercontent.com/vickyarrudi/newsletter-banderas/main/PERU.png"
}

def clean_value(val):
    """Limpia valores nulos o placeholders."""
    if val is None or pd.isna(val):
        return ""
    s_val = str(val).strip()
    # Regex para detectar placeholders tipo {Title}
    if re.match(r'^\s*\{.+?\}\s*$', s_val):
        return ""
    return s_val

def format_email_html(df, window_label):
    if df.empty:
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
        "<div style='margin-bottom:20px; text-align:center;'>"
        # "<img src='https://raw.githubusercontent.com/vickyarrudi/newsletter-banderas/main/cabezal.png' "
        "<img src='https://mcusercontent.com/624d462ddab9885481536fb77/images/f6eec52f-27c8-ee63-94dc-7a050407d770.png' "
        "alt='Header' style='max-width:70%; height:auto;'>"
        "</div>"
    ]

    def sort_news(dfpart):
        sort_key = pd.to_datetime(dfpart.get("date_utc", pd.NaT), errors="coerce", utc=True)
        if "scraped_at_dt" in dfpart.columns:
            sort_key = sort_key.fillna(dfpart["scraped_at_dt"])
        return dfpart.assign(_k=sort_key).sort_values("_k", ascending=False)

    # País
    for country, group_country in df.groupby("country"):
        img_url = COUNTRY_IMAGES.get(country, "")
        if img_url:
            body.append(
                f"<div style='margin-top:30px; margin-bottom:15px;'>"
                f"<img src='{img_url}' alt='{country}' style='max-height:40px;'>"
                f"</div>"
            )
        else:
            body.append(
                f"<h3 style='margin-top:30px; color:#444; font-family:Arial,Helvetica,sans-serif'>{country}</h3>"
            )

        known = group_country[group_country["tag_norm"].isin(orderTags)]
        unknown = group_country[~group_country["tag_norm"].isin(orderTags)]

        # Render de una noticia
        def render_card(row):
            """Genera el HTML de una tarjeta con los datos apilados verticalmente."""
            
            # --- 1. Extracción de datos (Igual que antes) ---
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
        
            # --- 2. HTML con layout vertical ---
            # Usamos divs independientes para Media, Tier, Sentiment y Article
            return (
                f"<div style='background:#fff;border:1px solid #e0e0e0;border-radius:8px;"
                f"padding:15px;margin:0 auto 15px auto;width:65%;"
                f"box-shadow:0 1px 2px rgba(0,0,0,0.05);'>"
                f"{tag_html}"
                
                # Título
                f"<h3 style='margin:5px 0 10px;font-size:36px;font-weight:700;color:#202124;"
                f"font-family:Helvetica,sans-serif;line-height:1.3'>"
                f"<a href='{link}' style='text-decoration:none;color:#000000'>{title}</a></h3>"
                
                # Resumen
                f"<p style='margin:0 0 15px;font-size:18px;color:#000000;font-family:Arial,sans-serif;"
                f"line-height:1.5'>{snippet}</p>"
                
                # --- SECCIÓN METADATOS VERTICAL ---
                f"<div style='border-top:1px solid #f1f3f4;padding-top:12px;font-size:13px;color:#444;font-family:Arial,sans-serif;line-height:1.6;'>"
                
                # 1. Media
                f"<div style='margin-bottom:4px;'>"
                f"<strong style='color:#000000'>Media:</strong> {source or '—'}"
                f"</div>"
                
                # 2. Tier
                f"<div style='margin-bottom:4px;'>"
                f"<strong style='color:#000000'></strong> {tier or '—'}"
                f"</div>"
                
                # 3. Sentiment (Badge al lado del label)
                f"<div style='margin-bottom:4px;'>"
                f"<strong style='color:#000000'>Sentiment:</strong> {sentiment_badge}"
                f"</div>"
                
                # 4. Article (Link)
                f"<div>"
                f"<strong style='color:#000000'>Article:</strong> "
                f"<a href='{link}' target='_blank' style='color:#1a73e8;text-decoration:none;font-weight:bold'>Leer nota →</a>"
                f"</div>"
                
                f"</div>" # Cierre div metadatos
                f"</div>" # Cierre card
            )
        

        # Tags conocidas
        for t in orderTags:
            block = known[known["tag_norm"] == t]
            if block.empty:
                continue
            # body.append(
            #     f"<h4 style='margin:10px 0 8px; font-family:Arial,Helvetica,sans-serif; "
            #     f"color:#222; text-transform:uppercase; letter-spacing:.5px; "
            #     f"font-size:16px; font-weight:bold;'>{t}</h4>"
            # )
            for _, row in sort_news(block).iterrows():
                body.append(render_card(row))

        # Tags no listadas
        if not unknown.empty:
            for t in sorted(unknown["tag_norm"].unique()):
                block = unknown[unknown["tag_norm"] == t]
                # body.append(
                #     f"<h4 style='margin:10px 0 8px; font-family:Arial,Helvetica,sans-serif; "
                #     f"color:#222; text-transform:uppercase; letter-spacing:.5px; "
                #     f"font-size:16px; font-weight:bold;'>{t}</h4>"
                # )
                for _, row in sort_news(block).iterrows():
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

    body = format_email_html(filtered, window_label)
    subject = f"Newsletter TikTok ({window_label})"

    send_email(subject, body)
    print("✅ Email enviado correctamente.")
