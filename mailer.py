import os
import json
import pandas as pd
from datetime import datetime, timedelta
import pytz
from googleapiclient.discovery import build
from google.oauth2 import service_account
import smtplib
from email.mime.text import MIMEText

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
        range="Data!A:J"
    ).execute()
    values = result.get("values", [])
    if not values:
        return pd.DataFrame()
    header = values[0]
    return pd.DataFrame(values[1:], columns=header)


def filter_by_window(df, now):
    # Parse y localiza scraped_at
    df["scraped_at_dt"] = pd.to_datetime(
        df["scraped_at"], format="%d/%m/%Y %H:%M"
    ).dt.tz_localize(TZ_ARG)

    # Ventana diaria fija: 09:00 del día anterior -> 09:00 de hoy (ART)
    start = (now - timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
    end = now.replace(hour=9, minute=0, second=0, microsecond=0)
    label = "09:00 (día anterior) - 09:00 (hoy)"

    return df[(df["scraped_at_dt"] >= start) & (df["scraped_at_dt"] < end)], label



# Diccionario de imágenes de país
COUNTRY_IMAGES = {
    "Argentina": "https://raw.githubusercontent.com/vickyarrudi/newsletter-banderas/main/ARG.png",
    "Chile": "https://raw.githubusercontent.com/vickyarrudi/newsletter-banderas/main/CHILE.png",
    "Peru": "https://raw.githubusercontent.com/vickyarrudi/newsletter-banderas/main/PERU.png"
}

def format_email_html(df, window_label):
    if df.empty:
        return f"<p>No news found for {window_label}.</p>"

    orderTags = ["proactivas", "grooming", "issues", "generales", "virales", "competencia"]

    df = df.copy()
    if "tag" not in df.columns:
        df["tag"] = "generales"
    df["tag_norm"] = df["tag"].fillna("generales").astype(str).str.strip().str.lower()

    # Normalizar sentiment
    if "sentiment" not in df.columns:
        df["sentiment"] = "NEUTRO"
    df["sentiment_norm"] = (
        df["sentiment"].fillna("NEUTRO").astype(str).str.strip().str.upper()
    )

    body = [
        "<div style='margin-bottom:20px; text-align:center;'>"
        "<img src='https://raw.githubusercontent.com/vickyarrudi/newsletter-banderas/main/cabezal.png' "
        "alt='Header' style='max-width:100%; height:auto;'>"
        "</div>",
        f"<h2 style='font-family:Arial,Helvetica,sans-serif; color:#333; "
        "border-bottom:2px solid #eee; padding-bottom:8px;'>"
        f"📰 News collected ({window_label})</h2>"
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

        # Render de una noticia (para no repetir)
        def render_card(row):
            sentiment_txt = row.get("sentiment_norm", "NEUTRO")
            return (
                "<div style='background:#fff; border:1px solid #ddd; border-radius:8px; "
                "padding:15px; margin-bottom:16px; box-shadow:0 2px 4px rgba(0,0,0,0.05);'>"
                f"<h3 style='margin:0; font-size:18px; color:#222; font-family:Arial,Helvetica,sans-serif'>{row['title']}</h3>"
                f"<p style='margin:0; font-size:12px; color:#777; font-family:Arial,Helvetica,sans-serif'>"
                f"<i>{row['date_utc']} - {row['domain']} - {sentiment_txt}</i></p>"
                f"<p style='margin:10px 0; font-size:14px; color:#333; line-height:1.4; font-family:Arial,Helvetica,sans-serif'>{row['snippet']}</p>"
                f"<a href='{row['link']}' target='_blank' "
                "style='display:inline-block; margin-top:5px; font-size:13px; color:#1a73e8; "
                "text-decoration:none; font-family:Arial,Helvetica,sans-serif'>🔗 Leer más</a>"
                "</div>"
            )

        # Tags conocidas en el orden pedido
        for t in orderTags:
            block = known[known["tag_norm"] == t]
            if block.empty:
                continue
            body.append(
                f"<h4 style='margin:10px 0 8px; font-family:Arial,Helvetica,sans-serif; "
                f"color:#222; text-transform:uppercase; letter-spacing:.5px;'>{t}</h4>"
            )
            for _, row in sort_news(block).iterrows():
                body.append(render_card(row))

        # Tags no listadas, al final
        if not unknown.empty:
            for t in sorted(unknown["tag_norm"].unique()):
                block = unknown[unknown["tag_norm"] == t]
                body.append(
                    f"<h4 style='margin:10px 0 8px; font-family:Arial,Helvetica,sans-serif; "
                    f"color:#222; text-transform:uppercase; letter-spacing:.5px;'>{t}</h4>"
                )
                for _, row in sort_news(block).iterrows():
                    body.append(render_card(row))

    return "\n".join(body)





def send_email(subject, body):
    """Envía el correo usando SMTP"""
    msg = MIMEText(body, "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = EMAIL_USER
    msg["To"] = ", ".join(RECIPIENTS)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, RECIPIENTS, msg.as_string())


# === Ejecución ===
if __name__ == "__main__":
    now = datetime.now(TZ_ARG)

    df = get_sheet_data()
    if df.empty:
        print("⚠️ No hay datos en la hoja.")
        exit(0)

    filtered, window_label = filter_by_window(df, now)
    if filtered.empty:
        print("⚠️ No hay noticias en esta ventana.")
        exit(0)

    body = format_email_html(filtered, window_label)
    subject = f"Newsletter TikTok ({window_label})"


    send_email(subject, body)
    print("✅ Email enviado correctamente.")
