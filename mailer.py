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
    # Parse and localize scraped_at
    df["scraped_at_dt"] = pd.to_datetime(
        df["scraped_at"], format="%d/%m/%Y %H:%M"
    ).dt.tz_localize(TZ_ARG)

    # Definir ventanas
    if 7 <= now.hour < 9:
        start = (now - timedelta(days=1)).replace(hour=18, minute=0, second=0, microsecond=0)
        end = now.replace(hour=8, minute=0, second=0, microsecond=0)
        label = "18:00 (previous day) - 08:00"
    elif 12 <= now.hour < 14:
        start = now.replace(hour=8, minute=0, second=0, microsecond=0)
        end = now.replace(hour=13, minute=0, second=0, microsecond=0)
        label = "08:00 - 13:00"
    elif 17 <= now.hour < 19:
        start = now.replace(hour=13, minute=0, second=0, microsecond=0)
        end = now.replace(hour=18, minute=0, second=0, microsecond=0)
        label = "13:00 - 18:00"
    else:
        return pd.DataFrame(), "Out of schedule"

    return df[(df["scraped_at_dt"] >= start) & (df["scraped_at_dt"] < end)], label



# Diccionario de banderas
FLAG_EMOJIS = {
    "Argentina": "🇦🇷",
    "Chile": "🇨🇱",
    "Peru": "🇵🇪"
}

def format_email_html(df, window_label):
    if df.empty:
        return f"<p>No news found for {window_label}.</p>"

    body = [f"<h2>News collected ({window_label})</h2>"]
    for country, group in df.groupby("country"):
        flag = FLAG_EMOJIS.get(country, "")
        body.append(f"<h2 style='margin-top:20px'>{flag} {country}</h2>")
        for _, row in group.iterrows():
            body.append(
                f"<div style='margin-bottom:20px;'>"
                f"<h3 style='margin:0; font-size:18px;'><b>{row['title']}</b></h3>"
                f"<p style='margin:0; font-size:13px; color:#555;'><i>{row['date_utc']} - {row['source']}</i></p>"
                f"<p style='margin:5px 0; font-size:15px;'>{row['snippet']}</p>"
                f"<a href='{row['link']}' target='_blank'>{row['link']}</a>"
                f"</div>"
            )
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
