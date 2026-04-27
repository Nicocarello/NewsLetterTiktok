import os
import json
import pandas as pd
import unicodedata
from datetime import datetime, timedelta
import pytz
from googleapiclient.discovery import build
from google.oauth2 import service_account
import smtplib
from email.mime.text import MIMEText
import re

# === Configuración Google Sheets ===
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = "19IqmQBolSHFvXJN5zNSEmUXw9ivqaxzymXg62S6QhkU"

creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

# === Configuración Email ===
EMAIL_USER = os.getenv("EMAIL_USER_TIKTOK")
EMAIL_PASS = os.getenv("EMAIL_PASS_TIKTOK")
RECIPIENTS = os.getenv("EMAIL_TO", "").split(",")

TZ_ARG = pytz.timezone("America/Argentina/Buenos_Aires")

# === FUNCIONES ===

def get_sheet_data():
    result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="2026!A:P"
    ).execute()
    values = result.get("values", [])
    if not values:
        return pd.DataFrame()

    header = values[0]
    rows = values[1:]

    n_cols = len(header)
    normalized = [
        (row + [""] * n_cols)[:n_cols] if len(row) < n_cols else row[:n_cols]
        for row in rows
    ]

    return pd.DataFrame(normalized, columns=header)


def get_competencia_data():
    result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="Competencia!A:L"
    ).execute()
    values = result.get("values", [])
    if not values:
        return pd.DataFrame()

    header = values[0]
    rows = values[1:]

    n_cols = len(header)
    normalized = [
        (row + [""] * n_cols)[:n_cols] if len(row) < n_cols else row[:n_cols]
        for row in rows
    ]

    return pd.DataFrame(normalized, columns=header)


def is_si_mask(series):
    s = series.fillna("").astype(str).str.strip().str.lower()

    def _normalize(text):
        nfkd = unicodedata.normalize("NFKD", text)
        return "".join([c for c in nfkd if not unicodedata.combining(c)])

    return s.apply(_normalize) == "si"


def sentiment_badge(label: str) -> str:
    lab = (label or "").strip().upper()
    color = "#9e9e9e"
    if "POSITIVO" in lab:
        color = "#2e7d32"
    elif "NEGATIVO" in lab:
        color = "#c62828"
    elif "NEUTRO" in lab:
        color = "#616161"

    return f"<span style='padding:2px 8px;border-radius:12px;font-size:12px;color:#fff;background:{color};'>{lab}</span>"


def filter_by_window(df, now):
    df["scraped_at_dt"] = pd.to_datetime(
        df["scraped_at"], format="%d/%m/%Y %H:%M", errors="coerce"
    ).dt.tz_localize(TZ_ARG)

    weekday = now.weekday()
    days_back = 3 if weekday == 0 else 1

    start = (now - timedelta(days=days_back)).replace(hour=9, minute=0, second=0, microsecond=0)
    end = now.replace(hour=9, minute=0, second=0, microsecond=0)

    label = f"{start.strftime('%d/%m/%Y 09:00')} - {end.strftime('%d/%m/%Y 09:00')}"

    return df[(df["scraped_at_dt"] >= start) & (df["scraped_at_dt"] < end)], label


def clean_value(val):
    if val is None or pd.isna(val):
        return ""
    s_val = str(val).strip()
    if re.match(r'^\s*\{.+?\}\s*$', s_val):
        return ""
    return s_val


def format_email_html(df, window_label, competencia_df=None):
    if df.empty:
        return f"<p>No news found for {window_label}.</p>"

    body = []

    # HEADER
    body.append(
        "<div style='text-align:center;'>"
        "<img src='https://mcusercontent.com/624d462ddab9885481536fb77/images/f6eec52f-27c8-ee63-94dc-7a050407d770.png' style='max-width:70%;'>"
        "</div>"
    )

    def render_card(row):
        title = clean_value(row.get("title"))
        snippet = clean_value(row.get("snippet"))
        source = clean_value(row.get("source"))
        tier = clean_value(row.get("tier"))
        link = clean_value(row.get("link"))
        tag = clean_value(row.get("tag"))
        sentiment = clean_value(row.get("sentiment"))

        return f"""
        <div style='background:#fff;border:1px solid #ddd;border-radius:8px;padding:15px;margin:15px auto;width:65%;'>
            <span style='background:#ff2c55;color:#fff;padding:3px 8px;border-radius:5px;font-size:12px;'>{tag}</span>
            <h3><a href='{link}' style='color:#000;text-decoration:none;'>{title}</a></h3>
            <p>{snippet}</p>
            <p><b>Media:</b> {source}</p>
            <p><b>{tier}</b></p>
            <p><b>Sentiment:</b> {sentiment_badge(sentiment)}</p>
            <p><a href='{link}'>Leer nota →</a></p>
        </div>
        """

    # AGRUPAR POR PAÍS
    for country, df_country in df.groupby("country"):

        body.append(f"<h2 style='text-align:center;background:#000;color:#fff;padding:10px;'>TikTok — {country}</h2>")

        # AGRUPAR POR TEMA
        for tema, grupo in df_country.groupby("tema"):

            grupo = grupo.copy()

            grupo["prioridad_flag"] = grupo["prioridad"].fillna("") != ""

            grupo = grupo.sort_values(by=["prioridad_flag"], ascending=False)

            principal = grupo.iloc[0]
            secundarias = grupo.iloc[1:]

            # ⭐ PRINCIPAL
            body.append(render_card(principal))

            # 🟡 TAMBIÉN EN
            if not secundarias.empty:
                medios = secundarias["source"].dropna().unique()[:3]

                if len(medios) > 0:
                    body.append(
                        "<div style='width:65%;margin:-10px auto 20px;font-size:12px;'>"
                        "<b>También en:</b><br>" +
                        "<br>".join(medios) +
                        "</div>"
                    )

    return "".join(body)


def send_email(subject, body):
    #recipients = [r.strip() for r in RECIPIENTS if r.strip()]
    recipients += ["victoria.arrudi@publicalatam.com"]

    msg = MIMEText(body, "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = EMAIL_USER
    msg["To"] = ", ".join(recipients)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, recipients, msg.as_string())


# === MAIN ===
if __name__ == "__main__":
    now = datetime.now(TZ_ARG)

    df = get_sheet_data()
    filtered, window_label = filter_by_window(df, now)

    if "enviar" in filtered.columns:
        filtered = filtered[is_si_mask(filtered["enviar"])]

    body = format_email_html(filtered, window_label)

    subject = f"Newsletter TikTok ({window_label})"

    send_email(subject, body)

    print("✅ Newsletter enviada")
