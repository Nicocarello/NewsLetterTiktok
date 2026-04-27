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

# === CONFIG ===
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = "19IqmQBolSHFvXJN5zNSEmUXw9ivqaxzymXg62S6QhkU"
CONTAINER_WIDTH = "700px"
CONTAINER_WIDTH2 = "750px"

creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

EMAIL_USER = os.getenv("EMAIL_USER_TIKTOK")
EMAIL_PASS = os.getenv("EMAIL_PASS_TIKTOK")

TZ_ARG = pytz.timezone("America/Argentina/Buenos_Aires")

# === DATA ===
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
    rows = [(row + [""] * n_cols)[:n_cols] for row in rows]

    return pd.DataFrame(rows, columns=header)

# === HELPERS ===
def is_si_mask(series):
    s = series.fillna("").astype(str).str.strip().str.lower()

    def norm(x):
        return ''.join(c for c in unicodedata.normalize('NFKD', x) if not unicodedata.combining(c))

    return s.apply(norm) == "si"


def sentiment_badge(label):
    lab = (label or "").upper()
    if "POSITIVO" in lab:
        color = "#2e7d32"
    elif "NEGATIVO" in lab:
        color = "#c62828"
    else:
        color = "#616161"

    return f"<span style='background:{color};color:#fff;padding:2px 8px;border-radius:12px;font-size:12px;'>{lab}</span>"


def clean_value(val):
    if val is None or pd.isna(val):
        return ""
    return str(val).strip()

# === CARD ===
def render_card(row, tambien_en_html=""):
    title = clean_value(row.get("title"))
    snippet = clean_value(row.get("snippet"))
    source = clean_value(row.get("source") or row.get("domain"))
    tier = clean_value(row.get("tier"))
    link = clean_value(row.get("link"))
    tag = clean_value(row.get("tag"))
    sentiment = clean_value(row.get("sentiment"))

    return f"""
    <div style='background:#fff;border:1px solid #ddd;border-radius:8px;
    padding:15px;margin:15px auto;max-width:{CONTAINER_WIDTH};'>
        
        <span style='background:#ff2c55;color:#fff;padding:3px 8px;border-radius:5px;font-size:12px;'>{tag}</span>
        
        <h3><a href='{link}' style='color:#000;text-decoration:none;'>{title}</a></h3>
        
        <p>{snippet}</p>
        
        <p><b>Media:</b> {source} | <b>{tier}</b></p>
        
        <p><b>Sentiment:</b> {sentiment_badge(sentiment)}</p>
        
        <p><a href='{link}'>Leer nota →</a></p>

        {tambien_en_html}
    </div>
    """

# === FILTER ===
def filter_by_window(df, now):
    df["scraped_at_dt"] = pd.to_datetime(
        df["scraped_at"], format="%d/%m/%Y %H:%M", errors="coerce"
    ).dt.tz_localize(TZ_ARG)

    days_back = 3 if now.weekday() == 0 else 1

    start = (now - timedelta(days=days_back)).replace(hour=9, minute=0)
    end = now.replace(hour=9, minute=0)

    label = f"{start.strftime('%d/%m/%Y 09:00')} - {end.strftime('%d/%m/%Y 09:00')}"

    return df[(df["scraped_at_dt"] >= start) & (df["scraped_at_dt"] < end)], label

# === HTML ===
def format_email_html(df, window_label):

    if df.empty:
        return f"<p>No news found for {window_label}</p>"

    body = [f"<div style='background:#f5f5f5;padding:20px 0;'>"]

    # HEADER
    body.append(
        f"<div style='max-width:{CONTAINER_WIDTH2};margin:auto;'>"
        f"<img src='https://mcusercontent.com/624d462ddab9885481536fb77/images/f6eec52f-27c8-ee63-94dc-7a050407d770.png' style='width:100%;'>"
        "</div>"
    )

    if "tema" not in df.columns:
        df["tema"] = ""

    for country, df_country in df.groupby("country"):

        # HEADER PAÍS
        body.append(
            f"<div style='max-width:{CONTAINER_WIDTH2};margin:20px auto 10px auto;background:#000;padding:10px 0;text-align:center;'>"
            f"<span style='color:#fff;font-size:22px;font-weight:800;'>TikTok — {country}</span>"
            f"</div>"
        )

        df_country = df_country.copy()
        df_country["tema"] = df_country["tema"].fillna("").str.strip()

        con_tema = df_country[df_country["tema"] != ""]
        sin_tema = df_country[df_country["tema"] == ""]

        # AGRUPADOS
        for tema, grupo in con_tema.groupby("tema"):

            grupo = grupo.copy()
            grupo["prioridad_flag"] = grupo.get("prioridad", "").astype(str).str.strip() != ""
            grupo = grupo.sort_values(by="prioridad_flag", ascending=False)

            principal = grupo.iloc[0]
            secundarias = grupo.iloc[1:]

            tambien_en_html = ""

            if not secundarias.empty:
                sec = secundarias.copy()
                sec["tier"] = sec["tier"].fillna("").astype(str)

                tiers = {}

                for _, row_sec in sec.iterrows():
                    tier = clean_value(row_sec.get("tier")) or "Otros"
                    source = clean_value(row_sec.get("source") or row_sec.get("domain"))
                    link = clean_value(row_sec.get("link"))

                    if not source:
                        continue

                    if tier not in tiers:
                        tiers[tier] = []

                    tiers[tier].append({"source": source, "link": link})

                def tier_sort_key(t):
                    try:
                        return int(t.replace("Tier", "").strip())
                    except:
                        return 99

                tiers_sorted = sorted(tiers.items(), key=lambda x: tier_sort_key(x[0]))

                tambien_en_html = "<div style='margin-top:10px;font-size:12px;color:#444;'>"
                tambien_en_html += "<strong>También en:</strong><br>"

                for tier, items in tiers_sorted:
                    tambien_en_html += f"<strong>{tier}:</strong> "

                    links = []
                    for item in items[:3]:
                        if item["link"]:
                            links.append(f"<a href='{item['link']}' target='_blank' style='color:#1a73e8;text-decoration:none'>{item['source']}</a>")
                        else:
                            links.append(item["source"])

                    tambien_en_html += " | ".join(links)
                    tambien_en_html += "<br>"

                tambien_en_html += "</div>"

            body.append(render_card(principal, tambien_en_html))

        # INDIVIDUALES
        for _, row in sin_tema.iterrows():
            body.append(render_card(row))

    body.append("</div>")

    return "".join(body)

# === EMAIL ===
def send_email(subject, body):
    recipients = ["victoria.arrudi@publicalatam.com"]

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
