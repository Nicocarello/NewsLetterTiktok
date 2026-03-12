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
    result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="2026!A:M"
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
    if lab == "POSITIVO":
        color = "#2e7d32"
    elif lab == "POSITIVO (PROACTIVO)":
        color = "#2e7d32"
    elif lab == "NEGATIVO":
        color = "#c62828"
    elif lab == "NEUTRO":
        color = "#616161"
    return (
        f"<span style='display:inline-block;padding:2px 8px;border-radius:12px;"
        f"font-size:12px;color:#fff;background:{color};'>{lab or 'NEUTRO'}</span>"
    )


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
    if df.empty and (competencia_df is None or competencia_df.empty):
        return f"<p>No news found for {window_label}.</p>"

    orderTags = ["PROACTIVAS", "ISSUES", "GROOMING", "GENERALES", "VIRALES", "COMPETENCIA"]

    df = df.copy()
    if "tag" not in df.columns:
        df["tag"] = "generales"
    df["tag_norm"] = df["tag"].fillna("generales").astype(str).str.strip().str.upper()

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
    ]

    def sort_news(dfpart):
        sort_key = pd.to_datetime(dfpart.get("date_utc", pd.NaT), errors="coerce", utc=True)
        if "scraped_at_dt" in dfpart.columns:
            sort_key = sort_key.fillna(dfpart["scraped_at_dt"])
        return dfpart.assign(_k=sort_key).sort_values("_k", ascending=False)

    def render_card(row, show_sentiment=True):
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

        sentiment_block = ""
        if show_sentiment:
            raw_sentiment = clean_value(row.get("sentiment_norm") or row.get("sentiment") or "NEUTRO")
            sentiment_html = sentiment_badge(raw_sentiment)
            sentiment_block = (
                f"<div style='margin-bottom:4px;'>"
                f"<strong style='color:#000000'>Sentiment:</strong> {sentiment_html}"
                f"</div>"
            )

        return (
            f"<div style='background:#fff;border:1px solid #e0e0e0;border-radius:8px;"
            f"padding:15px;margin:0 auto 15px auto;width:65%;"
            f"box-shadow:0 1px 2px rgba(0,0,0,0.05);'>"
            f"<h3 style='margin:5px 0 12px;font-size:20px;font-weight:800;letter-spacing:-0.4px;color:#000000;font-family:Arial, Helvetica, sans-serif;line-height:1.15;'>"
            f"<a href='{link}' style='text-decoration:none;color:#000000;'>"
            f"{title}</a></h3>"
            f"<p style='margin:0 0 15px;font-size:12px;color:#000000;font-family:Helvetica,sans-serif;line-height:1.5'>{snippet}</p>"
            f"<div style='border-top:1px solid #f1f3f4;padding-top:12px;font-size:12px;color:#444;font-family:Helvetica,sans-serif;line-height:1.6;'>"
            f"<div style='margin-bottom:4px;'><strong style='color:#000000'>Media:</strong> <span style='color:#000000'>{source or '—'}</span></div>"
            f"<div style='margin-bottom:4px;'><strong style='color:#000000'>{tier or '—'}</strong></div>"
            f"{sentiment_block}"
            f"<div><strong style='color:#000000'>Artículo:</strong> <a href='{link}' target='_blank' style='color:#1a73e8;text-decoration:none;font-weight:bold'>Leer nota →</a></div>"
            f"</div></div>"
        )

    COUNTRY_EMOJIS = {
        "Argentina": "🇦🇷",
        "Chile": "🇨🇱",
        "Peru": "🇵🇪"
    }

    countries_order = ["Argentina", "Chile", "Peru"]

    for country in countries_order:
        emoji = COUNTRY_EMOJIS.get(country, "")

        inst_group = df[df.get("country") == country] if not df.empty else pd.DataFrame()
        if not inst_group.empty:
            body.append(
                f"<div style='width:70%;margin:20px auto 10px auto;background-color:#000000;padding:10px 0;text-align:center;'>"
                f"<span style='font-family:Arial, Helvetica, sans-serif;font-size:36px;font-weight:800;letter-spacing:-0.5px;'>"
                f"<span style='color:#FFFFFF;'>TikTok</span><span style='color:#00F2EA;'> / </span>"
                f"<span style='color:#fe2c55;'>Institutional — {country} {emoji}</span></span></div>"
            )

            for _, row in sort_news(inst_group).iterrows():
                body.append(render_card(row, show_sentiment=True))

        comp_group = (
            competencia_df[competencia_df.get("country") == country]
            if (competencia_df is not None and not competencia_df.empty)
            else pd.DataFrame()
        )

        if not comp_group.empty:
            body.append(
                f"<div style='width:70%;margin:20px auto 10px auto;background-color:#000000;padding:10px 0;text-align:center;'>"
                f"<span style='font-family:Arial, Helvetica, sans-serif;font-size:36px;font-weight:800;letter-spacing:-0.5px;'>"
                f"<span style='color:#FFFFFF;'>TikTok</span><span style='color:#00F2EA;'> / </span>"
                f"<span style='color:#fe2c55;'>Competencia — {country} {emoji}</span></span></div>"
            )

            comp_sorted_limited = sort_news(comp_group).head(3)

            for _, row in comp_sorted_limited.iterrows():
                body.append(render_card(row, show_sentiment=False))

    return "\n".join(body)


def send_email(subject, body):
    recipients = [r.strip() for r in RECIPIENTS if r.strip()]
    # recipients = ["nicolas.carello@publicalatam.com"]
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


if __name__ == "__main__":
    now = datetime.now(TZ_ARG)

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

    if "enviar" in filtered.columns:
        filtered = filtered[is_si_mask(filtered["enviar"])]

    if filtered.empty:
        print(f"⚠️ No hay noticias marcadas para enviar en la ventana {window_label}.")
        raise SystemExit(0)

    competencia_df = get_competencia_data()
    competencia_filtered = pd.DataFrame()

    if not competencia_df.empty:
        competencia_filtered, _ = filter_by_window(competencia_df, now)

        if "enviar" in competencia_filtered.columns:
            competencia_filtered = competencia_filtered[
                is_si_mask(competencia_filtered["enviar"])
            ]

    body = format_email_html(filtered, window_label, competencia_df=competencia_filtered)
    subject = f"Newsletter TikTok ({window_label})"

    send_email(subject, body)
    print("✅ Email enviado correctamente.")
