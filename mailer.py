import os
import json
import pandas as pd
from datetime import datetime, timedelta
import pytz
from googleapiclient.discovery import build
from google.oauth2 import service_account
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import traceback
import time
import re

# === Configuracion / constantes ===
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = "1du5Cx3pK1LnxoVeBXTzP-nY-OSvflKXjJZw2Lq-AE14"
TZ_ARG = pytz.timezone("America/Argentina/Buenos_Aires")

# === Util helpers ===
def require_env(varname):
    val = os.getenv(varname)
    if not val:
        raise SystemExit(f"❌ Missing required env var: {varname}")
    return val

def log(msg):
    now = datetime.now(TZ_ARG).isoformat()
    print(f"[{now}] {msg}")

def retry(func, attempts=3, backoff=2, *args, **kwargs):
    """
    Simple retry wrapper with exponential backoff.
    func: callable that will be invoked as func(*args, **kwargs)
    """
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exc = e
            if attempt == attempts:
                raise
            sleep_for = backoff ** (attempt - 1)
            log(f"Warning: attempt {attempt} failed with {e!r}, retrying in {sleep_for}s...")
            time.sleep(sleep_for)
    if last_exc:
        raise last_exc

def normalize_columns(df):
    """
    Rename common column variations to canonical names used in the script:
    - title, snippet, country, tier, scraped_at, sentiment, tag, date_utc, link, source
    """
    df = df.copy()
    cols = list(df.columns)
    col_map = {}
    for c in cols:
        lc = c.strip().lower()
        if lc in ("d", "title", "titulo", "headline"):
            col_map[c] = "title"
        elif lc in ("h", "snippet", "resumen", "body"):
            col_map[c] = "snippet"
        elif lc in ("pais", "country"):
            col_map[c] = "country"
        elif lc == "tier":
            col_map[c] = "tier"
        elif lc == "scraped_at":
            col_map[c] = "scraped_at"
        elif lc == "sentiment":
            col_map[c] = "sentiment"
        elif lc == "tag":
            col_map[c] = "tag"
        elif lc in ("date_utc", "dateutc"):
            col_map[c] = "date_utc"
        elif lc in ("link", "url", "e"):
            col_map[c] = "link"
        elif lc in ("source", "domain", "g"):
            col_map[c] = "source"
    if col_map:
        df = df.rename(columns=col_map)
    # Trim column names
    df.columns = [c.strip() for c in df.columns]
    return df

# === Env / creds validation and service creation ===
# require critical env vars early
GOOGLE_CREDENTIALS = require_env("GOOGLE_CREDENTIALS")
EMAIL_USER = require_env("EMAIL_USER")
EMAIL_PASS = require_env("EMAIL_PASS")
# EMAIL_TO optional; if not provided and DRY_RUN not set, script will error later
RECIPIENTS = os.getenv("EMAIL_TO", "").split(",")
# Optionally override recipients for local testing (kept optional)
TEST_RECIPIENT = os.getenv("TEST_RECIPIENT")  # e.g. "you@example.com"
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"

# Build Sheets service (safe to do after reading creds)
creds_dict = json.loads(GOOGLE_CREDENTIALS)
creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

# === Data access functions with retry ===
def sheet_get_with_retry(spreadsheet_id, range_):
    def call():
        return sheet.values().get(spreadsheetId=spreadsheet_id, range=range_).execute()
    return retry(call, attempts=3, backoff=2)

def get_sheet_data():
    """Descarga los datos de la hoja de Google Sheets (2026!A:L)"""
    result = sheet_get_with_retry(SPREADSHEET_ID, "2026!A:L")
    values = result.get("values", [])
    if not values:
        return pd.DataFrame()
    header = values[0]
    df = pd.DataFrame(values[1:], columns=header)
    return normalize_columns(df)

def get_competencia_data():
    """Descarga los datos de la hoja 'Competencia'"""
    result = sheet_get_with_retry(SPREADSHEET_ID, "Competencia!A:K")
    values = result.get("values", [])
    if not values:
        return pd.DataFrame()
    header = values[0]
    df = pd.DataFrame(values[1:], columns=header)
    return normalize_columns(df)

# === Existing helpers (slightly hardened) ===
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

def clean_value(val):
    """Limpia valores nulos o placeholders."""
    if val is None or pd.isna(val):
        return ""
    s_val = str(val).strip()
    # Regex para detectar placeholders tipo {Title}
    if re.match(r'^\s*\{.+?\}\s*$', s_val):
        return ""
    return s_val

def filter_by_window(df, now):
    """
    - Lun 09:00: ventana desde Vie 09:00 -> Lun 09:00 (3 días hacia atrás)
    - Mar-Vie 09:00: ventana desde ayer 09:00 -> hoy 09:00 (1 día hacia atrás)
    - Sáb y Dom: no se envía (se corta en main)
    """
    if df.empty:
        return df, ""
    df = df.copy()
    # Parse and localize scraped_at; coerce errors
    parsed = pd.to_datetime(df.get("scraped_at"), format="%d/%m/%Y %H:%M", errors="coerce")
    failed = parsed.isna().sum()
    if failed:
        log(f"Warning: {failed} rows have invalid scraped_at and will be excluded by window filter.")
    parsed = parsed  # naive timestamps (no tz)
    # attach tz only to non-na entries
    parsed_non_na = parsed[~parsed.isna()].dt.tz_localize(TZ_ARG)
    # create a tz-aware series aligned to df index
    scraped_at_dt = pd.Series([pd.NaT] * len(parsed), index=df.index, dtype="datetime64[ns, UTC]")
    scraped_at_dt.loc[parsed_non_na.index] = parsed_non_na
    df["scraped_at_dt"] = scraped_at_dt

    weekday = now.weekday()  # Mon=0 ... Sun=6
    days_back = 3 if weekday == 0 else 1
    start = (now - timedelta(days=days_back)).replace(hour=9, minute=0, second=0, microsecond=0)
    end = now.replace(hour=9, minute=0, second=0, microsecond=0)
    label = f"{start.strftime('%d/%m/%Y 09:00')} - {end.strftime('%d/%m/%Y 09:00')}"

    # keep tz-aware comparison (both start/end are tz-aware since now is tz-aware)
    mask = (df["scraped_at_dt"] >= start) & (df["scraped_at_dt"] < end)
    return df[mask], label

def filter_tiktok_mentions(df):
    """
    Keep only rows where title or snippet contains 'tiktok' or its variations.
    Uses canonical column names 'title' and 'snippet'.
    """
    if df.empty:
        return df
    df = df.copy()
    # columns may be missing; safely handle
    title_col = "title" if "title" in df.columns else None
    snippet_col = "snippet" if "snippet" in df.columns else None
    pattern = r"\btik[\s\-]?tok\w*\b"
    title_match = pd.Series(False, index=df.index)
    snippet_match = pd.Series(False, index=df.index)
    if title_col:
        title_match = df[title_col].astype(str).str.contains(pattern, case=False, regex=True, na=False)
    if snippet_col:
        snippet_match = df[snippet_col].astype(str).str.contains(pattern, case=False, regex=True, na=False)
    return df[title_match | snippet_match]

# === Email HTML formatting (drop-in with earlier custom order) ===
def format_email_html(df, window_label, competencia_df=None):
    if (df is None or df.empty) and (competencia_df is None or competencia_df.empty):
        return f"<p>No news found for {window_label}.</p>"

    orderTags = ["PROACTIVAS", "ISSUES", "GROOMING", "GENERALES", "VIRALES", "COMPETENCIA"]

    df = (df.copy() if df is not None else pd.DataFrame())
    if "tag" not in df.columns:
        df["tag"] = "generales"
    df["tag_norm"] = df["tag"].fillna("generales").astype(str).str.strip().str.upper()

    if "sentiment" not in df.columns:
        df["sentiment"] = "NEUTRO"
    df["sentiment_norm"] = df["sentiment"].fillna("NEUTRO").astype(str).str.strip().str.upper()

    body = [
        "<div style='margin-bottom:10px; text-align:center;'>"
        "<img src='https://mcusercontent.com/624d462ddab9885481536fb77/images/f6eec52f-27c8-ee63-94dc-7a050407d770.png' "
        "alt='Header' style='max-width:70%; height:auto;'>"
        "</div>"
    ]

    def sort_news(dfpart):
        sort_key = pd.to_datetime(dfpart.get("date_utc", pd.NaT), errors="coerce", utc=True)
        if "scraped_at_dt" in dfpart.columns:
            sort_key = sort_key.fillna(dfpart["scraped_at_dt"])
        return dfpart.assign(_k=sort_key).sort_values("_k", ascending=False)

    def render_card(row):
        # row may be a Series or dict-like; use .get where possible
        title = ""
        snippet = ""
        for col in ["title", "Title", "titulo", "headline", "D"]:
            cand = row.get(col) if hasattr(row, "get") else (row[col] if col in row.index else None)
            cleaned = clean_value(cand)
            if cleaned:
                title = cleaned
                break
        if not title and len(row) > 3:
            title = clean_value(row.iloc[3])

        for col in ["snippet", "Snippet", "resumen", "body", "H"]:
            cand = row.get(col) if hasattr(row, "get") else (row[col] if col in row.index else None)
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

    countries_order = ["Argentina", "Chile", "Peru"]

    # Ensure competencia_df is a DataFrame
    competencia_df = (competencia_df.copy() if competencia_df is not None else pd.DataFrame())
    if not competencia_df.empty and "tag" not in competencia_df.columns:
        competencia_df["tag"] = "generales"
    if not competencia_df.empty:
        competencia_df["tag_norm"] = competencia_df["tag"].fillna("generales").astype(str).str.strip().str.upper()

    # Normalize country columns for matching (title-case)
    if "country" in df.columns:
        df["country_norm"] = df["country"].fillna("").astype(str).str.strip().str.title()
    else:
        df["country_norm"] = ""

    if "country" in competencia_df.columns:
        competencia_df["country_norm"] = competencia_df["country"].fillna("").astype(str).str.strip().str.title()
    else:
        competencia_df["country_norm"] = ""

    # Render per country: Institutional then Competencia
    for country in countries_order:
        emoji = COUNTRY_EMOJIS.get(country, "")
        inst_group = df[df["country_norm"] == country] if not df.empty else pd.DataFrame()
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

        # Competencia
        comp_group = competencia_df[competencia_df["country_norm"] == country] if not competencia_df.empty else pd.DataFrame()
        if not comp_group.empty:
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
                f"<span style='color:#fe2c55;'>Competencia — {country} {emoji}</span>"
                "</span>"
                "</div>"
            )
            for _, row in sort_news(comp_group).iterrows():
                body.append(render_card(row))

    # If nothing was appended (edge-case), show no-news message
    if len(body) == 0:
        return f"<p>No news found for {window_label}.</p>"

    return "\n".join(body)

# === Email sending with retry and multipart ===
def send_email(subject, html_body, plain_text=None):
    recipients = [r.strip() for r in RECIPIENTS if r.strip()]
    # apply test recipient override if set (useful for QA)
    if TEST_RECIPIENT:
        recipients = [TEST_RECIPIENT]

    if not recipients:
        raise SystemExit("⚠️ No recipients configured in EMAIL_TO or TEST_RECIPIENT.")

    # create multipart alternative
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = EMAIL_USER
    msg["To"] = ", ".join(recipients)

    if plain_text is None:
        # crude plain-text fallback by stripping tags (simple)
        plain_text = re.sub(r"<[^>]+>", "", html_body)
    msg.attach(MIMEText(plain_text, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    def smtp_send():
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_USER, EMAIL_PASS)
            server.sendmail(EMAIL_USER, recipients, msg.as_string())

    retry(smtp_send, attempts=3, backoff=2)
    log(f"Email sent to: {recipients}")

# === Main execution flow ===
if __name__ == "__main__":
    try:
        now = datetime.now(TZ_ARG)
        log("Starting newsletter job")

        # Weekend skip
        if now.weekday() in (5, 6):
            log("ℹ️ Weekend: no newsletter sent.")
            raise SystemExit(0)

        df = get_sheet_data()
        if df.empty:
            log("⚠️ No data in 2026 sheet.")
            raise SystemExit(0)

        filtered, window_label = filter_by_window(df, now)
        if filtered.empty:
            log(f"⚠️ No news in window {window_label}.")
            raise SystemExit(0)

        # Tier 1 filter (case-insensitive)
        filtered = filtered[filtered.get("tier", "").fillna("").astype(str).str.strip().str.upper() == "TIER 1"]
        if filtered.empty:
            log(f"⚠️ No Tier 1 news in window {window_label}.")
            raise SystemExit(0)

        # TikTok mentions filter (title/snippet)
        filtered = filter_tiktok_mentions(filtered)
        if filtered.empty:
            log(f"⚠️ No TikTok mentions among Tier 1 news in window {window_label}.")
            raise SystemExit(0)

        # Competencia flow
        competencia_df = get_competencia_data()
        competencia_filtered = pd.DataFrame()
        if not competencia_df.empty:
            competencia_filtered, _ = filter_by_window(competencia_df, now)
            competencia_filtered = competencia_filtered[
                competencia_filtered.get("tier", "").fillna("").astype(str).str.strip().str.upper().str.contains("TIER 1")
            ]

        body = format_email_html(filtered, window_label, competencia_df=competencia_filtered)
        subject = f"Newsletter TikTok ({window_label})"

        # Dry-run option: write preview HTML locally and skip sending
        if DRY_RUN:
            preview_path = "/tmp/newsletter_preview.html"
            with open(preview_path, "w", encoding="utf-8") as f:
                f.write(body)
            log(f"DRY_RUN=1: preview written to {preview_path}. Skipping send.")
            raise SystemExit(0)

        send_email(subject, body)
        log("✅ Email enviado correctamente.")
    except SystemExit as se:
        # allow SystemExit to behave normally but log it
        log(f"Exit: {se}")
    except Exception as e:
        log(f"Unhandled error: {e}")
        traceback.print_exc()
        raise
