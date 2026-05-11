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
SPREADSHEET_ID = "1du5Cx3pK1LnxoVeBXTzP-nY-OSvflKXjJZw2Lq-AE14"
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
    return get_data_from_range("2026!A:P")

def get_competencia_data():
    return get_data_from_range("Competencia!A:P")

def get_data_from_range(rng):
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=rng).execute()
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
def render_card(row, tambien_en_html="", mostrar_sentiment=True):
    title = clean_value(row.get("title"))
    snippet = clean_value(row.get("snippet"))
    source = clean_value(row.get("source") or row.get("domain"))
    tier = clean_value(row.get("tier"))
    link = clean_value(row.get("link"))
    tag = clean_value(row.get("tag")).upper()
    sentiment = clean_value(row.get("sentiment"))

    sentiment_html = ""
    if mostrar_sentiment:
        sentiment_html = f"<p><b>Sentiment:</b> {sentiment_badge(sentiment)}</p>"

    return f"""
    <div style='background:#fff;border:1px solid #ddd;border-radius:8px;
    padding:15px;margin:15px auto;max-width:{CONTAINER_WIDTH};'>
        
        <span style='background:#ff2c55;color:#fff;padding:4px 10px;border-radius:6px;font-size:12px;font-weight:700;letter-spacing:0.3px;'>{tag}</span>
        
        <h3 style='margin:5px 0 12px;font-size:20px;font-weight:800;line-height:1.2;'>
            <a href='{link}' style='color:#000;text-decoration:none;font-weight:800;'>{title}</a>
        </h3>
        
        <p>
            {snippet}
            <a href='{link}' style='color:#1a73e8;text-decoration:none;font-weight:500;margin-left:5px;'>Leer nota →</a>
        </p>

        <p><b>Media:</b> {source} | <b>{tier}</b></p>

        {sentiment_html}
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

COUNTRY_FLAGS = {
    "Argentina": "🇦🇷",
    "Chile": "🇨🇱",
    "Peru": "🇵🇪",
    "Perú": "🇵🇪"
}

# === HTML ===
def format_email_html(df, window_label, competencia_df=None):

    body = [f"<div style='background:#f5f5f5;padding:20px 0;'>"]

    body.append(
        f"<div style='max-width:{CONTAINER_WIDTH2};margin:auto;'>"
        f"<img src='https://mcusercontent.com/624d462ddab9885481536fb77/images/f6eec52f-27c8-ee63-94dc-7a050407d770.png' style='width:100%;'>"
        "</div>"
    )

    def render_block(dataframe, is_competencia=False):

        if dataframe.empty:
            return

        for country, df_country in dataframe.groupby("country"):

            flag = COUNTRY_FLAGS.get(str(country).strip(), "")

            label = f"{'Competencia' if is_competencia else 'TikTok'} — {country} {flag}"

            body.append(
                f"<div style='max-width:{CONTAINER_WIDTH2};margin:20px auto 10px auto;background:#000;padding:10px 0;text-align:center;'>"
                f"<span style='color:#fff;font-size:22px;font-weight:800;'>{label}</span>"
                f"</div>"
            )

            df_country = df_country.copy()

            def normalize_sentiment(val):
                s = clean_value(val).upper().strip()
                if "POSITIVO" in s and "PROACTIVO" in s:
                    return "POSITIVO (PROACTIVO)"
                if "POSITIVO" in s:
                    return "POSITIVO"
                if "NEGATIVO" in s:
                    return "NEGATIVO"
                return "NEUTRO"

            SENTIMENT_ORDER = {
                "POSITIVO (PROACTIVO)": 0,
                "POSITIVO": 1,
                "NEGATIVO": 2,
                "NEUTRO": 3,
            }

            if is_competencia:
                df_country["sentiment_norm"] = "NEUTRO"
            else:
                if "sentiment" not in df_country.columns:
                    df_country["sentiment"] = "NEUTRO"
                df_country["sentiment_norm"] = df_country["sentiment"].apply(normalize_sentiment)

            for sentiment_label in ["POSITIVO (PROACTIVO)", "POSITIVO", "NEGATIVO", "NEUTRO"]:

                df_sent = df_country[df_country["sentiment_norm"] == sentiment_label]

                con_tema = df_sent[df_sent["tema"].fillna("") != ""]
                sin_tema = df_sent[df_sent["tema"].fillna("") == ""]

                # === CON TEMA ===
                # Pre-calcular tier_order para poder ordenar grupos
                con_tema = con_tema.copy()
                con_tema["tier_order"] = (
                    con_tema["tier"]
                    .fillna("").astype(str)
                    .str.extract(r'(\d+)')[0]
                    .astype(float).fillna(99)
                )
                
                # Ordenar grupos de tema por el tier mínimo dentro de cada grupo
                tema_tier_order = con_tema.groupby("tema")["tier_order"].min().sort_values()
                
                for tema in tema_tier_order.index:
                    grupo = con_tema[con_tema["tema"] == tema].copy()
                
                    grupo["prioridad_flag"] = grupo["prioridad"].fillna("").astype(str).str.strip() != ""
                
                    # tier_order ya está calculado, no hace falta recalcular
                    grupo = grupo.sort_values(
                        by=["prioridad_flag", "tier_order"],
                        ascending=[False, True]
                    )
                   
                
                    principal = grupo.iloc[0]
                    secundarias = grupo.iloc[1:]

                    tambien_en_html = ""

                    if not secundarias.empty:
                        tiers = {}

                        for _, row_sec in secundarias.iterrows():
                            tier = clean_value(row_sec.get("tier"))
                            source = clean_value(row_sec.get("source"))
                            link = clean_value(row_sec.get("link"))
                            tiers.setdefault(tier, []).append((source, link))

                        tambien_en_html = "<div style='margin-top:10px;font-size:13px;color:#000;'>"
                        tambien_en_html += "<strong>También en:</strong><br>"

                        for tier, items in sorted(tiers.items()):
                            tambien_en_html += f"<strong>{tier}:</strong> "
                            tambien_en_html += " | ".join(
                                f"<a href='{l}' target='_blank'>{s}</a>" if l else s
                                for s, l in items[:3]
                            )
                            tambien_en_html += "<br>"

                        tambien_en_html += "</div>"

                    body.append(render_card(principal, tambien_en_html, mostrar_sentiment=not is_competencia))

                # === SIN TEMA ===
                sin_tema = sin_tema.copy()

                sin_tema["tier_order"] = (
                    sin_tema["tier"]
                    .fillna("")
                    .astype(str)
                    .str.extract(r'(\d+)')[0]
                    .astype(float)
                    .fillna(99)
                )

                sin_tema = sin_tema.sort_values(by="tier_order", ascending=True)

                for _, row in sin_tema.iterrows():
                    body.append(render_card(row, mostrar_sentiment=not is_competencia))

    render_block(df, is_competencia=False)
    if competencia_df is not None:
        render_block(competencia_df, is_competencia=True)

    body.append("</div>")
    return "".join(body)

# === EMAIL ===
def send_email(subject, body):
    #recipients = ["victoria.arrudi@publicalatam.com"]
    recipients = ["victoria.arrudi@publicalatam.com", "luz@publicalatam.com", "sofia.szekasy@publicalatam.com", "ezequiel@publicalatam.com", "matias@publicalatam.com", "sol.lopatin@publicalatam.com",
                 "bianca.rocatti@bytedance.com", "denise.estray@bytedance.com","german.nissen@bytedance.com", "german.nissen@tiktok.com", "hernan@quipuadvisors.com", "nadu.gonzalez@gmail.com",
                 "nicolas.sforzini@tiktok.com", "nicolas@quipuadvisors.com", "pri.pagliuso@bytedance.com", "tabakmansebastian@gmail.com", "seba.gombi@gmail.com", "germannissen@hotmail.com"]

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
    comp_df = get_competencia_data()

    filtered, window_label = filter_by_window(df, now)
    comp_filtered, _ = filter_by_window(comp_df, now)

    if "enviar" in filtered.columns:
        filtered = filtered[is_si_mask(filtered["enviar"])]

    if "enviar" in comp_filtered.columns:
        comp_filtered = comp_filtered[is_si_mask(comp_filtered["enviar"])]

    body = format_email_html(filtered, window_label, comp_filtered)

    subject = f"Newsletter TikTok ({window_label})"

    send_email(subject, body)

    print("✅ Newsletter enviada")

