# mailer.py
import os
import json
import pandas as pd
from datetime import datetime, timedelta
import pytz
from googleapiclient.discovery import build
from google.oauth2 import service_account
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr

# === Configuración Google Sheets ===
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# 👉 Ajustá estos dos si hace falta
SPREADSHEET_ID = "1DTMBII9byTfx9KU6M1QghhlU8abCRh8rKThcnaTbzpE"
SHEET_TAB = "NOTICIAS"   # el scraper escribe aquí

# Columnas esperadas por el scraper:
SCRAPER_HEADER = ["date_utc", "title", "link", "source", "snippet", "sentiment", "scraped_at"]

creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds, cache_discovery=False)
sheet = service.spreadsheets()

# === Configuración Email (Gmail SMTP) ===
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
RECIPIENTS = [e.strip() for e in os.getenv("EMAIL_TO_ELSZTAIN", "").split(",") if e.strip()]

# Zona horaria
TZ_ARG = pytz.timezone("America/Argentina/Buenos_Aires")

# ==== Helpers ====
def coalesce_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Asegura las columnas del scraper y mapea alias comunes."""
    cols = {c.lower(): c for c in df.columns}
    # alias domain -> source
    if "source" not in cols and "domain" in cols:
        df = df.rename(columns={cols["domain"]: "source"})
        cols = {c.lower(): c for c in df.columns}

    # agregar faltantes como vacío
    for col in SCRAPER_HEADER:
        if col not in df.columns:
            df[col] = ""

    # tipar como string para seguridad
    df = df.astype({c: str for c in SCRAPER_HEADER})
    # ordenar columnas como esperamos
    return df.reindex(columns=SCRAPER_HEADER)

def get_sheet_data() -> pd.DataFrame:
    """Descarga los datos de la hoja de Google Sheets."""
    result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_TAB}!A:G"
    ).execute()
    values = result.get("values", [])
    if not values:
        return pd.DataFrame(columns=SCRAPER_HEADER)

    header = values[0]
    df = pd.DataFrame(values[1:], columns=header)
    df = coalesce_columns(df)
    return df

def filter_by_window(df: pd.DataFrame, now: datetime) -> tuple[pd.DataFrame, str]:
    """Filtra por ventana basada en scraped_at (dd/mm/YYYY HH:MM, hora AR)."""
    if df.empty:
        return df, "Sin datos"

    # Parse de scraped_at (guardado en horario local AR por el scraper)
    dt = pd.to_datetime(df["scraped_at"], format="%d/%m/%Y %H:%M", errors="coerce")
    # localizamos como AR (naive -> TZ_ARG)
    df = df.copy()
    df["scraped_at_dt"] = dt.dt.tz_localize(TZ_ARG, nonexistent='NaT', ambiguous='NaT')

    # Ventanas
    if 7 <= now.hour < 9:
        start = (now - timedelta(days=1)).replace(hour=18, minute=0, second=0, microsecond=0)
        end   =  now.replace(hour=8, minute=0, second=0, microsecond=0)
        label = "18:00 (día previo) - 08:00"
    elif 12 <= now.hour < 14:
        start = now.replace(hour=8, minute=0, second=0, microsecond=0)
        end   = now.replace(hour=13, minute=0, second=0, microsecond=0)
        label = "08:00 - 13:00"
    elif 17 <= now.hour < 19:
        start = now.replace(hour=13, minute=0, second=0, microsecond=0)
        end   = now.replace(hour=18, minute=0, second=0, microsecond=0)
        label = "13:00 - 18:00"
    else:
        return pd.DataFrame(columns=SCRAPER_HEADER), "Fuera de ventana"

    mask = (df["scraped_at_dt"] >= start) & (df["scraped_at_dt"] < end)
    out = df.loc[mask].copy()

    # Orden sugerido: por source y luego por scraped_at descendente
    out = out.sort_values(["source", "scraped_at_dt"], ascending=[True, False])
    return out, label

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

def html_escape(s: str) -> str:
    # simplificado, evita romper HTML
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def format_email_html(df: pd.DataFrame, window_label: str) -> str:
    if df.empty:
        return f"<p>No se encontraron noticias para la ventana: <b>{html_escape(window_label)}</b>.</p>"

    body = [
        "<div style='margin-bottom:20px;'>"
        "<img src='https://raw.githubusercontent.com/vickyarrudi/newsletter-banderas/main/cabezal.png' "
        "alt='Header' style='max-width:100%; height:auto;'>"
        "</div>",
        f"<h2>Noticias recolectadas ({html_escape(window_label)})</h2>"
    ]

    # Agrupar por 'source' (dominio del medio)
    for source, group in df.groupby("source"):
        src = html_escape(source or "Fuente desconocida")
        body.append(
            f"<div style='margin-top:28px;margin-bottom:10px;border-bottom:1px solid #eee;'>"
            f"<h3 style='margin:0 0 10px 0;font-size:18px;'>{src}</h3>"
            f"</div>"
        )
        for _, row in group.iterrows():
            title = html_escape(row.get("title", ""))
            snippet = html_escape(row.get("snippet", ""))
            link = row.get("link", "")
            date_utc = html_escape(row.get("date_utc", ""))  # ya viene dd/mm/YYYY si seguiste el cambio
            badge = sentiment_badge(row.get("sentiment", ""))

            body.append(
                "<div style='margin:0 0 22px 0;'>"
                f"<div style='font-size:16px;line-height:1.3;margin:0 0 4px 0;'><b>{title}</b></div>"
                f"<div style='font-size:12px;color:#666;margin:0 0 6px 0;'>"
                f"{date_utc} · {badge}"
                "</div>"
                f"<div style='font-size:14px;color:#333;margin:0 0 6px 0;'>{snippet}</div>"
                f"<a href='{link}' target='_blank' style='font-size:13px;color:#1565c0;'>{html_escape(link)}</a>"
                "</div>"
            )

    return "\n".join(body)

def send_email(subject: str, body_html: str):
    """Envía el correo usando SMTP (Gmail)."""
    if not EMAIL_USER or not EMAIL_PASS or not RECIPIENTS:
        raise RuntimeError("Faltan EMAIL_USER, EMAIL_PASS o EMAIL_TO en variables de entorno.")

    msg = MIMEText(body_html, "html", "utf-8")
    msg["Subject"] = subject
    # Nombre legible opcional
    msg["From"] = formataddr(("Noticias", EMAIL_USER))
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
        raise SystemExit(0)

    filtered, window_label = filter_by_window(df, now)
    if filtered.empty:
        print(f"⚠️ No hay noticias en esta ventana ({window_label}).")
        raise SystemExit(0)

    body = format_email_html(filtered, window_label)
    subject = f"Newsletter Noticias ({window_label})"

    send_email(subject, body)
    print("✅ Email enviado correctamente.")
