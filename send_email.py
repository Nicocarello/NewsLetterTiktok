import os
import smtplib
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo  # Python 3.9+

CSV_FILE = "news_results.csv"

# Configuración de correo (mejor ponerlas como secrets en GitHub Actions)
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = os.getenv("EMAIL_USER")      # tu correo remitente
EMAIL_PASS = os.getenv("EMAIL_PASS")      # tu password o app password
EMAIL_TO = os.getenv("EMAIL_TO")          # destinatario(s), separados por coma
NEWS_QUERY = os.getenv("NEWS_QUERY", "tiktok")

# Zona horaria de Argentina
ART = ZoneInfo("America/Argentina/Buenos_Aires")

# Horas de corte locales (ART)
CUTS_LOCAL = [time(8,0), time(12,0), time(15,0), time(18,0), time(20,0)]


def current_window_utc():
    """
    Devuelve (start_utc, end_utc) para la ventana vigente según horarios ART.
    Reglas:
      - 08:00 → desde ayer 20:00 hasta hoy 08:00
      - 12:00 → 08:00–12:00
      - 15:00 → 12:00–15:00
      - 18:00 → 15:00–18:00
      - 20:00 → 18:00–20:00
    Si se corre antes de 08:00, toma la ventana de 20:00 (día anterior) a 08:00 (día actual).
    """
    now_local = datetime.now(ART)
    today_local = now_local.date()

    # último corte alcanzado hoy
    current_cut_local = None
    for t in reversed(CUTS_LOCAL):
        cut_dt = datetime.combine(today_local, t, tzinfo=ART)
        if now_local >= cut_dt:
            current_cut_local = cut_dt
            break

    if current_cut_local is None:
        # antes de las 08:00 ART → ayer 20:00 a hoy 08:00
        start_local = datetime.combine(today_local - timedelta(days=1), time(20,0), tzinfo=ART)
        end_local   = datetime.combine(today_local,                 time(8,0),  tzinfo=ART)
    else:
        idx = CUTS_LOCAL.index(current_cut_local.timetz())
        if idx == 0:
            # 08:00 ART → desde ayer 20:00
            start_local = datetime.combine(today_local - timedelta(days=1), time(20,0), tzinfo=ART)
        else:
            start_local = datetime.combine(today_local, CUTS_LOCAL[idx - 1], tzinfo=ART)
        end_local = current_cut_local

    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def safe_get(row, *cols, default=""):
    for c in cols:
        if c in row and pd.notna(row[c]):
            return str(row[c])
    return default


def format_news(df):
    """Convierte noticias en bloques HTML formateados (por país)."""
    html_content = ""
    for _, row in df.iterrows():
        title   = safe_get(row, "title", "headline")
        dateutc = safe_get(row, "date_utc", "publishedAt", "publicationDate")
        source  = safe_get(row, "source", "publisher", "site")
        snippet = safe_get(row, "snippet", "content", "text")
        link    = safe_get(row, "link", "url")

        html_content += "\n".join([
            "<p>",
            f"  <b>{title}</b><br>",
            f"  <i>{dateutc} - {source}</i><br>",
            f"  {snippet}<br>",
            f'  <a href="{link}">{link}</a>',
            "</p>",
            "<hr>"
        ])

    return html_content


def send_email():
    # Validaciones iniciales
    if not os.path.exists(CSV_FILE):
        print("⚠️ No existe el archivo de noticias.")
        return

    to_list = [e.strip() for e in (EMAIL_TO or "").split(",") if e.strip()]
    if not to_list:
        print("⚠️ EMAIL_TO vacío. No se envía correo.")
        return

    # Cargar CSV
    df = pd.read_csv(CSV_FILE)

    # Evitar duplicados por link (si existieran)
    if "link" in df.columns:
        df.drop_duplicates(subset=["link"], inplace=True)

    # Filtrar por ventana temporal usando scraped_at
    if "scraped_at" in df.columns:
        df["scraped_at"] = pd.to_datetime(df["scraped_at"], utc=True, errors="coerce")
        start_utc, end_utc = current_window_utc()
        df = df[(df["scraped_at"] >= start_utc) & (df["scraped_at"] < end_utc)].copy()
        window_label = f"{start_utc.astimezone(ART).strftime('%Y-%m-%d %H:%M')}–{end_utc.astimezone(ART).strftime('%Y-%m-%d %H:%M')}"
    else:
        print("⚠️ No existe la columna 'scraped_at'; se enviarán todas las filas.")
        window_label = "ventana no determinada"

    if df.empty:
        print("ℹ️ No hay noticias en la ventana definida. No se envía correo.")
        return

    # Ordenar por país y fecha si existe
    if "date_utc" in df.columns:
        df.sort_values(["country", "date_utc"], ascending=[True, False], inplace=True)
    else:
        df.sort_values(["country", "scraped_at"], ascending=[True, False], inplace=True)

    # Diccionario de países
    COUNTRY_NAMES = {
        "ar": ("Argentina", "🇦🇷"),
        "cl": ("Chile", "🇨🇱"),
        "pe": ("Perú", "🇵🇪"),
    }

    # Armar cuerpo por país
    MAX_PER_COUNTRY = int(os.getenv("MAX_PER_COUNTRY", "100"))
    grouped = df.groupby("country", sort=True)

    body = f"<h2>Noticias TikTok – {window_label}</h2>"
    for country, group in grouped:
        name, flag = COUNTRY_NAMES.get(country.lower(), (country.upper(), "🌎"))
        body += f"<h3>{flag} {name}</h3>"
        body += format_news(group.head(MAX_PER_COUNTRY))

    # Preparar mensaje
    msg = MIMEMultipart()
    msg["From"] = EMAIL_USER
    msg["To"] = ", ".join(to_list)
    msg["Subject"] = f"Noticias por país – '{NEWS_QUERY}' – {window_label}"
    msg.attach(MIMEText(body, "html"))

    # Enviar correo
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, to_list, msg.as_string())

    print(f"✅ Correo enviado ({window_label}).")



if __name__ == "__main__":
    send_email()
