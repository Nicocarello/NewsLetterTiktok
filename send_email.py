import os
import smtplib
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo

CSV_FILE = "news_results.csv"

# Configuración de correo (desde secrets)
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
EMAIL_TO = os.getenv("EMAIL_TO")
NEWS_QUERY = os.getenv("NEWS_QUERY", "tiktok")

# Zona horaria de Argentina
ART = ZoneInfo("America/Argentina/Buenos_Aires")

# ¡Ajuste aquí! Horas habilitadas para enviar correo (hora ART)
ALLOWED_HOURS = [9, 13, 18]

# ¡Ajuste aquí! Horas de corte locales para determinar ventanas
CUTS_LOCAL = [time(9,0), time(13,0), time(18,0)]


def current_window_utc():
    now_local = datetime.now(ART)
    today_local = now_local.date()

    current_cut_local = None
    for t in reversed(CUTS_LOCAL):
        cut_dt = datetime.combine(today_local, t, tzinfo=ART)
        if now_local >= cut_dt:
            current_cut_local = cut_dt
            break

    if current_cut_local is None:
        # antes de las 09:00 ART → desde ayer 18:00 hasta hoy 09:00
        start_local = datetime.combine(today_local - timedelta(days=1), time(18,0), tzinfo=ART)
        end_local = datetime.combine(today_local, time(9,0), tzinfo=ART)
    else:
        # El código aquí es muy robusto. No necesita cambios.
        idx = CUTS_LOCAL.index(current_cut_local.timetz())
        if idx == 0:
            start_local = datetime.combine(today_local - timedelta(days=1), time(18,0), tzinfo=ART)
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
    print("🚀 Iniciando envío de email...")

    # Validar hora actual (ART)
    now = datetime.now(ART)
    current_hour = now.hour

    if current_hour not in ALLOWED_HOURS:
        print(f"🕒 Ahora son las {current_hour}h ART. No se envía correo.")
        return

    # Validaciones de secrets y archivo
    print("🛠️ EMAIL_USER:", EMAIL_USER)
    print("🛠️ EMAIL_TO:", EMAIL_TO)
    print("📁 Existe CSV:", os.path.exists(CSV_FILE))

    if not EMAIL_USER or not EMAIL_PASS:
        print("❌ Faltan EMAIL_USER o EMAIL_PASS")
        return

    if not os.path.exists(CSV_FILE):
        print("⚠️ No existe el archivo de noticias.")
        return

    to_list = [e.strip() for e in (EMAIL_TO or "").split(",") if e.strip()]
    if not to_list:
        print("⚠️ EMAIL_TO vacío. No se envía correo.")
        return

    # Cargar CSV
    df = pd.read_csv(CSV_FILE)
    print(f"📊 Filas totales en CSV: {len(df)}")

    if "link" in df.columns:
        df.drop_duplicates(subset=["link"], inplace=True)

    # Filtrar por ventana temporal
    if "scraped_at" in df.columns:
        df["scraped_at"] = pd.to_datetime(df["scraped_at"], utc=True, errors="coerce")
        start_utc, end_utc = current_window_utc()
        print("🕒 Ventana actual (UTC):", start_utc, "→", end_utc)
        df = df[(df["scraped_at"] >= start_utc) & (df["scraped_at"] < end_utc)].copy()
        print("📊 Filas en ventana:", len(df))
        window_label = f"{start_utc.astimezone(ART).strftime('%Y-%m-%d %H:%M')}–{end_utc.astimezone(ART).strftime('%Y-%m-%d %H:%M')}"
    else:
        print("⚠️ No existe la columna 'scraped_at'; se enviarán todas las filas.")
        window_label = "ventana no determinada"

    if df.empty:
        print("ℹ️ No hay noticias en la ventana definida. No se envía correo.")
        return

    # Ordenar y preparar cuerpo
    if "date_utc" in df.columns:
        df.sort_values(["country", "date_utc"], ascending=[True, False], inplace=True)
    else:
        df.sort_values(["country", "scraped_at"], ascending=[True, False], inplace=True)

    COUNTRY_NAMES = {
        "ar": ("Argentina", "🇦🇷"),
        "cl": ("Chile", "🇨🇱"),
        "pe": ("Perú", "🇵🇪"),
    }

    MAX_PER_COUNTRY = int(os.getenv("MAX_PER_COUNTRY", "100"))
    grouped = df.groupby("country", sort=True)

    body = f"<h2>Noticias recolectadas – {window_label}</h2>"
    for country, group in grouped:
        name, flag = COUNTRY_NAMES.get(country.lower(), (country.upper(), "🌎"))
        body += f"<h3>{flag} {name}</h3>"
        body += format_news(group.head(MAX_PER_COUNTRY))

    # Crear y enviar correo
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_USER
        msg["To"] = ", ".join(to_list)
        msg["Subject"] = f"Noticias por país – '{NEWS_QUERY}' – {window_label}"
        msg.attach(MIMEText(body, "html"))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASS)
            server.sendmail(EMAIL_USER, to_list, msg.as_string())

        print(f"✅ Correo enviado a: {to_list} ({len(df)} noticias).")
    except Exception as e:
        print("❌ Error al enviar el correo:", e)


if __name__ == "__main__":
    send_email()
