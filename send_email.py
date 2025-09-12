import os
import smtplib
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta, time, timezone

CSV_FILE = "news_results.csv"

# Configuración de correo (mejor ponerlas como secrets en GitHub Actions)
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = os.getenv("EMAIL_USER")      # tu correo remitente
EMAIL_PASS = os.getenv("EMAIL_PASS")      # tu password o app password
EMAIL_TO = os.getenv("EMAIL_TO")          # destinatario(s), separados por coma

df = pd.read_csv(CSV_FILE)
if "scraped_at" not in df.columns:
    print("⚠️ No existe columna scraped_at, no puedo filtrar por ventana.")
else:
    start_utc, end_utc = current_window_utc()
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], utc=True, errors="coerce")
    df = df[(df["scraped_at"] >= start_utc) & (df["scraped_at"] < end_utc)].copy()


def get_time_window():
    now = datetime.now(timezone.utc)  # usamos UTC para consistencia
    today = now.date()

    # horarios de corte (UTC, ajusta si necesitas otra zona horaria)
    schedule = [
        time(8, 0), time(12, 0), time(15, 0), time(18, 0), time(20, 0)
    ]

    # encontrar el horario de corte más cercano
    current_cut = None
    for t in reversed(schedule):
        cut_dt = datetime.combine(today, t, tzinfo=timezone.utc)
        if now >= cut_dt:
            current_cut = cut_dt
            break

    if current_cut is None:
        # antes de las 08:00 → tomar desde ayer 20:00
        start = datetime.combine(today - timedelta(days=1), time(20, 0), tzinfo=timezone.utc)
        end = datetime.combine(today, time(8, 0), tzinfo=timezone.utc)
    else:
        idx = schedule.index(current_cut.timetz())
        if idx == 0:
            # caso especial: 08:00 → desde ayer 20:00
            start = datetime.combine(today - timedelta(days=1), time(20, 0), tzinfo=timezone.utc)
        else:
            start = datetime.combine(today, schedule[idx - 1], tzinfo=timezone.utc)
        end = current_cut

    return start.isoformat(), end.isoformat()


def format_news(df):
    """Convierte noticias en bloques HTML formateados"""
    html_content = ""
    for _, row in df.iterrows():
        title = f"<b>{row.get('title', '')}</b>"
        date_source = f"{row.get('date_utc', '')} - {row.get('source', '')}"
        snippet = row.get("snippet", "")
        link = f"<a href='{row.get('link', '')}'>Ver noticia</a>"

        html_content += f"""
        <p>
            {title}<br>
            <i>{date_source}</i><br>
            {snippet}<br>
            {link}
        </p>
        <hr>
        """
    return html_content


def send_email():
    # Cargar CSV
    if not os.path.exists(CSV_FILE):
        print("⚠️ No existe el archivo de noticias.")
        return

    df = pd.read_csv(CSV_FILE)
    df.drop_duplicates(subset = 'link')

    # Agrupar por país
    grouped = df.groupby("country")

    # Construir cuerpo HTML
    body = "<h2>Noticias recolectadas</h2>"
    for country, group in grouped:
        body += f"<h3>🌎 {country.upper()}</h3>"
        body += format_news(group)

    # Configurar mensaje
    msg = MIMEMultipart()
    msg["From"] = EMAIL_USER
    ahora = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    to_list = [e.strip() for e in EMAIL_TO.split(",") if e.strip()]
    msg["To"] = ", ".join(to_list)
    msg["Subject"] = f"Noticias TikTok por país – {ahora}"
    msg.attach(MIMEText(body, "html"))

    # Enviar correo
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, to_list, msg.as_string())

    print("✅ Correo enviado correctamente.")


if __name__ == "__main__":
    send_email()
