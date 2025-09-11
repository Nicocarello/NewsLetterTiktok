import os
import smtplib
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

CSV_FILE = "news_results.csv"

# Configuración de correo (mejor ponerlas como secrets en GitHub Actions)
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = os.getenv("EMAIL_USER")      # tu correo remitente
EMAIL_PASS = os.getenv("EMAIL_PASS")      # tu password o app password
EMAIL_TO = os.getenv("EMAIL_TO")          # destinatario(s), separados por coma


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
    msg["To"] = EMAIL_TO
    msg["Subject"] = "Noticias recolectadas por país"
    msg.attach(MIMEText(body, "html"))

    # Enviar correo
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.send_message(msg)

    print("✅ Correo enviado correctamente.")


if __name__ == "__main__":
    send_email()
