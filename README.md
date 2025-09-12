# 📰 Newsletter TikTok

Esta aplicación recolecta noticias desde Google News usando **Apify** y las envía por correo en horarios específicos del día. El flujo completo se ejecuta automáticamente con **GitHub Actions**, sin necesidad de servidores propios.

-----

## 🚀 Funcionalidades

  * **Scraping de noticias** de Argentina 🇦🇷, Chile 🇨🇱 y Perú 🇵🇪.
  * **Recolección cada hora**, acumulando resultados en `news_results.csv`.
  * **Filtrado automático** para evitar duplicados.
  * **Envío de correos** en horarios definidos (ART):
      * **08:00** → noticias desde el día anterior 20:00 hasta 08:00.
      * **12:00** → noticias entre 08:00 y 12:00.
      * **15:00** → noticias entre 12:00 y 15:00.
      * **18:00** → noticias entre 15:00 y 18:00.
      * **20:00** → noticias entre 18:00 y 20:00.
  * **Emails con formato HTML**:
      * Título en **negrita**
      * Fecha y medio
      * Snippet de la noticia
      * Enlace a la fuente

-----

## 📂 Estructura

```
.github/workflows/
├── scraper.yml 		  # Ejecuta el scraper cada hora
└── send-email.yml 		  # Envía correos en horarios específicos
scraper.py 			      # Scraper de noticias (Apify + Pandas)
send_email.py 		    # Lógica de envío de correos con ventanas horarias
requirements.txt 		  # Dependencias de Python
news_results.csv 		  # Archivo acumulativo con noticias
```

-----

## ⚙️ Configuración

### Clonar el repo

```bash
git clone https://github.com/<usuario>/NewsLetterTiktok.git
cd NewsLetterTiktok
```

### Dependencias

```bash
pip install -r requirements.txt
```

### Secrets en GitHub Actions

  * **APIFY\_TOKEN**: token de Apify.
  * **APIFY\_ACTOR\_ID**: ID del actor de Apify (ej: `easyapi/google-news-scraper`).
  * **EMAIL\_USER**: correo remitente (ej: Gmail).
  * **EMAIL\_PASS**: contraseña de aplicación (App Password).
  * **EMAIL\_TO**: destinatarios separados por comas.

### Variables opcionales

  * **NEWS\_QUERY**: palabra clave a buscar (por defecto "tiktok").
  * **MAX\_PER\_COUNTRY**: máximo de noticias por país en el email (default: 100).

-----

## 🛠️ Cómo funciona

### Scraper (`scraper.py`)

  * Corre cada hora (`cron` en GitHub Actions).
  * Guarda las noticias en `news_results.csv`.
  * Añade metadatos (`country`, `scraped_at`).
  * Deduplica por `link`.

### Envío (`send_email.py`)

  * Se ejecuta solo en los horarios definidos (08, 12, 15, 18, 20 ART).
  * Filtra noticias de la ventana temporal correspondiente.
  * Construye un email en HTML agrupado por país y lo envía.

-----

## 📧 Ejemplo de correo

**Noticias recolectadas – 2025-09-11 08:00–12:00 ART**

### 🇦🇷 Argentina

**Título de la noticia**
2025-09-11T09:32Z - Diario Ejemplo
Resumen breve...
[Ver noticia](https://www.google.com/search?q=https://example.com/noticia)

### 🇨🇱 Chile

...

-----
