# 📰 NewsLetterTiktok

Automated workflow to collect TikTok news, store them in Google Sheets, and send daily email digests.

---

## 🚀 Features

* **Scraping**: Uses [Apify Google News actor](https://apify.com/easyapi/google-news-scraper) to fetch TikTok news in Argentina, Chile, and Peru.
* **Google Sheets integration**: Results are appended and deduplicated by link.
* **Email reports**:

  * 08:00 → news between 18:00 (previous day) and 08:00.
  * 13:00 → news between 08:00 and 13:00.
  * 18:00 → news between 13:00 and 18:00.
  * Grouped by country.

---

## 📂 Structure

```
scraper.py          # runs hourly, updates Google Sheets
mailer.py           # sends digests at 08:00, 13:00, 18:00
requirements.txt    # dependencies
.github/workflows/
  ├── newsletter.yaml   # workflow for scraper
  └── mailer.yaml       # workflow for mailer
```

---

## 🔑 Setup

Add these **GitHub Secrets**:

* `GOOGLE_CREDENTIALS` → Google service account JSON
* `APIFY_ACTOR_ID` → Apify actor ID
* `APIFY_TOKEN` → Apify API token
* `EMAIL_USER` → sender email (e.g. Gmail)
* `EMAIL_PASS` → app password for the sender
* `EMAIL_TO` → comma-separated recipients

---

## 🤖 Automation

* **Scraper** runs hourly (`newsletter.yaml`).
* **Mailer** runs at 08:00, 13:00, 18:00 AR time (`mailer.yaml`).

---

## 📧 Sample email

```
News (08:00 - 13:00)

=== Argentina ===
- TikTok strengthens controls (La Nación)
  https://example.com/tiktok-argentina

=== Chile ===
- TikTok under scrutiny (El Mercurio)
  https://example.com/tiktok-chile
```

---
