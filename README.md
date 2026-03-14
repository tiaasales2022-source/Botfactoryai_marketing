# GoldenPages Intelligence Scraper

Professional Python scraper for `https://www.goldenpages.uz/uz/` with:

- automatic pagination detection
- retry + backoff on network failures
- random delays and User-Agent rotation
- tqdm progress bars
- rich colored terminal logs
- timestamped Excel and CSV exports
- JSON state file for resume/recovery

## File structure

```text
goldenpages_scraper/
  __init__.py
  __main__.py
  cli.py
  config.py
  exporters.py
  models.py
  parsers.py
  scraper.py
  state.py
  telegram_bot.py
  utils.py
pyproject.toml
requirements.txt
.env.example
README.md
```

## What each file does

- `cli.py`: argument parsing, startup banner, summary output
- `scraper.py`: request flow, retries, pagination crawl, company crawl
- `parsers.py`: HTML and JSON-LD extraction logic
- `state.py`: checkpoint/resume support
- `exporters.py`: CSV/XLSX export helpers
- `telegram_bot.py`: Telegram bot that launches scrapes and sends result files back to chats
- `models.py`: normalized company record
- `utils.py`: URL cleanup, string cleanup, meta-refresh parsing, atomic JSON writes
- `config.py`: base URLs, retry statuses, User-Agent pool

## Install

```bash
python -m pip install -r requirements.txt
```

Or:

```bash
python -m pip install -e .
```

## Usage

Scrape a single rubric:

```bash
python -m goldenpages_scraper "https://www.goldenpages.uz/uz/rubrics/?Id=4676"
```

Run with Docker:

```bash
docker build -t goldenpages-scraper .
docker run --rm -v ${PWD}/output:/app/output goldenpages-scraper "https://www.goldenpages.uz/uz/rubrics/?Id=4676"
```

Discover rubric links from the GoldenPages homepage and scrape them:

```bash
python -m goldenpages_scraper --discover-rubrics-from-home --max-rubrics 20
```

Limit test run:

```bash
python -m goldenpages_scraper "https://www.goldenpages.uz/uz/rubrics/?Id=4676" --max-companies 10 --max-pages-per-seed 2
```

Resume a stopped run:

```bash
python -m goldenpages_scraper --resume-state output/scrape_state_20260314_120000.json
```

Run the Telegram bot:

```bash
set TELEGRAM_BOT_TOKEN=123456:your_real_token
python -m goldenpages_scraper.telegram_bot
```

Run the Telegram bot in Docker:

```bash
docker build -t goldenpages-scraper .
docker run --rm ^
  -e TELEGRAM_BOT_TOKEN=123456:your_real_token ^
  -e TELEGRAM_ALLOWED_CHAT_IDS=123456789 ^
  -v %cd%\\telegram_output:/app/telegram_output ^
  --entrypoint python goldenpages-scraper -m goldenpages_scraper.telegram_bot
```

Telegram bot usage:

```text
/scrape https://www.goldenpages.uz/uz/rubrics/?Id=4676
/scrape https://www.goldenpages.uz/uz/rubrics/?Id=4676 50 2
/sms https://www.goldenpages.uz/uz/rubrics/?Id=4676 50 2
/demo
```

`/sms` komandasi uchta fayl yuboradi:

- oddiy SMS lead `CSV`
- oddiy SMS lead `XLSX`
- `Google Contacts` import uchun tayyor `CSV`

Optional bot access control:

```bash
set TELEGRAM_ALLOWED_CHAT_IDS=123456789,-1001234567890
python -m goldenpages_scraper.telegram_bot
```

## Botfactory lead machine

Build local leads and outreach workbook:

```bash
python main.py --mode scrape
```

Send outreach from pending leads:

```bash
python main.py --mode email
```

Recommended for Render free:

- set `EMAIL_TRANSPORT=gmail-api`
- set `GMAIL_API_CLIENT_ID=...`
- set `GMAIL_API_CLIENT_SECRET=...`
- set `GMAIL_API_REFRESH_TOKEN=...`
- set `EMAIL_SENDER_EMAIL=<your-gmail-address>`
- keep `EMAIL_REPLY_TO` / `GMAIL_EMAIL` for replies and IMAP sync

Run the full scrape + outreach pipeline:

```bash
python main.py --mode all
```

## Render web service

The project now includes a Flask service for Render:

- `GET /healthz` - Render health check
- `GET /status` - last pipeline state + lead counts
- `POST /trigger` - start `scrape`, `email`, `all`, or `sync-replies`

Local test:

```bash
python render_web_service.py
```

Trigger a run:

```bash
curl -X POST http://127.0.0.1:10000/trigger ^
  -H "Content-Type: application/json" ^
  -d "{\"mode\":\"all\"}"
```

Render deployment files:

- `render.yaml`
- `runtime.txt`
- `render_web_service.py`

Google Sheets can be used as the primary lead storage by setting:

- `GOOGLE_SHEETS_ENABLED=true`
- `GOOGLE_SHEETS_SPREADSHEET_ID=<sheet-id>`
- `GOOGLE_SHEETS_WORKSHEET=Leads`
- `GOOGLE_SERVICE_ACCOUNT_JSON_B64=<base64-json>`

Notes:

- free Render web services can sleep when idle, so built-in scheduling is not fully reliable on free
- free Render instances block SMTP ports, so `EMAIL_TRANSPORT=gmail-api` is the recommended production path on free
- `gmail_oauth_setup.py` helps generate a Gmail API refresh token locally

Gmail API setup for Render free:

1. Create a Google Cloud OAuth Desktop client.
2. Set `GMAIL_API_CLIENT_ID` and `GMAIL_API_CLIENT_SECRET` locally.
3. Run:

```bash
python gmail_oauth_setup.py
```

4. Copy the printed `GMAIL_API_REFRESH_TOKEN` into Render.
5. Set:

- `EMAIL_TRANSPORT=gmail-api`
- `GMAIL_EMAIL=<your-gmail-address>`
- `EMAIL_SENDER_EMAIL=<your-gmail-address>`
- `GMAIL_API_CLIENT_ID=<client-id>`
- `GMAIL_API_CLIENT_SECRET=<client-secret>`
- `GMAIL_API_REFRESH_TOKEN=<refresh-token>`

## Output

The scraper creates:

- `goldenpages_data_YYYYMMDD_HHMMSS.xlsx`
- `backup_data_YYYYMMDD_HHMMSS.csv`
- `scrape_state_YYYYMMDD_HHMMSS.json`

## Fields collected

- company name
- phone numbers
- address
- landmarks
- website
- emails
- activity types
- source URL
- source listing URL
- scrape timestamp

## Edge cases handled

- hidden phone/email UI: parser reads JSON-LD `LocalBusiness` data when present
- obfuscated website links: parser reads visible domain, then resolves meta-refresh fallback if needed
- duplicate company URLs across rubrics/pages: deduplicated by normalized URL and company id
- paginated rubrics: next pages are auto-discovered from pagination links
- temporary connection issues or HTTP 429/5xx: retried with exponential backoff
- interrupted runs: state file lets you resume without losing completed work
- missing fields: scraper keeps empty values instead of crashing

## Notes

- Respect the site's terms and robots policy before large crawls.
- Homepage discovery can return hundreds of rubric URLs, so start with `--max-rubrics` for trial runs.
