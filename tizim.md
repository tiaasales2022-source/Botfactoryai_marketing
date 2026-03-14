# Tizim Hujjati

## Loyiha Nomi

Botfactory Lead Machine

## Qisqacha Mazmun

Bu tizim GoldenPages'dan B2B lead yig'adi, ularni tozalaydi, saralaydi, AI yordamida outreach matn tayyorlaydi va email yuborish jarayonini boshqaradi.

Loyiha endi faqat scraper emas. U to'liq pipeline:

- GoldenPages'dan kompaniyalarni topadi
- email va telefonlarni ajratadi
- lead'larni kategoriyalarga bo'ladi
- dublikatlarni yo'q qiladi
- Google Sheets yoki Excel backup'da bazani saqlaydi
- Gemini orqali shaxsiylashtirilgan outreach yaratadi
- HTML va plain-text email tayyorlaydi
- yuborilgan holatni saqlaydi
- reply sync orqali unsubscribe / blacklist yuritadi
- Telegram va Render web service orqali boshqariladi

## Asosiy Maqsad

Botfactory AI uchun O'zbekiston bozoridan istiqbolli B2B lead'larni topish va ularga nazoratli, issiq, personalizatsiya qilingan outreach yuborish.

## Arxitektura Qatlamlari

Tizim 5 ta asosiy qatlamdan iborat.

### 1. Core Scraper Layer

Joylashuvi: `goldenpages_scraper/`

Vazifasi:

- GoldenPages sahifalarini yuklash
- rubric va company linklarni topish
- pagination bo'ylab yurish
- kompaniya sahifalaridan structured data ajratish
- CSV, XLSX va state fayl yaratish

Asosiy fayllar:

- `goldenpages_scraper/scraper.py` - crawling, retry, backoff, request flow
- `goldenpages_scraper/parsers.py` - HTML va JSON-LD parsing
- `goldenpages_scraper/models.py` - kompaniya modeli
- `goldenpages_scraper/exporters.py` - CSV/XLSX export
- `goldenpages_scraper/state.py` - resume uchun checkpoint
- `goldenpages_scraper/utils.py` - normalize va helper funksiyalar
- `goldenpages_scraper/config.py` - base URL, retry status, user-agent pool
- `goldenpages_scraper/cli.py` - standalone scraper CLI

### 2. Lead Machine Layer

Joylashuvi: `main.py`

Vazifasi:

- scrape natijasini o'qish
- emaili bor lead'larni ajratish
- kategoriya aniqlash
- email syntax va MX validatsiya qilish
- lead score hisoblash
- blacklist va statuslarni yuritish
- lead bazani Excel yoki Google Sheets'da saqlash

`main.py` rejimlari:

- `scrape`
- `email`
- `all`
- `sync-replies`

### 3. Outreach Layer

Joylashuvi: `main.py` va `template.html`

Vazifasi:

- kategoriyaga mos campaign tanlash
- deterministic A/B variant berish
- Gemini orqali AI personalizatsiya yaratish
- HTML va plain-text email tayyorlash
- retry va exponential backoff bilan yuborish
- transportni avtomatik tanlash: Gmail API, Brevo API yoki SMTP

AI personalization:

- default model: `gemini-3.1-flash-lite-preview`
- generation config: `temperature=0.7`, `top_p=0.95`, `top_k=40`, `max_output_tokens=300`
- AI ishlamasa yoki limitga yetsa tizim avtomatik statik shablonlarga qaytadi

### 4. Delivery and Control Layer

Bu qatlam ikki interfeysdan iborat:

- `goldenpages_scraper/telegram_bot.py` - Telegram orqali scrape
- `render_web_service.py` - Render web service orqali health, status va trigger

Telegram vazifalari:

- scrape job qabul qilish
- progress yuborish
- CSV/XLSX natijani qaytarish

Render vazifalari:

- `GET /` umumiy holat
- `GET /healthz` health check
- `GET /status` oxirgi run holati
- `POST /trigger` pipeline'ni masofadan ishga tushirish

### 5. Persistence Layer

Tizim 2 xil storage bilan ishlay oladi:

- Excel workbook
- Google Sheets + lokal Excel backup

Google Sheets yoqilgan bo'lsa:

- lead'lar avval Sheets'dan o'qiladi
- har yangilanishdan keyin worksheet yoziladi
- shu bilan birga lokal `LEADS_FILE` backup ham yozib boriladi

## Papka Tuzilishi

```text
.
|-- main.py
|-- render_web_service.py
|-- render.yaml
|-- runtime.txt
|-- template.html
|-- .env
|-- .env.example
|-- README.md
|-- tizim.md
|-- requirements.txt
|-- pyproject.toml
|-- Dockerfile
`-- goldenpages_scraper/
    |-- cli.py
    |-- config.py
    |-- exporters.py
    |-- models.py
    |-- parsers.py
    |-- scraper.py
    |-- state.py
    |-- telegram_bot.py
    |-- utils.py
    `-- __main__.py
```

## Tizim Qanday Ishlaydi

### A. Scrape Oqimi

1. Seed URL olinadi.
2. Tizim rubric yoki company sahifasini aniqlaydi.
3. Rubric bo'lsa pagination topiladi.
4. Listing page'lardan company URL'lar yig'iladi.
5. Har bir company sahifasi parse qilinadi.
6. Telefon, email, website, address, activity type, rating kabi maydonlar olinadi.
7. Natija normalize qilinadi.
8. CSV, XLSX va state fayllar yoziladi.

### B. Lead Build Oqimi

1. Scrape qilingan XLSX o'qiladi.
2. Faqat emaili bor row'lar olinadi.
3. Kategoriya aniqlanadi.
4. Priority filter ishlatiladi.
5. Email validation qilinadi.
6. Lead score hisoblanadi.
7. Lead'lar dublikat nazoratidan o'tadi.
8. Yakuniy lead bazaga yoziladi.

Lead ustunlari:

- `Company ID`
- `Company Name`
- `Email`
- `Phone`
- `Category`
- `Activity Types`
- `Website`
- `Source URL`
- `Source Listing URL`
- `Lead Captured At`
- `Validation Status`
- `Lead Score`
- `Rating Value`
- `Rating Count`
- `Language`
- `Status`
- `LastContacted`
- `Sent At`
- `Last Error`
- `Template Used`

### C. Outreach Oqimi

1. `main.py --mode email` ishga tushadi.
2. Lead bazadan `Sent` va `Blacklisted` bo'lmagan row'lar olinadi.
3. Lead score bo'yicha ustuvorlik beriladi.
4. Kategoriya asosida campaign tanlanadi.
5. Deterministic A/B variant olinadi.
6. Gemini yoqilgan bo'lsa AI personalizatsiya matni yaratiladi.
7. AI ishlamasa static copy ishlatiladi.
8. `template.html` orqali HTML email render qilinadi.
9. Plain-text fallback yaratiladi.
10. `EMAIL_TRANSPORT=auto` bo'lsa avval Gmail API, keyin Brevo API, oxirida SMTP tanlanadi.
11. Render free muhitida tavsiya etilgan kanal: Gmail API.
12. Status `Sent` yoki `Error` ga yangilanadi.
13. Warm-up state va JSON loglar yangilanadi.

### D. Reply Sync va Blacklist Oqimi

1. IMAP orqali inbox tekshiriladi.
2. Lead email'lardan kelgan yangi javoblar topiladi.
3. `stop`, `unsubscribe`, `bekor` kabi keyword'lar tekshiriladi.
4. Mos email'lar blacklist'ga qo'shiladi.
5. Keyingi outreach run'larda ular skip qilinadi.

### E. Google Sheets Oqimi

1. `GOOGLE_SHEETS_ENABLED=true` bo'lsa service account bilan ulanish qilinadi.
2. Kerakli worksheet topiladi yoki yaratiladi.
3. Lead'lar Sheets'dan o'qiladi.
4. O'zgartirilgan bazaning to'liq snapshot'i yana Sheets'ga yoziladi.
5. Shu paytda lokal Excel backup ham saqlanadi.

### F. Render Web Service Oqimi

1. `gunicorn render_web_service:app` Flask servisni ko'taradi.
2. Render `GET /healthz` orqali servisni tekshiradi.
3. `POST /trigger` orqali `scrape`, `email`, `all` yoki `sync-replies` run bosiladi.
4. Run background thread'da bajariladi.
5. `GET /status` oxirgi run natijasini ko'rsatadi.
6. `RENDER_ENABLE_SCHEDULER=true` bo'lsa cron scheduler ishlaydi.

### G. Telegram Oqimi

1. User botga URL yuboradi.
2. Bot request'ni parse qiladi.
3. Scrape run ishga tushadi.
4. Progress va natija chatga qaytariladi.

## Anti-Fragile Mexanizmlar

- User-Agent rotation
- random request delay
- retryable status code'lar uchun qayta urinish
- exponential backoff
- timeout limit
- state fayl orqali resume
- duplicate lead control
- email validation
- Gmail App Password preflight check
- API transport fallback: Render free muhitida SMTP o'rniga Gmail API yoki Brevo HTTPS API ishlatish
- AI fallback
- JSON log va checkpoint

## Muhim Konfiguratsiyalar

Asosiy sozlamalar `.env` ichida saqlanadi.

Muhim env'lar:

- `SCRAPE_SEED_URL` - default GoldenPages URL
- `SCRAPER_MAX_COMPANIES` - company limiti
- `SCRAPER_MAX_PAGES_PER_SEED` - rubric page limiti
- `SCRAPER_OUTPUT_DIR` - scraper export papkasi
- `LEADS_FILE` - lokal Excel backup
- `FILTER_PRIORITY_CATEGORIES` - faqat muhim kategoriyalarni olish
- `VALIDATE_EMAIL_MX` - MX validation
- `GEMINI_ENABLED` - AI personalizatsiyani yoqish
- `GEMINI_API_KEY` - Gemini API kaliti
- `GEMINI_MODEL` - ishlatiladigan Gemini model nomi
- `GOOGLE_SHEETS_ENABLED` - Sheets storage'ni yoqish
- `GOOGLE_SHEETS_SPREADSHEET_ID` - spreadsheet ID
- `GOOGLE_SHEETS_WORKSHEET` - worksheet nomi
- `GOOGLE_SERVICE_ACCOUNT_JSON_B64` - service account JSON'ning base64 ko'rinishi
- `GOOGLE_SERVICE_ACCOUNT_FILE` - lokal credential fayl yo'li
- `EMAIL_DELAY_MIN_SECONDS` - min kutish
- `EMAIL_DELAY_MAX_SECONDS` - max kutish
- `EMAIL_MAX_PER_RUN` - bitta run'dagi email limiti
- `WARM_UP_MODE` - warm-up rejimi
- `REPLY_SYNC_ENABLED` - IMAP reply sync
- `GMAIL_EMAIL` - yuboruvchi email
- `GMAIL_APP_PASSWORD` - Gmail App Password
- `RENDER_TRIGGER_TOKEN` - `/trigger` endpoint himoyasi
- `RENDER_ENABLE_SCHEDULER` - ichki scheduler
- `RENDER_SCHEDULE_CRON` - cron ifodasi
- `RENDER_TIMEZONE` - scheduler timezone

## Fayl Chiqishlari

Scraper chiqishlari:

- `backup_data_YYYYMMDD_HHMMSS.csv`
- `goldenpages_data_YYYYMMDD_HHMMSS.xlsx`
- `scrape_state_YYYYMMDD_HHMMSS.json`

Lead storage:

- `botfactory_leads.xlsx` yoki `leads.xlsx`
- `Google Sheets / Leads worksheet`

Qo'shimcha chiqishlar:

- `logs/*.json`
- `telegram_output/chat_<id>/<run_id>/...`

## Render Deploy Qatlami

Render uchun quyidagi fayllar tayyor:

- `render_web_service.py`
- `render.yaml`
- `runtime.txt`

Start command:

```bash
gunicorn render_web_service:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 180
```

## Ishga Tushirish Variantlari

### 1. Faqat scraper

```bash
python -m goldenpages_scraper "https://www.goldenpages.uz/uz/rubrics/?Id=4676"
```

### 2. Lead build

```bash
python main.py --mode scrape
```

### 3. Faqat email yuborish

```bash
python main.py --mode email
```

### 4. To'liq pipeline

```bash
python main.py --mode all
```

### 5. Reply sync

```bash
python main.py --mode sync-replies
```

### 6. Telegram bot

```bash
python -m goldenpages_scraper.telegram_bot
```

### 7. Render web service lokal test

```bash
python render_web_service.py
```

### 8. Render trigger test

```bash
curl -X POST http://127.0.0.1:10000/trigger -H "Content-Type: application/json" -d "{\"mode\":\"all\"}"
```

## Statuslar Ma'nosi

- `New` - yangi lead
- `Sent` - outreach yuborilgan
- `Error` - yuborishda xato bo'lgan
- `Blacklisted` - reply yoki manual blacklist sababli to'xtatilgan

Validation status misollari:

- `valid-mx`
- `valid-syntax`
- `mx-unchecked`
- `invalid-syntax`
- `no-mx`

## Kuchli Tomonlar

- scraping va outreach bitta loyiha ichida
- modular arxitektura
- recovery uchun state management
- rich dashboard va progress
- Telegram orqali masofadan boshqarish
- Google Sheets + Excel backup storage
- Gemini orqali dinamik outreach personalizatsiyasi
- deterministic A/B testing
- blacklist va unsubscribe nazorati
- Render web service trigger va health endpointlari

## Cheklovlar

- GoldenPages layout o'zgarsa parser'ga patch kerak bo'lishi mumkin
- MX validation email mavjudligini 100% kafolatlamaydi
- Gmail kunlik limit va spam policy mavjud
- Render bepul web service idle holatda uxlab qoladi
- shu sabab ichki scheduler free plan'da 100% ishonchli emas
- Render bepul web service'da SMTP outbound bloklanishi mumkin
- Gmail SMTP bilan free Render'da to'liq avtomatik outreach ishlamasligi mumkin
- Google Sheets yoqilgan bo'lsa service account va spreadsheet access to'g'ri sozlanishi kerak

## Tavsiya Qilinadigan Ishlash Tartibi

1. Avval kichik limit bilan scrape test qiling.
2. Lead bazani ko'zdan kechiring.
3. O'zingizga test email yuborib preview tekshiring.
4. 1-2 real lead bilan trial outreach qiling.
5. Google Sheets va Render endpointlarini alohida test qiling.
6. Shundan keyin bulk run yoki scheduler yoqing.

## Xulosa

Bu loyiha oddiy scraping script emas.

Bu Botfactory AI uchun to'liq lead engine:

- data topadi
- data'ni tozalaydi
- lead'ga aylantiradi
- AI bilan personalizatsiya qiladi
- outreach yuboradi
- statusni saqlaydi
- reply va blacklist'ni kuzatadi
- Telegram va Render orqali boshqariladi

Qisqa ko'rinish:

`GoldenPages -> Lead Build -> Storage -> AI Outreach -> Delivery -> Tracking -> Control`
