from __future__ import annotations

import argparse
import asyncio
import base64
import imaplib
import json
import random
import re
import smtplib
import ssl
import time
from dataclasses import dataclass
from datetime import datetime
from email import message_from_bytes
from email.message import EmailMessage
from email.utils import formataddr, parseaddr
from pathlib import Path
from typing import Any, Sequence
from urllib.parse import quote

import pandas as pd
import requests
from dotenv import load_dotenv
from jinja2 import BaseLoader, Environment, select_autoescape
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from goldenpages_scraper.scraper import GoldenPagesScraper, ScraperSettings
from goldenpages_scraper.utils import collapse_whitespace

try:
    import dns.resolver
except Exception:  # pragma: no cover
    dns = None

try:
    from google import genai as google_genai
    from google.genai import types as google_genai_types
except Exception:  # pragma: no cover
    google_genai = None
    google_genai_types = None

try:
    import gspread
    from google.oauth2.service_account import Credentials as GoogleServiceAccountCredentials
except Exception:  # pragma: no cover
    gspread = None
    GoogleServiceAccountCredentials = None


EMAIL_RE = re.compile(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$", re.IGNORECASE)
TEMPLATE_RE = re.compile(r"{{\s*([a-zA-Z0-9_]+)\s*}}")
_GEMINI_CLIENTS: dict[str, Any] = {}
_GOOGLE_SHEETS_WORKSHEETS: dict[str, Any] = {}
_GMAIL_API_TOKENS: dict[str, tuple[str, float]] = {}

HEALTHCARE_KEYWORDS = (
    "klinika",
    "tibbiyot",
    "med",
    "shifoxona",
    "poliklinika",
    "stomatolog",
    "diagnostika",
    "hospital",
    "pharma",
    "apteka",
)
EDUCATION_KEYWORDS = (
    "oquv",
    "o'quv",
    "talim",
    "ta'lim",
    "kurs",
    "maktab",
    "education",
    "training",
    "learning",
    "akademiya",
    "academy",
)
LOGISTICS_KEYWORDS = (
    "logistika",
    "transport",
    "cargo",
    "yuk",
    "dispatch",
    "ekspeditor",
    "ekspeditorlik",
    "tashish",
    "delivery",
    "ombor",
    "warehouse",
)
BUSINESS_KEYWORDS = (
    "savdo",
    "retail",
    "shop",
    "store",
    "market",
    "it",
    "dastur",
    "software",
    "bank",
    "sugurta",
    "sug'urta",
    "insurance",
    "call center",
    "aloqa",
    "kommunikatsiya",
    "support",
    "crm",
)

LEAD_COLUMNS = [
    "Company ID",
    "Company Name",
    "Email",
    "Phone",
    "Category",
    "Activity Types",
    "Website",
    "Source URL",
    "Source Listing URL",
    "Lead Captured At",
    "Validation Status",
    "Lead Score",
    "Rating Value",
    "Rating Count",
    "Language",
    "Status",
    "LastContacted",
    "Sent At",
    "Last Error",
    "Template Used",
]

DEFAULT_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="uz">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{{subject}}</title>
</head>
<body style="margin:0;padding:0;background:#f5efe6;font-family:Arial,sans-serif;color:#1f2937;">
  <div style="display:none;max-height:0;overflow:hidden;opacity:0;color:transparent;">{{preheader}}</div>
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f5efe6;padding:28px 14px;">
    <tr>
      <td align="center">
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:720px;background:#101418;border-radius:22px;overflow:hidden;">
          <tr>
            <td style="padding:22px 32px;background:linear-gradient(135deg,#c28b31 0%,#f1d08f 100%);">
              <div style="font-size:12px;letter-spacing:2px;text-transform:uppercase;color:#101418;font-weight:bold;">{{brand_name}}</div>
              <h1 style="margin:10px 0 0;font-size:30px;line-height:1.2;color:#101418;">{{headline}}</h1>
            </td>
          </tr>
          <tr>
            <td style="padding:34px 32px;color:#f8fafc;">
              <p style="margin:0 0 18px;font-size:16px;line-height:1.75;">{{greeting}}</p>
              <p style="margin:0 0 18px;font-size:16px;line-height:1.75;color:#d3d8df;">{{intro}}</p>
              <p style="margin:0 0 18px;font-size:16px;line-height:1.75;color:#d3d8df;">{{problem}}</p>
              <p style="margin:0 0 18px;font-size:16px;line-height:1.75;color:#d3d8df;">{{solution}}</p>
              <div style="margin:0 0 18px;padding:18px 20px;border:1px solid rgba(241,208,143,0.18);border-radius:16px;background:#141a20;">
                <p style="margin:0 0 8px;font-size:13px;letter-spacing:1px;text-transform:uppercase;color:#f1d08f;">{{custom_offer_title}}</p>
                <p style="margin:0;font-size:15px;line-height:1.75;color:#d3d8df;">{{custom_offer}}</p>
              </div>
              <div style="margin:26px 0 0;padding:18px 20px;border:1px solid rgba(241,208,143,0.28);border-radius:16px;background:#171d23;">
                <p style="margin:0;font-size:16px;line-height:1.75;color:#fff1cb;"><strong>{{cta}}</strong></p>
              </div>
              <p style="margin:16px 0 0;font-size:13px;line-height:1.75;color:#d3d8df;">{{discovery_call_text}}</p>
              <p style="margin:28px 0 0;font-size:15px;line-height:1.7;color:#d3d8df;">
                Hurmat bilan,<br>
                <strong>{{signature_name}}</strong><br>
                {{signature_role}}<br>
                {{signature_company}}
              </p>
              <p style="margin:16px 0 0;font-size:13px;line-height:1.7;color:#94a3b8;">
                Telefon: {{signature_phone}}<br>
                Website: <a href="{{signature_website}}" style="color:#f1d08f;text-decoration:none;">{{signature_website}}</a><br>
                Email: <a href="mailto:{{sender_email}}" style="color:#f1d08f;text-decoration:none;">{{sender_email}}</a>
              </p>
              <p style="margin:16px 0 0;font-size:12px;line-height:1.7;color:#7f8ea3;">{{unsubscribe_text}}</p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>
"""


@dataclass(slots=True)
class SMTPConfig:
    transport: str
    host: str
    port: int
    username: str
    password: str
    sender_email: str
    from_name: str
    reply_to: str
    retry_limit: int
    api_key: str
    api_url: str
    request_timeout_seconds: float
    sandbox_mode: bool
    oauth_client_id: str
    oauth_client_secret: str
    oauth_refresh_token: str
    oauth_token_url: str
    gmail_api_send_url: str


@dataclass(slots=True)
class BrandConfig:
    brand_name: str
    reply_phrase: str
    unsubscribe_text: str
    custom_offer: str
    discovery_call_url: str
    signature_name: str
    signature_role: str
    signature_company: str
    signature_phone: str
    signature_website: str


@dataclass(slots=True)
class GoogleSheetsConfig:
    spreadsheet_id: str
    worksheet_name: str
    service_account_json_b64: str
    service_account_file: str


@dataclass(slots=True)
class AppConfig:
    mode: str
    seed_url: str | None
    leads_file: Path
    template_file: Path
    logs_dir: Path
    blacklist_file: Path
    warm_up_state_file: Path
    scraper_output_dir: Path
    max_companies: int | None
    max_pages_per_seed: int | None
    delay_min_seconds: float
    delay_max_seconds: float
    email_max_per_run: int
    filter_priority_categories: bool
    validate_email_mx: bool
    reply_sync_enabled: bool
    imap_host: str
    imap_port: int
    imap_folder: str
    unsubscribe_keywords: tuple[str, ...]
    warm_up_mode: bool
    warm_up_start_daily_limit: int
    warm_up_daily_increment: int
    warm_up_max_daily_limit: int
    default_language: str
    gemini_enabled: bool
    gemini_api_key: str
    gemini_model: str
    sheets: GoogleSheetsConfig | None
    smtp: SMTPConfig
    brand: BrandConfig


@dataclass(slots=True)
class LeadBuildResult:
    dataframe: pd.DataFrame
    rows_with_email: int
    targeted_valid_rows: int
    skipped_priority_rows: int
    invalid_email_rows: int


@dataclass(slots=True)
class ScrapePhaseResult:
    total_scraped_rows: int
    rows_with_email: int
    targeted_valid_rows: int
    skipped_priority_rows: int
    invalid_email_rows: int
    new_leads_added: int
    existing_leads_updated: int
    total_leads_in_file: int
    output_file: Path


@dataclass(slots=True)
class SendPhaseResult:
    pending_before: int
    sent_now: int
    failed_now: int
    skipped_sent: int
    blacklisted_skipped: int
    warm_up_remaining: int
    reply_blacklisted_now: int
    output_file: Path


@dataclass(slots=True)
class EmailDraft:
    subject: str
    html_body: str
    plain_text_body: str
    template_used: str


@dataclass(slots=True)
class ReplySyncResult:
    matched_messages: int
    blacklisted_now: int
    total_blacklisted: int
    error: str = ""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="botfactory-lead-machine",
        description="GoldenPages scraper and Botfactory AI outreach automator.",
    )
    parser.add_argument("--mode", choices=["scrape", "email", "all", "sync-replies"], default="all")
    parser.add_argument("--seed-url", default=None)
    parser.add_argument("--max-companies", type=int, default=None)
    parser.add_argument("--max-pages-per-seed", type=int, default=None)
    parser.add_argument("--leads-file", type=Path, default=None)
    parser.add_argument("--template-file", type=Path, default=None)
    parser.add_argument("--email-max-per-run", type=int, default=None)
    parser.add_argument("--disable-priority-filter", action="store_true")
    parser.add_argument("--disable-mx-validation", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args(argv)
    console = Console()

    try:
        config = build_config(args)
        asyncio.run(main_async(config, console))
    except KeyboardInterrupt:
        console.print("[bold yellow]Interrupted.[/bold yellow]")
        return 130
    except Exception as exc:
        console.print(f"[bold red]Error:[/bold red] {exc}")
        return 1
    return 0


def build_config(args: argparse.Namespace) -> AppConfig:
    seed_url = collapse_whitespace(args.seed_url or getenv_str("SCRAPE_SEED_URL", ""))
    leads_file = args.leads_file or Path(getenv_str("LEADS_FILE", "botfactory_leads.xlsx"))
    template_file = args.template_file or Path(getenv_str("TEMPLATE_FILE", "template.html"))
    scraper_output_dir = Path(getenv_str("SCRAPER_OUTPUT_DIR", "output"))
    logs_dir = Path(getenv_str("LOGS_DIR", "logs"))
    blacklist_file = Path(getenv_str("BLACKLIST_FILE", str(logs_dir / "blacklist.json")))
    warm_up_state_file = Path(getenv_str("WARM_UP_STATE_FILE", str(logs_dir / "warmup_state.json")))
    max_companies = args.max_companies if args.max_companies is not None else getenv_optional_int("SCRAPER_MAX_COMPANIES")
    max_pages_per_seed = (
        args.max_pages_per_seed if args.max_pages_per_seed is not None else getenv_optional_int("SCRAPER_MAX_PAGES_PER_SEED")
    )
    email_max_per_run = (
        args.email_max_per_run if args.email_max_per_run is not None else getenv_optional_int("EMAIL_MAX_PER_RUN", 50)
    )
    delay_min_seconds = getenv_float("EMAIL_DELAY_MIN_SECONDS", 10.0)
    delay_max_seconds = getenv_float("EMAIL_DELAY_MAX_SECONDS", 20.0)
    filter_priority_categories = not args.disable_priority_filter and getenv_bool("FILTER_PRIORITY_CATEGORIES", True)
    validate_email_mx = not args.disable_mx_validation and getenv_bool("VALIDATE_EMAIL_MX", True)
    reply_sync_enabled = getenv_bool("REPLY_SYNC_ENABLED", True)
    warm_up_mode = getenv_bool("WARM_UP_MODE", True)
    default_language = normalize_language(getenv_str("OUTREACH_LANGUAGE", "uz"))
    gemini_enabled = getenv_bool("GEMINI_ENABLED", True)
    gemini_api_key = normalize_secret(getenv_str("GEMINI_API_KEY", ""))
    gemini_model = getenv_str("GEMINI_MODEL", "gemini-3.1-flash-lite-preview")
    google_sheets_enabled = getenv_bool("GOOGLE_SHEETS_ENABLED", False)
    preferred_email_transport = normalize_email_transport(getenv_str("EMAIL_TRANSPORT", "auto"))
    brevo_api_key = normalize_secret(getenv_str("BREVO_API_KEY", ""))
    gmail_api_client_id = normalize_secret(getenv_str("GMAIL_API_CLIENT_ID", ""))
    gmail_api_client_secret = normalize_secret(getenv_str("GMAIL_API_CLIENT_SECRET", ""))
    gmail_api_refresh_token = normalize_secret(getenv_str("GMAIL_API_REFRESH_TOKEN", ""))
    email_transport = resolve_email_transport(
        preferred_email_transport,
        brevo_api_key=brevo_api_key,
        gmail_api_client_id=gmail_api_client_id,
        gmail_api_refresh_token=gmail_api_refresh_token,
    )
    sender_email = getenv_str("EMAIL_SENDER_EMAIL", getenv_str("GMAIL_EMAIL", ""))

    if args.mode in {"scrape", "all"} and not seed_url:
        raise ValueError("SCRAPE_SEED_URL is missing. Set it in .env or pass --seed-url.")
    if delay_min_seconds < 0 or delay_max_seconds < 0 or delay_min_seconds > delay_max_seconds:
        raise ValueError("Email delay values are invalid.")
    if email_max_per_run < 1:
        raise ValueError("EMAIL_MAX_PER_RUN must be at least 1.")

    smtp = SMTPConfig(
        transport=email_transport,
        host=getenv_str("SMTP_HOST", "smtp.gmail.com"),
        port=getenv_int("SMTP_PORT", 465),
        username=getenv_str("GMAIL_EMAIL"),
        password=normalize_secret(getenv_str("GMAIL_APP_PASSWORD")),
        sender_email=sender_email,
        from_name=getenv_str("EMAIL_FROM_NAME", "Botfactory AI"),
        reply_to=getenv_str("EMAIL_REPLY_TO", getenv_str("GMAIL_EMAIL")),
        retry_limit=getenv_int("SMTP_RETRY_LIMIT", 3),
        api_key=brevo_api_key,
        api_url=getenv_str("BREVO_API_URL", "https://api.brevo.com/v3/smtp/email"),
        request_timeout_seconds=getenv_float("EMAIL_REQUEST_TIMEOUT_SECONDS", 30.0),
        sandbox_mode=getenv_bool("BREVO_SANDBOX_MODE", False),
        oauth_client_id=gmail_api_client_id,
        oauth_client_secret=gmail_api_client_secret,
        oauth_refresh_token=gmail_api_refresh_token,
        oauth_token_url=getenv_str("GMAIL_API_TOKEN_URL", "https://oauth2.googleapis.com/token"),
        gmail_api_send_url=getenv_str("GMAIL_API_SEND_URL", "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"),
    )
    brand_name = getenv_str("BOTFACTORY_BRAND_NAME", "Botfactory AI")
    brand = BrandConfig(
        brand_name=brand_name,
        reply_phrase=getenv_str("EMAIL_REPLY_PHRASE", "Shunchaki 'Ha' deb javob yozing"),
        unsubscribe_text=getenv_str(
            "EMAIL_UNSUBSCRIBE_TEXT",
            "Agar ushbu mavzu sizga qiziq bo'lmasa, 'Stop' deb javob bering.",
        ),
        custom_offer=getenv_str(
            "CUSTOM_SOLUTIONS_TEXT",
            "Bundan tashqari, agar sizga biznesingiz uchun maxsus AI yechim kerak bo'lsa "
            "(masalan: data analytics, ichki CRM integratsiyasi, hujjatlar bilan ishlovchi AI "
            "yoki xodimlar uchun AI-yordamchi), biz uni aynan sizning talablaringiz asosida "
            "noldan ishlab chiqib bera olamiz.",
        ),
        discovery_call_url=getenv_str("DISCOVERY_CALL_URL", ""),
        signature_name=getenv_str("EMAIL_SIGNATURE_NAME", brand_name),
        signature_role=getenv_str("EMAIL_SIGNATURE_ROLE", "AI Automation Agency"),
        signature_company=getenv_str("EMAIL_SIGNATURE_COMPANY", brand_name),
        signature_phone=getenv_str("EMAIL_SIGNATURE_PHONE", "+998901234567"),
        signature_website=getenv_str("EMAIL_SIGNATURE_WEBSITE", "https://botfactory.ai"),
    )
    sheets: GoogleSheetsConfig | None = None
    if google_sheets_enabled:
        spreadsheet_id = getenv_str("GOOGLE_SHEETS_SPREADSHEET_ID")
        worksheet_name = getenv_str("GOOGLE_SHEETS_WORKSHEET", "Leads")
        service_account_json_b64 = normalize_secret(getenv_raw("GOOGLE_SERVICE_ACCOUNT_JSON_B64", ""))
        service_account_file = collapse_whitespace(getenv_raw("GOOGLE_SERVICE_ACCOUNT_FILE", ""))
        if not spreadsheet_id:
            raise ValueError("GOOGLE_SHEETS_SPREADSHEET_ID is required when GOOGLE_SHEETS_ENABLED=true.")
        if not service_account_json_b64 and not service_account_file:
            raise ValueError(
                "GOOGLE_SERVICE_ACCOUNT_JSON_B64 or GOOGLE_SERVICE_ACCOUNT_FILE is required when GOOGLE_SHEETS_ENABLED=true."
            )
        sheets = GoogleSheetsConfig(
            spreadsheet_id=spreadsheet_id,
            worksheet_name=worksheet_name,
            service_account_json_b64=service_account_json_b64,
            service_account_file=service_account_file,
        )
    return AppConfig(
        mode=args.mode,
        seed_url=seed_url or None,
        leads_file=leads_file,
        template_file=template_file,
        logs_dir=logs_dir,
        blacklist_file=blacklist_file,
        warm_up_state_file=warm_up_state_file,
        scraper_output_dir=scraper_output_dir,
        max_companies=max_companies,
        max_pages_per_seed=max_pages_per_seed,
        delay_min_seconds=delay_min_seconds,
        delay_max_seconds=delay_max_seconds,
        email_max_per_run=email_max_per_run,
        filter_priority_categories=filter_priority_categories,
        validate_email_mx=validate_email_mx,
        reply_sync_enabled=reply_sync_enabled,
        imap_host=getenv_str("IMAP_HOST", "imap.gmail.com"),
        imap_port=getenv_int("IMAP_PORT", 993),
        imap_folder=getenv_str("IMAP_FOLDER", "INBOX"),
        unsubscribe_keywords=tuple(
            chunk
            for chunk in [
                collapse_whitespace(item).casefold()
                for item in getenv_str("UNSUBSCRIBE_KEYWORDS", "stop,unsubscribe,remove,bekor").split(",")
            ]
            if chunk
        ),
        warm_up_mode=warm_up_mode,
        warm_up_start_daily_limit=getenv_int("WARM_UP_START_DAILY_LIMIT", 5),
        warm_up_daily_increment=getenv_int("WARM_UP_DAILY_INCREMENT", 5),
        warm_up_max_daily_limit=getenv_int("WARM_UP_MAX_DAILY_LIMIT", 50),
        default_language=default_language,
        gemini_enabled=gemini_enabled,
        gemini_api_key=gemini_api_key,
        gemini_model=gemini_model,
        sheets=sheets,
        smtp=smtp,
        brand=brand,
    )


def normalize_email_transport(value: str) -> str:
    normalized = collapse_whitespace(value).casefold()
    if normalized in {"", "auto"}:
        return "auto"
    if normalized in {"gmail-api", "gmail_api", "gmailapi"}:
        return "gmail-api"
    if normalized in {"brevo", "brevo-api", "brevo_api"}:
        return "brevo"
    if normalized in {"smtp", "gmail"}:
        return "smtp"
    raise ValueError("EMAIL_TRANSPORT must be one of: auto, gmail-api, brevo, smtp.")


def resolve_email_transport(
    preferred: str,
    *,
    brevo_api_key: str,
    gmail_api_client_id: str,
    gmail_api_refresh_token: str,
) -> str:
    if preferred == "auto":
        if gmail_api_client_id and gmail_api_refresh_token:
            return "gmail-api"
        return "brevo" if brevo_api_key else "smtp"
    return preferred


def email_transport_label(config: SMTPConfig) -> str:
    if config.transport == "gmail-api":
        return "Gmail API"
    if config.transport == "brevo":
        return "Brevo API"
    return f"SMTP ({config.host}:{config.port})"


LANGUAGE_LABELS: dict[str, dict[str, str]] = {
    "uz": {
        "header_tagline": "Biznesingiz uchun aqlli texnologiyalar",
        "offer_label": "Siz uchun maxsus taklif:",
        "services_title": "Bizning xizmatlar",
        "service_ready_title": "Tayyor sun'iy intellekt agentlari",
        "service_ready_body": "Mijozlarga kecha-kunduz javob beruvchi, sotuvga yordam beruvchi va murojaatlarni qabul qiluvchi aqlli tizimlar.",
        "service_custom_title": "Maxsus sun'iy intellekt yechimlari",
        "meeting_label": "Tanishuv uchrashuvi uchun taklif:",
        "meeting_link_prefix": "Qulay vaqt tanlash:",
        "meeting_button": "Tanishuv uchrashuvini belgilash",
        "contact_button": "Biz bilan bog'laning",
        "cta_prompt": "Bepul konsultatsiya yoki namoyishni ko'rishni xohlaysizmi?",
        "mailto_subject": "Hamkorlik",
        "rights_text": "Barcha huquqlar himoyalangan.",
        "location_text": "Toshkent sh., O'zbekiston",
        "website_label": "Veb-saytimiz",
        "contact_label": "Aloqa",
        "custom_offer_title": "Maxsus sun'iy intellekt yechimlari",
        "closing_text": "Hurmat bilan",
    },
    "ru": {
        "header_tagline": "Умные технологии для вашего бизнеса",
        "offer_label": "Специальное предложение для вас:",
        "services_title": "Наши услуги",
        "service_ready_title": "Готовые интеллектуальные агенты",
        "service_ready_body": "Умные системы, которые круглосуточно отвечают клиентам, помогают продажам и принимают обращения.",
        "service_custom_title": "Индивидуальные решения на базе искусственного интеллекта",
        "meeting_label": "Предложение по ознакомительной встрече:",
        "meeting_link_prefix": "Выбрать удобное время:",
        "meeting_button": "Назначить ознакомительную встречу",
        "contact_button": "Связаться с нами",
        "cta_prompt": "Хотите получить бесплатную консультацию или демонстрацию?",
        "mailto_subject": "Сотрудничество",
        "rights_text": "Все права защищены.",
        "location_text": "Ташкент, Узбекистан",
        "website_label": "Наш сайт",
        "contact_label": "Контакты",
        "custom_offer_title": "Индивидуальные решения на базе искусственного интеллекта",
        "closing_text": "С уважением",
    },
}

CAMPAIGN_COPY: dict[str, dict[str, dict[str, dict[str, str]]]] = {
    "uz": {
        "general": {
            "A": {
                "subject": "{company_name} uchun operatsion xarajatlarni kamaytirish taklifi",
                "preheader": "Mijozlar bilan muloqotni kecha-kunduz avtomatlashtirish imkoniyati.",
                "headline": "Mijozlarga javob berish tezligini oshiring",
                "intro": "Biz {company_name} faoliyatini ko'rib chiqdik va mijozlar murojaatini tezroq qayta ishlash uchun aniq imkoniyatni ko'rdik.",
                "problem": "Ko'plab kompaniyalarda xodimlar bir xil savollarga javob berish va murojaatlarni saralashga ko'p vaqt sarflaydi.",
                "solution": "{brand_name} tizimi murojaatlarni qabul qiladi, ehtiyojni aniqlaydi va jamoangizga tayyor mijozlarni uzatadi.",
                "cta": "{reply_phrase}, sizga qisqa taqdimot va amaliy ssenariyni yuboramiz.",
            },
            "B": {
                "subject": "{company_name} uchun mijozlar bilan ishlashni avtomatlashtirish taklifi",
                "preheader": "Kamroq xarajat, tezroq javob va ko'proq natija.",
                "headline": "Murojaatlarni nazoratdan chiqarmang",
                "intro": "{company_name} kabi o'sayotgan bizneslarda har bir kechikkan javob yo'qolgan imkoniyatga aylanishi mumkin.",
                "problem": "Savdo va xizmat ko'rsatish sohalarida kunduzgi va tungi murojaatlar jamoaga ortiqcha bosim beradi.",
                "solution": "{brand_name} tizimi savollarni qabul qiladi, dastlabki saralashni bajaradi va menejerga tayyor ma'lumot uzatadi.",
                "cta": "{reply_phrase}, sizga mos yechimlarni birgalikda ko'rib chiqamiz.",
            },
        },
        "healthcare": {
            "A": {
                "subject": "{company_name} klinikasi uchun qabul jarayonini avtomatlashtirish taklifi",
                "preheader": "Bemor yozuvlari va navbatni tartibga solish bo'yicha yechim.",
                "headline": "Bemorlarni qabul qilishni soddalashtiring",
                "intro": "Biz tibbiyot markazlari uchun qo'ng'iroq va yozuv oqimini avtomatlashtiradigan yechimlar yaratamiz.",
                "problem": "Klinikalarda bemorlarning tez javob kutishi va navbatni qo'lda boshqarish ko'p vaqt oladi.",
                "solution": "{brand_name} yordamchisi yozuvlarni qabul qiladi, ko'p so'raladigan savollarga javob beradi va administrator yukini kamaytiradi.",
                "cta": "{reply_phrase}, sizga klinika uchun mos ssenariyni yuboramiz.",
            },
            "B": {
                "subject": "{company_name} uchun bemorlarni yozib qo'yishni avtomatlashtirish taklifi",
                "preheader": "Qo'ng'iroqlarni yo'qotmasdan qabulni tartibga soling.",
                "headline": "Bemor oqimini yo'qotmang",
                "intro": "{company_name} jamoasi uchun bemorlarni yozib qo'yish va navbatni boshqarishni avtomatlashtirish mumkin.",
                "problem": "Kech javob yoki tushib qolgan qo'ng'iroq klinika obro'si va natijasiga salbiy ta'sir qiladi.",
                "solution": "{brand_name} tizimi yozuvlarni qabul qiladi, savollarga javob beradi va qabul jarayonini tezlashtiradi.",
                "cta": "{reply_phrase}, sizga amaliy misollar va ulanish variantlarini yuboramiz.",
            },
        },
        "education": {
            "A": {
                "subject": "{company_name} uchun yangi o'quvchilar oqimini oshirish taklifi",
                "preheader": "Reklamadan kelgan so'rovlarga bir necha soniyada javob berish imkoniyati.",
                "headline": "Qiziqqan mijozlarni yo'qotmang",
                "intro": "O'quv markazlari uchun eng qimmat yo'qotishlardan biri bu qiziqqan foydalanuvchiga kech javob berishdir.",
                "problem": "Ko'p markazlarda reklama orqali kelgan so'rovlar kech qayta ishlanadi va yozilish foizi pasayadi.",
                "solution": "{brand_name} tizimi yangi so'rovni darhol kutib oladi, kurs bo'yicha ma'lumot beradi va yozib qo'yishga yordam beradi.",
                "cta": "{reply_phrase}, sizga markaz uchun mos ssenariyni yuboramiz.",
            },
            "B": {
                "subject": "{company_name} uchun o'quvchilarga xizmat ko'rsatishni tezlashtirish taklifi",
                "preheader": "So'rovdan yozilishgacha bo'lgan yo'lni soddalashtiring.",
                "headline": "Har bir murojaatni natijaga aylantiring",
                "intro": "{company_name} reklama va kiruvchi so'rovlardan maksimal foyda olish uchun tezkor birinchi javobga muhtoj bo'lishi mumkin.",
                "problem": "Agar so'rovlar kech qayta ishlansa, ota-onalar yoki talabalar boshqa markazni tanlab ketadi.",
                "solution": "{brand_name} tizimi kurslar, narxlar va jadval bo'yicha savollarga javob berib, menejerga faqat tayyor mijozni uzatadi.",
                "cta": "{reply_phrase}, sizga ishlaydigan aloqa ssenariysini yuboramiz.",
            },
        },
        "custom": {
            "A": {
                "subject": "{company_name} uchun maxsus sun'iy intellekt yechimlari",
                "preheader": "Biznes jarayonlarini sizga mos tarzda raqamlashtirish taklifi.",
                "headline": "Sizga mos, noldan qurilgan yechim",
                "intro": "Biz {company_name} faoliyatini samaraliroq qilish uchun tayyor vositalar bilan cheklanmaymiz.",
                "problem": "Ko'plab bizneslarda oddiy vosita yetarli bo'lmaydi va ichki jarayonlar uchun moslashtirilgan avtomatlashtirish kerak bo'ladi.",
                "solution": "{brand_name} jamoasi ichki tizimlar bilan ulanish, hujjatlar bilan ishlash va xodimlar uchun yordamchi vositalarni noldan quradi.",
                "cta": "{reply_phrase}, qaysi yo'nalish sizga qiziq ekanini aniqlab, tanishuv uchrashuvini belgilaymiz.",
            },
            "B": {
                "subject": "{company_name} uchun sun'iy intellekt va raqamlashtirish taklifi",
                "preheader": "Tayyor yechimlar ham, maxsus ishlab chiqish ham bir joyda.",
                "headline": "Tayyor vositalar ham, maxsus yechimlar ham",
                "intro": "Biz {company_name} uchun tayyor agentlar bilan birga, noodatiy ehtiyojlar uchun maxsus arxitektura ham taklif qilamiz.",
                "problem": "Murakkab ichki jarayonlar yoki hujjat oqimlari bo'lsa, oddiy vositalar ularni to'liq yopib bera olmaydi.",
                "solution": "{brand_name} jamoasi jarayonni tahlil qilib, mos ulanish va maxsus modulni ishlab chiqadi.",
                "cta": "{reply_phrase}, eng mos yo'nalishni birgalikda tanlaymiz.",
            },
        },
        "logistics": {
            "A": {
                "subject": "{company_name} uchun logistika jarayonlarini avtomatlashtirish taklifi",
                "preheader": "Buyurtma, yo'nalish va yuk holati bo'yicha murojaatlarni soddalashtiring.",
                "headline": "Logistika jarayonlarini tezroq boshqaring",
                "intro": "Logistika kompaniyalarida tezkor koordinatsiya va aniq javob operatsion samaradorlikka bevosita ta'sir qiladi.",
                "problem": "Xodimlar ko'p vaqtini takroriy savollar, yuk holati va yo'nalish bo'yicha qo'lda javob berishga sarflaydi.",
                "solution": "{brand_name} tizimi buyurtmalarni qabul qilish, holatni aytish va operator yukini kamaytirishga yordam beradi.",
                "cta": "{reply_phrase}, sizga logistika uchun mos ssenariyni yuboramiz.",
            },
            "B": {
                "subject": "{company_name} uchun yuk jarayonlarini soddalashtirish taklifi",
                "preheader": "Mijoz va operator orasidagi takroriy aloqalarni qisqartiring.",
                "headline": "Jarayonni kecha-kunduz nazoratda ushlang",
                "intro": "{company_name} uchun qo'ng'iroq, buyurtma va holat savollarini avtomatlashtirish orqali jamoa vaqtini tejash mumkin.",
                "problem": "Logistika jamoalarida aynan bir xil savollarga qayta-qayta javob berish mijoz tajribasini sekinlashtiradi.",
                "solution": "{brand_name} tizimi buyurtma oqimini saralaydi, mijozga tez javob beradi va ichki jamoaga tayyor ma'lumot uzatadi.",
                "cta": "{reply_phrase}, sizga qisqa ko'rsatma va foydalanish ssenariysini yuboramiz.",
            },
        },
    },
    "ru": {
        "general": {
            "A": {
                "subject": "Предложение по снижению операционных расходов для {company_name}",
                "preheader": "Возможность автоматизировать общение с клиентами круглосуточно.",
                "headline": "Ускорьте ответы клиентам",
                "intro": "Мы изучили деятельность {company_name} и увидели возможность быстрее обрабатывать обращения клиентов.",
                "problem": "Во многих компаниях сотрудники тратят много времени на одинаковые вопросы и первичную обработку обращений.",
                "solution": "{brand_name} принимает обращения, определяет потребность клиента и передает вашей команде уже подготовленные запросы.",
                "cta": "{reply_phrase}, и мы отправим короткую презентацию и практический сценарий.",
            },
            "B": {
                "subject": "Предложение по автоматизации работы с клиентами для {company_name}",
                "preheader": "Меньше затрат, быстрее ответ и больше результата.",
                "headline": "Не упускайте обращения",
                "intro": "Для растущего бизнеса, такого как {company_name}, каждый поздний ответ может означать потерянную возможность.",
                "problem": "В продажах и сервисе дневные и ночные обращения создают лишнюю нагрузку на команду.",
                "solution": "{brand_name} принимает обращения, выполняет первичную сортировку и передает менеджеру готовую информацию.",
                "cta": "{reply_phrase}, и мы обсудим подходящие именно вам решения.",
            },
        },
        "healthcare": {
            "A": {
                "subject": "Предложение по автоматизации приема для клиники {company_name}",
                "preheader": "Решение для записи пациентов и управления очередью.",
                "headline": "Упростите прием пациентов",
                "intro": "Мы создаем решения для медицинских центров, которые автоматизируют поток звонков и запись пациентов.",
                "problem": "В клиниках ожидание быстрого ответа и ручное управление очередью отнимают много времени.",
                "solution": "{brand_name} принимает записи, отвечает на частые вопросы и снижает нагрузку на администратора.",
                "cta": "{reply_phrase}, и мы отправим подходящий для вашей клиники сценарий.",
            },
            "B": {
                "subject": "Предложение по автоматизации записи пациентов для {company_name}",
                "preheader": "Организуйте прием без потери звонков.",
                "headline": "Не теряйте поток пациентов",
                "intro": "Для команды {company_name} можно автоматизировать запись пациентов и управление очередью.",
                "problem": "Поздний ответ или пропущенный звонок негативно влияют на репутацию и результат клиники.",
                "solution": "{brand_name} принимает записи, отвечает на вопросы и ускоряет процесс приема.",
                "cta": "{reply_phrase}, и мы пришлем практические примеры и варианты подключения.",
            },
        },
        "education": {
            "A": {
                "subject": "Предложение по увеличению потока учеников для {company_name}",
                "preheader": "Возможность отвечать на рекламные обращения за считанные секунды.",
                "headline": "Не теряйте заинтересованных клиентов",
                "intro": "Для учебных центров одна из самых дорогих потерь — это поздний ответ заинтересованному человеку.",
                "problem": "Во многих центрах заявки из рекламы обрабатываются поздно, и доля записавшихся снижается.",
                "solution": "{brand_name} сразу принимает новую заявку, дает информацию о курсах и помогает оформить запись.",
                "cta": "{reply_phrase}, и мы отправим подходящий для вашего центра сценарий.",
            },
            "B": {
                "subject": "Предложение по ускорению работы с учениками для {company_name}",
                "preheader": "Сделайте путь от обращения до записи проще.",
                "headline": "Преобразуйте каждое обращение в результат",
                "intro": "Чтобы получать максимум от рекламы и входящих обращений, {company_name} может быть важен быстрый первый ответ.",
                "problem": "Если обращения обрабатываются поздно, родители и ученики выбирают другой центр.",
                "solution": "{brand_name} отвечает на вопросы о курсах, ценах и расписании и передает менеджеру уже подготовленного клиента.",
                "cta": "{reply_phrase}, и мы отправим вам рабочий сценарий общения.",
            },
        },
        "custom": {
            "A": {
                "subject": "Индивидуальные решения на базе искусственного интеллекта для {company_name}",
                "preheader": "Предложение по цифровизации процессов под ваши задачи.",
                "headline": "Решение, созданное под вас с нуля",
                "intro": "Мы не ограничиваемся готовыми инструментами и подходим к задачам {company_name} индивидуально.",
                "problem": "Во многих компаниях стандартного инструмента недостаточно, и для внутренних процессов нужна точная автоматизация.",
                "solution": "{brand_name} создает с нуля решения для внутренних систем, работы с документами и помощников для сотрудников.",
                "cta": "{reply_phrase}, и мы определим интересующее вас направление и назначим ознакомительную встречу.",
            },
            "B": {
                "subject": "Предложение по искусственному интеллекту и цифровизации для {company_name}",
                "preheader": "Готовые решения и индивидуальная разработка в одном месте.",
                "headline": "И готовые инструменты, и индивидуальные решения",
                "intro": "Для {company_name} мы можем предложить как готовые интеллектуальные помощники, так и архитектуру под нестандартные задачи.",
                "problem": "Если внутри бизнеса есть сложные процессы или большой поток документов, стандартных инструментов часто недостаточно.",
                "solution": "{brand_name} анализирует процесс, подбирает подключения и разрабатывает индивидуальный модуль под вашу задачу.",
                "cta": "{reply_phrase}, и мы вместе выберем наиболее подходящее направление.",
            },
        },
        "logistics": {
            "A": {
                "subject": "Предложение по автоматизации логистических процессов для {company_name}",
                "preheader": "Упростите обращения по заказам, маршрутам и статусу груза.",
                "headline": "Ускорьте логистические процессы",
                "intro": "В логистике быстрая координация и точный ответ напрямую влияют на эффективность работы.",
                "problem": "Сотрудники тратят много времени на повторяющиеся вопросы о статусе груза, маршруте и заказе.",
                "solution": "{brand_name} помогает принимать заказы, сообщать статус и снижать нагрузку на операторов.",
                "cta": "{reply_phrase}, и мы отправим подходящий сценарий для логистики.",
            },
            "B": {
                "subject": "Предложение по упрощению грузовых процессов для {company_name}",
                "preheader": "Сократите повторяющиеся контакты между клиентом и оператором.",
                "headline": "Держите процессы под контролем круглосуточно",
                "intro": "Для {company_name} можно автоматизировать звонки, заказы и вопросы по статусу, экономя время команды.",
                "problem": "Когда логистическая команда постоянно отвечает на одни и те же вопросы, путь клиента замедляется.",
                "solution": "{brand_name} сортирует поток заказов, быстро отвечает клиенту и передает команде готовую информацию.",
                "cta": "{reply_phrase}, и мы пришлем краткий сценарий использования.",
            },
        },
    },
}


async def main_async(config: AppConfig, console: Console) -> None:
    console.print(
        Panel.fit(
            f"Mode: {config.mode}\n"
            f"Leads file: {config.leads_file}\n"
            f"Seed URL: {config.seed_url or '-'}\n"
            f"Priority filter: {'on' if config.filter_priority_categories else 'off'}\n"
            f"MX validation: {'on' if config.validate_email_mx else 'off'}\n"
            f"Warm-up mode: {'on' if config.warm_up_mode else 'off'}\n"
            f"Reply sync: {'on' if config.reply_sync_enabled else 'off'}\n"
            f"Email transport: {email_transport_label(config.smtp)}\n"
            f"Storage: {'Google Sheets + Excel backup' if config.sheets else 'Excel'}",
            title="Botfactory Lead Machine",
            border_style="cyan",
        )
    )

    if config.mode in {"scrape", "all"}:
        scrape_result = await run_scrape_phase(config, console)
        console.print(build_scrape_summary_table(scrape_result))

    if config.mode == "sync-replies":
        reply_result = await run_reply_sync_phase(config, console)
        if reply_result.error:
            console.print(f"[yellow]Reply sync warning:[/yellow] {reply_result.error}")
        else:
            console.print(
                f"[green]Reply sync[/green] matched={reply_result.matched_messages} "
                f"new_blacklist={reply_result.blacklisted_now} total_blacklist={reply_result.total_blacklisted}"
            )

    if config.mode in {"email", "all"}:
        send_result = await run_email_phase(config, console)
        console.print(build_send_summary_table(send_result))


async def run_scrape_phase(config: AppConfig, console: Console) -> ScrapePhaseResult:
    console.print("[bold cyan]Scrape phase started[/bold cyan]")
    settings = ScraperSettings(
        seed_urls=[config.seed_url or ""],
        max_companies=config.max_companies,
        max_pages_per_seed=config.max_pages_per_seed,
        output_dir=config.scraper_output_dir,
    )
    scraper = GoldenPagesScraper(settings=settings, console=console)
    summary = await asyncio.to_thread(scraper.run)
    scraped_df = pd.read_excel(summary.xlsx_path)
    lead_build = build_leads_dataframe(
        scraped_df,
        filter_priority_categories=config.filter_priority_categories,
        validate_email_mx=config.validate_email_mx,
        default_language=config.default_language,
    )
    merged_df, new_count, updated_count = merge_with_existing_leads(config.leads_file, lead_build.dataframe, config.sheets)
    save_leads_dataframe(merged_df, config.leads_file, config.sheets)
    result = ScrapePhaseResult(
        total_scraped_rows=len(scraped_df.index),
        rows_with_email=lead_build.rows_with_email,
        targeted_valid_rows=lead_build.targeted_valid_rows,
        skipped_priority_rows=lead_build.skipped_priority_rows,
        invalid_email_rows=lead_build.invalid_email_rows,
        new_leads_added=new_count,
        existing_leads_updated=updated_count,
        total_leads_in_file=len(merged_df.index),
        output_file=config.leads_file,
    )
    write_json_log(
        config.logs_dir,
        "scrape",
        {
            "seed_url": config.seed_url,
            "total_scraped_rows": result.total_scraped_rows,
            "rows_with_email": result.rows_with_email,
            "targeted_valid_rows": result.targeted_valid_rows,
            "skipped_priority_rows": result.skipped_priority_rows,
            "invalid_email_rows": result.invalid_email_rows,
            "new_leads_added": result.new_leads_added,
            "existing_leads_updated": result.existing_leads_updated,
            "total_leads_in_file": result.total_leads_in_file,
            "output_file": str(result.output_file),
        },
    )
    return result


async def run_reply_sync_phase(config: AppConfig, console: Console) -> ReplySyncResult:
    if not config.reply_sync_enabled:
        return ReplySyncResult(matched_messages=0, blacklisted_now=0, total_blacklisted=len(load_blacklist(config.blacklist_file)))

    console.print("[bold cyan]Reply sync started[/bold cyan]")
    result = await asyncio.to_thread(sync_reply_blacklist, config)
    write_json_log(
        config.logs_dir,
        "reply_sync",
        {
            "matched_messages": result.matched_messages,
            "blacklisted_now": result.blacklisted_now,
            "total_blacklisted": result.total_blacklisted,
            "error": result.error,
        },
    )
    return result


async def run_email_phase(config: AppConfig, console: Console) -> SendPhaseResult:
    validate_email_config(config)
    reply_result = await run_reply_sync_phase(config, console) if config.reply_sync_enabled else ReplySyncResult(0, 0, len(load_blacklist(config.blacklist_file)))
    leads_df = ensure_lead_columns(load_leads_dataframe(config.leads_file, config.sheets))
    blacklist = load_blacklist(config.blacklist_file)
    blacklisted_skipped = apply_blacklist_to_leads(leads_df, blacklist)
    score_series = pd.to_numeric(leads_df["Lead Score"], errors="coerce").fillna(0)
    leads_df["_lead_score_sort"] = score_series
    status_series = leads_df["Status"].fillna("").astype(str).str.strip().str.casefold()
    pending_mask = (leads_df["Email"].astype(str).str.strip() != "") & (~status_series.isin({"sent", "blacklisted"}))
    pending_frame = leads_df[pending_mask].sort_values(
        by=["_lead_score_sort", "Company Name", "Email"],
        ascending=[False, True, True],
    )
    pending_indexes = [int(index) for index in pending_frame.index.tolist()]
    pending_before = len(pending_indexes)
    skipped_sent = len(leads_df.index) - pending_before - blacklisted_skipped

    if not pending_indexes:
        console.print("[bold yellow]No pending emails found.[/bold yellow]")
        leads_df = leads_df.drop(columns="_lead_score_sort")
        save_leads_dataframe(leads_df, config.leads_file, config.sheets)
        result = SendPhaseResult(0, 0, 0, skipped_sent, blacklisted_skipped, 0, reply_result.blacklisted_now, config.leads_file)
        write_json_log(
            config.logs_dir,
            "email",
            {
                "pending_before": 0,
                "sent_now": 0,
                "failed_now": 0,
                "skipped_sent": skipped_sent,
                "blacklisted_skipped": blacklisted_skipped,
                "warm_up_remaining": 0,
                "reply_blacklisted_now": reply_result.blacklisted_now,
                "output_file": str(config.leads_file),
            },
        )
        return result

    allowed_to_send, warm_up_remaining = plan_warm_up_allowance(config)
    send_cap = min(config.email_max_per_run, allowed_to_send) if config.warm_up_mode else config.email_max_per_run
    if send_cap < 1:
        console.print("[bold yellow]Warm-up daily limit reached. No emails sent in this run.[/bold yellow]")
        leads_df = leads_df.drop(columns="_lead_score_sort")
        save_leads_dataframe(leads_df, config.leads_file, config.sheets)
        result = SendPhaseResult(
            pending_before=pending_before,
            sent_now=0,
            failed_now=0,
            skipped_sent=skipped_sent,
            blacklisted_skipped=blacklisted_skipped,
            warm_up_remaining=warm_up_remaining,
            reply_blacklisted_now=reply_result.blacklisted_now,
            output_file=config.leads_file,
        )
        write_json_log(
            config.logs_dir,
            "email",
            {
                "pending_before": pending_before,
                "sent_now": 0,
                "failed_now": 0,
                "skipped_sent": skipped_sent,
                "blacklisted_skipped": blacklisted_skipped,
                "warm_up_remaining": warm_up_remaining,
                "reply_blacklisted_now": reply_result.blacklisted_now,
                "output_file": str(config.leads_file),
            },
        )
        return result

    send_indexes = pending_indexes[:send_cap]
    template_text = load_email_template(config.template_file)
    sent_now = 0
    failed_now = 0

    console.print(f"[bold cyan]Email phase started[/bold cyan] Pending: {pending_before}")
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task_id = progress.add_task("Sending outreach", total=len(send_indexes))

        for offset, row_index in enumerate(send_indexes):
            row = leads_df.loc[row_index]
            draft = compose_outreach_email(config, row, template_text)
            progress.update(task_id, description=f"Sending to {row['Email']}")
            success, error_message = await asyncio.to_thread(
                send_email_with_backoff,
                config.smtp,
                str(row["Email"]).strip(),
                draft.subject,
                draft.html_body,
                draft.plain_text_body,
            )

            now_iso = datetime.now().isoformat(timespec="seconds")
            leads_df.at[row_index, "LastContacted"] = now_iso
            leads_df.at[row_index, "Template Used"] = draft.template_used
            if success:
                leads_df.at[row_index, "Status"] = "Sent"
                leads_df.at[row_index, "Sent At"] = now_iso
                leads_df.at[row_index, "Last Error"] = ""
                sent_now += 1
                console.print(f"[green]Sent[/green] {row['Email']} via {email_transport_label(config.smtp)}")
            else:
                leads_df.at[row_index, "Status"] = "Error"
                leads_df.at[row_index, "Last Error"] = truncate_error(error_message)
                failed_now += 1
                console.print(
                    f"[red]Failed[/red] {row['Email']} via {email_transport_label(config.smtp)}: "
                    f"{truncate_error(error_message, limit=160)}"
                )

            save_leads_dataframe(leads_df, config.leads_file, config.sheets)
            progress.advance(task_id)

            if offset < len(send_indexes) - 1:
                await asyncio.sleep(random.uniform(config.delay_min_seconds, config.delay_max_seconds))

    leads_df = leads_df.drop(columns="_lead_score_sort")
    save_leads_dataframe(leads_df, config.leads_file, config.sheets)
    if config.warm_up_mode and sent_now:
        record_warm_up_progress(config, sent_now)
    result = SendPhaseResult(
        pending_before=pending_before,
        sent_now=sent_now,
        failed_now=failed_now,
        skipped_sent=skipped_sent,
        blacklisted_skipped=blacklisted_skipped,
        warm_up_remaining=max(warm_up_remaining - sent_now, 0),
        reply_blacklisted_now=reply_result.blacklisted_now,
        output_file=config.leads_file,
    )
    write_json_log(
        config.logs_dir,
        "email",
        {
            "pending_before": result.pending_before,
            "sent_now": result.sent_now,
            "failed_now": result.failed_now,
            "skipped_sent": result.skipped_sent,
            "blacklisted_skipped": result.blacklisted_skipped,
            "warm_up_remaining": result.warm_up_remaining,
            "reply_blacklisted_now": result.reply_blacklisted_now,
            "output_file": str(result.output_file),
        },
    )
    return result


def build_leads_dataframe(
    scraped_df: pd.DataFrame,
    *,
    filter_priority_categories: bool,
    validate_email_mx: bool,
    default_language: str,
) -> LeadBuildResult:
    rows: list[dict[str, str]] = []
    rows_with_email = 0
    skipped_priority_rows = 0
    invalid_email_rows = 0

    for row in scraped_df.fillna("").to_dict(orient="records"):
        emails = normalize_pipe_list(row.get("emails", ""), emails_only=True)
        if not emails:
            continue

        rows_with_email += len(emails)
        company_name = collapse_whitespace(str(row.get("company_name", "")))
        activity_types = " | ".join(normalize_pipe_list(row.get("activity_types", "")))
        category = infer_category(company_name, activity_types)
        rating_value = safe_float(row.get("rating_value", 0.0))
        rating_count = safe_int(row.get("rating_count", 0))
        if filter_priority_categories and category == "Other":
            skipped_priority_rows += len(emails)
            continue

        phone = " | ".join(normalize_pipe_list(row.get("phones", "")))
        captured_at = datetime.now().isoformat(timespec="seconds")

        for email in emails:
            validation_status = validate_email_address(email, validate_email_mx=validate_email_mx)
            if not is_usable_email_validation(validation_status):
                invalid_email_rows += 1
                continue
            lead_score = calculate_lead_score(
                website=collapse_whitespace(str(row.get("website", ""))),
                phone=phone,
                rating_value=rating_value,
                rating_count=rating_count,
            )

            rows.append(
                {
                    "Company ID": collapse_whitespace(str(row.get("company_id", ""))),
                    "Company Name": company_name,
                    "Email": email,
                    "Phone": phone,
                    "Category": category,
                    "Activity Types": activity_types,
                    "Website": collapse_whitespace(str(row.get("website", ""))),
                    "Source URL": collapse_whitespace(str(row.get("source_url", ""))),
                    "Source Listing URL": collapse_whitespace(str(row.get("source_listing_url", ""))),
                    "Lead Captured At": captured_at,
                    "Validation Status": validation_status,
                    "Lead Score": str(lead_score),
                    "Rating Value": str(rating_value),
                    "Rating Count": str(rating_count),
                    "Language": default_language,
                    "Status": "New",
                    "LastContacted": "None",
                    "Sent At": "",
                    "Last Error": "",
                    "Template Used": "",
                }
            )

    dataframe = pd.DataFrame(rows, columns=LEAD_COLUMNS)
    if dataframe.empty:
        return LeadBuildResult(
            dataframe=ensure_lead_columns(dataframe),
            rows_with_email=rows_with_email,
            targeted_valid_rows=0,
            skipped_priority_rows=skipped_priority_rows,
            invalid_email_rows=invalid_email_rows,
        )

    dataframe["_email_key"] = dataframe["Email"].map(email_key)
    dataframe = dataframe[dataframe["_email_key"] != ""].drop_duplicates("_email_key", keep="first")
    dataframe = dataframe.drop(columns="_email_key").reset_index(drop=True)
    return LeadBuildResult(
        dataframe=ensure_lead_columns(dataframe),
        rows_with_email=rows_with_email,
        targeted_valid_rows=len(dataframe.index),
        skipped_priority_rows=skipped_priority_rows,
        invalid_email_rows=invalid_email_rows,
    )


def merge_with_existing_leads(
    leads_file: Path,
    new_df: pd.DataFrame,
    sheets_config: GoogleSheetsConfig | None = None,
) -> tuple[pd.DataFrame, int, int]:
    existing_df = load_leads_dataframe(leads_file, sheets_config)
    merged_rows: dict[str, dict[str, str]] = {}

    for row in existing_df.fillna("").to_dict(orient="records"):
        key = email_key(row.get("Email", ""))
        if key:
            merged_rows[key] = normalize_lead_record(row)

    new_count = 0
    updated_count = 0
    for row in new_df.fillna("").to_dict(orient="records"):
        normalized = normalize_lead_record(row)
        key = email_key(normalized.get("Email", ""))
        if not key:
            continue

        if key not in merged_rows:
            merged_rows[key] = normalized
            new_count += 1
            continue

        existing = merged_rows[key]
        changed = False
        for column in LEAD_COLUMNS:
            if column in {"Status", "LastContacted", "Sent At", "Last Error", "Template Used"}:
                continue
            incoming = collapse_whitespace(str(normalized.get(column, "")))
            if incoming and incoming != collapse_whitespace(str(existing.get(column, ""))):
                existing[column] = incoming
                changed = True

        if not collapse_whitespace(str(existing.get("Status", ""))):
            existing["Status"] = "New"
        if not collapse_whitespace(str(existing.get("LastContacted", ""))):
            existing["LastContacted"] = "None"
        merged_rows[key] = existing
        if changed:
            updated_count += 1

    final_df = pd.DataFrame(list(merged_rows.values()), columns=LEAD_COLUMNS)
    if final_df.empty:
        return ensure_lead_columns(final_df), new_count, updated_count

    final_df["_sent_rank"] = final_df["Status"].fillna("").astype(str).str.casefold().eq("sent").astype(int)
    final_df["_lead_score_sort"] = pd.to_numeric(final_df["Lead Score"], errors="coerce").fillna(0)
    final_df = (
        final_df.sort_values(
            ["_sent_rank", "_lead_score_sort", "Category", "Company Name", "Email"],
            ascending=[True, False, True, True, True],
        )
        .drop(columns=["_sent_rank", "_lead_score_sort"])
        .reset_index(drop=True)
    )
    return ensure_lead_columns(final_df), new_count, updated_count


def get_google_sheets_worksheet(sheets_config: GoogleSheetsConfig) -> Any:
    cache_key = f"{sheets_config.spreadsheet_id}:{sheets_config.worksheet_name}"
    cached_worksheet = _GOOGLE_SHEETS_WORKSHEETS.get(cache_key)
    if cached_worksheet is not None:
        return cached_worksheet
    if gspread is None or GoogleServiceAccountCredentials is None:
        raise RuntimeError("Google Sheets support requires gspread and google-auth.")

    service_account_info = load_google_service_account_info(sheets_config)
    credentials = GoogleServiceAccountCredentials.from_service_account_info(
        service_account_info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    client = gspread.authorize(credentials)
    spreadsheet = client.open_by_key(sheets_config.spreadsheet_id)
    try:
        worksheet = spreadsheet.worksheet(sheets_config.worksheet_name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=sheets_config.worksheet_name,
            rows=max(2000, len(LEAD_COLUMNS) * 100),
            cols=max(24, len(LEAD_COLUMNS) + 4),
        )
    _GOOGLE_SHEETS_WORKSHEETS[cache_key] = worksheet
    return worksheet


def load_google_service_account_info(sheets_config: GoogleSheetsConfig) -> dict[str, Any]:
    if sheets_config.service_account_file:
        return json.loads(Path(sheets_config.service_account_file).read_text(encoding="utf-8"))
    if sheets_config.service_account_json_b64:
        decoded_json = base64.b64decode(sheets_config.service_account_json_b64.encode("utf-8")).decode("utf-8")
        return json.loads(decoded_json)
    raise RuntimeError("Google Sheets service account credentials are missing.")


def load_leads_dataframe_from_google_sheets(sheets_config: GoogleSheetsConfig) -> pd.DataFrame | None:
    try:
        worksheet = get_google_sheets_worksheet(sheets_config)
        values = worksheet.get_all_values()
    except Exception:
        return None

    if not values:
        return pd.DataFrame(columns=LEAD_COLUMNS)

    header = [collapse_whitespace(str(item)) for item in values[0]]
    data_rows = values[1:]
    if not any(header):
        return pd.DataFrame(columns=LEAD_COLUMNS)

    normalized_rows: list[dict[str, str]] = []
    column_count = len(header)
    for row in data_rows:
        padded_row = list(row) + [""] * max(column_count - len(row), 0)
        normalized_rows.append({header[index]: padded_row[index] for index in range(column_count) if header[index]})
    return pd.DataFrame(normalized_rows)


def save_leads_dataframe_to_google_sheets(dataframe: pd.DataFrame, sheets_config: GoogleSheetsConfig) -> None:
    worksheet = get_google_sheets_worksheet(sheets_config)
    normalized_frame = ensure_lead_columns(dataframe).fillna("").astype(str)
    values = [list(normalized_frame.columns)] + normalized_frame.values.tolist()
    worksheet.clear()
    worksheet.update("A1", values, value_input_option="USER_ENTERED")


def load_leads_dataframe(leads_file: Path, sheets_config: GoogleSheetsConfig | None = None) -> pd.DataFrame:
    if sheets_config:
        sheets_frame = load_leads_dataframe_from_google_sheets(sheets_config)
        if sheets_frame is not None:
            return ensure_lead_columns(sheets_frame)
    if not leads_file.exists():
        return ensure_lead_columns(pd.DataFrame(columns=LEAD_COLUMNS))
    return ensure_lead_columns(pd.read_excel(leads_file, keep_default_na=False))


def save_leads_dataframe(
    dataframe: pd.DataFrame,
    leads_file: Path,
    sheets_config: GoogleSheetsConfig | None = None,
) -> None:
    leads_file.parent.mkdir(parents=True, exist_ok=True)
    normalized_frame = ensure_lead_columns(dataframe)
    normalized_frame.to_excel(leads_file, index=False)
    if sheets_config:
        save_leads_dataframe_to_google_sheets(normalized_frame, sheets_config)


def ensure_lead_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    frame = dataframe.copy()
    for column in LEAD_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    frame["Status"] = frame["Status"].replace("", "New").fillna("New")
    frame["LastContacted"] = frame["LastContacted"].replace("", "None").fillna("None")
    frame["Lead Score"] = frame["Lead Score"].replace("", "0").fillna("0")
    frame["Rating Value"] = frame["Rating Value"].replace("", "0").fillna("0")
    frame["Rating Count"] = frame["Rating Count"].replace("", "0").fillna("0")
    frame["Language"] = frame["Language"].replace("", "uz").fillna("uz")
    return frame[LEAD_COLUMNS]


def normalize_lead_record(row: dict[str, Any]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for column in LEAD_COLUMNS:
        normalized[column] = collapse_whitespace(str(row.get(column, "")))
    normalized["Email"] = normalized["Email"].lower()
    normalized["Status"] = normalized["Status"] or "New"
    normalized["LastContacted"] = normalized["LastContacted"] or "None"
    return normalized


def normalize_pipe_list(value: Any, *, emails_only: bool = False) -> list[str]:
    text = collapse_whitespace(str(value))
    if not text:
        return []

    seen: set[str] = set()
    items: list[str] = []
    for piece in text.split("|"):
        candidate = collapse_whitespace(piece).strip(" ,;")
        if not candidate:
            continue
        if emails_only and not EMAIL_RE.match(candidate):
            continue
        key = candidate.casefold()
        if key in seen:
            continue
        seen.add(key)
        items.append(candidate)
    return items


def email_key(email: Any) -> str:
    candidate = collapse_whitespace(str(email)).lower()
    if not candidate or not EMAIL_RE.match(candidate):
        return ""
    return candidate


def infer_category(company_name: str, activity_types: str) -> str:
    haystack = f"{company_name} {activity_types}".casefold()
    if contains_keyword(haystack, HEALTHCARE_KEYWORDS):
        return "Tibbiyot"
    if contains_keyword(haystack, EDUCATION_KEYWORDS):
        return "O'quv markazi"
    if contains_keyword(haystack, LOGISTICS_KEYWORDS):
        return "Logistika"
    if contains_keyword(haystack, BUSINESS_KEYWORDS):
        return "General Business"
    return "Other"


def contains_keyword(haystack: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword.casefold() in haystack for keyword in keywords)


def calculate_lead_score(
    *,
    website: str,
    phone: str,
    rating_value: float,
    rating_count: int,
) -> int:
    score = 0
    if website:
        score += 10
    if has_landline_phone(phone):
        score += 5
    if rating_value >= 4.5 and rating_count >= 2:
        score += 15
    return score


def has_landline_phone(phone: str) -> bool:
    mobile_prefixes = {"33", "50", "55", "77", "88", "90", "91", "93", "94", "95", "97", "98", "99"}
    for raw_phone in normalize_pipe_list(phone):
        digits = "".join(character for character in raw_phone if character.isdigit())
        if digits.startswith("998"):
            digits = digits[3:]
        if len(digits) < 2:
            continue
        if digits[:2] not in mobile_prefixes:
            return True
    return False


def safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def safe_int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def validate_email_address(email: str, *, validate_email_mx: bool) -> str:
    candidate = collapse_whitespace(email).lower()
    if not EMAIL_RE.match(candidate):
        return "invalid-syntax"
    if not validate_email_mx:
        return "valid-syntax"
    if dns is None:
        return "mx-unchecked"

    domain = candidate.rsplit("@", 1)[-1]
    try:
        answers = dns.resolver.resolve(domain, "MX")
        return "valid-mx" if len(answers) > 0 else "no-mx"
    except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer):
        return "no-mx"
    except (dns.resolver.NoNameservers, dns.resolver.LifetimeTimeout):
        return "mx-unchecked"
    except Exception:
        return "mx-unchecked"


def is_usable_email_validation(validation_status: str) -> bool:
    return validation_status in {"valid-mx", "valid-syntax", "mx-unchecked"}


def validate_email_config(config: AppConfig) -> None:
    missing = []
    if config.smtp.transport == "gmail-api":
        if not config.smtp.username:
            missing.append("GMAIL_EMAIL")
        if not config.smtp.oauth_client_id:
            missing.append("GMAIL_API_CLIENT_ID")
        if not config.smtp.oauth_client_secret:
            missing.append("GMAIL_API_CLIENT_SECRET")
        if not config.smtp.oauth_refresh_token:
            missing.append("GMAIL_API_REFRESH_TOKEN")
    elif config.smtp.transport == "brevo":
        if not config.smtp.api_key:
            missing.append("BREVO_API_KEY")
        if not config.smtp.sender_email:
            missing.append("EMAIL_SENDER_EMAIL")
    else:
        if not config.smtp.username:
            missing.append("GMAIL_EMAIL")
        if not config.smtp.password:
            missing.append("GMAIL_APP_PASSWORD")
    if missing:
        raise ValueError(f"Missing required email settings: {', '.join(missing)}")
    if (
        config.reply_sync_enabled
        and (not config.smtp.username or not config.smtp.password)
    ):
        raise ValueError("Reply sync requires GMAIL_EMAIL and GMAIL_APP_PASSWORD for IMAP access.")
    if (
        config.reply_sync_enabled
        and config.imap_host.casefold() == "imap.gmail.com"
        and len(config.smtp.password) != 16
    ):
        raise ValueError(
            "Reply sync with Gmail IMAP requires GMAIL_APP_PASSWORD to be a 16-character Gmail App Password."
        )
    if (
        config.smtp.transport == "smtp"
        and config.smtp.host.casefold() == "smtp.gmail.com"
        and len(config.smtp.password) != 16
    ):
        raise ValueError(
            "GMAIL_APP_PASSWORD must be a 16-character Gmail App Password. "
            "Enable Google 2-Step Verification, create an App Password, and paste it into .env."
        )


def load_email_template(template_file: Path) -> str:
    if template_file.exists():
        return template_file.read_text(encoding="utf-8")
    return DEFAULT_HTML_TEMPLATE


def load_blacklist(blacklist_file: Path) -> dict[str, dict[str, str]]:
    if not blacklist_file.exists():
        return {}
    try:
        payload = json.loads(blacklist_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    blacklist: dict[str, dict[str, str]] = {}
    for email_address, meta in payload.items():
        key = email_key(email_address)
        if not key or not isinstance(meta, dict):
            continue
        blacklist[key] = {
            "reason": collapse_whitespace(str(meta.get("reason", ""))) or "manual",
            "detected_at": collapse_whitespace(str(meta.get("detected_at", ""))),
            "source": collapse_whitespace(str(meta.get("source", ""))),
        }
    return blacklist


def save_blacklist(blacklist_file: Path, blacklist: dict[str, dict[str, str]]) -> None:
    blacklist_file.parent.mkdir(parents=True, exist_ok=True)
    blacklist_file.write_text(
        json.dumps(blacklist, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def apply_blacklist_to_leads(leads_df: pd.DataFrame, blacklist: dict[str, dict[str, str]]) -> int:
    blacklisted_count = 0
    for row_index, row in leads_df.iterrows():
        key = email_key(row.get("Email", ""))
        if not key or key not in blacklist:
            continue
        if str(leads_df.at[row_index, "Status"]).strip().casefold() == "sent":
            continue
        leads_df.at[row_index, "Status"] = "Blacklisted"
        leads_df.at[row_index, "Last Error"] = f"Blacklisted: {blacklist[key].get('reason', 'manual')}"
        blacklisted_count += 1
    return blacklisted_count


def sync_reply_blacklist(config: AppConfig) -> ReplySyncResult:
    blacklist = load_blacklist(config.blacklist_file)
    known_lead_emails = set()
    if config.sheets or config.leads_file.exists():
        leads_df = load_leads_dataframe(config.leads_file, config.sheets)
        known_lead_emails = {
            email_key(value)
            for value in leads_df["Email"].tolist()
            if email_key(value)
        }
    state_path = config.logs_dir / "reply_sync_state.json"
    state = load_json_data(state_path, default={"last_uid": 0})
    state_exists = state_path.exists()
    last_uid = safe_int(state.get("last_uid", 0))
    matched_messages = 0
    blacklisted_now = 0

    try:
        with imaplib.IMAP4_SSL(config.imap_host, config.imap_port) as mailbox:
            mailbox.login(config.smtp.username, config.smtp.password)
            mailbox.select(config.imap_folder)
            status, data = mailbox.uid("search", None, "ALL")
            if status != "OK":
                return ReplySyncResult(0, 0, len(blacklist), "Could not search IMAP inbox.")

            uid_values = [
                chunk
                for chunk in (data[0] or b"").decode().split()
                if chunk.isdigit() and int(chunk) > last_uid
            ]
            if not state_exists and last_uid == 0:
                all_uid_values = [int(chunk) for chunk in (data[0] or b"").decode().split() if chunk.isdigit()]
                state["last_uid"] = max(all_uid_values, default=0)
                write_json_data(state_path, state)
                return ReplySyncResult(
                    0,
                    0,
                    len(blacklist),
                    "Reply sync initialized. Only future replies will be tracked.",
                )
            max_uid = last_uid
            for uid_text in uid_values:
                uid_value = int(uid_text)
                fetch_status, fetch_data = mailbox.uid("fetch", uid_text, "(RFC822)")
                max_uid = max(max_uid, uid_value)
                if fetch_status != "OK" or not fetch_data:
                    continue

                raw_parts = [part[1] for part in fetch_data if isinstance(part, tuple) and len(part) > 1]
                if not raw_parts:
                    continue
                message = message_from_bytes(raw_parts[0])
                sender_email = email_key(parseaddr(message.get("From", ""))[1])
                if not sender_email or (known_lead_emails and sender_email not in known_lead_emails):
                    continue

                combined_text = " ".join(
                    [
                        collapse_whitespace(str(message.get("Subject", ""))),
                        extract_message_text(message),
                    ]
                ).casefold()
                if not contains_unsubscribe_keyword(combined_text, config.unsubscribe_keywords):
                    continue

                matched_messages += 1
                if sender_email not in blacklist:
                    blacklist[sender_email] = {
                        "reason": "reply-stop",
                        "detected_at": datetime.now().isoformat(timespec="seconds"),
                        "source": "imap-reply",
                    }
                    blacklisted_now += 1

            state["last_uid"] = max_uid
            write_json_data(state_path, state)
            save_blacklist(config.blacklist_file, blacklist)
            return ReplySyncResult(matched_messages, blacklisted_now, len(blacklist))
    except Exception as exc:
        return ReplySyncResult(matched_messages, blacklisted_now, len(blacklist), str(exc))


def extract_message_text(message: Any) -> str:
    payloads: list[str] = []
    if message.is_multipart():
        for part in message.walk():
            content_type = part.get_content_type()
            if content_type not in {"text/plain", "text/html"}:
                continue
            charset = part.get_content_charset() or "utf-8"
            try:
                text = part.get_payload(decode=True).decode(charset, errors="ignore")
            except Exception:
                continue
            payloads.append(strip_html(text) if content_type == "text/html" else collapse_whitespace(text))
    else:
        charset = message.get_content_charset() or "utf-8"
        try:
            text = message.get_payload(decode=True).decode(charset, errors="ignore")
        except Exception:
            text = ""
        payloads.append(strip_html(text) if message.get_content_type() == "text/html" else collapse_whitespace(text))
    return collapse_whitespace(" ".join(payloads))


def strip_html(value: str) -> str:
    return collapse_whitespace(re.sub(r"<[^>]+>", " ", value))


def contains_unsubscribe_keyword(body_text: str, keywords: Sequence[str]) -> bool:
    normalized = body_text.casefold()
    return any(keyword in normalized for keyword in keywords if keyword)


def plan_warm_up_allowance(config: AppConfig) -> tuple[int, int]:
    if not config.warm_up_mode:
        return config.email_max_per_run, config.email_max_per_run

    state = load_json_data(
        config.warm_up_state_file,
        default={
            "start_date": datetime.now().date().isoformat(),
            "sent_counts": {},
        },
    )
    start_date_raw = collapse_whitespace(str(state.get("start_date", ""))) or datetime.now().date().isoformat()
    try:
        start_date = datetime.fromisoformat(start_date_raw).date()
    except ValueError:
        start_date = datetime.now().date()
        state["start_date"] = start_date.isoformat()

    days_elapsed = max((datetime.now().date() - start_date).days, 0)
    daily_limit = min(
        config.warm_up_start_daily_limit + (days_elapsed * config.warm_up_daily_increment),
        config.warm_up_max_daily_limit,
    )
    sent_counts = state.get("sent_counts", {})
    if not isinstance(sent_counts, dict):
        sent_counts = {}
        state["sent_counts"] = sent_counts
    today_key = datetime.now().date().isoformat()
    sent_today = safe_int(sent_counts.get(today_key, 0))
    write_json_data(config.warm_up_state_file, state)
    return max(daily_limit - sent_today, 0), max(daily_limit - sent_today, 0)


def record_warm_up_progress(config: AppConfig, sent_now: int) -> None:
    state = load_json_data(
        config.warm_up_state_file,
        default={
            "start_date": datetime.now().date().isoformat(),
            "sent_counts": {},
        },
    )
    sent_counts = state.get("sent_counts", {})
    if not isinstance(sent_counts, dict):
        sent_counts = {}
        state["sent_counts"] = sent_counts
    today_key = datetime.now().date().isoformat()
    sent_counts[today_key] = safe_int(sent_counts.get(today_key, 0)) + sent_now
    write_json_data(config.warm_up_state_file, state)


def write_json_log(logs_dir: Path, log_type: str, payload: dict[str, Any]) -> Path:
    logs_dir.mkdir(parents=True, exist_ok=True)
    path = logs_dir / f"{log_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    output = {
        "log_type": log_type,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        **payload,
    }
    path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def load_json_data(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return dict(default)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return dict(default)
    if not isinstance(payload, dict):
        return dict(default)
    merged = dict(default)
    merged.update(payload)
    return merged


def write_json_data(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def get_gemini_client(api_key: str) -> Any | None:
    if not api_key or google_genai is None:
        return None
    cached_client = _GEMINI_CLIENTS.get(api_key)
    if cached_client is not None:
        return cached_client
    try:
        client = google_genai.Client(api_key=api_key)
    except Exception:
        return None
    _GEMINI_CLIENTS[api_key] = client
    return client


def build_gemini_prompt(
    company_name: str,
    category: str,
    description: str,
    language: str,
    reply_phrase: str,
) -> str:
    description_text = description or "Faoliyati haqida qo'shimcha tavsif ko'rsatilmagan."
    if language == "ru":
        description_text = description or "Дополнительное описание деятельности не указано."
        return (
            "Ты опытный B2B-маркетолог агентства Botfactory AI.\n"
            f"Компания: {company_name}\n"
            f"Категория: {category}\n"
            f"Описание деятельности: {description_text}\n\n"
            "Задача: напиши короткий персонализированный текст письма от имени Botfactory AI.\n"
            "Требования:\n"
            "- Язык: русский.\n"
            "- Объем: 4-5 коротких предложений.\n"
            "- Тон: профессиональный, теплый и уверенный.\n"
            "- Упомяни вероятную проблему компании в ее сфере.\n"
            "- Покажи, что у нас есть как готовые интеллектуальные агенты, так и индивидуальные решения под бизнес.\n"
            "- Не добавляй приветствие и подпись, они уже есть в шаблоне письма.\n"
            f"- Заверши мягким призывом ответить на письмо: {reply_phrase}.\n"
            "- Не пиши тему письма, заголовки, списки, markdown и кавычки вокруг ответа.\n"
            "- Не используй англоязычные рекламные штампы без необходимости.\n"
        )
    return (
        "Siz Botfactory AI agentligining tajribali B2B marketing mutaxassisisiz.\n"
        f"Kompaniya: {company_name}\n"
        f"Kategoriya: {category}\n"
        f"Faoliyati: {description_text}\n\n"
        "Vazifa: Botfactory AI nomidan ushbu kompaniya uchun qisqa va shaxsiylashtirilgan outreach matni yozing.\n"
        "Talablar:\n"
        "- Til: o'zbek tili.\n"
        "- Uzunlik: 4-5 ta qisqa gap.\n"
        "- Ohang: professional, samimiy va ishonchli.\n"
        "- Kompaniyaning o'z sohasidagi ehtimoliy muammosini tilga oling.\n"
        "- Bizda tayyor sun'iy intellekt agentlari ham, biznesga mos noldan quriladigan maxsus yechimlar ham borligini ko'rsating.\n"
        "- Salomlashuv va imzo yozmang, ular email shablonida allaqachon bor.\n"
        f"- Yakunda yumshoq CTA bo'lsin: {reply_phrase}.\n"
        "- Subject, sarlavha, ro'yxat, markdown yoki qo'shtirnoq yozmang.\n"
        "- Keraksiz inglizcha reklama iboralarini ishlatmang.\n"
    )


def extract_gemini_text(response: Any) -> str:
    text = collapse_whitespace(str(getattr(response, "text", "") or ""))
    if text:
        return text
    for candidate in getattr(response, "candidates", []) or []:
        content = getattr(candidate, "content", None)
        for part in getattr(content, "parts", []) or []:
            part_text = collapse_whitespace(str(getattr(part, "text", "") or ""))
            if part_text:
                return part_text
    return ""


def clean_ai_outreach_text(text: str) -> str:
    cleaned = text.strip()
    cleaned = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    cleaned = cleaned.strip().strip("\"' ")
    cleaned = re.sub(r"^(matn|xat|tekst|текст)\s*:\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"^(assalomu\s+alaykum[^.!?]*[.!?]\s*|zdravstvuyte[^.!?]*[.!?]\s*|здравствуйте[^.!?]*[.!?]\s*)",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    return collapse_whitespace(cleaned)


def generate_ai_outreach(
    config: AppConfig,
    company_name: str,
    category: str,
    description: str,
    language: str,
) -> str | None:
    if not config.gemini_enabled or not config.gemini_api_key:
        return None
    client = get_gemini_client(config.gemini_api_key)
    if client is None:
        return None
    prompt = build_gemini_prompt(
        company_name=company_name,
        category=category,
        description=description,
        language=language,
        reply_phrase=reply_phrase_for_language(config, language),
    )
    try:
        generation_config = (
            google_genai_types.GenerateContentConfig(
                temperature=0.7,
                top_p=0.95,
                top_k=40,
                max_output_tokens=300,
            )
            if google_genai_types is not None
            else None
        )
        response = client.models.generate_content(
            model=config.gemini_model,
            contents=prompt,
            config=generation_config,
        )
    except Exception:
        return None
    text = clean_ai_outreach_text(extract_gemini_text(response))
    return text or None


def contact_email_for_outreach(config: AppConfig) -> str:
    return (
        collapse_whitespace(config.smtp.reply_to)
        or collapse_whitespace(config.smtp.username)
        or collapse_whitespace(config.smtp.sender_email)
    )


def compose_outreach_email(config: AppConfig, row: pd.Series, template_text: str) -> EmailDraft:
    company_name = collapse_whitespace(str(row.get("Company Name", ""))) or "hamkor"
    category = collapse_whitespace(str(row.get("Category", ""))) or "General Business"
    language = normalize_language(str(row.get("Language", "")) or config.default_language)
    activity_description = collapse_whitespace(str(row.get("Activity Types", "")))
    campaign_key = campaign_key_for_category(category)
    variant = pick_variant(f"{row.get('Email', '')}|{company_name}|{category}")
    copy_block = CAMPAIGN_COPY[language][campaign_key][variant]
    labels = LANGUAGE_LABELS[language]
    reply_phrase = reply_phrase_for_language(config, language)
    unsubscribe_text = unsubscribe_text_for_language(config, language)
    custom_offer_text = custom_offer_for_language(config, language)
    contact_email = contact_email_for_outreach(config)

    text_context = {
        "brand_name": config.brand.brand_name,
        "company_name": company_name,
        "category": category,
        "reply_phrase": reply_phrase,
    }
    subject = render_text(copy_block["subject"], text_context)
    ai_offer_text = generate_ai_outreach(
        config=config,
        company_name=company_name,
        category=category,
        description=activity_description,
        language=language,
    )
    category_offer_text = ai_offer_text or render_text(copy_block["solution"], text_context)
    html_context = {
        "subject": subject,
        "preheader": render_text(copy_block["preheader"], text_context),
        "brand_name": config.brand.brand_name,
        "company_name": company_name,
        "header_tagline": labels["header_tagline"],
        "offer_label": labels["offer_label"],
        "services_title": labels["services_title"],
        "service_ready_title": labels["service_ready_title"],
        "service_ready_body": labels["service_ready_body"],
        "service_custom_title": labels["service_custom_title"],
        "meeting_label": labels["meeting_label"],
        "meeting_link_prefix": labels["meeting_link_prefix"],
        "meeting_button": labels["meeting_button"],
        "contact_button": labels["contact_button"],
        "cta_prompt": labels["cta_prompt"],
        "mailto_href": f"mailto:{contact_email}?subject={quote(labels['mailto_subject'])}",
        "rights_text": labels["rights_text"],
        "location_text": labels["location_text"],
        "website_label": labels["website_label"],
        "contact_label": labels["contact_label"],
        "closing_text": labels["closing_text"],
        "headline": render_text(copy_block["headline"], text_context),
        "greeting": greeting_for_language(language, company_name),
        "intro": render_text(copy_block["intro"], text_context),
        "problem": render_text(copy_block["problem"], text_context),
        "solution": render_text(copy_block["solution"], text_context),
        "category_offer": category_offer_text,
        "custom_offer_title": labels["custom_offer_title"],
        "custom_offer": custom_offer_text,
        "cta": render_text(copy_block["cta"], text_context),
        "discovery_call_text": (
            discovery_call_text_for_language(language, config.brand.discovery_call_url, labels["meeting_link_prefix"])
            if config.brand.discovery_call_url
            else ""
        ),
        "discovery_call_url": config.brand.discovery_call_url,
        "your_email": contact_email,
        "signature_name": config.brand.signature_name,
        "signature_role": config.brand.signature_role,
        "signature_company": config.brand.signature_company,
        "signature_phone": config.brand.signature_phone,
        "signature_website": config.brand.signature_website,
        "sender_email": contact_email,
        "unsubscribe_text": unsubscribe_text,
    }
    return EmailDraft(
        subject=subject,
        html_body=render_html_template(template_text, html_context),
        plain_text_body=build_plain_text_body(html_context),
        template_used=f"{campaign_key}-{variant}{'-ai' if ai_offer_text else ''}",
    )


def campaign_key_for_category(category: str) -> str:
    normalized = category.casefold()
    if "custom" in normalized or "maxsus" in normalized:
        return "custom"
    if "tibbiyot" in normalized or "klinika" in normalized:
        return "healthcare"
    if "o'quv" in normalized or "oquv" in normalized or "ta'lim" in normalized:
        return "education"
    if "logistika" in normalized:
        return "logistics"
    return "general"


def normalize_language(value: str) -> str:
    normalized = collapse_whitespace(value).casefold()
    if normalized.startswith("ru") or "рус" in normalized:
        return "ru"
    return "uz"


def greeting_for_language(language: str, company_name: str) -> str:
    if language == "ru":
        return f"Здравствуйте, команда {company_name}!"
    return f"Assalomu alaykum, {company_name} jamoasi!"


def reply_phrase_for_language(config: AppConfig, language: str) -> str:
    if language == "ru":
        return "Просто ответьте на это письмо"
    return config.brand.reply_phrase


def unsubscribe_text_for_language(config: AppConfig, language: str) -> str:
    if language == "ru":
        return "Если тема вам не интересна, просто ответьте словом 'Stop'."
    return config.brand.unsubscribe_text


def custom_offer_for_language(config: AppConfig, language: str) -> str:
    if language == "ru":
        return (
            "Кроме того, если вам требуется нестандартное решение на базе искусственного интеллекта "
            "(например: анализ данных, интеграция с внутренней системой, обработка документов или "
            "помощник для сотрудников), мы можем разработать его с нуля под ваши задачи."
        )
    return config.brand.custom_offer


def discovery_call_text_for_language(language: str, url: str, prefix: str) -> str:
    if not url:
        return ""
    return f"{prefix} {url}"


def pick_variant(seed: str) -> str:
    return "A" if sum(ord(character) for character in seed) % 2 == 0 else "B"


def render_text(template: str, context: dict[str, str]) -> str:
    class SafeFormatDict(dict):
        def __missing__(self, key: str) -> str:
            return "{" + key + "}"

    return template.format_map(SafeFormatDict(**context))


def render_html_template(template_text: str, context: dict[str, str]) -> str:
    environment = Environment(
        loader=BaseLoader(),
        autoescape=select_autoescape(default=True),
    )
    template = environment.from_string(template_text)
    return template.render(**context)


def build_plain_text_body(context: dict[str, str]) -> str:
    return (
        f"{context['greeting']}\n\n"
        f"{context['intro']}\n\n"
        f"{context['problem']}\n\n"
        f"{context['offer_label']}\n"
        f"{context['category_offer']}\n\n"
        f"{context['solution']}\n\n"
        f"{context['custom_offer_title']}\n"
        f"{context['custom_offer']}\n\n"
        f"{context['cta']}\n\n"
        f"{context['discovery_call_text']}\n\n"
        f"{context['closing_text']}\n"
        f"{context['signature_name']}\n"
        f"{context['signature_role']}\n"
        f"{context['signature_company']}\n"
        f"{context['signature_phone']}\n"
        f"{context['signature_website']}\n\n"
        f"{context['unsubscribe_text']}"
    )


def send_email_with_backoff(
    smtp_config: SMTPConfig,
    to_email: str,
    subject: str,
    html_body: str,
    plain_text_body: str,
) -> tuple[bool, str]:
    last_error = ""
    for attempt in range(1, smtp_config.retry_limit + 1):
        success, error_message = send_email_once(
            smtp_config,
            to_email,
            subject,
            html_body,
            plain_text_body,
        )
        if success:
            return True, ""

        last_error = error_message
        if attempt >= smtp_config.retry_limit:
            break
        wait_seconds = min(90.0, (2 ** (attempt - 1)) * 3 + random.uniform(0.5, 1.5))
        time.sleep(wait_seconds)
    return False, last_error


def send_email_once(
    smtp_config: SMTPConfig,
    to_email: str,
    subject: str,
    html_body: str,
    plain_text_body: str,
) -> tuple[bool, str]:
    if smtp_config.transport == "gmail-api":
        return send_email_via_gmail_api(
            smtp_config=smtp_config,
            to_email=to_email,
            subject=subject,
            html_body=html_body,
            plain_text_body=plain_text_body,
        )
    if smtp_config.transport == "brevo":
        return send_email_via_brevo(
            smtp_config=smtp_config,
            to_email=to_email,
            subject=subject,
            html_body=html_body,
            plain_text_body=plain_text_body,
        )

    message = EmailMessage()
    message["From"] = formataddr((smtp_config.from_name, smtp_config.sender_email or smtp_config.username))
    message["To"] = to_email
    message["Subject"] = subject
    message["Reply-To"] = smtp_config.reply_to or smtp_config.username
    message.set_content(plain_text_body)
    message.add_alternative(html_body, subtype="html")

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(
            smtp_config.host,
            smtp_config.port,
            context=context,
            timeout=30,
        ) as server:
            server.login(smtp_config.username, smtp_config.password)
            server.send_message(message)
        return True, ""
    except Exception as exc:
        return False, str(exc)


def get_gmail_api_access_token(smtp_config: SMTPConfig) -> tuple[str | None, str]:
    cache_key = smtp_config.oauth_refresh_token
    cached = _GMAIL_API_TOKENS.get(cache_key)
    if cached is not None:
        access_token, expires_at = cached
        if time.time() < expires_at - 60:
            return access_token, ""

    payload = {
        "client_id": smtp_config.oauth_client_id,
        "client_secret": smtp_config.oauth_client_secret,
        "refresh_token": smtp_config.oauth_refresh_token,
        "grant_type": "refresh_token",
    }
    try:
        response = requests.post(
            smtp_config.oauth_token_url,
            data=payload,
            timeout=smtp_config.request_timeout_seconds,
        )
    except requests.RequestException as exc:
        return None, f"Gmail API token request failed: {exc}"

    try:
        response_payload = response.json()
    except ValueError:
        response_payload = {}

    if response.status_code != 200:
        return None, f"Gmail API token error {response.status_code}: {collapse_whitespace(str(response_payload))}"

    access_token = collapse_whitespace(str(response_payload.get("access_token", "")))
    expires_in = int(response_payload.get("expires_in", 3600) or 3600)
    if not access_token:
        return None, "Gmail API token response did not include access_token."
    _GMAIL_API_TOKENS[cache_key] = (access_token, time.time() + expires_in)
    return access_token, ""


def send_email_via_gmail_api(
    smtp_config: SMTPConfig,
    to_email: str,
    subject: str,
    html_body: str,
    plain_text_body: str,
) -> tuple[bool, str]:
    access_token, token_error = get_gmail_api_access_token(smtp_config)
    if not access_token:
        return False, token_error

    sender_email = smtp_config.sender_email or smtp_config.username
    message = EmailMessage()
    message["From"] = formataddr((smtp_config.from_name, sender_email))
    message["To"] = to_email
    message["Subject"] = subject
    message["Reply-To"] = smtp_config.reply_to or smtp_config.username
    message.set_content(plain_text_body)
    message.add_alternative(html_body, subtype="html")

    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    payload = {"raw": raw_message}

    try:
        response = requests.post(
            smtp_config.gmail_api_send_url,
            headers=headers,
            json=payload,
            timeout=smtp_config.request_timeout_seconds,
        )
    except requests.RequestException as exc:
        return False, f"Gmail API send failed: {exc}"

    if 200 <= response.status_code < 300:
        return True, ""

    try:
        response_payload = response.json()
    except ValueError:
        response_payload = response.text.strip()
    return False, f"Gmail API {response.status_code}: {collapse_whitespace(str(response_payload))}"


def send_email_via_brevo(
    smtp_config: SMTPConfig,
    to_email: str,
    subject: str,
    html_body: str,
    plain_text_body: str,
) -> tuple[bool, str]:
    sender_email = smtp_config.sender_email or smtp_config.username
    if not sender_email:
        return False, "EMAIL_SENDER_EMAIL is missing."

    payload: dict[str, Any] = {
        "sender": {
            "name": smtp_config.from_name,
            "email": sender_email,
        },
        "to": [{"email": to_email}],
        "subject": subject,
        "htmlContent": html_body,
        "textContent": plain_text_body,
    }
    reply_to = collapse_whitespace(smtp_config.reply_to or smtp_config.username)
    if reply_to:
        payload["replyTo"] = {
            "email": reply_to,
            "name": smtp_config.from_name,
        }

    headers = {
        "api-key": smtp_config.api_key,
        "accept": "application/json",
        "content-type": "application/json",
    }
    if smtp_config.sandbox_mode:
        headers["X-Sib-Sandbox"] = "drop"

    try:
        response = requests.post(
            smtp_config.api_url,
            headers=headers,
            json=payload,
            timeout=smtp_config.request_timeout_seconds,
        )
    except requests.RequestException as exc:
        return False, f"Brevo request failed: {exc}"

    if 200 <= response.status_code < 300:
        return True, ""

    try:
        response_payload = response.json()
    except ValueError:
        response_payload = response.text.strip()
    error_message = collapse_whitespace(str(response_payload))
    return False, f"Brevo API {response.status_code}: {error_message}"


def truncate_error(message: str, limit: int = 240) -> str:
    cleaned = collapse_whitespace(message)
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3] + "..."


def build_scrape_summary_table(result: ScrapePhaseResult) -> Table:
    table = Table(title="Scrape Dashboard", header_style="bold cyan")
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    table.add_row("Total scraped", str(result.total_scraped_rows))
    table.add_row("Rows with email", str(result.rows_with_email))
    table.add_row("Targeted valid leads", str(result.targeted_valid_rows))
    table.add_row("Skipped by category", str(result.skipped_priority_rows))
    table.add_row("Rejected by validation", str(result.invalid_email_rows))
    table.add_row("New leads", str(result.new_leads_added))
    table.add_row("Updated leads", str(result.existing_leads_updated))
    table.add_row("Workbook rows", str(result.total_leads_in_file))
    table.add_row("Output file", str(result.output_file))
    return table


def build_send_summary_table(result: SendPhaseResult) -> Table:
    table = Table(title="Email Dashboard", header_style="bold green")
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    table.add_row("Pending before run", str(result.pending_before))
    table.add_row("Emails sent", str(result.sent_now))
    table.add_row("Failed", str(result.failed_now))
    table.add_row("Skipped", str(result.skipped_sent))
    table.add_row("Blacklisted skipped", str(result.blacklisted_skipped))
    table.add_row("Warm-up remaining", str(result.warm_up_remaining))
    table.add_row("Reply blacklisted now", str(result.reply_blacklisted_now))
    table.add_row("Output file", str(result.output_file))
    return table


def getenv_str(name: str, default: str = "") -> str:
    import os

    return collapse_whitespace(os.getenv(name, default))


def getenv_raw(name: str, default: str = "") -> str:
    import os

    return os.getenv(name, default)


def getenv_int(name: str, default: int) -> int:
    raw_value = getenv_str(name)
    if not raw_value:
        return default
    if not raw_value.isdigit():
        raise ValueError(f"{name} must be an integer.")
    return int(raw_value)


def getenv_optional_int(name: str, default: int | None = None) -> int | None:
    raw_value = getenv_str(name)
    if not raw_value:
        return default
    if not raw_value.isdigit():
        raise ValueError(f"{name} must be an integer.")
    parsed = int(raw_value)
    return parsed if parsed > 0 else default


def getenv_float(name: str, default: float) -> float:
    raw_value = getenv_str(name)
    if not raw_value:
        return default
    try:
        return float(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a float.") from exc


def getenv_bool(name: str, default: bool) -> bool:
    raw_value = getenv_str(name)
    if not raw_value:
        return default
    normalized = raw_value.casefold()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean.")


def normalize_secret(value: str) -> str:
    return "".join(character for character in value if not character.isspace())


if __name__ == "__main__":
    raise SystemExit(main())
