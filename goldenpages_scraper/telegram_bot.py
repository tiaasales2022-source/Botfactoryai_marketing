from __future__ import annotations

import argparse
import asyncio
import os
import re
import time
from concurrent.futures import Future
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import pandas as pd
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from .phone_leads import build_sms_leads_dataframe, export_sms_leads, parse_sms_mobile_prefixes
from .phone_leads import build_google_contacts_dataframe, export_google_contacts_csv
from .scraper import GoldenPagesScraper, ScraperSettings
from .utils import (
    collapse_whitespace,
    is_company_url,
    is_rubric_url,
    normalize_url,
    timestamp_now,
)

DEFAULT_DEMO_URL = "https://www.goldenpages.uz/uz/rubrics/?Id=4676"
RICH_TAG_RE = re.compile(r"\[/?[^\]]+\]")


@dataclass(slots=True)
class BotConfig:
    token: str
    output_dir: Path
    allowed_chat_ids: set[int]
    min_delay: float
    max_delay: float
    retries: int
    timeout: float
    sms_mobile_prefixes: tuple[str, ...]
    google_contacts_labels: str


@dataclass(slots=True)
class ScrapeRequest:
    seed_url: str
    chat_id: int
    max_companies: int | None = None
    max_pages_per_seed: int | None = None
    result_mode: str = "scrape"


@dataclass(slots=True)
class SmsExportSummary:
    total_rows: int
    csv_path: Path
    xlsx_path: Path
    google_contacts_csv_path: Path


class TelegramStatusConsole:
    def __init__(self, application: Application, chat_id: int) -> None:
        self.application = application
        self.chat_id = chat_id
        self.loop = asyncio.get_running_loop()
        self.discovered_companies: int | None = None
        self.processed_companies = 0
        self.last_progress_sent_at = 0.0

    def log(self, message: object) -> None:
        text = _strip_rich_markup(str(message))
        if not text:
            return

        if text.startswith("Discovered ") and " unique company URLs" in text:
            match = re.search(r"Discovered (\d+) unique company URLs", text)
            if match:
                self.discovered_companies = int(match.group(1))
                self._send_message(
                    f"Topildi: {self.discovered_companies} ta noyob kompaniya. Endi detail sahifalar olinmoqda.",
                    force=True,
                )
            return

        if text.startswith("Scraping company #"):
            self.processed_companies += 1
            if self.processed_companies == 1 or self.processed_companies % 25 == 0:
                total = self.discovered_companies or "?"
                self._send_message(
                    f"Jarayon: {self.processed_companies}/{total} kompaniya ishlanmoqda.",
                )
            return

        if text.startswith("Retry "):
            self._send_message(f"Qayta urinish: {text}", force=True)
            return

        if text.startswith("Listing page failed") or text.startswith("Company failed"):
            self._send_message(f"Ogohlantirish: {text}", force=True)

    def _send_message(self, text: str, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self.last_progress_sent_at < 8:
            return
        self.last_progress_sent_at = now

        future = asyncio.run_coroutine_threadsafe(
            self.application.bot.send_message(chat_id=self.chat_id, text=text),
            self.loop,
        )
        future.add_done_callback(_swallow_future_exception)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="goldenpages-telegram-bot",
        description="Telegram bot that runs the GoldenPages scraper and sends CSV/XLSX files back to the chat.",
    )
    parser.add_argument(
        "--token",
        default=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        help="Telegram bot token. Defaults to TELEGRAM_BOT_TOKEN.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(os.getenv("TELEGRAM_OUTPUT_DIR", "telegram_output")),
        help="Base directory for bot-generated output files.",
    )
    parser.add_argument(
        "--allowed-chat-ids",
        default=os.getenv("TELEGRAM_ALLOWED_CHAT_IDS", ""),
        help="Comma-separated chat IDs allowed to use the bot. Empty means open access.",
    )
    parser.add_argument(
        "--min-delay",
        type=float,
        default=float(os.getenv("TELEGRAM_SCRAPER_MIN_DELAY", "1.2")),
        help="Minimum delay between HTTP requests.",
    )
    parser.add_argument(
        "--max-delay",
        type=float,
        default=float(os.getenv("TELEGRAM_SCRAPER_MAX_DELAY", "3.2")),
        help="Maximum delay between HTTP requests.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=int(os.getenv("TELEGRAM_SCRAPER_RETRIES", "5")),
        help="Retry count for HTTP requests.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=float(os.getenv("TELEGRAM_SCRAPER_TIMEOUT", "25")),
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--sms-mobile-prefixes",
        default=os.getenv("TELEGRAM_SMS_MOBILE_PREFIXES", "33,50,90,91,93,94,95,97,98,99"),
        help="Comma-separated Uzbekistan mobile prefixes that should be treated as SMS-capable.",
    )
    parser.add_argument(
        "--google-contacts-labels",
        default=os.getenv("TELEGRAM_GOOGLE_CONTACTS_LABELS", "Botfactory SMS Leads:::GoldenPages"),
        help="Google Contacts labels for the SMS phonebook CSV. Multiple labels can be joined with :::",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not collapse_whitespace(args.token):
        parser.error("Telegram token is required. Set TELEGRAM_BOT_TOKEN or pass --token.")
    if args.min_delay < 0 or args.max_delay < 0:
        parser.error("Delays must be zero or positive.")
    if args.min_delay > args.max_delay:
        parser.error("--min-delay cannot be greater than --max-delay.")
    if args.retries < 1:
        parser.error("--retries must be at least 1.")

    config = BotConfig(
        token=args.token,
        output_dir=args.output_dir,
        allowed_chat_ids=_parse_allowed_chat_ids(args.allowed_chat_ids),
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        retries=args.retries,
        timeout=args.timeout,
        sms_mobile_prefixes=parse_sms_mobile_prefixes(args.sms_mobile_prefixes),
        google_contacts_labels=collapse_whitespace(args.google_contacts_labels),
    )

    application = (
        Application.builder()
        .token(config.token)
        .post_init(_post_init)
        .build()
    )

    application.bot_data["config"] = config
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("demo", demo_command))
    application.add_handler(CommandHandler("scrape", scrape_command))
    application.add_handler(CommandHandler("sms", sms_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, plain_text_handler))

    application.run_polling()
    return 0


async def _post_init(application: Application) -> None:
    application.bot_data["active_jobs"] = {}
    application.bot_data["scrape_lock"] = asyncio.Lock()
    application.bot_data["pending_requests"] = {}


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _is_authorized(update, context):
        return
    await update.effective_message.reply_text(
        _help_text(),
        disable_web_page_preview=True,
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _is_authorized(update, context):
        return
    await update.effective_message.reply_text(
        _help_text(),
        disable_web_page_preview=True,
    )


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _is_authorized(update, context):
        return

    active_jobs: dict[int, asyncio.Task[None]] = context.application.bot_data["active_jobs"]
    chat_id = update.effective_chat.id

    if chat_id in active_jobs:
        await update.effective_message.reply_text("Sizning aktiv scraping yoki SMS lead job'ingiz hozir ishlayapti.")
        return

    scrape_lock: asyncio.Lock = context.application.bot_data["scrape_lock"]
    if scrape_lock.locked():
        await update.effective_message.reply_text("Hozir boshqa chat uchun scrape ishlayapti. Bir ozdan keyin urinib ko'ring.")
        return

    await update.effective_message.reply_text("Hozir aktiv scraping job yo'q.")


async def demo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _is_authorized(update, context):
        return

    request = ScrapeRequest(
        seed_url=DEFAULT_DEMO_URL,
        chat_id=update.effective_chat.id,
        max_companies=10,
        max_pages_per_seed=1,
    )
    await _enqueue_scrape(update, context, request=request, label="demo")


async def scrape_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _is_authorized(update, context):
        return

    if not context.args:
        pending_requests: dict[int, str] = context.application.bot_data["pending_requests"]
        pending_requests[update.effective_chat.id] = "scrape"
        await update.effective_message.reply_text(
            "GoldenPages URL yuboring.\n"
            "Masalan:\n"
            "https://www.goldenpages.uz/uz/rubrics/?Id=4676\n\n"
            "Yoki bitta xabarda shunday yuboring:\n"
            "/scrape https://www.goldenpages.uz/uz/rubrics/?Id=4676 50 2",
            disable_web_page_preview=True,
        )
        return

    request, error_message = _parse_scrape_request(
        tokens=context.args,
        chat_id=update.effective_chat.id,
        result_mode="scrape",
    )
    if error_message:
        await update.effective_message.reply_text(error_message)
        return

    await _enqueue_scrape(update, context, request=request, label="manual")


async def sms_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _is_authorized(update, context):
        return

    if not context.args:
        pending_requests: dict[int, str] = context.application.bot_data["pending_requests"]
        pending_requests[update.effective_chat.id] = "sms"
        await update.effective_message.reply_text(
            "GoldenPages URL yuboring.\n"
            "Men faqat SMS yozish mumkin bo'lgan mobil raqamlarni ajratib CSV va XLSX qilib yuboraman.\n\n"
            "Masalan:\n"
            "https://www.goldenpages.uz/uz/rubrics/?Id=4676\n\n"
            "Yoki bitta xabarda shunday yuboring:\n"
            "/sms https://www.goldenpages.uz/uz/rubrics/?Id=4676 50 2",
            disable_web_page_preview=True,
        )
        return

    request, error_message = _parse_scrape_request(
        tokens=context.args,
        chat_id=update.effective_chat.id,
        result_mode="sms",
    )
    if error_message:
        await update.effective_message.reply_text(error_message)
        return

    await _enqueue_scrape(update, context, request=request, label="sms")


async def plain_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _is_authorized(update, context):
        return

    pending_requests: dict[int, str] = context.application.bot_data["pending_requests"]
    chat_id = update.effective_chat.id
    text = collapse_whitespace(update.effective_message.text)
    request_mode = pending_requests.get(chat_id, "scrape")

    if "goldenpages.uz" not in text:
        if chat_id in pending_requests:
            await update.effective_message.reply_text(
                "URL topilmadi. GoldenPages link yuboring.\n"
                "Masalan:\n"
                "https://www.goldenpages.uz/uz/rubrics/?Id=4676",
                disable_web_page_preview=True,
            )
        return

    request, error_message = _parse_scrape_request(
        tokens=text.split(),
        chat_id=chat_id,
        result_mode=request_mode,
    )
    if error_message:
        await update.effective_message.reply_text(
            error_message,
            disable_web_page_preview=True,
        )
        return

    pending_requests.pop(chat_id, None)
    await _enqueue_scrape(update, context, request=request, label=request_mode)


async def _enqueue_scrape(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    request: ScrapeRequest,
    label: str,
) -> None:
    active_jobs: dict[int, asyncio.Task[None]] = context.application.bot_data["active_jobs"]
    scrape_lock: asyncio.Lock = context.application.bot_data["scrape_lock"]
    pending_requests: dict[int, str] = context.application.bot_data["pending_requests"]
    chat_id = request.chat_id

    if chat_id in active_jobs:
        await update.effective_message.reply_text("Sizda allaqachon bitta scraping job ishlayapti.")
        return

    if scrape_lock.locked():
        await update.effective_message.reply_text(
            "Hozir boshqa foydalanuvchi uchun scrape ishlayapti. Navbat kutmasdan keyinroq qayta yuboring."
        )
        return

    await update.effective_message.reply_text(
        (
            "SMS lead yig'ish boshlandi.\n"
            if request.result_mode == "sms"
            else "Scrape boshlandi.\n"
        )
        + f"Turi: {label}\n"
        + f"URL: {request.seed_url}\n"
        + (
            "Jarayon tugagach faqat SMS yozish mumkin bo'lgan mobil raqamlar CSV va XLSX ko'rinishida yuboriladi."
            if request.result_mode == "sms"
            else "Jarayon tugagach CSV va XLSX fayllarni shu yerga yuboraman."
        ),
        disable_web_page_preview=True,
    )

    pending_requests.pop(chat_id, None)
    task = asyncio.create_task(_run_scrape_job(context.application, request))
    active_jobs[chat_id] = task


async def _run_scrape_job(application: Application, request: ScrapeRequest) -> None:
    active_jobs: dict[int, asyncio.Task[None]] = application.bot_data["active_jobs"]
    scrape_lock: asyncio.Lock = application.bot_data["scrape_lock"]
    config: BotConfig = application.bot_data["config"]

    try:
        async with scrape_lock:
            console = TelegramStatusConsole(application, request.chat_id)
            summary = await asyncio.to_thread(_execute_scrape, request, config, console)

            if request.result_mode == "sms":
                sms_summary = await asyncio.to_thread(_build_sms_export_from_summary, summary, config)
                await application.bot.send_message(
                    chat_id=request.chat_id,
                    text=(
                        "SMS lead yig'ish tugadi.\n"
                        f"Kompaniyalar: {summary.discovered_companies}\n"
                        f"SMS yozish mumkin bo'lgan raqamlar: {sms_summary.total_rows}\n"
                        f"Xatolar: {summary.failed_count}"
                    ),
                )

                with sms_summary.csv_path.open("rb") as csv_file:
                    await application.bot.send_document(
                        chat_id=request.chat_id,
                        document=csv_file,
                        filename=sms_summary.csv_path.name,
                        caption="SMS yozish mumkin bo'lgan raqamlar CSV",
                    )

                with sms_summary.xlsx_path.open("rb") as xlsx_file:
                    await application.bot.send_document(
                        chat_id=request.chat_id,
                        document=xlsx_file,
                        filename=sms_summary.xlsx_path.name,
                        caption="SMS yozish mumkin bo'lgan raqamlar XLSX",
                    )

                with sms_summary.google_contacts_csv_path.open("rb") as contacts_csv_file:
                    await application.bot.send_document(
                        chat_id=request.chat_id,
                        document=contacts_csv_file,
                        filename=sms_summary.google_contacts_csv_path.name,
                        caption="Google Contacts import CSV",
                    )

                await application.bot.send_message(
                    chat_id=request.chat_id,
                    text=(
                        "Google Contacts CSV ni contacts.google.com ichidan 'Import qilish' orqali yuklasangiz, "
                        "kontaktlar telefoningizga ham sinxron tushadi."
                    ),
                    disable_web_page_preview=True,
                )
            else:
                await application.bot.send_message(
                    chat_id=request.chat_id,
                    text=(
                        "Scrape tugadi.\n"
                        f"Kompaniyalar: {summary.discovered_companies}\n"
                        f"Export qatorlari: {summary.exported_rows}\n"
                        f"Xatolar: {summary.failed_count}"
                    ),
                )

                with summary.csv_path.open("rb") as csv_file:
                    await application.bot.send_document(
                        chat_id=request.chat_id,
                        document=csv_file,
                        filename=summary.csv_path.name,
                        caption="Backup CSV",
                    )

                with summary.xlsx_path.open("rb") as xlsx_file:
                    await application.bot.send_document(
                        chat_id=request.chat_id,
                        document=xlsx_file,
                        filename=summary.xlsx_path.name,
                        caption="Excel natija",
                    )

            if summary.failed_count:
                with summary.state_path.open("rb") as state_file:
                    await application.bot.send_document(
                        chat_id=request.chat_id,
                        document=state_file,
                        filename=summary.state_path.name,
                        caption="Resume uchun state fayl",
                    )
    except Exception as exc:
        await application.bot.send_message(
            chat_id=request.chat_id,
            text=f"Scrape ishlashida xatolik bo'ldi: {exc}",
            disable_web_page_preview=True,
        )
    finally:
        active_jobs.pop(request.chat_id, None)


def _execute_scrape(
    request: ScrapeRequest,
    config: BotConfig,
    console: TelegramStatusConsole,
):
    run_dir = config.output_dir / f"chat_{request.chat_id}" / timestamp_now()
    run_dir.mkdir(parents=True, exist_ok=True)

    settings = ScraperSettings(
        seed_urls=[request.seed_url],
        max_companies=request.max_companies,
        max_pages_per_seed=request.max_pages_per_seed,
        min_delay=config.min_delay,
        max_delay=config.max_delay,
        retries=config.retries,
        timeout=config.timeout,
        output_dir=run_dir,
    )
    scraper = GoldenPagesScraper(settings=settings, console=console)
    return scraper.run()


def _build_sms_export_from_summary(summary, config: BotConfig) -> SmsExportSummary:
    dataframe = pd.read_excel(summary.xlsx_path)
    sms_dataframe = build_sms_leads_dataframe(
        dataframe,
        mobile_prefixes=config.sms_mobile_prefixes,
    )
    google_contacts_dataframe = build_google_contacts_dataframe(
        sms_dataframe,
        labels=config.google_contacts_labels,
    )
    run_id = summary.csv_path.stem.removeprefix("backup_data_")
    sms_csv_path, sms_xlsx_path = export_sms_leads(
        sms_dataframe,
        summary.csv_path.parent,
        run_id,
    )
    google_contacts_csv_path = export_google_contacts_csv(
        google_contacts_dataframe,
        summary.csv_path.parent,
        run_id,
    )
    return SmsExportSummary(
        total_rows=len(sms_dataframe.index),
        csv_path=sms_csv_path,
        xlsx_path=sms_xlsx_path,
        google_contacts_csv_path=google_contacts_csv_path,
    )


def _parse_scrape_request(
    *,
    tokens: Sequence[str],
    chat_id: int,
    result_mode: str,
) -> tuple[ScrapeRequest | None, str | None]:
    if not tokens:
        command_name = "/sms" if result_mode == "sms" else "/scrape"
        return None, (
            "Format:\n"
            f"{command_name} <goldenpages_url> [max_companies] [max_pages_per_seed]\n"
            "Misol:\n"
            f"{command_name} https://www.goldenpages.uz/uz/rubrics/?Id=4676 50 3"
        )

    seed_url = normalize_url(tokens[0])
    if not (is_rubric_url(seed_url) or is_company_url(seed_url)):
        return None, "URL GoldenPages rubric yoki company sahifasi bo'lishi kerak."

    max_companies = _parse_optional_int(tokens, 1, "max_companies")
    max_pages_per_seed = _parse_optional_int(tokens, 2, "max_pages_per_seed")
    if isinstance(max_companies, str):
        return None, max_companies
    if isinstance(max_pages_per_seed, str):
        return None, max_pages_per_seed

    return (
        ScrapeRequest(
            seed_url=seed_url,
            chat_id=chat_id,
            max_companies=max_companies,
            max_pages_per_seed=max_pages_per_seed,
            result_mode=result_mode,
        ),
        None,
    )


def _parse_optional_int(tokens: Sequence[str], index: int, field_name: str) -> int | None | str:
    if len(tokens) <= index:
        return None
    value = collapse_whitespace(tokens[index])
    if not value:
        return None
    if not value.isdigit():
        return f"{field_name} butun son bo'lishi kerak."
    parsed = int(value)
    if parsed < 1:
        return f"{field_name} kamida 1 bo'lishi kerak."
    return parsed


def _parse_allowed_chat_ids(raw_value: str) -> set[int]:
    allowed: set[int] = set()
    for chunk in raw_value.split(","):
        cleaned = collapse_whitespace(chunk)
        if not cleaned:
            continue
        if cleaned.lstrip("-").isdigit():
            allowed.add(int(cleaned))
    return allowed


async def _is_authorized(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    config: BotConfig = context.application.bot_data["config"]
    if not config.allowed_chat_ids:
        return True

    chat_id = update.effective_chat.id
    if chat_id in config.allowed_chat_ids:
        return True

    await update.effective_message.reply_text("Bu bot sizning chat uchun ruxsat etilmagan.")
    return False


def _help_text() -> str:
    return (
        "GoldenPages Telegram Bot tayyor.\n\n"
        "Buyruqlar:\n"
        "/scrape <url> [max_companies] [max_pages_per_seed]\n"
        "/sms <url> [max_companies] [max_pages_per_seed]\n"
        "/demo\n"
        "/status\n"
        "/help\n\n"
        "Misollar:\n"
        "/scrape https://www.goldenpages.uz/uz/rubrics/?Id=4676\n"
        "/scrape https://www.goldenpages.uz/uz/rubrics/?Id=4676 50 2\n"
        "/sms https://www.goldenpages.uz/uz/rubrics/?Id=4676 50 2\n"
        "/demo\n\n"
        "Natija tugagach bot sizga CSV va XLSX fayl yuboradi.\n"
        "/sms komandasi esa faqat SMS yozish mumkin bo'lgan mobil raqamlarni ajratib beradi."
    )


def _strip_rich_markup(text: str) -> str:
    return collapse_whitespace(RICH_TAG_RE.sub("", text))


def _swallow_future_exception(future: Future[object]) -> None:
    try:
        future.result()
    except Exception:
        pass


if __name__ == "__main__":
    raise SystemExit(main())
