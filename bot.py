import os
from datetime import date, timedelta
from dotenv import load_dotenv

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    CommandHandler,
    ContextTypes,
    filters,
)

from database import init_db
from parser import parse_daily_report
from services import save_report, get_summary, report_exists

# ---------------- LOAD ENV ----------------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")


# ---------------- HELPERS ----------------
def format_report_message(station: str, report_date: str, fuels: dict) -> str | None:
    """Builds a formatted message for a daily sales report."""
    total_volume = sum(fuels.get(ft, 0) for ft in ["DO", "EA92", "EA95"])
    if total_volume == 0:
        return None  # skip empty reports

    msg = (
        "📊 *DAILY SALES REPORT DETECTED*\n"
        "----------------------------------------------------\n"
        f"សាខាស្ថានីយ: {station}\n"
        "----------------------------------------------------\n"
        "កាលបរិច្ឆេទ  | DO     | EA92   | EA95   | TOTAL\n"
        "----------------------------------------------------\n"
        f"{report_date} | {fuels.get('DO',0):.2f}L | {fuels.get('EA92',0):.2f}L | "
        f"{fuels.get('EA95',0):.2f}L | {total_volume:.2f}L\n"
    )
    return msg


def consolidate_station_summary(station: str, station_dates: dict) -> str:
    """
    Builds a single message per station for multiple dates in a Telegram code block.
    """
    msg = f"📊 SUMMARY FOR STATION: {station}\n"
    msg += "```\n"  # start code block
    msg += "--------------------------------------------\n"
    msg += "Date   | DO     | EA92   | EA95   | TOTAL\n"
    msg += "--------------------------------------------\n"

    for report_date, fuels in station_dates.items():
        # Normalize keys
        normalized_fuels = {k.strip(): v for k, v in fuels.items()}
        do = normalized_fuels.get("ប្រេងម៉ាស៊ូត DO - T1", 0)
        ea92 = normalized_fuels.get("សាំង EA92 - T2", 0)
        ea95 = normalized_fuels.get("សាំងស៊ុបពែរ EA95 -T3", 0)
        total_volume = do + ea92 + ea95

        # Format row
        msg += f"{report_date:<5}|{do:>5.2f}L|{ea92:>5.2f}L|{ea95:>5.2f}L|{total_volume:>5.2f}L\n"

    msg += "```"  # end code block
    return msg



# ---------------- HANDLERS ----------------
async def handle_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles incoming daily report messages."""
    try:
        if not update.message or not update.message.text:
            return

        print("📥 MESSAGE RECEIVED FROM CHAT:", update.effective_chat.id)
        print("📄 TEXT:", update.message.text)

        # Parse report
        data = parse_daily_report(update.message.text)
        station = data.get("station_name")
        report_date = data.get("report_date")

        if not station or not report_date:
            await update.message.reply_text("❌ Missing station or report date.")
            return

        # Check for duplicate
        if report_exists(station, report_date):
            await update.message.reply_text(
                f"⚠️ Report for *{station}* on {report_date} already exists. Skipping save.",
                parse_mode="Markdown"
            )
            return

        # Save report
        save_report(data)
        await update.message.reply_text(
            f"✅ Daily sales report for *{station}* on {report_date} saved successfully.",
            parse_mode="Markdown"
        )

    except Exception as e:
        print("❌ PARSE ERROR:", e)
        await update.message.reply_text(f"❌ Failed to process report.\nError: {e}")


async def summary(update: Update, context: ContextTypes.DEFAULT_TYPE, days: int = None):
    """Generates a table-style summary per station, consolidated."""
    try:
        start_date = str(date.today() - timedelta(days=days)) if days else None
        end_date = str(date.today()) if days else None

        rows = get_summary(start_date, end_date)
        if not rows:
            await update.message.reply_text("⚠️ No data found.")
            return

        # Organize data by station -> date -> fuel_type
        station_data = {}
        for station_name, fuel_type, volume, amount, report_date in rows:
            station_data.setdefault(station_name, {}).setdefault(report_date, {})[fuel_type] = volume

        # Send one consolidated message per station
        for station, dates in station_data.items():
            msg = consolidate_station_summary(station, dates)
            if msg:
                await update.message.reply_text(msg, parse_mode="Markdown")

    except Exception as e:
        print("❌ SUMMARY ERROR:", e)
        await update.message.reply_text(f"❌ Error generating summary:\n{e}")


# ---------------- COMMANDS ----------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command to check if bot is running."""
    await update.message.reply_text(
        "🤖 KONCHAT Bot is running!\n"
        "Commands available:\n"
        "/today - Today's report\n"
        "/yesterday - Yesterday's report\n"
        "/weekly - Last 7 days\n"
        "/monthly - Last 30 days\n"
        "/debug - Debug info"
    )


async def today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await summary(update, context, 1)


async def yesterday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await summary(update, context, 2)


async def weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await summary(update, context, 7)


async def monthly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await summary(update, context, 30)


async def debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = get_summary(None, None)
    await update.message.reply_text(f"DEBUG: {len(rows)} records found.")


# ---------------- MAIN ----------------
def main():
    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Message handler for daily reports
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_report))

    # Command handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("today", today))
    app.add_handler(CommandHandler("yesterday", yesterday))
    app.add_handler(CommandHandler("weekly", weekly))
    app.add_handler(CommandHandler("monthly", monthly))
    app.add_handler(CommandHandler("debug", debug))

    print("🤖 KONCHAT is running...")
    app.run_polling()


if __name__ == "__main__":
    main()
