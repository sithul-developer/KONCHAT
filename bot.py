import os
from datetime import date, timedelta
from dotenv import load_dotenv
from collections import defaultdict

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    CommandHandler,
    ContextTypes,
    filters,
    CallbackQueryHandler,
)

from database import init_db
from parser import parse_daily_report
from services import save_report, get_summary, report_exists, get_all_stations

# ---------------- LOAD ENV ----------------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")

# ---------------- STATE MANAGEMENT ----------------
# Store user selection states
user_selections = {}

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

def create_station_keyboard(command_type: str = "today") -> InlineKeyboardMarkup:
    """Create inline keyboard with station selection."""
    stations = get_all_stations()
    
    if not stations:
        return None
    
    # Create buttons
    keyboard = []
    
    # Add "ALL STATIONS" button at top
    keyboard.append([InlineKeyboardButton("🏪 ALL STATIONS", callback_data=f"{command_type}:ALL")])
    
    # Add individual stations in rows of 2
    row = []
    for i, station in enumerate(stations):
        # Truncate long station names
        display_name = station[:15] + "..." if len(station) > 15 else station
        row.append(InlineKeyboardButton(f"⛽ {display_name}", callback_data=f"{command_type}:{station}"))
        
        if len(row) == 2 or i == len(stations) - 1:
            keyboard.append(row)
            row = []
    
    return InlineKeyboardMarkup(keyboard)

def consolidate_station_summary(station: str, station_dates: dict) -> str:
    """
    Builds a single message per station for multiple dates in a Telegram code block.
    """
    if not station_dates:
        return None
    
    # Sort dates
    sorted_dates = sorted(station_dates.keys())
    
    msg = f"📊 SUMMARY FOR: {station}\n"
    msg += "```\n"
    msg += "Date  | DO     | EA92   | EA95   | TOTAL\n"
    msg += "--------------------------------------------\n"
    
    total_do = total_ea92 = total_ea95 = 0
    
    for report_date in sorted_dates:
        fuels = station_dates[report_date]
        # Normalize keys
        normalized_fuels = {k.strip(): v for k, v in fuels.items()}
        do = normalized_fuels.get("ប្រេងម៉ាស៊ូត DO - T1", 0)
        ea92 = normalized_fuels.get("សាំង EA92 - T2", 0)
        ea95 = normalized_fuels.get("សាំងស៊ុបពែរ EA95 -T3", 0)
        total_volume = do + ea92 + ea95
        
        # Accumulate totals
        total_do += do
        total_ea92 += ea92
        total_ea95 += ea95
        
        # Format row
        msg += f"{report_date:<5}|{do:>5.2f}L|{ea92:>5.2f}L|{ea95:>5.2f}L|{total_volume:>5.2f}L\n"
    
    # Add totals row
    msg += "--------------------------------------------\n"
    grand_total = total_do + total_ea92 + total_ea95
    msg += f"{'TOTAL':<5}|{total_do:>5.2f}L|{total_ea92:>5.2f}L|{total_ea95:>5.2f}L|{grand_total:>5.2f}L\n"
    msg += "```"
    
    return msg

def generate_summary_by_period(start_date: str, end_date: str, station: str = None):
    """Generate summary for specific period and station."""
    rows = get_summary(start_date, end_date)
    
    if not rows:
        return None, []
    
    if station and station != "ALL":
        # Filter for specific station
        rows = [r for r in rows if r[0] == station]
    
    # Organize data by station -> date -> fuel_type
    station_data = {}
    for station_name, fuel_type, volume, amount, report_date in rows:
        station_data.setdefault(station_name, {}).setdefault(report_date, {})[fuel_type] = volume
    
    return station_data, rows

def format_period_message(days: int) -> str:
    """Format period message for summary."""
    if days == 1:
        return "📅 *Today*"
    elif days == 2:
        return "📅 *Yesterday*"
    elif days == 7:
        return "📅 *Last 7 days*"
    elif days == 30:
        return "📅 *Last 30 days*"
    else:
        return f"📅 *Last {days} days*"

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

async def show_station_selection(update: Update, context: ContextTypes.DEFAULT_TYPE, command: str, days: int):
    """Show station selection for time-based commands."""
    stations = get_all_stations()
    
    if not stations:
        await update.message.reply_text(
            "📭 No station data available yet. Please send some daily reports first."
        )
        return
    
    # Store days in user context for later use
    user_id = update.effective_user.id
    user_selections[user_id] = {"command": command, "days": days}
    
    # Determine title based on command
    titles = {
        "today": "📊 TODAY'S REPORT",
        "yesterday": "📊 YESTERDAY'S REPORT",
        "weekly": "📊 WEEKLY REPORT (Last 7 days)",
        "monthly": "📊 MONTHLY REPORT (Last 30 days)"
    }
    
    title = titles.get(command, f"📊 {command.upper()} REPORT")
    
    # Create message
    message = (
        f"*{title}*\n"
        f"--------------------------------------------\n"
        f"Available stations: *{len(stations)}*\n"
        f"Please select a station:"
        f"• 🏪 **ALL STATIONS** - See combined report\n"
        f"• ⛽ **Individual station** - View specific station"
    )
    
    keyboard = create_station_keyboard(command)
    if keyboard:
        await update.message.reply_text(message, parse_mode="Markdown", reply_markup=keyboard)
    else:
        await update.message.reply_text("❌ No stations available for selection.")

async def handle_station_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle station selection from inline keyboard."""
    query = update.callback_query
    await query.answer()
    
    # Parse callback data
    data = query.data.split(":")
    if len(data) != 2:
        await query.edit_message_text("❌ Invalid selection")
        return
    
    command, station = data
    user_id = query.from_user.id
    
    # Get days from stored selection or default
    if user_id in user_selections and user_selections[user_id]["command"] == command:
        days = user_selections[user_id]["days"]
    else:
        # Default days based on command
        days_map = {"today": 1, "yesterday": 2, "weekly": 7, "monthly": 30}
        days = days_map.get(command, 1)
    
    # Calculate date range
    end_date = date.today()
    start_date = end_date - timedelta(days=days-1)
    
    # Show loading message
    station_display = "ALL STATIONS" if station == "ALL" else station
    await query.edit_message_text(f"⏳ Generating report for *{station_display}*...", parse_mode="Markdown")
    
    # Generate summary
    try:
        station_data, rows = generate_summary_by_period(str(start_date), str(end_date), station)
        
        if not station_data:
            await query.edit_message_text(
                f"⚠️ No data found for *{station_display}* from {start_date} to {end_date}",
                parse_mode="Markdown"
            )
            return
        
        period_msg = format_period_message(days)
        
        if station == "ALL":
            # Send summary for each station
            for station_name, dates in station_data.items():
                msg = consolidate_station_summary(station_name, dates)
                if msg:
                    await query.message.reply_text(msg, parse_mode="Markdown")
            
            # Send combined totals for all stations
            await send_combined_summary(query, station_data, start_date, end_date)
            await query.message.reply_text(f"{period_msg}: {start_date} to {end_date}", parse_mode="Markdown")
            
        else:
            # Send single station summary
            msg = consolidate_station_summary(station, station_data[station])
            if msg:
                await query.edit_message_text(msg, parse_mode="Markdown")
                await query.message.reply_text(f"{period_msg}: {start_date} to {end_date}", parse_mode="Markdown")
    
    except Exception as e:
        print(f"❌ SUMMARY ERROR: {e}")
        await query.edit_message_text(f"❌ Error generating summary:\n{e}")

async def send_combined_summary(query, station_data: dict, start_date: date, end_date: date):
    """Send combined summary for all stations."""
    total_do = total_ea92 = total_ea95 = 0
    station_count = len(station_data)
    
    # Calculate totals across all stations
    for station, dates in station_data.items():
        for report_date, fuels in dates.items():
            # Normalize keys
            normalized_fuels = {k.strip(): v for k, v in fuels.items()}
            total_do += normalized_fuels.get("ប្រេងម៉ាស៊ូត DO - T1", 0)
            total_ea92 += normalized_fuels.get("សាំង EA92 - T2", 0)
            total_ea95 += normalized_fuels.get("សាំងស៊ុបពែរ EA95 -T3", 0)
    
    total_all = total_do + total_ea92 + total_ea95
    
    # Format combined summary
    combined_msg = (
        f"📊 *COMBINED SUMMARY FOR ALL STATIONS*\n"
        f"--------------------------------------------\n"
        f"🏪 *Stations:* {station_count}\n"
        f"📅 *Period:* {start_date} to {end_date}\n\n"
        f"*TOTAL VOLUMES:*\n"
        f"```\n"
        f"DO     : {total_do:>10.2f}L\n"
        f"EA92   : {total_ea92:>10.2f}L\n"
        f"EA95   : {total_ea95:>10.2f}L\n"
        f"--------------------------------------------\n"
        f"TOTAL  : {total_all:>10.2f}L\n"
        f"```"
    )
    
    await query.message.reply_text(combined_msg, parse_mode="Markdown")

# ---------------- COMMANDS ----------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command to check if bot is running."""
    await update.message.reply_text(
        "🤖 KONCHAT Bot is running!\n\n"
        "📋 *Available Commands:*\n"
        "/today - Today's report (select station)\n"
        "/yesterday - Yesterday's report (select station)\n"
        "/weekly - Last 7 days (select station)\n"
        "/monthly - Last 30 days (select station)\n"
        "/stations - List all stations\n"
        "/stats - Show statistics\n"
        "/debug - Debug info\n\n"
        "📥 *How to use:*\n"
        "1. Send daily sales report text\n"
        "2. Bot will automatically parse and save it\n"
        "3. Use commands with station selection"
    )

async def stations_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all available stations."""
    stations = get_all_stations()
    
    if not stations:
        await update.message.reply_text("📭 No stations found in database.")
        return
    
    message = "🏪 *AVAILABLE STATIONS*\n--------------------------------------------\n"
    
    for i, station in enumerate(stations, 1):
        message += f"{i}. *{station}*\n"
    
    message += f"\n*Total:* {len(stations)} stations"
    
    await update.message.reply_text(message, parse_mode="Markdown")

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show statistics about stored reports."""
    rows = get_summary(None, None)
    
    if not rows:
        await update.message.reply_text("📭 No reports stored yet.")
        return
    
    # Count unique stations and dates
    stations = set()
    dates = set()
    total_volume = 0
    
    for station_name, fuel_type, volume, amount, report_date in rows:
        stations.add(station_name)
        dates.add(report_date)
        total_volume += volume
    
    # Get date range
    sorted_dates = sorted(dates)
    date_range = f"{sorted_dates[0]} to {sorted_dates[-1]}" if sorted_dates else "N/A"
    
    message = (
        f"📊 *STATISTICS*\n"
        f"--------------------------------------------\n"
        f"📅 Date range: {date_range}\n"
        f"📁 Total reports: {len(rows)}\n"
        f"⛽ Total volume: {total_volume:.2f}L\n"
        f"🏪 Unique stations: {len(stations)}\n"
        f"📆 Days recorded: {len(dates)}"
    )
    
    await update.message.reply_text(message, parse_mode="Markdown")

async def today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show today's report with station selection."""
    await show_station_selection(update, context, "today", 1)

async def yesterday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show yesterday's report with station selection."""
    await show_station_selection(update, context, "yesterday", 2)

async def weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show weekly report with station selection."""
    await show_station_selection(update, context, "weekly", 7)

async def monthly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show monthly report with station selection."""
    await show_station_selection(update, context, "monthly", 30)

async def debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Debug command."""
    rows = get_summary(None, None)
    stations = get_all_stations()
    
    debug_info = (
        f"🐛 *DEBUG INFORMATION*\n"
        f"--------------------------------------------\n"
        f"• Total records: {len(rows)}\n"
        f"• Unique stations: {len(stations)}\n"
        f"• User selections stored: {len(user_selections)}\n"
        f"• Bot token: {'✅ Set' if BOT_TOKEN else '❌ Not set'}"
    )
    
    await update.message.reply_text(debug_info, parse_mode="Markdown")

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
    app.add_handler(CommandHandler("stations", stations_command))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("debug", debug))
    
    # Callback query handler for station selection
    app.add_handler(CallbackQueryHandler(handle_station_selection))

    print("🤖 KONCHAT is running...")
    print(f"📊 Database initialized with stations: {len(get_all_stations())}")
    app.run_polling()

if __name__ == "__main__":
    main()