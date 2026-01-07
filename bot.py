import os
import logging
from datetime import date, datetime, timedelta
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

# Import from database and parser
from database import init_db, save_report, report_exists, get_summary, get_all_stations
from parser import parse_daily_report, format_for_database

# Setup
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    logger.error("BOT_TOKEN not found!")
    exit(1)

# Date format
DATE_FORMAT = "%Y/%m/%d"

def format_date(date_obj: date) -> str:
    return date_obj.strftime(DATE_FORMAT)

def parse_date_string(date_str: str) -> date:
    """Parse date string to date object."""
    try:
        if not date_str:
            return date.today()
        
        # Try common formats
        for fmt in [DATE_FORMAT, "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"]:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        
        # Try to extract numbers
        import re
        numbers = re.findall(r'\d+', date_str)
        if len(numbers) >= 3:
            y, m, d = int(numbers[0]), int(numbers[1]), int(numbers[2])
            if y < 100:
                y += 2000
            return date(y, m, d)
        
        return date.today()
    except:
        return date.today()

# State management
user_selections = defaultdict(dict)

# ==================== KEYBOARDS ====================

def create_main_menu() -> InlineKeyboardMarkup:
    """Main menu keyboard."""
    keyboard = [
        [InlineKeyboardButton("📊 View Reports", callback_data="view_reports")],
        [InlineKeyboardButton("🏪 All Stations", callback_data="all_stations")],

    ]
    return InlineKeyboardMarkup(keyboard)

def create_station_keyboard() -> InlineKeyboardMarkup:
    """Station selection keyboard."""
    stations = get_all_stations()
    
    if not stations:
        return InlineKeyboardMarkup([[InlineKeyboardButton("📭 No stations", callback_data="ignore")]])
    
    # Create 2 columns
    keyboard = []
    row = []
    for i, station in enumerate(stations):
        display_name = station[:15] + "..." if len(station) > 15 else station
        row.append(InlineKeyboardButton(f"⛽ {display_name}", callback_data=f"station:{i}"))
        
        if len(row) == 2 or i == len(stations) - 1:
            keyboard.append(row)
            row = []
    
    # Store station mapping
    for i, station in enumerate(stations):
        user_selections["station_mapping"][str(i)] = station
    
    keyboard.append([
        InlineKeyboardButton("◀️ Back", callback_data="main_menu"),
        InlineKeyboardButton("❌ Cancel", callback_data="cancel")
    ])
    
    return InlineKeyboardMarkup(keyboard)

def create_quick_dates_keyboard(station: str) -> InlineKeyboardMarkup:
    """Quick date selection keyboard."""
    today = format_date(date.today())
    yesterday = format_date(date.today() - timedelta(days=1))
    
    keyboard = [
        [InlineKeyboardButton("📅 Today", callback_data=f"date:{station}:{today}")],
        [InlineKeyboardButton("📅 Yesterday", callback_data=f"date:{station}:{yesterday}")],
        [InlineKeyboardButton("📅 Last 7 Days", callback_data=f"range:{station}:7")],
        [InlineKeyboardButton("📅 Last 30 Days", callback_data=f"range:{station}:30")],
        [InlineKeyboardButton("📆 Select Month", callback_data=f"select_month:{station}")],
        [
            InlineKeyboardButton("◀️ Back", callback_data="view_reports"),
            InlineKeyboardButton("🏠 Main", callback_data="main_menu")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_month_selector(station: str, year: int = None) -> InlineKeyboardMarkup:
    """Month selection keyboard."""
    if year is None:
        year = date.today().year
    
    keyboard = []
    
    # Year navigation
    keyboard.append([
        InlineKeyboardButton("◀️", callback_data=f"month_year:{station}:{year-1}"),
        InlineKeyboardButton(f"{year}", callback_data="ignore"),
        InlineKeyboardButton("▶️", callback_data=f"month_year:{station}:{year+1}")
    ])
    
    # Months (3x4 grid)
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    
    for i in range(0, 12, 3):
        row = []
        for j in range(3):
            month_idx = i + j + 1
            month_name = months[month_idx - 1]
            
            # Highlight current month
            current = (year == date.today().year and month_idx == date.today().month)
            display = f"•{month_name}•" if current else month_name
            
            row.append(InlineKeyboardButton(
                display,
                callback_data=f"month:{station}:{year}:{month_idx}"
            ))
        keyboard.append(row)
    
    keyboard.append([
        InlineKeyboardButton("📅 Current Month", callback_data=f"month:{station}:{year}:{date.today().month}"),
        InlineKeyboardButton("◀️ Back", callback_data=f"dates:{station}")
    ])
    
    return InlineKeyboardMarkup(keyboard)

# ==================== REPORT FORMATTING ====================

def generate_daily_report(station: str, date_str: str) -> dict:
    """Generate report for a single day."""
    rows = get_summary(date_str, date_str)
    
    if not rows:
        return None
    
    # Filter and organize by station
    station_data = {}
    for row in rows:
        if row["station_name"] == station:
            fuel_type = row["fuel_type"]
            volume = row["volume"]
            
            # Normalize fuel names
            if "Diesel" in fuel_type or "DO" in fuel_type.upper():
                fuel_key = "Diesel"
            elif "Regular" in fuel_type:
                fuel_key = "Regular"
            elif "Super" in fuel_type:
                fuel_key = "Super"
            else:
                fuel_key = fuel_type
            
            if date_str not in station_data:
                station_data[date_str] = {}
            
            station_data[date_str][fuel_key] = station_data[date_str].get(fuel_key, 0) + volume
    
    return station_data.get(date_str)

def generate_range_report(station: str, days: int) -> dict:
    """Generate report for a date range."""
    end_date = date.today()
    start_date = end_date - timedelta(days=days-1)
    
    start_str = format_date(start_date)
    end_str = format_date(end_date)
    
    rows = get_summary(start_str, end_str)
    
    if not rows:
        return None
    
    # Organize data
    daily_data = defaultdict(lambda: defaultdict(float))
    total_volume = 0
    
    for row in rows:
        if row["station_name"] == station:
            date_str = row["report_date"]
            fuel_type = row["fuel_type"]
            volume = row["volume"]
            
            # Normalize fuel names
            if "Diesel" in fuel_type or "DO" in fuel_type.upper():
                fuel_key = "Diesel"
            elif "Regular" in fuel_type:
                fuel_key = "Regular"
            elif "Super" in fuel_type:
                fuel_key = "Super"
            else:
                fuel_key = fuel_type
            
            daily_data[date_str][fuel_key] += volume
            total_volume += volume
    
    return {
        "daily_data": dict(daily_data),
        "total_volume": total_volume,
        "days": len(daily_data),
        "start_date": start_str,
        "end_date": end_str
    }

def format_daily_summary(station: str, date_str: str, fuels: dict) -> str:
    """Format daily report message."""
    if not fuels:
        return f"⚠️ No data for {station} on {date_str}"
    
    diesel = fuels.get("Diesel", 0)
    regular = fuels.get("Regular", 0)
    super_fuel = fuels.get("Super", 0)
    total = diesel + regular + super_fuel
    
    if total == 0:
        return f"⚠️ No data for {station} on {date_str}"
    
    diesel_pct = (diesel / total * 100) if total > 0 else 0
    regular_pct = (regular / total * 100) if total > 0 else 0
    super_pct = (super_fuel / total * 100) if total > 0 else 0
    
    return (
        f"📊 *DAILY REPORT*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏪 Station: {station}\n"
        f"📅 Date: {date_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"⛽ Diesel: {diesel:,.2f}L ({diesel_pct:.1f}%)\n"
        f"⛽ Regular: {regular:,.2f}L ({regular_pct:.1f}%)\n"
        f"⛽ Super: {super_fuel:,.2f}L ({super_pct:.1f}%)\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Total: {total:,.2f}L"
    )

def format_range_summary(station: str, report_data: dict) -> str:
    """Format range report message with table-style daily breakdown."""
    if not report_data:
        return f"⚠️ No data for {station} in this period"
    
    daily_data = report_data["daily_data"]
    total_volume = report_data["total_volume"]
    days_count = report_data["days"]
    start_date = report_data["start_date"]
    end_date = report_data["end_date"]
    
    # Calculate totals by fuel type
    total_diesel = total_regular = total_super = 0
    daily_breakdown = []
    
    # Sort dates (using yyyy/mm/dd for sorting)
    sorted_dates = sorted(daily_data.keys())
    
    for date_str in sorted_dates:
        fuels = daily_data[date_str]
        diesel = fuels.get("Diesel", 0)
        regular = fuels.get("Regular", 0)
        super_fuel = fuels.get("Super", 0)
        daily_total = diesel + regular + super_fuel
        
        # Convert yyyy/mm/dd to dd/mm for display
        try:
            date_obj = parse_date_string(date_str)
            display_date = f"{date_obj.day:02d}/{date_obj.month:02d}"
        except:
            display_date = date_str  # Fallback
        
        daily_breakdown.append({
            "date": date_str,  # Original for sorting
            "display_date": display_date,  # For display
            "diesel": diesel,
            "regular": regular,
            "super": super_fuel,
            "total": daily_total
        })
        
        total_diesel += diesel
        total_regular += regular
        total_super += super_fuel
    
    avg_daily = total_volume / days_count if days_count > 0 else 0
    
    # Calculate percentages
    diesel_pct = (total_diesel / total_volume * 100) if total_volume > 0 else 0
    regular_pct = (total_regular / total_volume * 100) if total_volume > 0 else 0
    super_pct = (total_super / total_volume * 100) if total_volume > 0 else 0
    
    # Build table with dd/mm format
    table = "```\n"
    table += f"{'Date':<5} | {'DO':>6} | {'EA92':>6} | {'EA95':>6} | {'Total':>8}\n"
    table += "-" * 41 + "\n"
    
    for day in daily_breakdown:
        table += f"{day['display_date']:<5} | "
        table += f"{day['diesel']:>6.2f} | "
        table += f"{day['regular']:>6.2f} | "
        table += f"{day['super']:>6.2f} | "
        table += f"{day['total']:>8.2f}\n"

    # Add totals row
    table += "-" * 41 + "\n"
    table += f"{'TOTAL':<5} | "
    table += f"{total_diesel:>6.2f} | "
    table += f"{total_regular:>6.2f} | "
    table += f"{total_super:>6.2f} | "
    table += f"{total_volume:>8.2f}\n"
    table += "```\n"
    
    # Format period display
    try:
        start_obj = parse_date_string(start_date)
        end_obj = parse_date_string(end_date)
        period_display = f"{start_obj.day:02d}/{start_obj.month:02d} - {end_obj.day:02d}/{end_obj.month:02d}/{end_obj.year}"
    except:
        period_display = f"{start_date} to {end_date}"
    
    # Build message
    msg = (
        f"📊 *RANGE REPORT*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏪 Station: {station}\n"
        f"📅 Period: {period_display}\n"
        f"📆 Days with data: {days_count}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"{table}"
        f"📈 *Summary*\n"
        f"• DO    :    {total_diesel:>8,.2f}L  | {diesel_pct:>4.1f}%\n"
        f"• EA92 :    {total_regular:>8,.2f}L | {regular_pct:>4.1f}%\n"
        f"• EA95 :    {total_super:>8,.2f}L  | {super_pct:>4.1f}%\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"• Total       :    {total_volume:,.2f}L\n"
        f"• Avg/Day :   {avg_daily:,.2f}L"
    )
    
    return msg
# ==================== HANDLERS ====================

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler."""
    welcome = (
        "🤖 *FUEL REPORT BOT*\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "Manage and view fuel station reports.\n\n"
        "📥 *How to add data:*\n"
        "Just send a daily fuel report text\n\n"
        "📊 *View reports:*\n"
        "Use /report or buttons below"
    )
    
    await update.message.reply_text(
        welcome,
        parse_mode="Markdown",
        reply_markup=create_main_menu()
    )

async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Report command - shortcut to main menu."""
    await update.message.reply_text(
        "📊 *VIEW REPORTS*\nSelect an option:",
        parse_mode="Markdown",
        reply_markup=create_main_menu()
    )

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all callback queries."""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    # Main menu navigation
    if data == "main_menu":
        await query.edit_message_text(
            "📊 *MAIN MENU*\nSelect an option:",
            parse_mode="Markdown",
            reply_markup=create_main_menu()
        )
        return
    
    elif data == "view_reports":
        stations = get_all_stations()
        if not stations:
            await query.edit_message_text(
                "📭 *No stations found*\nSend fuel reports first.",
                parse_mode="Markdown"
            )
            return
        
        await query.edit_message_text(
            "🏪 *SELECT STATION*\nChoose a station:",
            parse_mode="Markdown",
            reply_markup=create_station_keyboard()
        )
        return
    
        """ elif data == "quick_stats":
        stations = get_all_stations()
        today = format_date(date.today() - timedelta(days=1))
        
        # Get today's total
        rows = get_summary(today, today)
        today_total = sum(row["volume"] for row in rows) if rows else 0
        
        msg = (
            f"📈 *QUICK STATS*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🏪 Stations: {len(stations)}\n"
            f"📅 Today's total: {today_total:,.2f}L\n"
            f"📊 Total reports: {len(rows) if rows else 0}\n\n"
            f"Use /report for detailed reports"
        )
        await query.edit_message_text(msg, parse_mode="Markdown")
        return 
        elif data == "all_stations":
        stations = get_all_stations()
        if not stations:
            await query.edit_message_text("📭 No stations found")
            return
        
        msg = "🏪 *ALL STATIONS*\n━━━━━━━━━━━━━━━━━━━━━\n"
        for i, station in enumerate(stations, 1):
            msg += f"{i}. {station}\n"
        
        await query.edit_message_text(msg, parse_mode="Markdown")
        return
    
        elif data == "test_db":
        stations = get_all_stations()
        today = format_date(date.today())
        rows = get_summary(today, today) or []
        
        msg = (
            f"🧪 *DATABASE TEST*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ Connection: OK\n"
            f"🏪 Stations: {len(stations)}\n"
            f"📅 Today's reports: {len(rows)}\n"
            f"📊 Today's volume: {sum(r['volume'] for r in rows):,.2f}L"
        )
        await query.edit_message_text(msg, parse_mode="Markdown")
        return
        """
    # Cancel
    elif data == "cancel":
        await query.edit_message_text("❌ Cancelled")
        return
    
    # Station selection
    elif data.startswith("station:"):
        station_idx = data.split(":")[1]
        station = user_selections["station_mapping"].get(station_idx)
        
        if not station:
            await query.edit_message_text("❌ Station not found")
            return
        
        await query.edit_message_text(
            f"⛽ *{station}*\nSelect date option:",
            parse_mode="Markdown",
            reply_markup=create_quick_dates_keyboard(station)
        )
        return
    
    # Date selection shortcuts
    elif data.startswith("dates:"):
        station = data.split(":")[1]
        await query.edit_message_text(
            f"⛽ *{station}*\nSelect date option:",
            parse_mode="Markdown",
            reply_markup=create_quick_dates_keyboard(station)
        )
        return
    
    # Single date report
    elif data.startswith("date:"):
        _, station, date_str = data.split(":")
        await query.edit_message_text(f"⏳ Loading {date_str}...", parse_mode="Markdown")
        
        fuels = generate_daily_report(station, date_str)
        if fuels:
            msg = format_daily_summary(station, date_str, fuels)
            await query.edit_message_text(msg, parse_mode="Markdown")
        else:
            await query.edit_message_text(f"⚠️ No data for {station} on {date_str}")
        return
    
    # Range report
    elif data.startswith("range:"):
        _, station, days_str = data.split(":")
        days = int(days_str)
        
        await query.edit_message_text(f"⏳ Loading {days} days...", parse_mode="Markdown")
        
        report_data = generate_range_report(station, days)
        if report_data:
            msg = format_range_summary(station, report_data)
            await query.edit_message_text(msg, parse_mode="Markdown")
        else:
            await query.edit_message_text(f"⚠️ No data for {station} in last {days} days")
        return
    
    # Month selection
    elif data.startswith("select_month:"):
        station = data.split(":")[1]
        await query.edit_message_text(
            f"⛽ *{station}*\nSelect month:",
            parse_mode="Markdown",
            reply_markup=create_month_selector(station)
        )
        return
    
    # Month year navigation
    elif data.startswith("month_year:"):
        _, station, year_str = data.split(":")
        year = int(year_str)
        
        await query.edit_message_text(
            f"⛽ *{station}*\nSelect month:",
            parse_mode="Markdown",
            reply_markup=create_month_selector(station, year)
        )
        return
    
    # Month selection with table
    elif data.startswith("month:"):
        _, station, year_str, month_str = data.split(":")
        year, month = int(year_str), int(month_str)
        
        # Get month range
        start_date = date(year, month, 1)
        if month == 12:
            end_date = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = date(year, month + 1, 1) - timedelta(days=1)
        
        start_str = format_date(start_date)
        end_str = format_date(end_date)
        
        await query.edit_message_text(f"⏳ Loading {year}/{month:02d}...", parse_mode="Markdown")
        
        rows = get_summary(start_str, end_str)
        if not rows:
            await query.edit_message_text(f"⚠️ No data for {station} in {year}/{month:02d}")
            return
        
        # Filter for this station
        station_rows = [r for r in rows if r["station_name"] == station]
        if not station_rows:
            await query.edit_message_text(f"⚠️ No data for {station} in {year}/{month:02d}")
            return
        
        # Create table format monthly report
        msg = format_monthly_report_table(station, year, month, station_rows)
        await query.edit_message_text(msg, parse_mode="Markdown")
        return
def format_date_display(date_str: str) -> str:
    """Convert yyyy/mm/dd to dd/mm format for display."""
    try:
        # Direct parsing without datetime for efficiency
        if '/' in date_str:
            parts = date_str.split('/')
            if len(parts) >= 3:
                day = parts[2].strip()
                month = parts[1].strip()
                # Ensure 2 digits
                if len(day) == 1:
                    day = f"0{day}"
                if len(month) == 1:
                    month = f"0{month}"
                return f"{day}/{month}"
        
        # Fallback to parse_date_string
        date_obj = parse_date_string(date_str)
        return f"{date_obj.day:02d}/{date_obj.month:02d}"
    except:
        return date_str

def format_monthly_report_table(station: str, year: int, month: int, station_rows: list) -> str:
    """Format monthly report with table showing daily data in dd/mm format."""
    if not station_rows:
        return f"⚠️ No data for {station} in {year}/{month:02d}"
    
    # Organize data by date
    daily_data = {}
    total_diesel = total_regular = total_super = 0
    
    for row in station_rows:
        date_str = row["report_date"]
        fuel_type = row["fuel_type"]
        volume = row["volume"]
        
        if date_str not in daily_data:
            daily_data[date_str] = {"DO": 0, "EA92": 0, "EA95": 0}
        
        # Normalize fuel names to DO, EA92, EA95
        if "Diesel" in fuel_type or "Diesel" in fuel_type.upper() or "DO" in fuel_type.upper():
            daily_data[date_str]["DO"] += volume
            total_diesel += volume
        elif "Regular" in fuel_type or "92" in fuel_type:
            daily_data[date_str]["EA92"] += volume
            total_regular += volume
        elif "Super" in fuel_type or "95" in fuel_type:
            daily_data[date_str]["EA95"] += volume
            total_super += volume
    
    # Sort dates
    sorted_dates = sorted(daily_data.keys())
    days = len(sorted_dates)
    total_volume = total_diesel + total_regular + total_super
    avg_daily = total_volume / days if days > 0 else 0
    
    # Calculate percentages
    diesel_pct = (total_diesel / total_volume * 100) if total_volume > 0 else 0
    regular_pct = (total_regular / total_volume * 100) if total_volume > 0 else 0
    super_pct = (total_super / total_volume * 100) if total_volume > 0 else 0
    
    # Build table with dd/mm format
    table = "```\n"
    table += f"{'Date':<5} | {'DO':>7} | {'EA92':>7} | {'EA95':>7} | {'Total':>8}\n"
    table += "-" * 46 + "\n"
    
    for date_str in sorted_dates:
        fuels = daily_data[date_str]
        diesel = fuels.get("DO", 0)
        regular = fuels.get("EA92", 0)
        super_fuel = fuels.get("EA95", 0)
        daily_total = diesel + regular + super_fuel
        
        # Format date as dd/mm
        display_date = format_date_display(date_str)
        
        table += f"{display_date:<5} | "
        table += f"{diesel:>7.2f} | "
        table += f"{regular:>7.2f} | "
        table += f"{super_fuel:>7.2f} | "
        table += f"{daily_total:>8.2f}\n"
    
    # Add totals row
    table += "-" * 46 + "\n"
    table += f"{'TOTAL':<5} | "
    table += f"{total_diesel:>7.2f} | "
    table += f"{total_regular:>7.2f} | "
    table += f"{total_super:>7.2f} | "
    table += f"{total_volume:>8.2f}\n"
    table += "```\n"
    
    # Full month name for better display
    month_names = ["January", "February", "March", "April", "May", "June",
                   "July", "August", "September", "October", "November", "December"]
    month_name = month_names[month - 1]
    
    # Build message
    msg = (
        f"📊 *MONTHLY REPORT*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏪 {station}\n"
        f"📅 {month_name} {year}\n"
        f"📆 Days with data: {days}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"{table}"
        f"📈 *Summary*\n"
        f"Diesel    :     {total_diesel:,.2f}L\n"
        f"Regular   :     {total_regular:,.2f}L\n"
        f"Super     :     {total_super:,.2f}L\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"Total:    {total_volume:,.2f}L\n"
        f"Avg/Day:  {avg_daily:,.2f}L\n"
        f"━━━━━━━━━━━━━━━━━━━━━"
    )
    
    return msg
def format_monthly_report_table(station: str, year: int, month: int, station_rows: list) -> str:
    """Format monthly report with table showing daily data in dd/mm format."""
    if not station_rows:
        return f"⚠️ No data for {station} in {year}/{month:02d}"
    
    # Organize data by date
    daily_data = {}
    total_diesel = total_regular = total_super = 0
    
    for row in station_rows:
        date_str = row["report_date"]
        fuel_type = row["fuel_type"]
        volume = row["volume"]
        
        if date_str not in daily_data:
            daily_data[date_str] = {"DO": 0, "EA92": 0, "EA95": 0}
        
        # Normalize fuel names to DO, EA92, EA95
        if "Diesel" in fuel_type or "Diesel" in fuel_type.upper() or "DO" in fuel_type.upper():
            daily_data[date_str]["DO"] += volume
            total_diesel += volume
        elif "Regular" in fuel_type or "92" in fuel_type:
            daily_data[date_str]["EA92"] += volume
            total_regular += volume
        elif "Super" in fuel_type or "95" in fuel_type:
            daily_data[date_str]["EA95"] += volume
            total_super += volume
    
    # Sort dates
    sorted_dates = sorted(daily_data.keys())
    days = len(sorted_dates)
    total_volume = total_diesel + total_regular + total_super
    avg_daily = total_volume / days if days > 0 else 0
    
    # Calculate percentages
    diesel_pct = (total_diesel / total_volume * 100) if total_volume > 0 else 0
    regular_pct = (total_regular / total_volume * 100) if total_volume > 0 else 0
    super_pct = (total_super / total_volume * 100) if total_volume > 0 else 0
    
    # Build table with dd/mm format
    table = "```\n"
    table += f"{'Date':<5} | {'DO':>4} | {'EA92':>7} | {'EA95':>5} | {'Total':>6}\n"
    table += "-" * 44 + "\n"
    
    for date_str in sorted_dates:
        fuels = daily_data[date_str]
        diesel = fuels.get("DO", 0)
        regular = fuels.get("EA92", 0)
        super_fuel = fuels.get("EA95", 0)
        daily_total = diesel + regular + super_fuel
        
        # Format date as dd/mm
        display_date = format_date_display(date_str)
        
        table += f"{display_date:<5} | "
        table += f"{diesel:>4.2f} | "
        table += f"{regular:>7.2f} | "
        table += f"{super_fuel:>5.2f} | "
        table += f"{daily_total:>6.2f}\n"
    
    # Add totals row
    table += "-" * 44 + "\n"
    table += f"{'TOTAL':<5} | "
    table += f"{total_diesel:>4.2f} | "
    table += f"{total_regular:>7.2f} | "
    table += f"{total_super:>5.2f} | "
    table += f"{total_volume:>6.2f}\n"
    table += "```\n"
    
    # Full month name for better display
    month_names = ["January", "February", "March", "April", "May", "June",
                   "July", "August", "September", "October", "November", "December"]
    month_name = month_names[month - 1]
    
    # Build message
    msg = (
        f"📊 *MONTHLY REPORT*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏪 {station}\n"
        f"📅 {month_name} {year}\n"
        f"📆 Days with data: {days}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"{table}"
        f"📈 *Summary*\n"
        f"• DO       :    {total_diesel:>8,.2f}L     | {diesel_pct:>5.1f}%\n"
        f"• EA92   :    {total_regular:>8,.2f}L    | {regular_pct:>5.1f}%\n"
        f"• EA95   :    {total_super:>8,.2f}L    | {super_pct:>5.1f}%\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"• Total       :     {total_volume:,.2f} L\n"
        f"• Avg/Day :     {avg_daily:,.2f} L"
    )
    
    return msg

async def handle_report_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle incoming fuel report messages."""
    try:
        if not update.message.text:
            return
        
        # Parse report
        parsed = parse_daily_report(update.message.text)
        data = format_for_database(parsed)
        
        station = data.get("station_name")
        report_date = data.get("report_date")
        
        if not station or not report_date:
            await update.message.reply_text("❌ Could not parse report")
            return
        
        # Check duplicate
        if report_exists(station, report_date):
            await update.message.reply_text(f"⚠️ Report for {station} on {report_date} already exists")
            return
        
        # Save report
        if save_report(data):
            await update.message.reply_text(
                f"✅ Report saved!\n"
                f"🏪 {station}\n"
                f"📅 {report_date}\n"
                f"📊 Use /report to view"
            )
        else:
            await update.message.reply_text("❌ Failed to save report")
            
    except Exception as e:
        logger.error(f"Error processing report: {e}")
        await update.message.reply_text("❌ Error processing report")

async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text input - either fuel report or command response."""
    # Check if it's a command
    if update.message.text.startswith('/'):
        return
    
    # Otherwise treat as fuel report
    await handle_report_message(update, context)

# ==================== MAIN ====================

def main():
    """Start the bot."""
    # Initialize
    init_db()
    
    # Create app
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Add handlers
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("report", report_command))
    app.add_handler(CommandHandler("help", start_command))
    
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))
    
    # Start
    logger.info("🤖 Bot starting...")
    app.run_polling()

if __name__ == "__main__":
    main()