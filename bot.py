import os
import logging
import traceback
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
from collections import defaultdict
import calendar
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
from database import init_db, save_report, report_exists, get_summary, get_all_stations, get_station_statistics
from database import get_monthly_details, get_monthly_aggregate, get_monthly_station_summary, get_date_range_summary
from parser import parse_daily_report, format_for_database

# ---------------- SETUP LOGGING ----------------
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ---------------- LOAD ENV ----------------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    logger.error("BOT_TOKEN not found in environment variables!")
    exit(1)

# ---------------- DATE FORMAT UTILS ----------------
# Use yyyy/mm/dd format for all dates
DATE_FORMAT = "%Y/%m/%d"  # Unified date format for all purposes

def format_date(date_obj: date) -> str:
    """Format date to yyyy/mm/dd format."""
    return date_obj.strftime(DATE_FORMAT)

def parse_date_string(date_str: str) -> date:
    """Parse date string from various formats to date object (yyyy/mm/dd output)."""
    if not date_str or date_str.strip() == "":
        raise ValueError("Empty date string")
    
    # Check if it's a format string like %Y/%m/%d
    if date_str.startswith('%'):
        # Try to extract date from actual data or return a default
        logger.warning(f"Received format string instead of date: {date_str}")
        # Return today's date as fallback
        return date.today()
    
    date_formats = [
        "%Y/%m/%d",  # yyyy/mm/dd (primary)
        "%d/%m/%Y",  # dd/mm/yyyy
        "%d/%m/%y",  # dd/mm/yy
        "%Y-%m-%d",  # yyyy-mm-dd
        "%d-%b-%Y",  # dd-Mon-yyyy
        "%d-%m-%Y",  # dd-mm-yyyy
        "%d.%m.%Y",  # dd.mm.yyyy
        "%m/%d/%Y",  # mm/dd/yyyy (US format)
        "%Y.%m.%d",  # yyyy.mm.dd
    ]
    
    # Try standard formats first
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    # Try to clean and parse
    try:
        # Remove time part if present
        if ' ' in date_str:
            date_str = date_str.split(' ')[0]
        
        # Replace common separators with slashes
        date_str = date_str.replace('.', '/').replace('-', '/')
        
        # Try with cleaned string
        for fmt in ["%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y"]:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
    except Exception as e:
        logger.warning(f"Date parsing error for '{date_str}': {e}")
    
    # Last resort: extract numbers
    try:
        import re
        numbers = re.findall(r'\d+', date_str)
        if len(numbers) >= 3:
            # Try different orderings
            for order in [(0, 1, 2), (2, 1, 0), (2, 0, 1)]:  # yyyy,mm,dd | dd,mm,yyyy | yyyy,dd,mm
                try:
                    y, m, d = int(numbers[order[0]]), int(numbers[order[1]]), int(numbers[order[2]])
                    
                    # Normalize year
                    if y < 100:  # 2-digit year
                        y += 2000
                    elif y < 1000:  # 3-digit year
                        y += 1900
                    
                    # Validate month and day
                    if 1 <= m <= 12 and 1 <= d <= 31:
                        return date(y, m, d)
                except ValueError:
                    continue
    except:
        pass
    
    raise ValueError(f"Invalid date format: {date_str}. Please use yyyy/mm/dd format.")

def normalize_date(date_input) -> str:
    """Normalize date to yyyy/mm/dd format."""
    if isinstance(date_input, date):
        return format_date(date_input)
    elif isinstance(date_input, str):
        date_obj = parse_date_string(date_input)
        return format_date(date_obj)
    else:
        raise ValueError(f"Invalid date input type: {type(date_input)}")

def get_today() -> str:
    """Get today's date in yyyy/mm/dd format."""
    return format_date(date.today())

def get_yesterday() -> str:
    """Get yesterday's date in yyyy/mm/dd format."""
    yesterday = date.today() - timedelta(days=1)
    return format_date(yesterday)

def get_last_7_days() -> tuple[str, str]:
    """Get last 7 days date range in yyyy/mm/dd format."""
    end_date = date.today()
    start_date = end_date - timedelta(days=6)
    return format_date(start_date), format_date(end_date)

def get_last_30_days() -> tuple[str, str]:
    """Get last 30 days date range in yyyy/mm/dd format."""
    end_date = date.today()
    start_date = end_date - timedelta(days=29)
    return format_date(start_date), format_date(end_date)

def get_month_range(year: int, month: int) -> tuple[str, str]:
    """Get date range for specific month and year in yyyy/mm/dd format."""
    # First day of month
    first_day = date(year, month, 1)
    # Last day of month
    if month == 12:
        last_day = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    return format_date(first_day), format_date(last_day)

def get_current_month_range() -> tuple[str, str]:
    """Get current month date range in yyyy/mm/dd format."""
    today = date.today()
    return get_month_range(today.year, today.month)

def get_current_month_name() -> str:
    """Get current month name."""
    today = date.today()
    return calendar.month_name[today.month]

def get_month_name(month: int) -> str:
    """Get month name by number (1-12)."""
    return calendar.month_name[month]

def format_date_display(date_str: str) -> str:
    """Format date for display (yyyy/mm/dd to more readable format)."""
    try:
        date_obj = parse_date_string(date_str)
        return date_obj.strftime("%Y/%m/%d")  # Keep consistent format
    except:
        return date_str

def safe_date_sort(date_str: str) -> date:
    """Safely parse date string for sorting, with fallback."""
    try:
        return parse_date_string(date_str)
    except:
        # Return a very old date for invalid dates so they appear first
        return date(1900, 1, 1)

# ---------------- STATE MANAGEMENT ----------------
user_selections = defaultdict(dict)

# ---------------- MONTH SELECTION FUNCTIONS ----------------
def create_month_selection_keyboard(station: str, year: int = None) -> InlineKeyboardMarkup:
    """Create a month selection keyboard."""
    if year is None:
        year = date.today().year
    
    safe_station = station[:20]
    
    # Create 3x4 grid of months
    months = [
        ["January", "February", "March", "April"],
        ["May", "June", "July", "August"],
        ["September", "October", "November", "December"]
    ]
    
    keyboard = []
    
    # Year navigation
    keyboard.append([
        InlineKeyboardButton("◀️", callback_data=f"month_year:{safe_station}:{year-1}"),
        InlineKeyboardButton(f"📅 {year}", callback_data="ignore"),
        InlineKeyboardButton("▶️", callback_data=f"month_year:{safe_station}:{year+1}")
    ])
    
    # Add months
    for row in months:
        month_row = []
        for month_name in row:
            month_num = list(calendar.month_name).index(month_name)
            month_short = month_name[:3]  # Jan, Feb, etc.
            
            # Highlight current month
            current_month = (year == date.today().year and month_num == date.today().month)
            button_text = f"•{month_short}•" if current_month else month_short
            
            month_row.append(InlineKeyboardButton(
                button_text,
                callback_data=f"select_month:{safe_station}:{year}:{month_num}"
            ))
        keyboard.append(month_row)
    
    # Additional buttons
    keyboard.append([
        InlineKeyboardButton("📅 Current Month", callback_data=f"current_month:{safe_station}"),
        InlineKeyboardButton("◀️ Back", callback_data=f"back_to_dates:{safe_station}"),
        InlineKeyboardButton("❌ Cancel", callback_data="cancel")
    ])
    
    return InlineKeyboardMarkup(keyboard)

def create_month_confirmation_keyboard(station: str, year: int, month: int) -> InlineKeyboardMarkup:
    """Create confirmation keyboard for selected month."""
    safe_station = station[:20]
    month_name = calendar.month_name[month]
    start_date, end_date = get_month_range(year, month)
    
    keyboard = [
        [
            InlineKeyboardButton(f"✅ {month_name} {year}", callback_data=f"confirm_month:{safe_station}:{year}:{month}"),
            InlineKeyboardButton("🔄 Change", callback_data=f"change_month:{safe_station}")
        ],
        [
            InlineKeyboardButton("📅 Pick Another Month", callback_data=f"select_month_range:{safe_station}"),
            InlineKeyboardButton("◀️ Back", callback_data=f"back_to_dates:{safe_station}")
        ]
    ]
    
    return InlineKeyboardMarkup(keyboard)

def create_month_report_type_keyboard(station: str, year: int, month: int) -> InlineKeyboardMarkup:
    """Create keyboard to choose monthly report type."""
    safe_station = station[:20]
    month_name = calendar.month_name[month]
    
    keyboard = [
        [
            InlineKeyboardButton("📊 Summary", callback_data=f"month_summary:{safe_station}:{year}:{month}"),
            InlineKeyboardButton("📈 Detailed", callback_data=f"month_detailed:{safe_station}:{year}:{month}")
        ],
        [
            InlineKeyboardButton("📋 Day-by-Day", callback_data=f"month_daybyday:{safe_station}:{year}:{month}"),
            InlineKeyboardButton("📅 All Data", callback_data=f"month_alldata:{safe_station}:{year}:{month}")
        ],
        [
            InlineKeyboardButton("◀️ Back", callback_data=f"select_month:{safe_station}:{year}:{month}"),
            InlineKeyboardButton("🏠 Main Menu", callback_data="back_to_stations")
        ]
    ]
    
    return InlineKeyboardMarkup(keyboard)

# ---------------- CALENDAR FUNCTIONS ----------------
def create_calendar_keyboard(station: str, year: int, month: int) -> InlineKeyboardMarkup:
    """Create a proper calendar keyboard using Python's calendar module."""
    # Get month calendar (list of weeks)
    cal = calendar.monthcalendar(year, month)
    month_name = calendar.month_name[month]
    
    # Create keyboard
    keyboard = []
    
    # Month navigation row
    prev_month = month - 1 if month > 1 else 12
    prev_year = year if month > 1 else year - 1
    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1
    
    # Use safe station name
    safe_station = station[:15]
    
    keyboard.append([
        InlineKeyboardButton("◀️", callback_data=f"calendar:{safe_station}:{prev_year}:{prev_month}"),
        InlineKeyboardButton(f"{month_name} {year}", callback_data="ignore"),
        InlineKeyboardButton("▶️", callback_data=f"calendar:{safe_station}:{next_year}:{next_month}")
    ])
    
    # Weekday headers
    weekdays = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    keyboard.append([InlineKeyboardButton(day, callback_data="ignore") for day in weekdays])
    
    # Add weeks
    today = date.today()
    for week in cal:
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(" ", callback_data="ignore"))
            else:
                current_date = date(year, month, day)
                date_display = format_date(current_date)  # yyyy/mm/dd format
                
                # Highlight today
                if current_date == today:
                    button_text = f"•{day}•"
                elif current_date < today:
                    # Past dates
                    button_text = f"{day}"
                else:
                    # Future dates
                    button_text = f"{day}"
                
                # Create callback data
                callback_data = f"caldate:{safe_station}:{date_display}"
                
                # Ensure callback data doesn't exceed 64 bytes
                if len(callback_data.encode('utf-8')) > 64:
                    callback_data = f"cd:{safe_station[:10]}:{date_display}"
                    if len(callback_data.encode('utf-8')) > 64:
                        callback_data = f"c:{safe_station[:5]}:{date_display}"
                
                row.append(InlineKeyboardButton(
                    button_text,
                    callback_data=callback_data
                ))
        keyboard.append(row)
    
    # Additional navigation buttons
    keyboard.append([
        InlineKeyboardButton("◀️ Back", callback_data=f"back_cal:{safe_station}"),
        InlineKeyboardButton("📅 Today", callback_data=f"today_cal:{safe_station}"),
        InlineKeyboardButton("📆 Monthly", callback_data=f"select_month_range:{safe_station}"),
        InlineKeyboardButton("❌ Cancel", callback_data="cancel")
    ])
    
    # Quick navigation row
    keyboard.append([
        InlineKeyboardButton("Prev Year", callback_data=f"calyear:{safe_station}:{year-1}:{month}"),
        InlineKeyboardButton(f"Year {year}", callback_data="ignore"),
        InlineKeyboardButton("Next Year", callback_data=f"calyear:{safe_station}:{year+1}:{month}")
    ])
    
    return InlineKeyboardMarkup(keyboard)

def create_simple_date_keyboard(station: str, selected_date: date = None) -> InlineKeyboardMarkup:
    """
    Create a simple date selection keyboard (no full calendar).
    """
    if selected_date is None:
        selected_date = date.today()
    
    safe_station = station[:20]
    
    # Format dates in yyyy/mm/dd
    today_fmt = format_date(date.today())
    yesterday_fmt = format_date(date.today() - timedelta(days=1))
    tomorrow_fmt = format_date(date.today() + timedelta(days=1))
    selected_fmt = format_date(selected_date)
    
    keyboard = [
        [
            InlineKeyboardButton("📅 Open Calendar", 
                                callback_data=f"open_cal:{safe_station}:{selected_date.year}:{selected_date.month}")
        ],
        [
            InlineKeyboardButton("Yesterday", callback_data=f"quickdate:{safe_station}:{yesterday_fmt}"),
            InlineKeyboardButton("Today", callback_data=f"quickdate:{safe_station}:{today_fmt}"),
            InlineKeyboardButton("Tomorrow", callback_data=f"quickdate:{safe_station}:{tomorrow_fmt}")
        ],
        [
            InlineKeyboardButton("◀️ Previous Day", 
                                callback_data=f"prevday:{safe_station}:{selected_fmt}"),
            InlineKeyboardButton("Next Day ▶️", 
                                callback_data=f"nextday:{safe_station}:{selected_fmt}")
        ],
        [
            InlineKeyboardButton("📆 Select Monthly", callback_data=f"select_month_range:{safe_station}"),
            InlineKeyboardButton("📅 Custom Date...", callback_data=f"custom_date:{safe_station}")
        ],
        [
            InlineKeyboardButton("🏠 Main Menu", callback_data="back_to_stations"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel")
        ]
    ]
    
    # Show selected date
    if selected_date != date.today():
        keyboard.insert(1, [
            InlineKeyboardButton(f"Selected: {selected_fmt}", callback_data="ignore")
        ])
    
    return InlineKeyboardMarkup(keyboard)

def create_date_confirmation_keyboard(station: str, selected_date: date) -> InlineKeyboardMarkup:
    """
    Create confirmation keyboard for selected date.
    """
    safe_station = station[:20]
    date_fmt = format_date(selected_date)
    
    keyboard = [
        [
            InlineKeyboardButton(f"✅ Confirm {date_fmt}", 
                                callback_data=f"confirm_date:{safe_station}:{date_fmt}"),
            InlineKeyboardButton("🔄 Change Date", 
                                callback_data=f"change_date:{safe_station}")
        ],
        [
            InlineKeyboardButton("📆 Monthly View", 
                                callback_data=f"select_month_range:{safe_station}"),
            InlineKeyboardButton("◀️ Back to Calendar", 
                                callback_data=f"back_calendar:{safe_station}:{selected_date.year}:{selected_date.month}")
        ],
        [
            InlineKeyboardButton("🏠 Main Menu", callback_data="back_to_stations")
        ]
    ]
    
    return InlineKeyboardMarkup(keyboard)

# ---------------- KEYBOARD HELPERS ----------------
def create_station_keyboard() -> InlineKeyboardMarkup:
    """Create inline keyboard with all stations."""
    try:
        stations = get_all_stations()
        
        if not stations:
            logger.warning("No stations found in database")
            return InlineKeyboardMarkup([[InlineKeyboardButton("📭 No stations available", callback_data="ignore")]])
        
        logger.info(f"Creating keyboard with {len(stations)} stations")
        
        # Create buttons in rows of 2
        keyboard = []
        row = []
        for i, station in enumerate(stations):
            try:
                # Truncate station name for display
                display_name = station[:15] + "..." if len(station) > 15 else station
                
                # Create a safe callback data
                station_index = str(i)
                user_selections["station_mapping"][station_index] = station
                
                # Ensure callback data is within Telegram limits
                callback_data = f"station:{station_index}"
                if len(callback_data.encode('utf-8')) > 64:
                    callback_data = f"s:{station_index}"
                
                row.append(InlineKeyboardButton(f"⛽ {display_name}", callback_data=callback_data))
                
                if len(row) == 2 or i == len(stations) - 1:
                    keyboard.append(row)
                    row = []
            except Exception as e:
                logger.error(f"Error creating button for station {station}: {e}")
                continue
        
        # Add navigation buttons
        keyboard.append([
            InlineKeyboardButton("❌ Cancel", callback_data="cancel")
        ])
        
        return InlineKeyboardMarkup(keyboard)
        
    except Exception as e:
        logger.error(f"Error creating station keyboard: {e}")
        logger.error(traceback.format_exc())
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("⚠️ Error loading stations", callback_data="ignore")],
            [InlineKeyboardButton("🔄 Refresh", callback_data="refresh_stations")]
        ])

def create_date_keyboard(station: str) -> InlineKeyboardMarkup:
    """Create date selection keyboard with common date options including Monthly."""
    today_display = get_today()
    yesterday_display = get_yesterday()
    
    # Create safe callback data
    safe_station = station[:20]
    
    keyboard = [
        [InlineKeyboardButton("📅 Today", callback_data=f"date_single:{safe_station}:{today_display}")],
        [InlineKeyboardButton("📅 Yesterday", callback_data=f"date_single:{safe_station}:{yesterday_display}")],
        [InlineKeyboardButton("📅 Last 7 days", callback_data=f"date_range:{safe_station}:7")],
        [InlineKeyboardButton("📅 Last 30 days", callback_data=f"date_range:{safe_station}:30")],
        [InlineKeyboardButton("📆 Select Monthly", callback_data=f"select_month_range:{safe_station}")],
        [InlineKeyboardButton("📅 Open Calendar", callback_data=f"calendar:{safe_station}:{date.today().year}:{date.today().month}")],
        [
            InlineKeyboardButton("◀️ Back", callback_data="back_to_stations"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel")
        ]
    ]
    
    # Verify callback data lengths
    for row in keyboard:
        for button in row:
            if len(button.callback_data.encode('utf-8')) > 64:
                logger.warning(f"Button callback data too long: {button.callback_data}")
                # Shorten if necessary
                if button.callback_data.startswith("calendar:"):
                    parts = button.callback_data.split(":")
                    if len(parts) >= 4:
                        safe_station_short = safe_station[:10]
                        button.callback_data = f"calendar:{safe_station_short}:{parts[2]}:{parts[3]}"
    
    return InlineKeyboardMarkup(keyboard)

# Initialize station mapping
user_selections["station_mapping"] = {}

# ---------------- REPORT FORMATTING ----------------
def generate_summary_by_period(start_date: str, end_date: str, station: str = None):
    """Generate summary for specific period and station. Dates in yyyy/mm/dd format."""
    try:
        # Parse dates
        start_date_obj = parse_date_string(start_date)
        end_date_obj = parse_date_string(end_date)
        
        # Use yyyy/mm/dd format for database query
        start_date_db = format_date(start_date_obj)
        end_date_db = format_date(end_date_obj)
        
        logger.debug(f"Getting summary for period: {start_date_db} to {end_date_db}")
        
        # Use get_date_range_summary for better performance
        rows = get_summary(start_date_db, end_date_db)
        
        if not rows:
            logger.info(f"No data found for period {start_date_db} to {end_date_db}")
            return None, []
        
        logger.debug(f"Got {len(rows)} rows from database")
        
        # Filter by station if specified
        if station:
            filtered_rows = []
            for row in rows:
                if isinstance(row, dict) and row.get("station_name") == station:
                    filtered_rows.append(row)
            rows = filtered_rows
            logger.debug(f"Filtered to {len(rows)} rows for station {station}")
        
        # Organize data by station -> date -> fuel_type
        station_data = {}
        for row in rows:
            try:
                station_name = row.get("station_name", "")
                fuel_type = row.get("fuel_type", "")
                volume = row.get("volume", 0)
                report_date = row.get("report_date", "")
                
                if not station_name or not report_date:
                    continue
                    
                if station_name not in station_data:
                    station_data[station_name] = {}
                
                # Ensure date is in yyyy/mm/dd format
                if report_date not in station_data[station_name]:
                    station_data[station_name][report_date] = {}
                
                # Use English fuel type names
                if "Diesel" in fuel_type or "DO" in fuel_type.upper():
                    fuel_key = "Diesel"
                elif "Regular" in fuel_type or "Regular" in fuel_type:
                    fuel_key = "Regular"
                elif "Super" in fuel_type or "Super" in fuel_type:
                    fuel_key = "Super"
                else:
                    # Use original fuel type as fallback
                    fuel_key = fuel_type
                
                # Sum volumes for same fuel type on same date
                if fuel_key in station_data[station_name][report_date]:
                    station_data[station_name][report_date][fuel_key] += volume
                else:
                    station_data[station_name][report_date][fuel_key] = volume
                
            except Exception as e:
                logger.warning(f"Error processing row: {e}")
                continue
        
        logger.debug(f"Organized data for {len(station_data)} stations")
        return station_data, rows
        
    except Exception as e:
        logger.error(f"Error in generate_summary_by_period: {e}")
        logger.error(traceback.format_exc())
        return None, []

def generate_monthly_summary(year: int, month: int, station: str = None):
    """Generate monthly summary using detailed date range queries."""
    try:
        # Get month range
        start_date_obj = date(year, month, 1)
        if month == 12:
            end_date_obj = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date_obj = date(year, month + 1, 1) - timedelta(days=1)
        
        start_date = format_date(start_date_obj)
        end_date = format_date(end_date_obj)
        
        # Use existing function that works
        return generate_summary_by_period(start_date, end_date, station)
        
    except Exception as e:
        logger.error(f"Error in generate_monthly_summary: {e}")
        logger.error(traceback.format_exc())
        return None, []

def format_monthly_summary(station: str, year: int, month: int, station_data: dict) -> str:
    """Format a monthly summary report."""
    if not station_data or station not in station_data:
        month_name = calendar.month_name[month]
        return f"⚠️ *No data found for {station} in {month_name} {year}*"
    
    dates_data = station_data[station]
    month_name = calendar.month_name[month]
    
    # Calculate totals
    total_diesel = total_regular = total_super = 0
    total_all = 0
    days_with_data = len(dates_data)
    
    for date_str, fuels in dates_data.items():
        # Get values from aggregated data using English names
        diesel = fuels.get("Diesel", 0)
        regular = fuels.get("Regular", 0)
        super = fuels.get("Super", 0)
        
        total_diesel += diesel
        total_regular += regular
        total_super += super

    total_all = total_diesel + total_regular + total_super

    if total_all == 0:
        return f"⚠️ *No data found for {station} in {month_name} {year}*"
    
    # Get total days in month
    _, total_days_in_month = calendar.monthrange(year, month)
    
    # Calculate percentages
    diesel_percent = (total_diesel / total_all * 100) if total_all > 0 else 0
    regular_percent = (total_regular / total_all * 100) if total_all > 0 else 0
    super_percent = (total_super / total_all * 100) if total_all > 0 else 0
    
    # Calculate daily averages
    avg_diesel = total_diesel / days_with_data if days_with_data > 0 else 0
    avg_regular = total_regular / days_with_data if days_with_data > 0 else 0
    avg_super = total_super / days_with_data if days_with_data > 0 else 0
    avg_total = total_all / days_with_data if days_with_data > 0 else 0
    
    # Format with proper alignment
    msg = (
        f"📊 *MONTHLY SUMMARY*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏪 *Station:* {station}\n"
        f"📅 *Month:* {month_name} {year}\n"
        f"📆 *Days in month:* {total_days_in_month}\n"
        f"📈 *Days with data:* {days_with_data}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"```\n"
        f"Product | Total      | Avg/Day    | %\n"
        f"--------------------------------------------\n"
        f"Diesel  | {total_diesel:>8.2f}L  | {avg_diesel:>8.2f}L  | {diesel_percent:>4.1f}%\n"
        f"Regular | {total_regular:>8.2f}L  | {avg_regular:>8.2f}L  | {regular_percent:>4.1f}%\n"
        f"Super   | {total_super:>8.2f}L  | {avg_super:>8.2f}L  | {super_percent:>4.1f}%\n"
        f"--------------------------------------------\n"
        f"TOTAL   | {total_all:>8.2f}L  | {avg_total:>8.2f}L  | 100.0%\n"
        f"```\n"
        f"📈 *Monthly Statistics:*\n"
        f"• 📊 Total volume: {total_all:,.2f}L\n"
        f"• 📈 Average daily: {avg_total:.2f}L\n"
        f"• 🗓️ Days in month: {total_days_in_month}\n"
        f"• 📅 Days with data: {days_with_data}\n"
        f"• ⛽ Fuel distribution shown above"
    )
    
    return msg

def format_detailed_monthly_report(station: str, year: int, month: int) -> str:
    """Format a detailed monthly report using get_monthly_details."""
    try:
        # Get monthly details
        details = get_monthly_details(year, month, station)
        
        if not details:
            month_name = calendar.month_name[month]
            return f"⚠️ *No data found for {station} in {month_name} {year}*"
        
        # Get aggregate data for totals
        aggregate = get_monthly_aggregate(year, month, station)
        
        if not aggregate:
            month_name = calendar.month_name[month]
            return f"⚠️ *No aggregate data found for {station} in {month_name} {year}*"
        
        # Organize data by date
        daily_data = {}
        total_volume = aggregate.get("total_volume", 0)
        total_amount = aggregate.get("total_amount", 0)
        days_with_data = aggregate.get("days_with_data", 0)
        
        for record in details:
            date_str = record.get("report_date", "")
            fuel_type = record.get("fuel_type", "")
            volume = record.get("total_volume", 0)
            amount = record.get("total_amount", 0)
            
            # Skip if date_str is a format string
            if not date_str or date_str.startswith('%'):
                continue
                
            if date_str not in daily_data:
                daily_data[date_str] = {}
            
            # Map fuel type to English name
            if "Diesel" in fuel_type or "Diesel" in fuel_type:
                fuel_key = "Diesel"
            elif "Regular" in fuel_type or "Regular" in fuel_type:
                fuel_key = "Regular"
            elif "Super" in fuel_type or "Super" in fuel_type:
                fuel_key = "Super"
            else:
                fuel_key = fuel_type
            
            daily_data[date_str][fuel_key] = {
                "volume": volume,
                "amount": amount
            }
        
        if not daily_data:
            month_name = calendar.month_name[month]
            return f"⚠️ *No valid data found for {station} in {month_name} {year}*"
        
        # Sort dates safely
        sorted_dates = sorted(daily_data.keys(), key=safe_date_sort)
        
        month_name = calendar.month_name[month]
        
        # Build message
        msg = (
            f"📊 *DETAILED MONTHLY REPORT*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🏪 *Station:* {station}\n"
            f"📅 *Month:* {month_name} {year}\n"
            f"📆 *Days with data:* {days_with_data}\n"
            f"⛽ *Total Volume:* {total_volume:,.2f}L\n"
            f"💰 *Total Amount:* ${total_amount:,.2f}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
        )
        
        # Add daily breakdown
        msg += "```\n"
        msg += "Date| Diesel| Regular| Super| Grand Total \n"
        msg += "-------------------------------------------\n"
        
        for date_str in sorted_dates:
            fuels = daily_data[date_str]
            diesel_data = fuels.get("Diesel", {})
            regular_data = fuels.get("Regular", {})
            super_data = fuels.get("Super", {})
            
            diesel_vol = diesel_data.get("volume", 0) if isinstance(diesel_data, dict) else (diesel_data if diesel_data else 0)
            regular_vol = regular_data.get("volume", 0) if isinstance(regular_data, dict) else (regular_data if regular_data else 0)
            super_vol = super_data.get("volume", 0) if isinstance(super_data, dict) else (super_data if super_data else 0)
            
            daily_total = diesel_vol + regular_vol + super_vol
            
            # Calculate amount
            diesel_amt = diesel_data.get("amount", 0) if isinstance(diesel_data, dict) else 0
            regular_amt = regular_data.get("amount", 0) if isinstance(regular_data, dict) else 0
            super_amt = super_data.get("amount", 0) if isinstance(super_data, dict) else 0
            daily_amount = diesel_amt + regular_amt + super_amt
            
            # Format date for display (yyyy/mm/dd)
            display_date = date_str
            
            msg += f"{display_date:5s}|{diesel_vol:2.2f}L|{regular_vol:2.2f}L|{super_vol:2.2f}L|{daily_total:2.2f}L\n"
        
        msg += "-------------------------------------------\n"
        
        # Calculate totals by fuel type
        total_diesel = sum(fuels.get("Diesel", {}).get("volume", 0) if isinstance(fuels.get("Diesel", {}), dict) else 0 for fuels in daily_data.values())
        total_regular = sum(fuels.get("Regular", {}).get("volume", 0) if isinstance(fuels.get("Regular", {}), dict) else 0 for fuels in daily_data.values())
        total_super = sum(fuels.get("Super", {}).get("volume", 0) if isinstance(fuels.get("Super", {}), dict) else 0 for fuels in daily_data.values())

        # Calculate total amounts
        total_diesel_amt = sum(fuels.get("Diesel", {}).get("amount", 0) if isinstance(fuels.get("Diesel", {}), dict) else 0 for fuels in daily_data.values())
        total_regular_amt = sum(fuels.get("Regular", {}).get("amount", 0) if isinstance(fuels.get("Regular", {}), dict) else 0 for fuels in daily_data.values())
        total_super_amt = sum(fuels.get("Super", {}).get("amount", 0) if isinstance(fuels.get("Super", {}), dict) else 0 for fuels in daily_data.values())
        
        msg += f"TOTAL| {total_diesel:2.2f}L|{total_regular:2.2f}L|{total_super:2.2f}L|{total_volume:2.2f}L\n"
        msg += "```\n"
        
        # Add statistics
        if days_with_data > 0:
            avg_daily = total_volume / days_with_data if days_with_data > 0 else 0
            avg_amount = total_amount / days_with_data if days_with_data > 0 else 0
            
            # Calculate percentages
            diesel_percent = (total_diesel / total_volume * 100) if total_volume > 0 else 0
            regular_percent = (total_regular / total_volume * 100) if total_volume > 0 else 0
            super_percent = (total_super / total_volume * 100) if total_volume > 0 else 0
            
            msg += f"📈 *Monthly Statistics:*\n"
            msg += f"• 📊 Average daily volume: {avg_daily:.2f}L\n"
            msg += f"• 💰 Average daily amount: ${avg_amount:.2f}\n"
            msg += f"• ⛽ Diesel: {total_diesel:.2f}L ({diesel_percent:.1f}%) - ${total_diesel_amt:.2f}\n"
            msg += f"• ⛽ Regular: {total_regular:.2f}L ({regular_percent:.1f}%) - ${total_regular_amt:.2f}\n"
            msg += f"• ⛽ Super: {total_super:.2f}L ({super_percent:.1f}%) - ${total_super_amt:.2f}\n"
            msg += f"• 📅 Days with data: {days_with_data}\n"
            
            # Calculate days without data
            total_days_in_month = calendar.monthrange(year, month)[1]
            days_without_data = total_days_in_month - days_with_data
            if days_without_data > 0:
                msg += f"• 📭 Days without data: {days_without_data}\n"
        
        return msg
        
    except Exception as e:
        logger.error(f"Error formatting detailed monthly report: {e}")
        logger.error(traceback.format_exc())
        return f"❌ Error generating detailed monthly report: {str(e)[:200]}"

def format_monthly_daybyday(station: str, year: int, month: int) -> str:
    """Format monthly report with day-by-day breakdown."""
    try:
        # Get monthly details
        details = get_monthly_details(year, month, station)
        
        if not details:
            month_name = calendar.month_name[month]
            return f"⚠️ *No data found for {station} in {month_name} {year}*"
        
        # Organize data by date
        daily_data = {}
        for record in details:
            date_str = record.get("report_date", "")
            fuel_type = record.get("fuel_type", "")
            volume = record.get("total_volume", 0)
            amount = record.get("total_amount", 0)
            
            # Skip if date_str is a format string
            if not date_str or date_str.startswith('%'):
                continue
                
            if date_str not in daily_data:
                daily_data[date_str] = {}
            
            # Map fuel type to English name
            if "Diesel" in fuel_type or "Diesel" in fuel_type.upper():
                fuel_key = "Diesel"
            elif "Regular" in fuel_type or "Regular" in fuel_type:
                fuel_key = "Regular"
            elif "Super" in fuel_type or "Super" in fuel_type:
                fuel_key = "Super"
            else:
                fuel_key = fuel_type
            
            daily_data[date_str][fuel_key] = {
                "volume": volume,
                "amount": amount
            }
        
        if not daily_data:
            month_name = calendar.month_name[month]
            return f"⚠️ *No valid data found for {station} in {month_name} {year}*"
        
        # Sort dates safely
        sorted_dates = sorted(daily_data.keys(), key=safe_date_sort)
        
        month_name = calendar.month_name[month]
        days_with_data = len(sorted_dates)
        
        # Build message with pagination
        msg = (
            f"📅 *DAY-BY-DAY REPORT*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🏪 *Station:* {station}\n"
            f"📅 *Month:* {month_name} {year}\n"
            f"📆 *Days with data:* {days_with_data}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
        )
        
        # Add each day's details
        for i, date_str in enumerate(sorted_dates, 1):
            fuels = daily_data[date_str]
            
            # Extract fuel data with English names
            diesel_data = fuels.get("Diesel", {})
            regular_data = fuels.get("Regular", {})
            super_data = fuels.get("Super", {})
            
            diesel_vol = diesel_data.get("volume", 0) if isinstance(diesel_data, dict) else (diesel_data if diesel_data else 0)
            regular_vol = regular_data.get("volume", 0) if isinstance(regular_data, dict) else (regular_data if regular_data else 0)
            super_vol = super_data.get("volume", 0) if isinstance(super_data, dict) else (super_data if super_data else 0)
            
            daily_total = diesel_vol + regular_vol + super_vol
            
            # Extract amounts
            diesel_amt = diesel_data.get("amount", 0) if isinstance(diesel_data, dict) else 0
            regular_amt = regular_data.get("amount", 0) if isinstance(regular_data, dict) else 0
            super_amt = super_data.get("amount", 0) if isinstance(super_data, dict) else 0
            daily_amount = diesel_amt + regular_amt + super_amt
            
            msg += f"📅 *{date_str}*\n"
            msg += f"  • Diesel: {diesel_vol:7.2f}L (${diesel_amt:.2f})\n"
            msg += f"  • Regular: {regular_vol:7.2f}L (${regular_amt:.2f})\n"
            msg += f"  • Super: {super_vol:7.2f}L (${super_amt:.2f})\n"
            msg += f"  • Total: {daily_total:7.2f}L (${daily_amount:.2f})\n"
            
            if i < len(sorted_dates):
                msg += "━━━━━━━━━━━━━━━━━━━━━\n"
        
        return msg
        
    except Exception as e:
        logger.error(f"Error formatting day-by-day report: {e}")
        logger.error(traceback.format_exc())
        return f"❌ Error generating day-by-day report: {str(e)[:200]}"

def format_monthly_all_data(station: str, year: int, month: int) -> str:
    """Format complete monthly data including all records."""
    try:
        # Get monthly details
        details = get_monthly_details(year, month, station)
        
        if not details:
            month_name = calendar.month_name[month]
            return f"⚠️ *No data found for {station} in {month_name} {year}*"
        
        # Get aggregated statistics
        stats = get_monthly_aggregate(year, month, station)
        
        month_name = calendar.month_name[month]
        total_volume = stats.get("total_volume", 0)
        total_amount = stats.get("total_amount", 0)
        days_with_data = stats.get("days_with_data", 0)
        record_count = stats.get("record_count", 0)
        
        # Build header
        msg = (
            f"📋 *COMPLETE MONTHLY DATA*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🏪 *Station:* {station}\n"
            f"📅 *Month:* {month_name} {year}\n"
            f"📊 *Records:* {record_count}\n"
            f"📆 *Days with data:* {days_with_data}\n"
            f"⛽ *Total Volume:* {total_volume:,.2f}L\n"
            f"💰 *Total Amount:* ${total_amount:,.2f}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
        )
        
        # Group by date
        date_groups = defaultdict(list)
        for record in details:
            date_str = record.get("report_date", "")
            # Skip if date_str is a format string
            if not date_str or date_str.startswith('%'):
                continue
                
            if date_str:
                date_groups[date_str].append(record)
        
        if not date_groups:
            return f"⚠️ *No valid data found for {station} in {month_name} {year}*"
        
        # Sort dates safely
        sorted_dates = sorted(date_groups.keys(), key=safe_date_sort)
        
        # Add data for each date
        for date_str in sorted_dates:
            records = date_groups[date_str]
            date_total_volume = sum(r.get("total_volume", 0) for r in records)
            date_total_amount = sum(r.get("total_amount", 0) for r in records)
            
            msg += f"📅 *{date_str}*\n"
            msg += f"  Total: {date_total_volume:7.2f}L (${date_total_amount:.2f})\n"
            
            for record in records:
                fuel_type = record.get("fuel_type", "")
                volume = record.get("total_volume", 0)
                amount = record.get("total_amount", 0)
                
                # Convert to English fuel names
                if "Diesel" in fuel_type or "Diesel" in fuel_type.upper():
                    display_fuel = "Diesel"
                elif "Regular" in fuel_type or "Regular" in fuel_type:
                    display_fuel = "Regular"
                elif "Super" in fuel_type or "Super" in fuel_type:
                    display_fuel = "Super"
                else:
                    # Shorten long fuel type names
                    if len(fuel_type) > 20:
                        display_fuel = fuel_type[:17] + "..."
                    else:
                        display_fuel = fuel_type
                
                msg += f"    • {display_fuel}: {volume:7.2f}L (${amount:.2f})\n"
            
            if date_str != sorted_dates[-1]:
                msg += "\n"
        
        # Add summary at the end
        total_days_in_month = calendar.monthrange(year, month)[1]
        days_without_data = total_days_in_month - days_with_data
        
        msg += f"\━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"📈 *Summary for {month_name} {year}:*\n"
        msg += f"• Total days in month: {total_days_in_month}\n"
        msg += f"• Days with data: {days_with_data}\n"
        if days_without_data > 0:
            msg += f"• Days without data: {days_without_data}\n"
        msg += f"• Average daily volume: {total_volume/days_with_data:.2f}L\n" if days_with_data > 0 else ""
        msg += f"• Average daily amount: ${total_amount/days_with_data:.2f}\n" if days_with_data > 0 else ""
        
        return msg
        
    except Exception as e:
        logger.error(f"Error formatting complete monthly data: {e}")
        logger.error(traceback.format_exc())
        return f"❌ Error generating complete monthly data: {str(e)[:200]}"

def consolidate_station_summary(station: str, station_dates: dict) -> str:
    """
    Builds a single message per station for multiple dates.
    For monthly data, shows aggregated totals.
    """
    if not station_dates:
        return None
    
    # Calculate totals
    total_diesel = total_regular = total_super = 0
    
    for date_str, fuels in station_dates.items():
        # Extract values with English names
        diesel = 0
        regular = 0
        super = 0
        
        if isinstance(fuels, dict):
            # Handle direct fuel data with English names
            diesel = fuels.get("Diesel", 0)
            regular = fuels.get("Regular", 0)
            super = fuels.get("Super", 0)
        else:
            # Handle numeric values directly
            diesel = fuels if fuels else 0
        
        total_diesel += diesel if isinstance(diesel, (int, float)) else 0
        total_regular += regular if isinstance(regular, (int, float)) else 0
        total_super += super if isinstance(super, (int, float)) else 0

    grand_total = total_diesel + total_regular + total_super

    if grand_total == 0:
        return f"⚠️ *No valid data found for {station}*"
    
    # Calculate percentages
    diesel_percent = (total_diesel / grand_total * 100) if grand_total > 0 else 0
    regular_percent = (total_regular / grand_total * 100) if grand_total > 0 else 0
    super_percent = (total_super / grand_total * 100) if grand_total > 0 else 0

    msg = f"📊 *SUMMARY FOR: {station}*\n"
    msg += "```\n"
    msg += "Fuel Type   | Total Volume | Percentage\n"
    msg += "--------------------------------------------\n"
    msg += f"Diesel      | {total_diesel:>12.2f}L  | {diesel_percent:>9.1f}%\n"
    msg += f"Regular     | {total_regular:>12.2f}L | {regular_percent:>9.1f}%\n"
    msg += f"Super       | {total_super:>12.2f}L   | {super_percent:>9.1f}%\n"
    msg += "--------------------------------------------\n"
    msg += f"TOTAL       | {grand_total:>12.2f}L | 100.0%\n"
    msg += "```"
    
    # Add note about data source
    days_count = len(station_dates)
    msg += f"\n\n📆 *Data points:* {days_count} day(s)"
    
    return msg

def format_single_day_report(station: str, report_date: str, fuels: dict) -> str:
    """Format a report for a single day. report_date is in yyyy/mm/dd format."""
    # Normalize fuel keys to English names
    normalized_fuels = {}
    for k, v in fuels.items():
        key = k.strip()
        if "Diesel" in key or "DO" in key.upper():
            normalized_fuels["Diesel"] = v
        elif "Regular" in key:
            normalized_fuels["Regular"] = v
        elif "Super" in key:
            normalized_fuels["Super"] = v
        else:
            normalized_fuels[key] = v
    
    diesel = normalized_fuels.get("Diesel", 0)
    regular = normalized_fuels.get("Regular", 0)
    super = normalized_fuels.get("Super", 0)
    total_volume = diesel + regular + super

    if total_volume == 0:
        return f"⚠️ *No data found for {station} on {report_date}*"
    
    # Create a simple bar visualization
    max_volume = max(diesel, regular, super, 1)
    bar_length = 10
    
    diesel_bar = "█" * int((diesel / max_volume) * bar_length)
    regular_bar = "█" * int((regular / max_volume) * bar_length)
    super_bar = "█" * int((super / max_volume) * bar_length)
    
    # Calculate percentages
    diesel_percent = (diesel / total_volume * 100) if total_volume > 0 else 0
    regular_percent = (regular / total_volume * 100) if total_volume > 0 else 0
    super_percent = (super / total_volume * 100) if total_volume > 0 else 0
    
    msg = (
        f"📊 *DAILY REPORT*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏪 *Station:* {station}\n"
        f"📅 *Date:* {report_date}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"```\n"
        f"Product | Volume    | %         | Visual\n"
        f"--------------------------------------------\n"
        f"Diesel  | {diesel:>4.2f}L   | {diesel_percent:>2.1f}%     | {diesel_bar}\n"
        f"Regular | {regular:>4.2f}L  | {regular_percent:>2.1f}%     | {regular_bar}\n"
        f"Super   | {super:>4.2f}L   | {super_percent:>2.1f}%     | {super_bar}\n"
        f"--------------------------------------------\n"
        f"TOTAL   | {total_volume:>4.2f}L  | 100.0%\n"
        f"```\n"
        f"📈 *Summary:*\n"
        f"• ⛽ Diesel: {diesel:.2f}L ({diesel_percent:.1f}%)\n"
        f"• ⛽ Regular: {regular:.2f}L ({regular_percent:.1f}%)\n"
        f"• ⛽ Super: {super:.2f}L ({super_percent:.1f}%)\n"
        f"• 📊 Total: {total_volume:.2f}L"
    )
    
    return msg

def format_range_summary(station: str, start_date: str, end_date: str, station_data: dict) -> str:
    """Format a range report with summary statistics."""
    if station not in station_data:
        return None
    
    dates_data = station_data[station]
    
    # Sort dates safely
    date_items = []
    for db_date, fuels in dates_data.items():
        # Dates are already in yyyy/mm/dd format
        date_items.append((db_date, fuels))
    
    # Sort by safe date parsing
    date_items.sort(key=lambda x: safe_date_sort(x[0]))
    
    if not date_items:
        return None
    
    # Calculate totals
    total_diesel = total_regular = total_super = 0
    daily_totals = []
    dates_list = []
    
    for db_date, fuels in date_items:
        # Normalize fuel names to English
        normalized_fuels = {}
        for k, v in fuels.items():
            key = k.strip()
            if "Diesel" in key or "DO" in key.upper():
                normalized_fuels["Diesel"] = v
            elif "Regular" in key or "Regular" in key:
                normalized_fuels["Regular"] = v
            elif "Super" in key or "Super" in key:
                normalized_fuels["Super"] = v
            else:
                normalized_fuels[key] = v
        
        diesel = normalized_fuels.get("Diesel", 0)
        regular = normalized_fuels.get("Regular", 0)
        super = normalized_fuels.get("Super", 0)

        total_diesel += diesel
        total_regular += regular
        total_super += super
        
        daily_total = diesel + regular + super
        daily_totals.append(daily_total)
        dates_list.append(db_date)

    total_all = total_diesel + total_regular + total_super
    days_count = len(dates_list)
    
    if total_all == 0:
        return f"⚠️ *No data found for {station} from {start_date} to {end_date}*"
    
    # Calculate averages
    avg_diesel = total_diesel / days_count if days_count > 0 else 0
    avg_regular = total_regular / days_count if days_count > 0 else 0
    avg_super = total_super / days_count if days_count > 0 else 0
    avg_total = total_all / days_count if days_count > 0 else 0
    
    # Find min and max days
    if daily_totals:
        min_idx = daily_totals.index(min(daily_totals))
        max_idx = daily_totals.index(max(daily_totals))
        min_day = dates_list[min_idx]
        max_day = dates_list[max_idx]
        min_volume = daily_totals[min_idx]
        max_volume = daily_totals[max_idx]
    else:
        min_day = max_day = "N/A"
        min_volume = max_volume = 0
    
    # Calculate percentages
    diesel_percent = (total_diesel / total_all * 100) if total_all > 0 else 0
    regular_percent = (total_regular / total_all * 100) if total_all > 0 else 0
    super_percent = (total_super / total_all * 100) if total_all > 0 else 0

    msg = (
        f"📊 *RANGE SUMMARY*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏪 *Station:* {station}\n"
        f"📅 *Period:* {start_date} to {end_date}\n"
        f"📆 *Days with data:* {days_count}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"```\n"
        f"Fuel Type   | Total      | Avg/Day   | %\n"
        f"--------------------------------------------\n"
        f"Diesel      | {total_diesel:>8.2f}L  | {avg_diesel:>8.2f}L  | {diesel_percent:>4.1f}%\n"
        f"Regular     | {total_regular:>8.2f}L  | {avg_regular:>8.2f}L  | {regular_percent:>4.1f}%\n"
        f"Super       | {total_super:>8.2f}L  | {avg_super:>8.2f}L  | {super_percent:>4.1f}%\n"
        f"--------------------------------------------\n"
        f"TOTAL     | {total_all:>8.2f}L | {avg_total:>8.2f}L | 100.0%\n"
        f"```\n"
        f"📈 *Statistics:*\n"
        f"• 📈 Highest sales day: {max_day} ({max_volume:.2f}L)\n"
        f"• 📉 Lowest sales day: {min_day} ({min_volume:.2f}L)\n"
        f"• 📊 Average daily: {avg_total:.2f}L\n"
        f"• 📅 Days with data: {days_count}\n"
        f"• 📊 Total period: {total_all:.2f}L"
    )
    
    return msg

# ---------------- HANDLERS ----------------
async def handle_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles incoming daily report messages."""
    try:
        logger.info("=" * 50)
        logger.info("PROCESSING INCOMING REPORT")
        logger.info("=" * 50)
        
        if not update.message or not update.message.text:
            logger.error("No message or text in update")
            return

        logger.info(f"📥 Message received from chat: {update.effective_chat.id}")
        logger.info(f"📄 Text preview: {update.message.text[:200]}...")

        # Parse report
        logger.info("Parsing report...")
        parsed_data = parse_daily_report(update.message.text)
        
        # Format for database
        logger.info("Formatting for database...")
        data = format_for_database(parsed_data)
        
        station = data.get("station_name")
        report_date = data.get("report_date")

        if not station or not report_date:
            logger.error("Could not parse station or date from report")
            await update.message.reply_text("❌ Could not parse station name or date from the report.")
            return

        logger.info(f"✅ Parsed report: {station} - {report_date}")

        # Check for duplicate
        logger.info("Checking for duplicates...")
        if report_exists(station, report_date):
            logger.warning(f"Duplicate report detected: {station} - {report_date}")
            await update.message.reply_text(
                f"⚠️ Report for *{station}* on {report_date} already exists in database.",
                parse_mode="Markdown"
            )
            return

        # Save report
        logger.info("Saving report to database...")
        if save_report(data):
            logger.info(f"✅ Report saved: {station} - {report_date}")
            
            # Create confirmation message
            fuel_data = data.get("fuel_data", [])
            total_volume = data.get("total_volume", 0)
            total_amount = data.get("total_amount", 0)
            
            confirm_msg = (
                "✅ *REPORT SAVED SUCCESSFULLY!*\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                f"🏪 *Station:* {station}\n"
                f"📅 *Date:* {report_date}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
            )
            
            if fuel_data:
                confirm_msg += "📊 *Fuel Summary:*\n"
                for fuel in fuel_data:
                    fuel_type = fuel.get("fuel_type", "")
                    volume = fuel.get("volume", 0)
                    amount = fuel.get("amount", 0)
                    if volume > 0:
                        # Convert to English names
                        if "Diesel" in fuel_type or "Diesel" in fuel_type.upper():
                            display_fuel = "Diesel"
                        elif "Regular" in fuel_type or "Regular" in fuel_type:
                            display_fuel = "Regular"
                        elif "Super" in fuel_type or "Super" in fuel_type:
                            display_fuel = "Super"
                        else:
                            display_fuel = fuel_type
                        
                        confirm_msg += f"• {display_fuel}: {volume:.2f}L (${amount:.2f})\n"
                
                confirm_msg += f"\n📈 *Total Volume:* {total_volume:.2f}L\n"
                confirm_msg += f"💰 *Total Amount:* ${total_amount:.2f}\n"
            
            confirm_msg += "\n📋 *Next Steps:*\n"
            confirm_msg += "• Use `/report` to view saved reports\n"
            confirm_msg += "• Use `/stats` for overall statistics\n"
            confirm_msg += "• Use `/stations` to list all stations"
            
            await update.message.reply_text(confirm_msg, parse_mode="Markdown")
        else:
            logger.error(f"Failed to save report: {station} - {report_date}")
            await update.message.reply_text(
                "❌ Failed to save report to database. Please try again.",
                parse_mode="Markdown"
            )

    except Exception as e:
        logger.error(f"❌ Error processing report: {str(e)}")
        logger.error(traceback.format_exc())
        
        error_msg = (
            "❌ *Failed to process report!*\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            f"Error: `{str(e)[:200]}`\n\n"
            "Please check the report format and try again."
        )
        
        if update.message:
            await update.message.reply_text(
                error_msg, 
                parse_mode="Markdown"
            )

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all callback queries from inline keyboards."""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    user_id = query.from_user.id
    
    logger.info(f"🔘 Callback from user {user_id}: {data}")
    
    # Handle refresh stations
    if data == "refresh_stations":
        stations = get_all_stations()
        
        if not stations:
            await query.edit_message_text(
                "📭 *Still no stations found*\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "Please send some daily fuel reports first.",
                parse_mode="Markdown"
            )
            return
        
        message = (
            "📊 *VIEW REPORTS*\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "🏪 *SELECT A STATION*\n"
            f"Available stations: *{len(stations)}*\n\n"
            "Choose a station to view its reports:"
        )
        
        keyboard = create_station_keyboard()
        await query.edit_message_text(message, parse_mode="Markdown", reply_markup=keyboard)
        return
    
    # Handle cancel
    if data == "cancel":
        await query.edit_message_text("❌ Operation cancelled.")
        return
    
    # Handle ignore (buttons that shouldn't do anything)
    if data == "ignore":
        return
    
    # Handle back to stations
    if data == "back_to_stations":
        stations = get_all_stations()
        message = (
            "🏪 *SELECT A STATION*\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            f"Available stations: *{len(stations)}*\n\n"
            "Choose a station to view its reports:"
        )
        await query.edit_message_text(
            message, 
            parse_mode="Markdown", 
            reply_markup=create_station_keyboard()
        )
        return
    
    # Handle back from calendar
    if data.startswith("back_cal:"):
        station = data.split(":", 1)[1]
        await query.edit_message_text(
            f"⛽ *Selected Station:* {station}\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "📅 *SELECT DATE OPTION:*",
            parse_mode="Markdown",
            reply_markup=create_date_keyboard(station)
        )
        return
    
    # Handle cancel calendar
    if data == "cancel_cal":
        await query.edit_message_text("❌ Calendar selection cancelled.")
        return
    
    # Handle today in calendar
    if data.startswith("today_cal:"):
        station = data.split(":", 1)[1]
        today_display = get_today()
        await query.edit_message_text(f"⏳ Loading report for {today_display}...", parse_mode="Markdown")
        
        # Generate report for today
        station_data, rows = generate_summary_by_period(today_display, today_display, station)
        
        if not station_data or station not in station_data:
            await query.edit_message_text(
                f"⚠️ No report found for *{station}* on {today_display}",
                parse_mode="Markdown"
            )
            return
        
        # Get the date in database format
        fuels = station_data[station].get(today_display, {})
        
        if not fuels:
            await query.edit_message_text(
                f"⚠️ No report found for *{station}* on {today_display}",
                parse_mode="Markdown"
            )
            return
        
        # Format and send single day report
        report_msg = format_single_day_report(station, today_display, fuels)
        await query.edit_message_text(report_msg, parse_mode="Markdown")
        return
    
    # Handle calendar year navigation
    if data.startswith("calyear:"):
        parts = data.split(":")
        if len(parts) >= 5:
            station = parts[1]
            year = int(parts[2])
            month = int(parts[3])
            
            await query.edit_message_text(
                f"⛽ *{station}*\n"
                f"📅 *Select a date from calendar:*",
                parse_mode="Markdown",
                reply_markup=create_calendar_keyboard(station, year, month)
            )
        return
    
    # Handle station selection
    if data.startswith("station:") or data.startswith("s:"):
        # Extract station index
        parts = data.split(":")
        if len(parts) >= 2:
            station_index = parts[1]
            # Get actual station name from mapping
            station = user_selections["station_mapping"].get(station_index)
            
            if not station:
                # Fallback: try to get from stations list
                stations = get_all_stations()
                try:
                    station_idx = int(station_index)
                    if 0 <= station_idx < len(stations):
                        station = stations[station_idx]
                except (ValueError, IndexError):
                    await query.edit_message_text("❌ Invalid station selection.")
                    return
            
            if station:
                # Store station selection
                user_selections[user_id]["station"] = station
                
                message = (
                    f"⛽ *Selected Station:* {station}\n"
                    "━━━━━━━━━━━━━━━━━━━━━\n"
                    "📅 *SELECT DATE OPTION:*\n"
                    "• Today/Yesterday - Single day report\n"
                    "• Last 7/30 days - Range report\n"
                    "• Monthly - Select specific month\n"
                    "• Calendar - Pick specific date"
                )
                await query.edit_message_text(
                    message,
                    parse_mode="Markdown",
                    reply_markup=create_date_keyboard(station)
                )
            else:
                await query.edit_message_text("❌ Station not found. Please try again.")
        else:
            await query.edit_message_text("❌ Invalid station selection.")
        return
    
    # Handle back to dates
    if data.startswith("back_to_dates:"):
        station = data.split(":", 1)[1]
        await query.edit_message_text(
            f"⛽ *Selected Station:* {station}\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "📅 *SELECT DATE OPTION:*",
            parse_mode="Markdown",
            reply_markup=create_date_keyboard(station)
        )
        return
    
    # Handle single date selection (Today/Yesterday)
    if data.startswith("date_single:"):
        parts = data.split(":")
        station = parts[1]
        selected_date = parts[2]  # Already in yyyy/mm/dd format
        
        await query.edit_message_text(f"⏳ Loading report for {selected_date}...", parse_mode="Markdown")
        
        # Generate report for single day
        station_data, rows = generate_summary_by_period(selected_date, selected_date, station)
        
        if not station_data or station not in station_data:
            await query.edit_message_text(
                f"⚠️ No report found for *{station}* on {selected_date}",
                parse_mode="Markdown"
            )
            return
        
        fuels = station_data[station].get(selected_date, {})
        
        if not fuels:
            await query.edit_message_text(
                f"⚠️ No report found for *{station}* on {selected_date}",
                parse_mode="Markdown"
            )
            return
        
        # Format and send single day report
        report_msg = format_single_day_report(station, selected_date, fuels)
        await query.edit_message_text(report_msg, parse_mode="Markdown")
        return
    
    # Handle date range selection (7, 30 days)
    if data.startswith("date_range:"):
        parts = data.split(":")
        station = parts[1]
        days = int(parts[2])  # 7 or 30
        
        end_date = date.today()
        start_date = end_date - timedelta(days=days-1)
        
        start_date_display = format_date(start_date)
        end_date_display = format_date(end_date)
        
        await query.edit_message_text(f"⏳ Generating {days}-day report from {start_date_display} to {end_date_display}...", parse_mode="Markdown")
        
        # Generate range report
        station_data, rows = generate_summary_by_period(start_date_display, end_date_display, station)
        
        if not station_data:
            await query.edit_message_text(
                f"⚠️ No data found for *{station}* from {start_date_display} to {end_date_display}",
                parse_mode="Markdown"
            )
            return
        
        # Format and send range report
        report_msg = format_range_summary(station, start_date_display, end_date_display, station_data)
        if report_msg:
            await query.edit_message_text(report_msg, parse_mode="Markdown")
        
        # Also send detailed breakdown if requested
        if days <= 30:  # Only send detailed for reasonable ranges
            detailed_msg = consolidate_station_summary(station, station_data.get(station, {}))
            if detailed_msg:
                await query.message.reply_text(detailed_msg, parse_mode="Markdown")
        return
    
    # Handle month selection button
    if data.startswith("select_month_range:"):
        station = data.split(":", 1)[1]
        current_year = date.today().year
        
        message = (
            f"⛽ *Selected Station:* {station}\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "📅 *SELECT A MONTH*\n\n"
            "Choose a month to view monthly report:"
        )
        
        keyboard = create_month_selection_keyboard(station, current_year)
        await query.edit_message_text(message, parse_mode="Markdown", reply_markup=keyboard)
        return
    
    # Handle month-year navigation
    if data.startswith("month_year:"):
        parts = data.split(":")
        station = parts[1]
        year = int(parts[2])
        
        message = (
            f"⛽ *Selected Station:* {station}\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 *SELECT A MONTH ({year})*\n\n"
            "Choose a month to view monthly report:"
        )
        
        keyboard = create_month_selection_keyboard(station, year)
        await query.edit_message_text(message, parse_mode="Markdown", reply_markup=keyboard)
        return
    
    # Handle current month selection
    if data.startswith("current_month:"):
        station = data.split(":", 1)[1]
        today = date.today()
        year = today.year
        month = today.month
        
        month_name = calendar.month_name[month]
        
        await query.edit_message_text(f"⏳ Generating {month_name} {year} monthly report...", parse_mode="Markdown")
        
        # Generate monthly report using optimized function
        station_data, rows = generate_monthly_summary(year, month, station)
        
        if not station_data:
            await query.edit_message_text(
                f"⚠️ No data found for *{station}* in {month_name} {year}",
                parse_mode="Markdown"
            )
            return
        
        # Show report type selection
        message = (
            f"⛽ *Selected Station:* {station}\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 *Monthly Report: {month_name} {year}*\n\n"
            "Select report type:"
        )
        
        keyboard = create_month_report_type_keyboard(station, year, month)
        await query.edit_message_text(message, parse_mode="Markdown", reply_markup=keyboard)
        return
    
    # Handle specific month selection
    if data.startswith("select_month:"):
        parts = data.split(":")
        station = parts[1]
        year = int(parts[2])
        month = int(parts[3])
        
        month_name = calendar.month_name[month]
        start_date, end_date = get_month_range(year, month)
        
        message = (
            f"⛽ *Selected Station:* {station}\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 *Confirm Monthly Report*\n\n"
            f"Month: *{month_name} {year}*\n"
            f"Period: {start_date} to {end_date}\n\n"
            "Generate monthly report?"
        )
        
        keyboard = create_month_confirmation_keyboard(station, year, month)
        await query.edit_message_text(message, parse_mode="Markdown", reply_markup=keyboard)
        return
    
    # Handle month confirmation
    if data.startswith("confirm_month:"):
        parts = data.split(":")
        station = parts[1]
        year = int(parts[2])
        month = int(parts[3])
        
        month_name = calendar.month_name[month]
        
        # Show report type selection
        message = (
            f"⛽ *Selected Station:* {station}\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 *Monthly Report: {month_name} {year}*\n\n"
            "Select report type:"
        )
        
        keyboard = create_month_report_type_keyboard(station, year, month)
        await query.edit_message_text(message, parse_mode="Markdown", reply_markup=keyboard)
        return
    
    # Handle month summary report
    if data.startswith("month_summary:"):
        parts = data.split(":")
        station = parts[1]
        year = int(parts[2])
        month = int(parts[3])
        
        month_name = calendar.month_name[month]
        
        await query.edit_message_text(f"⏳ Generating {month_name} {year} monthly summary...", parse_mode="Markdown")
        
        # Generate monthly report using optimized function
        station_data, rows = generate_monthly_summary(year, month, station)
        
        if not station_data:
            await query.edit_message_text(
                f"⚠️ No data found for *{station}* in {month_name} {year}",
                parse_mode="Markdown"
            )
            return
        
        # Format and send monthly summary
        report_msg = format_monthly_summary(station, year, month, station_data)
        if report_msg:
            await query.edit_message_text(report_msg, parse_mode="Markdown")
        
        return
    
    # Handle month detailed report - FIXED VERSION
    if data.startswith("month_detailed:"):
        parts = data.split(":")
        station = parts[1]
        year = int(parts[2])
        month = int(parts[3])
        
        month_name = calendar.month_name[month]
        
        await query.edit_message_text(f"⏳ Generating {month_name} {year} detailed report...", parse_mode="Markdown")
        
        try:
            # Generate detailed monthly report using improved function
            report_msg = format_detailed_monthly_report(station, year, month)
            
            if report_msg:
                # Check if message is too long for Telegram (limit is 4096 characters)
                if len(report_msg) > 4000:
                    # Split into multiple messages
                    messages = []
                    lines = report_msg.split('\n')
                    current_message = ""
                    
                    for line in lines:
                        if len(current_message) + len(line) + 1 < 4000:
                            current_message += line + '\n'
                        else:
                            messages.append(current_message)
                            current_message = line + '\n'
                    
                    if current_message:
                        messages.append(current_message)
                    
                    # Send first message with "Part 1/X" indicator
                    messages[0] = f"📊 *DETAILED MONTHLY REPORT - PART 1/{len(messages)}*\n" + messages[0].split('\n', 1)[1] if len(messages) > 1 else messages[0]
                    await query.edit_message_text(messages[0], parse_mode="Markdown")
                    
                    # Send remaining messages
                    for i, msg in enumerate(messages[1:], 2):
                        msg_with_header = f"📊 *PART {i}/{len(messages)}*\n" + msg.split('\n', 1)[1] if len(messages) > 1 else msg
                        await query.message.reply_text(msg_with_header, parse_mode="Markdown")
                else:
                    await query.edit_message_text(report_msg, parse_mode="Markdown")
            else:
                await query.edit_message_text(
                    f"⚠️ No detailed report generated for *{station}* in {month_name} {year}",
                    parse_mode="Markdown"
                )
                
        except Exception as e:
            logger.error(f"Error generating detailed monthly report: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text(
                f"❌ Error generating detailed report for *{station}* in {month_name} {year}\n"
                f"Error: {str(e)[:200]}",
                parse_mode="Markdown"
            )
        return
    
    # Handle month day-by-day report
    if data.startswith("month_daybyday:"):
        parts = data.split(":")
        station = parts[1]
        year = int(parts[2])
        month = int(parts[3])
        
        month_name = calendar.month_name[month]
        
        await query.edit_message_text(f"⏳ Generating {month_name} {year} day-by-day report...", parse_mode="Markdown")
        
        # Generate day-by-day monthly report
        report_msg = format_monthly_daybyday(station, year, month)
        if report_msg:
            # Check if message is too long
            if len(report_msg) > 4000:
                # Split into multiple messages
                parts = []
                lines = report_msg.split('\n')
                current_part = []
                current_length = 0
                
                for line in lines:
                    if current_length + len(line) + 1 < 4000:
                        current_part.append(line)
                        current_length += len(line) + 1
                    else:
                        parts.append('\n'.join(current_part))
                        current_part = [line]
                        current_length = len(line) + 1
                
                if current_part:
                    parts.append('\n'.join(current_part))
                
                # Send first part
                await query.edit_message_text(parts[0], parse_mode="Markdown")
                
                # Send remaining parts
                for i, part in enumerate(parts[1:], 2):
                    await query.message.reply_text(f"*Part {i}:*\n{part}", parse_mode="Markdown")
            else:
                await query.edit_message_text(report_msg, parse_mode="Markdown")
        
        return
    
    # Handle month all data report
    if data.startswith("month_alldata:"):
        parts = data.split(":")
        station = parts[1]
        year = int(parts[2])
        month = int(parts[3])
        
        month_name = calendar.month_name[month]
        
        await query.edit_message_text(f"⏳ Generating {month_name} {year} complete data report...", parse_mode="Markdown")
        
        # Generate complete monthly data report
        report_msg = format_monthly_all_data(station, year, month)
        if report_msg:
            # Check if message is too long
            if len(report_msg) > 4000:
                # Split the message
                parts = []
                current_part = ""
                
                # Split by section
                sections = report_msg.split('\n\n')
                for section in sections:
                    if len(current_part) + len(section) + 2 < 4000:
                        current_part += section + '\n\n'
                    else:
                        parts.append(current_part)
                        current_part = section + '\n\n'
                
                if current_part:
                    parts.append(current_part)
                
                # Send first part
                await query.edit_message_text(parts[0], parse_mode="Markdown")
                
                # Send remaining parts
                for i, part in enumerate(parts[1:], 2):
                    await query.message.reply_text(f"*Continued... Part {i}:*\n{part}", parse_mode="Markdown")
            else:
                await query.edit_message_text(report_msg, parse_mode="Markdown")
        
        return
    
    # Handle change month
    if data.startswith("change_month:"):
        station = data.split(":", 1)[1]
        current_year = date.today().year
        
        message = (
            f"⛽ *Selected Station:* {station}\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "📅 *SELECT A MONTH*\n\n"
            "Choose a month to view monthly report:"
        )
        
        keyboard = create_month_selection_keyboard(station, current_year)
        await query.edit_message_text(message, parse_mode="Markdown", reply_markup=keyboard)
        return
    
    # Handle calendar navigation
    if data.startswith("calendar:"):
        parts = data.split(":")
        if len(parts) >= 4:
            station = parts[1]
            year = int(parts[2])
            month = int(parts[3])
            
            await query.edit_message_text(
                f"⛽ *{station}*\n"
                f"📅 *Select a date from calendar:*",
                parse_mode="Markdown",
                reply_markup=create_calendar_keyboard(station, year, month)
            )
        return
    
    # Handle date selection from calendar
    if data.startswith("caldate:") or data.startswith("cd:") or data.startswith("c:"):
        parts = data.split(":")
        station = parts[1]
        selected_date = parts[2]  # Already in yyyy/mm/dd format
        
        await query.edit_message_text(f"⏳ Loading report for {selected_date}...", parse_mode="Markdown")
        
        # Generate report for selected date
        station_data, rows = generate_summary_by_period(selected_date, selected_date, station)
        
        if not station_data or station not in station_data:
            await query.edit_message_text(
                f"⚠️ No report found for *{station}* on {selected_date}",
                parse_mode="Markdown"
            )
            return
        
        fuels = station_data[station].get(selected_date, {})
        
        if not fuels:
            await query.edit_message_text(
                f"⚠️ No report found for *{station}* on {selected_date}",
                parse_mode="Markdown"
            )
            return
        
        # Format and send single day report
        report_msg = format_single_day_report(station, selected_date, fuels)
        await query.edit_message_text(report_msg, parse_mode="Markdown")
        return
    
    # Handle quick date selection (today, yesterday, tomorrow)
    if data.startswith("quickdate:"):
        parts = data.split(":")
        station = parts[1]
        selected_date = parts[2]  # Already in yyyy/mm/dd format
        
        await query.edit_message_text(f"⏳ Loading report for {selected_date}...", parse_mode="Markdown")
        
        # Generate report for selected date
        station_data, rows = generate_summary_by_period(selected_date, selected_date, station)
        
        if not station_data or station not in station_data:
            await query.edit_message_text(
                f"⚠️ No report found for *{station}* on {selected_date}",
                parse_mode="Markdown"
            )
            return
        
        fuels = station_data[station].get(selected_date, {})
        
        if not fuels:
            await query.edit_message_text(
                f"⚠️ No report found for *{station}* on {selected_date}",
                parse_mode="Markdown"
            )
            return
        
        # Format and send single day report
        report_msg = format_single_day_report(station, selected_date, fuels)
        await query.edit_message_text(report_msg, parse_mode="Markdown")
        return
    
    # Handle confirm date
    if data.startswith("confirm_date:"):
        parts = data.split(":")
        station = parts[1]
        selected_date = parts[2]  # Already in yyyy/mm/dd format
        
        await query.edit_message_text(f"⏳ Loading report for {selected_date}...", parse_mode="Markdown")
        
        # Generate report for selected date
        station_data, rows = generate_summary_by_period(selected_date, selected_date, station)
        
        if not station_data or station not in station_data:
            await query.edit_message_text(
                f"⚠️ No report found for *{station}* on {selected_date}",
                parse_mode="Markdown"
            )
            return
        
        fuels = station_data[station].get(selected_date, {})
        
        if not fuels:
            await query.edit_message_text(
                f"⚠️ No report found for *{station}* on {selected_date}",
                parse_mode="Markdown"
            )
            return
        
        # Format and send single day report
        report_msg = format_single_day_report(station, selected_date, fuels)
        await query.edit_message_text(report_msg, parse_mode="Markdown")
        return
    
    # Handle change date
    if data.startswith("change_date:"):
        station = data.split(":", 1)[1]
        await query.edit_message_text(
            f"⛽ *Selected Station:* {station}\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "📅 *SELECT DATE OPTION:*",
            parse_mode="Markdown",
            reply_markup=create_date_keyboard(station)
        )
        return
    
    # Handle back to calendar
    if data.startswith("back_calendar:"):
        parts = data.split(":")
        if len(parts) >= 4:
            station = parts[1]
            year = int(parts[2])
            month = int(parts[3])
            
            await query.edit_message_text(
                f"⛽ *{station}*\n"
                f"📅 *Select a date from calendar:*",
                parse_mode="Markdown",
                reply_markup=create_calendar_keyboard(station, year, month)
            )
        return
    
    # Handle open calendar
    if data.startswith("open_cal:"):
        parts = data.split(":")
        if len(parts) >= 4:
            station = parts[1]
            year = int(parts[2])
            month = int(parts[3])
            
            await query.edit_message_text(
                f"⛽ *{station}*\n"
                f"📅 *Select a date from calendar:*",
                parse_mode="Markdown",
                reply_markup=create_calendar_keyboard(station, year, month)
            )
        return
    
    # Handle previous day
    if data.startswith("prevday:"):
        parts = data.split(":")
        station = parts[1]
        current_date_str = parts[2]
        
        # Parse current date and subtract one day
        current_date = parse_date_string(current_date_str)
        prev_date = current_date - timedelta(days=1)
        prev_date_str = format_date(prev_date)
        
        await query.edit_message_text(f"⏳ Loading report for {prev_date_str}...", parse_mode="Markdown")
        
        # Generate report for previous day
        station_data, rows = generate_summary_by_period(prev_date_str, prev_date_str, station)
        
        if not station_data or station not in station_data:
            await query.edit_message_text(
                f"⚠️ No report found for *{station}* on {prev_date_str}",
                parse_mode="Markdown"
            )
            return
        
        fuels = station_data[station].get(prev_date_str, {})
        
        if not fuels:
            await query.edit_message_text(
                f"⚠️ No report found for *{station}* on {prev_date_str}",
                parse_mode="Markdown"
            )
            return
        
        # Format and send single day report
        report_msg = format_single_day_report(station, prev_date_str, fuels)
        await query.edit_message_text(report_msg, parse_mode="Markdown")
        return
    
    # Handle next day
    if data.startswith("nextday:"):
        parts = data.split(":")
        station = parts[1]
        current_date_str = parts[2]
        
        # Parse current date and add one day
        current_date = parse_date_string(current_date_str)
        next_date = current_date + timedelta(days=1)
        next_date_str = format_date(next_date)
        
        await query.edit_message_text(f"⏳ Loading report for {next_date_str}...", parse_mode="Markdown")
        
        # Generate report for next day
        station_data, rows = generate_summary_by_period(next_date_str, next_date_str, station)
        
        if not station_data or station not in station_data:
            await query.edit_message_text(
                f"⚠️ No report found for *{station}* on {next_date_str}",
                parse_mode="Markdown"
            )
            return
        
        fuels = station_data[station].get(next_date_str, {})
        
        if not fuels:
            await query.edit_message_text(
                f"⚠️ No report found for *{station}* on {next_date_str}",
                parse_mode="Markdown"
            )
            return
        
        # Format and send single day report
        report_msg = format_single_day_report(station, next_date_str, fuels)
        await query.edit_message_text(report_msg, parse_mode="Markdown")
        return
    
    # Handle custom date
    if data.startswith("custom_date:"):
        station = data.split(":", 1)[1]
        await query.edit_message_text(
            f"⛽ *{station}*\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "📅 Please send a date in *yyyy/mm/dd* format.\n\n"
            "Example: `2024/12/27`",
            parse_mode="Markdown"
        )
        # Store state for custom date input
        user_selections[user_id]["waiting_for_date"] = station
        return
    
    # Default fallback
    await query.edit_message_text("❌ Invalid selection. Please try again.")

# ---------------- COMMAND HANDLERS ----------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command to check if bot is running."""
    welcome_msg = (
        "🤖 *KONCHAT FUEL REPORT BOT*\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "Welcome! This bot helps manage and view fuel station reports.\n\n"
        "📋 *AVAILABLE COMMANDS:*\n"
        "/report - View reports by station and date\n"
        "/today - Today's reports for all stations\n"
        "/yesterday - Yesterday's reports\n"
        "/weekly - Last 7 days summary\n"
        "/monthly - Last 30 days summary\n"
        "/stations - List all stations\n"
        "/stats - Show statistics\n"
        "/testdb - Test database connection\n"
        "/help - Show this help message\n\n"
        "📥 *HOW TO USE:*\n"
        "1. Simply send a daily sales report text\n"
        "2. Bot automatically parses and saves it\n"
        "3. Use /report to view saved reports\n\n"
        "⚡ *Quick Start:*\n"
        "Type /report to begin viewing reports!"
    )
    await update.message.reply_text(welcome_msg, parse_mode="Markdown")

async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Main command to view reports with station and date selection."""
    try:
        logger.info(f"Report command received from user {update.message.from_user.id if update.message.from_user else 'Unknown'}")
        
        stations = get_all_stations()
        
        if not stations:
            logger.info("No stations in database yet")
            await update.message.reply_text(
                "📭 *No station data available yet.*\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "Please send some daily fuel reports first.\n\n"
                "📋 *How to add reports:*\n"
                "1. Copy and paste a daily fuel report\n"
                "2. Send it to this chat\n"
                "3. The bot will automatically parse and save it",
                parse_mode="Markdown"
            )
            return
        
        # Show station selection
        message = (
            "📊 *VIEW REPORTS*\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "🏪 *SELECT A STATION*\n"
            f"Available stations: *{len(stations)}*\n\n"
            "Choose a station to view its reports:"
        )
        
        keyboard = create_station_keyboard()
        await update.message.reply_text(message, parse_mode="Markdown", reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Error in report_command: {e}")
        logger.error(traceback.format_exc())
        
        await update.message.reply_text(
            "❌ *Error loading report viewer*\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "An error occurred while trying to load the report viewer.\n"
            f"Error: `{str(e)[:100]}`\n\n"
            "Please try again or use /testdb to check the database.",
            parse_mode="Markdown"
        )

async def stations_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all available stations with counts."""
    try:
        stations = get_all_stations()
        
        if not stations:
            await update.message.reply_text("📭 No stations found in database.")
            return
        
        # Get report counts for each station
        today = date.today()
        thirty_days_ago = today - timedelta(days=30)
        start_date_db = format_date(thirty_days_ago)
        end_date_db = format_date(today)
        
        rows = get_summary(start_date_db, end_date_db)
        
        # Count reports per station
        station_counts = defaultdict(int)
        for row in rows:
            station_name = row["station_name"]
            station_counts[station_name] += 1
        
        # Get statistics for each station
        station_stats = []
        for station in stations:
            stats = get_station_statistics(station)
            station_stats.append({
                "name": station,
                "recent_count": station_counts.get(station, 0),
                "total_reports": stats.get("report_count", 0),
                "total_volume": stats.get("total_volume", 0),
                "first_report": stats.get("first_report", "N/A"),
                "last_report": stats.get("last_report", "N/A")
            })
        
        # Sort by total reports
        station_stats.sort(key=lambda x: x["total_reports"], reverse=True)
        
        message = "🏪 *AVAILABLE STATIONS*\━━━━━━━━━━━━━━━━━━━━━\n"
        
        for i, stats in enumerate(station_stats, 1):
            message += f"{i}. *{stats['name']}*\n"
            message += f"   📊 Reports: {stats['recent_count']} (30d) / {stats['total_reports']} (total)\n"
            if stats['total_volume'] > 0:
                message += f"   ⛽ Total Volume: {stats['total_volume']:,.2f}L\n"
            if stats['last_report'] != "N/A":
                message += f"   📅 Last report: {stats['last_report']}\n"
            message += "\n"
        
        message += f"📊 *Total:* {len(stations)} stations"
        
        await update.message.reply_text(message, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Error in stations_command: {e}")
        await update.message.reply_text(
            f"❌ Error loading stations: {str(e)[:100]}",
            parse_mode="Markdown"
        )

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show comprehensive statistics about stored reports."""
    try:
        rows = get_summary(None, None)
        
        if not rows:
            await update.message.reply_text("📭 No reports stored yet.")
            return
        
        # Calculate statistics
        stations = set()
        dates = set()
        total_volume = 0
        total_amount = 0
        
        for row in rows:
            stations.add(row["station_name"])
            dates.add(row["report_date"])
            total_volume += row["volume"]
            total_amount += row["amount"]
        
        # Get date range
        sorted_dates = sorted(dates)
        if sorted_dates:
            first_date = sorted_dates[0]
            last_date = sorted_dates[-1]
            date_range = f"{first_date} to {last_date}"
        else:
            date_range = "N/A"
        
        # Calculate reports per day
        days_count = len(dates)
        reports_per_day = len(rows) / days_count if days_count > 0 else 0
        
        # Get overall statistics
        overall_stats = get_station_statistics()
        
        message = (
            f"📊 *STATISTICS OVERVIEW*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 *Date Range:* {date_range}\n"
            f"📁 *Total Reports:* {len(rows)}\n"
            f"⛽ *Total Volume:* {total_volume:,.2f}L\n"
            f"💰 *Total Amount:* ${total_amount:,.2f}\n"
            f"🏪 *Unique Stations:* {len(stations)}\n"
            f"📆 *Days Recorded:* {days_count}\n"
            f"📈 *Avg Reports/Day:* {reports_per_day:.1f}\n"
            f"📊 *Avg Volume/Report:* {total_volume/len(rows):.2f}L"
        )
        
        if overall_stats.get("station_count"):
            message += f"\n🔢 *Total Stations:* {overall_stats['station_count']}"
        if overall_stats.get("total_days"):
            message += f"\n📅 *Total Days:* {overall_stats['total_days']}"
        
        await update.message.reply_text(message, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Error in stats_command: {e}")
        await update.message.reply_text(
            f"❌ Error loading statistics: {str(e)[:100]}",
            parse_mode="Markdown"
        )

async def today_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show today's report for all stations."""
    try:
        today_date = date.today()
        today_display = format_date(today_date)
        station_data, rows = generate_summary_by_period(today_display, today_display)
        
        if not station_data:
            await update.message.reply_text(f"📭 No reports for today ({today_display}).")
            return
        
        message = f"📊 *TODAY'S REPORTS* ({today_display})\━━━━━━━━━━━━━━━━━━━━━\n"
        
        total_diesel = total_regular = total_super = 0
        
        for station, dates in station_data.items():
            if today_display in dates:
                fuels = dates[today_display]
                # Normalize to English names
                normalized_fuels = {}
                for k, v in fuels.items():
                    key = k.strip()
                    if "Diesel" in key or "DO" in key.upper():
                        normalized_fuels["Diesel"] = v
                    elif "Regular" in key or "Regular" in key:
                        normalized_fuels["Regular"] = v
                    elif "Super" in key or "Super" in key:
                        normalized_fuels["Super"] = v
                    else:
                        normalized_fuels[key] = v
                
                diesel = normalized_fuels.get("Diesel", 0)
                regular = normalized_fuels.get("Regular", 0)
                super = normalized_fuels.get("Super", 0)
                total = diesel + regular + super
                
                total_diesel += diesel
                total_regular += regular
                total_super += super
                
                message += f"⛽ *{station}*\n"
                message += f"  Diesel: {diesel:7.2f}L | Regular: {regular:7.2f}L | Super: {super:7.2f}L | Total: {total:7.2f}L\n\n"
        
        grand_total = total_diesel + total_regular + total_super
        message += f"━━━━━━━━━━━━━━━━━━━━━\n"
        if grand_total > 0:
            message += f"📊 *TOTAL TODAY:* {grand_total:,.2f}L\n"
            message += f"• Diesel: {total_diesel:,.2f}L ({total_diesel/grand_total*100:.1f}%)\n"
            message += f"• Regular: {total_regular:,.2f}L ({total_regular/grand_total*100:.1f}%)\n"
            message += f"• Super: {total_super:,.2f}L ({total_super/grand_total*100:.1f}%)\n"
        message += f"• Stations: {len(station_data)}"
        
        await update.message.reply_text(message, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Error in today_command: {e}")
        await update.message.reply_text(
            f"❌ Error loading today's reports: {str(e)[:100]}",
            parse_mode="Markdown"
        )

async def yesterday_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show yesterday's report for all stations."""
    try:
        yesterday_date = date.today() - timedelta(days=1)
        yesterday_display = format_date(yesterday_date)
        station_data, rows = generate_summary_by_period(yesterday_display, yesterday_display)
        
        if not station_data:
            await update.message.reply_text(f"📭 No reports for yesterday ({yesterday_display}).")
            return
        
        message = f"📊 *YESTERDAY'S REPORTS* ({yesterday_display})\━━━━━━━━━━━━━━━━━━━━━\n"
        
        total_diesel = total_regular = total_super = 0
        
        for station, dates in station_data.items():
            if yesterday_display in dates:
                fuels = dates[yesterday_display]
                # Normalize to English names
                normalized_fuels = {}
                for k, v in fuels.items():
                    key = k.strip()
                    if "Diesel" in key or "DO" in key.upper():
                        normalized_fuels["Diesel"] = v
                    elif "Regular" in key or "Regular" in key:
                        normalized_fuels["Regular"] = v
                    elif "Super" in key or "Super" in key:
                        normalized_fuels["Super"] = v
                    else:
                        normalized_fuels[key] = v
                
                diesel = normalized_fuels.get("Diesel", 0)
                regular = normalized_fuels.get("Regular", 0)
                super = normalized_fuels.get("Super", 0)
                total = diesel + regular + super
                
                total_diesel += diesel
                total_regular += regular
                total_super += super
                
                message += f"⛽ *{station}*\n"
                message += f"  Diesel: {diesel:7.2f}L | Regular: {regular:7.2f}L | Super: {super:7.2f}L | Total: {total:7.2f}L\n\n"
        
        grand_total = total_diesel + total_regular + total_super
        message += f"━━━━━━━━━━━━━━━━━━━━━\n"
        message += f"📊 *TOTAL YESTERDAY:* {grand_total:,.2f}L"
        
        await update.message.reply_text(message, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Error in yesterday_command: {e}")
        await update.message.reply_text(
            f"❌ Error loading yesterday's reports: {str(e)[:100]}",
            parse_mode="Markdown"
        )

async def weekly_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show weekly summary for all stations."""
    try:
        start_date_display, end_date_display = get_last_7_days()
        
        station_data, rows = generate_summary_by_period(start_date_display, end_date_display)
        
        if not station_data:
            await update.message.reply_text(f"📭 No reports for the last 7 days ({start_date_display} to {end_date_display}).")
            return
        
        message = f"📊 *WEEKLY SUMMARY* (Last 7 days)\━━━━━━━━━━━━━━━━━━━━━\n"
        message += f"📅 *Period:* {start_date_display} to {end_date_display}\n\n"
        
        # Calculate totals per station
        station_totals = []
        for station, dates in station_data.items():
            station_diesel = station_regular = station_super = 0
            for date_str, fuels in dates.items():
                # Normalize to English names
                normalized_fuels = {}
                for k, v in fuels.items():
                    key = k.strip()
                    if "Diesel" in key or "DO" in key.upper():
                        normalized_fuels["Diesel"] = v
                    elif "Regular" in key or "Regular" in key:
                        normalized_fuels["Regular"] = v
                    elif "Super" in key or "Super" in key:
                        normalized_fuels["Super"] = v
                    else:
                        normalized_fuels[key] = v
                
                station_diesel += normalized_fuels.get("Diesel", 0)
                station_regular += normalized_fuels.get("Regular", 0)
                station_super += normalized_fuels.get("Super", 0)

            station_total = station_diesel + station_regular + station_super
            station_totals.append((station, station_total, station_diesel, station_regular, station_super))
        
        # Sort stations by total volume (descending)
        station_totals.sort(key=lambda x: x[1], reverse=True)
        
        for station, total, diesel, regular, super in station_totals:
            message += f"⛽ *{station}*\n"
            message += f"  Total: {total:8.2f}L | Diesel: {diesel:7.2f}L | Regular: {regular:7.2f}L | Super: {super:7.2f}L\n\n"
        
        await update.message.reply_text(message, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Error in weekly_command: {e}")
        await update.message.reply_text(
            f"❌ Error loading weekly summary: {str(e)[:100]}",
            parse_mode="Markdown"
        )

async def monthly_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show monthly summary for all stations."""
    try:
        start_date_display, end_date_display = get_last_30_days()
        
        station_data, rows = generate_summary_by_period(start_date_display, end_date_display)
        
        if not station_data:
            await update.message.reply_text(f"📭 No reports for the last 30 days ({start_date_display} to {end_date_display}).")
            return

        message = f"📊 *MONTHLY SUMMARY* (Last 30 days)\n━━━━━━━━━━━━━━━━━━━━━\n"
        message += f"📅 *Period:* {start_date_display} to {end_date_display}\n\n"
        
        # Calculate totals and find top station
        top_station = None
        top_volume = 0
        grand_total = 0
        
        for station, dates in station_data.items():
            station_total = 0
            for date_str, fuels in dates.items():
                # Normalize to English names
                normalized_fuels = {}
                for k, v in fuels.items():
                    key = k.strip()
                    if "Diesel" in key or "DO" in key.upper():
                        normalized_fuels["Diesel"] = v
                    elif "Regular" in key or "Regular" in key:
                        normalized_fuels["Regular"] = v
                    elif "Super" in key or "Super" in key:
                        normalized_fuels["Super"] = v
                    else:
                        normalized_fuels[key] = v
                
                diesel = normalized_fuels.get("Diesel", 0)
                regular = normalized_fuels.get("Regular", 0)
                super = normalized_fuels.get("Super", 0)
                station_total += diesel + regular + super
            
            grand_total += station_total
            
            if station_total > top_volume:
                top_volume = station_total
                top_station = station
        
        message += f"📊 *OVERVIEW:*\n"
        message += f"• Total Volume: {grand_total:,.2f}L\n"
        message += f"• Stations with data: {len(station_data)}\n"
        if top_station:
            message += f"• Top Station: {top_station} ({top_volume:,.2f}L)\n"
        if len(station_data) > 0:
            message += f"• Average per station: {grand_total/len(station_data):,.2f}L\n\n"
        message += f"💡 *Tip:* Use /report for detailed station breakdowns"
        
        await update.message.reply_text(message, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Error in monthly_command: {e}")
        await update.message.reply_text(
            f"❌ Error loading monthly summary: {str(e)[:100]}",
            parse_mode="Markdown"
        )

async def test_db_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Test database connection and data."""
    try:
        logger.info("Testing database connection...")
        
        # Test database functions
        stations = get_all_stations()
        today = date.today()
        thirty_days_ago = today - timedelta(days=30)
        start_date_db = format_date(thirty_days_ago)
        end_date_db = format_date(today)
        
        summary = get_summary(start_date_db, end_date_db)
        
        # Test monthly functions
        current_year = today.year
        current_month = today.month
        monthly_details = get_monthly_details(current_year, current_month)
        monthly_aggregate = get_monthly_aggregate(current_year, current_month)
        
        # Test monthly station summary
        station_summary = None
        if stations:
            station_summary = get_monthly_station_summary(current_year, current_month, stations[0])
        
        message = (
            "🧪 *DATABASE TEST*\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ Database connection: OK\n"
            f"📊 Stations in DB: {len(stations)}\n"
            f"📈 Reports in last 30 days: {len(summary)}\n"
            f"📅 Date range: {start_date_db} to {end_date_db}\n"
            f"📆 Monthly details: {'✅ OK' if monthly_details is not None else '❌ Failed'}\n"
            f"📊 Monthly aggregate: {'✅ OK' if monthly_aggregate else '❌ Failed'}\n"
            f"🏪 Station summary: {'✅ OK' if station_summary is not None else '❌ Failed'}\n\n"
        )
        
        if stations:
            message += "🏪 *Stations found:*\n"
            for i, station in enumerate(stations[:5], 1):  # Show first 5
                message += f"{i}. {station}\n"
            if len(stations) > 5:
                message += f"... and {len(stations) - 5} more\n"
        else:
            message += "📭 *No stations found*\n"
            message += "Send a fuel report to add data.\n\n"
        
        if monthly_aggregate:
            message += f"📅 *Current month data:*\n"
            message += f"• Records: {monthly_aggregate.get('record_count', 0)}\n"
            message += f"• Total volume: {monthly_aggregate.get('total_volume', 0):,.2f}L\n"
            message += f"• Total amount: ${monthly_aggregate.get('total_amount', 0):,.2f}\n"
            message += f"• Stations: {monthly_aggregate.get('station_count', 0)}\n"
            message += f"• Days with data: {monthly_aggregate.get('days_with_data', 0)}\n"
        
        message += "━━━━━━━━━━━━━━━━━━━━━\n"
        message += "Use /report to view reports"
        
        await update.message.reply_text(message, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Database test failed: {e}")
        logger.error(traceback.format_exc())
        
        await update.message.reply_text(
            f"❌ *Database test failed*\nError: `{str(e)[:200]}`",
            parse_mode="Markdown"
        )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show help message."""
    await start(update, context)

# ---------------- HANDLE CUSTOM DATE INPUT ----------------
async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text input for custom dates."""
    user_id = update.message.from_user.id
    
    # Check if user is waiting for a custom date
    if user_id in user_selections and "waiting_for_date" in user_selections[user_id]:
        station = user_selections[user_id]["waiting_for_date"]
        date_input = update.message.text.strip()
        
        try:
            # Parse the date
            selected_date = parse_date_string(date_input)
            selected_date_str = format_date(selected_date)
            
            await update.message.reply_text(f"⏳ Loading report for {selected_date_str}...", parse_mode="Markdown")
            
            # Generate report for selected date
            station_data, rows = generate_summary_by_period(selected_date_str, selected_date_str, station)
            
            if not station_data or station not in station_data:
                await update.message.reply_text(
                    f"⚠️ No report found for *{station}* on {selected_date_str}",
                    parse_mode="Markdown"
                )
                # Clear waiting state
                if user_id in user_selections and "waiting_for_date" in user_selections[user_id]:
                    del user_selections[user_id]["waiting_for_date"]
                return
            
            fuels = station_data[station].get(selected_date_str, {})
            
            if not fuels:
                await update.message.reply_text(
                    f"⚠️ No report found for *{station}* on {selected_date_str}",
                    parse_mode="Markdown"
                )
                # Clear waiting state
                if user_id in user_selections and "waiting_for_date" in user_selections[user_id]:
                    del user_selections[user_id]["waiting_for_date"]
                return
            
            # Format and send single day report
            report_msg = format_single_day_report(station, selected_date_str, fuels)
            await update.message.reply_text(report_msg, parse_mode="Markdown")
            
            # Clear waiting state
            if user_id in user_selections and "waiting_for_date" in user_selections[user_id]:
                del user_selections[user_id]["waiting_for_date"]
                
        except Exception as e:
            logger.error(f"Error processing custom date input: {e}")
            await update.message.reply_text(
                f"❌ *Invalid date format*\n\n"
                f"Please send a date in *yyyy/mm/dd* format.\n"
                f"Example: `2024/12/27`\n\n"
                f"Error: `{str(e)[:100]}`",
                parse_mode="Markdown"
            )
            # Keep waiting state for retry
        return
    
    # If not waiting for custom date, check if it's a fuel report
    await handle_report(update, context)

# ---------------- ERROR HANDLER ----------------
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors in the bot with better debugging."""
    error = context.error
    
    # Log the error with as much detail as possible
    logger.error("=" * 60)
    logger.error("BOT ERROR OCCURRED")
    logger.error("=" * 60)
    
    # Get error details
    error_type = type(error).__name__
    error_message = str(error)
    
    logger.error(f"Error Type: {error_type}")
    logger.error(f"Error Message: {error_message}")
    
    # Log the full traceback
    tb_string = traceback.format_exc()
    logger.error(f"Traceback:\n{tb_string}")
    
    # Log update details if available
    if update:
        logger.error(f"Update ID: {update.update_id}")
        if update.message:
            logger.error(f"Message from: {update.message.from_user.id if update.message.from_user else 'Unknown'}")
            logger.error(f"Chat ID: {update.message.chat_id}")
            logger.error(f"Message text: {update.message.text if update.message.text else 'No text'}")
            
            # Check if it's /report command
            if update.message.text and update.message.text.startswith('/report'):
                logger.error("⚠️ This error occurred with /report command")
                logger.error(f"Full command: {update.message.text}")
        elif update.callback_query:
            logger.error(f"Callback from: {update.callback_query.from_user.id}")
            logger.error(f"Callback data: {update.callback_query.data}")
    
    logger.error("=" * 60)
    
    # Notify user
    try:
        if update and update.effective_message:
            error_msg = "❌ An error occurred while processing your request."
            
            # Specific message for /report command
            if update.message and update.message.text and '/report' in update.message.text:
                error_msg = "❌ Error loading reports. Please try /testdb to check database."
            
            full_message = (
                f"{error_msg}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"Error type: `{error_type}`\n"
                f"Please try again or contact support."
            )
            
            await update.effective_message.reply_text(
                full_message,
                parse_mode="Markdown"
            )
    except Exception as e:
        logger.error(f"Failed to send error message to user: {e}")

# ---------------- MAIN ----------------
def main():
    """Initialize and run the bot."""
    try:
        # Initialize database
        logger.info("Initializing database...")
        init_db()
        logger.info("✅ Database initialized successfully")
        
        # Test database connection
        stations = get_all_stations()
        logger.info(f"📊 Found {len(stations)} stations in database")
        
    except Exception as e:
        logger.error(f"❌ Database initialization failed: {e}")
        logger.error(traceback.format_exc())
        return
    
    try:
        # Create application
        logger.info("Creating bot application...")
        app = ApplicationBuilder() \
            .token(BOT_TOKEN) \
            .read_timeout(30) \
            .write_timeout(30) \
            .pool_timeout(30) \
            .build()
        logger.info("✅ Bot application created")
        
    except Exception as e:
        logger.error(f"❌ Failed to create bot application: {e}")
        logger.error(traceback.format_exc())
        return
    
    try:
        # Add handlers
        logger.info("Adding handlers...")
        
        # Process text messages (fuel reports and custom date input)
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))
        
        # Command handlers
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("report", report_command))
        app.add_handler(CommandHandler("stations", stations_command))
        app.add_handler(CommandHandler("stats", stats_command))
        app.add_handler(CommandHandler("today", today_command))
        app.add_handler(CommandHandler("yesterday", yesterday_command))
        app.add_handler(CommandHandler("weekly", weekly_command))
        app.add_handler(CommandHandler("monthly", monthly_command))
        app.add_handler(CommandHandler("testdb", test_db_command))
        app.add_handler(CommandHandler("help", help_command))
        
        # Callback query handler
        app.add_handler(CallbackQueryHandler(handle_callback_query))
        
        # Error handler
        app.add_error_handler(error_handler)
        
        logger.info("✅ All handlers added successfully")
        
    except Exception as e:
        logger.error(f"❌ Failed to add handlers: {e}")
        logger.error(traceback.format_exc())
        return
    
    # Start the bot
    logger.info("🤖 KONCHAT BOT STARTING...")
    logger.info(f"📊 Database has {len(stations)} stations")
    logger.info("✅ Bot is now running. Press Ctrl+C to stop.")
    logger.info("\n📋 BOT IS LISTENING FOR:")
    logger.info("   • Text messages (fuel reports)")
    logger.info("   • Custom date input (when requested)")
    logger.info("   • Commands in private chats or groups")
    logger.info("\n🔧 DEBUG COMMANDS:")
    logger.info("   • /testdb - Test database connection")
    logger.info("\n📊 MONTHLY REPORTS AVAILABLE:")
    logger.info("   • 📊 Summary - Monthly totals and averages")
    logger.info("   • 📈 Detailed - Day-by-day breakdown with amounts")
    logger.info("   • 📋 Day-by-Day - Each day's full data")
    logger.info("   • 📅 All Data - Complete monthly records")
    logger.info("\n📅 DATE FORMAT:")
    logger.info("   • All dates now use yyyy/mm/dd format")
    logger.info("   • Example: 2024/12/27")
    
    try:
        app.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
            close_loop=False,
            poll_interval=1.0
        )
    except KeyboardInterrupt:
        logger.info("\n👋 Bot stopped by user")
    except Exception as e:
        logger.error(f"❌ Bot crashed with error: {e}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()