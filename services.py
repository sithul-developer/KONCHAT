from database import get_conn
from datetime import datetime

# ---------------- SAVE REPORT ----------------
def save_report(data: dict):
    """
    Saves a daily sales report into the database.
    Stores dates in dd/mm/yy format.
    """
    conn = get_conn()
    cur = conn.cursor()

    # Get and format the report_date to dd/mm/yy
    report_date = data.get("report_date")
    formatted_date = None
    
    if isinstance(report_date, datetime):
        formatted_date = report_date.strftime('%d/%m/%y')  # dd/mm/yy
    elif isinstance(report_date, str):
        # Try to parse various formats and convert to dd/mm/yy
        try:
            # List of possible input formats
            input_formats = [
                '%Y-%m-%d',      # 2025-03-15 → 15/03/25
                '%m/%d/%Y',      # 03/15/2025 → 15/03/25
                '%m/%d/%y',      # 03/15/25 → 15/03/25
                '%d-%m-%Y',      # 15-03-2025 → 15/03/25
                '%d/%m/%Y',      # 15/03/2025 → 15/03/25
                '%d/%m/%y',      # 15/03/25 (already correct)
                '%Y%m%d',        # 20250315 → 15/03/25
            ]
            
            for fmt in input_formats:
                try:
                    dt = datetime.strptime(report_date.strip(), fmt)
                    formatted_date = dt.strftime('%d/%m/%y')
                    break
                except ValueError:
                    continue
            
            # If still not parsed, use current date
            if not formatted_date:
                formatted_date = datetime.now().strftime('%d/%m/%y')
                
        except Exception as e:
            print(f"Date parsing error for '{report_date}': {e}")
            formatted_date = datetime.now().strftime('%d/%m/%y')
    else:
        # Default to current date
        formatted_date = datetime.now().strftime('%d/%m/%y')

    for fuel in data.get("fuels", []):
        cur.execute("""
            INSERT INTO reports (
                station_name, manager_name, report_date,
                fuel_type, volume, amount, pos_variance,
                total_volume, total_amount, gain_loss, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get("station_name"),
            data.get("manager_name"),
            formatted_date,  # Now in dd/mm/yy format
            fuel.get("fuel_type"),
            fuel.get("volume"),
            fuel.get("amount"),
            fuel.get("pos_variance"),
            data.get("total_volume"),
            data.get("total_amount"),
            data.get("gain_loss"),
            data.get("status")
        ))

    conn.commit()
    conn.close()


# ---------------- CHECK DUPLICATE ----------------
def report_exists(station_name: str, report_date: str) -> bool:
    """
    Checks if a report already exists for a station on a given date.
    
    Args:
        report_date: Should be in dd/mm/yy format for consistency
    """
    conn = get_conn()
    cur = conn.cursor()
    
    # Convert input date to dd/mm/yy format if needed
    formatted_date = None
    if isinstance(report_date, str):
        try:
            # Try common formats and convert to dd/mm/yy
            for fmt in ['%d/%m/%y', '%d/%m/%Y', '%Y-%m-%d', '%m/%d/%Y', '%m/%d/%y']:
                try:
                    dt = datetime.strptime(report_date.strip(), fmt)
                    formatted_date = dt.strftime('%d/%m/%y')
                    break
                except ValueError:
                    continue
        except:
            formatted_date = report_date
    
    if not formatted_date:
        formatted_date = str(report_date)
    
    cur.execute("""
        SELECT 1 FROM reports
        WHERE station_name=? AND report_date=?
        LIMIT 1
    """, (station_name, formatted_date))
    
    exists = cur.fetchone() is not None
    conn.close()
    return exists


# ---------------- GET SUMMARY ----------------
def get_summary(start_date: str = None, end_date: str = None) -> list:
    """
    Returns aggregated report data.
    
    Args:
        start_date (str): Start date in 'dd/mm/yy' format
        end_date (str): End date in 'dd/mm/yy' format
    
    Returns:
        list of tuples: (station_name, fuel_type, SUM(volume), SUM(amount), report_date)
    """
    conn = get_conn()
    cur = conn.cursor()

    # Base query
    query = """
        SELECT 
            station_name, 
            fuel_type, 
            SUM(volume) as total_volume, 
            SUM(amount) as total_amount,
            report_date
        FROM reports
    """
    
    params = []
    
    if start_date and end_date:
        # Convert input dates to datetime for proper comparison
        try:
            # Parse input dates from dd/mm/yy to datetime
            start_dt = datetime.strptime(start_date, '%d/%m/%y')
            end_dt = datetime.strptime(end_date, '%d/%m/%y')
            
            # We need to compare dates properly. Since they're stored as dd/mm/yy strings,
            # we'll convert them to sortable format in the query
            query += """
                WHERE 
                    CAST(
                        SUBSTR(report_date, 7, 2) ||  -- Year (yy)
                        SUBSTR(report_date, 4, 2) ||  -- Month (mm)
                        SUBSTR(report_date, 1, 2)     -- Day (dd)
                    AS INTEGER
                    ) BETWEEN ? AND ?
            """
            
            # Convert start and end dates to yymmdd integer format for comparison
            start_sortable = int(start_dt.strftime('%y%m%d'))
            end_sortable = int(end_dt.strftime('%y%m%d'))
            
            params = [start_sortable, end_sortable]
            
        except ValueError as e:
            print(f"Invalid date format: {e}. Expected dd/mm/yy")
            # Fall back to no date filter
            pass

    query += """
        GROUP BY station_name, fuel_type, report_date 
        ORDER BY 
            station_name,
            CAST(
                SUBSTR(report_date, 7, 2) || 
                SUBSTR(report_date, 4, 2) || 
                SUBSTR(report_date, 1, 2)
            AS INTEGER),
            fuel_type
    """
    
    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()
    return rows


# ---------------- GET STATION SUMMARY ----------------
def get_station_summary(station_name: str = None, start_date: str = None, end_date: str = None) -> list:
    """
    Get summary for specific station with date range.
    
    Args:
        station_name: Optional station filter
        start_date: dd/mm/yy format
        end_date: dd/mm/yy format
    
    Returns:
        list of tuples: (report_date, fuel_type, volume, amount)
    """
    conn = get_conn()
    cur = conn.cursor()
    
    query = """
        SELECT report_date, fuel_type, SUM(volume), SUM(amount)
        FROM reports
        WHERE 1=1
    """
    
    params = []
    
    if station_name:
        query += " AND station_name = ?"
        params.append(station_name)
    
    if start_date and end_date:
        try:
            # Parse dates
            start_dt = datetime.strptime(start_date, '%d/%m/%y')
            end_dt = datetime.strptime(end_date, '%d/%m/%y')
            
            # Convert to sortable format for comparison
            start_sortable = int(start_dt.strftime('%y%m%d'))
            end_sortable = int(end_dt.strftime('%y%m%d'))
            
            query += """
                AND CAST(
                    SUBSTR(report_date, 7, 2) || 
                    SUBSTR(report_date, 4, 2) || 
                    SUBSTR(report_date, 1, 2)
                AS INTEGER) BETWEEN ? AND ?
            """
            params.extend([start_sortable, end_sortable])
            
        except ValueError:
            print("Invalid date format in get_station_summary. Expected dd/mm/yy")
    
    query += """
        GROUP BY report_date, fuel_type 
        ORDER BY 
            CAST(
                SUBSTR(report_date, 7, 2) || 
                SUBSTR(report_date, 4, 2) || 
                SUBSTR(report_date, 1, 2)
            AS INTEGER),
            fuel_type
    """
    
    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()
    return rows


# ---------------- GET ALL STATIONS ----------------
def get_all_stations() -> list:
    """Get list of all unique station names."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT station_name FROM reports ORDER BY station_name")
    stations = [row[0] for row in cur.fetchall()]
    conn.close()
    return stations


# ---------------- GET ALL DATES ----------------
def get_all_dates() -> list:
    """
    Get list of all unique report dates in dd/mm/yy format.
    Returns dates sorted chronologically.
    """
    conn = get_conn()
    cur = conn.cursor()
    
    # Get all dates and sort them properly
    cur.execute("""
        SELECT DISTINCT report_date 
        FROM reports 
        ORDER BY 
            CAST(
                SUBSTR(report_date, 7, 2) || 
                SUBSTR(report_date, 4, 2) || 
                SUBSTR(report_date, 1, 2)
            AS INTEGER)
    """)
    
    dates = [row[0] for row in cur.fetchall()]
    conn.close()
    return dates


# ---------------- MIGRATION FUNCTION ----------------
def migrate_dates_to_ddmmyy_format():
    """
    Migrate existing dates to dd/mm/yy format.
    Run this once if you have old data in different formats.
    """
    conn = get_conn()
    cur = conn.cursor()
    
    # Get all distinct dates in old format
    cur.execute("SELECT DISTINCT report_date FROM reports")
    dates = [row[0] for row in cur.fetchall()]
    
    updated_count = 0
    for old_date in dates:
        try:
            # Try to parse various formats
            input_formats = [
                '%Y-%m-%d',      # 2025-03-15
                '%m/%d/%Y',      # 03/15/2025
                '%m/%d/%y',      # 03/15/25
                '%d-%m-%Y',      # 15-03-2025
                '%d/%m/%Y',      # 15/03/2025
                '%Y%m%d',        # 20250315
                '%d/%m/%y',      # 15/03/25 (already correct)
            ]
            
            dt = None
            for fmt in input_formats:
                try:
                    dt = datetime.strptime(old_date, fmt)
                    break
                except ValueError:
                    continue
            
            if dt:
                new_date = dt.strftime('%d/%m/%y')
                
                # Update all records with this date
                cur.execute(
                    "UPDATE reports SET report_date = ? WHERE report_date = ?",
                    (new_date, old_date)
                )
                updated_count += cur.rowcount
            else:
                print(f"Could not parse date: {old_date}")
                
        except Exception as e:
            print(f"Error processing date '{old_date}': {e}")
            continue
    
    conn.commit()
    conn.close()
    print(f"Migrated {updated_count} records to dd/mm/yy format")


# ---------------- HELPER FUNCTIONS ----------------
def convert_to_ddmmyy(date_input):
    """
    Convert any date input to dd/mm/yy format.
    
    Args:
        date_input: datetime object or string in any common format
        
    Returns:
        str: Date in dd/mm/yy format
    """
    if isinstance(date_input, datetime):
        return date_input.strftime('%d/%m/%y')
    
    if isinstance(date_input, str):
        # Try common formats
        formats = [
            '%Y-%m-%d', '%m/%d/%Y', '%m/%d/%y',
            '%d-%m-%Y', '%d/%m/%Y', '%Y%m%d'
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_input.strip(), fmt)
                return dt.strftime('%d/%m/%y')
            except ValueError:
                continue
        
        # If already in dd/mm/yy, return as-is
        try:
            datetime.strptime(date_input, '%d/%m/%y')
            return date_input
        except ValueError:
            pass
    
    # Default to current date
    return datetime.now().strftime('%d/%m/%y')


def ddmmyy_to_sortable(date_str: str) -> int:
    """
    Convert dd/mm/yy date to sortable integer (yymmdd).
    
    Args:
        date_str: Date in dd/mm/yy format
        
    Returns:
        int: Date as yymmdd integer
    """
    try:
        dt = datetime.strptime(date_str, '%d/%m/%y')
        return int(dt.strftime('%y%m%d'))
    except ValueError:
        return 0


# ---------------- TEST THE FUNCTIONS ----------------
if __name__ == "__main__":
    # Test data
    test_data = {
        "station_name": "Test Station",
        "manager_name": "Test Manager",
        "report_date": "15/03/25",  # dd/mm/yy format
        "fuels": [
            {"fuel_type": "DO", "volume": 100.5, "amount": 500.25, "pos_variance": 0},
            {"fuel_type": "EA92", "volume": 200.3, "amount": 800.75, "pos_variance": 0},
        ],
        "total_volume": 300.8,
        "total_amount": 1301.0,
        "gain_loss": 0,
        "status": "completed"
    }
    
    # Test save_report
    save_report(test_data)
    print("Test report saved successfully")
    
    # Test report_exists
    exists = report_exists("Test Station", "15/03/25")
    print(f"Report exists: {exists}")
    
    # Test get_summary
    summary = get_summary(start_date="01/03/25", end_date="31/03/25")
    print(f"Summary records: {len(summary)}")
    
    # Test get_all_dates
    dates = get_all_dates()
    print(f"All dates: {dates}")