import sqlite3
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Tuple
import logging
import calendar
import re

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_NAME = "konchat.db"

def get_conn() -> sqlite3.Connection:
    """Get database connection with row factory for dict-like access."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row  # This allows dict-like access to rows
    # Enable foreign keys
    conn.execute("PRAGMA foreign_keys = ON")
    # Better performance settings
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA cache_size = -2000")  # 2MB cache
    return conn

def init_db():
    """Initialize database with optimized schema."""
    conn = get_conn()
    cur = conn.cursor()

    # Main reports table (stores aggregated fuel data per station per day)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        station_name TEXT NOT NULL,
        report_date TEXT NOT NULL,  -- Format: dd/mm/yy
        total_volume REAL DEFAULT 0,
        total_amount REAL DEFAULT 0,
        pump_count INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(station_name, report_date)  -- Prevent duplicate reports
    )
    """)

    # Fuel details table (stores individual fuel types)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fuel_details (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_id INTEGER NOT NULL,
        fuel_type TEXT NOT NULL,
        volume REAL NOT NULL,
        amount REAL NOT NULL,
        FOREIGN KEY (report_id) REFERENCES reports (id) ON DELETE CASCADE,
        UNIQUE(report_id, fuel_type)
    )
    """)

    # Pump details table (optional - stores pump-by-pump data)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS pump_details (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_id INTEGER NOT NULL,
        pump_number TEXT,
        fuel_type TEXT,
        volume REAL,
        amount REAL,
        FOREIGN KEY (report_id) REFERENCES reports (id) ON DELETE CASCADE
    )
    """)

    # Create indexes for faster queries
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_reports_station_date 
    ON reports (station_name, report_date DESC)
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_reports_date 
    ON reports (report_date DESC)
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_reports_station 
    ON reports (station_name)
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_fuel_details_report 
    ON fuel_details (report_id)
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_pump_details_report 
    ON pump_details (report_id)
    """)
    
    # Index for fuel_type in fuel_details for faster monthly queries
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_fuel_details_type 
    ON fuel_details (fuel_type)
    """)

    # Create materialized view for daily summaries (optional, for performance)
    try:
        cur.execute("""
        CREATE VIEW IF NOT EXISTS daily_summary_view AS
        SELECT 
            r.report_date,
            r.station_name,
            fd.fuel_type,
            SUM(fd.volume) as total_volume,
            SUM(fd.amount) as total_amount,
            COUNT(*) as record_count
        FROM reports r
        JOIN fuel_details fd ON r.id = fd.report_id
        GROUP BY r.report_date, r.station_name, fd.fuel_type
        """)
    except:
        pass  # View might already exist with different structure

    # Create triggers to update updated_at timestamp
    cur.execute("""
    CREATE TRIGGER IF NOT EXISTS update_reports_timestamp 
    AFTER UPDATE ON reports
    BEGIN
        UPDATE reports SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
    END;
    """)

    conn.commit()
    conn.close()
    logger.info("Database initialized successfully")

def save_report(data: Dict[str, Any]) -> bool:
    """
    Save a parsed report to the database.
    
    Expected data format:
    {
        "station_name": "BVM ព្រែកអញ្ចាញ",
        "report_date": "27/12/25",
        "fuel_data": [
            {"fuel_type": "Diesel", "volume": 790.26, "amount": 695.45},
            {"fuel_type": "Regular", "volume": 1798.43, "amount": 1690.48},
            {"fuel_type": "Super", "volume": 543.15, "amount": 586.53}
        ],
        "total_volume": 3131.84,
        "total_amount": 2972.46,
        "pump_data": [...]  # Optional
    }
    """
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        # Check if report already exists
        station_name = data.get("station_name", "UNKNOWN")
        report_date = data.get("report_date", "")
        
        # Get total volume and amount
        total_volume = data.get("total_volume", 0.0)
        total_amount = data.get("total_amount", 0.0)
        
        # Calculate totals from fuel_data if not provided
        if total_volume == 0.0 or total_amount == 0.0:
            fuel_data = data.get("fuel_data", [])
            if fuel_data:
                total_volume = sum(item.get("volume", 0.0) for item in fuel_data)
                total_amount = sum(item.get("amount", 0.0) for item in fuel_data)
        
        # Get pump count
        pump_data = data.get("pump_data", [])
        pump_count = len(pump_data)
        
        # Insert or update main report
        cur.execute("""
        INSERT OR REPLACE INTO reports 
        (station_name, report_date, total_volume, total_amount, pump_count)
        VALUES (?, ?, ?, ?, ?)
        """, (station_name, report_date, total_volume, total_amount, pump_count))
        
        # Get the report ID
        report_id = cur.lastrowid
        if not report_id:
            # If it was an UPDATE (not INSERT), get the existing ID
            cur.execute("""
            SELECT id FROM reports 
            WHERE station_name = ? AND report_date = ?
            """, (station_name, report_date))
            result = cur.fetchone()
            report_id = result["id"] if result else None
        
        if not report_id:
            logger.error("Failed to get report ID")
            conn.rollback()
            conn.close()
            return False
        
        # Save fuel details (delete old ones first if updating)
        cur.execute("DELETE FROM fuel_details WHERE report_id = ?", (report_id,))
        
        fuel_data = data.get("fuel_data", [])
        for fuel in fuel_data:
            fuel_type = fuel.get("fuel_type", "").strip()
            volume = fuel.get("volume", 0.0)
            amount = fuel.get("amount", 0.0)
            
            if fuel_type:  # Only save if we have a fuel type
                cur.execute("""
                INSERT INTO fuel_details 
                (report_id, fuel_type, volume, amount)
                VALUES (?, ?, ?, ?)
                """, (report_id, fuel_type, volume, amount))
        
        # Save pump details (optional) - delete old ones first
        cur.execute("DELETE FROM pump_details WHERE report_id = ?", (report_id,))
        
        pump_data = data.get("pump_data", [])
        if pump_data and report_id:
            for pump in pump_data:
                pump_number = pump.get("pump_number", "")
                fuels = pump.get("fuels", [])
                
                for fuel in fuels:
                    fuel_type = fuel.get("fuel_type", "").strip()
                    volume = fuel.get("volume", 0.0)
                    amount = fuel.get("amount", 0.0)
                    
                    if fuel_type:
                        cur.execute("""
                        INSERT INTO pump_details 
                        (report_id, pump_number, fuel_type, volume, amount)
                        VALUES (?, ?, ?, ?, ?)
                        """, (report_id, pump_number, fuel_type, volume, amount))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Report saved successfully: {station_name} - {report_date}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving report: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False

def report_exists(station_name: str, report_date: str) -> bool:
    """Check if a report already exists for given station and date."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        cur.execute("""
        SELECT COUNT(*) as count 
        FROM reports 
        WHERE station_name = ? AND report_date = ?
        """, (station_name, report_date))
        
        result = cur.fetchone()
        exists = result["count"] > 0 if result else False
        
        conn.close()
        return exists
        
    except Exception as e:
        logger.error(f"Error checking report existence: {str(e)}")
        return False

def get_summary(start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict]:
    """
    Get summary data for date range.
    Returns: List of tuples (station_name, fuel_type, volume, amount, report_date)
    """
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        query = """
        SELECT 
            r.station_name,
            fd.fuel_type,
            fd.volume,
            fd.amount,
            r.report_date
        FROM reports r
        JOIN fuel_details fd ON r.id = fd.report_id
        WHERE 1=1
        """
        
        params = []
        
        if start_date:
            query += " AND r.report_date >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND r.report_date <= ?"
            params.append(end_date)
        
        query += " ORDER BY r.report_date DESC, r.station_name, fd.fuel_type"
        
        cur.execute(query, params)
        rows = cur.fetchall()
        
        # Convert to list of dicts
        result = []
        for row in rows:
            result.append({
                "station_name": row["station_name"],
                "fuel_type": row["fuel_type"],
                "volume": row["volume"],
                "amount": row["amount"],
                "report_date": row["report_date"]
            })
        
        conn.close()
        logger.info(f"Retrieved {len(result)} summary records")
        return result
        
    except Exception as e:
        logger.error(f"Error getting summary: {str(e)}")
        return []

def get_all_stations() -> List[str]:
    """Get list of all unique station names."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        cur.execute("""
        SELECT DISTINCT station_name 
        FROM reports 
        ORDER BY station_name COLLATE NOCASE
        """)
        rows = cur.fetchall()
        
        stations = [row["station_name"] for row in rows]
        
        conn.close()
        logger.info(f"Retrieved {len(stations)} unique stations")
        return stations
        
    except Exception as e:
        logger.error(f"Error getting stations: {str(e)}")
        return []

def get_station_report(station_name: str, report_date: str) -> Optional[Dict]:
    """Get complete report for a specific station and date."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        # Get main report
        cur.execute("""
        SELECT * FROM reports 
        WHERE station_name = ? AND report_date = ?
        """, (station_name, report_date))
        
        report_row = cur.fetchone()
        if not report_row:
            conn.close()
            return None
        
        # Get fuel details
        cur.execute("""
        SELECT fuel_type, volume, amount 
        FROM fuel_details 
        WHERE report_id = ?
        ORDER BY fuel_type
        """, (report_row["id"],))
        
        fuel_rows = cur.fetchall()
        fuel_data = [dict(row) for row in fuel_rows]
        
        # Get pump details (optional)
        cur.execute("""
        SELECT pump_number, fuel_type, volume, amount 
        FROM pump_details 
        WHERE report_id = ?
        ORDER BY pump_number, fuel_type
        """, (report_row["id"],))
        
        pump_rows = cur.fetchall()
        pump_data = [dict(row) for row in pump_rows]
        
        # Combine everything
        report = dict(report_row)
        report["fuel_data"] = fuel_data
        report["pump_data"] = pump_data
        
        conn.close()
        return report
        
    except Exception as e:
        logger.error(f"Error getting station report: {str(e)}")
        return None

def get_daily_summary(report_date: str) -> List[Dict]:
    """Get summary for all stations on a specific date."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        cur.execute("""
        SELECT 
            r.station_name,
            SUM(fd.volume) as total_volume,
            SUM(fd.amount) as total_amount,
            GROUP_CONCAT(fd.fuel_type || ': ' || ROUND(fd.volume, 2) || 'L') as fuel_breakdown
        FROM reports r
        JOIN fuel_details fd ON r.id = fd.report_id
        WHERE r.report_date = ?
        GROUP BY r.station_name
        ORDER BY SUM(fd.volume) DESC
        """, (report_date,))
        
        rows = cur.fetchall()
        result = [dict(row) for row in rows]
        
        conn.close()
        return result
        
    except Exception as e:
        logger.error(f"Error getting daily summary: {str(e)}")
        return []

def get_date_range() -> Tuple[str, str]:
    """Get the earliest and latest report dates in database."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        cur.execute("""
        SELECT MIN(report_date) as earliest, MAX(report_date) as latest 
        FROM reports
        """)
        row = cur.fetchone()
        
        conn.close()
        return (row["earliest"] if row["earliest"] else "", 
                row["latest"] if row["latest"] else "")
        
    except Exception as e:
        logger.error(f"Error getting date range: {str(e)}")
        return ("", "")

def get_station_statistics(station_name: str = None) -> Dict:
    """Get statistics for a specific station or all stations."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        if station_name:
            # Statistics for specific station
            cur.execute("""
            SELECT 
                COUNT(DISTINCT report_date) as report_count,
                SUM(total_volume) as total_volume,
                SUM(total_amount) as total_amount,
                AVG(total_volume) as avg_daily_volume,
                MIN(report_date) as first_report,
                MAX(report_date) as last_report
            FROM reports
            WHERE station_name = ?
            """, (station_name,))
        else:
            # Statistics for all stations
            cur.execute("""
            SELECT 
                COUNT(DISTINCT station_name) as station_count,
                COUNT(DISTINCT report_date) as total_days,
                COUNT(*) as total_reports,
                SUM(total_volume) as total_volume,
                SUM(total_amount) as total_amount,
                AVG(total_volume) as avg_daily_volume,
                MIN(report_date) as first_report,
                MAX(report_date) as last_report
            FROM reports
            """)
        
        row = cur.fetchone()
        conn.close()
        
        return dict(row) if row else {}
        
    except Exception as e:
        logger.error(f"Error getting statistics: {str(e)}")
        return {}

def delete_report(station_name: str, report_date: str) -> bool:
    """Delete a specific report."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        cur.execute("""
        DELETE FROM reports 
        WHERE station_name = ? AND report_date = ?
        """, (station_name, report_date))
        
        affected = cur.rowcount
        conn.commit()
        conn.close()
        
        logger.info(f"Deleted {affected} report(s) for {station_name} on {report_date}")
        return affected > 0
        
    except Exception as e:
        logger.error(f"Error deleting report: {str(e)}")
        return False

# ==================== MONTHLY REPORTING FUNCTIONS ====================

def get_monthly_details(year: int, month: int, station: str = None) -> List[Dict[str, Any]]:
    """
    Optimized version for better performance.
    
    Uses indexed queries and optimized SQL.
    """
    conn = None
    try:
        # Quick validation
        if month < 1 or month > 12:
            return []
        
        # Create date range
        start_date = date(year, month, 1)
        if month == 12:
            end_date = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = date(year, month + 1, 1) - timedelta(days=1)
        
        start_str = start_date.strftime("%d/%m/%y")
        end_str = end_date.strftime("%d/%m/%y")
        
        conn = get_conn()
        if not conn:
            return []
        
        # Use parameterized queries for security and performance
        if station:
            # Check if station exists first (optimization)
            station_check = conn.execute(
                "SELECT 1 FROM reports WHERE station_name = ? LIMIT 1",
                (station,)
            ).fetchone()
            
            if not station_check:
                logger.info(f"Station '{station}' not found in database")
                return []
            
            # Optimized query with EXISTS clause
            query = """
            WITH monthly_reports AS (
                SELECT id, report_date, station_name
                FROM reports 
                WHERE report_date BETWEEN ? AND ?
                AND station_name = ?
            )
            SELECT 
                mr.report_date,
                mr.station_name,
                fd.fuel_type,
                SUM(fd.volume) as total_volume,
                SUM(fd.amount) as total_amount
            FROM monthly_reports mr
            JOIN fuel_details fd ON mr.id = fd.report_id
            GROUP BY mr.report_date, mr.station_name, fd.fuel_type
            ORDER BY mr.report_date, fd.fuel_type
            """
            params = (start_str, end_str, station)
        else:
            # Optimized query for all stations
            query = """
            WITH monthly_reports AS (
                SELECT id, report_date, station_name
                FROM reports 
                WHERE report_date BETWEEN ? AND ?
            )
            SELECT 
                mr.report_date,
                mr.station_name,
                fd.fuel_type,
                SUM(fd.volume) as total_volume,
                SUM(fd.amount) as total_amount
            FROM monthly_reports mr
            JOIN fuel_details fd ON mr.id = fd.report_id
            GROUP BY mr.station_name, mr.report_date, fd.fuel_type
            ORDER BY mr.station_name, mr.report_date, fd.fuel_type
            """
            params = (start_str, end_str)
        
        # Execute with fetchmany for large datasets
        cursor = conn.execute(query, params)
        
        # Process in chunks for large datasets
        chunk_size = 1000
        result = []
        
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            
            for row in rows:
                result.append({
                    'report_date': row['report_date'],
                    'station_name': row['station_name'],
                    'fuel_type': row['fuel_type'],
                    'total_volume': float(row['total_volume'] or 0),
                    'total_amount': float(row['total_amount'] or 0)
                })
        
        logger.debug(f"Retrieved {len(result)} records in optimized query")
        return result
        
    except Exception as e:
        logger.error(f"Error in optimized query: {e}")
        return []
        
    finally:
        if conn:
            conn.close()
def get_date_range_summary(start_date: str, end_date: str, station: str = None) -> List[Dict]:
    """
    Get summary data for a specific date range with optimized query.
    
    Args:
        start_date (str): Start date in dd/mm/yy format
        end_date (str): End date in dd/mm/yy format
        station (str, optional): Station name to filter by
    
    Returns:
        List[Dict]: List of summary records
    """
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        query = """
        SELECT 
            r.station_name,
            fd.fuel_type,
            SUM(fd.volume) as volume,
            SUM(fd.amount) as amount,
            r.report_date
        FROM reports r
        JOIN fuel_details fd ON r.id = fd.report_id
        WHERE r.report_date BETWEEN ? AND ?
        """
        
        params = [start_date, end_date]
        
        if station:
            query += " AND r.station_name = ?"
            params.append(station)
        
        query += " GROUP BY r.station_name, r.report_date, fd.fuel_type"
        query += " ORDER BY r.report_date DESC, r.station_name, fd.fuel_type"
        
        cur.execute(query, params)
        rows = cur.fetchall()
        
        # Convert to list of dicts
        result = []
        for row in rows:
            result.append({
                "station_name": row["station_name"],
                "fuel_type": row["fuel_type"],
                "volume": row["volume"],
                "amount": row["amount"],
                "report_date": row["report_date"]
            })
        
        conn.close()
        logger.info(f"Retrieved {len(result)} records for date range {start_date} to {end_date}")
        return result
        
    except Exception as e:
        logger.error(f"Error in get_date_range_summary: {str(e)}")
        return []
def get_station_summary_by_date_range(station: str, start_date: str, end_date: str) -> List[Dict]:
    """
    Get summary for a specific station within a date range.
    
    Args:
        station (str): Station name
        start_date (str): Start date in dd/mm/yy format
        end_date (str): End date in dd/mm/yy format
    
    Returns:
        List[Dict]: List of summary records
    """
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        query = """
        SELECT 
            r.report_date,
            fd.fuel_type,
            fd.volume,
            fd.amount
        FROM reports r
        JOIN fuel_details fd ON r.id = fd.report_id
        WHERE r.station_name = ? 
            AND r.report_date BETWEEN ? AND ?
        ORDER BY r.report_date DESC, fd.fuel_type
        """
        
        cur.execute(query, (station, start_date, end_date))
        rows = cur.fetchall()
        
        result = [dict(row) for row in rows]
        conn.close()
        return result
        
    except Exception as e:
        logger.error(f"Error in get_station_summary_by_date_range: {str(e)}")
        return []

def get_all_stations_summary(start_date: str, end_date: str) -> Dict[str, List[Dict]]:
    """
    Get summary for all stations within a date range, grouped by station.
    
    Args:
        start_date (str): Start date in dd/mm/yy format
        end_date (str): End date in dd/mm/yy format
    
    Returns:
        Dict[str, List[Dict]]: Dictionary with station names as keys
    """
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        query = """
        SELECT 
            r.station_name,
            r.report_date,
            fd.fuel_type,
            fd.volume,
            fd.amount
        FROM reports r
        JOIN fuel_details fd ON r.id = fd.report_id
        WHERE r.report_date BETWEEN ? AND ?
        ORDER BY r.station_name, r.report_date DESC, fd.fuel_type
        """
        
        cur.execute(query, (start_date, end_date))
        rows = cur.fetchall()
        
        # Group by station
        result = {}
        for row in rows:
            station = row["station_name"]
            if station not in result:
                result[station] = []
            
            result[station].append({
                "report_date": row["report_date"],
                "fuel_type": row["fuel_type"],
                "volume": row["volume"],
                "amount": row["amount"]
            })
        
        conn.close()
        return result
        
    except Exception as e:
        logger.error(f"Error in get_all_stations_summary: {str(e)}")
        return {}
def get_monthly_aggregate(year: int, month: int, station: str = None) -> Dict[str, Any]:
    """
    Get aggregated monthly statistics.
    
    Args:
        year (int): Year (e.g., 2025)
        month (int): Month (1-12)
        station (str, optional): Station name to filter by. If None, returns all stations.
    
    Returns:
        Dict[str, Any]: Aggregated monthly statistics
    """
    try:
        conn = get_conn()
        
        # Create date range for the month
        start_date_obj = date(year, month, 1)
        if month == 12:
            end_date_obj = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date_obj = date(year, month + 1, 1) - timedelta(days=1)
        
        # Format dates for database query
        start_date_str = start_date_obj.strftime("%d/%m/%y")
        end_date_str = end_date_obj.strftime("%d/%m/%y")
        
        if station:
            # Query for specific station
            query = """
            SELECT 
                COUNT(DISTINCT r.report_date) as days_with_data,
                COUNT(DISTINCT r.station_name) as station_count,
                COUNT(*) as record_count,
                SUM(fd.volume) as total_volume,
                SUM(fd.amount) as total_amount,
                AVG(fd.volume) as avg_daily_volume,
                MIN(r.report_date) as first_date,
                MAX(r.report_date) as last_date
            FROM reports r
            JOIN fuel_details fd ON r.id = fd.report_id
            WHERE r.report_date BETWEEN ? AND ?
                AND r.station_name = ?
            """
            params = (start_date_str, end_date_str, station)
        else:
            # Query for all stations
            query = """
            SELECT 
                COUNT(DISTINCT r.report_date) as days_with_data,
                COUNT(DISTINCT r.station_name) as station_count,
                COUNT(*) as record_count,
                SUM(fd.volume) as total_volume,
                SUM(fd.amount) as total_amount,
                AVG(fd.volume) as avg_daily_volume,
                MIN(r.report_date) as first_date,
                MAX(r.report_date) as last_date
            FROM reports r
            JOIN fuel_details fd ON r.id = fd.report_id
            WHERE r.report_date BETWEEN ? AND ?
            """
            params = (start_date_str, end_date_str)
        
        row = conn.execute(query, params).fetchone()
        
        if not row:
            conn.close()
            return {
                "days_with_data": 0,
                "station_count": 0,
                "record_count": 0,
                "total_volume": 0,
                "total_amount": 0,
                "avg_daily_volume": 0,
                "first_date": None,
                "last_date": None
            }
        
        result = dict(row)
        
        # Get fuel type breakdown
        fuel_breakdown_query = """
        SELECT 
            fd.fuel_type,
            SUM(fd.volume) as total_volume,
            SUM(fd.amount) as total_amount
        FROM reports r
        JOIN fuel_details fd ON r.id = fd.report_id
        WHERE r.report_date BETWEEN ? AND ?
        """
        fuel_params = (start_date_str, end_date_str)
        
        if station:
            fuel_breakdown_query += " AND r.station_name = ?"
            fuel_params = (start_date_str, end_date_str, station)
        
        fuel_breakdown_query += " GROUP BY fd.fuel_type ORDER BY SUM(fd.volume) DESC"
        
        fuel_rows = conn.execute(fuel_breakdown_query, fuel_params).fetchall()
        result["fuel_breakdown"] = [dict(row) for row in fuel_rows]
        
        conn.close()
        return result
        
    except Exception as e:
        logger.error(f"Database error in get_monthly_aggregate: {e}")
        return {}

def get_monthly_station_summary(year: int, month: int) -> List[Dict[str, Any]]:
    """
    Get monthly summary for all stations.
    
    Args:
        year (int): Year (e.g., 2025)
        month (int): Month (1-12)
    
    Returns:
        List[Dict[str, Any]]: Summary for each station
    """
    try:
        conn = get_conn()
        
        # Create date range for the month
        start_date_obj = date(year, month, 1)
        if month == 12:
            end_date_obj = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date_obj = date(year, month + 1, 1) - timedelta(days=1)
        
        start_date_str = start_date_obj.strftime("%d/%m/%y")
        end_date_str = end_date_obj.strftime("%d/%m/%y")
        
        query = """
        SELECT 
            r.station_name,
            COUNT(DISTINCT r.report_date) as days_reported,
            SUM(fd.volume) as total_volume,
            SUM(fd.amount) as total_amount,
            AVG(fd.volume) as avg_daily_volume,
            MIN(r.report_date) as first_report,
            MAX(r.report_date) as last_report,
            GROUP_CONCAT(DISTINCT fd.fuel_type) as fuel_types
        FROM reports r
        JOIN fuel_details fd ON r.id = fd.report_id
        WHERE r.report_date BETWEEN ? AND ?
        GROUP BY r.station_name
        ORDER BY total_volume DESC
        """
        
        rows = conn.execute(query, (start_date_str, end_date_str)).fetchall()
        result = []
        
        for row in rows:
            row_dict = dict(row)
            # Calculate percentages if needed
            result.append(row_dict)
        
        conn.close()
        return result
        
    except Exception as e:
        logger.error(f"Database error in get_monthly_station_summary: {e}")
        return []

def get_monthly_fuel_summary(year: int, month: int) -> List[Dict[str, Any]]:
    """
    Get monthly summary by fuel type.
    
    Args:
        year (int): Year (e.g., 2025)
        month (int): Month (1-12)
    
    Returns:
        List[Dict[str, Any]]: Summary for each fuel type
    """
    try:
        conn = get_conn()
        
        # Create date range for the month
        start_date_obj = date(year, month, 1)
        if month == 12:
            end_date_obj = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date_obj = date(year, month + 1, 1) - timedelta(days=1)
        
        start_date_str = start_date_obj.strftime("%d/%m/%y")
        end_date_str = end_date_obj.strftime("%d/%m/%y")
        
        query = """
        SELECT 
            fd.fuel_type,
            COUNT(DISTINCT r.report_date) as days_reported,
            COUNT(DISTINCT r.station_name) as stations_reported,
            SUM(fd.volume) as total_volume,
            SUM(fd.amount) as total_amount,
            AVG(fd.volume) as avg_volume_per_report,
            MIN(r.report_date) as first_report,
            MAX(r.report_date) as last_report
        FROM reports r
        JOIN fuel_details fd ON r.id = fd.report_id
        WHERE r.report_date BETWEEN ? AND ?
        GROUP BY fd.fuel_type
        ORDER BY total_volume DESC
        """
        
        rows = conn.execute(query, (start_date_str, end_date_str)).fetchall()
        result = [dict(row) for row in rows]
        
        conn.close()
        return result
        
    except Exception as e:
        logger.error(f"Database error in get_monthly_fuel_summary: {e}")
        return []

def get_monthly_trend(station: str = None, months: int = 6) -> List[Dict[str, Any]]:
    """
    Get trend data for the last N months.
    
    Args:
        station (str, optional): Station name to filter by
        months (int): Number of months to include
    
    Returns:
        List[Dict[str, Any]]: Monthly trend data
    """
    try:
        conn = get_conn()
        
        # Calculate date range
        end_date = date.today()
        start_date = date(end_date.year, end_date.month, 1) - timedelta(days=30 * (months - 1))
        
        start_date_str = start_date.strftime("%d/%m/%y")
        end_date_str = end_date.strftime("%d/%m/%y")
        
        query = """
        SELECT 
            SUBSTR(r.report_date, 4) as month_year,
            SUM(fd.volume) as total_volume,
            SUM(fd.amount) as total_amount,
            COUNT(DISTINCT r.report_date) as days_reported
        FROM reports r
        JOIN fuel_details fd ON r.id = fd.report_id
        WHERE r.report_date BETWEEN ? AND ?
        """
        
        params = [start_date_str, end_date_str]
        
        if station:
            query += " AND r.station_name = ?"
            params.append(station)
        
        query += """
        GROUP BY SUBSTR(r.report_date, 4)
        ORDER BY month_year
        """
        
        rows = conn.execute(query, params).fetchall()
        result = [dict(row) for row in rows]
        
        conn.close()
        return result
        
    except Exception as e:
        logger.error(f"Database error in get_monthly_trend: {e}")
        return []

def get_top_stations(year: int, month: int, limit: int = 5) -> List[Dict[str, Any]]:
    """
    Get top performing stations for a specific month.
    
    Args:
        year (int): Year
        month (int): Month
        limit (int): Number of top stations to return
    
    Returns:
        List[Dict[str, Any]]: Top stations with their volumes
    """
    try:
        conn = get_conn()
        
        # Create date range for the month
        start_date_obj = date(year, month, 1)
        if month == 12:
            end_date_obj = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date_obj = date(year, month + 1, 1) - timedelta(days=1)
        
        start_date_str = start_date_obj.strftime("%d/%m/%y")
        end_date_str = end_date_obj.strftime("%d/%m/%y")
        
        query = """
        SELECT 
            r.station_name,
            SUM(fd.volume) as total_volume,
            SUM(fd.amount) as total_amount,
            COUNT(DISTINCT r.report_date) as days_reported,
            AVG(fd.volume) as avg_daily_volume
        FROM reports r
        JOIN fuel_details fd ON r.id = fd.report_id
        WHERE r.report_date BETWEEN ? AND ?
        GROUP BY r.station_name
        ORDER BY total_volume DESC
        LIMIT ?
        """
        
        rows = conn.execute(query, (start_date_str, end_date_str, limit)).fetchall()
        result = [dict(row) for row in rows]
        
        conn.close()
        return result
        
    except Exception as e:
        logger.error(f"Database error in get_top_stations: {e}")
        return []

# ==================== UTILITY FUNCTIONS ====================

def convert_to_date_obj(date_str: str) -> Optional[date]:
    """
    Convert date string in dd/mm/yy format to date object.
    """
    try:
        day, month, year = map(int, date_str.split('/'))
        if year < 100:  # 2-digit year
            year += 2000
        return date(year, month, day)
    except Exception as e:
        logger.error(f"Error converting date string: {e}")
        return None

def parse_date_string(date_str: str) -> Optional[date]:
    """
    Parse date string from various formats to date object.
    """
    date_formats = [
        "%d/%m/%y",  # 27/12/25
        "%d/%m/%Y",  # 27/12/2025
        "%Y-%m-%d",  # 2025-12-27
        "%d-%b-%Y",  # 27-Dec-2025
        "%d-%m-%Y",  # 27-12-2025
        "%d.%m.%Y",  # 27.12.2025
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    return None

def format_date_for_db(date_obj: date) -> str:
    """
    Format date object to database format (dd/mm/yy).
    """
    return date_obj.strftime("%d/%m/%y")

def backup_database(backup_path: str = None) -> bool:
    """Create a backup of the database."""
    import shutil
    from datetime import datetime
    
    try:
        if not backup_path:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"konchat_backup_{timestamp}.db"
        
        shutil.copy2(DB_NAME, backup_path)
        logger.info(f"Database backed up to: {backup_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error backing up database: {str(e)}")
        return False

def optimize_database() -> bool:
    """Optimize database performance."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        # Run VACUUM to defragment database
        cur.execute("VACUUM")
        
        # Run ANALYZE to update statistics
        cur.execute("ANALYZE")
        
        # Rebuild indexes
        cur.execute("REINDEX")
        
        conn.commit()
        conn.close()
        
        logger.info("Database optimized successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error optimizing database: {str(e)}")
        return False

def get_database_stats() -> Dict[str, Any]:
    """Get database statistics."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        stats = {}
        
        # Table sizes
        tables = ['reports', 'fuel_details', 'pump_details']
        for table in tables:
            cur.execute(f"SELECT COUNT(*) as count FROM {table}")
            stats[f"{table}_count"] = cur.fetchone()["count"]
        
        # Date range
        cur.execute("SELECT MIN(report_date) as first_date, MAX(report_date) as last_date FROM reports")
        date_range = cur.fetchone()
        stats["first_date"] = date_range["first_date"] if date_range["first_date"] else None
        stats["last_date"] = date_range["last_date"] if date_range["last_date"] else None
        
        # Unique stations
        cur.execute("SELECT COUNT(DISTINCT station_name) as station_count FROM reports")
        stats["station_count"] = cur.fetchone()["station_count"]
        
        # Total volume and amount
        cur.execute("SELECT SUM(total_volume) as total_volume, SUM(total_amount) as total_amount FROM reports")
        totals = cur.fetchone()
        stats["total_volume"] = totals["total_volume"] if totals["total_volume"] else 0
        stats["total_amount"] = totals["total_amount"] if totals["total_amount"] else 0
        
        conn.close()
        return stats
        
    except Exception as e:
        logger.error(f"Error getting database stats: {str(e)}")
        return {}

def search_reports(search_term: str) -> List[Dict[str, Any]]:
    """
    Search reports by station name or date.
    
    Args:
        search_term: Search term (can be station name or date pattern)
    
    Returns:
        List of matching reports
    """
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        # Check if search term looks like a date
        is_date_search = re.match(r'^\d{1,2}[/\-.]\d{1,2}[/\-.]\d{2,4}$', search_term)
        
        if is_date_search:
            # Try to parse as date
            date_obj = parse_date_string(search_term)
            if date_obj:
                date_str = format_date_for_db(date_obj)
                query = """
                SELECT r.*, GROUP_CONCAT(fd.fuel_type || ': ' || fd.volume) as fuel_info
                FROM reports r
                JOIN fuel_details fd ON r.id = fd.report_id
                WHERE r.report_date = ?
                GROUP BY r.id
                ORDER BY r.report_date DESC
                """
                cur.execute(query, (date_str,))
            else:
                conn.close()
                return []
        else:
            # Search by station name
            query = """
            SELECT r.*, GROUP_CONCAT(fd.fuel_type || ': ' || fd.volume) as fuel_info
            FROM reports r
            JOIN fuel_details fd ON r.id = fd.report_id
            WHERE r.station_name LIKE ?
            GROUP BY r.id
            ORDER BY r.report_date DESC
            LIMIT 50
            """
            cur.execute(query, (f'%{search_term}%',))
        
        rows = cur.fetchall()
        result = [dict(row) for row in rows]
        
        conn.close()
        return result
        
    except Exception as e:
        logger.error(f"Error searching reports: {str(e)}")
        return []

# ==================== TEST FUNCTIONS ====================

def test_connection() -> bool:
    """Test database connection."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        # Simple query to test connection
        cur.execute("SELECT 1 as test")
        result = cur.fetchone()
        
        conn.close()
        return result["test"] == 1
        
    except Exception as e:
        logger.error(f"Database connection test failed: {str(e)}")
        return False

def create_test_data():
    """Create test data for development."""
    try:
        # This is just for testing - remove in production
        test_reports = [
            {
                "station_name": "Test Station 1",
                "report_date": "01/01/25",
                "fuel_data": [
                    {"fuel_type": "DO", "volume": 1000.0, "amount": 950.0},
                    {"fuel_type": "EA92", "volume": 2000.0, "amount": 2100.0},
                    {"fuel_type": "EA95", "volume": 1500.0, "amount": 1650.0}
                ],
                "total_volume": 4500.0,
                "total_amount": 4700.0
            },
            {
                "station_name": "Test Station 2",
                "report_date": "02/01/25",
                "fuel_data": [
                    {"fuel_type": "DO", "volume": 800.0, "amount": 760.0},
                    {"fuel_type": "EA92", "volume": 1800.0, "amount": 1890.0},
                    {"fuel_type": "EA95", "volume": 1200.0, "amount": 1320.0}
                ],
                "total_volume": 3800.0,
                "total_amount": 3970.0
            }
        ]
        
        for report in test_reports:
            save_report(report)
        
        logger.info("Test data created successfully")
        
    except Exception as e:
        logger.error(f"Error creating test data: {str(e)}")

# ==================== MAIN ====================

if __name__ == "__main__":
    # Initialize database
    init_db()
    
    # Test connection
    if test_connection():
        logger.info("Database connection test: SUCCESS")
    else:
        logger.error("Database connection test: FAILED")
    
    # Get database statistics
    stats = get_database_stats()
    logger.info(f"Database stats: {stats}")