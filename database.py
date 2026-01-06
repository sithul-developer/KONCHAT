import os
import mysql.connector
from mysql.connector import Error, pooling
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Tuple, Union
from dotenv import load_dotenv
import contextlib

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('MYSQLHOST', 'localhost'),
    'port': int(os.getenv('MYSQLPORT', 3306)),
    'user': os.getenv('MYSQLUSER', 'root'),
    'password': os.getenv('MYSQLPASSWORD', ''),
    'database': os.getenv('MYSQLDATABASE', 'telegram_bot_db'),
    'pool_name': 'telegram_bot_pool',
    'pool_size': 10,
    'pool_reset_session': True,
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci',
    'use_unicode': True,
    'autocommit': False,
    'buffered': True
}

# Create connection pool
connection_pool = None

def get_connection():
    """Get a connection from the pool."""
    global connection_pool
    
    if connection_pool is None:
        try:
            # First, test connection without database
            test_config = DB_CONFIG.copy()
            test_config.pop('database', None)
            
            test_conn = mysql.connector.connect(**test_config)
            cursor = test_conn.cursor()
            
            # Ensure database exists with correct collation
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            cursor.execute(f"ALTER DATABASE {DB_CONFIG['database']} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            
            cursor.close()
            test_conn.close()
            
            # Now create pool with database
            connection_pool = mysql.connector.pooling.MySQLConnectionPool(**DB_CONFIG)
            logger.info("✅ Database connection pool created")
            
        except Error as e:
            logger.error(f"❌ Error creating connection pool: {e}")
            raise e
    
    try:
        conn = connection_pool.get_connection()
        conn.set_charset_collation('utf8mb4', 'utf8mb4_unicode_ci')
        return conn
    except Error as e:
        logger.error(f"❌ Error getting connection from pool: {e}")
        raise e

@contextlib.contextmanager
def db_cursor(dictionary=True):
    """Context manager for database cursor."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=dictionary, buffered=True)
        yield cursor
        conn.commit()
    except Error as e:
        if conn:
            conn.rollback()
        logger.error(f"❌ Database error: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

def parse_date_string(date_str: str, output_format: str = '%Y-%m-%d') -> Union[date, str]:
    """Parse date string from various formats to date object or formatted string."""
    if not date_str or date_str.strip() == '':
        raise ValueError("Empty date string")
    
    # List of possible date formats to try
    date_formats = [
        '%Y/%m/%d',  # yyyy/mm/dd
        '%d/%m/%Y',  # dd/mm/yyyy
        '%d/%m/%y',  # dd/mm/yy
        '%Y-%m-%d',  # yyyy-mm-dd
        '%d-%m-%Y',  # dd-mm-yyyy
        '%d-%m-%y',  # dd-mm-yy
        '%d.%m.%Y',  # dd.mm.yyyy
        '%d.%m.%y',  # dd.mm.yy
        '%Y.%m.%d',  # yyyy.mm.dd
        '%m/%d/%Y',  # mm/dd/yyyy (US format)
        '%m-%d-%Y',  # mm-dd-yyyy (US format)
    ]
    
    # Try each format
    for fmt in date_formats:
        try:
            date_obj = datetime.strptime(date_str.strip(), fmt).date()
            if output_format:
                return date_obj.strftime(output_format)
            return date_obj
        except ValueError:
            continue
    
    # Try to extract numbers from string
    try:
        import re
        numbers = re.findall(r'\d+', date_str)
        if len(numbers) >= 3:
            # Try different orderings
            for order in [(2, 1, 0), (0, 1, 2), (2, 0, 1)]:  # yyyy,mm,dd | dd,mm,yyyy | yyyy,dd,mm
                try:
                    y, m, d = int(numbers[order[0]]), int(numbers[order[1]]), int(numbers[order[2]])
                    
                    # Normalize year
                    if y < 100:  # 2-digit year
                        y += 2000
                    elif y < 1000:  # 3-digit year
                        y += 1900
                    
                    # Validate month and day
                    if 1 <= m <= 12 and 1 <= d <= 31:
                        date_obj = date(y, m, d)
                        if output_format:
                            return date_obj.strftime(output_format)
                        return date_obj
                except (ValueError, TypeError):
                    continue
    except:
        pass
    
    raise ValueError(f"Invalid date format: {date_str}")

def init_db():
    """Initialize database with required tables."""
    try:
        with db_cursor(dictionary=False) as cursor:
            
            # Drop existing procedures and triggers first
            try:
                cursor.execute("DROP PROCEDURE IF EXISTS update_daily_summary")
                cursor.execute("DROP TRIGGER IF EXISTS update_monthly_summary_after_daily")
            except:
                pass
            
            # Create stations table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stations (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL UNIQUE,
                    location VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_station_name (name)
                ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            ''')
            
            # Create fuel_reports table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS fuel_reports (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    station_id INT NOT NULL,
                    report_date DATE NOT NULL,
                    fuel_type VARCHAR(100) NOT NULL,
                    volume DECIMAL(10, 2) NOT NULL,
                    amount DECIMAL(12, 2) NOT NULL,
                    pump_count INT DEFAULT 1,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (station_id) REFERENCES stations(id) ON DELETE CASCADE,
                    UNIQUE KEY unique_report (station_id, report_date, fuel_type),
                    INDEX idx_report_date (report_date),
                    INDEX idx_station_date (station_id, report_date),
                    INDEX idx_fuel_type (fuel_type)
                ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            ''')
            
            # Create daily_summary table for faster queries
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS daily_summary (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    station_id INT NOT NULL,
                    report_date DATE NOT NULL,
                    diesel_volume DECIMAL(10, 2) DEFAULT 0,
                    diesel_amount DECIMAL(12, 2) DEFAULT 0,
                    regular_volume DECIMAL(10, 2) DEFAULT 0,
                    regular_amount DECIMAL(12, 2) DEFAULT 0,
                    super_volume DECIMAL(10, 2) DEFAULT 0,
                    super_amount DECIMAL(12, 2) DEFAULT 0,
                    total_volume DECIMAL(10, 2) DEFAULT 0,
                    total_amount DECIMAL(12, 2) DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (station_id) REFERENCES stations(id) ON DELETE CASCADE,
                    UNIQUE KEY unique_daily_summary (station_id, report_date),
                    INDEX idx_summary_date (report_date),
                    INDEX idx_summary_station_date (station_id, report_date)
                ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            ''')
            
            # Create monthly_summary table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS monthly_summary (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    station_id INT NOT NULL,
                    year INT NOT NULL,
                    month INT NOT NULL,
                    diesel_volume DECIMAL(12, 2) DEFAULT 0,
                    diesel_amount DECIMAL(14, 2) DEFAULT 0,
                    regular_volume DECIMAL(12, 2) DEFAULT 0,
                    regular_amount DECIMAL(14, 2) DEFAULT 0,
                    super_volume DECIMAL(12, 2) DEFAULT 0,
                    super_amount DECIMAL(14, 2) DEFAULT 0,
                    total_volume DECIMAL(12, 2) DEFAULT 0,
                    total_amount DECIMAL(14, 2) DEFAULT 0,
                    days_with_data INT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (station_id) REFERENCES stations(id) ON DELETE CASCADE,
                    UNIQUE KEY unique_monthly_summary (station_id, year, month),
                    INDEX idx_monthly_year_month (year, month),
                    INDEX idx_monthly_station (station_id, year, month)
                ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            ''')
            
            logger.info("✅ Database tables created/verified")
            
            # Create optimized stored procedure
            cursor.execute('''
                CREATE PROCEDURE update_daily_summary(
                    IN p_station_name VARCHAR(255),
                    IN p_report_date DATE,
                    IN p_fuel_type VARCHAR(100),
                    IN p_volume DECIMAL(10,2),
                    IN p_amount DECIMAL(12,2)
                )
                BEGIN
                    DECLARE v_station_id INT;
                    DECLARE fuel_category VARCHAR(20);
                    
                    -- Determine fuel category
                    SET fuel_category = CASE 
                        WHEN LOWER(p_fuel_type) LIKE '%diesel%' OR p_fuel_type LIKE '%ម៉ាស៊ូត%' THEN 'DIESEL'
                        WHEN LOWER(p_fuel_type) LIKE '%regular%' OR p_fuel_type LIKE '%សាំង%' THEN 'REGULAR'
                        WHEN LOWER(p_fuel_type) LIKE '%super%' OR p_fuel_type LIKE '%ស៊ុប%' THEN 'SUPER'
                        ELSE 'OTHER'
                    END;
                    
                    -- Get or create station
                    SELECT id INTO v_station_id 
                    FROM stations 
                    WHERE name = p_station_name COLLATE utf8mb4_unicode_ci;
                    
                    IF v_station_id IS NULL THEN
                        INSERT INTO stations (name) VALUES (p_station_name);
                        SET v_station_id = LAST_INSERT_ID();
                    END IF;
                    
                    -- Insert or update fuel report
                    INSERT INTO fuel_reports (station_id, report_date, fuel_type, volume, amount)
                    VALUES (v_station_id, p_report_date, p_fuel_type, p_volume, p_amount)
                    ON DUPLICATE KEY UPDATE
                        volume = VALUES(volume),
                        amount = VALUES(amount),
                        updated_at = CURRENT_TIMESTAMP;
                    
                    -- Update daily summary
                    INSERT INTO daily_summary (station_id, report_date,
                        diesel_volume, diesel_amount,
                        regular_volume, regular_amount,
                        super_volume, super_amount,
                        total_volume, total_amount)
                    VALUES (
                        v_station_id, p_report_date,
                        CASE WHEN fuel_category = 'DIESEL' THEN p_volume ELSE 0 END,
                        CASE WHEN fuel_category = 'DIESEL' THEN p_amount ELSE 0 END,
                        CASE WHEN fuel_category = 'REGULAR' THEN p_volume ELSE 0 END,
                        CASE WHEN fuel_category = 'REGULAR' THEN p_amount ELSE 0 END,
                        CASE WHEN fuel_category = 'SUPER' THEN p_volume ELSE 0 END,
                        CASE WHEN fuel_category = 'SUPER' THEN p_amount ELSE 0 END,
                        p_volume, p_amount)
                    ON DUPLICATE KEY UPDATE
                        diesel_volume = diesel_volume + CASE WHEN fuel_category = 'DIESEL' THEN p_volume ELSE 0 END,
                        diesel_amount = diesel_amount + CASE WHEN fuel_category = 'DIESEL' THEN p_amount ELSE 0 END,
                        regular_volume = regular_volume + CASE WHEN fuel_category = 'REGULAR' THEN p_volume ELSE 0 END,
                        regular_amount = regular_amount + CASE WHEN fuel_category = 'REGULAR' THEN p_amount ELSE 0 END,
                        super_volume = super_volume + CASE WHEN fuel_category = 'SUPER' THEN p_volume ELSE 0 END,
                        super_amount = super_amount + CASE WHEN fuel_category = 'SUPER' THEN p_amount ELSE 0 END,
                        total_volume = total_volume + p_volume,
                        total_amount = total_amount + p_amount,
                        updated_at = CURRENT_TIMESTAMP;
                END
            ''')
            
            # Create trigger to update monthly summary
            cursor.execute('''
                CREATE TRIGGER update_monthly_summary_after_daily
                AFTER INSERT ON daily_summary
                FOR EACH ROW
                BEGIN
                    DECLARE v_year INT;
                    DECLARE v_month INT;
                    DECLARE v_days_with_data INT;
                    
                    SET v_year = YEAR(NEW.report_date);
                    SET v_month = MONTH(NEW.report_date);
                    
                    -- Get count of days with data for this month
                    SELECT COUNT(DISTINCT report_date) INTO v_days_with_data
                    FROM daily_summary 
                    WHERE station_id = NEW.station_id 
                    AND YEAR(report_date) = v_year 
                    AND MONTH(report_date) = v_month;
                    
                    INSERT INTO monthly_summary (station_id, year, month, 
                        diesel_volume, diesel_amount,
                        regular_volume, regular_amount,
                        super_volume, super_amount,
                        total_volume, total_amount,
                        days_with_data)
                    SELECT 
                        NEW.station_id,
                        v_year,
                        v_month,
                        COALESCE(SUM(diesel_volume), 0),
                        COALESCE(SUM(diesel_amount), 0),
                        COALESCE(SUM(regular_volume), 0),
                        COALESCE(SUM(regular_amount), 0),
                        COALESCE(SUM(super_volume), 0),
                        COALESCE(SUM(super_amount), 0),
                        COALESCE(SUM(total_volume), 0),
                        COALESCE(SUM(total_amount), 0),
                        v_days_with_data
                    FROM daily_summary
                    WHERE station_id = NEW.station_id
                    AND YEAR(report_date) = v_year
                    AND MONTH(report_date) = v_month
                    ON DUPLICATE KEY UPDATE
                        diesel_volume = VALUES(diesel_volume),
                        diesel_amount = VALUES(diesel_amount),
                        regular_volume = VALUES(regular_volume),
                        regular_amount = VALUES(regular_amount),
                        super_volume = VALUES(super_volume),
                        super_amount = VALUES(super_amount),
                        total_volume = VALUES(total_volume),
                        total_amount = VALUES(total_amount),
                        days_with_data = VALUES(days_with_data),
                        updated_at = CURRENT_TIMESTAMP;
                END
            ''')
            
            logger.info("✅ Stored procedures and triggers created")
            
        return True
        
    except Error as e:
        logger.error(f"❌ Error initializing database: {e}")
        return False

def save_report(data: Dict[str, Any]) -> bool:
    """Save a parsed report to database."""
    try:
        station_name = data.get('station_name', '').strip()
        report_date_str = data.get('report_date', '')
        fuel_data = data.get('fuel_data', [])
        
        if not station_name or not report_date_str or not fuel_data:
            logger.error("❌ Missing required data for report")
            return False
        
        # Parse date to proper format
        try:
            report_date = parse_date_string(report_date_str, output_format='%Y-%m-%d')
        except ValueError as e:
            logger.error(f"❌ Error parsing date {report_date_str}: {e}")
            return False
        
        # Process all fuel data in a single transaction
        with db_cursor(dictionary=False) as cursor:
            # Get or create station
            cursor.execute('SELECT id FROM stations WHERE name = %s', (station_name,))
            result = cursor.fetchone()
            
            if result:
                station_id = result[0]
            else:
                cursor.execute('INSERT INTO stations (name) VALUES (%s)', (station_name,))
                station_id = cursor.lastrowid
            
            # Process each fuel type
            for fuel in fuel_data:
                fuel_type = fuel.get('fuel_type', '').strip()
                volume = float(fuel.get('volume', 0))
                amount = float(fuel.get('amount', 0))
                
                if not fuel_type or volume <= 0:
                    continue
                
                # Normalize fuel type
                fuel_lower = fuel_type.lower()
                if 'diesel' in fuel_lower or 'ម៉ាស៊ូត' in fuel_type:
                    fuel_category = 'DIESEL'
                elif 'regular' in fuel_lower or 'សាំង' in fuel_type:
                    fuel_category = 'REGULAR'
                elif 'super' in fuel_lower or 'ស៊ុប' in fuel_type:
                    fuel_category = 'SUPER'
                else:
                    fuel_category = 'OTHER'
                
                # Insert fuel report
                cursor.execute('''
                    INSERT INTO fuel_reports (station_id, report_date, fuel_type, volume, amount)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        volume = VALUES(volume),
                        amount = VALUES(amount),
                        updated_at = CURRENT_TIMESTAMP
                ''', (station_id, report_date, fuel_type, volume, amount))
                
                # Update daily summary
                if fuel_category in ['DIESEL', 'REGULAR', 'SUPER']:
                    update_query = f'''
                        INSERT INTO daily_summary (station_id, report_date,
                            {fuel_category.lower()}_volume, {fuel_category.lower()}_amount,
                            total_volume, total_amount)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            {fuel_category.lower()}_volume = {fuel_category.lower()}_volume + VALUES({fuel_category.lower()}_volume),
                            {fuel_category.lower()}_amount = {fuel_category.lower()}_amount + VALUES({fuel_category.lower()}_amount),
                            total_volume = total_volume + VALUES(total_volume),
                            total_amount = total_amount + VALUES(total_amount),
                            updated_at = CURRENT_TIMESTAMP
                    '''
                    cursor.execute(update_query, (station_id, report_date, volume, amount, volume, amount))
                else:
                    update_query = '''
                        INSERT INTO daily_summary (station_id, report_date, total_volume, total_amount)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            total_volume = total_volume + VALUES(total_volume),
                            total_amount = total_amount + VALUES(total_amount),
                            updated_at = CURRENT_TIMESTAMP
                    '''
                    cursor.execute(update_query, (station_id, report_date, volume, amount))
        
        logger.info(f"✅ Report saved: {station_name} - {report_date}")
        return True
        
    except Error as e:
        logger.error(f"❌ Database error saving report: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error saving report: {e}")
        return False

def report_exists(station_name: str, report_date: str) -> bool:
    """Check if a report already exists."""
    try:
        # Parse date
        try:
            report_date_formatted = parse_date_string(report_date, output_format='%Y-%m-%d')
        except ValueError:
            return False
        
        with db_cursor(dictionary=True) as cursor:
            query = '''
                SELECT COUNT(*) as count
                FROM daily_summary ds
                JOIN stations s ON ds.station_id = s.id
                WHERE s.name = %s AND ds.report_date = %s
            '''
            
            cursor.execute(query, (station_name, report_date_formatted))
            result = cursor.fetchone()
            
            return result['count'] > 0 if result else False
            
    except Error as e:
        logger.error(f"❌ Error checking report existence: {e}")
        return False

def get_summary(start_date: str = None, end_date: str = None, station: str = None) -> List[Dict]:
    """Get summary data for date range."""
    try:
        # Parse dates
        start_date_formatted = None
        end_date_formatted = None
        
        if start_date:
            try:
                start_date_formatted = parse_date_string(start_date, output_format='%Y-%m-%d')
            except ValueError:
                logger.warning(f"Invalid start date format: {start_date}")
        
        if end_date:
            try:
                end_date_formatted = parse_date_string(end_date, output_format='%Y-%m-%d')
            except ValueError:
                logger.warning(f"Invalid end date format: {end_date}")
        
        with db_cursor(dictionary=True) as cursor:
            query = '''
                SELECT 
                    s.name as station_name,
                    ds.report_date,
                    ds.diesel_volume,
                    ds.diesel_amount,
                    ds.regular_volume,
                    ds.regular_amount,
                    ds.super_volume,
                    ds.super_amount,
                    ds.total_volume,
                    ds.total_amount
                FROM daily_summary ds
                JOIN stations s ON ds.station_id = s.id
                WHERE 1=1
            '''
            
            params = []
            
            if start_date_formatted and end_date_formatted:
                query += ' AND ds.report_date BETWEEN %s AND %s'
                params.extend([start_date_formatted, end_date_formatted])
            
            if station:
                query += ' AND s.name = %s'
                params.append(station)
            
            query += ' ORDER BY ds.report_date DESC, s.name'
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            # Format results
            formatted_results = []
            for row in results:
                # Format report_date to yyyy/mm/dd
                report_date_obj = row['report_date']
                if isinstance(report_date_obj, date):
                    report_date_str = report_date_obj.strftime('%Y/%m/%d')
                else:
                    report_date_str = str(row['report_date'])
                
                # Diesel
                if row['diesel_volume'] > 0:
                    formatted_results.append({
                        'station_name': row['station_name'],
                        'report_date': report_date_str,
                        'fuel_type': 'Diesel',
                        'volume': float(row['diesel_volume']),
                        'amount': float(row['diesel_amount'])
                    })
                
                # Regular
                if row['regular_volume'] > 0:
                    formatted_results.append({
                        'station_name': row['station_name'],
                        'report_date': report_date_str,
                        'fuel_type': 'Regular',
                        'volume': float(row['regular_volume']),
                        'amount': float(row['regular_amount'])
                    })
                
                # Super
                if row['super_volume'] > 0:
                    formatted_results.append({
                        'station_name': row['station_name'],
                        'report_date': report_date_str,
                        'fuel_type': 'Super',
                        'volume': float(row['super_volume']),
                        'amount': float(row['super_amount'])
                    })
            
            return formatted_results
            
    except Error as e:
        logger.error(f"❌ Error getting summary: {e}")
        return []

def get_all_stations() -> List[str]:
    """Get all unique station names."""
    try:
        with db_cursor(dictionary=False) as cursor:
            cursor.execute("SELECT DISTINCT name FROM stations ORDER BY name")
            stations = [row[0] for row in cursor.fetchall()]
            return stations
    except Error as e:
        logger.error(f"❌ Error getting stations: {e}")
        return []

def get_station_statistics(station: str = None) -> Dict[str, Any]:
    """Get statistics for a station or all stations."""
    try:
        with db_cursor(dictionary=True) as cursor:
            if station:
                query = '''
                    SELECT 
                        s.name,
                        COUNT(DISTINCT ds.report_date) as report_count,
                        MIN(ds.report_date) as first_report,
                        MAX(ds.report_date) as last_report,
                        SUM(ds.total_volume) as total_volume,
                        SUM(ds.total_amount) as total_amount,
                        AVG(ds.total_volume) as avg_daily_volume,
                        AVG(ds.total_amount) as avg_daily_amount
                    FROM daily_summary ds
                    JOIN stations s ON ds.station_id = s.id
                    WHERE s.name = %s
                    GROUP BY s.id
                '''
                cursor.execute(query, (station,))
            else:
                query = '''
                    SELECT 
                        COUNT(DISTINCT s.id) as station_count,
                        COUNT(DISTINCT ds.report_date) as total_days,
                        MIN(ds.report_date) as first_report,
                        MAX(ds.report_date) as last_report,
                        SUM(ds.total_volume) as total_volume,
                        SUM(ds.total_amount) as total_amount,
                        AVG(ds.total_volume) as avg_daily_volume,
                        AVG(ds.total_amount) as avg_daily_amount
                    FROM daily_summary ds
                    JOIN stations s ON ds.station_id = s.id
                '''
                cursor.execute(query)
            
            result = cursor.fetchone()
            if result:
                # Format dates
                if 'first_report' in result and result['first_report']:
                    result['first_report'] = result['first_report'].strftime('%Y/%m/%d')
                if 'last_report' in result and result['last_report']:
                    result['last_report'] = result['last_report'].strftime('%Y/%m/%d')
            
            return dict(result) if result else {}
            
    except Error as e:
        logger.error(f"❌ Error getting statistics: {e}")
        return {}

def get_monthly_details(year: int, month: int, station: str = None) -> List[Dict]:
    """Get detailed monthly data."""
    try:
        with db_cursor(dictionary=True) as cursor:
            query = '''
                SELECT 
                    s.name as station_name,
                    ds.report_date,
                    'Diesel' as fuel_type,
                    ds.diesel_volume as total_volume,
                    ds.diesel_amount as total_amount
                FROM daily_summary ds
                JOIN stations s ON ds.station_id = s.id
                WHERE YEAR(ds.report_date) = %s AND MONTH(ds.report_date) = %s
                AND ds.diesel_volume > 0
                
                UNION ALL
                
                SELECT 
                    s.name as station_name,
                    ds.report_date,
                    'Regular' as fuel_type,
                    ds.regular_volume as total_volume,
                    ds.regular_amount as total_amount
                FROM daily_summary ds
                JOIN stations s ON ds.station_id = s.id
                WHERE YEAR(ds.report_date) = %s AND MONTH(ds.report_date) = %s
                AND ds.regular_volume > 0
                
                UNION ALL
                
                SELECT 
                    s.name as station_name,
                    ds.report_date,
                    'Super' as fuel_type,
                    ds.super_volume as total_volume,
                    ds.super_amount as total_amount
                FROM daily_summary ds
                JOIN stations s ON ds.station_id = s.id
                WHERE YEAR(ds.report_date) = %s AND MONTH(ds.report_date) = %s
                AND ds.super_volume > 0
            '''
            
            params = [year, month, year, month, year, month]
            
            if station:
                query += ' AND s.name = %s'
                params.append(station)
            
            query += ' ORDER BY report_date, station_name, fuel_type'
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            # Format dates to yyyy/mm/dd
            for row in results:
                if 'report_date' in row and row['report_date']:
                    if isinstance(row['report_date'], date):
                        row['report_date'] = row['report_date'].strftime('%Y/%m/%d')
            
            return results
            
    except Error as e:
        logger.error(f"❌ Error getting monthly details: {e}")
        return []

def get_monthly_aggregate(year: int, month: int, station: str = None) -> Dict[str, Any]:
    """Get monthly aggregated data."""
    try:
        with db_cursor(dictionary=True) as cursor:
            query = '''
                SELECT 
                    COUNT(DISTINCT ds.report_date) as days_with_data,
                    COUNT(DISTINCT s.id) as station_count,
                    SUM(ds.diesel_volume + ds.regular_volume + ds.super_volume) as total_volume,
                    SUM(ds.diesel_amount + ds.regular_amount + ds.super_amount) as total_amount,
                    SUM(ds.diesel_volume) as diesel_volume,
                    SUM(ds.diesel_amount) as diesel_amount,
                    SUM(ds.regular_volume) as regular_volume,
                    SUM(ds.regular_amount) as regular_amount,
                    SUM(ds.super_volume) as super_volume,
                    SUM(ds.super_amount) as super_amount,
                    AVG(ds.diesel_volume + ds.regular_volume + ds.super_volume) as avg_daily_volume,
                    AVG(ds.diesel_amount + ds.regular_amount + ds.super_amount) as avg_daily_amount
                FROM daily_summary ds
                JOIN stations s ON ds.station_id = s.id
                WHERE YEAR(ds.report_date) = %s AND MONTH(ds.report_date) = %s
            '''
            
            params = [year, month]
            
            if station:
                query += ' AND s.name = %s'
                params.append(station)
            
            cursor.execute(query, params)
            result = cursor.fetchone()
            
            if result:
                # Add record count
                details = get_monthly_details(year, month, station)
                result['record_count'] = len(details)
                
                # Calculate percentages
                total_vol = result['total_volume'] or 0
                if total_vol > 0:
                    result['diesel_percentage'] = (result['diesel_volume'] or 0) / total_vol * 100
                    result['regular_percentage'] = (result['regular_volume'] or 0) / total_vol * 100
                    result['super_percentage'] = (result['super_volume'] or 0) / total_vol * 100
            
            return result or {}
            
    except Error as e:
        logger.error(f"❌ Error getting monthly aggregate: {e}")
        return {}

def get_monthly_station_summary(year: int, month: int, station: str) -> List[Dict]:
    """Get monthly summary for specific station."""
    try:
        with db_cursor(dictionary=True) as cursor:
            query = '''
                SELECT 
                    ds.*,
                    ds.report_date as report_date_fmt,
                    (ds.diesel_volume + ds.regular_volume + ds.super_volume) as daily_total_volume,
                    (ds.diesel_amount + ds.regular_amount + ds.super_amount) as daily_total_amount
                FROM daily_summary ds
                JOIN stations s ON ds.station_id = s.id
                WHERE s.name = %s 
                AND YEAR(ds.report_date) = %s 
                AND MONTH(ds.report_date) = %s
                ORDER BY ds.report_date
            '''
            
            cursor.execute(query, (station, year, month))
            results = cursor.fetchall()
            
            # Format dates
            for row in results:
                if 'report_date_fmt' in row and row['report_date_fmt']:
                    if isinstance(row['report_date_fmt'], date):
                        row['report_date_fmt'] = row['report_date_fmt'].strftime('%Y/%m/%d')
                if 'report_date' in row and row['report_date']:
                    if isinstance(row['report_date'], date):
                        row['report_date'] = row['report_date'].strftime('%Y/%m/%d')
            
            return results
            
    except Error as e:
        logger.error(f"❌ Error getting monthly station summary: {e}")
        return []

def get_date_range_summary(start_date: str, end_date: str, station: str = None) -> List[Dict]:
    """Get summary for specific date range."""
    try:
        # Parse dates
        try:
            start_date_formatted = parse_date_string(start_date, output_format='%Y-%m-%d')
            end_date_formatted = parse_date_string(end_date, output_format='%Y-%m-%d')
        except ValueError as e:
            logger.error(f"❌ Error parsing dates: {e}")
            return []
        
        with db_cursor(dictionary=True) as cursor:
            query = '''
                SELECT 
                    s.name as station_name,
                    ds.report_date,
                    ds.diesel_volume,
                    ds.diesel_amount,
                    ds.regular_volume,
                    ds.regular_amount,
                    ds.super_volume,
                    ds.super_amount,
                    ds.total_volume,
                    ds.total_amount
                FROM daily_summary ds
                JOIN stations s ON ds.station_id = s.id
                WHERE ds.report_date BETWEEN %s AND %s
            '''
            
            params = [start_date_formatted, end_date_formatted]
            
            if station:
                query += ' AND s.name = %s'
                params.append(station)
            
            query += ' ORDER BY ds.report_date, s.name'
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            # Format for compatibility
            formatted = []
            for row in results:
                # Format date
                report_date_obj = row['report_date']
                if isinstance(report_date_obj, date):
                    report_date_str = report_date_obj.strftime('%Y/%m/%d')
                else:
                    report_date_str = str(row['report_date'])
                
                if row['diesel_volume'] > 0:
                    formatted.append({
                        'station_name': row['station_name'],
                        'report_date': report_date_str,
                        'fuel_type': 'Diesel',
                        'volume': float(row['diesel_volume']),
                        'amount': float(row['diesel_amount'])
                    })
                
                if row['regular_volume'] > 0:
                    formatted.append({
                        'station_name': row['station_name'],
                        'report_date': report_date_str,
                        'fuel_type': 'Regular',
                        'volume': float(row['regular_volume']),
                        'amount': float(row['regular_amount'])
                    })
                
                if row['super_volume'] > 0:
                    formatted.append({
                        'station_name': row['station_name'],
                        'report_date': report_date_str,
                        'fuel_type': 'Super',
                        'volume': float(row['super_volume']),
                        'amount': float(row['super_amount'])
                    })
            
            return formatted
            
    except Error as e:
        logger.error(f"❌ Error getting date range summary: {e}")
        return []

def get_reports_by_date(report_date: str) -> List[Dict]:
    """Get all reports for a specific date."""
    try:
        return get_date_range_summary(report_date, report_date)
    except Exception as e:
        logger.error(f"❌ Error getting reports by date: {e}")
        return []

def get_top_stations(limit: int = 5, days: int = 30) -> List[Dict]:
    """Get top performing stations."""
    try:
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        
        with db_cursor(dictionary=True) as cursor:
            query = '''
                SELECT 
                    s.name as station_name,
                    COUNT(DISTINCT ds.report_date) as days_with_data,
                    SUM(ds.total_volume) as total_volume,
                    SUM(ds.total_amount) as total_amount,
                    AVG(ds.total_volume) as avg_daily_volume
                FROM daily_summary ds
                JOIN stations s ON ds.station_id = s.id
                WHERE ds.report_date BETWEEN %s AND %s
                GROUP BY s.id
                ORDER BY total_volume DESC
                LIMIT %s
            '''
            
            cursor.execute(query, (start_date, end_date, limit))
            return cursor.fetchall()
            
    except Error as e:
        logger.error(f"❌ Error getting top stations: {e}")
        return []

def cleanup_old_data(days_to_keep: int = 365):
    """Clean up old data to keep database size manageable."""
    try:
        cutoff_date = date.today() - timedelta(days=days_to_keep)
        
        with db_cursor(dictionary=False) as cursor:
            # Delete from daily_summary (cascade will handle related records)
            cursor.execute('''
                DELETE FROM daily_summary 
                WHERE report_date < %s
            ''', (cutoff_date,))
            
            deleted_count = cursor.rowcount
            
            # Delete stations with no reports
            cursor.execute('''
                DELETE s FROM stations s
                LEFT JOIN daily_summary ds ON s.id = ds.station_id
                WHERE ds.id IS NULL
            ''')
            
        logger.info(f"✅ Cleaned up {deleted_count} old records")
        return True
        
    except Error as e:
        logger.error(f"❌ Error cleaning up old data: {e}")
        return False

def test_connection() -> bool:
    """Test database connection."""
    try:
        with db_cursor(dictionary=False) as cursor:
            cursor.execute("SELECT 1")
        logger.info("✅ Database connection test passed")
        return True
    except Error as e:
        logger.error(f"❌ Database connection test failed: {e}")
        return False

def get_database_info() -> Dict[str, Any]:
    """Get database information and statistics."""
    try:
        with db_cursor(dictionary=True) as cursor:
            info = {}
            
            # Table counts
            cursor.execute("SELECT COUNT(*) as count FROM stations")
            info['station_count'] = cursor.fetchone()['count']
            
            cursor.execute("SELECT COUNT(*) as count FROM daily_summary")
            info['daily_summary_count'] = cursor.fetchone()['count']
            
            cursor.execute("SELECT COUNT(*) as count FROM monthly_summary")
            info['monthly_summary_count'] = cursor.fetchone()['count']
            
            cursor.execute("SELECT COUNT(*) as count FROM fuel_reports")
            info['fuel_report_count'] = cursor.fetchone()['count']
            
            # Date range
            cursor.execute("SELECT MIN(report_date) as first_date, MAX(report_date) as last_date FROM daily_summary")
            date_range = cursor.fetchone()
            info['first_date'] = date_range['first_date'].strftime('%Y/%m/%d') if date_range['first_date'] else None
            info['last_date'] = date_range['last_date'].strftime('%Y/%m/%d') if date_range['last_date'] else None
            
            return info
            
    except Error as e:
        logger.error(f"❌ Error getting database info: {e}")
        return {}

def backup_database(backup_path: str = None):
    """Create a database backup."""
    try:
        import subprocess
        import gzip
        
        if not backup_path:
            backup_path = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql.gz"
        
        # Get database credentials
        config = DB_CONFIG.copy()
        
        # Create mysqldump command
        cmd = [
            'mysqldump',
            '--host', config['host'],
            '--port', str(config['port']),
            '--user', config['user'],
            '--password=' + config['password'],
            '--databases', config['database'],
            '--single-transaction',
            '--quick',
            '--compress',
            '--skip-comments',
            '--skip-dump-date'
        ]
        
        # Execute backup
        with gzip.open(backup_path, 'wb') as f:
            subprocess.run(cmd, stdout=f, check=True)
        
        logger.info(f"✅ Database backup created: {backup_path}")
        return backup_path
        
    except Exception as e:
        logger.error(f"❌ Error creating database backup: {e}")
        return None

# Initialize database connection pool on import
try:
    init_db()
    logger.info("✅ Database initialized")
except Exception as e:
    logger.error(f"❌ Failed to initialize database: {e}")