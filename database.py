import sqlite3

DB_NAME = "konchat.db"

def get_conn():
    return sqlite3.connect(DB_NAME)

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        station_name TEXT,
        manager_name TEXT,
        report_date TEXT,
        fuel_type TEXT,
        volume REAL,
        amount REAL,
        pos_variance TEXT,
        total_volume REAL,
        total_amount REAL,
        gain_loss REAL,
        status TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()
    conn.close()
