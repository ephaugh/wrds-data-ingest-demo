"""
WRDS Data Ingestion Demo - Database Loader
Loads normalized CSV data into SQLite database
"""
import os
import sqlite3
import pandas as pd

# Configuration
INPUT_FILE = os.path.join("data", "prices_raw.csv")
DB_DIR = "db"
DB_FILE = os.path.join(DB_DIR, "marketdata.db")
TABLE_NAME = "prices_daily"


def ensure_directory_exists(directory):
    """Create directory if it doesn't exist"""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"✓ Created directory: {directory}")


def create_table(conn):
    """
    Create prices_daily table with appropriate schema and indexes
    
    Args:
        conn: SQLite database connection
    """
    cursor = conn.cursor()
    
    # Create table with composite primary key
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        date TEXT NOT NULL,
        symbol TEXT NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        adj_close REAL,
        volume INTEGER,
        PRIMARY KEY (date, symbol)
    )
    """
    
    cursor.execute(create_table_sql)
    
    # Create index for efficient querying by symbol and date
    create_index_sql = f"""
    CREATE INDEX IF NOT EXISTS idx_symbol_date 
    ON {TABLE_NAME} (symbol, date)
    """
    
    cursor.execute(create_index_sql)
    conn.commit()
    
    print(f"✓ Table '{TABLE_NAME}' ready with indexes")


def load_data_to_db(csv_file, db_file):
    """
    Load CSV data into SQLite database using upsert pattern
    
    Args:
        csv_file: Path to input CSV file
        db_file: Path to SQLite database file
    
    Returns:
        Number of rows loaded
    """
    # Read CSV
    print(f"Reading data from {csv_file}...")
    df = pd.read_csv(csv_file)
    
    if df.empty:
        raise ValueError("CSV file is empty")
    
    print(f"  → Loaded {len(df)} rows from CSV")
    
    # Connect to database
    conn = sqlite3.connect(db_file)
    
    try:
        # Create table and indexes
        create_table(conn)
        
        # Implement upsert: delete existing then insert
        print(f"Loading data into database...")
        cursor = conn.cursor()
        
        rows_loaded = 0
        
        for _, row in df.iterrows():
            # Delete existing record with same primary key
            delete_sql = f"""
            DELETE FROM {TABLE_NAME} 
            WHERE date = ? AND symbol = ?
            """
            cursor.execute(delete_sql, (row['date'], row['symbol']))
            
            # Insert new record
            insert_sql = f"""
            INSERT INTO {TABLE_NAME} 
            (date, symbol, open, high, low, close, adj_close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(insert_sql, (
                row['date'],
                row['symbol'],
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['adj_close'],
                row['volume']
            ))
            
            rows_loaded += 1
            
            # Commit in batches for performance
            if rows_loaded % 100 == 0:
                conn.commit()
        
        # Final commit
        conn.commit()
        
        return rows_loaded
        
    finally:
        conn.close()


def main():
    """Main execution function"""
    print("=" * 60)
    print("WRDS Data Ingestion Demo - Database Load")
    print("=" * 60)
    
    # Ensure database directory exists
    ensure_directory_exists(DB_DIR)
    
    # Check if input file exists
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")
    
    try:
        # Load data
        rows_loaded = load_data_to_db(INPUT_FILE, DB_FILE)
        
        print("\n" + "=" * 60)
        print(f"✓ Database load complete!")
        print(f"  Rows loaded: {rows_loaded}")
        print(f"  Database: {DB_FILE}")
        print(f"  Table: {TABLE_NAME}")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Error during database load: {str(e)}")
        raise


if __name__ == "__main__":
    main()
