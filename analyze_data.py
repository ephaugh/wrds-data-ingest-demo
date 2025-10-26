"""
WRDS Data Ingestion Demo - Analytics Engine
Performs data analysis and generates reports with visualizations
"""
import os
import sqlite3
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Headless backend for server environments
import matplotlib.pyplot as plt

# Configuration
DB_FILE = os.path.join("db", "marketdata.db")
TABLE_NAME = "prices_daily"
REPORTS_DIR = "reports"
CHARTS_DIR = os.path.join(REPORTS_DIR, "charts")
SUMMARY_FILE = os.path.join(REPORTS_DIR, "summary.csv")
VOLATILITY_FILE = os.path.join(REPORTS_DIR, "volatility.csv")
CHART_FILE = os.path.join(CHARTS_DIR, "adj_close_example.png")


def ensure_directory_exists(directory):
    """Create directory if it doesn't exist"""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"✓ Created directory: {directory}")


def load_data_from_db(db_file, table_name):
    """
    Load data from SQLite database
    
    Args:
        db_file: Path to SQLite database
        table_name: Name of table to query
    
    Returns:
        DataFrame with market data
    """
    conn = sqlite3.connect(db_file)
    
    try:
        query = f"SELECT * FROM {table_name} ORDER BY symbol, date"
        df = pd.read_sql_query(query, conn)
        
        # Convert date to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        return df
        
    finally:
        conn.close()


def calculate_daily_returns(df):
    """
    Calculate daily returns for each symbol
    
    Args:
        df: DataFrame with price data
    
    Returns:
        DataFrame with returns added
    """
    df = df.sort_values(['symbol', 'date'])
    df['daily_return'] = df.groupby('symbol')['adj_close'].pct_change()
    
    return df


def generate_summary_statistics(df):
    """
    Generate summary statistics per symbol
    
    Args:
        df: DataFrame with returns
    
    Returns:
        DataFrame with summary statistics
    """
    summary = df.groupby('symbol').agg({
        'daily_return': ['count', 'mean', 'std'],
        'volume': 'mean'
    }).reset_index()
    
    # Flatten multi-level columns
    summary.columns = ['symbol', 'obs', 'mean_daily_return', 'std_daily_return', 'avg_volume']
    
    # Remove NaN count from returns (first day has no return)
    summary['obs'] = summary['obs'] - 1
    
    return summary


def calculate_rolling_volatility(df, window=20, annualization_factor=252):
    """
    Calculate rolling volatility (annualized)
    
    Args:
        df: DataFrame with returns
        window: Rolling window size
        annualization_factor: Factor to annualize (252 trading days)
    
    Returns:
        DataFrame with latest volatility per symbol
    """
    df = df.sort_values(['symbol', 'date'])
    
    # Calculate rolling standard deviation
    df['rolling_vol'] = df.groupby('symbol')['daily_return'].transform(
        lambda x: x.rolling(window=window).std()
    )
    
    # Annualize volatility
    df['ann_vol_20d'] = df['rolling_vol'] * np.sqrt(annualization_factor)
    
    # Get latest volatility for each symbol
    latest_vol = df.groupby('symbol').agg({
        'ann_vol_20d': 'last'
    }).reset_index()
    
    return latest_vol


def create_price_chart(df, output_file):
    """
    Create a simple line chart of adjusted close prices
    
    Args:
        df: DataFrame with price data
        output_file: Path to save chart
    """
    # Select first symbol alphabetically
    symbols = sorted(df['symbol'].unique())
    selected_symbol = symbols[0]
    
    # Filter data for selected symbol
    symbol_df = df[df['symbol'] == selected_symbol].sort_values('date')
    
    # Create figure
    plt.figure(figsize=(12, 6))
    plt.plot(symbol_df['date'], symbol_df['adj_close'], linewidth=2, color='#2E86AB')
    
    plt.title(f'{selected_symbol} - Adjusted Close Price', fontsize=16, fontweight='bold')
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Adjusted Close ($)', fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    # Save figure
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"✓ Chart created for {selected_symbol}: {output_file}")


def main():
    """Main execution function"""
    print("=" * 60)
    print("WRDS Data Ingestion Demo - Analytics")
    print("=" * 60)
    
    # Ensure output directories exist
    ensure_directory_exists(REPORTS_DIR)
    ensure_directory_exists(CHARTS_DIR)
    
    # Check if database exists
    if not os.path.exists(DB_FILE):
        raise FileNotFoundError(f"Database not found: {DB_FILE}")
    
    try:
        # Load data
        print(f"Loading data from {DB_FILE}...")
        df = load_data_from_db(DB_FILE, TABLE_NAME)
        print(f"  → Loaded {len(df)} rows for {df['symbol'].nunique()} symbols")
        
        # Calculate returns
        print("Calculating daily returns...")
        df = calculate_daily_returns(df)
        
        # Generate summary statistics
        print("Generating summary statistics...")
        summary = generate_summary_statistics(df)
        summary.to_csv(SUMMARY_FILE, index=False)
        print(f"  → Saved: {SUMMARY_FILE}")
        
        # Calculate rolling volatility
        print("Calculating 20-day rolling volatility...")
        volatility = calculate_rolling_volatility(df)
        volatility.to_csv(VOLATILITY_FILE, index=False)
        print(f"  → Saved: {VOLATILITY_FILE}")
        
        # Create price chart
        print("Creating price chart...")
        create_price_chart(df, CHART_FILE)
        
        print("\n" + "=" * 60)
        print("✓ Analytics complete!")
        print(f"  Summary statistics: {SUMMARY_FILE}")
        print(f"  Volatility metrics: {VOLATILITY_FILE}")
        print(f"  Example chart: {CHART_FILE}")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Error during analysis: {str(e)}")
        raise


if __name__ == "__main__":
    main()
