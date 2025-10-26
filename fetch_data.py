"""
WRDS Data Ingestion Demo - Data Fetcher
Extracts daily OHLCV market data using yfinance API
"""
import os
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf

# Configuration
DEFAULT_TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "JPM", "V", "PG", "XOM", "NVDA"]
LOOKBACK_DAYS = 365
OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "prices_raw.csv")


def ensure_directory_exists(directory):
    """Create directory if it doesn't exist"""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"✓ Created directory: {directory}")


def fetch_market_data(tickers, start_date, end_date):
    """
    Fetch OHLCV data for multiple tickers from yfinance
    
    Args:
        tickers: List of ticker symbols
        start_date: Start date for data fetch
        end_date: End date for data fetch
    
    Returns:
        DataFrame with normalized columns
    """
    print(f"Fetching data for {len(tickers)} tickers...")
    print(f"Date range: {start_date} to {end_date}")
    
    successful = {}
    failures = {}
    
    for ticker in tickers:
        try:
            print(f"  → Downloading {ticker}...", end=" ", flush=True)
            
            # Download data for single ticker
            # CRITICAL: Set auto_adjust=False to get both Close and Adj Close
            df = yf.download(
                ticker,
                start=start_date,
                end=end_date,
                progress=False,
                auto_adjust=False  # This ensures we get Adj Close column
            )
            
            if df is None or df.empty:
                failures[ticker] = "No data returned"
                print("⚠ No data")
                continue
            
            # Handle MultiIndex columns from yfinance
            if isinstance(df.columns, pd.MultiIndex):
                # Flatten MultiIndex: ('Close', 'AAPL') -> 'Close'
                # For single ticker, level 1 is just the ticker name, so we only need level 0
                df.columns = df.columns.get_level_values(0)
            
            # Reset index to make Date a column
            df = df.reset_index()
            
            # Normalize column names to lowercase with underscores
            df.columns = df.columns.str.lower().str.replace(' ', '_')
            
            # Add symbol column
            df['symbol'] = ticker
            
            # Map to expected schema
            column_mapping = {
                'date': 'date',
                'open': 'open',
                'high': 'high', 
                'low': 'low',
                'close': 'close',
                'adj_close': 'adj_close',
                'volume': 'volume'
            }
            
            # Check if we have the required columns
            required_cols = ['date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']
            missing = [col for col in required_cols if col not in df.columns]
            
            if missing:
                failures[ticker] = f"Missing columns: {missing} (found: {list(df.columns)})"
                print(f"✗ Missing: {missing}")
                continue
            
            # Select and order columns
            df = df[['date', 'symbol', 'open', 'high', 'low', 'close', 'adj_close', 'volume']]
            
            # Convert date to string format (YYYY-MM-DD)
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            
            successful[ticker] = df
            print(f"✓ {len(df)} rows")
            
        except Exception as e:
            failures[ticker] = str(e)
            print(f"✗ Error: {str(e)}")
            continue
    
    if not successful:
        error_msg = f"No data was successfully fetched for any ticker. Failures: {failures}"
        raise ValueError(error_msg)
    
    # Show summary
    print(f"\nFetch summary: {len(successful)} succeeded, {len(failures)} failed")
    if failures:
        print(f"Failed tickers: {list(failures.keys())}")
    
    # Concatenate all successful dataframes
    combined_df = pd.concat(successful.values(), ignore_index=True)
    
    return combined_df


def main():
    """Main execution function"""
    print("=" * 60)
    print("WRDS Data Ingestion Demo - Data Fetch")
    print("=" * 60)
    
    # Ensure output directory exists
    ensure_directory_exists(OUTPUT_DIR)
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    
    # Fetch data
    try:
        df = fetch_market_data(
            DEFAULT_TICKERS,
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        # Save to CSV
        df.to_csv(OUTPUT_FILE, index=False)
        
        print("\n" + "=" * 60)
        print(f"✓ Data fetch complete!")
        print(f"  Total rows: {len(df)}")
        print(f"  Symbols: {df['symbol'].nunique()}")
        print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
        print(f"  Output: {OUTPUT_FILE}")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Error during data fetch: {str(e)}")
        raise


if __name__ == "__main__":
    main()
