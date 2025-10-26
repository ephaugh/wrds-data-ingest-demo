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
    
    all_data = []
    
    for ticker in tickers:
        try:
            print(f"  → Downloading {ticker}...", end=" ")
            
            # Download data for single ticker
            df = yf.download(
                ticker,
                start=start_date,
                end=end_date,
                progress=False,
            )
            
            if df.empty:
                print("⚠ No data")
                continue
            
            # Normalize column names (yfinance returns multi-index for single ticker)
            df = df.reset_index()
            
            # Flatten multi-index columns if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [col[0] if col[1] == '' else col[1] for col in df.columns]
            
            # Standardize column names
            df.columns = df.columns.str.lower().str.replace(' ', '_')
            
            # Add symbol column
            df['symbol'] = ticker
            
            # Rename columns to match schema
            column_mapping = {
                'date': 'date',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'adj_close': 'adj_close',
                'volume': 'volume'
            }
            
            df = df.rename(columns=column_mapping)
            
            # Select and order columns
            df = df[['date', 'symbol', 'open', 'high', 'low', 'close', 'adj_close', 'volume']]
            
            # Convert date to string format
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            
            all_data.append(df)
            print(f"✓ {len(df)} rows")
            
        except Exception as e:
            print(f"✗ Error: {str(e)}")
            continue
    
    if not all_data:
        raise ValueError("No data was successfully fetched for any ticker")
    
    # Concatenate all dataframes
    combined_df = pd.concat(all_data, ignore_index=True)
    
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
