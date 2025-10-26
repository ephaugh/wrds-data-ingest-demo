# WRDS Data Ingestion Demo

[![Test Data Pipeline](https://github.com/ephaugh/wrds-data-ingest-demo/actions/workflows/test-pipeline.yml/badge.svg)](https://github.com/ephaugh/wrds-data-ingest-demo/actions/workflows/test-pipeline.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A lightweight, reproducible data pipeline demonstrating end-to-end ETL (Extract-Transform-Load) and analytics workflows similar to those used by the Wharton Research Data Services (WRDS) platform.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Pipeline Components](#pipeline-components)
- [Outputs](#outputs)
- [Troubleshooting](#troubleshooting)
- [Development](#development)
- [License](#license)

---

## Overview

This project simulates academic-style research data workflows by demonstrating:

1. **Extract**: Fetch daily OHLCV (Open, High, Low, Close, Volume) market data from Yahoo Finance
2. **Transform**: Normalize and clean data for consistency
3. **Load**: Store in a relational database (SQLite) with proper indexing
4. **Analyze**: Compute financial metrics (returns, volatility)
5. **Report**: Generate CSV summaries and visualizations

**Why this matters**: Research platforms like WRDS must ensure data reproducibility, version control, and consistent transformations. This demo shows those principles in a compact, runnable project that can be extended for real research workflows.

---

## Features

- End-to-end ETL pipeline for financial market data
- Automated data normalization handling various API formats
- SQLite database with proper schema and indexing
- Financial analytics: daily returns, volatility metrics
- Visualization: automated chart generation
- CI/CD integration: GitHub Actions testing on Python 3.10, 3.11, 3.12
- Reproducible: runs identically on any system
- Error handling: gracefully handles API failures and missing data

---

## Quick Start

### Prerequisites

- Python 3.10 or higher
- Internet connection (to fetch market data)

### Installation & Execution

```bash
# Clone the repository
git clone https://github.com/ephaugh/wrds-data-ingest-demo.git
cd wrds-data-ingest-demo

# Create a virtual environment
python -m venv .venv

# Activate virtual environment
# On macOS/Linux:
source .venv/bin/activate
# On Windows:
# .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the complete pipeline
python run_all.py
```

### Expected Results

The pipeline takes 30-60 seconds to complete and will:
- Fetch 365 days of data for 10 major stocks (AAPL, MSFT, GOOGL, AMZN, META, JPM, V, PG, XOM, NVDA)
- Store approximately 2,500 rows in a SQLite database
- Generate summary statistics and volatility metrics
- Create a sample price chart

---

## Project Structure

```
wrds-data-ingest-demo/
├── .github/
│   └── workflows/
│       └── test-pipeline.yml    # CI/CD automation
├── data/
│   └── prices_raw.csv           # Generated: raw market data
├── db/
│   └── marketdata.db            # Generated: SQLite database
├── reports/
│   ├── summary.csv              # Generated: summary statistics
│   ├── volatility.csv           # Generated: volatility metrics
│   └── charts/
│       └── adj_close_example.png # Generated: sample chart
├── fetch_data.py                # Step 1: Extract market data
├── load_to_db.py                # Step 2: Load to database
├── analyze_data.py              # Step 3: Analytics & reports
├── run_all.py                   # Orchestrator: runs full pipeline
├── requirements.txt             # Python dependencies
├── .gitignore                   # Git ignore rules
├── LICENSE                      # MIT License
└── README.md                    # This file
```

---

## Pipeline Components

### 1. fetch_data.py - Data Extraction

**Purpose**: Extract daily OHLCV data from Yahoo Finance API

**Key Features**:
- Fetches data for 10 default tickers over 365 days
- Handles MultiIndex DataFrames from yfinance
- Normalizes column names for consistency
- Robust error handling for API failures
- Outputs single consolidated CSV

**Usage**:
```bash
python fetch_data.py
```

**Output**: `data/prices_raw.csv`

---

### 2. load_to_db.py - Database Loading

**Purpose**: Load normalized CSV data into SQLite database

**Key Features**:
- Creates `prices_daily` table with proper schema
- Implements idempotent upsert pattern (DELETE + INSERT)
- Creates indexes for efficient querying
- Primary key on `(date, symbol)`

**Schema**:
```sql
CREATE TABLE prices_daily (
    date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    adj_close REAL,
    volume INTEGER,
    PRIMARY KEY (date, symbol)
);
CREATE INDEX idx_symbol_date ON prices_daily (symbol, date);
```

**Usage**:
```bash
python load_to_db.py
```

**Output**: `db/marketdata.db`

---

### 3. analyze_data.py - Analytics Engine

**Purpose**: Compute financial metrics and generate reports

**Key Features**:
- Calculates daily returns using adjusted close prices
- Computes summary statistics (mean, std, volume)
- Calculates 20-day rolling volatility (annualized)
- Generates price charts using matplotlib
- Uses Agg backend for headless environments

**Metrics**:
- **Daily Returns**: `(price_today - price_yesterday) / price_yesterday`
- **Annualized Volatility**: `rolling_std(20) × √252`

**Usage**:
```bash
python analyze_data.py
```

**Outputs**:
- `reports/summary.csv`
- `reports/volatility.csv`
- `reports/charts/adj_close_example.png`

---

### 4. run_all.py - Pipeline Orchestrator

**Purpose**: Run the complete ETL workflow in sequence

**Features**:
- Executes all three steps in order
- Provides clear progress messages
- Handles errors gracefully
- Reports execution time

**Usage**:
```bash
python run_all.py
```

---

## Outputs Explained

### 1. db/marketdata.db (SQLite Database)

**Table**: `prices_daily`

**Schema**: 
- `date` (TEXT): Trading date in YYYY-MM-DD format
- `symbol` (TEXT): Stock ticker symbol
- `open`, `high`, `low`, `close` (REAL): Daily price data
- `adj_close` (REAL): Split/dividend adjusted close price
- `volume` (INTEGER): Number of shares traded

**Query Examples**:
```sql
-- Get all data for Apple
SELECT * FROM prices_daily WHERE symbol = 'AAPL' ORDER BY date DESC;

-- Get latest prices for all symbols
SELECT symbol, MAX(date) as latest_date, adj_close 
FROM prices_daily 
GROUP BY symbol;

-- Calculate average volume by symbol
SELECT symbol, AVG(volume) as avg_volume 
FROM prices_daily 
GROUP BY symbol 
ORDER BY avg_volume DESC;
```

---

### 2. reports/summary.csv (Summary Statistics)

Per-symbol summary metrics:

| Column | Description |
|--------|-------------|
| `symbol` | Stock ticker |
| `obs` | Number of trading days with return data |
| `mean_daily_return` | Average daily return (decimal, e.g., 0.001 = 0.1%) |
| `std_daily_return` | Standard deviation of daily returns |
| `avg_volume` | Average daily trading volume |

**Example**:
```csv
symbol,obs,mean_daily_return,std_daily_return,avg_volume
AAPL,251,0.00123,0.0187,54321000
MSFT,251,0.00098,0.0165,32100000
```

**Use Cases**:
- Compare average returns across stocks
- Identify high-volatility assets
- Analyze trading volume patterns

---

### 3. reports/volatility.csv (Volatility Metrics)

Latest 20-day rolling volatility for each symbol:

| Column | Description |
|--------|-------------|
| `symbol` | Stock ticker |
| `ann_vol_20d` | Annualized volatility based on 20-day rolling window |

**Calculation**: `rolling_std(20 days) × √252`

**Example**:
```csv
symbol,ann_vol_20d
AAPL,0.2847
MSFT,0.2134
```

**Interpretation**:
- 0.28 = 28% annualized volatility
- Higher values indicate more price fluctuation
- Used for risk assessment and option pricing

---

### 4. reports/charts/adj_close_example.png (Price Chart)

**Description**: Line chart showing adjusted close prices over time for one example ticker (alphabetically first, typically AAPL)

**Features**:
- Clean, professional formatting
- Date on x-axis, price on y-axis
- Gridlines for readability

**Use Cases**:
- Quick visual inspection of price trends
- Template for creating additional charts
- Inclusion in reports or presentations

---

## Troubleshooting

### No data fetched for a ticker

**Symptom**: Warning messages like `⚠ No data` or `✗ Error`

**Causes**:
- yfinance API temporarily unavailable
- Ticker symbol changed or delisted
- Network connectivity issues

---

### "Database is locked" error

**Symptom**: `sqlite3.OperationalError: database is locked`

**Cause**: Another process is accessing the database

---

### Matplotlib warnings in headless environment

**Symptom**: Warnings about display backend

**Cause**: Running on a server without GUI

**Note**: The code uses the `Agg` backend for headless compatibility.

---

### Import errors

**Symptom**: `ModuleNotFoundError: No module named 'yfinance'`

**Cause**: Dependencies not installed

---

### yfinance API changes

**Symptom**: Column name mismatches or structure changes

**Cause**: yfinance library updates its API frequently

**Note**: The code is designed to handle MultiIndex and various column formats.

---

## Development

### Running Individual Scripts

Each script can be run independently:

```bash
# Fetch data only
python fetch_data.py

# Load to database only (requires data/prices_raw.csv)
python load_to_db.py

# Run analytics only (requires db/marketdata.db)
python analyze_data.py
```

---

### Customizing the Pipeline

#### Change tickers:
Edit `fetch_data.py`:
```python
DEFAULT_TICKERS = ["TSLA", "AMD", "NFLX", "NVDA"]
```

#### Adjust date range:
Edit `fetch_data.py`:
```python
LOOKBACK_DAYS = 730  # 2 years instead of 1
```

#### Modify volatility window:
Edit `analyze_data.py`:
```python
volatility = calculate_rolling_volatility(df, window=30)  # 30-day instead of 20-day
```

---

### Testing

The project includes automated testing via GitHub Actions:

**Workflow**: `.github/workflows/test-pipeline.yml`

**Tests**:
- Full pipeline execution
- Database creation and data validation
- CSV report generation
- Chart creation
- Cross-platform (Python 3.10, 3.11, 3.12)

**Manual testing**:
```bash
# Run the full pipeline
python run_all.py

# Verify outputs exist
ls -lh db/marketdata.db
ls -lh reports/*.csv
ls -lh reports/charts/*.png
```

---

## Academic Data Reproducibility

This project demonstrates key principles of reproducible research:

### Version-controlled pipeline
All data transformations are scripted and version-controlled, ensuring:
- Exact replication of results
- Audit trail of changes
- Collaboration without ambiguity

### Idempotent operations
Re-running the pipeline produces consistent results:
- Upsert pattern prevents duplicate data
- Deterministic calculations
- Predictable outputs

### Separation of concerns
Modular design with clear responsibilities:
- **Extract** (fetch_data.py): Only concerned with data acquisition
- **Load** (load_to_db.py): Only handles database operations
- **Analyze** (analyze_data.py): Pure analytics and reporting

### Documentation
- Inline code comments explain "why"
- README explains "what" and "how"
- Schema documented for database consumers

### Automated testing
CI/CD ensures:
- Code works on fresh environments
- Breaking changes are caught immediately
- Documentation stays accurate

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

**Data Attribution**: Market data is fetched from Yahoo Finance via the `yfinance` library. Please review [Yahoo Finance's terms of service](https://legal.yahoo.com/us/en/yahoo/terms/otos/index.html) for usage limitations.
