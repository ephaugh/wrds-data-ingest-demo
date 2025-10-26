"""
WRDS Data Ingestion Demo - Pipeline Orchestrator
Runs the complete ETL and analytics pipeline
"""
import subprocess
import sys
from datetime import datetime


def run_script(script_name, description):
    """
    Run a Python script and handle errors
    
    Args:
        script_name: Name of the Python script to run
        description: Human-readable description of the step
    
    Returns:
        True if successful, False otherwise
    """
    print(f"\n{'=' * 60}")
    print(f"STEP: {description}")
    print(f"{'=' * 60}")
    
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            check=True,
            capture_output=False,
            text=True
        )
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"\n✗ Error running {script_name}")
        print(f"  Exit code: {e.returncode}")
        return False
    
    except Exception as e:
        print(f"\n✗ Unexpected error: {str(e)}")
        return False


def main():
    """Main orchestration function"""
    start_time = datetime.now()
    
    print("\n" + "=" * 60)
    print("WRDS DATA INGESTION DEMO - FULL PIPELINE")
    print("=" * 60)
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Define pipeline steps
    steps = [
        ("fetch_data.py", "Fetch Market Data"),
        ("load_to_db.py", "Load to Database"),
        ("analyze_data.py", "Run Analytics & Generate Reports")
    ]
    
    # Execute each step
    for script, description in steps:
        success = run_script(script, description)
        
        if not success:
            print(f"\n{'=' * 60}")
            print("✗ PIPELINE FAILED")
            print(f"{'=' * 60}")
            sys.exit(1)
    
    # Calculate elapsed time
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    
    # Success message
    print(f"\n{'=' * 60}")
    print("✓ PIPELINE COMPLETE!")
    print(f"{'=' * 60}")
    print("Outputs generated:")
    print("  → Database:     db/marketdata.db")
    print("  → Summary:      reports/summary.csv")
    print("  → Volatility:   reports/volatility.csv")
    print("  → Chart:        reports/charts/adj_close_example.png")
    print(f"\nTotal time: {elapsed:.1f} seconds")
    print(f"Completed: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
