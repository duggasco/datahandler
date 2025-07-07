# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a production ETL pipeline for processing daily fund data from SAP BusinessObjects. It runs automatically at 6 AM Eastern Time and processes ~2,000-3,000 fund records per region (AMRS and EMEA) with intelligent weekend/holiday handling.

## Key Commands

### Docker Operations
```bash
./run-etl.sh build              # Build Docker images
./run-etl.sh start              # Start all services
./run-etl.sh stop               # Stop all services
./run-etl.sh restart            # Restart services
./run-etl.sh ui-start           # Start web UI on port 8080
./run-etl.sh ui-stop            # Stop web UI
```

### ETL Operations
```bash
./run-etl.sh run                # Run ETL manually for today
./run-etl.sh run-date YYYY-MM-DD # Run ETL for specific date
./run-etl.sh backfill 30        # Backfill last 30 days
./run-etl.sh validate           # Run 30-day lookback validation
./run-etl.sh validate-dry-run   # Preview validation changes
```

### Testing & Diagnostics
```bash
./run-etl.sh test               # Run comprehensive test suite
./run-etl.sh test-validation    # Test validation functionality
./run-etl.sh diagnose-validation # Debug record updates
./run-etl.sh diagnose-comprehensive # Deep data analysis
./run-etl.sh check-history      # View ETL run history
./run-etl.sh quick-status       # Database status summary
```

### Running Individual Tests
```bash
docker compose exec fund-etl python /app/test_fund_etl_setup.py
docker compose exec fund-etl python /app/test_lookback_validation.py
```

## Architecture

### Core Components
- **fund_etl_pipeline.py**: Main ETL engine with FundETL class
- **fund_etl_scheduler.py**: Orchestration, scheduling, and validation logic
- **sap_download_module.py**: Selenium-based SAP downloads with retry logic
- **fund_etl_utilities.py**: Monitoring, analysis, and diagnostic tools
- **fund_etl_ui.py**: Flask web dashboard (optional, runs on port 8080)

### Data Flow
1. **Download**: Selenium fetches Excel files from SAP OpenDocument URLs
2. **Transform**: Pandas processes data with type conversions and validations
3. **Load**: SQLite database with selective updates based on changes
4. **Validate**: 30-day lookback compares current vs historical data

### Database Schema
- Primary table: `fund_data` with ~50 columns
- Indexes on: region, as_of_date, fund_code, (region, as_of_date, fund_code)
- ETL history tracked in `etl_runs` table

### Key Business Logic
- Weekend/holiday data carries forward from previous business day
- US holidays handled via `holidays` library
- 5% change threshold for validation warnings
- Selective updates only modify changed records
- 30-day lookback validation ensures data integrity

## Development Patterns

### Date Handling
- Always use `fund_etl_utilities.get_previous_business_day()` for date logic
- Format dates as YYYY-MM-DD for consistency
- SAP uses MM/DD/YYYY format in URLs

### Error Handling
- All modules use comprehensive try/except blocks with logging
- Retry logic (3 attempts) for SAP downloads
- Extended timeouts (300s) for large file processing

### Configuration
- Settings in `fund_etl_config.py`
- SAP URLs for AMRS/EMEA regions (daily + lookback)
- Email alerts configurable but disabled by default

### Docker Context
- Runs as non-root user `etluser` (UID 1000)
- Working directory: `/app`
- Data persisted in: `/app/data/fund_data.db`
- Logs in: `/app/logs/`

## Common Development Tasks

### Adding New Data Fields
1. Update `fund_etl_pipeline.py`: Add column to COLUMN_MAPPING
2. Update database schema in `initialize_tables()` 
3. Add field to data processing in `transform_data()`
4. Update validation logic if needed

### Modifying SAP URLs
1. Edit `fund_etl_config.py`: Update URL constants
2. Test with: `./run-etl.sh test`
3. Verify downloads: Check `/app/data/downloads/`

### Debugging Failed ETL Runs
1. Check logs: `docker compose logs fund-etl`
2. Run diagnostics: `./run-etl.sh diagnose-comprehensive`
3. Test SAP connectivity: `./run-etl.sh test`
4. Review history: `./run-etl.sh check-history`

### Testing Changes
1. Run full test suite: `./run-etl.sh test`
2. Test specific date: `./run-etl.sh run-date 2024-01-15`
3. Validate without updates: `./run-etl.sh validate-dry-run`

## Important Notes

- No linting configuration exists - maintain consistent Python style
- Tests are standalone scripts, not pytest modules
- All operations run inside Docker containers
- Database operations use raw SQL, not an ORM
- Selenium runs in headless Chrome mode
- Supervisor manages cron jobs and processes