# Fund ETL Pipeline - Comprehensive Documentation

A production-ready ETL pipeline for downloading and processing daily fund data from SAP BusinessObjects OpenDocument. The system downloads XLSX files for AMRS and EMEA regions, performs data quality checks, and loads to a SQLite database with intelligent date handling for weekends and holidays.

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Key Features](#key-features)
- [Quick Start](#quick-start)
- [Core Components](#core-components)
- [Testing & Diagnostic Tools](#testing--diagnostic-tools)
- [Command Reference](#command-reference)
- [Business Rules](#business-rules)
- [Data Validation](#data-validation)
- [Monitoring & UI](#monitoring--ui)
- [Troubleshooting](#troubleshooting)
- [Development](#development)

## Overview

The Fund ETL Pipeline automates the daily download and processing of fund data from SAP OpenDocument URLs. It handles two regions (AMRS and EMEA), processes approximately 2,000-3,000 fund records per region daily, and maintains a comprehensive SQLite database with historical data.

### Key Capabilities
- **Automated Daily ETL**: Scheduled at 6 AM Eastern Time
- **Weekend/Holiday Handling**: Intelligent data carry-forward logic
- **30-Day Lookback Validation**: Detects and corrects data discrepancies
- **Web Dashboard**: Real-time monitoring and data exploration
- **Docker Containerized**: Easy deployment and consistent environment
- **Comprehensive Testing Suite**: Validation and diagnostic tools

## Architecture

```
┌─────────────────────┐     ┌─────────────────────┐
│   SAP OpenDocument  │     │   SAP OpenDocument  │
│      (AMRS)         │     │      (EMEA)         │
└──────────┬──────────┘     └──────────┬──────────┘
           │                           │
           └─────────────┬─────────────┘
                         │
                    ┌────▼────┐
                    │Selenium │
                    │Downloader│
                    └────┬────┘
                         │
                ┌────────▼────────┐
                │  Fund ETL       │
                │  Pipeline       │
                └────────┬────────┘
                         │
                ┌────────▼────────┐
                │ SQLite Database │
                └────────┬────────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
   ┌────▼────┐    ┌──────▼──────┐  ┌─────▼─────┐
   │ Web UI  │    │ Monitoring  │  │ Scheduler │
   └─────────┘    └─────────────┘  └───────────┘
```

## Key Features

### 1. **Intelligent Date Handling**
- Files contain prior business day data
- Friday data automatically carries to Saturday and Sunday
- US holidays recognized with data carry-forward
- Missing data detection and backfill capabilities

### 2. **Data Quality & Validation**
- Column completeness checks
- Duplicate fund code handling (#MULTIVALUE)
- Numeric field validation
- 30-day lookback validation with configurable thresholds
- Selective update capability (only updates changed records)

### 3. **Robust Download Mechanism**
- Selenium-based SAP authentication
- Extended timeouts for large lookback files (30-day reports)
- Automatic retry logic
- Fallback HTTP download option

### 4. **Comprehensive Monitoring**
- Web dashboard with real-time metrics
- ETL history tracking
- Data quality reports
- System telemetry
- Export capabilities for analysis

## Quick Start

### Prerequisites
- Docker and Docker Compose V2
- 4GB+ available RAM
- 10GB+ available disk space

### Installation

1. **Clone the repository and navigate to the directory:**
   ```bash
   cd fund-etl-docker
   ```

2. **Check Docker environment:**
   ```bash
   ./check-docker.sh
   ```

3. **Build and start the containers:**
   ```bash
   docker compose build
   docker compose up -d
   ```

4. **Initialize the database with recent data:**
   ```bash
   ./run-etl.sh initialize
   ```

5. **Verify the setup:**
   ```bash
   ./run-etl.sh test
   ./run-etl.sh status
   ```

6. **Access the web UI (optional):**
   ```bash
   ./run-etl.sh ui
   # Visit http://localhost:8080
   ```

## Core Components

### Python Modules

#### `fund_etl_pipeline.py`
The main ETL engine that:
- Downloads files from SAP OpenDocument URLs
- Validates data structure and quality
- Processes dates according to business rules
- Loads data to SQLite database
- Performs 30-day lookback validation
- Handles selective updates for changed records

#### `fund_etl_scheduler.py`
Orchestration layer that:
- Manages daily ETL runs with retry logic
- Handles backfilling of missing dates
- Sends email alerts (when configured)
- Provides CLI interface for manual operations
- Coordinates validation runs with different update modes

#### `fund_etl_utilities.py`
Monitoring and analysis tools:
- `FundDataMonitor`: ETL status tracking, data quality reports
- `FundDataQuery`: Search and export functionality
- Missing date detection
- Data trend visualization

#### `sap_download_module.py`
Selenium-based downloader that:
- Handles SAP BusinessObjects authentication
- Downloads files with configurable timeouts
- Supports both daily and 30-day lookback files
- Manages browser automation in headless mode

#### `fund_etl_ui.py`
Web dashboard providing:
- Real-time ETL status monitoring
- Fund data exploration with filtering
- ETL history viewing
- System telemetry and statistics
- CSV export functionality

#### `etl_monitor.py`
Container health monitoring that:
- Runs continuous health checks
- Generates daily summary reports
- Cleans up old log files
- Writes health status for container health checks

### Configuration Files

#### `/config/config.json`
Main configuration including:
```json
{
    "sap_urls": {
        "amrs": "...",
        "emea": "...",
        "amrs_30days": "...",
        "emea_30days": "..."
    },
    "auth": {
        "username": "sduggan",
        "password": "sduggan"
    },
    "validation": {
        "enabled": true,
        "update_mode": "selective",
        "change_threshold_percent": 5.0
    }
}
```

#### `/config/scheduler_config.json`
Scheduler settings for retry logic and notifications

## Testing & Diagnostic Tools

The project includes a comprehensive suite of testing and diagnostic utilities:

### Setup & Verification Tools

#### `test_fund_etl_setup.py`
**Usage:** `./run-etl.sh test`

Comprehensive test suite that verifies:
- Database setup and table creation
- ETL component imports and initialization
- Sample ETL run with mock data
- All 27 expected columns are present
- Indices are properly created

#### `initialize_database.py`
**Usage:** `./run-etl.sh initialize`

Interactive tool for initial database population:
- Prompts for confirmation before proceeding
- Loads last 7 days of data by default
- Shows progress for each date processed
- Provides summary of successful/failed loads
- Essential for new deployments

#### `quick_status.py`
**Usage:** `./run-etl.sh quick-status`

Provides instant database health check:
- Total record count
- Records by region
- Date range of data
- Last successful ETL run
- Compact summary format

### Validation & Diagnostic Tools

#### `test_lookback_validation.py`
**Usage:** `./run-etl.sh test-validation`

Tests the 30-day lookback validation feature:
- Verifies validation configuration
- Checks required methods exist
- Runs mock validation scenario
- Confirms lookback URLs are configured

#### `diagnose_validation.py`
**Usage:** `./run-etl.sh diagnose-validation`

Detailed diagnostic for validation issues:
- Downloads lookback file sample
- Performs field-by-field comparison
- Shows exact values and data types
- Identifies why records are flagged as changed
- Highlights precision and formatting differences

#### `comprehensive_diagnostic.py`
**Usage:** `./run-etl.sh diagnose-comprehensive`

Deep analysis of database vs lookback mismatches:
- Database record counts and date ranges
- Sample fund codes from both sources
- Fund code overlap analysis
- Formatting issue detection
- Detailed recommendations for resolution

#### `check_etl_history.py`
**Usage:** `./run-etl.sh check-history`

ETL history analysis tool:
- Shows last 50 ETL runs
- Success/failure summary statistics
- Data distribution by date
- Recent issues and error messages
- Recommendations based on findings

### Development & Testing Tools

#### `sap_connectivity_test.py`
**Usage:** Called internally by `./run-etl.sh test`

Tests SAP OpenDocument connectivity:
- Verifies URLs are accessible
- Tests with/without authentication
- Checks SSL certificates
- Attempts partial download
- Creates `sap_auth.json` template

#### `fund_etl_test.py`
**Note:** Development tool not integrated into run-etl.sh

Manual testing utility for:
- Reading actual XLSX files
- Date parsing validation
- Duplicate handling logic
- Numeric conversion testing
- Database operation verification

**To remove if not needed:**
```bash
# Remove from container
docker compose exec fund-etl rm /app/fund_etl_test.py

# Remove from local directory
rm fund_etl_test.py
```

## Command Reference

The `./run-etl.sh` script provides convenient access to all functionality:

### Basic Operations
```bash
./run-etl.sh start              # Start the ETL container
./run-etl.sh stop               # Stop the ETL container
./run-etl.sh restart            # Restart the ETL container
./run-etl.sh status             # Show container and database status
./run-etl.sh logs               # Follow ETL logs
./run-etl.sh shell              # Open interactive shell
```

### ETL Operations
```bash
./run-etl.sh run                # Run ETL manually for today
./run-etl.sh backfill 7         # Backfill last 7 days
./run-etl.sh historical 2025-06-01 2025-06-30  # Load date range
./run-etl.sh initialize         # Initialize empty database
```

### Validation Commands
```bash
./run-etl.sh validate           # Run validation with selective updates
./run-etl.sh validate-full      # Full replacement for changed dates
./run-etl.sh validate-dry-run   # Preview changes without updating
./run-etl.sh validate-verbose   # Detailed debug output
```

### Testing & Diagnostics
```bash
./run-etl.sh test               # Run comprehensive test suite
./run-etl.sh test-validation    # Test validation functionality
./run-etl.sh diagnose-validation      # Why records are updating
./run-etl.sh diagnose-comprehensive   # Database vs lookback analysis
./run-etl.sh check-history      # Review ETL run history
./run-etl.sh quick-status       # Quick database summary
```

### Monitoring & Reports
```bash
./run-etl.sh report             # Generate data quality report
./run-etl.sh ui                 # Start web dashboard
./run-etl.sh logs-ui            # View UI container logs
```

### Maintenance
```bash
./run-etl.sh fix-permissions    # Fix file permissions
./run-etl.sh clean              # Stop containers and clean network
./run-etl.sh clean-all          # Remove everything including images
./run-etl.sh build              # Rebuild containers
```

## Business Rules

### Date Processing
1. **Business Days Only**: ETL runs only on US business days (Mon-Fri, excluding holidays)
2. **Prior Day Data**: Files always contain the prior business day's data
3. **Weekend Expansion**: Friday data automatically expands to Saturday and Sunday
4. **Holiday Handling**: On holidays, previous available data carries forward

### Data Quality Rules
1. **Required Fields**: Date, Fund Code, Fund Name, Currency must be present
2. **Duplicate Handling**: #MULTIVALUE fund codes get unique suffixes
3. **Numeric Validation**: '-' values convert to NULL
4. **Text Cleaning**: All text fields are trimmed of whitespace
5. **Special Characters**: Properly escaped for database insertion

### Validation Thresholds
- **Change Detection**: 5% threshold for material changes (configurable)
- **Critical Fields**: share_class_assets, portfolio_assets, one_day_yield, seven_day_yield
- **Update Modes**: 
  - Selective: Only update changed records (default)
  - Full: Replace entire date when changes detected

## Data Validation

The system includes comprehensive 30-day lookback validation:

### How It Works
1. Downloads special 30-day lookback files from SAP
2. Compares with existing database data
3. Identifies missing dates and changed records
4. Updates database based on configured mode

### Validation Modes
- **Selective Mode** (default): Only updates records with material changes above threshold
- **Full Mode**: Replaces all data for dates with any changes

### Configuration
```json
"validation": {
    "enabled": true,
    "update_mode": "selective",
    "change_threshold_percent": 5.0,
    "critical_fields": ["share_class_assets", "portfolio_assets", "one_day_yield", "seven_day_yield"]
}
```

## Monitoring & UI

### Web Dashboard
Access at http://localhost:8080 when UI is running:

1. **Overview Tab**: System metrics, recent runs, data quality
2. **Fund Data Tab**: Browse and search fund records with pagination
3. **ETL History Tab**: Detailed run history and issues
4. **Telemetry Tab**: System statistics and performance

### Key Metrics Monitored
- Total records and unique funds
- Latest data date
- Missing dates in last 7 days
- ETL success/failure rates
- Data completeness percentages
- Processing times
- Database size and growth

### Export Capabilities
- Fund data export to CSV with filters
- ETL log export for analysis
- Date range and region-based exports

### Health Monitoring
- Continuous health checks via `etl_monitor.py`
- Health status available at `/data/health.json`
- Automatic log cleanup (30-day retention)
- Daily summary report generation

## Troubleshooting

### Common Issues

#### 1. Database Empty After Validation
```bash
# Check ETL history
./run-etl.sh check-history

# Initialize with data
./run-etl.sh initialize

# Then run validation
./run-etl.sh validate
```

#### 2. SAP Download Failures
```bash
# Test connectivity
./run-etl.sh test

# Check credentials in config
cat config/config.json

# View detailed logs
./run-etl.sh logs

# Check for timeout issues (lookback files are large)
# Consider increasing lookback_timeout in config
```

#### 3. Permission Errors
```bash
# Fix permissions
./run-etl.sh fix-permissions

# Check ownership
docker compose exec fund-etl ls -la /data
```

#### 4. Validation Updating Everything
```bash
# Run comprehensive diagnostic
./run-etl.sh diagnose-comprehensive

# Check for fund code mismatches
./run-etl.sh diagnose-validation

# Review validation configuration
cat config/config.json | grep -A5 validation
```

#### 5. Weekend Data Issues
```bash
# Check if Friday data exists
./run-etl.sh quick-status

# Verify weekend expansion logic
docker compose exec fund-etl sqlite3 /data/fund_data.db \
  "SELECT date, COUNT(*) FROM fund_data WHERE date LIKE '%-06' OR date LIKE '%-07' GROUP BY date"
```

### Log Locations
- Application logs: `./logs/`
- ETL history: In database `etl_log` table
- Container logs: `docker compose logs`
- Health status: `./data/health.json`
- Supervisor logs: `/logs/supervisord.log`

## Development

### Project Structure
```
fund-etl-docker/
├── Core ETL Components
│   ├── fund_etl_pipeline.py      # Core ETL logic
│   ├── fund_etl_scheduler.py     # Orchestration
│   ├── fund_etl_utilities.py     # Monitoring tools
│   └── sap_download_module.py    # SAP integration
├── User Interface
│   ├── fund_etl_ui.py           # Web dashboard
│   └── Dockerfile.ui            # UI container
├── Testing & Diagnostics
│   ├── test_fund_etl_setup.py   # Setup verification
│   ├── test_lookback_validation.py # Validation tests
│   ├── diagnose_validation.py   # Validation diagnostics
│   ├── comprehensive_diagnostic.py # Deep analysis
│   ├── check_etl_history.py    # History analysis
│   ├── initialize_database.py  # DB initialization
│   └── quick_status.py          # Status check
├── Container Configuration
│   ├── docker-compose.yml       # Container orchestration
│   ├── Dockerfile              # Main container
│   ├── docker-entrypoint.sh   # Container initialization
│   ├── supervisord.conf        # Process management
│   └── requirements.txt        # Python dependencies
├── Scripts & Configuration
│   ├── run-etl.sh             # CLI interface
│   ├── check-docker.sh        # Environment check
│   └── config/                # Configuration files
└── Data & Logs
    ├── data/                  # SQLite database
    └── logs/                  # Application logs
```

### Database Schema
The main `fund_data` table includes 28 columns:

**Identifiers:**
- date, region, fund_code, fund_name, master_class_fund_name

**Financial Metrics:**
- share_class_assets, portfolio_assets
- one_day_yield, one_day_gross_yield
- seven_day_yield, seven_day_gross_yield
- expense_ratio

**Risk & Liquidity:**
- wam (Weighted Average Maturity)
- wal (Weighted Average Life)
- daily_liquidity, weekly_liquidity

**Metadata:**
- rating, unique_identifier, nasdaq
- fund_complex, subcategory
- domicile, currency
- transactional_nav, market_nav
- fees, gates

**System Fields:**
- created_at (timestamp)

### Environment Variables
- `TZ`: America/New_York (for proper holiday handling)
- `PYTHONUNBUFFERED`: 1 (for real-time logging)
- `LOG_LEVEL`: INFO/DEBUG (logging verbosity)
- `DB_PATH`: /data/fund_data.db (database location)

### Adding New Features
1. Test in development environment first
2. Update configuration templates if needed
3. Add command to `run-etl.sh` if user-facing
4. Create/update test utilities
5. Update this documentation
6. Test with production data volume

### Performance Considerations
- Database indices on date, region, fund_code
- Pagination for large result sets in UI
- Batch inserts for database operations
- Extended timeouts for 30-day lookback files
- Headless Chrome for reduced resource usage

## Best Practices

### Daily Operations
1. Monitor the dashboard for failed runs
2. Check for missing dates weekly
3. Run validation after any extended downtime
4. Review data quality metrics monthly

### Maintenance
1. Keep logs under control (auto-cleanup after 30 days)
2. Monitor database growth
3. Update SAP credentials before expiration
4. Test disaster recovery quarterly

### Troubleshooting Workflow
1. Check container status first
2. Review recent ETL logs
3. Run appropriate diagnostic tool
4. Check configuration if needed
5. Verify SAP connectivity
6. Contact support with diagnostic output

## Support
For issues or questions:
1. Check troubleshooting section
2. Run relevant diagnostic commands
3. Review ETL history for patterns
4. Check logs for detailed errors
5. Contact support team with diagnostic outputs

## Version History
- v1.0: Initial release with basic ETL
- v2.0: Added 30-day lookback validation
- v3.0: Selective update capability
- v3.1: Enhanced UI with telemetry
- Current: Full testing suite and diagnostics