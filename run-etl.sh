#!/bin/bash
# Convenience script for common ETL operations

case "$1" in
    "start")
        echo "Starting Fund ETL..."
        docker compose up -d
        echo "ETL started. View logs with: docker compose logs -f"
        ;;
    
    "stop")
        echo "Stopping Fund ETL..."
        docker compose down
        ;;
    
    "clean")
        echo "Cleaning up all Fund ETL components..."
        echo "Stopping ETL container, UI (if running), and removing network..."
        # Stop all services including those with profiles
        docker compose --profile ui down -v
        echo ""
        echo "Diagnostic commands:"
        echo "  diagnose-validation, diagnose-comprehensive, check-history"
        echo ""
        echo "Cleanup complete. Removed:"
        echo "  ✓ Fund ETL container"
        echo "  ✓ Fund ETL UI container (if running)"
        echo "  ✓ Docker network"
        echo "  ✓ Anonymous volumes"
        echo ""
        echo "Note: Data in ./data, ./logs, and ./config is preserved"
        ;;
    
    "clean-all")
        echo "⚠️  AGGRESSIVE CLEANUP - This will remove containers, images, and networks!"
        read -p "Are you sure you want to remove everything? (y/N) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo "Removing all Fund ETL components including images..."
            # Stop and remove containers, networks, volumes
            docker compose --profile ui down -v
            # Remove images
            docker rmi fund-etl:latest fund-etl-ui:latest 2>/dev/null || true
            echo ""
            echo "Deep cleanup complete. Removed:"
            echo "  ✓ All containers"
            echo "  ✓ Docker images (fund-etl:latest, fund-etl-ui:latest)"
            echo "  ✓ Docker network"
            echo "  ✓ Anonymous volumes"
            echo ""
            echo "Note: Data in ./data, ./logs, and ./config is still preserved"
            echo "Run 'docker compose build' to rebuild images"
        else
            echo "Cleanup cancelled."
        fi
        ;;
    
    "restart")
        echo "Restarting Fund ETL..."
        docker compose restart
        ;;
    
    "status")
        docker compose ps
        echo ""
        echo "Testing database..."
        docker compose exec fund-etl python -c "import sqlite3; conn = sqlite3.connect('/data/fund_data.db'); print('Database OK'); conn.close()" 2>/dev/null || echo "Database not ready"
        echo ""
        echo "Quick database status:"
        docker compose exec fund-etl python /app/quick_status.py 2>/dev/null || echo "Unable to get status"
        ;;
    
    "ps")
        echo "Fund ETL Containers:"
        echo "==================="
        docker compose ps --all
        echo ""
        echo "To check UI containers: docker compose --profile ui ps"
        ;;
    
    "quick-status")
        echo "Quick database status check..."
        docker compose exec fund-etl python /app/quick_status.py
        ;;
    
    "logs")
        docker compose logs -f fund-etl
        ;;
    
    "logs-ui")
        echo "Following Fund ETL UI logs..."
        docker compose --profile ui logs -f fund-etl-ui
        ;;
    
    "run")
        echo "Running ETL manually..."
        docker compose run --rm fund-etl run-daily
        ;;
    
    "test")
        echo "Running test suite..."
        docker compose exec fund-etl python /app/test_fund_etl_setup.py
        ;;
    
    "report")
        echo "Generating report..."
        docker compose run --rm fund-etl report
        ;;
    
    "ui")
        echo "Starting with UI..."
        docker compose --profile ui up -d
        echo "UI available at http://localhost:8080"
        ;;
    
    "build")
        echo "Building containers..."
        docker compose build
        ;;
    
    "shell")
        echo "Opening shell..."
        docker compose run --rm fund-etl shell
        ;;
    
    "validate")
        echo "Running 30-day lookback validation with SELECTIVE updates (only changed records)..."
        echo "This will update only records that have material changes above the configured threshold."
        docker compose run --rm fund-etl validate
        ;;
    
    "validate-verbose")
        echo "Running 30-day lookback validation with VERBOSE output..."
        echo "This will show detailed comparison information for debugging."
        docker compose run --rm fund-etl validate-verbose
        ;;
    
    "validate-full")
        echo "Running 30-day lookback validation with FULL replacement..."
        echo "WARNING: This will replace ALL data for dates with any changes!"
        read -p "Are you sure you want to run full replacement? (y/N) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            docker compose run --rm fund-etl validate-full
        else
            echo "Full validation cancelled."
        fi
        ;;
    
    "validate-dry-run")
        echo "Running validation in dry-run mode (no updates)..."
        docker compose exec fund-etl python -c "
import sys
sys.path.append('/app')
from fund_etl_pipeline import FundDataETL
etl = FundDataETL('/config/config.json')
for region in ['AMRS', 'EMEA']:
    print(f'\n=== {region} ===')
    lookback_df = etl.download_lookback_file(region)
    if lookback_df is not None:
        results = etl.validate_against_lookback(region, lookback_df)
        print(f'Missing dates: {results[\"summary\"][\"missing_dates_count\"]}')
        print(f'Changed records: {results[\"summary\"][\"changed_records_count\"]}')
        if results['summary']['requires_update']:
            print('Status: Updates required')
            # Show sample of changes
            for i, change in enumerate(results['changed_records'][:3]):
                if change['type'] == 'value_change':
                    print(f'  Sample change {i+1}: Fund {change[\"fund_code\"]} on {change[\"date\"]}')
                    for field in change.get('changed_fields', []):
                        print(f'    {field[\"field\"]}: {field.get(\"db_value\")} -> {field.get(\"lookback_value\")} ({field.get(\"pct_change\", 0):.1f}% change)')
        else:
            print('Status: No updates required')
"
        ;;
    
    "diagnose-validation")
        echo "Running validation diagnostic to identify why records are being updated..."
        docker compose exec fund-etl python /app/diagnose_validation.py
        ;;
    
    "diagnose-comprehensive")
        echo "Running comprehensive diagnostic to check database vs lookback file..."
        docker compose exec fund-etl python /app/comprehensive_diagnostic.py
        ;;
    
    "check-history")
        echo "Checking ETL history to understand database state..."
        docker compose exec fund-etl python /app/check_etl_history.py
        ;;
    
    "test-validation")
        echo "Testing validation feature..."
        docker compose exec fund-etl python /app/test_lookback_validation.py
        ;;
    
    "fix-permissions")
        echo "Fixing database permissions..."
        docker compose exec -u root fund-etl chown -R etluser:etluser /data
        docker compose exec -u root fund-etl chmod 664 /data/fund_data.db 2>/dev/null || true
        docker compose exec -u root fund-etl bash -c 'for f in /data/fund_data.db-*; do [ -f "$f" ] && chmod 664 "$f" && chown etluser:etluser "$f"; done' 2>/dev/null || true
        echo "Permissions fixed. Current status:"
        docker compose exec fund-etl ls -la /data/fund_data.db
        ;;
    
    "initialize")
        echo "Initializing database with recent data..."
        docker compose exec fund-etl python /app/initialize_database.py
        ;;
    
    "backfill")
        if [ -z "$2" ]; then
            echo "Usage: $0 backfill DAYS"
            echo "Example: $0 backfill 7"
            exit 1
        fi
        echo "Backfilling $2 days of data..."
        docker compose run --rm fund-etl backfill $2
        ;;
    
    "historical")
        if [ -z "$2" ] || [ -z "$3" ]; then
            echo "Usage: $0 historical START_DATE END_DATE"
            echo "Example: $0 historical 2025-06-01 2025-06-30"
            exit 1
        fi
        echo "Loading historical data from $2 to $3..."
        docker compose run --rm fund-etl historical $2 $3
        ;;
    
    "help"|"--help"|"-h")
        echo "Fund ETL Management Script"
        echo ""
        echo "Usage: $0 COMMAND [options]"
        echo ""
        echo "Commands:"
        echo "  start              Start the ETL container"
        echo "  stop               Stop the ETL container"
        echo "  clean              Stop all containers (ETL & UI) and remove network"
        echo "  clean-all          Remove everything including Docker images (with confirmation)"
        echo "  restart            Restart the ETL container"
        echo "  status             Show container status and health"
        echo "  quick-status       Quick database status summary"
        echo "  ps                 List all Fund ETL containers"
        echo "  logs               Follow ETL container logs"
        echo "  logs-ui            Follow UI container logs"
        echo "  run                Run ETL manually for today"
        echo "  test               Run test suite"
        echo "  report             Generate data quality report"
        echo "  ui                 Start with web UI"
        echo "  build              Build/rebuild containers"
        echo "  shell              Open interactive shell"
        echo ""
        echo "Validation Commands:"
        echo "  validate           Run validation with selective updates (default)"
        echo "  validate-full      Run validation with full replacement"
        echo "  validate-dry-run   Check what would be updated without making changes"
        echo "  validate-verbose   Run validation with detailed debug output"
        echo ""
        echo "Diagnostic Commands:"
        echo "  diagnose-validation     Diagnose why records are being updated"
        echo "  diagnose-comprehensive  Check database vs lookback file mismatches"
        echo "  check-history          Review ETL run history and database state"
        echo "  test-validation        Test validation functionality"
        echo ""
        echo "Data Commands:"
        echo "  initialize         Initialize empty database with recent data"
        echo "  backfill DAYS      Backfill missing data for N days"
        echo "  historical START END  Load historical data for date range"
        echo ""
        echo "Maintenance:"
        echo "  fix-permissions    Fix file permissions"
        echo ""
        ;;
    
    *)
        echo "Fund ETL Management Script"
        echo ""
        echo "Usage: $0 COMMAND [options]"
        echo ""
        echo "Common commands:"
        echo "  start, stop, clean, restart, status, quick-status, ps, logs, logs-ui"
        echo "  run, test, report, ui, build, shell"
        echo ""
        echo "Validation commands:"
        echo "  validate, validate-full, validate-dry-run, validate-verbose"
        echo ""
        echo "Diagnostic commands:"
        echo "  diagnose-validation, diagnose-comprehensive, check-history, test-validation"
        echo ""
        echo "Data commands:"
        echo "  initialize, backfill, historical"
        echo ""
        echo "Maintenance:"
        echo "  fix-permissions, clean-all"
        echo ""
        echo "Run '$0 help' for detailed information about each command"
        exit 1
        ;;
esac
