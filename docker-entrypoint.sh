#!/bin/bash
set -e

# Function to wait for directory to be writable
wait_for_writable_dir() {
    local dir=$1
    local max_attempts=30
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if [ -w "$dir" ]; then
            echo "Directory $dir is writable"
            return 0
        fi
        echo "Waiting for $dir to become writable... (attempt $((attempt + 1))/$max_attempts)"
        sleep 1
        attempt=$((attempt + 1))
    done
    
    echo "ERROR: Directory $dir is not writable after $max_attempts attempts"
    return 1
}

# Ensure directories exist with proper permissions
init_directories() {
    echo "Initializing directories..."
    
    # Create directories as root if they don't exist
    for dir in /data /logs /config; do
        if [ ! -d "$dir" ]; then
            mkdir -p "$dir"
            echo "Created directory: $dir"
        fi
        
        # Set ownership to etluser
        chown etluser:etluser "$dir"
        chmod 755 "$dir"
    done
    
    # Verify directories are writable
    for dir in /data /logs /config; do
        wait_for_writable_dir "$dir" || exit 1
    done
}

# Function to create default config if not exists - FIXED WITH ABSOLUTE PATHS
init_config() {
    if [ ! -f "/config/config.json" ]; then
        echo "Creating default configuration..."
        
        cat > /config/config.json << 'EOF'
{
    "sap_urls": {
        "amrs": "https://www.mfanalyzer.com/BOE/OpenDocument/opendoc/openDocument.jsp?sIDType=CUID&iDocID=AYscKsmnmVFMgwa4u8GO5GU&sOutputFormat=E",
        "emea": "https://www.mfanalyzer.com/BOE/OpenDocument/opendoc/openDocument.jsp?sIDType=CUID&iDocID=AXFSzkEFSQpOrrU9_35AhpQ&sOutputFormat=E",
        "amrs_30days": "https://www.mfanalyzer.com/BOE/OpenDocument/opendoc/openDocument.jsp?sIDType=CUID&iDocID=AXmFuFTG4DBBrefomiwL1aE&sOutputFormat=E",
        "emea_30days": "https://www.mfanalyzer.com/BOE/OpenDocument/opendoc/openDocument.jsp?sIDType=CUID&iDocID=AQbKBz8wx0pHojHl0uBm2sw&sOutputFormat=E"
    },
    "auth": {
        "username": "sduggan",
        "password": "sduggan"
    },
    "db_path": "/data/fund_data.db",
    "data_dir": "/data",
    "download_timeout": 300,
    "lookback_timeout": 1200,
    "verify_ssl": true,
    "email_alerts": {
        "enabled": false,
        "recipients": ["etl-team@company.com"],
        "smtp_server": "smtp.company.com"
    },
    "validation": {
        "enabled": true,
        "change_threshold_percent": 5.0,
        "critical_fields": [
            "share_class_assets",
            "portfolio_assets",
            "one_day_yield",
            "seven_day_yield"
        ],
        "alert_on_missing_dates": true,
        "alert_on_major_changes": true
    }
}
EOF
        
        cat > /config/scheduler_config.json << 'EOF'
{
    "etl_config_path": "/config/config.json",
    "email_alerts": {
        "enabled": false,
        "smtp_server": "smtp.gmail.com",
        "smtp_port": 587,
        "from_email": "etl-alerts@company.com",
        "to_emails": ["data-team@company.com"],
        "use_tls": true
    },
    "retry_config": {
        "max_retries": 3,
        "retry_delay_minutes": 30
    },
    "backfill_days": 7,
    "log_dir": "/logs"
}
EOF
        
        chown etluser:etluser /config/*.json
        echo "Default configuration created with credentials sduggan/sduggan."
    fi
}

# Function to initialize database
init_database() {
    # Fix ownership of existing database if it exists
    if [ -f "/data/fund_data.db" ]; then
        current_owner=$(stat -c '%U' /data/fund_data.db)
        if [ "$current_owner" != "etluser" ]; then
            echo "Fixing database ownership (currently owned by $current_owner)..."
            chown etluser:etluser /data/fund_data.db
            chmod 664 /data/fund_data.db
            echo "Database ownership fixed"
        fi
    fi
    
    if [ ! -f "/data/fund_data.db" ]; then
        echo "Database not found. Initializing..."
        
        # Run as etluser to ensure proper ownership
        su -c "cd /app && python3 -c '
import sys
import os
sys.path.append(\"/app\")

# Test imports
try:
    from fund_etl_pipeline import FundDataETL
    print(\"Successfully imported FundDataETL\")
except Exception as e:
    print(f\"Import error: {e}\")
    sys.exit(1)

# Initialize ETL with config
try:
    etl = FundDataETL(\"/config/config.json\")
    print(f\"ETL initialized with db_path: {etl.db_path}\")
except Exception as e:
    print(f\"ETL initialization error: {e}\")
    sys.exit(1)

# Setup database
try:
    etl.setup_database()
    print(\"Database setup completed successfully\")
except Exception as e:
    print(f\"Database setup error: {e}\")
    sys.exit(1)

# Verify database exists
if os.path.exists(\"/data/fund_data.db\"):
    print(\"Database file created successfully\")
    # Test connection
    import sqlite3
    conn = sqlite3.connect(\"/data/fund_data.db\")
    cursor = conn.cursor()
    cursor.execute(\"SELECT name FROM sqlite_master WHERE type=\\\"table\\\"\")
    tables = cursor.fetchall()
    print(f\"Tables created: {tables}\")
    conn.close()
else:
    print(\"ERROR: Database file was not created\")
    sys.exit(1)
'" etluser
        
        if [ $? -eq 0 ]; then
            echo "Database initialized successfully"
            # Ensure proper ownership
            chown etluser:etluser /data/fund_data.db
            chmod 664 /data/fund_data.db
            ls -la /data/fund_data.db
        else
            echo "ERROR: Failed to initialize database"
            exit 1
        fi
    else
        echo "Database already exists at /data/fund_data.db"
        ls -la /data/fund_data.db
    fi
}

# Function to test the ETL workflow
test_workflow() {
    echo "Testing ETL workflow..."
    
    su -c "cd /app && python3 -c '
import sys
sys.path.append(\"/app\")
from fund_etl_utilities import FundDataMonitor
import sqlite3

# Test database connection
try:
    conn = sqlite3.connect(\"/data/fund_data.db\")
    cursor = conn.cursor()
    
    # Check tables exist
    cursor.execute(\"SELECT name FROM sqlite_master WHERE type=\\\"table\\\" ORDER BY name\")
    tables = cursor.fetchall()
    print(f\"Tables in database: {[t[0] for t in tables]}\")
    
    # Check fund_data schema
    cursor.execute(\"PRAGMA table_info(fund_data)\")
    columns = cursor.fetchall()
    print(f\"\\nfund_data columns: {len(columns)}\")
    for col in columns[:5]:  # Show first 5 columns
        print(f\"  - {col[1]} ({col[2]})\")
    print(\"  ...\")
    
    conn.close()
    print(\"\\nDatabase connectivity: OK\")
    
except Exception as e:
    print(f\"Database test failed: {e}\")
    sys.exit(1)

# Test monitor functionality
try:
    monitor = FundDataMonitor(\"/data/fund_data.db\")
    print(\"Monitor initialization: OK\")
except Exception as e:
    print(f\"Monitor test failed: {e}\")
    sys.exit(1)

print(\"\\nAll tests passed!\")
'" etluser
    
    if [ $? -eq 0 ]; then
        echo "Workflow test completed successfully"
    else
        echo "WARNING: Workflow test failed, but continuing..."
    fi
}

# Fix database permissions before operations
fix_db_permissions() {
    if [ -f "/data/fund_data.db" ]; then
        # Check current ownership
        current_owner=$(stat -c '%U' /data/fund_data.db)
        current_perms=$(stat -c '%a' /data/fund_data.db)
        
        if [ "$current_owner" != "etluser" ] || [ "$current_perms" != "664" ]; then
            echo "Fixing database permissions..."
            chown etluser:etluser /data/fund_data.db
            chmod 664 /data/fund_data.db
            echo "Database permissions fixed"
        fi
    fi
    
    # Also fix any SQLite journal files
    for f in /data/fund_data.db-*; do
        if [ -f "$f" ]; then
            chown etluser:etluser "$f"
            chmod 664 "$f"
        fi
    done
}

# Main initialization sequence
echo "Starting initialization sequence..."

# Always run directory initialization as root
init_directories

# Initialize configuration
init_config

# Initialize database
init_database

# Test the workflow
test_workflow

echo "Initialization complete"

# Handle different commands
case "$1" in
    "scheduler")
        echo "Starting Fund ETL scheduler with cron..."
        exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf
        ;;
    
    "run-daily")
        echo "Running daily ETL..."
        fix_db_permissions
        exec su -c "cd /app && python fund_etl_scheduler.py --run-daily" etluser
        ;;
    
    "backfill")
        shift
        echo "Running backfill for $1 days..."
        fix_db_permissions
        exec su -c "cd /app && python fund_etl_scheduler.py --backfill $1" etluser
        ;;
    
    "historical")
        shift
        echo "Loading historical data from $1 to $2..."
        fix_db_permissions
        exec su -c "cd /app && python fund_etl_scheduler.py --historical $1 $2" etluser
        ;;
    
    "test")
        echo "Testing SAP connectivity..."
        exec su -c "cd /app && python sap_connectivity_test.py" etluser
        ;;
    
    "validate")
        echo "Running 30-day lookback validation..."
        fix_db_permissions
        exec su -c "cd /app && python fund_etl_scheduler.py --validate" etluser
        ;;
    
    "report")
        echo "Generating data quality report..."
        exec su -c "cd /app && python -c 'from fund_etl_utilities import FundDataMonitor; monitor = FundDataMonitor(\"/data/fund_data.db\"); print(monitor.generate_data_quality_report())'" etluser
        ;;
    
    "shell")
        echo "Starting interactive shell..."
        exec su -c "/bin/bash" etluser
        ;;
    
    *)
        echo "Running custom command: $@"
        exec "$@"
        ;;
esac
