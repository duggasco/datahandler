#!/bin/bash
# Fix permissions for Fund ETL database and directories

echo "Fixing Fund ETL permissions..."

# Fix database ownership
if docker compose ps | grep -q fund-etl; then
    echo "Fixing database ownership..."
    docker compose exec -u root fund-etl chown -R etluser:etluser /data
    docker compose exec -u root fund-etl chmod 664 /data/fund_data.db 2>/dev/null || true
    
    # Fix any SQLite journal files
    docker compose exec -u root fund-etl bash -c 'for f in /data/fund_data.db-*; do [ -f "$f" ] && chmod 664 "$f" && chown etluser:etluser "$f"; done' 2>/dev/null || true
    
    echo "✓ Permissions fixed"
else
    echo "Fund ETL container is not running. Start it first with: ./run-etl.sh start"
    exit 1
fi

# Show current permissions
echo ""
echo "Current permissions:"
docker compose exec fund-etl ls -la /data/fund_data.db

echo ""
echo "You can now run: ./run-etl.sh validate"
