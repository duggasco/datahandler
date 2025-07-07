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
    
    "restart")
        echo "Restarting Fund ETL..."
        docker compose restart
        ;;
    
    "status")
        docker compose ps
        echo ""
        echo "Testing database..."
        docker compose exec fund-etl python -c "import sqlite3; conn = sqlite3.connect('/data/fund_data.db'); print('Database OK'); conn.close()" 2>/dev/null || echo "Database not ready"
        ;;
    
    "logs")
        docker compose logs -f fund-etl
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
        echo "Running 30-day lookback validation..."
        docker compose run --rm fund-etl validate
        ;;
    
    "test-validation")
        echo "Testing validation feature..."
        docker compose exec fund-etl python /app/test_lookback_validation.py
        ;;
    
    *)
        echo "Usage: $0 {start|stop|restart|status|logs|run|test|report|ui|build|shell|validate|test-validation}"
        exit 1
        ;;
esac
