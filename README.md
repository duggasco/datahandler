# Fund ETL Docker

Dockerized Fund ETL pipeline for processing daily fund data from SAP OpenDocument.

## Quick Start

1. **Build and start the containers:**
   ```bash
   cd fund-etl-docker
   docker compose build
   docker compose up -d
   ```

2. **Verify the setup:**
   ```bash
   docker compose exec fund-etl python /app/test_fund_etl_setup.py
   ```

3. **View logs:**
   ```bash
   docker compose logs -f fund-etl
   ```

4. **Start with UI (optional):**
   ```bash
   docker compose --profile ui up -d
   # Access UI at http://localhost:8080
   ```

## Default Configuration

The system comes pre-configured with:
- SAP URLs for AMRS and EMEA regions
- Default credentials: `sduggan/sduggan`
- SQLite database at `/data/fund_data.db`
- Daily ETL scheduled at 6 AM Eastern Time

To change credentials, edit `config/config.json` after first run.

## Commands

### Run ETL manually:
```bash
docker compose run --rm fund-etl run-daily
```

### Backfill data:
```bash
docker compose run --rm fund-etl backfill 7
```

### Load historical data:
```bash
docker compose run --rm fund-etl historical 2025-06-01 2025-06-30
```

### Test SAP connectivity:
```bash
docker compose run --rm fund-etl test
```

### Generate report:
```bash
docker compose run --rm fund-etl report
```

### Interactive shell:
```bash
docker compose run --rm fund-etl shell
```

## Directory Structure

- `config/` - Configuration files (created on first run)
- `data/` - SQLite database and downloaded files
- `logs/` - Application logs
- `*.py` - ETL source code

## Database Schema

The `fund_data` table contains 28 columns matching the Excel structure:
- Date/Region/Fund identifiers
- Asset and portfolio values
- Yield metrics (1-day, 7-day)
- Liquidity percentages
- Risk metrics (WAM, WAL)
- And more...

## Monitoring

- Check health: `docker compose ps`
- View logs: `docker compose logs -f`
- Access UI: http://localhost:8080 (when running with --profile ui)
- Database is at `data/fund_data.db`

## Troubleshooting

### Database initialization errors:
```bash
# Check permissions
ls -la data/

# Run test suite
docker compose exec fund-etl python /app/test_fund_etl_setup.py

# Check logs
docker compose logs fund-etl | grep -i error
```

### SAP connectivity issues:
```bash
# Test connectivity
docker compose run --rm fund-etl test

# Check config
cat config/config.json
```

## Maintenance

### Update code:
```bash
docker compose build
docker compose up -d
```

### Backup database:
```bash
cp data/fund_data.db data/fund_data_backup_$(date +%Y%m%d).db
```

### Clean old logs:
```bash
find logs -name "*.log" -mtime +30 -delete
```

## Architecture

- **Main ETL**: Processes AMRS and EMEA files daily
- **Scheduler**: Manages retries and backfills
- **Monitor**: Health checks and reporting
- **UI**: Web dashboard for monitoring
- **Database**: SQLite with proper indices

## Business Rules

1. ETL runs on US business days only
2. Friday data carries over to Saturday/Sunday
3. Holiday data carries forward
4. Failed downloads trigger data carry-forward
5. Duplicate fund codes (#MULTIVALUE) are handled
