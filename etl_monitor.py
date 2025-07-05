#!/usr/bin/env python3
"""
ETL Monitor - Runs inside container to provide health metrics and monitoring
"""

import time
import json
import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path
import schedule
from fund_etl_utilities import FundDataMonitor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ETLHealthMonitor:
    """Monitor ETL health and write status for health checks"""
    
    def __init__(self):
        self.monitor = FundDataMonitor('/data/fund_data.db')
        self.health_file = Path('/data/health.json')
        
    def check_health(self):
        """Check ETL health and write status"""
        try:
            health_status = {
                'timestamp': datetime.now().isoformat(),
                'status': 'healthy',
                'checks': {}
            }
            
            # Check database connectivity
            try:
                conn = sqlite3.connect('/data/fund_data.db')
                conn.execute('SELECT COUNT(*) FROM fund_data')
                conn.close()
                health_status['checks']['database'] = 'ok'
            except Exception as e:
                health_status['checks']['database'] = f'error: {str(e)}'
                health_status['status'] = 'unhealthy'
            
            # Check recent ETL runs
            etl_status = self.monitor.get_etl_status(days=1)
            if len(etl_status) > 0:
                failed_runs = etl_status[etl_status['status'] == 'FAILED']
                if len(failed_runs) > 0:
                    health_status['checks']['recent_runs'] = f'failed: {len(failed_runs)} failures'
                    health_status['status'] = 'degraded'
                else:
                    health_status['checks']['recent_runs'] = 'ok'
            else:
                # Check if it's a weekend
                if datetime.now().weekday() in [5, 6]:
                    health_status['checks']['recent_runs'] = 'ok (weekend)'
                else:
                    health_status['checks']['recent_runs'] = 'warning: no recent runs'
                    health_status['status'] = 'degraded'
            
            # Check for missing data
            missing_dates = self.monitor.find_missing_dates(
                (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
                datetime.now().strftime('%Y-%m-%d')
            )
            
            total_missing = sum(len(dates) for dates in missing_dates.values())
            if total_missing > 3:  # Allow some missing dates
                health_status['checks']['missing_data'] = f'warning: {total_missing} missing dates'
                health_status['status'] = 'degraded'
            else:
                health_status['checks']['missing_data'] = 'ok'
            
            # Check disk space
            data_dir = Path('/data')
            free_space_mb = (data_dir.stat().st_blocks * 512) / (1024 * 1024)  # Approximate
            if free_space_mb < 100:  # Less than 100MB free
                health_status['checks']['disk_space'] = f'low: {free_space_mb:.1f}MB free'
                health_status['status'] = 'degraded'
            else:
                health_status['checks']['disk_space'] = 'ok'
            
            # Write health status
            with open(self.health_file, 'w') as f:
                json.dump(health_status, f, indent=2)
            
            logger.info(f"Health check: {health_status['status']}")
            
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
            
            # Write error status
            error_status = {
                'timestamp': datetime.now().isoformat(),
                'status': 'error',
                'error': str(e)
            }
            with open(self.health_file, 'w') as f:
                json.dump(error_status, f, indent=2)
    
    def generate_daily_summary(self):
        """Generate daily summary report"""
        try:
            logger.info("Generating daily summary...")
            
            # Get data quality report
            report = self.monitor.generate_data_quality_report()
            
            # Save to file
            report_file = Path(f'/logs/daily_report_{datetime.now().strftime("%Y%m%d")}.txt')
            with open(report_file, 'w') as f:
                f.write(report)
            
            logger.info(f"Daily summary saved to {report_file}")
            
        except Exception as e:
            logger.error(f"Failed to generate daily summary: {str(e)}")
    
    def cleanup_old_logs(self):
        """Clean up logs older than 30 days"""
        try:
            cutoff_date = datetime.now() - timedelta(days=30)
            log_dir = Path('/logs')
            
            for log_file in log_dir.glob('*.log'):
                if log_file.stat().st_mtime < cutoff_date.timestamp():
                    log_file.unlink()
                    logger.info(f"Deleted old log file: {log_file}")
                    
        except Exception as e:
            logger.error(f"Failed to cleanup logs: {str(e)}")
    
    def run(self):
        """Run the monitor"""
        logger.info("Starting ETL Health Monitor...")
        
        # Schedule tasks
        schedule.every(5).minutes.do(self.check_health)
        schedule.every().day.at("07:00").do(self.generate_daily_summary)
        schedule.every().sunday.at("00:00").do(self.cleanup_old_logs)
        
        # Run initial health check
        self.check_health()
        
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute


if __name__ == "__main__":
    monitor = ETLHealthMonitor()
    monitor.run()
