#!/usr/bin/env python3
"""
Fund ETL Scheduler
Orchestrates daily ETL runs with error handling, notifications, and recovery
Now supports selective vs full validation modes
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json
import traceback
from pathlib import Path
from typing import Optional
import fcntl
import tempfile

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from fund_etl_pipeline import FundDataETL
from fund_etl_utilities import FundDataMonitor


class ETLScheduler:
    """Orchestrates ETL runs with monitoring and alerting"""
    
    def __init__(self, config_path: str = '/config/scheduler_config.json'):
        # Setup basic logging first to avoid AttributeError
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        self.config = self._load_config(config_path)
        self.etl = FundDataETL(self.config.get('etl_config_path', '/config/config.json'))
        self.monitor = FundDataMonitor(self.etl.db_path)
        
        # Setup locking to prevent concurrent executions
        self.lock_file_path = os.path.join(tempfile.gettempdir(), 'fund_etl_scheduler.lock')
        self.lock_file = None
        
        # Setup logging with proper directory
        log_dir = Path(self.config.get('log_dir', '/logs'))
        log_dir.mkdir(exist_ok=True)
        
        log_file = log_dir / f"etl_scheduler_{datetime.now().strftime('%Y%m%d')}.log"
        
        # Reconfigure logging with file handler
        logging.getLogger().handlers = []  # Clear existing handlers
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ],
            force=True  # Force reconfiguration
        )
        self.logger = logging.getLogger(__name__)
    
    def acquire_lock(self):
        """Acquire exclusive lock to prevent concurrent ETL runs"""
        try:
            self.lock_file = open(self.lock_file_path, 'w')
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.lock_file.write(str(os.getpid()))
            self.lock_file.flush()
            return True
        except (IOError, OSError) as e:
            if self.lock_file:
                self.lock_file.close()
                self.lock_file = None
            self.logger.error(f"Failed to acquire lock: {e}")
            return False
    
    def release_lock(self):
        """Release exclusive lock"""
        if self.lock_file:
            try:
                fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
                self.lock_file.close()
                os.remove(self.lock_file_path)
            except (IOError, OSError) as e:
                self.logger.error(f"Failed to release lock: {e}")
            finally:
                self.lock_file = None
    
    def _load_config(self, config_path: str) -> dict:
        """Load scheduler configuration"""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.warning(f"Config file {config_path} not found. Using defaults.")
            return {
                'email_alerts': {
                    'enabled': False,
                    'smtp_server': 'smtp.gmail.com',
                    'smtp_port': 587,
                    'from_email': 'etl@company.com',
                    'to_emails': ['admin@company.com'],
                    'use_tls': True
                },
                'retry_config': {
                    'max_retries': 3,
                    'retry_delay_minutes': 30
                },
                'backfill_days': 7,
                'log_dir': '/logs',
                'etl_config_path': '/config/config.json'
            }
    
    def send_email_alert(self, subject: str, body: str, is_error: bool = False):
        """Send email notification"""
        if not self.config.get('email_alerts', {}).get('enabled', False):
            return
        
        try:
            email_config = self.config['email_alerts']
            
            msg = MIMEMultipart()
            msg['From'] = email_config['from_email']
            msg['To'] = ', '.join(email_config['to_emails'])
            msg['Subject'] = f"[{'ERROR' if is_error else 'INFO'}] ETL Alert: {subject}"
            
            msg.attach(MIMEText(body, 'plain'))
            
            with smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port']) as server:
                if email_config.get('use_tls', True):
                    server.starttls()
                
                if email_config.get('username') and email_config.get('password'):
                    server.login(email_config['username'], email_config['password'])
                
                server.send_message(msg)
                
            self.logger.info(f"Email alert sent: {subject}")
            
        except Exception as e:
            self.logger.error(f"Failed to send email alert: {str(e)}")
    
    def run_with_retry(self, run_date: datetime) -> bool:
        """Run ETL with retry logic"""
        max_retries = self.config.get('retry_config', {}).get('max_retries', 3)
        retry_delay = self.config.get('retry_config', {}).get('retry_delay_minutes', 30)
        
        for attempt in range(max_retries):
            try:
                self.logger.info(f"ETL attempt {attempt + 1} of {max_retries}")
                
                # Run the ETL - now returns dict with validation info
                result = self.etl.run_daily_etl(run_date)
                
                # Check if ETL returned a result dict (new behavior)
                if isinstance(result, dict) and result.get('success'):
                    # Send validation alerts if any
                    if result.get('validation_alerts'):
                        alert_body = "ETL completed successfully with validation updates:"
                        alert_body += "".join(result['validation_alerts'])
                        alert_body += "Database has been updated with corrected data."
                        
                        self.send_email_alert(
                            f"ETL Validation Updates - {run_date.strftime('%Y-%m-%d')}",
                            alert_body,
                            is_error=False
                        )
                    
                    self.logger.info("ETL completed successfully")
                    return True
                
                # Legacy behavior - if ETL doesn't return a dict
                # Verify the run was successful
                etl_status = self.monitor.get_etl_status(days=1)
                
                failed_regions = []
                for _, run in etl_status.iterrows():
                    if run['run_date'] == run_date.date() and run['status'] == 'FAILED':
                        failed_regions.append(run['region'])
                
                if failed_regions:
                    raise Exception(f"ETL failed for regions: {', '.join(failed_regions)}")
                
                self.logger.info("ETL completed successfully")
                return True
                
            except Exception as e:
                self.logger.error(f"ETL attempt {attempt + 1} failed: {str(e)}")
                
                if attempt < max_retries - 1:
                    self.logger.info(f"Waiting {retry_delay} minutes before retry...")
                    import time
                    time.sleep(retry_delay * 60)
                else:
                    # Final attempt failed
                    error_msg = f"ETL failed after {max_retries} attempts"
                    error_msg += f"Error: {str(e)}"
                    error_msg += f"Traceback:{traceback.format_exc()}"
                    
                    self.send_email_alert(
                        f"ETL Failed - {run_date.strftime('%Y-%m-%d')}",
                        error_msg,
                        is_error=True
                    )
                    return False
        
        return False

    def backfill_missing_dates(self, days: int = None):
        """Backfill any missing dates"""
        if days is None:
            days = self.config.get('backfill_days', 7)
        
        self.logger.info(f"Checking for missing data in the last {days} days")
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        missing_dates = self.monitor.find_missing_dates(
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        dates_to_backfill = set()
        for region, dates in missing_dates.items():
            for date_str in dates:
                dates_to_backfill.add(datetime.strptime(date_str, '%Y-%m-%d'))
        
        if dates_to_backfill:
            self.logger.info(f"Found {len(dates_to_backfill)} dates to backfill")
            
            for date in sorted(dates_to_backfill):
                self.logger.info(f"Backfilling data for {date.strftime('%Y-%m-%d')}")
                self.run_with_retry(date)
        else:
            self.logger.info("No missing dates found")
    
    def run_daily_schedule(self):
        """Main scheduling function for daily runs"""
        if not self.acquire_lock():
            self.logger.error("Another ETL process is already running. Skipping this run.")
            return False
        
        try:
            run_date = datetime.now()
            
            self.logger.info("="*60)
            self.logger.info(f"Starting scheduled ETL run for {run_date.strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info("="*60)
            
            # Run today's ETL
            success = self.run_with_retry(run_date)
            
            if success:
                # Generate and send daily report
                report = self.monitor.generate_data_quality_report()
                
                self.send_email_alert(
                    f"ETL Success - {run_date.strftime('%Y-%m-%d')}",
                    report,
                    is_error=False
                )
                
                # Check for and backfill any missing recent dates
                self.backfill_missing_dates()
            
            self.logger.info("Scheduled ETL run completed")
            
            return success
        finally:
            self.release_lock()
    
    def run_date_schedule(self, target_date: datetime):
        """Run ETL for a specific date with locking"""
        if not self.acquire_lock():
            self.logger.error("Another ETL process is already running. Skipping this run.")
            return False
        
        try:
            self.logger.info("="*60)
            self.logger.info(f"Starting ETL run for {target_date.strftime('%Y-%m-%d')}")
            self.logger.info("="*60)
            
            # Run ETL for the specific date
            success = self.run_with_retry(target_date)
            
            if success:
                self.logger.info(f"ETL run completed successfully for {target_date.strftime('%Y-%m-%d')}")
            else:
                self.logger.error(f"ETL run failed for {target_date.strftime('%Y-%m-%d')}")
            
            return success
        finally:
            self.release_lock()
    
    def run_historical_load(self, start_date: str, end_date: str):
        """Load historical data for a date range"""
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        self.logger.info(f"Running historical load from {start_date} to {end_date}")
        
        current = start
        success_count = 0
        total_count = 0
        
        while current <= end:
            if self.etl.is_business_day(current):
                total_count += 1
                self.logger.info(f"Processing {current.strftime('%Y-%m-%d')}")
                
                if self.run_with_retry(current):
                    success_count += 1
                
            current += timedelta(days=1)
        
        self.logger.info(f"Historical load complete: {success_count}/{total_count} successful")

    def run_validation(self, update_mode: Optional[str] = None):
        """
        Run 30-day lookback validation with configurable update mode
        
        Args:
            update_mode: 'selective' or 'full' - overrides config setting
        """
        if not self.acquire_lock():
            print("Another ETL process is already running. Skipping validation.")
            return False
        
        try:
            print("Running 30-day lookback validation...")
            
            # Set logging to DEBUG for validation to see detailed comparison info
            logging.getLogger('fund_etl_pipeline').setLevel(logging.DEBUG)
            
            # Check environment variable for update mode override
            if update_mode is None:
                update_mode = os.environ.get('VALIDATION_MODE', None)
            
            validation_summary = []
            
            for region in ['AMRS', 'EMEA']:
                print(f"\nValidating {region}...")
                try:
                    lookback_df = self.etl.download_lookback_file(region)
                    if lookback_df is not None:
                        results = self.etl.validate_against_lookback(region, lookback_df)
                        
                        # Display validation results
                        print(f"Total records in lookback file: {len(lookback_df)}")
                        print(f"Missing dates: {results['summary']['missing_dates_count']}")
                        print(f"Changed records: {results['summary']['changed_records_count']}")
                        
                        if results['summary']['requires_update']:
                            # Determine update mode
                            mode = update_mode or self.etl.config.get('validation', {}).get('update_mode', 'selective')
                            print(f"Updating database with corrected data using {mode} mode...")
                            
                            # Update with specified mode
                            self.etl.update_from_lookback(region, lookback_df, results, update_mode=mode)
                            
                            # Create summary message
                            if mode == 'selective':
                                # Count actual changes made
                                updated_count = len([c for c in results['changed_records'] if c['type'] == 'value_change'])
                                inserted_count = len([c for c in results['changed_records'] if c['type'] == 'new_fund'])
                                validation_summary.append(
                                    f"{region}: {mode.capitalize()} update - "
                                    f"{results['summary']['missing_dates_count']} missing dates added, "
                                    f"{updated_count} records updated, {inserted_count} new funds added"
                                )
                            else:
                                validation_summary.append(
                                    f"{region}: Full replacement - "
                                    f"{results['summary']['missing_dates_count']} missing dates, "
                                    f"{results['summary']['changed_records_count']} changed records"
                                )
                        else:
                            validation_summary.append(f"{region}: No updates required")
                    else:
                        print(f"Failed to download lookback file for {region}")
                        validation_summary.append(f"{region}: Failed to download lookback file")
                except Exception as e:
                    print(f"Error validating {region}: {str(e)}")
                    validation_summary.append(f"{region}: Error - {str(e)}")
            
            print("\n" + "="*60)
            print("Validation Summary:")
            for summary in validation_summary:
                print(f"  - {summary}")
            print("="*60)
            
            # Generate validation report
            validation_report = self.monitor.generate_validation_report()
            print(validation_report)
            
            # Reset logging level
            logging.getLogger('fund_etl_pipeline').setLevel(logging.INFO)
            
            return True
        finally:
            self.release_lock()


# Scheduler configuration template - FIXED WITH ABSOLUTE PATHS
SCHEDULER_CONFIG_TEMPLATE = {
    "etl_config_path": "/config/config.json",
    "email_alerts": {
        "enabled": True,
        "smtp_server": "smtp.gmail.com",
        "smtp_port": 587,
        "from_email": "etl-alerts@company.com",
        "to_emails": ["data-team@company.com", "ops@company.com"],
        "username": "etl-alerts@company.com",
        "password": "app-specific-password",
        "use_tls": True
    },
    "retry_config": {
        "max_retries": 3,
        "retry_delay_minutes": 30
    },
    "backfill_days": 7,
    "log_dir": "/logs"
}


def create_scheduler_config_template():
    """Create a template scheduler configuration file"""
    with open('scheduler_config_template.json', 'w') as f:
        json.dump(SCHEDULER_CONFIG_TEMPLATE, f, indent=2)
    print("Created scheduler_config_template.json - please update with your settings")


def setup_cron_job():
    """Print instructions for setting up cron job"""
    script_path = os.path.abspath(__file__)
    
    print("\nTo schedule this ETL to run daily, add the following to your crontab:")
    print("(Edit crontab with: crontab -e)")
    print("\n# Run Fund ETL daily at 6 AM Eastern Time")
    print(f"0 6 * * * /usr/bin/python3 {script_path} --run-daily >> /var/log/fund_etl_cron.log 2>&1")
    print("\nFor Windows Task Scheduler, create a task that runs:")
    print(f"python {script_path} --run-daily")


def main():
    """Main entry point with CLI arguments"""
    parser = argparse.ArgumentParser(description='Fund Data ETL Scheduler')
    parser.add_argument('--run-daily', action='store_true',
                       help='Run the daily ETL schedule')
    parser.add_argument('--backfill', type=int, metavar='DAYS',
                       help='Backfill missing data for the last N days')
    parser.add_argument('--historical', nargs=2, metavar=('START_DATE', 'END_DATE'),
                       help='Load historical data for date range (YYYY-MM-DD format)')
    parser.add_argument('--create-config', action='store_true',
                       help='Create configuration file templates')
    parser.add_argument('--setup-cron', action='store_true',
                       help='Show cron setup instructions')
    parser.add_argument('--validate', action='store_true',
                       help='Run 30-day lookback validation with selective updates')
    parser.add_argument('--validate-full', action='store_true',
                       help='Run 30-day lookback validation with full replacement')
    parser.add_argument('--update-mode', choices=['selective', 'full'],
                       help='Override validation update mode')
    parser.add_argument('--run-date', metavar='DATE',
                       help='Run ETL for a specific date (YYYY-MM-DD format)')
    
    args = parser.parse_args()
    
    if args.create_config:
        create_scheduler_config_template()
        from fund_etl_pipeline import create_config_template
        create_config_template()
        return
    
    if args.setup_cron:
        setup_cron_job()
        return
    
    # Initialize scheduler
    scheduler = ETLScheduler()
    
    if args.run_daily:
        success = scheduler.run_daily_schedule()
        sys.exit(0 if success else 1)
    
    elif args.backfill:
        scheduler.backfill_missing_dates(args.backfill)
    
    elif args.historical:
        scheduler.run_historical_load(args.historical[0], args.historical[1])
    
    elif args.validate:
        # Default validation with selective mode
        scheduler.run_validation(update_mode=args.update_mode or 'selective')
    
    elif args.validate_full:
        # Full replacement validation
        scheduler.run_validation(update_mode='full')
    
    elif args.run_date:
        # Run ETL for specific date
        try:
            target_date = datetime.strptime(args.run_date, '%Y-%m-%d')
            success = scheduler.run_date_schedule(target_date)
            sys.exit(0 if success else 1)
        except ValueError:
            print(f"Error: Invalid date format '{args.run_date}'. Use YYYY-MM-DD format.")
            sys.exit(1)
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()