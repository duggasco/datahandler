#!/usr/bin/env python3
"""
Fund Data ETL Pipeline
Downloads daily fund data files for AMRS and EMEA regions, performs data quality checks,
and loads to SQLite database with proper date handling and holiday logic.
"""

import os
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
import logging
from typing import Dict, List, Tuple, Optional
import holidays
import json
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fund_etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FundDataETL:
    """Main ETL class for processing fund data files"""
    
    def __init__(self, config_path: str = 'config.json'):
        """Initialize ETL with configuration"""
        self.config = self._load_config(config_path)
        self.db_path = self.config.get('db_path', '/data/fund_data.db')
        self.data_dir = Path(self.config.get('data_dir', '/data'))
        self.data_dir.mkdir(exist_ok=True)
        
        # Initialize US holidays (AMRS)
        self.us_holidays = holidays.US(years=range(2020, 2030))
        
        # Expected columns based on file analysis
        self.expected_columns = [
            'Date', 'Fund Code', 'Fund Name', 'Master Class Fund Name',
            'Rating (M/S&P/F)', 'Unique Identifier', 'NASDAQ',
            'Fund Complex (Historical)', 'SubCategory Historical',
            'Domicile', 'Currency', 'Share Class Assets (dly/$mils)',
            'Portfolio Assets (dly/$mils)', '1-DSY (dly)', '1-GDSY (dly)',
            '7-DSY (dly)', '7-GDSY (dly)', 'Chgd Expense Ratio (mo/dly)',
            'WAM (dly)', 'WAL (dly)', 'Transactional NAV', 'Market NAV',
            'Daily Liquidity (%)', 'Weekly Liquidity (%)', 'Fees', 'Gates'
        ]
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"Config file {config_path} not found. Using defaults.")
            return {
                'sap_urls': {
                    'amrs': 'https://www.mfanalyzer.com/BOE/OpenDocument/opendoc/openDocument.jsp?sIDType=CUID&iDocID=AYscKsmnmVFMgwa4u8GO5GU&sOutputFormat=E',
                    'emea': 'https://www.mfanalyzer.com/BOE/OpenDocument/opendoc/openDocument.jsp?sIDType=CUID&iDocID=AXFSzkEFSQpOrrU9_35AhpQ&sOutputFormat=E'
                },
                'db_path': '/data/fund_data.db',
                'data_dir': '/data'
            }
    
    def is_business_day(self, date: datetime) -> bool:
        """Check if date is a US business day (not weekend or holiday)"""
        return date.weekday() < 5 and date not in self.us_holidays
    
    def get_prior_business_day(self, date: datetime) -> datetime:
        """Get the prior business day for a given date"""
        prior_date = date - timedelta(days=1)
        while not self.is_business_day(prior_date):
            prior_date -= timedelta(days=1)
        return prior_date
    
	def download_file(self, url: str, region: str, date: datetime) -> Optional[str]:
        """
        Download file from SAP OpenDocument URL
        Returns: Path to downloaded file or None if failed
        """
        try:
            # Try to use Selenium-based downloader first
            from sap_download_module import SAPOpenDocumentDownloader
            
            # Configure Selenium downloader
            sap_config = {
                'username': self.config.get('auth', {}).get('username', 'sduggan'),
                'password': self.config.get('auth', {}).get('password', 'sduggan'),
                'download_dir': str(self.data_dir / 'downloads'),
                'headless': True,  # Always use headless in container
                'timeout': self.config.get('download_timeout', 300)
            }
            
            downloader = SAPOpenDocumentDownloader(sap_config)
            
            logger.info(f"Downloading {region} file for {date.strftime('%Y-%m-%d')} using Selenium")
            
            try:
                # Download the file
                filepath = downloader.download_file(region.upper(), date, self.data_dir)
                
                if filepath and os.path.exists(filepath):
                    logger.info(f"Successfully downloaded {region} file: {filepath}")
                    return filepath
                else:
                    logger.error(f"Selenium download failed for {region}")
                    return None
                    
            finally:
                # Always close the browser
                downloader.close()
                
        except ImportError:
            logger.error("Selenium-based SAP download module not available")
            logger.info("Falling back to HTTP download (likely to fail)")
            
            # Fallback to basic HTTP download (kept for compatibility)
            try:
                import requests
                from requests.auth import HTTPBasicAuth
                
                auth = None
                if self.config.get('auth'):
                    auth = HTTPBasicAuth(
                        self.config['auth']['username'],
                        self.config['auth']['password']
                    )
                
                response = requests.get(
                    url,
                    auth=auth,
                    timeout=300,
                    verify=self.config.get('verify_ssl', True)
                )
                response.raise_for_status()
                
                filename = f"DataDump__{region.upper()}_{date.strftime('%Y%m%d')}.xlsx"
                filepath = self.data_dir / filename
                
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                
                return str(filepath)
                
            except Exception as e:
                logger.error(f"HTTP download failed for {region}: {str(e)}")
                return None
            
        except Exception as e:
            logger.error(f"Failed to download {region} file: {str(e)}")
            return None
    
    def validate_dataframe(self, df: pd.DataFrame, region: str) -> Tuple[bool, List[str]]:
        """
        Validate dataframe structure and data quality
        Returns: (is_valid, list_of_issues)
        """
        issues = []
        
        # Clean whitespace from all string columns
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        
        # Check if all expected columns are present
        missing_cols = set(self.expected_columns) - set(df.columns)
        if missing_cols:
            issues.append(f"Missing columns: {missing_cols}")
        
        # Check if dataframe is empty
        if len(df) == 0:
            issues.append("Dataframe is empty")
            return False, issues
        
        # Check for completely empty rows
        empty_rows = df.isna().all(axis=1).sum()
        if empty_rows > 0:
            issues.append(f"Found {empty_rows} completely empty rows")
            df = df.dropna(how='all')
        
        # Check for duplicate primary keys
        duplicate_keys = df.groupby(['Fund Code']).size()
        duplicates = duplicate_keys[duplicate_keys > 1]
        if len(duplicates) > 0:
            issues.append(f"Found {len(duplicates)} duplicate Fund Codes")
            # Handle special case of #MULTIVALUE fund code
            if '#MULTIVALUE' in duplicates.index:
                logger.warning("Found #MULTIVALUE fund code - assigning unique identifiers")
                multivalue_mask = df['Fund Code'] == '#MULTIVALUE'
                multivalue_count = multivalue_mask.sum()
                df.loc[multivalue_mask, 'Fund Code'] = [f'#MULTIVALUE_{i+1}' for i in range(multivalue_count)]
        
        # Check required columns for null values
        required_cols = ['Date', 'Fund Code', 'Fund Name', 'Currency']
        for col in required_cols:
            if col in df.columns:
                null_count = df[col].isna().sum()
                if null_count > 0:
                    issues.append(f"Column '{col}' has {null_count} null values")
        
        # Region-specific validation
        if region == 'EMEA':
            # EMEA typically has more missing NASDAQ values (expected)
            nasdaq_missing = df['NASDAQ'].isna().sum() / len(df)
            if nasdaq_missing < 0.9:
                logger.info(f"EMEA: {nasdaq_missing:.1%} NASDAQ values missing (expected ~100%)")
        else:  # US
            # US should have most NASDAQ values populated
            nasdaq_missing = df['NASDAQ'].isna().sum() / len(df)
            if nasdaq_missing > 0.1:
                issues.append(f"US: High proportion of missing NASDAQ values ({nasdaq_missing:.1%})")
        
        # Data quality metrics
        logger.info(f"\n{region} Data Quality Metrics:")
        logger.info(f"Total rows: {len(df)}")
        logger.info(f"Unique funds: {df['Fund Code'].nunique()}")
        logger.info(f"Date range: {df['Date'].min()} to {df['Date'].max()}")
        
        # Calculate missing data percentages for key financial columns
        financial_cols = [
            'Share Class Assets (dly/$mils)', 'Portfolio Assets (dly/$mils)',
            '1-DSY (dly)', '7-DSY (dly)', 'WAM (dly)', 'WAL (dly)'
        ]
        
        for col in financial_cols:
            if col in df.columns:
                missing_pct = (df[col].isna().sum() / len(df)) * 100
                logger.info(f"{col}: {missing_pct:.1f}% missing")
        
        return len(issues) == 0, issues
    
    def process_dates(self, df: pd.DataFrame, file_date: datetime) -> pd.DataFrame:
        """
        Process dates according to business rules:
        - Friday data carries over to Saturday and Sunday
        - Holiday data carries forward
        """
        df = df.copy()
        
        # Ensure Date column is datetime - handle different date formats
        # Both "7/01/2025" and "07/02/2025" formats should parse correctly
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        
        # Check for any date parsing errors
        if df['Date'].isna().any():
            logger.warning(f"Found {df['Date'].isna().sum()} rows with invalid dates")
            df = df.dropna(subset=['Date'])
        
        # Get the date from the data (should be prior business day)
        data_date = df['Date'].iloc[0].date()
        
        # If it's Friday data, create Saturday and Sunday copies
        if data_date.weekday() == 4:  # Friday
            logger.info(f"Processing Friday data from {data_date}")
            
            # Create Saturday data
            saturday_df = df.copy()
            saturday_df['Date'] = data_date + timedelta(days=1)
            
            # Create Sunday data  
            sunday_df = df.copy()
            sunday_df['Date'] = data_date + timedelta(days=2)
            
            # Combine all three days
            df = pd.concat([df, saturday_df, sunday_df], ignore_index=True)
            logger.info(f"Created weekend data for {data_date + timedelta(days=1)} and {data_date + timedelta(days=2)}")
        
        return df
    
    def setup_database(self):
        """Create database tables if they don't exist"""
        import os
        
        # Ensure the directory exists
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            try:
                os.makedirs(db_dir, mode=0o755, exist_ok=True)
                logger.info(f"Created directory: {db_dir}")
            except Exception as e:
                logger.error(f"Failed to create directory {db_dir}: {e}")
                raise
        
        # Check if we can write to the directory
        if db_dir and not os.access(db_dir, os.W_OK):
            logger.error(f"No write permission for directory: {db_dir}")
            raise PermissionError(f"Cannot write to directory: {db_dir}")
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create main fund data table - matching our Excel structure
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS fund_data (
                date DATE,
                region TEXT,
                fund_code TEXT,
                fund_name TEXT,
                master_class_fund_name TEXT,
                rating TEXT,
                unique_identifier TEXT,
                nasdaq TEXT,
                fund_complex TEXT,
                subcategory TEXT,
                domicile TEXT,
                currency TEXT,
                share_class_assets REAL,
                portfolio_assets REAL,
                one_day_yield REAL,
                one_day_gross_yield REAL,
                seven_day_yield REAL,
                seven_day_gross_yield REAL,
                expense_ratio REAL,
                wam REAL,
                wal REAL,
                transactional_nav TEXT,
                market_nav TEXT,
                daily_liquidity REAL,
                weekly_liquidity REAL,
                fees TEXT,
                gates TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (date, region, fund_code)
            )
            """)
            
            # Create ETL log table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS etl_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_date DATE,
                region TEXT,
                file_date DATE,
                status TEXT,
                records_processed INTEGER,
                issues TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            # Create indices for better query performance
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_fund_data_date 
            ON fund_data(date)
            """)
            
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_fund_data_region 
            ON fund_data(region)
            """)
            
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_fund_data_fund_code 
            ON fund_data(fund_code)
            """)
            
            # Verify tables were created
            cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name IN ('fund_data', 'etl_log')
            """)
            tables = cursor.fetchall()
            
            if len(tables) == 2:
                logger.info(f"Database tables verified/created at {self.db_path}")
            else:
                raise Exception("Failed to create all required tables")
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Database setup failed: {str(e)}")
            raise
    
    def load_to_database(self, df: pd.DataFrame, region: str, file_date: datetime):
        """Load processed data to SQLite database"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Prepare dataframe for loading
            df_load = df.copy()
            df_load['region'] = region
            
            # Rename columns to match database schema
            column_mapping = {
                'Date': 'date',
                'Fund Code': 'fund_code',
                'Fund Name': 'fund_name',
                'Master Class Fund Name': 'master_class_fund_name',
                'Rating (M/S&P/F)': 'rating',
                'Unique Identifier': 'unique_identifier',
                'NASDAQ': 'nasdaq',
                'Fund Complex (Historical)': 'fund_complex',
                'SubCategory Historical': 'subcategory',
                'Domicile': 'domicile',
                'Currency': 'currency',
                'Share Class Assets (dly/$mils)': 'share_class_assets',
                'Portfolio Assets (dly/$mils)': 'portfolio_assets',
                '1-DSY (dly)': 'one_day_yield',
                '1-GDSY (dly)': 'one_day_gross_yield',
                '7-DSY (dly)': 'seven_day_yield',
                '7-GDSY (dly)': 'seven_day_gross_yield',
                'Chgd Expense Ratio (mo/dly)': 'expense_ratio',
                'WAM (dly)': 'wam',
                'WAL (dly)': 'wal',
                'Transactional NAV': 'transactional_nav',
                'Market NAV': 'market_nav',
                'Daily Liquidity (%)': 'daily_liquidity',
                'Weekly Liquidity (%)': 'weekly_liquidity',
                'Fees': 'fees',
                'Gates': 'gates'
            }
            
            df_load = df_load.rename(columns=column_mapping)
            
            # Clean text fields to handle special characters
            text_columns = [
                'fund_code', 'fund_name', 'master_class_fund_name', 'rating',
                'unique_identifier', 'nasdaq', 'fund_complex', 'subcategory',
                'domicile', 'currency', 'transactional_nav', 'market_nav',
                'fees', 'gates'
            ]
            
            for col in text_columns:
                if col in df_load.columns:
                    # Replace None with empty string and strip whitespace
                    df_load[col] = df_load[col].fillna('')
                    df_load[col] = df_load[col].apply(lambda x: str(x).strip() if x else '')
            
            # Convert numeric columns
            numeric_columns = [
                'share_class_assets', 'portfolio_assets', 'one_day_yield',
                'one_day_gross_yield', 'seven_day_yield', 'seven_day_gross_yield',
                'expense_ratio', 'wam', 'wal', 'daily_liquidity', 'weekly_liquidity'
            ]
            
            for col in numeric_columns:
                if col in df_load.columns:
                    # Replace '-' with NaN before converting to numeric
                    df_load[col] = df_load[col].replace(['-', ''], np.nan)
                    df_load[col] = pd.to_numeric(df_load[col], errors='coerce')
            
            # Delete existing data for this date/region to handle updates
            cursor = conn.cursor()
            cursor.execute("""
            DELETE FROM fund_data 
            WHERE date = ? AND region = ?
            """, (df_load['date'].iloc[0], region))
            
            # Load to database (replace existing data for the date/region)
            df_load.to_sql('fund_data', conn, if_exists='append', index=False)
            
            # Log the ETL run
            cursor = conn.cursor()
            cursor.execute("""
            INSERT INTO etl_log (run_date, region, file_date, status, records_processed)
            VALUES (?, ?, ?, ?, ?)
            """, (datetime.now().date(), region, file_date.date(), 'SUCCESS', len(df)))
            
            conn.commit()
            logger.info(f"Successfully loaded {len(df)} records for {region}")
            
        except Exception as e:
            logger.error(f"Database load failed: {str(e)}")
            raise
        finally:
            conn.close()
    
    def carry_forward_data(self, date: datetime, region: str):
        """Carry forward previous day's data when no new file is available"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Find the most recent data for this region
            query = """
            SELECT DISTINCT date FROM fund_data 
            WHERE region = ? AND date < ?
            ORDER BY date DESC LIMIT 1
            """
            
            cursor = conn.cursor()
            result = cursor.execute(query, (region, date.strftime('%Y-%m-%d'))).fetchone()
            
            if result:
                source_date = result[0]
                logger.info(f"Carrying forward {region} data from {source_date} to {date.strftime('%Y-%m-%d')}")
                
                # Copy data with new date
                cursor.execute("""
                INSERT INTO fund_data 
                SELECT ?, region, fund_code, fund_name, master_class_fund_name,
                       rating, unique_identifier, nasdaq, fund_complex, subcategory,
                       domicile, currency, share_class_assets, portfolio_assets,
                       one_day_yield, one_day_gross_yield, seven_day_yield,
                       seven_day_gross_yield, expense_ratio, wam, wal,
                       transactional_nav, market_nav, daily_liquidity,
                       weekly_liquidity, fees, gates, CURRENT_TIMESTAMP
                FROM fund_data
                WHERE date = ? AND region = ?
                """, (date.strftime('%Y-%m-%d'), source_date, region))
                
                records = cursor.rowcount
                
                # Log the carry forward
                cursor.execute("""
                INSERT INTO etl_log (run_date, region, file_date, status, records_processed, issues)
                VALUES (?, ?, ?, ?, ?, ?)
                """, (datetime.now().date(), region, date.date(), 'CARRIED_FORWARD', 
                      records, f'Data carried forward from {source_date}'))
                
                conn.commit()
                logger.info(f"Carried forward {records} records")
            else:
                logger.warning(f"No previous data found for {region} to carry forward")
                
        except Exception as e:
            logger.error(f"Failed to carry forward data: {str(e)}")
            raise
        finally:
            conn.close()
    
    def run_daily_etl(self, run_date: Optional[datetime] = None):
        """
        Main ETL process - runs for a specific date or current date
        """
        if run_date is None:
            run_date = datetime.now()
        
        logger.info(f"Starting ETL process for {run_date.strftime('%Y-%m-%d')}")
        
        # Check if it's a business day for AMRS
        if not self.is_business_day(run_date):
            logger.info(f"{run_date.strftime('%Y-%m-%d')} is not a US business day")
            
            # For weekends and holidays, carry forward previous data
            for region in ['AMRS', 'EMEA']:
                self.carry_forward_data(run_date, region)
            return
        
        # Get prior business day (files contain prior day's data)
        data_date = self.get_prior_business_day(run_date)
        logger.info(f"Processing data for {data_date.strftime('%Y-%m-%d')}")
        
        # Process each region
        for region in ['AMRS', 'EMEA']:
            try:
                # Download the file using SAP OpenDocument
                filepath = self.download_file(
                    self.config['sap_urls'][region.lower()],
                    region,
                    data_date
                )
                
                if not filepath or not os.path.exists(filepath):
                    logger.warning(f"No file available for {region}, carrying forward data")
                    self.carry_forward_data(run_date, region)
                    continue
                
                # Read the Excel file
                logger.info(f"Reading {region} file: {filepath}")
                df = pd.read_excel(filepath)
                
                # Validate data
                is_valid, issues = self.validate_dataframe(df, region)
                if not is_valid:
                    logger.error(f"Validation failed for {region}: {issues}")
                    continue
                elif issues:
                    logger.warning(f"Validation warnings for {region}: {issues}")
                
                # Process dates (handle Friday -> weekend logic)
                df = self.process_dates(df, data_date)
                
                # Load to database
                self.load_to_database(df, region, data_date)
                
            except Exception as e:
                logger.error(f"ETL failed for {region}: {str(e)}")
                
                # Log failure
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute("""
                INSERT INTO etl_log (run_date, region, file_date, status, issues)
                VALUES (?, ?, ?, ?, ?)
                """, (datetime.now().date(), region, data_date.date(), 'FAILED', str(e)))
                conn.commit()
                conn.close()
        
        logger.info("ETL process completed")


# Configuration file template - FIXED WITH ABSOLUTE PATHS
CONFIG_TEMPLATE = {
    "sap_urls": {
        "amrs": "https://www.mfanalyzer.com/BOE/OpenDocument/opendoc/openDocument.jsp?sIDType=CUID&iDocID=AYscKsmnmVFMgwa4u8GO5GU&sOutputFormat=E",
        "emea": "https://www.mfanalyzer.com/BOE/OpenDocument/opendoc/openDocument.jsp?sIDType=CUID&iDocID=AXFSzkEFSQpOrrU9_35AhpQ&sOutputFormat=E"
    },
    "auth": {
        "username": "sduggan",
        "password": "sduggan"
    },
    "db_path": "/data/fund_data.db",
    "data_dir": "/data",
    "download_timeout": 300,
    "verify_ssl": True,
    "email_alerts": {
        "enabled": True,
        "recipients": ["etl-team@company.com"],
        "smtp_server": "smtp.company.com"
    }
}


def create_config_template():
    """Create a template configuration file"""
    with open('config_template.json', 'w') as f:
        json.dump(CONFIG_TEMPLATE, f, indent=2)
    print("Created config_template.json - please update with your settings")


if __name__ == "__main__":
    # Create config template if needed
    if not os.path.exists('config.json') and not os.path.exists('config_template.json'):
        create_config_template()
    
    # Initialize ETL
    etl = FundDataETL()
    
    # Setup database
    etl.setup_database()
    
    # Run daily ETL
    etl.run_daily_etl()
    
    # Example: Run for a specific date
    # etl.run_daily_etl(datetime(2025, 7, 3))
