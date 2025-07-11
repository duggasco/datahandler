#!/usr/bin/env python3
"""
Fund Data ETL Pipeline
Downloads daily fund data files for AMRS and EMEA regions, performs data quality checks,
and loads to SQLite database with proper date handling and holiday logic.
Now includes selective update capability for validation to only update materially changed records.
"""

import os
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
import logging
from typing import Dict, List, Tuple, Optional, Any
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

# Check if LOG_LEVEL environment variable is set
log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
if log_level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
    logging.getLogger().setLevel(getattr(logging, log_level))

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
                'timeout': self.config.get('download_timeout', 300),
                'lookback_timeout': self.config.get('lookback_timeout', 600),
                'sap_urls': self.config.get('sap_urls', {})
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
                    timeout=self.config.get('download_timeout', 300),
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
        
        # Check for duplicate primary keys (now that #MULTIVALUE has been handled)
        duplicate_keys = df.groupby(['Fund Code']).size()
        duplicates = duplicate_keys[duplicate_keys > 1]
        
        if len(duplicates) > 0:
            issues.append(f"Found {len(duplicates)} duplicate Fund Codes")
        
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
            saturday_df['Date'] = pd.to_datetime(data_date + timedelta(days=1))
            
            # Create Sunday data  
            sunday_df = df.copy()
            sunday_df['Date'] = pd.to_datetime(data_date + timedelta(days=2))
            
            # Combine all three days
            df = pd.concat([df, saturday_df, sunday_df], ignore_index=True)
            logger.info(f"Created weekend data for {data_date + timedelta(days=1)} and {data_date + timedelta(days=2)}")
        
        return df
    
    def initialize_tables(self):
        """Create database tables if they don't exist (alias for setup_database)"""
        return self.setup_database()
    
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
                download_time REAL,
                processing_time REAL,
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
    
    def transform_data(self, df: pd.DataFrame, region: str, date: datetime) -> pd.DataFrame:
        """Transform raw Excel data into database-ready format
        
        Args:
            df: Raw DataFrame from Excel file
            region: Region code (AMRS or EMEA)
            date: Date for the data (may differ from file date for weekends)
            
        Returns:
            Transformed DataFrame ready for database insertion
        """
        # Create a copy to avoid modifying the original
        df_transformed = df.copy()
        
        # Add region and date columns
        df_transformed['region'] = region
        df_transformed['date'] = date.strftime('%Y-%m-%d')
        
        # Add file_date for tracking (date from the actual file)
        if 'Date' in df_transformed.columns:
            df_transformed['file_date'] = pd.to_datetime(df_transformed['Date']).dt.strftime('%Y-%m-%d')
            # Drop the original Date column to avoid duplication
            df_transformed = df_transformed.drop('Date', axis=1)
        
        # Filter out #MULTIVALUE rows
        if 'Fund Code' in df_transformed.columns:
            multivalue_mask = df_transformed['Fund Code'] == '#MULTIVALUE'
            if multivalue_mask.sum() > 0:
                logger.info(f"Filtering out {multivalue_mask.sum()} #MULTIVALUE records")
                df_transformed = df_transformed[~multivalue_mask]
        
        # CRITICAL: Ensure Fund Code is string before transformation
        if 'Fund Code' in df_transformed.columns:
            df_transformed['Fund Code'] = df_transformed['Fund Code'].astype(str).str.strip()
        
        # Rename columns to match database schema
        column_mapping = {
            # 'Date' is handled separately above
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
        
        df_transformed = df_transformed.rename(columns=column_mapping)
        
        # Clean text fields to handle special characters
        text_columns = [
            'fund_code', 'fund_name', 'master_class_fund_name', 'rating',
            'unique_identifier', 'nasdaq', 'fund_complex', 'subcategory',
            'domicile', 'currency', 'transactional_nav', 'market_nav',
            'fees', 'gates'
        ]
        
        for col in text_columns:
            if col in df_transformed.columns:
                # Replace None with empty string and strip whitespace
                df_transformed[col] = df_transformed[col].fillna('')
                df_transformed[col] = df_transformed[col].apply(lambda x: str(x).strip() if x else '')
        
        # Convert numeric columns
        numeric_columns = [
            'share_class_assets', 'portfolio_assets', 'one_day_yield',
            'one_day_gross_yield', 'seven_day_yield', 'seven_day_gross_yield',
            'expense_ratio', 'wam', 'wal', 'daily_liquidity', 'weekly_liquidity'
        ]
        
        for col in numeric_columns:
            if col in df_transformed.columns:
                # First convert to string to handle any non-string values
                df_transformed[col] = df_transformed[col].astype(str)
                # Remove commas from numeric strings
                df_transformed[col] = df_transformed[col].str.replace(',', '')
                # Replace '-', 'N/A', and empty strings with NaN before converting to numeric
                df_transformed[col] = df_transformed[col].replace(['-', '', 'N/A', 'nan'], np.nan)
                df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')
        
        # Ensure date column is in the correct format
        if 'date' in df_transformed.columns:
            df_transformed['date'] = date.strftime('%Y-%m-%d')
        
        return df_transformed
    
    def load_to_database(self, df: pd.DataFrame, region: str, file_date: datetime, conn=None):
        """Load processed data to SQLite database"""
        close_conn = False  # Initialize first to prevent NameError
        
        if conn is None:
            conn = sqlite3.connect(self.db_path)
            close_conn = True
        
        try:
            # Transform the data using the new method
            df_load = self.transform_data(df, region, file_date)
            
            # Delete existing data for this date/region to handle updates
            cursor = conn.cursor()
            
            # Log what we're about to do
            logger.debug(f"About to load {len(df_load)} records for {region} on {df_load['date'].iloc[0]}")
            logger.debug(f"Unique fund codes in data: {df_load['fund_code'].nunique()}")
            
            cursor.execute("""
            DELETE FROM fund_data 
            WHERE date = ? AND region = ?
            """, (df_load['date'].iloc[0], region))
            
            # If this is Friday data, also delete weekend dates to avoid duplicates
            if len(df_load) > 0:
                first_date = pd.to_datetime(df_load['date'].iloc[0])
                if first_date.weekday() == 4:  # Friday
                    # Also delete Saturday and Sunday
                    saturday = (first_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                    sunday = (first_date + pd.Timedelta(days=2)).strftime('%Y-%m-%d')
                    cursor.execute("""
                    DELETE FROM fund_data
                    WHERE date IN (?, ?) AND region = ?
                    """, (saturday, sunday, region))
            
            # Load to database (replace existing data for the date/region)
            try:
                df_load.to_sql('fund_data', conn, if_exists='append', index=False)
            except sqlite3.IntegrityError as e:
                # During lookback updates, duplicates are expected for weekend dates
                logger.warning(f"Ignoring duplicate entries during update: {str(e)}")
            
            # Log the ETL run
            cursor.execute("""
            INSERT INTO etl_log (run_date, region, file_date, status, records_processed)
            VALUES (?, ?, ?, ?, ?)
            """, (datetime.now().strftime('%Y-%m-%d'), region, file_date.strftime('%Y-%m-%d'), 'SUCCESS', len(df)))
            
            conn.commit()
            logger.info(f"Successfully loaded {len(df)} records for {region}")
            
        except Exception as e:
            logger.error(f"Database load failed: {str(e)}")
            raise
        finally:
            if close_conn:
                conn.close()
    
    def _handle_multivalue_funds(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle #MULTIVALUE fund codes by assigning unique identifiers"""
        df = df.copy()
        
        # Check if there are any #MULTIVALUE fund codes
        if 'Fund Code' in df.columns:
            # Log initial state
            logger.debug(f"_handle_multivalue_funds: Processing {len(df)} records")
            unique_codes_before = df['Fund Code'].nunique()
            
            multivalue_mask = df['Fund Code'] == '#MULTIVALUE'
            multivalue_count = multivalue_mask.sum()
            
            if multivalue_count > 0:
                logger.info(f"Found {multivalue_count} #MULTIVALUE fund codes - assigning unique identifiers")
                # Assign unique identifiers to each #MULTIVALUE record
                df.loc[multivalue_mask, 'Fund Code'] = [f'#MULTIVALUE_{i+1}' for i in range(multivalue_count)]
                
                # Log results
                unique_codes_after = df['Fund Code'].nunique()
                logger.info(f"Fund codes: {unique_codes_before} unique before -> {unique_codes_after} unique after")
        
        return df
    
    def carry_forward_data(self, date: datetime, region: str):
        """Carry forward previous day's data when no new file is available"""
        
        try:
            conn = sqlite3.connect(self.db_path)
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
                
                # First, delete any existing data for the target date to avoid conflicts
                cursor.execute("""
                DELETE FROM fund_data 
                WHERE date = ? AND region = ?
                """, (date.strftime('%Y-%m-%d'), region))
                
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
                """, (datetime.now().strftime('%Y-%m-%d'), region, date.strftime('%Y-%m-%d'), 'CARRIED_FORWARD', 
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
    
    def get_lookback_file_path(self, region: str, date: datetime) -> Path:
        """Get the path for a lookback file"""
        lookback_dir = self.data_dir / 'lookback'
        lookback_dir.mkdir(exist_ok=True)
        filename = f"DataDump__{region.upper()}_30DAYS_{date.strftime('%Y%m%d')}.xlsx"
        return lookback_dir / filename
    
    def download_lookback_file(self, region: str, lookback_days: int = 30) -> Optional[pd.DataFrame]:
        """Download and return 30-day lookback file for validation"""
        try:
            lookback_url_key = f"{region.lower()}_30days"
            if lookback_url_key not in self.config.get('sap_urls', {}):
                logger.warning(f"No lookback URL configured for {region}")
                return None
                
            url = self.config['sap_urls'][lookback_url_key]
            
            # Download using existing SAP module
            from sap_download_module import SAPOpenDocumentDownloader
            
            # Fixed configuration without duplicates
            sap_config = {
                'username': self.config.get('auth', {}).get('username', 'sduggan'),
                'password': self.config.get('auth', {}).get('password', 'sduggan'),
                'download_dir': str(self.data_dir / 'lookback'),
                'headless': True,
                'timeout': self.config.get('download_timeout', 300),
                'lookback_timeout': self.config.get('lookback_timeout', 600),
                'sap_urls': self.config.get('sap_urls', {})
            }
            
            downloader = SAPOpenDocumentDownloader(sap_config)
            
            try:
                # Create lookback directory
                lookback_dir = self.data_dir / 'lookback'
                lookback_dir.mkdir(exist_ok=True)
                
                # Log the extended timeout being used
                logger.info(f"Downloading {region} lookback file with extended timeout of {sap_config['lookback_timeout']} seconds")
                
                filepath = downloader.download_file(
                    f"{region.upper()}_30DAYS", 
                    datetime.now(), 
                    lookback_dir
                )
                
                if filepath and os.path.exists(filepath):
                    df = pd.read_excel(filepath)
                    # Add region column to the lookback data
                    df['Region'] = region
                    logger.info(f"Downloaded {region} lookback file with {len(df)} records, assigned Region={region}")
                    return df
                else:
                    logger.error(f"Failed to download {region} lookback file")
                    return None
                    
            finally:
                downloader.close()
                
        except Exception as e:
            logger.error(f"Error downloading lookback file for {region}: {str(e)}")
            return None

    def validate_against_lookback(self, region: str, lookback_df: pd.DataFrame) -> Dict[str, Any]:
        """Validate database data against 30-day lookback file"""
        conn = sqlite3.connect(self.db_path)  # Open connection
        
        validation_results = {
            'missing_dates': [],
            'changed_records': [],
            'summary': {
                'total_dates_checked': 0,
                'missing_dates_count': 0,
                'changed_records_count': 0,
                'requires_update': False
            }
        }
        
        try:
            # Clean and prepare lookback data
            lookback_df['Date'] = pd.to_datetime(lookback_df['Date'], errors='coerce')
            
            # CRITICAL FIX: Ensure fund codes are strings in both datasets
            lookback_df['Fund Code'] = lookback_df['Fund Code'].astype(str).str.strip()
            
            # Get unique dates from lookback file
            lookback_dates = lookback_df['Date'].dt.date.unique()
            validation_results['summary']['total_dates_checked'] = len(lookback_dates)
            
            logger.info(f"Validating {region} against {len(lookback_dates)} dates from lookback file")
            
            # Check for missing dates in database
            for date in lookback_dates:
                date_str = date.strftime('%Y-%m-%d')
                check_query = f"""
                SELECT COUNT(*) as count FROM fund_data 
                WHERE date = '{date_str}' AND region = '{region}'
                """
                result = pd.read_sql_query(check_query, conn)
                
                if result.iloc[0]['count'] == 0:
                    validation_results['missing_dates'].append(date_str)
                    validation_results['summary']['missing_dates_count'] += 1
                    validation_results['summary']['requires_update'] = True
            
            # Get validation configuration
            change_threshold = self.config.get('validation', {}).get('change_threshold_percent', 5.0)
            critical_fields = self.config.get('validation', {}).get('critical_fields', 
                ['share_class_assets', 'portfolio_assets', 'one_day_yield', 'seven_day_yield'])
            
            logger.info(f"Using change threshold: {change_threshold}% for fields: {critical_fields}")
            
            # Check for changed records
            total_comparisons = 0
            for date in lookback_dates:
                if date.strftime('%Y-%m-%d') not in validation_results['missing_dates']:
                    # Get data from database for this date
                    db_query = f"""
                    SELECT * FROM fund_data 
                    WHERE date = '{date.strftime('%Y-%m-%d')}' 
                    AND region = '{region}'
                    """
                    db_data = pd.read_sql_query(db_query, conn)
                    
                    # CRITICAL FIX: Ensure database fund codes are also strings
                    db_data['fund_code'] = db_data['fund_code'].astype(str).str.strip()
                    
                    # Get lookback data for this date
                    lookback_date_data = lookback_df[lookback_df['Date'].dt.date == date].copy()
                    
                    # If lookback data has a region column, filter by it
                    if 'Region' in lookback_date_data.columns:
                        lookback_date_data = lookback_date_data[lookback_date_data['Region'] == region]
                        logger.debug(f"Filtered lookback data for {region}: {len(lookback_date_data)} records")
                    elif 'region' in lookback_date_data.columns:
                        lookback_date_data = lookback_date_data[lookback_date_data['region'] == region]
                        logger.debug(f"Filtered lookback data for {region}: {len(lookback_date_data)} records")
                    else:
                        # Log warning if no region column found - lookback file should be region-specific
                        if len(lookback_date_data) > 0:
                            logger.warning(f"No region column found in lookback data. Processing {len(lookback_date_data)} records for {region}. "
                                         f"Ensure the lookback file is specific to {region} region.")
                    
                    total_comparisons += len(lookback_date_data)
                    
                    # Compare records
                    changes = self._compare_dataframes(
                        db_data, lookback_date_data, 
                        critical_fields, change_threshold
                    )
                    
                    if changes:
                        validation_results['changed_records'].extend(changes)
                        validation_results['summary']['changed_records_count'] += len(changes)
                        validation_results['summary']['requires_update'] = True
            
            # Log validation summary
            logger.info(f"Validation summary for {region}:")
            logger.info(f"  - Total records compared: {total_comparisons}")
            logger.info(f"  - Missing dates: {validation_results['summary']['missing_dates_count']}")
            logger.info(f"  - Records with changes: {validation_results['summary']['changed_records_count']}")
            
            # Log sample of changes for debugging
            if validation_results['changed_records']:
                logger.debug("Sample of detected changes:")
                for i, change in enumerate(validation_results['changed_records'][:3]):
                    if change['type'] == 'value_change':
                        logger.debug(f"  {i+1}. Fund {change['fund_code']} on {change['date']}:")
                        for field_change in change['changed_fields']:
                            logger.debug(f"     - {field_change['field']}: "
                                       f"{field_change.get('db_value')} -> {field_change.get('lookback_value')} "
                                       f"({field_change.get('pct_change', 0):.2f}% change)")
            
        except Exception as e:
            logger.error(f"Validation error for {region}: {str(e)}")
            
        finally:
            if conn:
                conn.close()
        
        return validation_results

    def _compare_dataframes(self, db_df: pd.DataFrame, lookback_df: pd.DataFrame, 
                           critical_fields: List[str], threshold_pct: float) -> List[Dict]:
        """Compare two dataframes and identify significant changes"""
        changes = []
        
        # Map database columns to Excel columns
        column_mapping_reverse = {
            'fund_code': 'Fund Code',
            'share_class_assets': 'Share Class Assets (dly/$mils)',
            'portfolio_assets': 'Portfolio Assets (dly/$mils)',
            'one_day_yield': '1-DSY (dly)',
            'seven_day_yield': '7-DSY (dly)'
        }
        
        # Add logging for debugging
        logger.debug(f"Comparing {len(lookback_df)} records from lookback file")
        
        # Process each fund in lookback data
        for _, lookback_row in lookback_df.iterrows():
            # Ensure fund code is string
            fund_code = str(lookback_row['Fund Code']).strip()
            
            # Find corresponding database record
            db_record = db_df[db_df['fund_code'] == fund_code]
            
            if len(db_record) == 0:
                # New fund not in database
                changes.append({
                    'type': 'new_fund',
                    'fund_code': fund_code,
                    'date': lookback_row['Date'].strftime('%Y-%m-%d'),
                    'details': 'Fund not found in database',
                    'lookback_row': lookback_row  # Store full row for insertion
                })
                continue
                
            db_record = db_record.iloc[0]
            
            # Track which fields have changed for this record
            changed_fields = []
            
            # Check critical fields for changes
            for db_field in critical_fields:
                if db_field in column_mapping_reverse:
                    excel_field = column_mapping_reverse[db_field]
                    
                    if excel_field in lookback_row.index:
                        db_value = db_record[db_field]
                        lookback_value = lookback_row[excel_field]
                        
                        # Convert lookback value to numeric, handling special cases
                        if pd.isna(lookback_value) or lookback_value == '-' or lookback_value == '':
                            lookback_value = None
                        else:
                            lookback_value = pd.to_numeric(lookback_value, errors='coerce')
                        
                        # Convert database value to float for comparison if not null
                        if pd.notna(db_value):
                            db_value = float(db_value)
                        
                        # Skip if both values are null
                        if pd.isna(db_value) and pd.isna(lookback_value):
                            continue
                        
                        # Flag if one is null and other isn't
                        if pd.isna(db_value) != pd.isna(lookback_value):
                            logger.debug(f"Fund {fund_code}, field {db_field}: null mismatch - "
                                       f"DB: {db_value}, Lookback: {lookback_value}")
                            changed_fields.append({
                                'field': db_field,
                                'db_value': db_value,
                                'lookback_value': lookback_value
                            })
                            continue
                        
                        # Check for material changes in numeric values
                        if not pd.isna(db_value) and not pd.isna(lookback_value):
                            # Use a small epsilon for float comparison to handle precision issues
                            epsilon = 1e-10
                            
                            # First check if values are effectively equal (handles float precision)
                            if abs(db_value - lookback_value) < epsilon:
                                continue
                            
                            # Calculate percentage change
                            if abs(db_value) > epsilon:  # db_value is not effectively zero
                                pct_change = abs((lookback_value - db_value) / db_value * 100)
                                if pct_change > threshold_pct:
                                    logger.debug(f"Fund {fund_code}, field {db_field}: "
                                               f"{pct_change:.2f}% change - "
                                               f"DB: {db_value}, Lookback: {lookback_value}")
                                    changed_fields.append({
                                        'field': db_field,
                                        'db_value': db_value,
                                        'lookback_value': lookback_value,
                                        'pct_change': pct_change
                                    })
                            elif abs(lookback_value) > epsilon:  # db_value is ~0 but lookback isn't
                                logger.debug(f"Fund {fund_code}, field {db_field}: "
                                           f"zero to non-zero change - "
                                           f"DB: {db_value}, Lookback: {lookback_value}")
                                changed_fields.append({
                                    'field': db_field,
                                    'db_value': db_value,
                                    'lookback_value': lookback_value
                                })
            
            # If any fields changed, record the change
            if changed_fields:
                changes.append({
                    'type': 'value_change',
                    'fund_code': fund_code,
                    'date': db_record['date'],
                    'changed_fields': changed_fields,
                    'lookback_row': lookback_row  # Store full row for update
                })
        
        logger.info(f"Comparison complete: {len(changes)} records with changes detected")
        return changes

    def update_from_lookback(self, region: str, lookback_df: pd.DataFrame, 
                            validation_results: Dict[str, Any], update_mode: Optional[str] = None):
        """
        Update database with data from lookback file where changes detected
        
        Args:
            region: Region to update
            lookback_df: DataFrame with lookback data
            validation_results: Results from validation
            update_mode: 'selective' (default) or 'full' - override config setting
        """
        if not validation_results['summary']['requires_update']:
            logger.info(f"No updates required for {region}")
            return
        
        # Determine update mode from config or parameter
        if update_mode is None:
            update_mode = self.config.get('validation', {}).get('update_mode', 'selective')
        
        logger.info(f"Running {update_mode} update for {region}")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Begin transaction
            cursor.execute("BEGIN TRANSACTION")
            
            updates_made = 0
            records_updated = 0
            records_inserted = 0
            
            # Process missing dates (always full insert)
            for missing_date in validation_results['missing_dates']:
                date_data = lookback_df[
                    lookback_df['Date'].dt.strftime('%Y-%m-%d') == missing_date
                ]
                
                if len(date_data) > 0:
                    logger.info(f"Adding {len(date_data)} records for missing date {missing_date}")
                    
                    # Handle #MULTIVALUE fund codes before processing
                    date_data = self._handle_multivalue_funds(date_data.copy())
                    
                    # Process and load missing date data
                    date_data_processed = self.process_dates(date_data, 
                        datetime.strptime(missing_date, '%Y-%m-%d'))
                    self.load_to_database(date_data_processed, region, 
                        datetime.strptime(missing_date, '%Y-%m-%d'), conn)
                    records_inserted += len(date_data)
            
            # Process changed records based on update mode
            if update_mode == 'selective':
                # SELECTIVE MODE: Update only specific changed records
                for change in validation_results['changed_records']:
                    if change['type'] == 'new_fund':
                        # Insert new fund
                        self._insert_single_record(conn, cursor, change['lookback_row'], 
                                                 region, change['date'])
                        records_inserted += 1
                    elif change['type'] == 'value_change':
                        # Update existing fund with only changed fields
                        self._update_single_record(conn, cursor, change['lookback_row'], 
                                                 region, change['date'], change['changed_fields'])
                        records_updated += 1
                
                updates_made = records_updated + records_inserted
                
            else:
                # FULL MODE: Replace all data for changed dates (original behavior)
                changed_dates = set()
                for change in validation_results['changed_records']:
                    changed_dates.add(change['date'])
                
                for date_str in changed_dates:
                    # Delete existing data for this date
                    cursor.execute("""
                        DELETE FROM fund_data 
                        WHERE date = ? AND region = ?
                    """, (date_str, region))
                    
                    # If this is Friday, also delete weekend dates
                    check_date = pd.to_datetime(date_str)
                    if check_date.weekday() == 4:
                        saturday = (check_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                        sunday = (check_date + pd.Timedelta(days=2)).strftime('%Y-%m-%d')
                        cursor.execute("""
                        DELETE FROM fund_data
                        WHERE date IN (?, ?) AND region = ?
                        """, (saturday, sunday, region))
                    
                    # Load new data from lookback file
                    date_data = lookback_df[
                        lookback_df['Date'].dt.strftime('%Y-%m-%d') == date_str
                    ]
                    
                    if len(date_data) > 0:
                        logger.info(f"Replacing {len(date_data)} records for {date_str}")
                        
                        # Handle #MULTIVALUE fund codes before processing
                        date_data = self._handle_multivalue_funds(date_data.copy())
                        
                        date_data_processed = self.process_dates(date_data, 
                            datetime.strptime(date_str, '%Y-%m-%d'))
                        self.load_to_database(date_data_processed, region, 
                            datetime.strptime(date_str, '%Y-%m-%d'), conn)
                        updates_made += len(date_data)
            
            # Commit transaction
            conn.commit()
            
            if update_mode == 'selective':
                logger.info(f"Successfully updated {records_updated} records and inserted "
                           f"{records_inserted} new records for {region}")
            else:
                logger.info(f"Successfully replaced {updates_made} records for {region}")
            
            # Log the update in ETL log
            update_description = (
                f"{update_mode.capitalize()} update from lookback: "
                f"{validation_results['summary']['missing_dates_count']} missing dates, "
            )
            if update_mode == 'selective':
                update_description += f"{records_updated} records updated, {records_inserted} records inserted"
            else:
                update_description += f"{validation_results['summary']['changed_records_count']} dates replaced"
            
            cursor.execute("""
                INSERT INTO etl_log (run_date, region, file_date, status, records_processed, issues)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                datetime.now().date(), 
                region, 
                datetime.now().date(), 
                'LOOKBACK_UPDATE',
                updates_made,
                update_description
            ))
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to update from lookback for {region}: {str(e)}")
            raise
        finally:
            conn.close()

    def _update_single_record(self, conn, cursor, lookback_record, region: str, date_str: str, changed_fields: List[Dict]):
        """Update a single fund record with only changed fields from lookback file"""
        
        # Log what we're updating
        fund_code = lookback_record['Fund Code']
        logger.debug(f"Updating fund {fund_code} on {date_str} with {len(changed_fields)} changed fields")
        
        # Map Excel columns to database columns - comprehensive mapping
        column_mapping = {
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
        
        # Build UPDATE statement with only changed fields
        update_fields = []
        update_values = []
        
        # Get list of changed field names
        changed_field_names = [cf['field'] for cf in changed_fields]
        
        logger.debug(f"Changed fields to update: {changed_field_names}")
        
        # Update only the changed fields
        for excel_col, db_col in column_mapping.items():
            if db_col in changed_field_names and excel_col in lookback_record.index:
                value = lookback_record[excel_col]
                
                # Handle numeric conversion for numeric columns
                numeric_columns = [
                    'share_class_assets', 'portfolio_assets', 'one_day_yield',
                    'one_day_gross_yield', 'seven_day_yield', 'seven_day_gross_yield',
                    'expense_ratio', 'wam', 'wal', 'daily_liquidity', 'weekly_liquidity'
                ]
                
                if db_col in numeric_columns:
                    if pd.notna(value) and value != '-':
                        value = pd.to_numeric(value, errors='coerce')
                    elif value == '-':
                        value = None
                else:
                    # For text fields, clean whitespace
                    if pd.notna(value):
                        value = str(value).strip()
                    else:
                        value = ''
                
                update_fields.append(f"{db_col} = ?")
                update_values.append(value)
        
        if update_fields:
            # Add WHERE clause values
            update_values.extend([date_str, region, fund_code])
            
            update_sql = f"""
            UPDATE fund_data 
            SET {', '.join(update_fields)}
            WHERE date = ? AND region = ? AND fund_code = ?
            """
            
            logger.debug(f"Executing update with {len(update_fields)} fields")
            cursor.execute(update_sql, update_values)
            
            # Handle weekend dates if this is a Friday
            if pd.to_datetime(date_str).weekday() == 4:
                for days_ahead in [1, 2]:  # Saturday and Sunday
                    weekend_date = (pd.to_datetime(date_str) + pd.Timedelta(days=days_ahead)).strftime('%Y-%m-%d')
                    update_values[-3] = weekend_date  # Update the date parameter
                    cursor.execute(update_sql, update_values)
                    logger.debug(f"Also updated weekend date: {weekend_date}")
        else:
            logger.warning(f"No fields to update for fund {fund_code} on {date_str}")

    def _insert_single_record(self, conn, cursor, lookback_record, region: str, date_str: str):
        """Insert a single new fund record from lookback file"""
        
        # Prepare the record as a single-row DataFrame for processing
        single_row_df = pd.DataFrame([lookback_record])
        
        # Process dates to handle Friday->weekend expansion
        processed_df = self.process_dates(single_row_df, datetime.strptime(date_str, '%Y-%m-%d'))
        
        # Use existing load_to_database method to handle all the column mapping and cleaning
        self.load_to_database(processed_df, region, datetime.strptime(date_str, '%Y-%m-%d'), conn)

    def run_daily_etl(self, run_date: Optional[datetime] = None):
        """
        Main ETL process - runs for a specific date or current date
        Now includes 30-day lookback validation after successful load
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
            return {'success': True}
        
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
                
                # Handle #MULTIVALUE fund codes before validation
                df = self._handle_multivalue_funds(df)
                
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
        
        # After successful daily load, run lookback validation
        validation_alerts = []
        if self.config.get('validation', {}).get('enabled', True):
            logger.info("Starting 30-day lookback validation...")
            
            for region in ['AMRS', 'EMEA']:
                try:
                    # Download lookback file
                    lookback_df = self.download_lookback_file(region)
                    
                    if lookback_df is not None:
                        # Validate against database
                        validation_results = self.validate_against_lookback(region, lookback_df)
                        
                        # Log results
                        logger.info(f"{region} validation results: "
                                  f"{validation_results['summary']['missing_dates_count']} missing dates, "
                                  f"{validation_results['summary']['changed_records_count']} changed records")
                        
                        # Generate alert details if needed
                        if validation_results['summary']['requires_update']:
                            alert_msg = f"\n{region} Validation Alert:\n"
                            alert_msg += f"- Missing dates: {', '.join(validation_results['missing_dates'][:5])}"
                            if len(validation_results['missing_dates']) > 5:
                                alert_msg += f" and {len(validation_results['missing_dates']) - 5} more"
                            alert_msg += f"\n- Changed records: {validation_results['summary']['changed_records_count']}"
                            
                            validation_alerts.append(alert_msg)
                            
                            # Update database with corrected data
                            self.update_from_lookback(region, lookback_df, validation_results)
                            
                except Exception as e:
                    logger.error(f"Lookback validation failed for {region}: {str(e)}")
                    validation_alerts.append(f"\n{region} Validation Error: {str(e)}")
        
        logger.info("ETL process completed")
        
        # Return validation alerts for scheduler to send
        if validation_alerts:
            return {'success': True, 'validation_alerts': validation_alerts}
        else:
            return {'success': True}
    
    def _format_validation_summary(self, results: Dict) -> str:
        """Format validation results into a summary string"""
        summary = results.get('summary', {})
        lines = [
            "Validation Summary:",
            f"Total lookback records: {summary.get('total_lookback_records', 0)}",
            f"Missing dates: {summary.get('missing_dates_count', 0)}",
            f"Changed records: {summary.get('changed_records_count', 0)}",
            f"Requires update: {summary.get('requires_update', False)}"
        ]
        return "\n".join(lines)
    
    def update_from_lookback(self, region: str, lookback_df: pd.DataFrame, 
                           validation_results: Dict, update_mode: str = 'selective') -> Dict:
        """Update database from lookback data based on validation results"""
        try:
            if update_mode == 'selective':
                # Only update changed records
                changed_records = validation_results.get('changed_records', [])
                records_updated = 0
                
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                for record in changed_records:
                    fund_code = record['fund_code']
                    date = record['date']
                    
                    # Get the lookback record (lookback_df has Excel column names)
                    lb_record = lookback_df[
                        (lookback_df['Date'].dt.strftime('%Y-%m-%d') == date) & 
                        (lookback_df['Fund Code'] == fund_code)
                    ]
                    
                    if not lb_record.empty:
                        # Update the record - need to map Excel columns to DB columns
                        excel_data = lb_record.iloc[0].to_dict()
                        
                        # Column mapping from Excel to database
                        column_mapping = {
                            'Share Class Assets (dly/$mils)': 'share_class_assets',
                            'Portfolio Assets (dly/$mils)': 'portfolio_assets',
                            '1-DSY (dly)': 'one_day_yield',
                            '7-DSY (dly)': 'seven_day_yield',
                            'WAM (dly)': 'wam',
                            'WAL (dly)': 'wal',
                            'Daily Liquidity (%)': 'daily_liquidity',
                            'Weekly Liquidity (%)': 'weekly_liquidity'
                        }
                        
                        # Build update data with only mapped columns that exist
                        update_values = []
                        update_columns = []
                        for excel_col, db_col in column_mapping.items():
                            if excel_col in excel_data:
                                update_columns.append(db_col)
                                update_values.append(excel_data[excel_col])
                        
                        if update_columns:
                            set_clause = ', '.join([f"{col} = ?" for col in update_columns])
                            update_values.extend([region, date, fund_code])
                            
                            cursor.execute(f"""
                            UPDATE fund_data 
                            SET {set_clause}
                            WHERE region = ? AND date = ? AND fund_code = ?
                            """, update_values)
                        
                        records_updated += 1
                
                conn.commit()
                conn.close()
                
                return {
                    'records_updated': records_updated,
                    'mode': 'selective'
                }
                
            elif update_mode == 'full':
                # Replace all records for the dates in lookback
                # Convert dates to strings
                dates = lookback_df['Date'].dt.strftime('%Y-%m-%d').unique()
                
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                # Delete existing records for these dates
                for date in dates:
                    cursor.execute("""
                    DELETE FROM fund_data 
                    WHERE region = ? AND date = ?
                    """, (region, date))
                
                # Prepare lookback data for insertion
                insert_df = lookback_df.copy()
                # Use the same column mapping as in load_to_database
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
                insert_df = insert_df.rename(columns=column_mapping)
                insert_df['region'] = region
                insert_df['date'] = insert_df['date'].dt.strftime('%Y-%m-%d')
                
                # Insert all lookback records
                insert_df.to_sql('fund_data', conn, if_exists='append', index=False)
                
                conn.commit()
                conn.close()
                
                return {
                    'records_updated': len(lookback_df),
                    'mode': 'full'
                }
            
            else:
                return None
                
        except Exception as e:
            logger.error(f"Failed to update from lookback: {str(e)}")
            return None


# Configuration template
CONFIG_TEMPLATE = {
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
    "verify_ssl": True,
    "email_alerts": {
        "enabled": False,
        "recipients": ["etl-team@company.com"],
        "smtp_server": "smtp.company.com"
    },
    "validation": {
        "enabled": True,
        "update_mode": "selective",
        "change_threshold_percent": 5.0,
        "critical_fields": [
            "share_class_assets",
            "portfolio_assets",
            "one_day_yield",
            "seven_day_yield"
        ],
        "alert_on_missing_dates": True,
        "alert_on_major_changes": True
    }
}


def create_config_template():
    """Create a template configuration file"""
    with open('config_template.json', 'w') as f:
        json.dump(CONFIG_TEMPLATE, f, indent=2)
    print("Created config_template.json - please update with your settings")


if __name__ == "__main__":
    # Example usage
    import argparse
    
    parser = argparse.ArgumentParser(description='Fund Data ETL Pipeline')
    parser.add_argument('--config', default='/config/config.json', help='Configuration file path')
    parser.add_argument('--date', help='Run ETL for specific date (YYYY-MM-DD)')
    parser.add_argument('--create-config', action='store_true', help='Create configuration template')
    
    args = parser.parse_args()
    
    if args.create_config:
        create_config_template()
    else:
        etl = FundDataETL(args.config)
        
        # Setup database if needed
        etl.setup_database()
        
        # Run ETL
        if args.date:
            run_date = datetime.strptime(args.date, '%Y-%m-%d')
        else:
            run_date = None
            
        etl.run_daily_etl(run_date)