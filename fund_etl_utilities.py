#!/usr/bin/env python3
"""
Fund ETL Utilities and Monitoring Tools
Provides utilities for monitoring ETL runs, data quality checks, and reporting
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Optional, Dict, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FundDataMonitor:
    """Monitor and analyze fund data ETL processes"""
    
    def __init__(self, db_path: str = '/data/fund_data.db'):
        self.db_path = db_path
    
    def get_etl_status(self, days: int = 7) -> pd.DataFrame:
        """Get ETL run status for the last N days"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            run_date,
            region,
            file_date,
            status,
            records_processed,
            issues,
            created_at
        FROM etl_log
        WHERE run_date >= date('now', '-{} days')
        ORDER BY created_at DESC
        """.format(days)
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
    
    def check_data_completeness(self, date: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """Check data completeness for a specific date or latest date"""
        conn = sqlite3.connect(self.db_path)
        
        if date is None:
            # Get the latest date
            date_query = "SELECT MAX(date) FROM fund_data"
            date = pd.read_sql_query(date_query, conn).iloc[0, 0]
        
        results = {}
        
        for region in ['AMRS', 'EMEA']:
            query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT fund_code) as unique_funds,
                AVG(CASE WHEN share_class_assets IS NOT NULL THEN 1 ELSE 0 END) * 100 as pct_with_assets,
                AVG(CASE WHEN one_day_yield IS NOT NULL THEN 1 ELSE 0 END) * 100 as pct_with_1d_yield,
                AVG(CASE WHEN seven_day_yield IS NOT NULL THEN 1 ELSE 0 END) * 100 as pct_with_7d_yield,
                AVG(CASE WHEN wam IS NOT NULL THEN 1 ELSE 0 END) * 100 as pct_with_wam,
                AVG(CASE WHEN wal IS NOT NULL THEN 1 ELSE 0 END) * 100 as pct_with_wal,
                AVG(CASE WHEN daily_liquidity IS NOT NULL THEN 1 ELSE 0 END) * 100 as pct_with_daily_liq,
                AVG(CASE WHEN weekly_liquidity IS NOT NULL THEN 1 ELSE 0 END) * 100 as pct_with_weekly_liq
            FROM fund_data
            WHERE date = '{date}' AND region = '{region}'
            """
            
            results[region] = pd.read_sql_query(query, conn)
        
        conn.close()
        return results
    
    def find_missing_dates(self, start_date: str, end_date: str) -> Dict[str, List[str]]:
        """Find missing dates in the database for each region"""
        conn = sqlite3.connect(self.db_path)
        
        # Generate all business days in the range
        date_range = pd.bdate_range(start=start_date, end=end_date, freq='B')
        
        missing_dates = {}
        
        for region in ['AMRS', 'EMEA']:
            query = f"""
            SELECT DISTINCT date 
            FROM fund_data 
            WHERE region = '{region}' 
            AND date BETWEEN '{start_date}' AND '{end_date}'
            """
            
            existing_dates = pd.read_sql_query(query, conn)['date'].tolist()
            existing_dates = [pd.to_datetime(d).date() for d in existing_dates]
            
            missing = [d.date() for d in date_range if d.date() not in existing_dates]
            missing_dates[region] = [str(d) for d in missing]
        
        conn.close()
        return missing_dates
    
    def get_lookback_validation_history(self, days: int = 7) -> pd.DataFrame:
        """Get history of lookback validation updates"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            run_date,
            region,
            records_processed,
            issues,
            created_at
        FROM etl_log
        WHERE status = 'LOOKBACK_UPDATE'
        AND run_date >= date('now', '-{} days')
        ORDER BY created_at DESC
        """.format(days)
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
    
    def generate_validation_report(self) -> str:
        """Generate a report of recent validation activities"""
        validation_history = self.get_lookback_validation_history(days=30)
        
        report = f"\n{'='*60}\n"
        report += f"30-Day Lookback Validation Report\n"
        report += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        report += f"{'='*60}\n\n"
        
        if len(validation_history) == 0:
            report += "No validation updates in the last 30 days.\n"
        else:
            report += f"Total validation updates: {len(validation_history)}\n\n"
            
            for region in ['AMRS', 'EMEA']:
                region_updates = validation_history[validation_history['region'] == region]
                if len(region_updates) > 0:
                    report += f"\n{region} Region Updates:\n"
                    report += f"{'-'*30}\n"
                    
                    for _, update in region_updates.iterrows():
                        report += f"Date: {update['run_date']}\n"
                        report += f"Records updated: {update['records_processed']}\n"
                        report += f"Details: {update['issues']}\n\n"
        
        return report
    
    def generate_data_quality_report(self, date: Optional[str] = None) -> str:
        """Generate a comprehensive data quality report"""
        if date is None:
            conn = sqlite3.connect(self.db_path)
            date = pd.read_sql_query("SELECT MAX(date) FROM fund_data", conn).iloc[0, 0]
            conn.close()
        
        report = f"\n{'='*60}\n"
        report += f"Fund Data Quality Report - {date}\n"
        report += f"{'='*60}\n\n"
        
        # Get completeness metrics
        completeness = self.check_data_completeness(date)
        
        for region, metrics in completeness.items():
            report += f"\n{region} Region:\n"
            report += f"{'-'*30}\n"
            
            if len(metrics) > 0 and metrics.iloc[0]['total_records'] > 0:
                row = metrics.iloc[0]
                report += f"Total Records: {row['total_records']:,}\n"
                report += f"Unique Funds: {row['unique_funds']:,}\n"
                report += f"\nData Completeness:\n"
                report += f"  Assets Data: {row['pct_with_assets']:.1f}%\n"
                report += f"  1-Day Yield: {row['pct_with_1d_yield']:.1f}%\n"
                report += f"  7-Day Yield: {row['pct_with_7d_yield']:.1f}%\n"
                report += f"  WAM: {row['pct_with_wam']:.1f}%\n"
                report += f"  WAL: {row['pct_with_wal']:.1f}%\n"
                report += f"  Daily Liquidity: {row['pct_with_daily_liq']:.1f}%\n"
                report += f"  Weekly Liquidity: {row['pct_with_weekly_liq']:.1f}%\n"
            else:
                report += "No data found for this date\n"
        
        # Recent ETL status
        report += f"\n\nRecent ETL Runs:\n"
        report += f"{'-'*30}\n"
        
        etl_status = self.get_etl_status(days=3)
        if len(etl_status) > 0:
            for _, run in etl_status.iterrows():
                report += f"{run['run_date']} - {run['region']}: {run['status']}"
                if run['records_processed']:
                    report += f" ({run['records_processed']:,} records)"
                if run['issues']:
                    report += f"\n  Issues: {run['issues']}"
                report += "\n"
        else:
            report += "No recent ETL runs found\n"
        
        return report
    
    def plot_data_trends(self, days: int = 30):
        """Plot fund data trends over time"""
        conn = sqlite3.connect(self.db_path)
        
        # Get daily record counts
        query = """
        SELECT 
            date,
            region,
            COUNT(*) as record_count,
            COUNT(DISTINCT fund_code) as unique_funds,
            AVG(share_class_assets) as avg_assets
        FROM fund_data
        WHERE date >= date('now', '-{} days')
        GROUP BY date, region
        ORDER BY date
        """.format(days)
        
        df = pd.read_sql_query(query, conn, parse_dates=['date'])
        conn.close()
        
        if len(df) == 0:
            logger.warning("No data found for plotting")
            return
        
        # Create plots
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle(f'Fund Data Trends - Last {days} Days', fontsize=16)
        
        # Plot 1: Record counts by region
        ax1 = axes[0, 0]
        for region in df['region'].unique():
            region_data = df[df['region'] == region]
            ax1.plot(region_data['date'], region_data['record_count'], 
                    marker='o', label=region)
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Record Count')
        ax1.set_title('Daily Record Counts by Region')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Plot 2: Unique funds by region
        ax2 = axes[0, 1]
        for region in df['region'].unique():
            region_data = df[df['region'] == region]
            ax2.plot(region_data['date'], region_data['unique_funds'], 
                    marker='s', label=region)
        ax2.set_xlabel('Date')
        ax2.set_ylabel('Unique Funds')
        ax2.set_title('Unique Funds by Region')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Plot 3: Average assets trend
        ax3 = axes[1, 0]
        for region in df['region'].unique():
            region_data = df[df['region'] == region]
            if region_data['avg_assets'].notna().any():
                ax3.plot(region_data['date'], region_data['avg_assets'], 
                        marker='^', label=region)
        ax3.set_xlabel('Date')
        ax3.set_ylabel('Average Assets ($M)')
        ax3.set_title('Average Fund Assets by Region')
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        # Plot 4: ETL success rate
        ax4 = axes[1, 1]
        etl_df = self.get_etl_status(days)
        if len(etl_df) > 0:
            etl_summary = etl_df.groupby(['region', 'status']).size().unstack(fill_value=0)
            etl_summary.plot(kind='bar', ax=ax4, stacked=True)
            ax4.set_xlabel('Region')
            ax4.set_ylabel('ETL Run Count')
            ax4.set_title('ETL Run Status')
            ax4.legend(title='Status')
        
        plt.tight_layout()
        plt.savefig('fund_data_trends.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        logger.info("Trends plot saved as 'fund_data_trends.png'")


class FundDataQuery:
    """Query utilities for fund data"""
    
    def __init__(self, db_path: str = '/data/fund_data.db'):
        self.db_path = db_path
    
    def search_funds(self, search_term: str, region: Optional[str] = None) -> pd.DataFrame:
        """Search for funds by name or code"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT DISTINCT
            fund_code,
            fund_name,
            master_class_fund_name,
            region,
            currency,
            domicile,
            fund_complex
        FROM fund_data
        WHERE (fund_name LIKE ? OR fund_code LIKE ? OR master_class_fund_name LIKE ?)
        """
        
        params = [f'%{search_term}%'] * 3
        
        if region:
            query += " AND region = ?"
            params.append(region)
        
        query += " ORDER BY fund_name"
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        return df
    
    def get_fund_history(self, fund_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Get historical data for a specific fund"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            date,
            fund_name,
            share_class_assets,
            portfolio_assets,
            one_day_yield,
            seven_day_yield,
            expense_ratio,
            wam,
            wal,
            daily_liquidity,
            weekly_liquidity
        FROM fund_data
        WHERE fund_code = ?
        AND date BETWEEN ? AND ?
        ORDER BY date
        """
        
        df = pd.read_sql_query(query, conn, params=[fund_code, start_date, end_date],
                              parse_dates=['date'])
        conn.close()
        
        return df
    
    def export_data(self, date: str, region: str, output_file: str):
        """Export data for a specific date and region to CSV"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT * FROM fund_data
        WHERE date = ? AND region = ?
        ORDER BY fund_code
        """
        
        df = pd.read_sql_query(query, conn, params=[date, region])
        conn.close()
        
        df.to_csv(output_file, index=False)
        logger.info(f"Exported {len(df)} records to {output_file}")


# Example usage and testing
if __name__ == "__main__":
    # Initialize monitoring tools
    monitor = FundDataMonitor()
    query_tool = FundDataQuery()
    
    # Generate data quality report
    print(monitor.generate_data_quality_report())
    
    # Check ETL status
    print("\nRecent ETL Status:")
    print(monitor.get_etl_status(days=7))
    
    # Search for funds
    print("\nSearching for 'BlackRock' funds:")
    results = query_tool.search_funds('BlackRock')
    print(results.head())
    
    # Generate trends plot
    # monitor.plot_data_trends(days=30)
    
    # Check for missing dates
    missing = monitor.find_missing_dates('2025-06-01', '2025-07-03')
    print("\nMissing dates by region:")
    for region, dates in missing.items():
        if dates:
            print(f"{region}: {', '.join(dates[:5])}{'...' if len(dates) > 5 else ''}")
        else:
            print(f"{region}: No missing dates")