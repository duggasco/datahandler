#!/usr/bin/env python3
"""
Simple Web UI for Fund ETL Monitoring
"""

from flask import Flask, render_template_string, jsonify
import sqlite3
import pandas as pd
import json
from datetime import datetime, timedelta
import os

app = Flask(__name__)

DB_PATH = os.environ.get('DB_PATH', '/data/fund_data.db')

# HTML template
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Fund ETL Monitor</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        h1, h2 {
            color: #333;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }
        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        th {
            background-color: #f8f9fa;
            font-weight: bold;
        }
        .status-success {
            color: #28a745;
            font-weight: bold;
        }
        .status-failed {
            color: #dc3545;
            font-weight: bold;
        }
        .status-carried {
            color: #ffc107;
            font-weight: bold;
        }
        .metric {
            display: inline-block;
            margin: 10px 20px 10px 0;
            padding: 15px;
            background: #f8f9fa;
            border-radius: 4px;
            min-width: 200px;
        }
        .metric-label {
            font-size: 12px;
            color: #6c757d;
        }
        .metric-value {
            font-size: 24px;
            font-weight: bold;
            color: #333;
        }
        .refresh-btn {
            background: #007bff;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 4px;
            cursor: pointer;
        }
        .refresh-btn:hover {
            background: #0056b3;
        }
    </style>
    <script>
        function refreshData() {
            location.reload();
        }
        
        // Auto-refresh every 60 seconds
        setTimeout(refreshData, 60000);
    </script>
</head>
<body>
    <div class="container">
        <h1>Fund ETL Monitor</h1>
        <p>Last updated: {{ current_time }} <button class="refresh-btn" onclick="refreshData()">Refresh</button></p>
        
        <h2>Overview</h2>
        <div>
            <div class="metric">
                <div class="metric-label">Total Records</div>
                <div class="metric-value">{{ total_records }}</div>
            </div>
            <div class="metric">
                <div class="metric-label">Unique Funds</div>
                <div class="metric-value">{{ unique_funds }}</div>
            </div>
            <div class="metric">
                <div class="metric-label">Latest Data Date</div>
                <div class="metric-value">{{ latest_date }}</div>
            </div>
            <div class="metric">
                <div class="metric-label">Missing Dates (7d)</div>
                <div class="metric-value">{{ missing_dates }}</div>
            </div>
        </div>
        
        <h2>Recent ETL Runs</h2>
        <table>
            <thead>
                <tr>
                    <th>Run Date</th>
                    <th>Region</th>
                    <th>File Date</th>
                    <th>Status</th>
                    <th>Records</th>
                    <th>Issues</th>
                </tr>
            </thead>
            <tbody>
                {% for run in recent_runs %}
                <tr>
                    <td>{{ run.run_date }}</td>
                    <td>{{ run.region }}</td>
                    <td>{{ run.file_date }}</td>
                    <td class="status-{{ run.status_class }}">{{ run.status }}</td>
                    <td>{{ run.records_processed or '-' }}</td>
                    <td>{{ run.issues or '-' }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        
        <h2>Data Quality by Region</h2>
        <table>
            <thead>
                <tr>
                    <th>Region</th>
                    <th>Records</th>
                    <th>Assets Data</th>
                    <th>1-Day Yield</th>
                    <th>7-Day Yield</th>
                    <th>Liquidity Data</th>
                </tr>
            </thead>
            <tbody>
                {% for region in data_quality %}
                <tr>
                    <td>{{ region.name }}</td>
                    <td>{{ region.records }}</td>
                    <td>{{ region.assets_pct }}%</td>
                    <td>{{ region.yield_1d_pct }}%</td>
                    <td>{{ region.yield_7d_pct }}%</td>
                    <td>{{ region.liquidity_pct }}%</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
</body>
</html>
'''

@app.route('/')
def index():
    """Main dashboard page"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Get overview metrics
        total_records = pd.read_sql_query("SELECT COUNT(*) as count FROM fund_data", conn).iloc[0]['count']
        unique_funds = pd.read_sql_query("SELECT COUNT(DISTINCT fund_code) as count FROM fund_data", conn).iloc[0]['count']
        latest_date = pd.read_sql_query("SELECT MAX(date) as date FROM fund_data", conn).iloc[0]['date']
        
        # Get missing dates count
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=7)
        missing_query = f"""
        SELECT COUNT(DISTINCT date) as count 
        FROM (
            SELECT DISTINCT date FROM fund_data 
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
        )
        """
        actual_dates = pd.read_sql_query(missing_query, conn).iloc[0]['count']
        expected_dates = 5  # Weekdays only
        missing_dates = max(0, expected_dates - actual_dates)
        
        # Get recent ETL runs
        recent_runs_df = pd.read_sql_query("""
        SELECT run_date, region, file_date, status, records_processed, issues
        FROM etl_log
        ORDER BY created_at DESC
        LIMIT 20
        """, conn)
        
        recent_runs = []
        for _, run in recent_runs_df.iterrows():
            status_class = 'success' if run['status'] == 'SUCCESS' else 'failed' if run['status'] == 'FAILED' else 'carried'
            recent_runs.append({
                'run_date': run['run_date'],
                'region': run['region'],
                'file_date': run['file_date'],
                'status': run['status'],
                'status_class': status_class,
                'records_processed': run['records_processed'],
                'issues': run['issues']
            })
        
        # Get data quality by region
        data_quality = []
        for region in ['AMRS', 'EMEA']:
            quality_query = f"""
            SELECT 
                COUNT(*) as records,
                ROUND(AVG(CASE WHEN share_class_assets IS NOT NULL THEN 1 ELSE 0 END) * 100, 1) as assets_pct,
                ROUND(AVG(CASE WHEN one_day_yield IS NOT NULL THEN 1 ELSE 0 END) * 100, 1) as yield_1d_pct,
                ROUND(AVG(CASE WHEN seven_day_yield IS NOT NULL THEN 1 ELSE 0 END) * 100, 1) as yield_7d_pct,
                ROUND(AVG(CASE WHEN daily_liquidity IS NOT NULL THEN 1 ELSE 0 END) * 100, 1) as liquidity_pct
            FROM fund_data
            WHERE region = '{region}' AND date = '{latest_date}'
            """
            
            quality_df = pd.read_sql_query(quality_query, conn)
            if len(quality_df) > 0 and quality_df.iloc[0]['records'] > 0:
                row = quality_df.iloc[0]
                data_quality.append({
                    'name': region,
                    'records': row['records'],
                    'assets_pct': row['assets_pct'],
                    'yield_1d_pct': row['yield_1d_pct'],
                    'yield_7d_pct': row['yield_7d_pct'],
                    'liquidity_pct': row['liquidity_pct']
                })
        
        conn.close()
        
        return render_template_string(
            HTML_TEMPLATE,
            current_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            total_records=f"{total_records:,}",
            unique_funds=f"{unique_funds:,}",
            latest_date=latest_date,
            missing_dates=missing_dates,
            recent_runs=recent_runs,
            data_quality=data_quality
        )
        
    except Exception as e:
        return f"Error: {str(e)}", 500

@app.route('/api/health')
def health():
    """Health check endpoint"""
    try:
        # Check database connection
        conn = sqlite3.connect(DB_PATH)
        conn.execute("SELECT 1")
        conn.close()
        
        # Check health file
        health_file = '/data/health.json'
        if os.path.exists(health_file):
            with open(health_file, 'r') as f:
                health_data = json.load(f)
        else:
            health_data = {'status': 'unknown', 'message': 'Health file not found'}
        
        return jsonify(health_data)
        
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)
