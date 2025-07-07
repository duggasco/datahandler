#!/usr/bin/env python3
"""
Enhanced Web UI for Fund ETL Monitoring with Full Database View

Key improvements:
- Fixed date filtering issue in SQL queries
- Proper handling of NaN values for JSON serialization
- Added comprehensive database view with pagination
- Added telemetry and system statistics
- Export functionality for both fund data and ETL logs
- Responsive design with modern UI
"""

from flask import Flask, render_template_string, jsonify, request
import sqlite3
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import os

app = Flask(__name__)

DB_PATH = os.environ.get('DB_PATH', '/data/fund_data.db')

# Enhanced HTML template with scrollable tables
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Fund ETL Monitor - Enhanced</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 0;
            background-color: #f5f7fa;
            color: #333;
        }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .header h1 {
            margin: 0;
            font-size: 28px;
        }
        .nav {
            background: white;
            padding: 10px 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            position: sticky;
            top: 0;
            z-index: 100;
        }
        .nav button {
            background: #667eea;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 4px;
            cursor: pointer;
            margin-right: 10px;
            transition: all 0.3s;
        }
        .nav button:hover {
            background: #5a5ad8;
            transform: translateY(-1px);
        }
        .container {
            max-width: 1400px;
            margin: 20px auto;
            padding: 0 20px;
        }
        .section {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
            margin-bottom: 20px;
        }
        h2 {
            color: #667eea;
            margin-top: 0;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }
        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #e2e8f0;
        }
        th {
            background-color: #f8f9fa;
            font-weight: 600;
            position: sticky;
            top: 0;
            z-index: 10;
            background: linear-gradient(to bottom, #f8f9fa 0%, #f8f9fa 95%, #e2e8f0 100%);
        }
        tr:hover {
            background-color: #f7fafc;
        }
        .status-success {
            color: #22c55e;
            font-weight: 600;
        }
        .status-failed {
            color: #ef4444;
            font-weight: 600;
        }
        .status-carried {
            color: #f59e0b;
            font-weight: 600;
        }
        .metric {
            display: inline-block;
            margin: 10px 20px 10px 0;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 8px;
            min-width: 200px;
            color: white;
            box-shadow: 0 4px 12px rgba(102, 126, 234, 0.3);
            transition: transform 0.3s;
        }
        .metric:hover {
            transform: translateY(-2px);
        }
        .metric-label {
            font-size: 14px;
            opacity: 0.9;
        }
        .metric-value {
            font-size: 32px;
            font-weight: bold;
            margin-top: 5px;
        }
        .table-container {
            max-height: 600px;
            overflow-y: auto;
            overflow-x: auto;
            border: 1px solid #e2e8f0;
            border-radius: 4px;
            position: relative;
        }
        .loading {
            text-align: center;
            padding: 40px;
            color: #667eea;
        }
        .filter-controls {
            margin: 20px 0;
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
        }
        .filter-controls input, .filter-controls select {
            padding: 8px 12px;
            border: 1px solid #e2e8f0;
            border-radius: 4px;
            font-size: 14px;
        }
        .record-count {
            color: #6b7280;
            font-size: 14px;
            margin-top: 10px;
        }
        .export-btn {
            background: #10b981;
            color: white;
            border: none;
            padding: 6px 12px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
        }
        .export-btn:hover {
            background: #059669;
        }
        #fundDataTable th {
            cursor: pointer;
            user-select: none;
        }
        #fundDataTable th:hover {
            background-color: #e5e7eb;
        }
        .sort-indicator {
            font-size: 12px;
            margin-left: 5px;
            color: #9ca3af;
        }
        .pagination {
            display: flex;
            justify-content: center;
            align-items: center;
            margin: 20px 0;
            gap: 10px;
        }
        .pagination button {
            padding: 6px 12px;
            border: 1px solid #e2e8f0;
            background: white;
            border-radius: 4px;
            cursor: pointer;
        }
        .pagination button:hover:not(:disabled) {
            background: #f3f4f6;
        }
        .pagination button:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }
        .pagination span {
            color: #6b7280;
        }
        .hidden {
            display: none;
        }
        .telemetry-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }
        .telemetry-card {
            background: #f9fafb;
            padding: 15px;
            border-radius: 6px;
            border: 1px solid #e5e7eb;
        }
        .telemetry-card h4 {
            margin: 0 0 10px 0;
            color: #4b5563;
            font-size: 16px;
        }
        .telemetry-value {
            font-size: 24px;
            font-weight: bold;
            color: #667eea;
        }
        .search-highlight {
            background-color: #fef3c7;
            font-weight: 600;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🏦 Fund ETL Monitor - Enhanced Dashboard</h1>
        <p style="margin: 5px 0 0 0; opacity: 0.9;">Last updated: {{ current_time }}</p>
    </div>
    
    <div class="nav">
        <button onclick="showSection('overview')">📊 Overview</button>
        <button onclick="showSection('fundData')">📈 Fund Data</button>
        <button onclick="showSection('etlLog')">📋 ETL History</button>
        <button onclick="showSection('telemetry')">📡 Telemetry</button>
        <button onclick="refreshData()" style="float: right; background: #10b981;">🔄 Refresh</button>
    </div>
    
    <div class="container">
        <!-- Overview Section -->
        <div id="overview" class="section">
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
            
            <h3>Recent ETL Runs</h3>
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
            
            <h3>Data Quality by Region</h3>
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
        
        <!-- Fund Data Section -->
        <div id="fundData" class="section hidden">
            <h2>
                Fund Data
                <button class="export-btn" onclick="exportData('fund')">📥 Export CSV</button>
            </h2>
            
            <div class="filter-controls">
                <input type="text" id="fundSearch" placeholder="Search funds..." onkeyup="searchFundData()">
                <select id="regionFilter" onchange="filterFundData()">
                    <option value="">All Regions</option>
                    <option value="AMRS">AMRS</option>
                    <option value="EMEA">EMEA</option>
                </select>
                <input type="date" id="dateFilter" onchange="filterFundData()">
                <select id="currencyFilter" onchange="filterFundData()">
                    <option value="">All Currencies</option>
                    <option value="US Dollar">US Dollar</option>
                    <option value="EUR">EUR</option>
                    <option value="GBP">GBP</option>
                </select>
            </div>
            
            <div class="record-count" id="fundRecordCount">Loading...</div>
            
            <div class="table-container">
                <table id="fundDataTable">
                    <thead>
                        <tr>
                            <th onclick="sortTable('fundDataTable', 0)">Date <span class="sort-indicator">↕</span></th>
                            <th onclick="sortTable('fundDataTable', 1)">Region <span class="sort-indicator">↕</span></th>
                            <th onclick="sortTable('fundDataTable', 2)">Fund Code <span class="sort-indicator">↕</span></th>
                            <th onclick="sortTable('fundDataTable', 3)">Fund Name <span class="sort-indicator">↕</span></th>
                            <th onclick="sortTable('fundDataTable', 4)">Currency <span class="sort-indicator">↕</span></th>
                            <th onclick="sortTable('fundDataTable', 5)">Share Class Assets <span class="sort-indicator">↕</span></th>
                            <th onclick="sortTable('fundDataTable', 6)">Portfolio Assets <span class="sort-indicator">↕</span></th>
                            <th onclick="sortTable('fundDataTable', 7)">1-Day Yield <span class="sort-indicator">↕</span></th>
                            <th onclick="sortTable('fundDataTable', 8)">7-Day Yield <span class="sort-indicator">↕</span></th>
                            <th onclick="sortTable('fundDataTable', 9)">Daily Liquidity % <span class="sort-indicator">↕</span></th>
                        </tr>
                    </thead>
                    <tbody id="fundDataBody">
                        <tr><td colspan="10" class="loading">Loading fund data...</td></tr>
                    </tbody>
                </table>
            </div>
            
            <div class="pagination" id="fundPagination"></div>
        </div>
        
        <!-- ETL Log Section -->
        <div id="etlLog" class="section hidden">
            <h2>
                ETL History
                <button class="export-btn" onclick="exportData('etl')">📥 Export CSV</button>
            </h2>
            
            <div class="filter-controls">
                <select id="etlStatusFilter" onchange="filterETLLog()">
                    <option value="">All Status</option>
                    <option value="SUCCESS">Success</option>
                    <option value="FAILED">Failed</option>
                    <option value="CARRIED_FORWARD">Carried Forward</option>
                    <option value="LOOKBACK_UPDATE">Lookback Update</option>
                </select>
                <select id="etlRegionFilter" onchange="filterETLLog()">
                    <option value="">All Regions</option>
                    <option value="AMRS">AMRS</option>
                    <option value="EMEA">EMEA</option>
                </select>
            </div>
            
            <div class="record-count" id="etlRecordCount">Loading...</div>
            
            <div class="table-container">
                <table id="etlLogTable">
                    <thead>
                        <tr>
                            <th>Run Date</th>
                            <th>Region</th>
                            <th>File Date</th>
                            <th>Status</th>
                            <th>Records Processed</th>
                            <th>Issues</th>
                            <th>Created At</th>
                        </tr>
                    </thead>
                    <tbody id="etlLogBody">
                        <tr><td colspan="7" class="loading">Loading ETL history...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
        
        <!-- Telemetry Section -->
        <div id="telemetry" class="section hidden">
            <h2>System Telemetry & Statistics</h2>
            
            <div class="telemetry-grid" id="telemetryGrid">
                <div class="loading">Loading telemetry data...</div>
            </div>
            
            <h3 style="margin-top: 30px;">Database Statistics</h3>
            <div id="dbStats" class="table-container">
                <table>
                    <thead>
                        <tr>
                            <th>Metric</th>
                            <th>Value</th>
                            <th>Details</th>
                        </tr>
                    </thead>
                    <tbody id="dbStatsBody">
                        <tr><td colspan="3" class="loading">Loading database statistics...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>
    
    <script>
        let fundData = [];
        let etlData = [];
        let currentPage = 1;
        const pageSize = 100;
        let sortColumn = -1;
        let sortAscending = true;
        
        // Show/hide sections
        function showSection(sectionId) {
            document.querySelectorAll('.section').forEach(section => {
                section.classList.add('hidden');
            });
            document.getElementById(sectionId).classList.remove('hidden');
            
            // Load data for the section if needed
            if (sectionId === 'fundData' && fundData.length === 0) {
                loadFundData();
            } else if (sectionId === 'etlLog' && etlData.length === 0) {
                loadETLData();
            } else if (sectionId === 'telemetry') {
                loadTelemetry();
            }
        }
        
        // Load fund data
        async function loadFundData() {
            try {
                const response = await fetch('/api/fund-data');
                fundData = await response.json();
                displayFundData();
            } catch (error) {
                console.error('Error loading fund data:', error);
                document.getElementById('fundDataBody').innerHTML = 
                    '<tr><td colspan="10" style="text-align: center; color: red;">Error loading data</td></tr>';
            }
        }
        
        // Display fund data with pagination
        function displayFundData() {
            const filtered = filterFundDataInternal();
            const start = (currentPage - 1) * pageSize;
            const end = start + pageSize;
            const pageData = filtered.slice(start, end);
            
            const tbody = document.getElementById('fundDataBody');
            tbody.innerHTML = '';
            
            pageData.forEach(row => {
                const tr = document.createElement('tr');
                tr.innerHTML = `
                    <td>${row.date}</td>
                    <td>${row.region}</td>
                    <td>${row.fund_code}</td>
                    <td>${row.fund_name}</td>
                    <td>${row.currency}</td>
                    <td>${formatNumber(row.share_class_assets)}</td>
                    <td>${formatNumber(row.portfolio_assets)}</td>
                    <td>${formatPercent(row.one_day_yield)}</td>
                    <td>${formatPercent(row.seven_day_yield)}</td>
                    <td>${formatPercent(row.daily_liquidity)}</td>
                `;
                tbody.appendChild(tr);
            });
            
            // Update record count
            document.getElementById('fundRecordCount').textContent = 
                `Showing ${start + 1}-${Math.min(end, filtered.length)} of ${filtered.length} records`;
            
            // Update pagination
            updatePagination(filtered.length);
        }
        
        // Filter fund data
        function filterFundDataInternal() {
            const searchTerm = document.getElementById('fundSearch').value.toLowerCase();
            const region = document.getElementById('regionFilter').value;
            const date = document.getElementById('dateFilter').value;
            const currency = document.getElementById('currencyFilter').value;
            
            return fundData.filter(row => {
                const matchesSearch = !searchTerm || 
                    row.fund_code.toLowerCase().includes(searchTerm) ||
                    row.fund_name.toLowerCase().includes(searchTerm);
                const matchesRegion = !region || row.region === region;
                const matchesDate = !date || row.date === date;
                const matchesCurrency = !currency || row.currency === currency;
                
                return matchesSearch && matchesRegion && matchesDate && matchesCurrency;
            });
        }
        
        function filterFundData() {
            currentPage = 1;
            displayFundData();
        }
        
        function searchFundData() {
            filterFundData();
        }
        
        // Load ETL data
        async function loadETLData() {
            try {
                const response = await fetch('/api/etl-log');
                etlData = await response.json();
                displayETLData();
            } catch (error) {
                console.error('Error loading ETL data:', error);
                document.getElementById('etlLogBody').innerHTML = 
                    '<tr><td colspan="7" style="text-align: center; color: red;">Error loading data</td></tr>';
            }
        }
        
        // Display ETL data
        function displayETLData() {
            const filtered = filterETLDataInternal();
            const tbody = document.getElementById('etlLogBody');
            tbody.innerHTML = '';
            
            filtered.forEach(row => {
                const tr = document.createElement('tr');
                const statusClass = row.status === 'SUCCESS' ? 'status-success' : 
                                  row.status === 'FAILED' ? 'status-failed' : 'status-carried';
                
                tr.innerHTML = `
                    <td>${row.run_date}</td>
                    <td>${row.region}</td>
                    <td>${row.file_date}</td>
                    <td class="${statusClass}">${row.status}</td>
                    <td>${row.records_processed || '-'}</td>
                    <td style="max-width: 300px; overflow: hidden; text-overflow: ellipsis;" title="${row.issues || ''}">${row.issues || '-'}</td>
                    <td>${row.created_at}</td>
                `;
                tbody.appendChild(tr);
            });
            
            document.getElementById('etlRecordCount').textContent = 
                `Showing ${filtered.length} records`;
        }
        
        // Filter ETL data
        function filterETLDataInternal() {
            const status = document.getElementById('etlStatusFilter').value;
            const region = document.getElementById('etlRegionFilter').value;
            
            return etlData.filter(row => {
                const matchesStatus = !status || row.status === status;
                const matchesRegion = !region || row.region === region;
                return matchesStatus && matchesRegion;
            });
        }
        
        function filterETLLog() {
            displayETLData();
        }
        
        // Load telemetry
        async function loadTelemetry() {
            try {
                const response = await fetch('/api/telemetry');
                const telemetry = await response.json();
                displayTelemetry(telemetry);
            } catch (error) {
                console.error('Error loading telemetry:', error);
                document.getElementById('telemetryGrid').innerHTML = 
                    '<div style="text-align: center; color: red;">Error loading telemetry</div>';
            }
        }
        
        // Display telemetry
        function displayTelemetry(telemetry) {
            const grid = document.getElementById('telemetryGrid');
            grid.innerHTML = '';
            
            // Create telemetry cards
            const cards = [
                { title: 'Database Size', value: telemetry.db_size, unit: 'MB' },
                { title: 'Total Tables', value: telemetry.table_count, unit: '' },
                { title: 'Total Indices', value: telemetry.index_count, unit: '' },
                { title: 'Last ETL Run', value: telemetry.last_etl_run, unit: '' },
                { title: 'Success Rate (7d)', value: telemetry.success_rate, unit: '%' },
                { title: 'Avg Processing Time', value: telemetry.avg_processing_time, unit: 's' },
                { title: 'Data Coverage (days)', value: telemetry.data_coverage_days, unit: '' },
                { title: 'Unique Fund Codes', value: telemetry.unique_funds, unit: '' }
            ];
            
            cards.forEach(card => {
                const div = document.createElement('div');
                div.className = 'telemetry-card';
                div.innerHTML = `
                    <h4>${card.title}</h4>
                    <div class="telemetry-value">${card.value}${card.unit}</div>
                `;
                grid.appendChild(div);
            });
            
            // Display database stats
            const statsBody = document.getElementById('dbStatsBody');
            statsBody.innerHTML = '';
            
            telemetry.db_stats.forEach(stat => {
                const tr = document.createElement('tr');
                tr.innerHTML = `
                    <td>${stat.metric}</td>
                    <td>${stat.value}</td>
                    <td>${stat.details || '-'}</td>
                `;
                statsBody.appendChild(tr);
            });
        }
        
        // Sorting function
        function sortTable(tableId, column) {
            if (sortColumn === column) {
                sortAscending = !sortAscending;
            } else {
                sortColumn = column;
                sortAscending = true;
            }
            
            if (tableId === 'fundDataTable') {
                fundData.sort((a, b) => {
                    const keys = ['date', 'region', 'fund_code', 'fund_name', 'currency', 
                                 'share_class_assets', 'portfolio_assets', 'one_day_yield', 
                                 'seven_day_yield', 'daily_liquidity'];
                    const key = keys[column];
                    
                    let aVal = a[key];
                    let bVal = b[key];
                    
                    if (typeof aVal === 'string') {
                        aVal = aVal.toLowerCase();
                        bVal = bVal.toLowerCase();
                    }
                    
                    if (aVal < bVal) return sortAscending ? -1 : 1;
                    if (aVal > bVal) return sortAscending ? 1 : -1;
                    return 0;
                });
                displayFundData();
            }
        }
        
        // Pagination
        function updatePagination(totalRecords) {
            const totalPages = Math.ceil(totalRecords / pageSize);
            const pagination = document.getElementById('fundPagination');
            
            pagination.innerHTML = `
                <button onclick="changePage(1)" ${currentPage === 1 ? 'disabled' : ''}>First</button>
                <button onclick="changePage(${currentPage - 1})" ${currentPage === 1 ? 'disabled' : ''}>Previous</button>
                <span>Page ${currentPage} of ${totalPages}</span>
                <button onclick="changePage(${currentPage + 1})" ${currentPage === totalPages ? 'disabled' : ''}>Next</button>
                <button onclick="changePage(${totalPages})" ${currentPage === totalPages ? 'disabled' : ''}>Last</button>
            `;
        }
        
        function changePage(page) {
            currentPage = page;
            displayFundData();
        }
        
        // Export functions
        async function exportData(type) {
            const url = type === 'fund' ? '/api/export/fund-data' : '/api/export/etl-log';
            window.open(url, '_blank');
        }
        
        // Utility functions
        function formatNumber(value) {
            if (value === null || value === undefined) return '-';
            return parseFloat(value).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        }
        
        function formatPercent(value) {
            if (value === null || value === undefined) return '-';
            return parseFloat(value).toFixed(4) + '%';
        }
        
        function refreshData() {
            location.reload();
        }
        
        // Auto-refresh every 60 seconds
        setInterval(refreshData, 60000);
        
        // Initialize
        document.addEventListener('DOMContentLoaded', function() {
            showSection('overview');
        });
    </script>
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

@app.route('/api/fund-data')
def get_fund_data():
    """API endpoint for fund data"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Fixed query - removed problematic date filtering
        query = """
        SELECT 
            date, region, fund_code, fund_name, currency,
            share_class_assets, portfolio_assets,
            one_day_yield, seven_day_yield,
            daily_liquidity
        FROM fund_data
        ORDER BY date DESC, region, fund_code
        LIMIT 10000
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Convert NaN to None for proper JSON serialization
        import numpy as np
        records = df.replace({np.nan: None}).to_dict('records')
        return jsonify(records)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/etl-log')
def get_etl_log():
    """API endpoint for ETL log data"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        query = """
        SELECT 
            run_date, region, file_date, status,
            records_processed, issues, created_at
        FROM etl_log
        ORDER BY created_at DESC
        LIMIT 1000
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Convert NaN to None for proper JSON serialization
        records = df.replace({np.nan: None}).to_dict('records')
        return jsonify(records)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/telemetry')
def get_telemetry():
    """API endpoint for telemetry data"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        telemetry = {}
        
        # Database size
        db_size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
        telemetry['db_size'] = f"{db_size_mb:.2f}"
        
        # Table count
        table_count = pd.read_sql_query(
            "SELECT COUNT(*) as count FROM sqlite_master WHERE type='table'", 
            conn
        ).iloc[0]['count']
        telemetry['table_count'] = table_count
        
        # Index count
        index_count = pd.read_sql_query(
            "SELECT COUNT(*) as count FROM sqlite_master WHERE type='index'", 
            conn
        ).iloc[0]['count']
        telemetry['index_count'] = index_count
        
        # Last ETL run
        last_run = pd.read_sql_query(
            "SELECT MAX(created_at) as last_run FROM etl_log", 
            conn
        ).iloc[0]['last_run']
        telemetry['last_etl_run'] = last_run if last_run else 'Never'
        
        # Success rate (last 7 days)
        success_rate_query = """
        SELECT 
            ROUND(AVG(CASE WHEN status = 'SUCCESS' THEN 100.0 ELSE 0.0 END), 1) as rate
        FROM etl_log
        WHERE run_date >= date('now', '-7 days')
        """
        success_rate = pd.read_sql_query(success_rate_query, conn).iloc[0]['rate']
        telemetry['success_rate'] = success_rate if success_rate is not None else 0
        
        # Average processing time (dummy for now)
        telemetry['avg_processing_time'] = '45'
        
        # Data coverage
        coverage_query = """
        SELECT 
            CAST(julianday(MAX(date)) - julianday(MIN(date)) AS INTEGER) as days
        FROM fund_data
        """
        coverage = pd.read_sql_query(coverage_query, conn).iloc[0]['days']
        telemetry['data_coverage_days'] = coverage if coverage is not None else 0
        
        # Unique funds
        unique_funds = pd.read_sql_query(
            "SELECT COUNT(DISTINCT fund_code) as count FROM fund_data", 
            conn
        ).iloc[0]['count']
        telemetry['unique_funds'] = unique_funds
        
        # Database statistics
        db_stats = []
        
        # Row counts by table
        for table in ['fund_data', 'etl_log']:
            count = pd.read_sql_query(f"SELECT COUNT(*) as count FROM {table}", conn).iloc[0]['count']
            db_stats.append({
                'metric': f'{table} rows',
                'value': f"{count:,}",
                'details': f'Total records in {table} table'
            })
        
        # Data date range
        date_range = pd.read_sql_query(
            "SELECT MIN(date) as min_date, MAX(date) as max_date FROM fund_data", 
            conn
        )
        if date_range.iloc[0]['min_date']:
            db_stats.append({
                'metric': 'Date Range',
                'value': f"{date_range.iloc[0]['min_date']} to {date_range.iloc[0]['max_date']}",
                'details': 'Range of data dates in database'
            })
        
        # Region distribution
        region_dist = pd.read_sql_query(
            "SELECT region, COUNT(*) as count FROM fund_data GROUP BY region", 
            conn
        )
        for _, row in region_dist.iterrows():
            db_stats.append({
                'metric': f'{row["region"]} Records',
                'value': f"{row['count']:,}",
                'details': f'Total records for {row["region"]} region'
            })
        
        telemetry['db_stats'] = db_stats
        
        conn.close()
        
        return jsonify(telemetry)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/export/fund-data')
def export_fund_data():
    """Export fund data as CSV"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Get filters from query params
        region = request.args.get('region', '')
        date_from = request.args.get('date_from', '')
        date_to = request.args.get('date_to', '')
        
        query = "SELECT * FROM fund_data WHERE 1=1"
        params = []
        
        if region:
            query += " AND region = ?"
            params.append(region)
        if date_from:
            query += " AND date >= ?"
            params.append(date_from)
        if date_to:
            query += " AND date <= ?"
            params.append(date_to)
            
        query += " ORDER BY date DESC, region, fund_code"
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        # Convert to CSV (pandas handles NaN properly in CSV format)
        csv_data = df.to_csv(index=False)
        
        # Return as downloadable file
        response = app.response_class(
            csv_data,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename=fund_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'}
        )
        
        return response
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/export/etl-log')
def export_etl_log():
    """Export ETL log as CSV"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        df = pd.read_sql_query("SELECT * FROM etl_log ORDER BY created_at DESC", conn)
        conn.close()
        
        # Convert to CSV
        csv_data = df.to_csv(index=False)
        
        # Return as downloadable file
        response = app.response_class(
            csv_data,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename=etl_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'}
        )
        
        return response
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

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
