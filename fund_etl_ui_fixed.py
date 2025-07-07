# Copy the entire content but replace the get_fund_data function
# Find this section in your fund_etl_ui.py file:

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
        
        # Handle None/NULL values for JSON serialization
        df = df.where(pd.notnull(df), None)
        
        return jsonify(df.to_dict('records'))
        
    except Exception as e:
        app.logger.error(f"Error in get_fund_data: {str(e)}")
        return jsonify({'error': str(e)}), 500
