#!/usr/bin/env python3
"""
Script to add workflow persistence to the database
This adds a workflows table to track all ETL workflows persistently
"""

import sqlite3
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def add_workflows_table(db_path='/data/fund_data.db'):
    """Add workflows table to the database"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create workflows table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS workflows (
            id TEXT PRIMARY KEY,
            type TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            params TEXT,  -- JSON string of parameters
            output TEXT,  -- JSON string of output messages
            error TEXT,
            message TEXT,
            etl_workflow_id TEXT  -- Reference to backend ETL workflow if applicable
        )
        """)
        
        # Create indices for efficient querying
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_workflows_status 
        ON workflows(status)
        """)
        
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_workflows_created_at 
        ON workflows(created_at DESC)
        """)
        
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_workflows_type 
        ON workflows(type)
        """)
        
        conn.commit()
        logger.info("Successfully created workflows table and indices")
        
        # Verify table creation
        cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='workflows'
        """)
        
        if cursor.fetchone():
            logger.info("✓ Workflows table verified")
        else:
            logger.error("✗ Workflows table not found after creation")
            
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error adding workflows table: {e}")
        return False


if __name__ == "__main__":
    # Add the table
    if add_workflows_table():
        print("Workflow persistence table added successfully!")
    else:
        print("Failed to add workflow persistence table")