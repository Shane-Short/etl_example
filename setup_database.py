"""
Database setup script for PM Flex pipeline.

Creates all tables, indexes, stored procedures, and views.
"""

import sys
from pathlib import Path
import logging

from connectors import SQLServerConnector
from utils.logger import setup_logger


def setup_database():
    """
    Run all DDL scripts to set up the database schema.
    """
    logger = setup_logger(name="pm_flex_pipeline.setup")
    
    logger.info("=" * 60)
    logger.info("PM Flex Database Setup - STARTED")
    logger.info("=" * 60)
    
    try:
        # Initialize connector
        logger.info("Connecting to SQL Server...")
        connector = SQLServerConnector()
        
        # Get project root
        project_root = Path(__file__).parent.parent
        sql_dir = project_root / "sql" / "ddl"
        
        # DDL scripts to execute in order
        scripts = [
            "01_copper_schema.sql",
            "02_silver_schema.sql",
            "03_gold_schema.sql"
        ]
        
        # Execute each script
        for script_name in scripts:
            script_path = sql_dir / script_name
            
            if not script_path.exists():
                logger.error(f"Script not found: {script_path}")
                raise FileNotFoundError(f"DDL script missing: {script_path}")
            
            logger.info(f"Executing: {script_name}")
            connector.execute_script_file(str(script_path))
            logger.info(f"✓ {script_name} completed")
        
        # Verify tables were created
        logger.info("\nVerifying schema...")
        
        required_tables = [
            'pm_flex_raw',
            'pm_flex_load_log',
            'pm_flex_enriched',
            'pm_flex_downtime_summary',
            'pm_flex_chronic_tools',
            'DimDate',
            'DimEntity',
            'fact_pm_kpis_by_site_ww',
            'fact_pm_kpis_by_ceid_ww'
        ]
        
        all_tables_exist = True
        for table_name in required_tables:
            exists = connector.table_exists(table_name)
            status = "✓" if exists else "✗"
            logger.info(f"{status} {table_name}")
            
            if not exists:
                all_tables_exist = False
        
        if all_tables_exist:
            logger.info("\n" + "=" * 60)
            logger.info("Database Setup - COMPLETED SUCCESSFULLY")
            logger.info("All required tables created")
            logger.info("=" * 60)
        else:
            logger.error("\nSome tables were not created!")
            raise Exception("Database setup incomplete")
        
        connector.close()
        
    except Exception as e:
        logger.error(f"Database setup failed: {str(e)}")
        raise


def main():
    """
    Command-line interface for database setup.
    """
    try:
        setup_database()
        print("\n✅ Database setup completed successfully!")
        sys.exit(0)
        
    except Exception as e:
        print(f"\n❌ Database setup failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
