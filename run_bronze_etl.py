"""
Bronze Layer ETL Orchestration.

Main entry point for discovering and loading PM_Flex raw data.
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime
import logging

from etl.bronze import PMFlexFileDiscovery, PMFlexRawLoader
from connectors import SQLServerConnector
from utils.logger import setup_logger
from utils.env import load_environment
from utils.exceptions import PMFlexError


def run_bronze_etl(
    work_week: str = None,
    find_latest: bool = True,
    max_weeks_back: int = 4
) -> dict:
    """
    Execute bronze layer ETL: discover and load PM_Flex file.
    
    Args:
        work_week: Specific work week to load (e.g., '2025WW22')
        find_latest: If True, find the most recent file automatically
        max_weeks_back: Maximum weeks to look back when finding latest
        
    Returns:
        Dictionary with load statistics
    """
    # Setup logging
    config = load_environment()
    logger = setup_logger(
        name="pm_flex_pipeline",
        log_file=config.get("log_file"),
        log_level=config.get("log_level", "INFO")
    )
    
    logger.info("=" * 60)
    logger.info("PM Flex Bronze Layer ETL - STARTED")
    logger.info(f"Timestamp: {datetime.now()}")
    logger.info("=" * 60)
    
    try:
        # Step 1: File Discovery
        logger.info("STEP 1: File Discovery")
        discovery = PMFlexFileDiscovery()
        
        if find_latest:
            file_path, work_week = discovery.find_latest_file(max_weeks_back)
        else:
            if work_week is None:
                raise ValueError("Must provide work_week if find_latest=False")
            file_path = discovery.find_file_for_week(work_week)
        
        # Validate file
        discovery.validate_file(file_path)
        
        # Get file info
        file_info = discovery.get_file_info(file_path)
        logger.info(f"File found: {file_path}")
        logger.info(f"Work Week: {work_week}")
        logger.info(f"File Size: {file_info['size_mb']:.2f} MB")
        logger.info(f"Estimated Rows: {file_info.get('estimated_rows', 'Unknown')}")
        
        # Step 2: Load to SQL Server
        logger.info("STEP 2: Load to SQL Server")
        loader = PMFlexRawLoader()
        
        # Check current row count
        current_rows = loader.get_row_count()
        logger.info(f"Current rows in table: {current_rows:,}")
        
        # Load the file
        stats = loader.load_file(
            file_path=file_path,
            work_week=work_week,
            validate_schema=True,
            add_altair=True
        )
        
        # Log final statistics
        logger.info("=" * 60)
        logger.info("Bronze Layer ETL - COMPLETED SUCCESSFULLY")
        logger.info(f"Rows Loaded: {stats['rows_loaded']:,}")
        logger.info(f"Execution Time: {stats['execution_time_seconds']:.2f} seconds")
        logger.info(f"New Total Rows: {current_rows + stats['rows_loaded']:,}")
        logger.info("=" * 60)
        
        return stats
        
    except PMFlexError as e:
        logger.error(f"PM Flex ETL Error: {str(e)}")
        raise
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise


def main():
    """
    Command-line interface for bronze layer ETL.
    """
    parser = argparse.ArgumentParser(
        description="PM Flex Bronze Layer ETL - Load raw data from CSV to SQL Server"
    )
    
    parser.add_argument(
        "--work-week",
        type=str,
        help="Specific work week to load (e.g., 2025WW22)"
    )
    
    parser.add_argument(
        "--no-find-latest",
        action="store_true",
        help="Don't automatically find latest file (requires --work-week)"
    )
    
    parser.add_argument(
        "--max-weeks-back",
        type=int,
        default=4,
        help="Maximum weeks to look back when finding latest file (default: 4)"
    )
    
    args = parser.parse_args()
    
    try:
        stats = run_bronze_etl(
            work_week=args.work_week,
            find_latest=not args.no_find_latest,
            max_weeks_back=args.max_weeks_back
        )
        
        print("\n✅ Bronze ETL completed successfully!")
        print(f"   Rows loaded: {stats['rows_loaded']:,}")
        print(f"   Time: {stats['execution_time_seconds']:.2f}s")
        
        sys.exit(0)
        
    except Exception as e:
        print(f"\n❌ Bronze ETL failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
