"""
Silver Layer ETL Orchestration.

Main entry point for enriching and transforming copper data to silver tables.
"""

import sys
import argparse
from datetime import datetime, timedelta
import logging

from etl.silver import PMFlexEnrichment
from connectors import SQLServerConnector
from utils.logger import setup_logger
from utils.env import load_environment
from utils.exceptions import PMFlexError


def run_silver_etl(
    start_date: str = None,
    end_date: str = None,
    incremental: bool = True,
    full_refresh: bool = False
) -> dict:
    """
    Execute silver layer ETL: enrich copper data and create silver tables.
    
    Args:
        start_date: Start date (YYYY-MM-DD format)
        end_date: End date (YYYY-MM-DD format)
        incremental: If True, only process new data
        full_refresh: If True, truncate and reload all data
        
    Returns:
        Dictionary with processing statistics
    """
    # Setup logging
    config = load_environment()
    logger = setup_logger(
        name="pm_flex_pipeline",
        log_file=config.get("log_file"),
        log_level=config.get("log_level", "INFO")
    )
    
    logger.info("=" * 60)
    logger.info("PM Flex Silver Layer ETL - STARTED")
    logger.info(f"Timestamp: {datetime.now()}")
    logger.info(f"Mode: {'Full Refresh' if full_refresh else 'Incremental' if incremental else 'Historical'}")
    logger.info("=" * 60)
    
    try:
        # Parse dates if provided
        parsed_start = None
        parsed_end = None
        
        if start_date:
            parsed_start = datetime.strptime(start_date, '%Y-%m-%d')
            logger.info(f"Start date: {parsed_start}")
        
        if end_date:
            parsed_end = datetime.strptime(end_date, '%Y-%m-%d')
            logger.info(f"End date: {parsed_end}")
        
        # Initialize enrichment processor
        logger.info("Initializing enrichment processor...")
        enrichment = PMFlexEnrichment()
        
        # Handle full refresh
        if full_refresh:
            logger.warning("Full refresh requested - truncating silver tables")
            connector = SQLServerConnector()
            
            # Truncate silver tables
            for table in ['pm_flex_enriched', 'pm_flex_downtime_summary', 'pm_flex_chronic_tools']:
                try:
                    connector.truncate_table(table, 'dbo')
                    logger.info(f"Truncated {table}")
                except Exception as e:
                    logger.warning(f"Could not truncate {table}: {str(e)}")
            
            # Set incremental to False for full reload
            incremental = False
        
        # Run enrichment
        stats = enrichment.enrich_and_load(
            start_date=parsed_start,
            end_date=parsed_end,
            incremental=incremental
        )
        
        # Log final statistics
        logger.info("=" * 60)
        logger.info("Silver Layer ETL - COMPLETED SUCCESSFULLY")
        logger.info(f"Rows Processed: {stats['rows_processed']:,}")
        logger.info(f"Enriched Rows: {stats['enriched_rows_loaded']:,}")
        logger.info(f"Summary Rows: {stats['summary_rows_created']:,}")
        logger.info(f"Chronic Tools Analyzed: {stats['chronic_tools_analyzed']:,}")
        logger.info(f"Execution Time: {stats['execution_time_seconds']:.2f} seconds")
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
    Command-line interface for silver layer ETL.
    """
    parser = argparse.ArgumentParser(
        description="PM Flex Silver Layer ETL - Enrich and transform data"
    )
    
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for processing (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for processing (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--no-incremental",
        action="store_true",
        help="Process all data (not just new records)"
    )
    
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Truncate silver tables and reload all data"
    )
    
    args = parser.parse_args()
    
    try:
        stats = run_silver_etl(
            start_date=args.start_date,
            end_date=args.end_date,
            incremental=not args.no_incremental,
            full_refresh=args.full_refresh
        )
        
        print("\n✅ Silver ETL completed successfully!")
        print(f"   Rows processed: {stats['rows_processed']:,}")
        print(f"   Enriched rows: {stats['enriched_rows_loaded']:,}")
        print(f"   Summary rows: {stats['summary_rows_created']:,}")
        print(f"   Time: {stats['execution_time_seconds']:.2f}s")
        
        sys.exit(0)
        
    except Exception as e:
        print(f"\n❌ Silver ETL failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
