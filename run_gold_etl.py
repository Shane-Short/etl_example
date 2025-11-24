"""
Gold Layer ETL Orchestration.

Main entry point for creating KPI fact tables from silver layer data.
"""

import sys
import argparse
from datetime import datetime
import logging

from etl.gold import KPIAggregator
from connectors import SQLServerConnector
from utils.logger import setup_logger
from utils.env import load_environment
from utils.exceptions import PMFlexError


def run_gold_etl(
    start_ww: str = None,
    end_ww: str = None,
    incremental: bool = True,
    full_refresh: bool = False
) -> dict:
    """
    Execute gold layer ETL: create KPI fact tables from silver data.
    
    Args:
        start_ww: Start work week (e.g., '2025WW20')
        end_ww: End work week (e.g., '2025WW22')
        incremental: If True, only process new work weeks
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
    logger.info("PM Flex Gold Layer ETL - STARTED")
    logger.info(f"Timestamp: {datetime.now()}")
    logger.info(f"Mode: {'Full Refresh' if full_refresh else 'Incremental' if incremental else 'Historical'}")
    logger.info("=" * 60)
    
    try:
        # Initialize KPI aggregator
        logger.info("Initializing KPI aggregator...")
        aggregator = KPIAggregator()
        
        # Handle full refresh
        if full_refresh:
            logger.warning("Full refresh requested - truncating gold tables")
            connector = SQLServerConnector()
            
            # Truncate gold tables
            gold_tables = [
                'fact_pm_kpis_by_site_ww',
                'fact_pm_kpis_by_ceid_ww',
                'fact_part_replacement_summary',
                'fact_chronic_tools_history'
            ]
            
            for table in gold_tables:
                try:
                    connector.truncate_table(table, 'dbo')
                    logger.info(f"Truncated {table}")
                except Exception as e:
                    logger.warning(f"Could not truncate {table}: {str(e)}")
            
            # Set incremental to False for full reload
            incremental = False
        
        # Run KPI aggregation
        stats = aggregator.create_kpi_tables(
            start_ww=start_ww,
            end_ww=end_ww,
            incremental=incremental
        )
        
        # Log final statistics
        logger.info("=" * 60)
        logger.info("Gold Layer ETL - COMPLETED SUCCESSFULLY")
        logger.info(f"Rows Processed: {stats['rows_processed']:,}")
        logger.info(f"Site KPI Rows: {stats['site_kpi_rows']:,}")
        logger.info(f"CEID KPI Rows: {stats['ceid_kpi_rows']:,}")
        logger.info(f"Part Summary Rows: {stats['part_summary_rows']:,}")
        logger.info(f"Chronic History Rows: {stats['chronic_history_rows']:,}")
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
    Command-line interface for gold layer ETL.
    """
    parser = argparse.ArgumentParser(
        description="PM Flex Gold Layer ETL - Create KPI fact tables"
    )
    
    parser.add_argument(
        "--start-ww",
        type=str,
        help="Start work week (e.g., 2025WW20)"
    )
    
    parser.add_argument(
        "--end-ww",
        type=str,
        help="End work week (e.g., 2025WW22)"
    )
    
    parser.add_argument(
        "--no-incremental",
        action="store_true",
        help="Process all data (not just new work weeks)"
    )
    
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Truncate gold tables and reload all data"
    )
    
    args = parser.parse_args()
    
    try:
        stats = run_gold_etl(
            start_ww=args.start_ww,
            end_ww=args.end_ww,
            incremental=not args.no_incremental,
            full_refresh=args.full_refresh
        )
        
        print("\n✅ Gold ETL completed successfully!")
        print(f"   Rows processed: {stats['rows_processed']:,}")
        print(f"   Site KPIs: {stats['site_kpi_rows']:,}")
        print(f"   CEID KPIs: {stats['ceid_kpi_rows']:,}")
        print(f"   Part Summary: {stats['part_summary_rows']:,}")
        print(f"   Chronic History: {stats['chronic_history_rows']:,}")
        print(f"   Time: {stats['execution_time_seconds']:.2f}s")
        
        sys.exit(0)
        
    except Exception as e:
        print(f"\n❌ Gold ETL failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
