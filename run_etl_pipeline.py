"""
Master ETL Runner - Execute Bronze, Silver, and Gold layers in sequence.

This script orchestrates the complete PM Flex ETL pipeline.
"""

import sys
import argparse
from datetime import datetime
import logging

from etl.bronze.run_bronze_etl import run_bronze_etl
from etl.silver.run_silver_etl import run_silver_etl
from etl.gold.run_gold_etl import run_gold_etl
from utils.logger import setup_logger
from utils.env import load_environment
from utils.exceptions import PMFlexError


def run_full_pipeline(
    bronze: bool = True,
    silver: bool = True,
    gold: bool = True,
    work_week: str = None,
    full_refresh: bool = False
) -> dict:
    """
    Execute the complete PM Flex ETL pipeline.
    
    Args:
        bronze: Run bronze layer (file ingestion)
        silver: Run silver layer (enrichment)
        gold: Run gold layer (KPI aggregation)
        work_week: Specific work week to process
        full_refresh: Truncate and reload all layers
        
    Returns:
        Dictionary with statistics from all layers
    """
    # Setup logging
    config = load_environment()
    logger = setup_logger(
        name="pm_flex_pipeline",
        log_file=config.get("log_file"),
        log_level=config.get("log_level", "INFO")
    )
    
    logger.info("=" * 70)
    logger.info("PM FLEX COMPLETE ETL PIPELINE - STARTED")
    logger.info(f"Timestamp: {datetime.now()}")
    logger.info(f"Mode: {'Full Refresh' if full_refresh else 'Incremental'}")
    logger.info("=" * 70)
    
    results = {}
    overall_start = datetime.now()
    
    try:
        # BRONZE LAYER: File Ingestion
        if bronze:
            logger.info("\n" + "▶" * 70)
            logger.info("BRONZE LAYER: File Discovery & Raw Ingestion")
            logger.info("▶" * 70)
            
            try:
                bronze_stats = run_bronze_etl(
                    work_week=work_week,
                    find_latest=(work_week is None),
                    max_weeks_back=4
                )
                results['bronze'] = bronze_stats
                logger.info(f"✓ Bronze layer completed: {bronze_stats['rows_loaded']:,} rows loaded")
            except Exception as e:
                logger.error(f"✗ Bronze layer failed: {str(e)}")
                results['bronze'] = {'status': 'FAILED', 'error': str(e)}
                if not silver and not gold:
                    raise
        
        # SILVER LAYER: Enrichment & Classification
        if silver:
            logger.info("\n" + "▶" * 70)
            logger.info("SILVER LAYER: Enrichment & Classification")
            logger.info("▶" * 70)
            
            try:
                silver_stats = run_silver_etl(
                    incremental=(not full_refresh),
                    full_refresh=full_refresh
                )
                results['silver'] = silver_stats
                logger.info(f"✓ Silver layer completed: {silver_stats['rows_processed']:,} rows processed")
            except Exception as e:
                logger.error(f"✗ Silver layer failed: {str(e)}")
                results['silver'] = {'status': 'FAILED', 'error': str(e)}
                if not gold:
                    raise
        
        # GOLD LAYER: KPI Aggregation
        if gold:
            logger.info("\n" + "▶" * 70)
            logger.info("GOLD LAYER: KPI Aggregation")
            logger.info("▶" * 70)
            
            try:
                gold_stats = run_gold_etl(
                    incremental=(not full_refresh),
                    full_refresh=full_refresh
                )
                results['gold'] = gold_stats
                logger.info(f"✓ Gold layer completed: {gold_stats['site_kpi_rows']:,} site KPI rows created")
            except Exception as e:
                logger.error(f"✗ Gold layer failed: {str(e)}")
                results['gold'] = {'status': 'FAILED', 'error': str(e)}
                raise
        
        # Calculate overall execution time
        overall_end = datetime.now()
        overall_duration = (overall_end - overall_start).total_seconds()
        
        # Final summary
        logger.info("\n" + "=" * 70)
        logger.info("PM FLEX COMPLETE ETL PIPELINE - COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)
        
        if bronze and results.get('bronze', {}).get('status') != 'FAILED':
            logger.info(f"  Bronze: {results['bronze']['rows_loaded']:,} rows loaded")
        
        if silver and results.get('silver', {}).get('status') != 'FAILED':
            logger.info(f"  Silver: {results['silver']['rows_processed']:,} rows enriched")
            logger.info(f"         {results['silver']['chronic_tools_analyzed']} chronic tools analyzed")
        
        if gold and results.get('gold', {}).get('status') != 'FAILED':
            logger.info(f"  Gold:   {results['gold']['site_kpi_rows']:,} site KPI rows")
            logger.info(f"         {results['gold']['ceid_kpi_rows']:,} CEID KPI rows")
        
        logger.info(f"\n  Total Execution Time: {overall_duration:.2f} seconds ({overall_duration/60:.1f} minutes)")
        logger.info("=" * 70)
        
        results['overall_duration'] = overall_duration
        results['status'] = 'SUCCESS'
        
        return results
        
    except Exception as e:
        logger.error(f"\n{'=' * 70}")
        logger.error(f"PM FLEX ETL PIPELINE - FAILED")
        logger.error(f"Error: {str(e)}")
        logger.error(f"{'=' * 70}")
        
        results['status'] = 'FAILED'
        results['error'] = str(e)
        
        raise


def main():
    """
    Command-line interface for master ETL pipeline.
    """
    parser = argparse.ArgumentParser(
        description="PM Flex Master ETL Pipeline - Run Bronze, Silver, and Gold layers"
    )
    
    parser.add_argument(
        "--skip-bronze",
        action="store_true",
        help="Skip bronze layer (file ingestion)"
    )
    
    parser.add_argument(
        "--skip-silver",
        action="store_true",
        help="Skip silver layer (enrichment)"
    )
    
    parser.add_argument(
        "--skip-gold",
        action="store_true",
        help="Skip gold layer (KPI aggregation)"
    )
    
    parser.add_argument(
        "--work-week",
        type=str,
        help="Specific work week to process (e.g., 2025WW22)"
    )
    
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Truncate and reload all layers"
    )
    
    args = parser.parse_args()
    
    try:
        results = run_full_pipeline(
            bronze=not args.skip_bronze,
            silver=not args.skip_silver,
            gold=not args.skip_gold,
            work_week=args.work_week,
            full_refresh=args.full_refresh
        )
        
        # Print summary
        print("\n" + "=" * 70)
        print("✅ PM FLEX ETL PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 70)
        
        if not args.skip_bronze and 'bronze' in results:
            print(f"  Bronze: {results['bronze'].get('rows_loaded', 0):,} rows loaded")
        
        if not args.skip_silver and 'silver' in results:
            print(f"  Silver: {results['silver'].get('rows_processed', 0):,} rows enriched")
        
        if not args.skip_gold and 'gold' in results:
            print(f"  Gold:   {results['gold'].get('site_kpi_rows', 0):,} site KPIs")
        
        print(f"\n  Total Time: {results['overall_duration']:.2f}s ({results['overall_duration']/60:.1f} min)")
        print("=" * 70)
        
        sys.exit(0)
        
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"❌ PM FLEX ETL PIPELINE FAILED")
        print(f"   Error: {str(e)}")
        print("=" * 70)
        sys.exit(1)


if __name__ == "__main__":
    main()
