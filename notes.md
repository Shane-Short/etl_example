"""
KPI aggregation module for gold layer.

Creates fact tables from enriched silver data.
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional, Dict, Any
import logging

from connectors.sqlserver_connector import SQLServerConnector
from utils.exceptions import TransformationError


class KPIAggregator:
    """
    Aggregates enriched PM data into Gold layer KPI fact tables.
    """
    
    def __init__(self, connector: Optional[SQLServerConnector] = None):
        """
        Initialize KPI aggregator.
        
        Args:
            connector: SQL Server connector (creates new one if not provided)
        """
        self.logger = logging.getLogger("pm_flex_pipeline.kpi_aggregator")
        
        if connector is None:
            connector = SQLServerConnector()
        
        self.connector = connector
        self.schema_name = "dbo"
    
    def _load_enriched_data(self, start_ww: str = None, end_ww: str = None) -> pd.DataFrame:
        """
        Load enriched data from silver layer.
        
        Args:
            start_ww: Start work week (e.g., '2025WW46')
            end_ww: End work week (e.g., '2025WW48')
            
        Returns:
            DataFrame with enriched PM data
        """
        # Build query - simple SELECT with optional work week filters
        query = "SELECT * FROM dbo.pm_flex_enriched WHERE 1=1"
        
        # Add work week filters if provided
        if start_ww:
            query += f" AND YEARWW >= '{start_ww}'"
        if end_ww:
            query += f" AND YEARWW <= '{end_ww}'"
        
        self.logger.info(f"Loading enriched data from pm_flex_enriched...")
        if start_ww or end_ww:
            self.logger.info(f"Work week filter: {start_ww or 'ALL'} to {end_ww or 'ALL'}")
        
        # Execute query
        df = self.connector.fetch_dataframe(query)
        
        self.logger.info(f"Loaded {len(df):,} rows from pm_flex_enriched")
        
        return df
    
    def create_kpi_tables(self, df: pd.DataFrame) -> Dict[str, int]:
        """
        Create all Gold layer KPI tables from enriched data.
        
        Implements idempotent upsert logic:
        1. Delete existing KPIs for the work weeks in the data
        2. Insert new KPIs for those work weeks
        
        This ensures no duplicates when reprocessing the same weeks.
        
        Args:
            df: Enriched DataFrame
            
        Returns:
            Dictionary with row counts for each table created
        """
        self.logger.info("Creating Gold layer KPI tables...")
        
        # Get list of unique work weeks in the data
        work_weeks = df['YEARWW'].dropna().unique().tolist()
        self.logger.info(f"Processing {len(work_weeks)} work weeks: {sorted(work_weeks)}")
        
        # DELETE existing data for these work weeks to prevent duplicates
        self._delete_existing_kpis(work_weeks)
        
        # Create site-level KPIs (FACILITY + YEARWW)
        self.logger.info("Step 1: Creating site-level KPIs...")
        site_kpis = self._create_site_kpis(df)
        site_rows = self._load_kpis(site_kpis, 'fact_pm_kpis_by_site_ww')
        self.logger.info(f"  Loaded {site_rows:,} site KPI rows")
        
        # Create CEID-level KPIs (CEID + YEARWW)
        self.logger.info("Step 2: Creating CEID-level KPIs...")
        ceid_kpis = self._create_ceid_kpis(df)
        ceid_rows = self._load_kpis(ceid_kpis, 'fact_pm_kpis_by_ceid_ww')
        self.logger.info(f"  Loaded {ceid_rows:,} CEID KPI rows")
        
        # Create part replacement summary
        self.logger.info("Step 3: Creating part replacement summary...")
        part_summary = self._create_part_summary(df)
        part_rows = self._load_kpis(part_summary, 'fact_part_replacement_summary')
        self.logger.info(f"  Loaded {part_rows:,} part summary rows")
        
        # Create chronic tools history
        self.logger.info("Step 4: Creating chronic tools history...")
        chronic_history = self._create_chronic_history(df)
        chronic_rows = self._load_kpis(chronic_history, 'fact_chronic_tools_history')
        self.logger.info(f"  Loaded {chronic_rows:,} chronic history rows")
        
        return {
            'site_kpi_rows': site_rows,
            'ceid_kpi_rows': ceid_rows,
            'part_summary_rows': part_rows,
            'chronic_history_rows': chronic_rows
        }
    
    def _delete_existing_kpis(self, work_weeks: list):
        """
        Delete existing KPI rows for the work weeks being processed.
        
        This ensures idempotency - running the pipeline multiple times
        for the same work weeks won't create duplicates.
        
        Args:
            work_weeks: List of work weeks (e.g., ['2025WW46', '2025WW47'])
        """
        if not work_weeks:
            self.logger.info("No work weeks to delete")
            return
        
        # Build WHERE clause for work weeks
        ww_list = "', '".join(work_weeks)
        where_clause = f"YEARWW IN ('{ww_list}')"
        
        self.logger.info(f"Deleting existing KPIs for work weeks: {sorted(work_weeks)}")
        
        # Delete from each Gold table
        tables = [
            'fact_pm_kpis_by_site_ww',
            'fact_pm_kpis_by_ceid_ww',
            'fact_part_replacement_summary',
            'fact_chronic_tools_history'
        ]
        
        total_deleted = 0
        for table in tables:
            try:
                # Delete and get count
                delete_query = f"""
                DELETE FROM dbo.{table} 
                WHERE {where_clause};
                SELECT @@ROWCOUNT as deleted_count;
                """
                
                # Execute delete
                self.connector.execute_query(f"DELETE FROM dbo.{table} WHERE {where_clause}")
                
                # Get count (may not work with execute_query, so we'll just log the attempt)
                self.logger.info(f"  Deleted existing rows from {table}")
                
            except Exception as e:
                # Table might not exist yet on first run, or might be empty
                self.logger.debug(f"  No rows to delete from {table}: {str(e)}")
        
        self.logger.info("Deletion complete - ready for fresh insert")
    
    def _create_site_kpis(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create site-level KPIs aggregated by FACILITY + YEARWW.
        
        One row per FACILITY per YEARWW.
        
        Args:
            df: Enriched DataFrame
            
        Returns:
            DataFrame with site-level KPIs
        """
        # Group by FACILITY and YEARWW
        site_kpis = df.groupby(['FACILITY', 'YEARWW']).agg({
            'ENTITY': 'count',  # Total PM events
            'CUSTOM_DELTA': ['mean', 'median', 'std'],  # PM life statistics
            'DOWN_WINDOW_DURATION_HR': ['sum', 'mean', 'median'],  # Downtime statistics
            'scheduled_flag': lambda x: (x == 1).sum(),  # Count of scheduled PMs
            'pm_timing_classification': [
                ('early_count', lambda x: (x == 'Early').sum()),
                ('on_time_count', lambda x: (x == 'On-Time').sum()),
                ('late_count', lambda x: (x == 'Late').sum()),
                ('overdue_count', lambda x: (x == 'Overdue').sum())
            ],
            'reclean_event_flag': lambda x: (x == 1).sum(),  # Reclean count
            'sympathy_pm_flag': lambda x: (x == 1).sum(),  # Sympathy PM count
            'ww_year': 'first',  # Get year from work week
            'ww_number': 'first'  # Get week number
        }).reset_index()
        
        # Flatten multi-level column names
        site_kpis.columns = ['_'.join(str(col)).strip('_') if isinstance(col, tuple) else col 
                             for col in site_kpis.columns.values]
        
        # Rename columns to match schema
        site_kpis = site_kpis.rename(columns={
            'ENTITY_count': 'total_pm_events',
            'CUSTOM_DELTA_mean': 'avg_pm_life',
            'CUSTOM_DELTA_median': 'median_pm_life',
            'CUSTOM_DELTA_std': 'pm_life_std_dev',
            'DOWN_WINDOW_DURATION_HR_sum': 'total_downtime_hours',
            'DOWN_WINDOW_DURATION_HR_mean': 'avg_downtime_hours',
            'DOWN_WINDOW_DURATION_HR_median': 'median_downtime_hours',
            'scheduled_flag_<lambda>': 'scheduled_pm_count',
            'pm_timing_classification_early_count': 'early_pm_count',
            'pm_timing_classification_on_time_count': 'on_time_pm_count',
            'pm_timing_classification_late_count': 'late_pm_count',
            'pm_timing_classification_overdue_count': 'overdue_pm_count',
            'reclean_event_flag_<lambda>': 'reclean_count',
            'sympathy_pm_flag_<lambda>': 'sympathy_pm_count',
            'ww_year_first': 'ww_year',
            'ww_number_first': 'ww_number'
        })
        
        # Calculate derived metrics
        site_kpis['unscheduled_pm_count'] = (
            site_kpis['total_pm_events'] - site_kpis['scheduled_pm_count']
        )
        
        site_kpis['unscheduled_pm_rate'] = (
            site_kpis['unscheduled_pm_count'] / site_kpis['total_pm_events']
        ).fillna(0)
        
        site_kpis['early_pm_rate'] = (
            site_kpis['early_pm_count'] / site_kpis['total_pm_events']
        ).fillna(0)
        
        site_kpis['overdue_pm_rate'] = (
            site_kpis['overdue_pm_count'] / site_kpis['total_pm_events']
        ).fillna(0)
        
        site_kpis['reclean_rate'] = (
            site_kpis['reclean_count'] / site_kpis['total_pm_events']
        ).fillna(0)
        
        # Add timestamp
        site_kpis['calculation_timestamp'] = datetime.now()
        
        return site_kpis
    
    def _create_ceid_kpis(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create tool-level KPIs aggregated by CEID + YEARWW.
        
        One row per CEID per YEARWW.
        
        Args:
            df: Enriched DataFrame
            
        Returns:
            DataFrame with CEID-level KPIs
        """
        # Group by FACILITY, CEID, and YEARWW
        ceid_kpis = df.groupby(['FACILITY', 'CEID', 'YEARWW']).agg({
            'ENTITY': 'count',  # Total PM events
            'CUSTOM_DELTA': ['mean', 'median', 'std'],  # PM life statistics
            'DOWN_WINDOW_DURATION_HR': ['sum', 'mean', 'median'],  # Downtime statistics
            'scheduled_flag': lambda x: (x == 1).sum(),  # Scheduled PMs
            'pm_timing_classification': [
                ('early_count', lambda x: (x == 'Early').sum()),
                ('on_time_count', lambda x: (x == 'On-Time').sum()),
                ('late_count', lambda x: (x == 'Late').sum()),
                ('overdue_count', lambda x: (x == 'Overdue').sum())
            ],
            'reclean_event_flag': lambda x: (x == 1).sum(),
            'sympathy_pm_flag': lambda x: (x == 1).sum(),
            'AltairFlag': 'first',  # Altair classification
            'ww_year': 'first',
            'ww_number': 'first'
        }).reset_index()
        
        # Flatten column names
        ceid_kpis.columns = ['_'.join(str(col)).strip('_') if isinstance(col, tuple) else col 
                             for col in ceid_kpis.columns.values]
        
        # Rename columns
        ceid_kpis = ceid_kpis.rename(columns={
            'ENTITY_count': 'total_pm_events',
            'CUSTOM_DELTA_mean': 'avg_pm_life',
            'CUSTOM_DELTA_median': 'median_pm_life',
            'CUSTOM_DELTA_std': 'pm_life_std_dev',
            'DOWN_WINDOW_DURATION_HR_sum': 'total_downtime_hours',
            'DOWN_WINDOW_DURATION_HR_mean': 'avg_downtime_hours',
            'DOWN_WINDOW_DURATION_HR_median': 'median_downtime_hours',
            'scheduled_flag_<lambda>': 'scheduled_pm_count',
            'pm_timing_classification_early_count': 'early_pm_count',
            'pm_timing_classification_on_time_count': 'on_time_pm_count',
            'pm_timing_classification_late_count': 'late_pm_count',
            'pm_timing_classification_overdue_count': 'overdue_pm_count',
            'reclean_event_flag_<lambda>': 'reclean_count',
            'sympathy_pm_flag_<lambda>': 'sympathy_pm_count',
            'AltairFlag_first': 'AltairFlag',
            'ww_year_first': 'ww_year',
            'ww_number_first': 'ww_number'
        })
        
        # Calculate derived metrics
        ceid_kpis['unscheduled_pm_count'] = (
            ceid_kpis['total_pm_events'] - ceid_kpis['scheduled_pm_count']
        )
        
        ceid_kpis['unscheduled_pm_rate'] = (
            ceid_kpis['unscheduled_pm_count'] / ceid_kpis['total_pm_events']
        ).fillna(0)
        
        ceid_kpis['reclean_rate'] = (
            ceid_kpis['reclean_count'] / ceid_kpis['total_pm_events']
        ).fillna(0)
        
        # Add timestamp
        ceid_kpis['calculation_timestamp'] = datetime.now()
        
        return ceid_kpis
    
    def _create_part_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create part replacement summary.
        
        Args:
            df: Enriched DataFrame
            
        Returns:
            DataFrame with part replacement summary
        """
        # Check if part cost columns exist
        if 'PART_COST_PER_PM' not in df.columns:
            self.logger.warning("PART_COST_PER_PM not found, returning empty part summary")
            return pd.DataFrame()
        
        # Group by FACILITY, CEID, YEARWW
        part_summary = df.groupby(['FACILITY', 'CEID', 'YEARWW']).agg({
            'PART_COST_PER_PM': ['sum', 'mean', 'count'],
            'PART_COST_SAVING_ROI': 'sum',
            'ww_year': 'first',
            'ww_number': 'first'
        }).reset_index()
        
        # Flatten columns
        part_summary.columns = ['_'.join(str(col)).strip('_') if isinstance(col, tuple) else col 
                                for col in part_summary.columns.values]
        
        # Rename
        part_summary = part_summary.rename(columns={
            'PART_COST_PER_PM_sum': 'total_part_cost',
            'PART_COST_PER_PM_mean': 'avg_part_cost',
            'PART_COST_PER_PM_count': 'part_replacement_count',
            'PART_COST_SAVING_ROI_sum': 'total_part_savings',
            'ww_year_first': 'ww_year',
            'ww_number_first': 'ww_number'
        })
        
        part_summary['calculation_timestamp'] = datetime.now()
        
        return part_summary
    
    def _create_chronic_history(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create chronic tools history from chronic tools analysis.
        
        Loads from pm_flex_chronic_tools Silver table.
        
        Args:
            df: Enriched DataFrame (used to get work week range)
            
        Returns:
            DataFrame with chronic tools history
        """
        # Get work weeks from enriched data
        work_weeks = df['YEARWW'].unique().tolist()
        
        if not work_weeks:
            return pd.DataFrame()
        
        # Load chronic tools data from Silver
        ww_list = "', '".join(work_weeks)
        query = f"""
        SELECT 
            ENTITY,
            FACILITY,
            CEID,
            YEARWW,
            total_pm_events,
            unscheduled_pm_count,
            unscheduled_pm_rate,
            avg_pm_life,
            total_downtime_hours,
            avg_downtime_hours_per_pm,
            reclean_rate,
            sympathy_pm_rate,
            ww_year,
            ww_number,
            AltairFlag,
            calculation_timestamp
        FROM dbo.pm_flex_chronic_tools
        WHERE YEARWW IN ('{ww_list}')
        """
        
        chronic_history = self.connector.fetch_dataframe(query)
        
        if chronic_history.empty:
            self.logger.warning("No chronic tools data found for these work weeks")
        
        return chronic_history
    
    def _load_kpis(self, df: pd.DataFrame, table_name: str) -> int:
        """
        Load KPI DataFrame to Gold table.
        
        Args:
            df: KPI DataFrame
            table_name: Target table name
            
        Returns:
            Number of rows loaded
        """
        if df.empty:
            self.logger.warning(f"No data to load to {table_name}")
            return 0
        
        rows_loaded = self.connector.load_dataframe(
            df=df,
            table_name=table_name,
            schema=self.schema_name,
            if_exists='append',
            chunksize=1000
        )
        
        return rows_loaded












"""
Gold layer ETL execution.

Orchestrates KPI aggregation from enriched silver data.
"""

import logging
import time
from typing import Optional, Dict, Any
from datetime import datetime

from connectors.sqlserver_connector import SQLServerConnector
from etl.gold.kpi_aggregator import KPIAggregator
from utils.logger import setup_logger, log_execution_time
from utils.env import load_environment
from utils.exceptions import TransformationError


@log_execution_time(logging.getLogger("pm_flex_pipeline.gold"))
def run_gold_etl(
    start_ww: str = None,
    end_ww: str = None,
    incremental: bool = True,
    full_refresh: bool = False,
    process_all: bool = False
) -> Dict[str, Any]:
    """
    Execute gold layer ETL: create KPI fact tables.
    
    Args:
        start_ww: Start work week (e.g., '2025WW46')
        end_ww: End work week (e.g., '2025WW48')
        incremental: If True, only process new/changed data
        full_refresh: If True, truncate all tables before loading
        process_all: If True, process all available work weeks
        
    Returns:
        Dictionary with execution statistics
    """
    # Setup
    config = load_environment()
    logger = setup_logger(
        name="pm_flex_pipeline",
        log_file=config.get("log_file"),
        log_level=config.get("log_level", "INFO")
    )
    
    logger.info("=" * 60)
    logger.info("PM Flex Gold Layer ETL - STARTED")
    logger.info("=" * 60)
    
    start_time = time.time()
    connector = None
    
    try:
        # Connect to database
        connector = SQLServerConnector()
        
        # Handle full refresh - truncate tables
        if full_refresh:
            logger.info("Full refresh requested - truncating Gold layer tables")
            tables = [
                'fact_pm_kpis_by_site_ww',
                'fact_pm_kpis_by_ceid_ww',
                'fact_part_replacement_summary',
                'fact_chronic_tools_history'
            ]
            
            for table in tables:
                try:
                    connector.truncate_table(table, 'dbo')
                    logger.info(f"  Truncated {table}")
                except Exception as e:
                    logger.warning(f"Could not truncate {table}: {e}")
        
        # Initialize stats
        stats = {
            'rows_processed': 0,
            'site_kpi_rows': 0,
            'ceid_kpi_rows': 0,
            'part_summary_rows': 0,
            'chronic_history_rows': 0,
            'execution_time': 0,
            'status': 'PENDING'
        }
        
        # Determine work week range
        if process_all or (start_ww is None and end_ww is None):
            logger.info("Processing all available work weeks from enriched data")
            start_ww = None
            end_ww = None
        elif start_ww and not end_ww:
            # If only start specified, process that single week
            end_ww = start_ww
            logger.info(f"Processing single work week: {start_ww}")
        else:
            logger.info(f"Processing work week range: {start_ww} to {end_ww}")
        
        # Step 1: Load enriched data
        logger.info("=" * 60)
        logger.info("Step 1: Loading enriched data from silver layer")
        logger.info("=" * 60)
        
        aggregator = KPIAggregator(connector=connector)
        df_enriched = aggregator._load_enriched_data(start_ww=start_ww, end_ww=end_ww)
        
        if df_enriched.empty:
            # Check what weeks ARE available
            check_query = """
            SELECT DISTINCT YEARWW, COUNT(*) as row_count
            FROM dbo.pm_flex_enriched
            GROUP BY YEARWW
            ORDER BY YEARWW DESC
            """
            available = connector.fetch_dataframe(check_query)
            
            logger.warning("No data found for specified work week range")
            logger.warning("Available work weeks in enriched data:")
            for _, row in available.iterrows():
                logger.warning(f"  {row['YEARWW']}: {row['row_count']:,} rows")
            
            stats['status'] = 'NO_DATA'
            
        else:
            # Step 2: Create KPI tables
            logger.info("=" * 60)
            logger.info("Step 2: Creating KPI tables")
            logger.info("=" * 60)
            
            kpi_stats = aggregator.create_kpi_tables(df_enriched)
            
            # Update stats
            stats.update(kpi_stats)
            stats['rows_processed'] = len(df_enriched)
            stats['status'] = 'SUCCESS'
        
        # Calculate execution time
        execution_time = time.time() - start_time
        stats['execution_time'] = execution_time
        
        # Log results
        logger.info("=" * 60)
        logger.info("Gold Layer ETL - COMPLETED")
        logger.info("=" * 60)
        logger.info(f"Rows Processed: {stats['rows_processed']:,}")
        logger.info(f"Site KPI Rows: {stats['site_kpi_rows']:,}")
        logger.info(f"CEID KPI Rows: {stats['ceid_kpi_rows']:,}")
        logger.info(f"Part Summary Rows: {stats['part_summary_rows']:,}")
        logger.info(f"Chronic History Rows: {stats['chronic_history_rows']:,}")
        logger.info(f"Execution Time: {execution_time:.2f}s")
        logger.info(f"Status: {stats['status']}")
        logger.info("=" * 60)
        
        return stats
        
    except Exception as e:
        logger.error(f"Gold layer failed: {str(e)}", exc_info=True)
        raise TransformationError(f"Gold layer ETL failed: {str(e)}")
    
    finally:
        if connector:
            connector.close()









# Run Gold layer
if gold:
    logger.info("=" * 60)
    logger.info("GOLD LAYER - STARTING")
    logger.info("=" * 60)
    
    # Determine if we should process all weeks or specific range
    if work_week:
        # If Bronze loaded a specific week, process that week in Gold
        logger.info(f"Processing specific work week: {work_week}")
        gold_stats = run_gold_etl(
            start_ww=work_week,
            end_ww=work_week,
            incremental=not full_refresh,
            full_refresh=full_refresh,
            process_all=False
        )
    else:
        # Process all available enriched data
        logger.info("Processing all available work weeks")
        gold_stats = run_gold_etl(
            incremental=not full_refresh,
            full_refresh=full_refresh,
            process_all=True
        )
    
    pipeline_stats['gold'] = gold_stats
