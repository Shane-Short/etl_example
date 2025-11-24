"""
Data enrichment module for silver layer.

Orchestrates all transformations from copper to silver tables:
- pm_flex_enriched
- pm_flex_downtime_summary
- pm_flex_chronic_tools
- DimDate population
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional, Dict, Any
import logging
import time

from connectors import SQLServerConnector
from etl.silver.classification import PMTimingClassifier, ChronicToolAnalyzer
from utils.helpers import get_intel_ww_calendar, parse_ww_string
from utils.logger import log_execution_time
from utils.exceptions import TransformationError


class PMFlexEnrichment:
    """
    Enriches raw PM_Flex data with business logic and loads to silver tables.
    """
    
    def __init__(self, connector: Optional[SQLServerConnector] = None):
        """
        Initialize enrichment processor.
        
        Args:
            connector: SQL Server connector (creates new one if not provided)
        """
        self.logger = logging.getLogger("pm_flex_pipeline.enrichment")
        
        if connector is None:
            connector = SQLServerConnector()
        
        self.connector = connector
        self.schema_name = "dbo"
        
        # Initialize classifiers
        self.timing_classifier = PMTimingClassifier()
        self.chronic_analyzer = ChronicToolAnalyzer()
    
    @log_execution_time(logging.getLogger("pm_flex_pipeline.enrichment"))
    def enrich_and_load(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        incremental: bool = True
    ) -> Dict[str, Any]:
        """
        Main enrichment process: copper → silver tables.
        
        Args:
            start_date: Start date for processing (None = process all)
            end_date: End date for processing (None = today)
            incremental: If True, only process new data
            
        Returns:
            Dictionary with processing statistics
        """
        start_time = time.time()
        
        self.logger.info("=" * 60)
        self.logger.info("Silver Layer Enrichment - STARTED")
        self.logger.info("=" * 60)
        
        try:
            # Step 1: Load raw data from copper
            self.logger.info("STEP 1: Loading raw data from copper layer")
            raw_df = self._load_raw_data(start_date, end_date, incremental)
            
            if len(raw_df) == 0:
                self.logger.warning("No new data to process")
                return {
                    "rows_processed": 0,
                    "execution_time_seconds": 0,
                    "status": "NO_DATA"
                }
            
            self.logger.info(f"Loaded {len(raw_df)} rows from copper")
            
            # Step 2: Enrich data
            self.logger.info("STEP 2: Enriching data")
            enriched_df = self._enrich_data(raw_df)
            
            # Step 3: Load to pm_flex_enriched
            self.logger.info("STEP 3: Loading to pm_flex_enriched")
            enriched_rows = self._load_enriched(enriched_df)
            
            # Step 4: Create downtime summary
            self.logger.info("STEP 4: Creating downtime summary")
            summary_rows = self._create_downtime_summary(enriched_df)
            
            # Step 5: Analyze chronic tools
            self.logger.info("STEP 5: Analyzing chronic tools")
            chronic_rows = self._analyze_chronic_tools(enriched_df)
            
            # Step 6: Populate DimDate (if needed)
            self.logger.info("STEP 6: Populating DimDate")
            self._populate_dim_date()
            
            # Calculate statistics
            execution_time = time.time() - start_time
            
            stats = {
                "rows_processed": len(raw_df),
                "enriched_rows_loaded": enriched_rows,
                "summary_rows_created": summary_rows,
                "chronic_tools_analyzed": chronic_rows,
                "execution_time_seconds": execution_time,
                "status": "SUCCESS"
            }
            
            self.logger.info("=" * 60)
            self.logger.info("Silver Layer Enrichment - COMPLETED")
            self.logger.info(f"Rows processed: {stats['rows_processed']:,}")
            self.logger.info(f"Execution time: {execution_time:.2f}s")
            self.logger.info("=" * 60)
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Enrichment failed: {str(e)}", exc_info=True)
            raise TransformationError(f"Silver layer enrichment failed: {str(e)}")
    
    def _load_raw_data(
        self,
        start_date: Optional[datetime],
        end_date: Optional[datetime],
        incremental: bool
    ) -> pd.DataFrame:
        """
        Load raw data from pm_flex_raw table.
        
        Args:
            start_date: Start date filter
            end_date: End date filter
            incremental: Only load records not yet in pm_flex_enriched
            
        Returns:
            DataFrame with raw PM_Flex data
        """
        # Build query
        if incremental:
            # Only load records not yet enriched
            query = """
            SELECT r.*
            FROM dbo.pm_flex_raw r
            WHERE NOT EXISTS (
                SELECT 1 
                FROM dbo.pm_flex_enriched e
                WHERE e.source_pm_flex_raw_id = r.pm_flex_raw_id
            )
            """
        else:
            query = "SELECT * FROM dbo.pm_flex_raw"
        
        # Add date filters if provided
        conditions = []
        if start_date:
            conditions.append(f"TXN_DATE >= '{start_date}'")
        if end_date:
            conditions.append(f"TXN_DATE < '{end_date}'")
        
        if conditions:
            query += " AND " + " AND ".join(conditions)
        
        df = self.connector.fetch_dataframe(query)
        return df
    
    def _enrich_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all enrichment logic to raw data.
        
        Args:
            df: Raw PM_Flex DataFrame
            
        Returns:
            Enriched DataFrame with all calculated columns
        """
        self.logger.info("Applying enrichment transformations...")
        
        # Make a copy to avoid modifying original
        enriched = df.copy()
        
        # 1. Parse work week into year and number
        enriched = self._parse_work_week(enriched)
        
        # 2. Classify PM timing
        enriched = self.timing_classifier.classify_timing(enriched)
        
        # 3. Classify scheduled vs unscheduled
        enriched = self.timing_classifier.classify_scheduled(enriched)
        
        # 4. Add PM cycle metrics
        enriched = self._add_pm_cycle_metrics(enriched)
        
        # 5. Add downtime categories
        enriched = self._add_downtime_categories(enriched)
        
        # 6. Add data quality score
        enriched = self._add_data_quality_score(enriched)
        
        # 7. Add enrichment timestamp
        enriched['enrichment_timestamp'] = datetime.now()
        
        self.logger.info(f"Enrichment complete: {len(enriched)} rows")
        return enriched
    
    def _parse_work_week(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse YEARWW into year and week number."""
        def parse_ww(ww_string):
            try:
                if pd.isna(ww_string):
                    return None, None
                year, week = parse_ww_string(str(ww_string))
                return year, week
            except:
                return None, None
        
        df[['ww_year', 'ww_number']] = df['YEARWW'].apply(
            lambda x: pd.Series(parse_ww(x))
        )
        
        # Add fiscal quarter and month (simplified - 13 weeks per quarter)
        df['fiscal_quarter'] = ((df['ww_number'] - 1) // 13) + 1
        df['fiscal_month'] = ((df['ww_number'] - 1) // 4) + 1
        
        return df
    
    def _add_pm_cycle_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add PM cycle-related metrics."""
        # PM cycle efficiency (from pm_cycle_utilization if available)
        df['pm_cycle_efficiency'] = df.get('pm_cycle_utilization', np.nan)
        
        # PM duration outlier flag
        df['pm_duration_outlier_flag'] = df['DOWN_WINDOW_DURATION_OUTLIER_LABEL_FOR_PMCYCLE'].apply(
            lambda x: 1 if x == 1 else 0
        )
        
        # Reclean event flag
        df['reclean_event_flag'] = df['Reclean_Label'].apply(
            lambda x: 1 if x == 1 else 0
        )
        
        # Sympathy PM flag
        df['sympathy_pm_flag'] = df['Sympathy_PM'].apply(
            lambda x: 1 if x == 1 else 0
        )
        
        return df
    
    def _add_downtime_categories(self, df: pd.DataFrame) -> pd.DataFrame:
        """Combine downtime type and class into categories."""
        # Combine DOWNTIME_TYPE and DOWNTIME_CLASS
        df['downtime_category'] = (
            df['DOWNTIME_TYPE'].fillna('Unknown').astype(str) + 
            ' - ' + 
            df['DOWNTIME_CLASS'].fillna('Unknown').astype(str)
        )
        
        # Primary reason from PM_Reason_Deepdive or DOWNTIME_SUBCLASS_DETAILS
        df['downtime_primary_reason'] = df['PM_Reason_Deepdive'].fillna(
            df['DOWNTIME_SUBCLASS_DETAILS']
        )
        
        return df
    
    def _add_data_quality_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate data quality score (0-100) based on completeness."""
        # Critical columns for quality score
        critical_cols = [
            'ENTITY', 'FACILITY', 'CEID', 'YEARWW', 'TXN_DATE',
            'DOWNTIME_TYPE', 'CUSTOM_DELTA', 'Median_Delta'
        ]
        
        # Calculate percentage of non-null critical columns
        df['data_quality_score'] = df[critical_cols].notna().sum(axis=1) / len(critical_cols) * 100
        
        return df
    
    def _load_enriched(self, df: pd.DataFrame) -> int:
        """
        Load enriched data to pm_flex_enriched table.
        
        Args:
            df: Enriched DataFrame
            
        Returns:
            Number of rows loaded
        """
        # Select columns for pm_flex_enriched
        # Note: We're loading a subset of columns + enriched columns
        # Adjust this list based on your actual table schema
        
        enriched_cols = [
            # Original columns (key ones)
            'pm_flex_raw_id', 'ENTITY', 'FACILITY', 'UNIQUE_ENTITY_ID',
            'CEID', 'YEARWW', 'TXN_DATE', 'PM_NAME', 'ATTRIBUTE_NAME',
            'CUSTOM_DELTA', 'COUNTER_UPPER_VALUE', 'DOWNTIME_TYPE',
            'DOWNTIME_CLASS', 'DOWNTIME_SUBCLASS', 'DOWN_WINDOW_DURATION_HR',
            'PM_FREQUENCY', 'Sympathy_PM',
            
            # Enriched columns
            'AltairFlag', 'ww_year', 'ww_number', 'fiscal_quarter', 'fiscal_month',
            'pm_timing_classification', 'pm_life_vs_target', 'pm_life_vs_target_pct',
            'scheduled_flag', 'scheduled_category',
            'pm_cycle_efficiency', 'pm_duration_outlier_flag',
            'reclean_event_flag', 'sympathy_pm_flag',
            'downtime_category', 'downtime_primary_reason',
            'enrichment_timestamp', 'data_quality_score'
        ]
        
        # Filter to existing columns
        enriched_cols = [col for col in enriched_cols if col in df.columns]
        
        # Rename pm_flex_raw_id to source_pm_flex_raw_id
        load_df = df[enriched_cols].copy()
        load_df = load_df.rename(columns={'pm_flex_raw_id': 'source_pm_flex_raw_id'})
        
        rows_loaded = self.connector.load_dataframe(
            df=load_df,
            table_name='pm_flex_enriched',
            schema=self.schema_name,
            if_exists='append'
        )
        
        return rows_loaded
    
    def _create_downtime_summary(self, df: pd.DataFrame) -> int:
        """
        Create aggregated downtime summary.
        
        Args:
            df: Enriched DataFrame
            
        Returns:
            Number of summary rows created
        """
        # Group by FACILITY, CEID, WW, AltairFlag
        summary = df.groupby([
            'FACILITY', 'CEID', 'ww_year', 'ww_number', 'YEARWW', 'AltairFlag'
        ]).agg({
            'pm_flex_raw_id': 'count',  # Total PM events
            'scheduled_flag': [
                ('scheduled_count', lambda x: (x == 1).sum()),
                ('unscheduled_count', lambda x: (x == 0).sum())
            ],
            'pm_timing_classification': [
                ('early_count', lambda x: (x == 'Early').sum()),
                ('on_time_count', lambda x: (x == 'On-Time').sum()),
                ('late_count', lambda x: (x == 'Late').sum()),
                ('overdue_count', lambda x: (x == 'Overdue').sum())
            ],
            'DOWN_WINDOW_DURATION_HR': [
                ('total_downtime', 'sum'),
                ('avg_downtime', 'mean')
            ],
            'CUSTOM_DELTA': [
                ('avg_pm_life', 'mean'),
                ('median_pm_life', 'median'),
                ('pm_life_std_dev', 'std')
            ]
        }).reset_index()
        
        # Flatten multi-level columns
        summary.columns = ['_'.join(col).strip('_') for col in summary.columns.values]
        
        # Rename columns
        summary = summary.rename(columns={
            'pm_flex_raw_id_count': 'total_pm_events',
            'scheduled_flag_scheduled_count': 'scheduled_pm_count',
            'scheduled_flag_unscheduled_count': 'unscheduled_pm_count',
            'pm_timing_classification_early_count': 'early_pm_count',
            'pm_timing_classification_on_time_count': 'on_time_pm_count',
            'pm_timing_classification_late_count': 'late_pm_count',
            'pm_timing_classification_overdue_count': 'overdue_pm_count',
            'DOWN_WINDOW_DURATION_HR_total_downtime': 'total_downtime_hours',
            'DOWN_WINDOW_DURATION_HR_avg_downtime': 'avg_downtime_hours',
            'CUSTOM_DELTA_avg_pm_life': 'avg_pm_life',
            'CUSTOM_DELTA_median_pm_life': 'median_pm_life',
            'CUSTOM_DELTA_pm_life_std_dev': 'pm_life_std_dev'
        })
        
        # Calculate rates
        summary['unscheduled_pm_rate'] = (
            summary['unscheduled_pm_count'] / summary['total_pm_events']
        ).fillna(0)
        
        summary['early_pm_rate'] = (
            summary['early_pm_count'] / summary['total_pm_events']
        ).fillna(0)
        
        summary['overdue_pm_rate'] = (
            summary['overdue_pm_count'] / summary['total_pm_events']
        ).fillna(0)
        
        # Calculate scheduled downtime (approximation)
        summary['scheduled_downtime_hours'] = (
            summary['total_downtime_hours'] * 
            (summary['scheduled_pm_count'] / summary['total_pm_events'])
        ).fillna(0)
        
        summary['unscheduled_downtime_hours'] = (
            summary['total_downtime_hours'] - summary['scheduled_downtime_hours']
        )
        
        # Calculate variance
        summary['pm_life_variance'] = summary['pm_life_std_dev'] ** 2
        
        # Add timestamp
        summary['calculation_timestamp'] = datetime.now()
        
        # Add record count
        summary['record_count'] = summary['total_pm_events']
        
        # Load to database
        rows_loaded = self.connector.load_dataframe(
            df=summary,
            table_name='pm_flex_downtime_summary',
            schema=self.schema_name,
            if_exists='append'
        )
        
        return rows_loaded
    
    def _analyze_chronic_tools(self, df: pd.DataFrame) -> int:
        """
        Analyze and identify chronic tools.
        
        Args:
            df: Enriched DataFrame
            
        Returns:
            Number of chronic tool records created
        """
        # Get date range for analysis
        analysis_start = df['TXN_DATE'].min().date()
        analysis_end = df['TXN_DATE'].max().date()
        weeks_analyzed = ((analysis_end - analysis_start).days / 7)
        
        # Analyze by entity
        chronic_df = self.chronic_analyzer.analyze_by_entity(df)
        
        # Add analysis period info
        chronic_df['analysis_start_date'] = analysis_start
        chronic_df['analysis_end_date'] = analysis_end
        chronic_df['weeks_analyzed'] = int(weeks_analyzed)
        chronic_df['calculation_timestamp'] = datetime.now()
        
        # Load to database
        rows_loaded = self.connector.load_dataframe(
            df=chronic_df,
            table_name='pm_flex_chronic_tools',
            schema=self.schema_name,
            if_exists='append'
        )
        
        return rows_loaded
    
    def _populate_dim_date(self):
        """
        Populate DimDate table with Intel WW calendar.
        
        Only populates if table is empty or missing recent dates.
        """
        # Check if DimDate needs population
        check_query = "SELECT COUNT(*) as cnt FROM dbo.DimDate"
        result = self.connector.fetch_dataframe(check_query)
        
        if result.iloc[0]['cnt'] > 0:
            self.logger.info("DimDate already populated, skipping")
            return
        
        self.logger.info("Populating DimDate with Intel WW calendar...")
        
        # Generate calendar for current year - 2 to current year + 2
        current_year = datetime.now().year
        start_fy = current_year - 2
        num_years = 5
        
        calendar_df = get_intel_ww_calendar(start_fy, num_years)
        
        # Transform to match DimDate schema
        dim_date = pd.DataFrame({
            'date_key': calendar_df['DATE'].dt.strftime('%Y%m%d').astype(int),
            'date_value': calendar_df['DATE'],
            'day_of_year': calendar_df['DOY'],
            'fiscal_year': calendar_df['FY'],
            'fiscal_quarter': ((calendar_df['WK'] - 1) // 13) + 1,
            'fiscal_month': ((calendar_df['WK'] - 1) // 4) + 1,
            'fiscal_week': calendar_df['WK'],
            'work_week': calendar_df['WW'],
            'day_of_week': calendar_df['DAYOFWEEK'],
            'day_name': calendar_df['DATE'].dt.day_name(),
            'is_weekend': (calendar_df['DAYOFWEEK'] >= 5).astype(int)
        })
        
        # Load to database
        rows_loaded = self.connector.load_dataframe(
            df=dim_date,
            table_name='DimDate',
            schema=self.schema_name,
            if_exists='append'
        )
        
        self.logger.info(f"DimDate populated with {rows_loaded} dates")
