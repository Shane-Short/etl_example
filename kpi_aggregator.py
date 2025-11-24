"""
KPI aggregation module for gold layer.

Creates fact tables with pre-calculated KPIs optimized for Power BI:
- fact_pm_kpis_by_site_ww
- fact_pm_kpis_by_ceid_ww
- fact_part_replacement_summary
- fact_chronic_tools_history
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional, Dict, Any, List
import logging
import time

from connectors import SQLServerConnector
from utils.logger import log_execution_time
from utils.exceptions import TransformationError


class KPIAggregator:
    """
    Aggregates silver layer data into gold layer KPI fact tables.
    """
    
    def __init__(self, connector: Optional[SQLServerConnector] = None):
        """
        Initialize KPI aggregator.
        
        Args:
            connector: SQL Server connector (creates new one if not provided)
        """
        self.logger = logging.getLogger("pm_flex_pipeline.gold")
        
        if connector is None:
            connector = SQLServerConnector()
        
        self.connector = connector
        self.schema_name = "dbo"
    
    @log_execution_time(logging.getLogger("pm_flex_pipeline.gold"))
    def create_kpi_tables(
        self,
        start_ww: Optional[str] = None,
        end_ww: Optional[str] = None,
        incremental: bool = True
    ) -> Dict[str, Any]:
        """
        Main KPI aggregation process: silver → gold fact tables.
        
        Args:
            start_ww: Start work week (e.g., '2025WW20')
            end_ww: End work week (e.g., '2025WW22')
            incremental: If True, only process new work weeks
            
        Returns:
            Dictionary with processing statistics
        """
        start_time = time.time()
        
        self.logger.info("=" * 60)
        self.logger.info("Gold Layer KPI Aggregation - STARTED")
        self.logger.info("=" * 60)
        
        try:
            # Step 1: Load enriched data from silver
            self.logger.info("STEP 1: Loading enriched data from silver layer")
            enriched_df = self._load_enriched_data(start_ww, end_ww, incremental)
            
            if len(enriched_df) == 0:
                self.logger.warning("No data to process")
                return {
                    "rows_processed": 0,
                    "execution_time_seconds": 0,
                    "status": "NO_DATA"
                }
            
            self.logger.info(f"Loaded {len(enriched_df)} enriched rows")
            
            # Step 2: Create site-level KPIs
            self.logger.info("STEP 2: Creating site-level KPIs")
            site_kpi_rows = self._create_site_kpis(enriched_df)
            
            # Step 3: Create CEID-level KPIs
            self.logger.info("STEP 3: Creating CEID-level KPIs")
            ceid_kpi_rows = self._create_ceid_kpis(enriched_df)
            
            # Step 4: Create part replacement summary
            self.logger.info("STEP 4: Creating part replacement summary")
            part_summary_rows = self._create_part_summary(enriched_df)
            
            # Step 5: Create chronic tools history
            self.logger.info("STEP 5: Creating chronic tools history")
            chronic_history_rows = self._create_chronic_history(enriched_df)
            
            # Step 6: Calculate rolling averages
            self.logger.info("STEP 6: Calculating rolling averages")
            self._calculate_rolling_averages()
            
            # Calculate statistics
            execution_time = time.time() - start_time
            
            stats = {
                "rows_processed": len(enriched_df),
                "site_kpi_rows": site_kpi_rows,
                "ceid_kpi_rows": ceid_kpi_rows,
                "part_summary_rows": part_summary_rows,
                "chronic_history_rows": chronic_history_rows,
                "execution_time_seconds": execution_time,
                "status": "SUCCESS"
            }
            
            self.logger.info("=" * 60)
            self.logger.info("Gold Layer KPI Aggregation - COMPLETED")
            self.logger.info(f"Site KPIs: {site_kpi_rows:,}")
            self.logger.info(f"CEID KPIs: {ceid_kpi_rows:,}")
            self.logger.info(f"Part Summary: {part_summary_rows:,}")
            self.logger.info(f"Chronic History: {chronic_history_rows:,}")
            self.logger.info(f"Execution time: {execution_time:.2f}s")
            self.logger.info("=" * 60)
            
            return stats
            
        except Exception as e:
            self.logger.error(f"KPI aggregation failed: {str(e)}", exc_info=True)
            raise TransformationError(f"Gold layer aggregation failed: {str(e)}")
    
    def _load_enriched_data(
        self,
        start_ww: Optional[str],
        end_ww: Optional[str],
        incremental: bool
    ) -> pd.DataFrame:
        """
        Load enriched data from pm_flex_enriched.
        
        Args:
            start_ww: Start work week filter
            end_ww: End work week filter
            incremental: Only load work weeks not yet in gold tables
            
        Returns:
            DataFrame with enriched data
        """
        # Build query
        if incremental:
            # Only load work weeks not yet processed in gold
            query = """
            SELECT e.*
            FROM dbo.pm_flex_enriched e
            WHERE NOT EXISTS (
                SELECT 1 
                FROM dbo.fact_pm_kpis_by_site_ww f
                WHERE f.YEARWW = e.YEARWW
                  AND f.FACILITY = e.FACILITY
            )
            """
        else:
            query = "SELECT * FROM dbo.pm_flex_enriched"
        
        # Add work week filters if provided
        conditions = []
        if start_ww:
            conditions.append(f"YEARWW >= '{start_ww}'")
        if end_ww:
            conditions.append(f"YEARWW <= '{end_ww}'")
        
        if conditions:
            if incremental:
                query += " AND " + " AND ".join(conditions)
            else:
                query += " WHERE " + " AND ".join(conditions)
        
        df = self.connector.fetch_dataframe(query)
        return df
    
    def _create_site_kpis(self, df: pd.DataFrame) -> int:
        """
        Create fact_pm_kpis_by_site_ww table.
        
        Aggregates by FACILITY and work week.
        
        Args:
            df: Enriched DataFrame
            
        Returns:
            Number of rows created
        """
        self.logger.info("Aggregating site-level KPIs...")
        
        # Group by FACILITY and work week
        site_kpis = df.groupby([
            'FACILITY', 'ww_year', 'ww_number', 'YEARWW'
        ]).agg({
            # PM Event Counts
            'pm_flex_enriched_id': 'count',
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
            # Downtime
            'DOWN_WINDOW_DURATION_HR': [
                ('total_downtime', 'sum'),
                ('avg_downtime', 'mean')
            ],
            # PM Life
            'CUSTOM_DELTA': [
                ('avg_pm_life', 'mean'),
                ('median_pm_life', 'median')
            ],
            'Median_Delta': 'first',  # Target PM life
            'pm_life_vs_target': 'mean',
            # Chronic Tools
            'ENTITY': lambda x: x.nunique()  # Total unique tools
        }).reset_index()
        
        # Flatten multi-level columns
        site_kpis.columns = ['_'.join(col).strip('_') for col in site_kpis.columns.values]
        
        # Rename columns to match schema
        site_kpis = site_kpis.rename(columns={
            'pm_flex_enriched_id_count': 'total_pm_events',
            'scheduled_flag_scheduled_count': 'scheduled_pm_events',
            'scheduled_flag_unscheduled_count': 'unscheduled_pm_events',
            'pm_timing_classification_early_count': 'early_pm_count',
            'pm_timing_classification_on_time_count': 'on_time_pm_count',
            'pm_timing_classification_late_count': 'late_pm_count',
            'pm_timing_classification_overdue_count': 'overdue_pm_count',
            'DOWN_WINDOW_DURATION_HR_total_downtime': 'total_downtime_hours',
            'DOWN_WINDOW_DURATION_HR_avg_downtime': 'avg_downtime_per_pm',
            'CUSTOM_DELTA_avg_pm_life': 'avg_pm_life',
            'CUSTOM_DELTA_median_pm_life': 'median_pm_life',
            'Median_Delta_first': 'target_pm_life',
            'pm_life_vs_target_mean': 'pm_life_variance',
            'ENTITY_<lambda>': 'total_tools_count'
        })
        
        # Calculate rates
        site_kpis['unscheduled_pm_rate'] = (
            site_kpis['unscheduled_pm_events'] / site_kpis['total_pm_events']
        ).fillna(0)
        
        site_kpis['early_pm_rate'] = (
            site_kpis['early_pm_count'] / site_kpis['total_pm_events']
        ).fillna(0)
        
        site_kpis['on_time_pm_rate'] = (
            site_kpis['on_time_pm_count'] / site_kpis['total_pm_events']
        ).fillna(0)
        
        site_kpis['overdue_pm_rate'] = (
            site_kpis['overdue_pm_count'] / site_kpis['total_pm_events']
        ).fillna(0)
        
        # Calculate scheduled vs unscheduled downtime (approximation)
        site_kpis['scheduled_downtime_hours'] = (
            site_kpis['total_downtime_hours'] * 
            (site_kpis['scheduled_pm_events'] / site_kpis['total_pm_events'])
        ).fillna(0)
        
        site_kpis['unscheduled_downtime_hours'] = (
            site_kpis['total_downtime_hours'] - site_kpis['scheduled_downtime_hours']
        )
        
        # Get chronic tools count per site/week
        chronic_counts = df[df['ENTITY'].notna()].groupby([
            'FACILITY', 'ww_year', 'ww_number', 'YEARWW'
        ])['ENTITY'].apply(
            lambda entities: self._count_chronic_tools(entities.unique())
        ).reset_index(name='chronic_tools_count')
        
        # Merge chronic counts
        site_kpis = site_kpis.merge(
            chronic_counts,
            on=['FACILITY', 'ww_year', 'ww_number', 'YEARWW'],
            how='left'
        )
        site_kpis['chronic_tools_count'] = site_kpis['chronic_tools_count'].fillna(0)
        
        # Calculate chronic tools percentage
        site_kpis['chronic_tools_pct'] = (
            site_kpis['chronic_tools_count'] / site_kpis['total_tools_count']
        ).fillna(0)
        
        # Add timestamp
        site_kpis['calculation_timestamp'] = datetime.now()
        
        # Initialize rolling average columns (will be calculated later)
        site_kpis['rolling_4wk_avg_pm_life'] = np.nan
        site_kpis['rolling_4wk_pm_count'] = np.nan
        site_kpis['rolling_4wk_downtime_hours'] = np.nan
        
        # Load to database
        rows_loaded = self.connector.load_dataframe(
            df=site_kpis,
            table_name='fact_pm_kpis_by_site_ww',
            schema=self.schema_name,
            if_exists='append'
        )
        
        return rows_loaded
    
    def _count_chronic_tools(self, entities: List[str]) -> int:
        """
        Count how many entities are chronic tools.
        
        Args:
            entities: List of ENTITY values
            
        Returns:
            Count of chronic tools
        """
        if len(entities) == 0:
            return 0
        
        # Query chronic tools table
        entities_str = "','".join(entities)
        query = f"""
        SELECT COUNT(*) as count
        FROM dbo.pm_flex_chronic_tools
        WHERE ENTITY IN ('{entities_str}')
          AND chronic_flag = 1
        """
        
        try:
            result = self.connector.fetch_dataframe(query)
            return int(result.iloc[0]['count'])
        except:
            return 0
    
    def _create_ceid_kpis(self, df: pd.DataFrame) -> int:
        """
        Create fact_pm_kpis_by_ceid_ww table.
        
        Aggregates by CEID, FACILITY, and work week.
        
        Args:
            df: Enriched DataFrame
            
        Returns:
            Number of rows created
        """
        self.logger.info("Aggregating CEID-level KPIs...")
        
        # Group by CEID, FACILITY, and work week
        ceid_kpis = df.groupby([
            'CEID', 'FACILITY', 'AltairFlag', 'ww_year', 'ww_number', 'YEARWW'
        ]).agg({
            # PM Counts
            'pm_flex_enriched_id': 'count',
            'scheduled_flag': [
                ('scheduled_count', lambda x: (x == 1).sum()),
                ('unscheduled_count', lambda x: (x == 0).sum())
            ],
            # Downtime
            'DOWN_WINDOW_DURATION_HR': 'sum',
            # PM Life
            'CUSTOM_DELTA': ['mean', 'median', 'std'],
            # Tool Counts
            'ENTITY': lambda x: x.nunique()
        }).reset_index()
        
        # Flatten columns
        ceid_kpis.columns = ['_'.join(col).strip('_') for col in ceid_kpis.columns.values]
        
        # Rename columns
        ceid_kpis = ceid_kpis.rename(columns={
            'pm_flex_enriched_id_count': 'total_pm_events',
            'scheduled_flag_scheduled_count': 'scheduled_pm_events',
            'scheduled_flag_unscheduled_count': 'unscheduled_pm_events',
            'DOWN_WINDOW_DURATION_HR_sum': 'total_downtime_hours',
            'CUSTOM_DELTA_mean': 'avg_pm_life',
            'CUSTOM_DELTA_median': 'median_pm_life',
            'CUSTOM_DELTA_std': 'pm_life_std_dev',
            'ENTITY_<lambda>': 'total_chambers'
        })
        
        # Calculate unscheduled downtime
        ceid_kpis['unscheduled_downtime_hours'] = (
            ceid_kpis['total_downtime_hours'] * 
            (ceid_kpis['unscheduled_pm_events'] / ceid_kpis['total_pm_events'])
        ).fillna(0)
        
        # Calculate rates
        ceid_kpis['unscheduled_pm_rate'] = (
            ceid_kpis['unscheduled_pm_events'] / ceid_kpis['total_pm_events']
        ).fillna(0)
        
        ceid_kpis['early_pm_rate'] = 0  # Simplified for CEID level
        ceid_kpis['overdue_pm_rate'] = 0
        
        # Count chronic chambers per CEID
        chronic_chambers = df.groupby([
            'CEID', 'FACILITY', 'AltairFlag', 'ww_year', 'ww_number', 'YEARWW'
        ])['ENTITY'].apply(
            lambda entities: self._count_chronic_tools(entities.unique())
        ).reset_index(name='chronic_chambers')
        
        # Merge chronic counts
        ceid_kpis = ceid_kpis.merge(
            chronic_chambers,
            on=['CEID', 'FACILITY', 'AltairFlag', 'ww_year', 'ww_number', 'YEARWW'],
            how='left'
        )
        ceid_kpis['chronic_chambers'] = ceid_kpis['chronic_chambers'].fillna(0)
        
        # Add timestamp
        ceid_kpis['calculation_timestamp'] = datetime.now()
        
        # Load to database
        rows_loaded = self.connector.load_dataframe(
            df=ceid_kpis,
            table_name='fact_pm_kpis_by_ceid_ww',
            schema=self.schema_name,
            if_exists='append'
        )
        
        return rows_loaded
    
    def _create_part_summary(self, df: pd.DataFrame) -> int:
        """
        Create fact_part_replacement_summary table.
        
        Aggregates part replacement trends by ATTRIBUTE_NAME.
        
        Args:
            df: Enriched DataFrame
            
        Returns:
            Number of rows created
        """
        self.logger.info("Creating part replacement summary...")
        
        # Filter to records with part/attribute information
        parts_df = df[df['ATTRIBUTE_NAME'].notna()].copy()
        
        if len(parts_df) == 0:
            self.logger.warning("No part data to summarize")
            return 0
        
        # Group by part, entity, and work week
        part_summary = parts_df.groupby([
            'ATTRIBUTE_NAME', 'ENTITY', 'FACILITY', 'CEID', 
            'ww_year', 'ww_number', 'YEARWW'
        ]).agg({
            'pm_flex_enriched_id': 'count',
            'CUSTOM_DELTA': ['mean', 'median', 'min', 'max'],
            'TXN_DATE': lambda x: (x.max() - x.min()).days,
            'pm_timing_classification': [
                ('early_count', lambda x: (x == 'Early').sum()),
                ('late_count', lambda x: (x == 'Late').sum() + (x == 'Overdue').sum())
            ]
        }).reset_index()
        
        # Flatten columns
        part_summary.columns = ['_'.join(col).strip('_') for col in part_summary.columns.values]
        
        # Rename columns
        part_summary = part_summary.rename(columns={
            'pm_flex_enriched_id_count': 'replacement_count',
            'CUSTOM_DELTA_mean': 'avg_wafers_at_replacement',
            'CUSTOM_DELTA_median': 'median_wafers_at_replacement',
            'CUSTOM_DELTA_min': 'min_wafers_at_replacement',
            'CUSTOM_DELTA_max': 'max_wafers_at_replacement',
            'TXN_DATE_<lambda>': 'avg_part_life_days',
            'pm_timing_classification_early_count': 'early_replacement_count',
            'pm_timing_classification_late_count': 'late_replacement_count'
        })
        
        # Calculate part life variance
        part_variance = parts_df.groupby([
            'ATTRIBUTE_NAME', 'ENTITY', 'FACILITY', 'CEID',
            'ww_year', 'ww_number', 'YEARWW'
        ])['CUSTOM_DELTA'].std().reset_index(name='part_life_variance')
        
        # Merge variance
        part_summary = part_summary.merge(
            part_variance,
            on=['ATTRIBUTE_NAME', 'ENTITY', 'FACILITY', 'CEID', 
                'ww_year', 'ww_number', 'YEARWW'],
            how='left'
        )
        
        # Add timestamp
        part_summary['calculation_timestamp'] = datetime.now()
        
        # Load to database
        rows_loaded = self.connector.load_dataframe(
            df=part_summary,
            table_name='fact_part_replacement_summary',
            schema=self.schema_name,
            if_exists='append'
        )
        
        return rows_loaded
    
    def _create_chronic_history(self, df: pd.DataFrame) -> int:
        """
        Create fact_chronic_tools_history table.
        
        Tracks chronic tool status over time.
        
        Args:
            df: Enriched DataFrame
            
        Returns:
            Number of rows created
        """
        self.logger.info("Creating chronic tools history...")
        
        # Get chronic tool data from pm_flex_chronic_tools
        chronic_query = """
        SELECT 
            ENTITY,
            FACILITY,
            CEID,
            AltairFlag,
            chronic_flag,
            chronic_score,
            chronic_severity,
            unscheduled_pm_rate,
            pm_life_variance,
            total_pm_events,
            unscheduled_pm_count,
            total_downtime_hours,
            calculation_timestamp
        FROM dbo.pm_flex_chronic_tools
        """
        
        chronic_df = self.connector.fetch_dataframe(chronic_query)
        
        if len(chronic_df) == 0:
            self.logger.warning("No chronic tools data found")
            return 0
        
        # Get work weeks from enriched data
        work_weeks = df[['ww_year', 'ww_number', 'YEARWW']].drop_duplicates()
        
        # Create history records for each entity and work week
        history_records = []
        
        for _, ww_row in work_weeks.iterrows():
            ww_year = ww_row['ww_year']
            ww_number = ww_row['ww_number']
            yearww = ww_row['YEARWW']
            
            # Get chronic status for this week
            for _, chronic_row in chronic_df.iterrows():
                history_records.append({
                    'ENTITY': chronic_row['ENTITY'],
                    'FACILITY': chronic_row['FACILITY'],
                    'CEID': chronic_row['CEID'],
                    'AltairFlag': chronic_row.get('AltairFlag', 'UNKNOWN'),
                    'ww_year': ww_year,
                    'ww_number': ww_number,
                    'YEARWW': yearww,
                    'chronic_flag': chronic_row['chronic_flag'],
                    'chronic_score': chronic_row['chronic_score'],
                    'chronic_severity': chronic_row['chronic_severity'],
                    'unscheduled_pm_count': chronic_row.get('unscheduled_pm_count', 0),
                    'unscheduled_pm_rate': chronic_row.get('unscheduled_pm_rate', 0),
                    'pm_life_variance': chronic_row.get('pm_life_variance', 0),
                    'total_downtime_hours': chronic_row.get('total_downtime_hours', 0),
                    'chronic_score_change': 0.0,  # Will calculate later
                    'status_changed': 0,  # Will calculate later
                    'calculation_timestamp': datetime.now()
                })
        
        if len(history_records) == 0:
            self.logger.warning("No history records to create")
            return 0
        
        history_df = pd.DataFrame(history_records)
        
        # Load to database
        rows_loaded = self.connector.load_dataframe(
            df=history_df,
            table_name='fact_chronic_tools_history',
            schema=self.schema_name,
            if_exists='append'
        )
        
        return rows_loaded
    
    def _calculate_rolling_averages(self):
        """
        Calculate 4-week rolling averages for site KPIs.
        
        Updates fact_pm_kpis_by_site_ww table.
        """
        self.logger.info("Calculating 4-week rolling averages...")
        
        # Use stored procedure if available, otherwise calculate in Python
        try:
            self.connector.execute_query(
                "EXEC dbo.sp_calculate_rolling_averages"
            )
            self.logger.info("Rolling averages calculated via stored procedure")
        except Exception as e:
            self.logger.warning(f"Stored procedure failed, calculating in Python: {str(e)}")
            self._calculate_rolling_averages_python()
    
    def _calculate_rolling_averages_python(self):
        """Calculate rolling averages using Python (fallback method)."""
        # Load current KPIs
        query = """
        SELECT 
            FACILITY,
            ww_year,
            ww_number,
            YEARWW,
            avg_pm_life,
            total_pm_events,
            total_downtime_hours
        FROM dbo.fact_pm_kpis_by_site_ww
        ORDER BY FACILITY, ww_year, ww_number
        """
        
        kpis_df = self.connector.fetch_dataframe(query)
        
        if len(kpis_df) == 0:
            return
        
        # Calculate rolling averages per facility
        for facility in kpis_df['FACILITY'].unique():
            facility_df = kpis_df[kpis_df['FACILITY'] == facility].copy()
            
            # Sort by week
            facility_df = facility_df.sort_values(['ww_year', 'ww_number'])
            
            # Calculate 4-week rolling averages
            facility_df['rolling_4wk_avg_pm_life'] = (
                facility_df['avg_pm_life'].rolling(window=4, min_periods=1).mean()
            )
            facility_df['rolling_4wk_pm_count'] = (
                facility_df['total_pm_events'].rolling(window=4, min_periods=1).sum()
            )
            facility_df['rolling_4wk_downtime_hours'] = (
                facility_df['total_downtime_hours'].rolling(window=4, min_periods=1).sum()
            )
            
            # Update database
            for _, row in facility_df.iterrows():
                update_query = f"""
                UPDATE dbo.fact_pm_kpis_by_site_ww
                SET rolling_4wk_avg_pm_life = {row['rolling_4wk_avg_pm_life']},
                    rolling_4wk_pm_count = {row['rolling_4wk_pm_count']},
                    rolling_4wk_downtime_hours = {row['rolling_4wk_downtime_hours']}
                WHERE FACILITY = '{row['FACILITY']}'
                  AND YEARWW = '{row['YEARWW']}'
                """
                self.connector.execute_query(update_query)
        
        self.logger.info("Rolling averages calculated in Python")
