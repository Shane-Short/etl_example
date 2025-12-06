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
    
    # Log actual columns for debugging
    self.logger.debug(f"CEID KPI columns after aggregation: {list(ceid_kpis.columns)}")
    
    # Smart column renaming - find columns by pattern
    new_columns = {}
    for col in ceid_kpis.columns:
        if col in ['FACILITY', 'CEID', 'YEARWW']:
            continue  # Keep grouping columns as-is
        elif 'ENTITY' in col and 'count' in col:
            new_columns[col] = 'total_pm_events'
        elif 'CUSTOM_DELTA' in col and 'mean' in col:
            new_columns[col] = 'avg_pm_life'
        elif 'CUSTOM_DELTA' in col and 'median' in col:
            new_columns[col] = 'median_pm_life'
        elif 'CUSTOM_DELTA' in col and 'std' in col:
            new_columns[col] = 'pm_life_std_dev'
        elif 'DOWN_WINDOW_DURATION_HR' in col and 'sum' in col:
            new_columns[col] = 'total_downtime_hours'
        elif 'DOWN_WINDOW_DURATION_HR' in col and 'mean' in col:
            new_columns[col] = 'avg_downtime_hours'
        elif 'DOWN_WINDOW_DURATION_HR' in col and 'median' in col:
            new_columns[col] = 'median_downtime_hours'
        elif 'scheduled_flag' in col and '<lambda' in col:
            new_columns[col] = 'scheduled_pm_count'
        elif 'early_count' in col:
            new_columns[col] = 'early_pm_count'
        elif 'on_time_count' in col:
            new_columns[col] = 'on_time_pm_count'
        elif 'late_count' in col:
            new_columns[col] = 'late_pm_count'
        elif 'overdue_count' in col:
            new_columns[col] = 'overdue_pm_count'
        elif 'reclean_event_flag' in col and '<lambda' in col:
            new_columns[col] = 'reclean_count'
        elif 'sympathy_pm_flag' in col and '<lambda' in col:
            new_columns[col] = 'sympathy_pm_count'
        elif 'AltairFlag' in col and 'first' in col:
            new_columns[col] = 'AltairFlag'
        elif 'ww_year' in col and 'first' in col:
            new_columns[col] = 'ww_year'
        elif 'ww_number' in col and 'first' in col:
            new_columns[col] = 'ww_number'
    
    ceid_kpis = ceid_kpis.rename(columns=new_columns)
    self.logger.debug(f"CEID KPI columns after rename: {list(ceid_kpis.columns)}")
    
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
