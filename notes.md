def _create_downtime_summary(self, df: pd.DataFrame) -> int:
    """
    Create aggregated downtime summary.
    
    Args:
        df: Enriched DataFrame
        
    Returns:
        Number of summary rows created
    """
    # Group by ENTITY, FACILITY, CEID, YEARWW, DOWNTIME_TYPE, DOWNTIME_CLASS, AltairFlag
    summary = df.groupby([
        'ENTITY', 'FACILITY', 'CEID', 'YEARWW', 'DOWNTIME_TYPE', 'DOWNTIME_CLASS', 'AltairFlag'
    ]).agg({
        'pm_flex_raw_id': 'count',  # Total PM events / record count
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
            ('avg_downtime', 'mean'),
            ('median_downtime', 'median'),
            ('min_downtime', 'min'),
            ('max_downtime', 'max'),
            ('std_downtime', 'std'),
            ('count_downtime', 'count')
        ],
        'CUSTOM_DELTA': [
            ('avg_pm_life', 'mean'),
            ('median_pm_life', 'median'),
            ('pm_life_std_dev', 'std')
        ],
        'reclean_event_flag': lambda x: (x == 1).sum(),
        'ww_year': 'first',
        'ww_number': 'first'
    }).reset_index()
    
    # Flatten multi-level columns
    summary.columns = ['_'.join(str(col)).strip('_') if isinstance(col, tuple) else col 
                       for col in summary.columns.values]
    
    # Rename columns to match schema
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
        'DOWN_WINDOW_DURATION_HR_median_downtime': 'median_downtime_hours',
        'DOWN_WINDOW_DURATION_HR_min_downtime': 'min_downtime_hours',
        'DOWN_WINDOW_DURATION_HR_max_downtime': 'max_downtime_hours',
        'DOWN_WINDOW_DURATION_HR_std_downtime': 'downtime_std_dev',
        'DOWN_WINDOW_DURATION_HR_count_downtime': 'downtime_count',
        'CUSTOM_DELTA_avg_pm_life': 'avg_pm_life',
        'CUSTOM_DELTA_median_pm_life': 'median_pm_life',
        'CUSTOM_DELTA_pm_life_std_dev': 'pm_life_std_dev',
        'reclean_event_flag_<lambda>': 'reclean_events',
        'ww_year_first': 'ww_year',
        'ww_number_first': 'ww_number'
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
    
    # Calculate scheduled/unscheduled downtime (proportional approximation)
    summary['scheduled_downtime_hours'] = (
        summary['total_downtime_hours'] * 
        (summary['scheduled_pm_count'] / summary['total_pm_events'])
    ).fillna(0)
    
    summary['unscheduled_downtime_hours'] = (
        summary['total_downtime_hours'] - summary['scheduled_downtime_hours']
    )
    
    # Calculate variance
    summary['pm_life_variance'] = summary['pm_life_std_dev'] ** 2
    summary['downtime_variance'] = summary['downtime_std_dev'] ** 2
    
    # Calculate reclean rate
    summary['reclean_rate'] = (
        summary['reclean_events'] / summary['total_pm_events']
    ).fillna(0)
    
    # Add record count (same as total_pm_events)
    summary['record_count'] = summary['total_pm_events']
    
    # Add timestamp
    summary['calculation_timestamp'] = datetime.now()
    
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
    analysis_start = df['TXN_DATE'].min()
    analysis_end = df['TXN_DATE'].max()
    weeks_analyzed = int(((analysis_end - analysis_start).days / 7))
    
    # Group by ENTITY, FACILITY, CEID, YEARWW to create chronic analysis
    chronic_df = df.groupby(['ENTITY', 'FACILITY', 'CEID', 'YEARWW']).agg({
        'pm_flex_raw_id': 'count',  # total_pm_events
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
        'CUSTOM_DELTA': ['mean', 'median', 'min', 'max', 'std'],
        'DOWN_WINDOW_DURATION_HR': ['sum', 'mean', 'median', 'min', 'max', 'std'],
        'reclean_event_flag': lambda x: (x == 1).sum(),
        'sympathy_pm_flag': lambda x: (x == 1).sum(),
        'AltairFlag': 'first',
        'ww_year': 'first',
        'ww_number': 'first'
    }).reset_index()
    
    # Flatten columns
    chronic_df.columns = ['_'.join(str(col)).strip('_') if isinstance(col, tuple) else col 
                          for col in chronic_df.columns.values]
    
    # Rename
    chronic_df = chronic_df.rename(columns={
        'pm_flex_raw_id_count': 'total_pm_events',
        'scheduled_flag_scheduled_count': 'scheduled_pm_count',
        'scheduled_flag_unscheduled_count': 'unscheduled_pm_count',
        'pm_timing_classification_early_count': 'early_pm_count',
        'pm_timing_classification_on_time_count': 'on_time_pm_count',
        'pm_timing_classification_late_count': 'late_pm_count',
        'pm_timing_classification_overdue_count': 'overdue_pm_count',
        'CUSTOM_DELTA_mean': 'avg_pm_life',
        'CUSTOM_DELTA_median': 'median_pm_life',
        'CUSTOM_DELTA_min': 'min_pm_life',
        'CUSTOM_DELTA_max': 'max_pm_life',
        'CUSTOM_DELTA_std': 'pm_life_std_dev',
        'DOWN_WINDOW_DURATION_HR_sum': 'total_downtime_hours',
        'DOWN_WINDOW_DURATION_HR_mean': 'avg_downtime_hours',
        'DOWN_WINDOW_DURATION_HR_median': 'median_downtime_hours',
        'DOWN_WINDOW_DURATION_HR_min': 'min_downtime_hours',
        'DOWN_WINDOW_DURATION_HR_max': 'max_downtime_hours',
        'DOWN_WINDOW_DURATION_HR_std': 'downtime_std_dev',
        'reclean_event_flag_<lambda>': 'reclean_count',
        'sympathy_pm_flag_<lambda>': 'sympathy_pm_count',
        'AltairFlag_first': 'AltairFlag',
        'ww_year_first': 'ww_year',
        'ww_number_first': 'ww_number'
    })
    
    # Calculate rates
    chronic_df['unscheduled_pm_rate'] = (
        chronic_df['unscheduled_pm_count'] / chronic_df['total_pm_events']
    ).fillna(0)
    
    chronic_df['early_pm_rate'] = (
        chronic_df['early_pm_count'] / chronic_df['total_pm_events']
    ).fillna(0)
    
    chronic_df['on_time_pm_rate'] = (
        chronic_df['on_time_pm_count'] / chronic_df['total_pm_events']
    ).fillna(0)
    
    chronic_df['late_pm_rate'] = (
        chronic_df['late_pm_count'] / chronic_df['total_pm_events']
    ).fillna(0)
    
    chronic_df['overdue_pm_rate'] = (
        chronic_df['overdue_pm_count'] / chronic_df['total_pm_events']
    ).fillna(0)
    
    chronic_df['reclean_rate'] = (
        chronic_df['reclean_count'] / chronic_df['total_pm_events']
    ).fillna(0)
    
    chronic_df['sympathy_pm_rate'] = (
        chronic_df['sympathy_pm_count'] / chronic_df['total_pm_events']
    ).fillna(0)
    
    # Calculate downtime metrics
    chronic_df['scheduled_downtime_hours'] = (
        chronic_df['total_downtime_hours'] * 
        (chronic_df['scheduled_pm_count'] / chronic_df['total_pm_events'])
    ).fillna(0)
    
    chronic_df['unscheduled_downtime_hours'] = (
        chronic_df['total_downtime_hours'] - chronic_df['scheduled_downtime_hours']
    )
    
    chronic_df['avg_downtime_hours_per_pm'] = (
        chronic_df['total_downtime_hours'] / chronic_df['total_pm_events']
    ).fillna(0)
    
    chronic_df['pm_life_variance'] = chronic_df['pm_life_std_dev'] ** 2
    chronic_df['downtime_variance'] = chronic_df['downtime_std_dev'] ** 2
    
    # Placeholder values for columns we don't calculate yet
    chronic_df['total_chambers'] = 1  # Default to 1
    chronic_df['chronic_chambers'] = 0  # Will be calculated later if needed
    
    # Add analysis period info
    chronic_df['analysis_start_date'] = analysis_start
    chronic_df['analysis_end_date'] = analysis_end
    chronic_df['weeks_analyzed'] = weeks_analyzed
    chronic_df['calculation_timestamp'] = datetime.now()
    chronic_df['record_count'] = chronic_df['total_pm_events']
    
    # Load to database
    rows_loaded = self.connector.load_dataframe(
        df=chronic_df,
        table_name='pm_flex_chronic_tools',
        schema=self.schema_name,
        if_exists='append'
    )
    
    return rows_loaded




    
