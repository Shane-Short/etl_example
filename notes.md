def _load_enriched(self, df: pd.DataFrame) -> int:
    """
    Load enriched data to pm_flex_enriched table.
    """
    # Prepare data for loading
    load_df = df.copy()
    
    # Rename pm_flex_raw_id to source_pm_flex_raw_id for foreign key
    if 'pm_flex_raw_id' in load_df.columns:
        load_df = load_df.rename(columns={'pm_flex_raw_id': 'source_pm_flex_raw_id'})
    
    # Remove columns that exist in Bronze but have different names in Silver
    columns_to_drop = ['load_timestamp']
    for col in columns_to_drop:
        if col in load_df.columns:
            load_df = load_df.drop(columns=[col])
    
    # ===== ADD THIS SECTION: DATA TYPE CONVERSION =====
    self.logger.info("Converting data types for SQL Server compatibility...")
    
    # Numeric columns that should be FLOAT
    float_columns = [
        'PREV_ATTRIBUTE_VALUE', 'ATTRIBUTE_VALUE', 'NEXT_ATTRIBUTE_VALUE',
        'CUSTOM_DELTA', 'Lower_IQR_Limit_Delta', 'Median_Delta', 'Delta_75th_Percentile',
        'COUNTER_UPPER_VALUE', 'UPPER_LIMIT_FACILITY', 'upper_limit_perc_target',
        'CKL_DURATION_IN_HOURS', 'MIN_OF_CKL_START_END_DISTANCE_TO_TXN_DATE',
        'CL_NAME_SIMILARITY_SCORE', 'DURATION_IN_HOURS_75TH_PERCENTILE',
        'pm_cycle_utilization', 'Down_Window_Reclean_Rate',
        'DOWN_WINDOW_DURATION_OUTLIER_THRESHOLD_FOR_TOOLSET',
        'DOWN_WINDOW_DURATION_HR', 'UPPER_LIMIT_THRESHOLD', 'UPPER_VALUE_THRESHOLD',
        'VALUE_LOSS_AT_PMRESET', 'PM_REDUCTION_ROI', 'NORMALIZING_FACTOR',
        'PM_REDUCTION_ROI_NORMALIZED', 'G2G_PER_PM', 'EQUIPMENT_DOWNTIME_ROI_Hrs',
        'PART_COST_PER_PM', 'PART_COST_SAVING_ROI', 'PM_DURATION', 'MTS_NEEDED',
        'LABORHOUR_PER_PM', 'LABOR_HOUR_ROI_Hrs', 'HEADCOUNT_ROI',
        'pm_life_vs_target', 'pm_life_vs_target_pct', 'pm_cycle_efficiency',
        'data_quality_score'
    ]
    
    for col in float_columns:
        if col in load_df.columns:
            load_df[col] = pd.to_numeric(load_df[col], errors='coerce')
    
    # Integer columns that should be INT
    int_columns = [
        'NUM_STEPS_IN_CL', 'num_of_resets_on_parent_txndate', 'PMCycle_Counter_by_UEI_AttrName',
        'ww_year', 'ww_number', 'fiscal_quarter', 'fiscal_month',
        'pm_duration_outlier_flag', 'reclean_event_flag', 'sympathy_pm_flag'
    ]
    
    for col in int_columns:
        if col in load_df.columns:
            load_df[col] = pd.to_numeric(load_df[col], errors='coerce').astype('Int64')
    
    # DateTime columns
    datetime_columns = [
        'TXN_DATE', 'CKL_START_TIME', 'CKL_END_TIME',
        'DOWN_WINDOW_START_TXN_DATE', 'DOWN_WINDOW_END_TXN_DATE',
        'enrichment_timestamp'
    ]
    
    for col in datetime_columns:
        if col in load_df.columns:
            load_df[col] = pd.to_datetime(load_df[col], errors='coerce')
    
    # String columns - ensure they're actually strings, not objects
    string_columns = [
        'ENTITY', 'FACILITY', 'UNIQUE_ENTITY_ID', 'SUPPLIER', 'FUNCTIONAL_AREA',
        'TOOLSET', 'CEID', 'VFMFGID', 'CUSTOM_MODULE_GROUP', 'PM_NAME',
        'ATTRIBUTE_NAME', 'YEARWW', 'CHECKLIST_NAME', 'DOWNTIME_TYPE',
        'DOWNTIME_CLASS', 'DOWNTIME_SUBCLASS', 'pm_timing_classification',
        'scheduled_category', 'downtime_category', 'downtime_primary_reason',
        'AltairFlag'
    ]
    
    for col in string_columns:
        if col in load_df.columns:
            load_df[col] = load_df[col].astype(str)
            # Replace 'nan' and 'None' strings with actual None
            load_df[col] = load_df[col].replace(['nan', 'None', '<NA>'], None)
    
    self.logger.info("Data type conversion complete")
    # ===== END DATA TYPE CONVERSION =====
    
    # Log what we're about to load
    self.logger.info(f"Preparing to load {len(load_df):,} rows to pm_flex_enriched")
    self.logger.info(f"DataFrame has {len(load_df.columns)} columns")
    
    try:
        rows_loaded = self.connector.load_dataframe(
            df=load_df,
            table_name='pm_flex_enriched',
            schema=self.schema_name,
            if_exists='append',
            chunksize=1000
        )
        
        self.logger.info(f"Successfully loaded {rows_loaded:,} rows to pm_flex_enriched")
        return rows_loaded
        
    except Exception as e:
        self.logger.error(f"Failed to load enriched data: {str(e)}")
        raise
