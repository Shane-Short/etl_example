def _load_enriched(self, df: pd.DataFrame) -> int:
    """
    Load enriched data to pm_flex_enriched table.
    
    Args:
        df: Enriched DataFrame (should contain ALL original columns + enriched columns)
        
    Returns:
        Number of rows loaded
    """
    # Prepare data for loading
    load_df = df.copy()
    
    # Rename pm_flex_raw_id to source_pm_flex_raw_id for foreign key
    if 'pm_flex_raw_id' in load_df.columns:
        load_df = load_df.rename(columns={'pm_flex_raw_id': 'source_pm_flex_raw_id'})
    
    # Remove columns that exist in Bronze but have different names in Silver
    columns_to_drop = ['load_timestamp']  # Bronze has this, Silver uses enrichment_timestamp
    for col in columns_to_drop:
        if col in load_df.columns:
            load_df = load_df.drop(columns=[col])
    
    # Log what we're about to load
    self.logger.info(f"Preparing to load {len(load_df):,} rows to pm_flex_enriched")
    self.logger.info(f"DataFrame has {len(load_df.columns)} columns")
    
    # List of all columns (for debugging if needed)
    self.logger.debug(f"Column list: {list(load_df.columns)[:30]}...")  # First 30
    
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
        
        # Check for column mismatch
        if "Invalid column name" in str(e):
            # Extract the problematic column from error message
            self.logger.error("Column mismatch detected!")
            self.logger.error(f"DataFrame columns: {list(load_df.columns)}")
        
        raise
