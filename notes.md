def _load_enriched(self, df: pd.DataFrame) -> int:
    """
    Load enriched data to pm_flex_enriched table.
    
    Args:
        df: Enriched DataFrame
        
    Returns:
        Number of rows loaded
    """
    # Prepare data for loading
    load_df = df.copy()
    
    # Rename pm_flex_raw_id to source_pm_flex_raw_id
    if 'pm_flex_raw_id' in load_df.columns:
        load_df = load_df.rename(columns={'pm_flex_raw_id': 'source_pm_flex_raw_id'})
    
    # Remove columns that don't exist in pm_flex_enriched table
    # (like load_timestamp from Bronze which becomes enrichment_timestamp in Silver)
    drop_cols = ['load_timestamp', 'source_file', 'source_ww']
    for col in drop_cols:
        if col in load_df.columns:
            load_df = load_df.drop(columns=[col])
    
    # Get list of columns in the target table
    table_cols_query = """
    SELECT COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'pm_flex_enriched' 
    AND TABLE_SCHEMA = 'dbo'
    AND COLUMN_NAME NOT IN ('pm_flex_enriched_id')  -- Exclude identity column
    ORDER BY ORDINAL_POSITION
    """
    
    table_cols_df = self.connector.fetch_dataframe(table_cols_query)
    table_columns = table_cols_df['COLUMN_NAME'].tolist()
    
    # Only keep columns that exist in both the DataFrame and the target table
    final_cols = [col for col in load_df.columns if col in table_columns]
    load_df = load_df[final_cols]
    
    self.logger.info(f"Loading {len(load_df):,} rows with {len(final_cols)} columns to pm_flex_enriched")
    
    rows_loaded = self.connector.load_dataframe(
        df=load_df,
        table_name='pm_flex_enriched',
        schema=self.schema_name,
        if_exists='append'
    )
    
    return rows_loaded
