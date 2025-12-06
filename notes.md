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
