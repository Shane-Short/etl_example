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
    
    for table in tables:
        try:
            # Delete
            delete_query = f"DELETE FROM dbo.{table} WHERE {where_clause}"
            self.connector.execute_query(delete_query)
            self.logger.info(f"  Deleted existing rows from {table}")
        except Exception as e:
            # Table might not exist yet on first run, or might be empty
            self.logger.debug(f"  No rows to delete from {table}: {str(e)}")
    
    self.logger.info("Deletion complete - ready for fresh insert")
