"""
Populate DimDate dimension table with complete date attributes.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any
import logging
from connectors.sqlserver_connector import SQLServerConnector
from utils.helpers import get_intel_work_week


def populate_dim_date(
    start_year: int = 2020,
    end_year: int = 2030,
    connector: SQLServerConnector = None
) -> int:
    """
    Populate DimDate table with complete date dimension.
    
    Generates all dates from start_year to end_year with:
    - Standard calendar attributes
    - Intel fiscal calendar attributes
    - Work week mappings
    
    Args:
        start_year: First year to include
        end_year: Last year to include
        connector: Database connector (creates new if not provided)
        
    Returns:
        Number of rows inserted
    """
    logger = logging.getLogger("pm_flex_pipeline.dim_date")
    
    logger.info(f"Generating date dimension from {start_year} to {end_year}...")
    
    # Generate all dates in range
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    
    date_list = []
    current_date = start_date
    
    while current_date <= end_date:
        # Calculate all date attributes
        date_attrs = calculate_date_attributes(current_date)
        date_list.append(date_attrs)
        
        # Move to next day
        current_date += timedelta(days=1)
    
    logger.info(f"Generated {len(date_list):,} date records")
    
    # Create DataFrame
    df = pd.DataFrame(date_list)
    
    # Load to database
    close_connector = False
    if connector is None:
        connector = SQLServerConnector()
        close_connector = True
    
    try:
        # Truncate existing data
        logger.info("Truncating DimDate table...")
        connector.truncate_table('DimDate', 'dbo')
        
        # Load new data
        logger.info("Loading date dimension to database...")
        rows_loaded = connector.load_dataframe(
            df=df,
            table_name='DimDate',
            schema='dbo',
            if_exists='append',
            chunksize=5000
        )
        
        logger.info(f"Successfully loaded {rows_loaded:,} dates to DimDate")
        return rows_loaded
        
    finally:
        if close_connector:
            connector.close()


def calculate_date_attributes(date: datetime) -> Dict[str, Any]:
    """
    Calculate all attributes for a single date.
    
    Args:
        date: Date to calculate attributes for
        
    Returns:
        Dictionary with all date attributes
    """
    # Basic calendar attributes
    date_key = int(date.strftime('%Y%m%d'))
    year = date.year
    month = date.month
    day = date.day
    
    # Quarter (1-4)
    quarter = (month - 1) // 3 + 1
    
    # Week of year (ISO week)
    week_of_year = date.isocalendar()[1]
    
    # Day of year (1-365/366)
    day_of_year = date.timetuple().tm_yday
    
    # Day of week (0=Monday, 6=Sunday)
    day_of_week = date.weekday()
    
    # Day name
    day_name = date.strftime('%A')
    
    # Month name
    month_name = date.strftime('%B')
    
    # Is weekend (Saturday=5, Sunday=6)
    is_weekend = 1 if day_of_week >= 5 else 0
    
    # Intel fiscal calendar
    # Intel fiscal year starts last Saturday of December
    fiscal_year = calculate_intel_fiscal_year(date)
    fiscal_quarter = calculate_intel_fiscal_quarter(date)
    fiscal_month = month  # Simplified - adjust if Intel has different fiscal months
    
    # Intel work week (e.g., "2025WW46")
    try:
        work_week = get_intel_work_week(date)
        # Parse work week to get year and number
        if work_week and 'WW' in work_week:
            ww_year = int(work_week[:4])
            ww_number = int(work_week[6:])
        else:
            ww_year = None
            ww_number = None
    except:
        work_week = None
        ww_year = None
        ww_number = None
    
    return {
        'date_key': date_key,
        'date_value': date,
        'year': year,
        'quarter': quarter,
        'month': month,
        'week_of_year': week_of_year,
        'day_of_year': day_of_year,
        'day_of_month': day,
        'day_of_week': day_of_week,
        'day_name': day_name,
        'month_name': month_name,
        'is_weekend': is_weekend,
        'is_holiday': 0,  # Can be updated later with holiday calendar
        'fiscal_year': fiscal_year,
        'fiscal_quarter': fiscal_quarter,
        'fiscal_month': fiscal_month,
        'fiscal_week': work_week,
        'work_week': work_week,
        'ww_year': ww_year,
        'ww_number': ww_number
    }


def calculate_intel_fiscal_year(date: datetime) -> int:
    """
    Calculate Intel fiscal year for a given date.
    
    Intel fiscal year starts on the last Saturday of December.
    
    Args:
        date: Date to calculate fiscal year for
        
    Returns:
        Fiscal year (e.g., 2025)
    """
    # Find last Saturday of December of current year
    dec_31 = datetime(date.year, 12, 31)
    
    # Work backwards to find last Saturday
    days_back = (dec_31.weekday() + 2) % 7  # Saturday is 5, so we need to go back
    last_saturday = dec_31 - timedelta(days=days_back)
    
    # If date is on or after last Saturday of Dec, it's next fiscal year
    if date >= last_saturday:
        return date.year + 1
    else:
        return date.year


def calculate_intel_fiscal_quarter(date: datetime) -> int:
    """
    Calculate Intel fiscal quarter (simplified version).
    
    Args:
        date: Date to calculate fiscal quarter for
        
    Returns:
        Fiscal quarter (1-4)
    """
    fiscal_year = calculate_intel_fiscal_year(date)
    
    # Simplified: Use calendar quarter adjusted for fiscal year start
    # Fiscal Q1 starts with fiscal year (late Dec)
    # This is simplified - actual Intel fiscal calendar may be different
    
    if date.month >= 1 and date.month <= 3:
        return 2
    elif date.month >= 4 and date.month <= 6:
        return 3
    elif date.month >= 7 and date.month <= 9:
        return 4
    else:  # Oct, Nov, Dec
        # Check if before or after fiscal year boundary
        last_sat = datetime(date.year, 12, 31)
        days_back = (last_sat.weekday() + 2) % 7
        last_saturday = last_sat - timedelta(days=days_back)
        
        if date >= last_saturday:
            return 1  # New fiscal year
        else:
            return 4  # End of current fiscal year






def run_silver_etl(
    incremental: bool = True,
    full_refresh: bool = False
) -> dict:
    """Execute silver layer ETL."""
    
    # ... existing setup code ...
    
    try:
        # Handle full refresh
        if full_refresh:
            logger.info("Full refresh requested - truncating Silver layer tables")
            # ... existing truncation code ...
        
        # ===== ADD THIS SECTION =====
        # Step 1: Populate DimDate (do this first)
        logger.info("=" * 60)
        logger.info("Step 1: Populating DimDate dimension")
        logger.info("=" * 60)
        
        dim_date_rows = populate_dim_date(
            start_year=2020,
            end_year=2030,
            connector=connector
        )
        logger.info(f"DimDate populated with {dim_date_rows:,} rows")
        # ===== END ADD SECTION =====
        
        # Step 2: Load from Bronze and enrich
        logger.info("=" * 60)
        logger.info("Step 2: Loading and enriching PM Flex data")
        logger.info("=" * 60)
        
        # ... rest of existing code ...
