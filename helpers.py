"""
Helper functions for PM Flex ETL pipeline.

Contains utility functions including Intel's fiscal year week calendar calculation.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Tuple
import re


def get_intel_ww_calendar(start_fy: int, num_of_yr: int) -> pd.DataFrame:
    """
    Create an Intel WW calendar that starts with the first day of given fiscal year
    for given duration.
    
    Intel's fiscal year starts on the LAST Saturday of December.
    
    Args:
        start_fy: A positive integer to specify the starting fiscal year of the output
        num_of_yr: A positive integer to specify number of years
        
    Returns:
        A Pandas dataframe that contains date, fiscal year, WW
        
    Table Example:
        df:
            DOY  FY   WK  DATE        WW          DAYOFWEEK
            0    1   2020  1  2019-12-29  2020WW01    6
            1    2   2020  1  2019-12-30  2020WW01    0
            2    3   2020  1  2019-12-31  2020WW01    1
            
    Column names:
        DOY: day of year
        FY: fiscal year
        WK: week number
        WW: work week string
        DAYOFWEEK: Monday=0 --- Sunday=6
    """
    # Create an offset for the Intel fiscal calendar, which ends on the LAST Saturday of December
    yoffset = pd.tseries.offsets.FY5253(
        n=1, weekday=5, startingMonth=12, variation="last"
    )
    
    # Use the offset to create a date range with intervals at each fiscal year beginning
    # (one day before the beginning)
    yoffset_range = pd.date_range(
        str(start_fy - 1), periods=num_of_yr + 1, freq=yoffset
    )
    
    # Create a '1 day' offset:
    d1 = pd.tseries.offsets.DateOffset(n=1)
    
    # Specify the starting fiscal year (go back 8 days then +1 year)
    yr = (yoffset_range[0] - pd.tseries.offsets.DateOffset(n=8)).year + 1
    
    # Iterate over each item in the date range we created earlier:
    result = pd.DataFrame()
    for i in yoffset_range:
        # Recall each item is a Timestamp that represents the first day of the fiscal year,
        # so create a date_range from beginning to end of the fiscal year
        current_range = pd.date_range(i + d1, i + yoffset, freq="D")
        interim_df = pd.DataFrame(index=current_range)
        
        # Day of year
        interim_df["DOY"] = (current_range - current_range[0]).days + 1
        
        # Fiscal year
        interim_df["FY"] = yr
        result = pd.concat([result, interim_df])
        
        # WK calculation: (DOY - 1) // 7 + 1
        result["WK"] = ((result["DOY"] - 1) // 7) + 1
        
        yr += 1
    
    # Define columns: DATE, WW, DAYOFWEEK
    result["DATE"] = result.index
    result["WW"] = (
        result["FY"].astype(str) + "WW" + result["WK"].astype(str).str.zfill(2)
    )
    result["DAYOFWEEK"] = result["DATE"].dt.dayofweek
    result["Month"] = result["DATE"].dt.month.astype(str).str.zfill(2)
    result = result.reset_index(drop=True)
    
    return result


def get_current_ww(ref_date: Optional[datetime] = None) -> str:
    """
    Get the current Intel work week string for a given date.
    
    Args:
        ref_date: Reference date (defaults to today)
        
    Returns:
        Work week string in format 'YYYYWWNN' (e.g., '2025WW22')
    """
    if ref_date is None:
        ref_date = datetime.now()
    
    # Get current year and generate calendar for this year and next
    current_year = ref_date.year
    
    # Generate calendar covering previous, current, and next fiscal year
    calendar_df = get_intel_ww_calendar(current_year - 1, 3)
    
    # Find the WW for the given date
    matching_row = calendar_df[calendar_df["DATE"].dt.date == ref_date.date()]
    
    if matching_row.empty:
        raise ValueError(f"Date {ref_date} not found in Intel WW calendar")
    
    return matching_row.iloc[0]["WW"]


def parse_ww_string(ww_string: str) -> Tuple[int, int]:
    """
    Parse Intel work week string into year and week number.
    
    Args:
        ww_string: Work week string (e.g., '2025WW22' or '2025WW22nn')
        
    Returns:
        Tuple of (year, week_number)
        
    Example:
        >>> parse_ww_string('2025WW22')
        (2025, 22)
    """
    # Pattern: YYYYWWNN or YYYYWWNNnn
    match = re.match(r"(\d{4})WW(\d{2})", ww_string)
    if not match:
        raise ValueError(f"Invalid WW string format: {ww_string}")
    
    year = int(match.group(1))
    week = int(match.group(2))
    
    return year, week


def get_ww_date_range(ww_string: str) -> Tuple[datetime, datetime]:
    """
    Get the start and end date for a given Intel work week.
    
    Args:
        ww_string: Work week string (e.g., '2025WW22')
        
    Returns:
        Tuple of (start_date, end_date)
    """
    year, week = parse_ww_string(ww_string)
    
    # Generate calendar for the fiscal year
    calendar_df = get_intel_ww_calendar(year, 1)
    
    # Filter to the specific week
    week_data = calendar_df[calendar_df["WW"] == ww_string]
    
    if week_data.empty:
        raise ValueError(f"Work week {ww_string} not found in calendar")
    
    start_date = week_data["DATE"].min()
    end_date = week_data["DATE"].max()
    
    return start_date, end_date


def validate_pm_flex_schema(df: pd.DataFrame) -> bool:
    """
    Validate that a DataFrame has the expected PM_Flex schema.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        True if schema is valid
        
    Raises:
        SchemaValidationError: If schema is invalid
    """
    from .exceptions import SchemaValidationError
    
    # Expected columns (88 total from your synthetic data)
    expected_columns = [
        "ENTITY", "FACILITY", "UNIQUE_ENTITY_ID", "SUPPLIER", "FUNCTIONAL_AREA",
        "TOOLSET", "CEID", "VFMFGID", "CUSTOM_MODULE_GROUP", "Dominant_Tech_Node",
        "PM_NAME", "ATTRIBUTE_NAME", "YEARWW", "TXN_DATE", "PREV_ATTRIBUTE_VALUE",
        "ATTRIBUTE_VALUE", "NEXT_ATTRIBUTE_VALUE", "CUSTOM_DELTA", "Lower_IQR_Limit_Delta",
        "Median_Delta", "Delta_75th_Percentile", "COUNTER_UPPER_VALUE", "UPPER_LIMIT_FACILITY",
        "upper_limit_perc_target", "Met_Upper_Limit", "PM_Label", "GVB_PMCycle_Label",
        "PMCycle_Counter_by_UEI_AttrName", "CHECKLIST_NAME", "CKL_START_TIME",
        "CKL_END_TIME", "CKL_DURATION_IN_HOURS", "NUM_STEPS_IN_CL",
        "MIN_OF_CKL_START_END_DISTANCE_TO_TXN_DATE", "CL_NAME_SIMILARITY_SCORE",
        "MOST_FREQUENT_CHECKLIST_FACILITY_CMG_TECHNODE", "DURATION_IN_HOURS_75TH_PERCENTILE",
        "pm_cycle_utilization", "reliable_upper_limit_insight", "TECHNODE_CEID_VFMFGID",
        "PARENT_ENTITY", "UNIQUE_PARENT_FAB", "SUB_ENTITY_ASSOCIATED_TO_ATTR",
        "ATTRIBUTE_NAME_ASSOCIATED_ENTITY", "num_of_resets_on_parent_txndate",
        "multiple_or_single_pm", "Sympathy_PM", "most_common_pm_grouping",
        "MOST_COMMON_PM_TYPE", "PM_FREQUENCY", "WINDOW_ID", "DOWN_WINDOW_START_TXN_DATE",
        "DOWN_WINDOW_END_TXN_DATE", "DOWN_WINDOW_DURATION_HR", "DOWN_WINDOW_DETAILS",
        "DOWN_WINDOW_COMMENTS", "ALL_STATES_IN_WINDOW", "WINDOW_TYPE", "DOWNTIME_TYPE",
        "DOWNTIME_CLASS", "DOWNTIME_SUBCLASS", "OLD_ENTITY_STATE", "NEW_ENTITY_STATE",
        "Reclean_Label", "Down_Window_Reclean_Rate",
        "DOWN_WINDOW_DURATION_OUTLIER_THRESHOLD_FOR_TOOLSET",
        "DOWN_WINDOW_DURATION_OUTLIER_LABEL_FOR_PMCYCLE", "WORKORDERID", "WO_TOOLNAME",
        "WO_DESCRIPTION", "PM_Reason_Deepdive", "DOWNTIME_SUBCLASS_DETAILS",
        "UPPER_LIMIT_THRESHOLD", "UPPER_VALUE_THRESHOLD", "VALUE_LOSS_AT_PMRESET",
        "PM_REDUCTION_ROI", "NORMALIZING_FACTOR", "PM_REDUCTION_ROI_NORMALIZED",
        "G2G_PER_PM", "EQUIPMENT_DOWNTIME_ROI(Hrs)", "PART_COST_PER_PM",
        "PART_COST_SAVING_ROI($)", "PM_DURATION", "MTS_NEEDED", "LABORHOUR_PER_PM",
        "LABORHOUR_PER_PM.1", "LABOR_HOUR_ROI(Hrs)", "HEADCOUNT_ROI(#)"
    ]
    
    # Check for missing columns
    missing_columns = set(expected_columns) - set(df.columns)
    if missing_columns:
        raise SchemaValidationError(
            f"Missing expected columns: {', '.join(sorted(missing_columns))}"
        )
    
    # Check for unexpected columns (warning, not error)
    extra_columns = set(df.columns) - set(expected_columns)
    if extra_columns:
        import logging
        logger = logging.getLogger("pm_flex_pipeline")
        logger.warning(f"Unexpected columns found: {', '.join(sorted(extra_columns))}")
    
    return True


def load_altair_classification() -> pd.DataFrame:
    """
    Load Altair vs Non-Altair tool classification from CSV.
    
    Returns:
        DataFrame with ENTITY and ProcessAllowed columns
    """
    import os
    from pathlib import Path
    
    # Find the data directory
    current_dir = Path(__file__).parent.parent
    altair_file = current_dir / "data" / "altair_tools.csv"
    
    if not altair_file.exists():
        raise FileNotFoundError(f"Altair classification file not found: {altair_file}")
    
    df = pd.read_csv(altair_file)
    return df


def add_altair_flag(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add Altair flag to DataFrame based on ENTITY column.
    
    Args:
        df: DataFrame with ENTITY column
        
    Returns:
        DataFrame with added AltairFlag column
    """
    altair_df = load_altair_classification()
    
    # Merge with main DataFrame
    df = df.merge(
        altair_df,
        left_on="ENTITY",
        right_on="ENTITY",
        how="left"
    )
    
    # Rename and set default for unknown entities
    df = df.rename(columns={"ProcessAllowed": "AltairFlag"})
    df["AltairFlag"] = df["AltairFlag"].fillna("UNKNOWN")
    
    return df
