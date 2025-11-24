"""
Raw data loader for PM_Flex CSV files.

Loads raw PM_Flex data from CSV into SQL Server copper layer.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any
import logging
import time

from connectors import SQLServerConnector
from utils.helpers import validate_pm_flex_schema, add_altair_flag
from utils.exceptions import SchemaValidationError, DataQualityError
from utils.logger import log_execution_time


class PMFlexRawLoader:
    """
    Loads raw PM_Flex data into SQL Server copper layer.
    """
    
    def __init__(self, connector: Optional[SQLServerConnector] = None):
        """
        Initialize raw loader.
        
        Args:
            connector: SQL Server connector (creates new one if not provided)
        """
        self.logger = logging.getLogger("pm_flex_pipeline.raw_loader")
        
        if connector is None:
            connector = SQLServerConnector()
        
        self.connector = connector
        self.table_name = "pm_flex_raw"
        self.schema_name = "dbo"
    
    @log_execution_time(logging.getLogger("pm_flex_pipeline.raw_loader"))
    def load_file(
        self, 
        file_path: Path,
        work_week: str,
        validate_schema: bool = True,
        add_altair: bool = True
    ) -> Dict[str, Any]:
        """
        Load a PM_Flex CSV file into SQL Server.
        
        Args:
            file_path: Path to PM_Flex.csv file
            work_week: Work week string (e.g., '2025WW22')
            validate_schema: Whether to validate CSV schema
            add_altair: Whether to add Altair classification
            
        Returns:
            Dictionary with load statistics
            
        Raises:
            SchemaValidationError: If schema validation fails
            DataQualityError: If data quality checks fail
        """
        start_time = time.time()
        
        self.logger.info(f"Starting load for {file_path}")
        
        # Step 1: Read CSV file
        df = self._read_csv(file_path)
        self.logger.info(f"Read {len(df)} rows from CSV")
        
        # Step 2: Validate schema
        if validate_schema:
            self._validate_schema(df)
        
        # Step 3: Add metadata columns
        df = self._add_metadata(df, file_path, work_week)
        
        # Step 4: Add Altair classification (optional)
        if add_altair:
            df = self._add_altair_classification(df)
        
        # Step 5: Clean column names for SQL
        df = self._clean_column_names(df)
        
        # Step 6: Data quality checks
        self._data_quality_checks(df)
        
        # Step 7: Load to SQL Server
        rows_loaded = self._load_to_sql(df)
        
        # Step 8: Log the load
        execution_time = time.time() - start_time
        self._log_load(
            source_file=str(file_path),
            source_ww=work_week,
            rows_loaded=rows_loaded,
            execution_time=execution_time,
            status="SUCCESS"
        )
        
        # Return statistics
        stats = {
            "file_path": str(file_path),
            "work_week": work_week,
            "rows_loaded": rows_loaded,
            "columns": len(df.columns),
            "execution_time_seconds": execution_time,
            "load_timestamp": datetime.now()
        }
        
        self.logger.info(
            f"Load completed successfully: {rows_loaded} rows in "
            f"{execution_time:.2f} seconds"
        )
        
        return stats
    
    def _read_csv(self, file_path: Path) -> pd.DataFrame:
        """
        Read PM_Flex CSV file with appropriate settings.
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            DataFrame with CSV contents
        """
        try:
            # Read CSV with specific settings
            df = pd.read_csv(
                file_path,
                encoding='utf-8',
                low_memory=False,
                # Parse datetime columns
                parse_dates=[
                    'TXN_DATE', 
                    'CKL_START_TIME', 
                    'CKL_END_TIME',
                    'DOWN_WINDOW_START_TXN_DATE',
                    'DOWN_WINDOW_END_TXN_DATE'
                ]
            )
            
            self.logger.info(f"Successfully read CSV: {len(df)} rows, {len(df.columns)} columns")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read CSV: {str(e)}")
            raise DataQualityError(f"Cannot read CSV file: {str(e)}")
    
    def _validate_schema(self, df: pd.DataFrame) -> None:
        """
        Validate DataFrame schema matches expected PM_Flex format.
        
        Args:
            df: DataFrame to validate
            
        Raises:
            SchemaValidationError: If schema is invalid
        """
        try:
            validate_pm_flex_schema(df)
            self.logger.info("Schema validation passed")
        except SchemaValidationError as e:
            self.logger.error(f"Schema validation failed: {str(e)}")
            raise
    
    def _add_metadata(
        self, 
        df: pd.DataFrame, 
        file_path: Path, 
        work_week: str
    ) -> pd.DataFrame:
        """
        Add metadata columns to DataFrame.
        
        Args:
            df: Input DataFrame
            file_path: Source file path
            work_week: Work week string
            
        Returns:
            DataFrame with metadata columns added
        """
        # Add metadata columns
        df['load_timestamp'] = datetime.now()
        df['source_file'] = str(file_path)
        df['source_ww'] = work_week
        
        self.logger.debug("Added metadata columns")
        return df
    
    def _add_altair_classification(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add Altair vs Non-Altair classification.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with AltairFlag column
        """
        try:
            df = add_altair_flag(df)
            
            # Log distribution
            altair_counts = df['AltairFlag'].value_counts()
            self.logger.info(f"Altair classification: {altair_counts.to_dict()}")
            
            return df
            
        except Exception as e:
            self.logger.warning(f"Could not add Altair classification: {str(e)}")
            # Add as UNKNOWN if classification fails
            df['AltairFlag'] = 'UNKNOWN'
            return df
    
    def _clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean column names for SQL Server compatibility.
        
        Removes special characters that cause issues in SQL Server.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with cleaned column names
        """
        # Column name mappings (special characters removed)
        column_mappings = {
            'EQUIPMENT_DOWNTIME_ROI(Hrs)': 'EQUIPMENT_DOWNTIME_ROI_Hrs',
            'PART_COST_SAVING_ROI($)': 'PART_COST_SAVING_ROI',
            'LABORHOUR_PER_PM.1': 'LABORHOUR_PER_PM_2',
            'LABOR_HOUR_ROI(Hrs)': 'LABOR_HOUR_ROI_Hrs',
            'HEADCOUNT_ROI(#)': 'HEADCOUNT_ROI'
        }
        
        df = df.rename(columns=column_mappings)
        
        self.logger.debug("Cleaned column names for SQL compatibility")
        return df
    
    def _data_quality_checks(self, df: pd.DataFrame) -> None:
        """
        Perform data quality checks.
        
        Args:
            df: DataFrame to check
            
        Raises:
            DataQualityError: If critical quality issues found
        """
        # Check for completely empty DataFrame
        if len(df) == 0:
            raise DataQualityError("DataFrame is empty")
        
        # Check for critical columns having too many nulls
        critical_columns = ['ENTITY', 'FACILITY', 'CEID', 'YEARWW']
        
        for col in critical_columns:
            if col in df.columns:
                null_pct = (df[col].isnull().sum() / len(df)) * 100
                if null_pct > 50:
                    self.logger.warning(
                        f"Column {col} has {null_pct:.1f}% null values"
                    )
        
        # Log overall data quality
        total_nulls = df.isnull().sum().sum()
        total_cells = len(df) * len(df.columns)
        overall_null_pct = (total_nulls / total_cells) * 100
        
        self.logger.info(
            f"Data quality: {overall_null_pct:.2f}% null values overall"
        )
    
    def _load_to_sql(self, df: pd.DataFrame) -> int:
        """
        Load DataFrame to SQL Server.
        
        Args:
            df: DataFrame to load
            
        Returns:
            Number of rows loaded
        """
        try:
            rows_loaded = self.connector.load_dataframe(
                df=df,
                table_name=self.table_name,
                schema=self.schema_name,
                if_exists='append',  # Always append to copper
                chunksize=1000
            )
            
            return rows_loaded
            
        except Exception as e:
            self.logger.error(f"Failed to load data to SQL: {str(e)}")
            raise
    
    def _log_load(
        self,
        source_file: str,
        source_ww: str,
        rows_loaded: int,
        execution_time: float,
        status: str = "SUCCESS",
        error_message: Optional[str] = None
    ) -> None:
        """
        Log the load execution to pm_flex_load_log table.
        
        Args:
            source_file: Source file path
            source_ww: Work week string
            rows_loaded: Number of rows loaded
            execution_time: Execution time in seconds
            status: Load status (SUCCESS, FAILED, PARTIAL)
            error_message: Error message if failed
        """
        try:
            log_data = pd.DataFrame([{
                'load_timestamp': datetime.now(),
                'source_file': source_file,
                'source_ww': source_ww,
                'rows_loaded': rows_loaded,
                'load_status': status,
                'error_message': error_message,
                'execution_time_seconds': execution_time,
                'layer': 'COPPER'
            }])
            
            self.connector.load_dataframe(
                df=log_data,
                table_name='pm_flex_load_log',
                schema=self.schema_name,
                if_exists='append'
            )
            
            self.logger.debug("Load logged to pm_flex_load_log")
            
        except Exception as e:
            self.logger.warning(f"Could not log load: {str(e)}")
    
    def truncate_table(self) -> None:
        """
        Truncate the pm_flex_raw table (removes all data).
        
        Use with caution!
        """
        self.logger.warning(f"Truncating table {self.schema_name}.{self.table_name}")
        self.connector.truncate_table(self.table_name, self.schema_name)
        self.logger.info("Table truncated successfully")
    
    def get_row_count(self) -> int:
        """
        Get current row count in pm_flex_raw table.
        
        Returns:
            Number of rows
        """
        count = self.connector.get_row_count(self.table_name, self.schema_name)
        self.logger.info(f"Current row count: {count:,}")
        return count
