"""
File discovery module for PM_Flex CSV files.

Scans network share and locates the appropriate PM_Flex.csv file
based on Intel work week calendar.
"""

import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
import pandas as pd
import logging

from utils.helpers import get_intel_ww_calendar, get_current_ww, parse_ww_string
from utils.exceptions import FileNotFoundError as PMFlexFileNotFoundError
from utils.env import load_environment


class PMFlexFileDiscovery:
    """
    Discovers and validates PM_Flex CSV files on network share.
    """
    
    def __init__(self, network_share_path: Optional[str] = None):
        """
        Initialize file discovery.
        
        Args:
            network_share_path: Network share root path 
                              (e.g., \\\\server\\share\\Data)
        """
        self.logger = logging.getLogger("pm_flex_pipeline.file_discovery")
        
        if network_share_path is None:
            config = load_environment()
            network_share_path = config["network_share_path"]
        
        self.network_share_path = Path(network_share_path)
        self.logger.info(f"Initialized file discovery for: {self.network_share_path}")
    
    def get_expected_file_path(
        self, 
        work_week: Optional[str] = None,
        variation: int = 0
    ) -> Path:
        """
        Get the expected file path for a given work week.
        
        File structure: \\\\share\\Data\\YYYYWWnn\\PM_Flex.csv
        
        Args:
            work_week: Work week string (e.g., '2025WW22'). 
                      Defaults to current WW.
            variation: Number of weeks to offset (0 = current, -1 = last week)
            
        Returns:
            Path object for the expected file
            
        Example:
            >>> discovery.get_expected_file_path('2025WW22')
            Path('\\\\server\\share\\Data\\2025WW22\\PM_Flex.csv')
        """
        if work_week is None:
            work_week = get_current_ww()
        
        # Apply variation if needed
        if variation != 0:
            year, week = parse_ww_string(work_week)
            week += variation
            
            # Handle week overflow/underflow
            if week < 1:
                year -= 1
                week = 52 + week
            elif week > 52:
                year += 1
                week = week - 52
            
            work_week = f"{year}WW{week:02d}"
        
        # Construct file path: \\share\Data\YYYYWWnn\PM_Flex.csv
        file_path = self.network_share_path / work_week / "PM_Flex.csv"
        
        self.logger.debug(f"Expected file path: {file_path}")
        return file_path
    
    def find_latest_file(
        self, 
        max_weeks_back: int = 4
    ) -> Tuple[Path, str]:
        """
        Find the most recent PM_Flex file within the last N weeks.
        
        Args:
            max_weeks_back: Maximum number of weeks to look back
            
        Returns:
            Tuple of (file_path, work_week_string)
            
        Raises:
            PMFlexFileNotFoundError: If no file found within the time window
        """
        self.logger.info(
            f"Searching for latest PM_Flex file "
            f"(up to {max_weeks_back} weeks back)"
        )
        
        for week_offset in range(max_weeks_back + 1):
            # Try current week, then previous weeks
            expected_path = self.get_expected_file_path(variation=-week_offset)
            work_week = expected_path.parent.name
            
            if expected_path.exists():
                self.logger.info(f"Found file: {expected_path} (WW: {work_week})")
                return expected_path, work_week
            else:
                self.logger.debug(f"File not found: {expected_path}")
        
        # No file found
        raise PMFlexFileNotFoundError(
            f"No PM_Flex file found within last {max_weeks_back} weeks. "
            f"Search root: {self.network_share_path}"
        )
    
    def find_file_for_week(self, work_week: str) -> Path:
        """
        Find PM_Flex file for a specific work week.
        
        Args:
            work_week: Work week string (e.g., '2025WW22')
            
        Returns:
            Path to the file
            
        Raises:
            PMFlexFileNotFoundError: If file doesn't exist
        """
        file_path = self.get_expected_file_path(work_week)
        
        if not file_path.exists():
            raise PMFlexFileNotFoundError(
                f"PM_Flex file not found for {work_week}: {file_path}"
            )
        
        self.logger.info(f"Found file for {work_week}: {file_path}")
        return file_path
    
    def list_available_weeks(self) -> List[str]:
        """
        List all work weeks with available PM_Flex files.
        
        Returns:
            List of work week strings sorted by date (newest first)
        """
        self.logger.info(f"Scanning {self.network_share_path} for available files")
        
        available_weeks = []
        
        # Scan network share for directories matching YYYYWWnn pattern
        if not self.network_share_path.exists():
            self.logger.warning(f"Network share not accessible: {self.network_share_path}")
            return []
        
        for item in self.network_share_path.iterdir():
            if item.is_dir():
                # Check if directory name matches work week pattern
                dir_name = item.name
                if len(dir_name) == 8 and 'WW' in dir_name:
                    # Check if PM_Flex.csv exists in this directory
                    pm_flex_file = item / "PM_Flex.csv"
                    if pm_flex_file.exists():
                        available_weeks.append(dir_name)
        
        # Sort by work week (newest first)
        available_weeks.sort(reverse=True)
        
        self.logger.info(f"Found {len(available_weeks)} available work weeks")
        return available_weeks
    
    def validate_file(self, file_path: Path) -> bool:
        """
        Validate that a file exists and is readable.
        
        Args:
            file_path: Path to file to validate
            
        Returns:
            True if valid, raises exception otherwise
            
        Raises:
            PMFlexFileNotFoundError: If file doesn't exist or isn't readable
        """
        if not file_path.exists():
            raise PMFlexFileNotFoundError(f"File does not exist: {file_path}")
        
        if not file_path.is_file():
            raise PMFlexFileNotFoundError(f"Path is not a file: {file_path}")
        
        # Check if file is readable and not empty
        try:
            file_size = file_path.stat().st_size
            if file_size == 0:
                raise PMFlexFileNotFoundError(f"File is empty: {file_path}")
            
            self.logger.info(
                f"File validation passed: {file_path} "
                f"({file_size / (1024*1024):.2f} MB)"
            )
            return True
            
        except Exception as e:
            raise PMFlexFileNotFoundError(
                f"Cannot access file {file_path}: {str(e)}"
            )
    
    def get_file_info(self, file_path: Path) -> dict:
        """
        Get detailed information about a PM_Flex file.
        
        Args:
            file_path: Path to file
            
        Returns:
            Dictionary with file metadata
        """
        stat = file_path.stat()
        
        info = {
            "file_path": str(file_path),
            "file_name": file_path.name,
            "work_week": file_path.parent.name,
            "size_bytes": stat.st_size,
            "size_mb": stat.st_size / (1024 * 1024),
            "modified_time": datetime.fromtimestamp(stat.st_mtime),
            "created_time": datetime.fromtimestamp(stat.st_ctime),
        }
        
        # Try to get row count (without loading full file)
        try:
            # Count lines efficiently
            with open(file_path, 'r', encoding='utf-8') as f:
                row_count = sum(1 for _ in f) - 1  # Subtract header
            info["estimated_rows"] = row_count
        except Exception as e:
            self.logger.warning(f"Could not count rows: {e}")
            info["estimated_rows"] = None
        
        return info
