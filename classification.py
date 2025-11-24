"""
Classification logic for PM events and chronic tools.

Contains business rules for:
- PM timing classification (Early, On-Time, Late, Overdue)
- Scheduled vs Unscheduled classification
- Chronic tool identification
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple
import logging
import yaml
from pathlib import Path


class PMTimingClassifier:
    """
    Classifies PM timing as Early, On-Time, Late, or Overdue.
    """
    
    def __init__(self, config: Dict = None):
        """
        Initialize PM timing classifier.
        
        Args:
            config: Configuration dictionary with thresholds
        """
        self.logger = logging.getLogger("pm_flex_pipeline.classification")
        
        if config is None:
            config = self._load_default_config()
        
        # Load thresholds from config
        pm_timing = config.get('pm_timing', {})
        self.early_threshold = pm_timing.get('early_threshold', -15)
        self.on_time_min = pm_timing.get('on_time_min', -15)
        self.on_time_max = pm_timing.get('on_time_max', 15)
        self.late_threshold = pm_timing.get('late_threshold', 15)
        self.overdue_threshold = pm_timing.get('overdue_threshold', 30)
        
        self.logger.info(
            f"PM Timing Classifier initialized with thresholds: "
            f"early<{self.early_threshold}%, on-time {self.on_time_min}-{self.on_time_max}%, "
            f"late>{self.late_threshold}%, overdue>{self.overdue_threshold}%"
        )
    
    def _load_default_config(self) -> Dict:
        """Load default configuration from config.yaml."""
        config_path = Path(__file__).parent.parent.parent / "config" / "config.yaml"
        
        if config_path.exists():
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        else:
            return {}
    
    def classify_timing(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Classify PM timing for all records.
        
        Adds columns:
        - pm_life_vs_target: Actual wafer count - target
        - pm_life_vs_target_pct: Percentage deviation
        - pm_timing_classification: Early/On-Time/Late/Overdue
        
        Args:
            df: DataFrame with CUSTOM_DELTA and Median_Delta columns
            
        Returns:
            DataFrame with timing classification columns added
        """
        self.logger.info("Classifying PM timing...")
        
        # Calculate PM life vs target
        # Using CUSTOM_DELTA as actual and Median_Delta as target
        df['pm_life_vs_target'] = df['CUSTOM_DELTA'] - df['Median_Delta']
        
        # Calculate percentage deviation
        df['pm_life_vs_target_pct'] = np.where(
            df['Median_Delta'] > 0,
            (df['pm_life_vs_target'] / df['Median_Delta']) * 100,
            np.nan
        )
        
        # Classify timing
        def classify_row(pct):
            if pd.isna(pct):
                return 'Unknown'
            elif pct < self.early_threshold:
                return 'Early'
            elif self.on_time_min <= pct <= self.on_time_max:
                return 'On-Time'
            elif self.late_threshold < pct <= self.overdue_threshold:
                return 'Late'
            elif pct > self.overdue_threshold:
                return 'Overdue'
            else:
                return 'On-Time'  # Default for edge cases
        
        df['pm_timing_classification'] = df['pm_life_vs_target_pct'].apply(classify_row)
        
        # Log distribution
        timing_dist = df['pm_timing_classification'].value_counts()
        self.logger.info(f"Timing distribution: {timing_dist.to_dict()}")
        
        return df
    
    def classify_scheduled(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Classify PMs as Scheduled or Unscheduled.
        
        Adds columns:
        - scheduled_flag: 1=Scheduled, 0=Unscheduled
        - scheduled_category: 'Scheduled', 'Unscheduled', 'Unknown'
        
        Args:
            df: DataFrame with DOWNTIME_TYPE and DOWNTIME_CLASS columns
            
        Returns:
            DataFrame with scheduled classification columns
        """
        self.logger.info("Classifying scheduled vs unscheduled...")
        
        # Primary classification from DOWNTIME_TYPE
        df['scheduled_flag'] = df['DOWNTIME_TYPE'].apply(
            lambda x: 1 if str(x).lower() == 'scheduled' else 0
        )
        
        df['scheduled_category'] = df['DOWNTIME_TYPE'].apply(
            lambda x: 'Scheduled' if str(x).lower() == 'scheduled' 
                     else 'Unscheduled' if str(x).lower() == 'unscheduled'
                     else 'Unknown'
        )
        
        # Log distribution
        sched_dist = df['scheduled_category'].value_counts()
        self.logger.info(f"Scheduled distribution: {sched_dist.to_dict()}")
        
        return df


class ChronicToolAnalyzer:
    """
    Identifies and scores chronic tools based on multiple factors.
    """
    
    def __init__(self, config: Dict = None):
        """
        Initialize chronic tool analyzer.
        
        Args:
            config: Configuration dictionary with thresholds and weights
        """
        self.logger = logging.getLogger("pm_flex_pipeline.chronic_tools")
        
        if config is None:
            config = self._load_default_config()
        
        # Load thresholds
        chronic_config = config.get('chronic_tools', {})
        thresholds = chronic_config.get('chronic_tool_threshold', {})
        self.unscheduled_pm_threshold = thresholds.get('unscheduled_pm_rate', 0.30)
        self.pm_variance_threshold = thresholds.get('pm_life_variance', 0.40)
        self.min_pm_events = thresholds.get('min_pm_events', 5)
        
        # Load score weights
        weights = chronic_config.get('score_weights', {})
        self.weight_unscheduled = weights.get('unscheduled_pm_rate', 0.35)
        self.weight_variance = weights.get('pm_life_variance', 0.25)
        self.weight_downtime = weights.get('downtime_hours', 0.20)
        self.weight_reclean = weights.get('reclean_rate', 0.10)
        self.weight_sympathy = weights.get('sympathy_pm_rate', 0.10)
        
        # Load severity thresholds
        severity = chronic_config.get('severity_thresholds', {})
        self.severity_low = severity.get('low', 25)
        self.severity_medium = severity.get('medium', 50)
        self.severity_high = severity.get('high', 75)
        self.severity_critical = severity.get('critical', 90)
        
        self.logger.info("Chronic Tool Analyzer initialized")
    
    def _load_default_config(self) -> Dict:
        """Load default configuration from config.yaml."""
        config_path = Path(__file__).parent.parent.parent / "config" / "config.yaml"
        
        if config_path.exists():
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        else:
            return {}
    
    def calculate_chronic_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate chronic score for each entity.
        
        Score is a weighted composite of:
        - Unscheduled PM rate
        - PM life variance
        - Downtime hours per PM
        - Reclean rate
        - Sympathy PM rate
        
        Args:
            df: DataFrame grouped by ENTITY with aggregated metrics
            
        Returns:
            DataFrame with chronic_score column (0-100)
        """
        self.logger.info("Calculating chronic scores...")
        
        # Normalize each factor to 0-100 scale
        
        # 1. Unscheduled PM rate (higher = worse)
        unscheduled_score = np.clip(
            (df['unscheduled_pm_rate'] / self.unscheduled_pm_threshold) * 100,
            0, 100
        )
        
        # 2. PM life variance (higher = worse)
        variance_score = np.clip(
            (df['pm_life_variance'] / self.pm_variance_threshold) * 100,
            0, 100
        )
        
        # 3. Downtime hours per PM (normalize to reasonable range)
        # Assume >10 hours/PM is very high
        downtime_score = np.clip(
            (df['avg_downtime_hours_per_pm'] / 10) * 100,
            0, 100
        )
        
        # 4. Reclean rate (higher = worse)
        reclean_score = df['reclean_rate'] * 100
        
        # 5. Sympathy PM rate (higher = worse)
        sympathy_score = df['sympathy_pm_rate'] * 100
        
        # Calculate weighted composite score
        df['chronic_score'] = (
            (unscheduled_score * self.weight_unscheduled) +
            (variance_score * self.weight_variance) +
            (downtime_score * self.weight_downtime) +
            (reclean_score * self.weight_reclean) +
            (sympathy_score * self.weight_sympathy)
        )
        
        # Round to 2 decimal places
        df['chronic_score'] = df['chronic_score'].round(2)
        
        self.logger.info(
            f"Chronic scores calculated. Range: {df['chronic_score'].min():.1f} - "
            f"{df['chronic_score'].max():.1f}"
        )
        
        return df
    
    def classify_chronic_tools(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Classify tools as chronic or not, and assign severity.
        
        Adds columns:
        - chronic_flag: 1=Chronic, 0=Normal
        - chronic_severity: Low/Medium/High/Critical
        
        Args:
            df: DataFrame with chronic_score column
            
        Returns:
            DataFrame with chronic classification columns
        """
        self.logger.info("Classifying chronic tools...")
        
        # Filter by minimum PM events
        df['chronic_flag'] = np.where(
            (df['total_pm_events'] >= self.min_pm_events) &
            (
                (df['unscheduled_pm_rate'] > self.unscheduled_pm_threshold) |
                (df['pm_life_variance'] > self.pm_variance_threshold)
            ),
            1, 0
        )
        
        # Assign severity based on chronic_score
        def assign_severity(row):
            if row['chronic_flag'] == 0:
                return None
            elif row['chronic_score'] >= self.severity_critical:
                return 'Critical'
            elif row['chronic_score'] >= self.severity_high:
                return 'High'
            elif row['chronic_score'] >= self.severity_medium:
                return 'Medium'
            else:
                return 'Low'
        
        df['chronic_severity'] = df.apply(assign_severity, axis=1)
        
        # Log results
        chronic_count = df['chronic_flag'].sum()
        total_count = len(df)
        chronic_pct = (chronic_count / total_count * 100) if total_count > 0 else 0
        
        self.logger.info(
            f"Chronic tools: {chronic_count} of {total_count} ({chronic_pct:.1f}%)"
        )
        
        if chronic_count > 0:
            severity_dist = df[df['chronic_flag'] == 1]['chronic_severity'].value_counts()
            self.logger.info(f"Severity distribution: {severity_dist.to_dict()}")
        
        return df
    
    def analyze_by_entity(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate PM metrics by entity and calculate chronic scores.
        
        Args:
            df: Raw PM_Flex data
            
        Returns:
            DataFrame with one row per entity with chronic analysis
        """
        self.logger.info("Analyzing chronic tools by entity...")
        
        # Group by entity
        entity_metrics = df.groupby(['ENTITY', 'FACILITY', 'CEID']).agg({
            'pm_flex_raw_id': 'count',  # Total PM events
            'scheduled_flag': lambda x: (x == 0).sum(),  # Unscheduled count
            'DOWN_WINDOW_DURATION_HR': 'sum',  # Total downtime
            'CUSTOM_DELTA': ['std', 'mean'],  # PM life variance
            'Reclean_Label': 'mean',  # Reclean rate
            'Sympathy_PM': 'mean'  # Sympathy PM rate
        }).reset_index()
        
        # Flatten multi-level columns
        entity_metrics.columns = [
            'ENTITY', 'FACILITY', 'CEID',
            'total_pm_events',
            'unscheduled_pm_count',
            'total_downtime_hours',
            'pm_life_std_dev',
            'avg_pm_life',
            'reclean_rate',
            'sympathy_pm_rate'
        ]
        
        # Calculate derived metrics
        entity_metrics['unscheduled_pm_rate'] = (
            entity_metrics['unscheduled_pm_count'] / entity_metrics['total_pm_events']
        )
        
        entity_metrics['avg_downtime_hours_per_pm'] = (
            entity_metrics['total_downtime_hours'] / entity_metrics['total_pm_events']
        )
        
        # Calculate coefficient of variation for PM life
        entity_metrics['pm_life_variance'] = np.where(
            entity_metrics['avg_pm_life'] > 0,
            entity_metrics['pm_life_std_dev'] / entity_metrics['avg_pm_life'],
            0
        )
        
        # Calculate chronic score
        entity_metrics = self.calculate_chronic_score(entity_metrics)
        
        # Classify as chronic or not
        entity_metrics = self.classify_chronic_tools(entity_metrics)
        
        return entity_metrics
