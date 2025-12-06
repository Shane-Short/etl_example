# Flatten multi-level columns
summary.columns = ['_'.join(str(col)).strip('_') if isinstance(col, tuple) else col 
                   for col in summary.columns.values]

# Create simple column mapping
# The aggregation creates columns like: 'scheduled_flag_<lambda_0>', 'DOWN_WINDOW_DURATION_HR_sum', etc.
# We need to map these to our schema column names

# Find actual column names created by pandas
actual_columns = list(summary.columns)
self.logger.debug(f"Actual columns after aggregation: {actual_columns}")

# Manual column assignment based on position/pattern
new_columns = {}
for col in actual_columns:
    if 'pm_flex_raw_id' in col and 'count' in col:
        new_columns[col] = 'total_pm_events'
    elif 'scheduled_flag' in col and 'scheduled_count' in col:
        new_columns[col] = 'scheduled_pm_count'
    elif 'scheduled_flag' in col and 'unscheduled_count' in col:
        new_columns[col] = 'unscheduled_pm_count'
    elif 'early_count' in col:
        new_columns[col] = 'early_pm_count'
    elif 'on_time_count' in col:
        new_columns[col] = 'on_time_pm_count'
    elif 'late_count' in col:
        new_columns[col] = 'late_pm_count'
    elif 'overdue_count' in col:
        new_columns[col] = 'overdue_pm_count'
    elif 'DOWN_WINDOW_DURATION_HR' in col and 'total_downtime' in col:
        new_columns[col] = 'total_downtime_hours'
    elif 'DOWN_WINDOW_DURATION_HR' in col and 'avg_downtime' in col:
        new_columns[col] = 'avg_downtime_hours'
    elif 'DOWN_WINDOW_DURATION_HR' in col and 'median_downtime' in col:
        new_columns[col] = 'median_downtime_hours'
    elif 'DOWN_WINDOW_DURATION_HR' in col and 'min_downtime' in col:
        new_columns[col] = 'min_downtime_hours'
    elif 'DOWN_WINDOW_DURATION_HR' in col and 'max_downtime' in col:
        new_columns[col] = 'max_downtime_hours'
    elif 'DOWN_WINDOW_DURATION_HR' in col and 'std_downtime' in col:
        new_columns[col] = 'downtime_std_dev'
    elif 'DOWN_WINDOW_DURATION_HR' in col and 'count_downtime' in col:
        new_columns[col] = 'downtime_count'
    elif 'CUSTOM_DELTA' in col and 'avg_pm_life' in col:
        new_columns[col] = 'avg_pm_life'
    elif 'CUSTOM_DELTA' in col and 'median_pm_life' in col:
        new_columns[col] = 'median_pm_life'
    elif 'CUSTOM_DELTA' in col and 'pm_life_std_dev' in col:
        new_columns[col] = 'pm_life_std_dev'
    elif 'reclean_event_flag' in col:
        new_columns[col] = 'reclean_events'
    elif 'ww_year' in col and 'first' in col:
        new_columns[col] = 'ww_year'
    elif 'ww_number' in col and 'first' in col:
        new_columns[col] = 'ww_number'

summary = summary.rename(columns=new_columns)
self.logger.debug(f"Columns after rename: {list(summary.columns)}")
