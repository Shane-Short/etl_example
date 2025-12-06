# Flatten multi-level column names
site_kpis.columns = ['_'.join(str(col)).strip('_') if isinstance(col, tuple) else col 
                     for col in site_kpis.columns.values]

# Log actual columns for debugging
self.logger.debug(f"Columns after aggregation: {list(site_kpis.columns)}")

# Smart column renaming - find columns by pattern
new_columns = {}
for col in site_kpis.columns:
    if col in ['FACILITY', 'YEARWW']:
        continue  # Keep grouping columns as-is
    elif 'ENTITY' in col and 'count' in col:
        new_columns[col] = 'total_pm_events'
    elif 'CUSTOM_DELTA' in col and 'mean' in col:
        new_columns[col] = 'avg_pm_life'
    elif 'CUSTOM_DELTA' in col and 'median' in col:
        new_columns[col] = 'median_pm_life'
    elif 'CUSTOM_DELTA' in col and 'std' in col:
        new_columns[col] = 'pm_life_std_dev'
    elif 'DOWN_WINDOW_DURATION_HR' in col and 'sum' in col:
        new_columns[col] = 'total_downtime_hours'
    elif 'DOWN_WINDOW_DURATION_HR' in col and 'mean' in col:
        new_columns[col] = 'avg_downtime_hours'
    elif 'DOWN_WINDOW_DURATION_HR' in col and 'median' in col:
        new_columns[col] = 'median_downtime_hours'
    elif 'scheduled_flag' in col and '<lambda' in col:
        new_columns[col] = 'scheduled_pm_count'
    elif 'early_count' in col:
        new_columns[col] = 'early_pm_count'
    elif 'on_time_count' in col:
        new_columns[col] = 'on_time_pm_count'
    elif 'late_count' in col:
        new_columns[col] = 'late_pm_count'
    elif 'overdue_count' in col:
        new_columns[col] = 'overdue_pm_count'
    elif 'reclean_event_flag' in col and '<lambda' in col:
        new_columns[col] = 'reclean_count'
    elif 'sympathy_pm_flag' in col and '<lambda' in col:
        new_columns[col] = 'sympathy_pm_count'
    elif 'ww_year' in col and 'first' in col:
        new_columns[col] = 'ww_year'
    elif 'ww_number' in col and 'first' in col:
        new_columns[col] = 'ww_number'

site_kpis = site_kpis.rename(columns=new_columns)
self.logger.debug(f"Columns after rename: {list(site_kpis.columns)}")
