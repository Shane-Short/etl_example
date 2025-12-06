# Create summary with direct calculations (avoids duplicate column issues)
grouped = df.groupby(['ENTITY', 'FACILITY', 'CEID', 'YEARWW', 'DOWNTIME_TYPE', 'DOWNTIME_CLASS', 'AltairFlag'])

summary = pd.DataFrame()
summary['ENTITY'] = grouped['ENTITY'].first()
summary['FACILITY'] = grouped['FACILITY'].first()
summary['CEID'] = grouped['CEID'].first()
summary['YEARWW'] = grouped['YEARWW'].first()
summary['DOWNTIME_TYPE'] = grouped['DOWNTIME_TYPE'].first()
summary['DOWNTIME_CLASS'] = grouped['DOWNTIME_CLASS'].first()
summary['AltairFlag'] = grouped['AltairFlag'].first()

# PM event counts
summary['total_pm_events'] = grouped['pm_flex_raw_id'].count()
summary['scheduled_pm_count'] = grouped['scheduled_flag'].apply(lambda x: (x == 1).sum())
summary['unscheduled_pm_count'] = grouped['scheduled_flag'].apply(lambda x: (x == 0).sum())

# PM timing counts
summary['early_pm_count'] = grouped['pm_timing_classification'].apply(lambda x: (x == 'Early').sum())
summary['on_time_pm_count'] = grouped['pm_timing_classification'].apply(lambda x: (x == 'On-Time').sum())
summary['late_pm_count'] = grouped['pm_timing_classification'].apply(lambda x: (x == 'Late').sum())
summary['overdue_pm_count'] = grouped['pm_timing_classification'].apply(lambda x: (x == 'Overdue').sum())

# Downtime aggregations
summary['total_downtime_hours'] = grouped['DOWN_WINDOW_DURATION_HR'].sum()
summary['avg_downtime_hours'] = grouped['DOWN_WINDOW_DURATION_HR'].mean()
summary['median_downtime_hours'] = grouped['DOWN_WINDOW_DURATION_HR'].median()
summary['min_downtime_hours'] = grouped['DOWN_WINDOW_DURATION_HR'].min()
summary['max_downtime_hours'] = grouped['DOWN_WINDOW_DURATION_HR'].max()
summary['downtime_std_dev'] = grouped['DOWN_WINDOW_DURATION_HR'].std()
summary['downtime_count'] = grouped['DOWN_WINDOW_DURATION_HR'].count()

# PM life statistics
summary['avg_pm_life'] = grouped['CUSTOM_DELTA'].mean()
summary['median_pm_life'] = grouped['CUSTOM_DELTA'].median()
summary['pm_life_std_dev'] = grouped['CUSTOM_DELTA'].std()

# Reclean events
summary['reclean_events'] = grouped['reclean_event_flag'].apply(lambda x: (x == 1).sum())

# Work week
summary['ww_year'] = grouped['ww_year'].first()
summary['ww_number'] = grouped['ww_number'].first()

summary = summary.reset_index(drop=True)

# Calculate derived metrics (rates and variances)
summary['unscheduled_pm_rate'] = (summary['unscheduled_pm_count'] / summary['total_pm_events']).fillna(0)
summary['early_pm_rate'] = (summary['early_pm_count'] / summary['total_pm_events']).fillna(0)
summary['overdue_pm_rate'] = (summary['overdue_pm_count'] / summary['total_pm_events']).fillna(0)
summary['reclean_rate'] = (summary['reclean_events'] / summary['total_pm_events']).fillna(0)

summary['scheduled_downtime_hours'] = (
    summary['total_downtime_hours'] * (summary['scheduled_pm_count'] / summary['total_pm_events'])
).fillna(0)
summary['unscheduled_downtime_hours'] = summary['total_downtime_hours'] - summary['scheduled_downtime_hours']

summary['pm_life_variance'] = summary['pm_life_std_dev'] ** 2
summary['downtime_variance'] = summary['downtime_std_dev'] ** 2

summary['record_count'] = summary['total_pm_events']
summary['calculation_timestamp'] = datetime.now()
