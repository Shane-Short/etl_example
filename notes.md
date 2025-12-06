# Debug: Check what columns we actually have
self.logger.info(f"Columns available after rename: {list(summary.columns)}")

# Calculate unscheduled count (must happen BEFORE rate calculation)
if 'scheduled_pm_count' in summary.columns and 'total_pm_events' in summary.columns:
    summary['unscheduled_pm_count'] = (
        summary['total_pm_events'] - summary['scheduled_pm_count']
    )
else:
    self.logger.warning("Missing scheduled_pm_count or total_pm_events, setting unscheduled_pm_count to 0")
    summary['unscheduled_pm_count'] = 0

# Calculate rates
summary['unscheduled_pm_rate'] = (
    summary['unscheduled_pm_count'] / summary['total_pm_events']
).fillna(0) if 'unscheduled_pm_count' in summary.columns else 0






# Step 2: Create KPI tables
logger.info("=" * 60)
logger.info("Step 2: Creating KPI tables")
logger.info("=" * 60)

try:
    kpi_stats = aggregator.create_kpi_tables(df_enriched)
    
    # Update stats
    stats.update(kpi_stats)
    stats['rows_processed'] = len(df_enriched)
    stats['status'] = 'SUCCESS'
    
except AttributeError as e:
    logger.error(f"Method not found: {str(e)}")
    logger.error(f"Available methods: {[m for m in dir(aggregator) if not m.startswith('_')]}")
    raise
except Exception as e:
    logger.error(f"Failed to create KPI tables: {str(e)}")
    raise



