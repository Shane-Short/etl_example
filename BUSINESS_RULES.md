# Business Rules Configuration Guide

This document explains all business rules and thresholds used in the PM Flex pipeline, and how to change them.

## 📍 Location of All Business Rules

**ALL business rules are in ONE file:**
```
config/config.yaml
```

This file controls every threshold, classification rule, and calculation parameter used in the pipeline.

## 🔧 How to Change Business Rules

1. **Edit** `config/config.yaml`
2. **Save** the file
3. **Run** the pipeline with full refresh:
   ```bash
   python run_etl_pipeline.py --full-refresh
   ```
4. **Verify** changes in Power BI dashboard

**Note:** Changes to config.yaml only affect NEW data processed. Use `--full-refresh` to reprocess all existing data with new rules.

---

## 1. PM Timing Classification

**Location in config.yaml:** `pm_timing` section

### What It Does
Classifies each PM as Early, On-Time, Late, or Overdue based on how the actual wafer count compares to the target.

### Formula
```
deviation = (actual_wafers - target_wafers) / target_wafers × 100
```

### Current Rules
```yaml
early_threshold: -15      # <-15% = Early
on_time_min: -15          # -15% to +15% = On-Time
on_time_max: 15
late_threshold: 15        # +15% to +30% = Late
overdue_threshold: 30     # >+30% = Overdue
```

### Examples

**Target: 10,000 wafers**

| Actual Wafers | Deviation | Classification |
|--------------|-----------|----------------|
| 8,000 | -20% | Early |
| 9,000 | -10% | On-Time |
| 10,500 | +5% | On-Time |
| 11,800 | +18% | Late |
| 13,500 | +35% | Overdue |

### Common Changes

**Make "Early" stricter (catch PMs done too soon):**
```yaml
early_threshold: -10      # Change from -15 to -10
on_time_min: -10          # Must match early_threshold
```

**Make "Late" less strict (more tolerance):**
```yaml
late_threshold: 20        # Change from 15 to 20
overdue_threshold: 40     # Change from 30 to 40
```

**Tighter "On-Time" window:**
```yaml
on_time_min: -10
on_time_max: 10
```

### Impact
- Affects silver layer: `pm_timing_classification` column
- Affects gold layer: `early_pm_count`, `on_time_pm_count`, `late_pm_count`, `overdue_pm_count`
- Affects Power BI: PM timing charts and filters

---

## 2. Chronic Tool Identification

**Location in config.yaml:** `chronic_tools` section

### What It Does
Identifies tools with persistent PM/reliability issues using a composite scoring system.

### Chronic Flag Criteria
A tool is flagged as chronic if:
1. **Has at least `min_pm_events` PMs**, AND
2. Either:
   - **Unscheduled PM rate > `unscheduled_pm_rate` threshold**, OR
   - **PM life variance > `pm_life_variance` threshold**

### Current Rules
```yaml
chronic_tool_threshold:
  min_pm_events: 5                # Minimum PMs for analysis
  unscheduled_pm_rate: 0.30       # 30% unscheduled = chronic
  pm_life_variance: 0.40          # 40% CV = chronic
```

### Examples

**Tool A:**
- PMs: 10 total, 4 unscheduled
- Unscheduled rate: 40% (4/10)
- Result: **CHRONIC** (exceeds 30% threshold)

**Tool B:**
- PMs: 10 total, 2 unscheduled
- Unscheduled rate: 20%
- PM life: avg=10K wafers, std=5K (CV=50%)
- Result: **CHRONIC** (variance exceeds 40% threshold)

**Tool C:**
- PMs: 3 total, 2 unscheduled
- Result: **NOT CHRONIC** (below min_pm_events=5)

### Chronic Score (0-100)

Composite score calculated from 5 factors:

```yaml
score_weights:
  unscheduled_pm_rate: 0.35      # 35%
  pm_life_variance: 0.25         # 25%
  downtime_hours: 0.20           # 20%
  reclean_rate: 0.10             # 10%
  sympathy_pm_rate: 0.10         # 10%
```

**Formula:**
```
chronic_score = (unscheduled_rate_normalized × 0.35) +
                (variance_normalized × 0.25) +
                (downtime_normalized × 0.20) +
                (reclean_rate × 0.10) +
                (sympathy_rate × 0.10)
```

Each factor is normalized to 0-100 before weighting.

### Severity Levels

Based on chronic_score:

```yaml
severity_thresholds:
  low: 25          # 25-49 = Low
  medium: 50       # 50-74 = Medium
  high: 75         # 75-89 = High
  critical: 90     # 90-100 = Critical
```

**Examples:**
- Score 45 → **Low severity**
- Score 65 → **Medium severity**
- Score 80 → **High severity**
- Score 95 → **Critical severity**

### Common Changes

**Too many tools flagged as chronic? Make it stricter:**
```yaml
chronic_tool_threshold:
  unscheduled_pm_rate: 0.35     # Increase from 0.30 to 0.35
  pm_life_variance: 0.50        # Increase from 0.40 to 0.50
  min_pm_events: 10             # Increase from 5 to 10
```

**Too few chronic tools? Make it more sensitive:**
```yaml
chronic_tool_threshold:
  unscheduled_pm_rate: 0.25     # Decrease from 0.30 to 0.25
  pm_life_variance: 0.30        # Decrease from 0.40 to 0.30
  min_pm_events: 3              # Decrease from 5 to 3
```

**Emphasize downtime more than other factors:**
```yaml
score_weights:
  unscheduled_pm_rate: 0.25     # Reduce
  pm_life_variance: 0.15        # Reduce
  downtime_hours: 0.40          # Increase (was 0.20)
  reclean_rate: 0.10            # Keep
  sympathy_pm_rate: 0.10        # Keep
```

**Adjust severity cutoffs:**
```yaml
severity_thresholds:
  low: 30          # Increase from 25
  medium: 60       # Increase from 50
  high: 80         # Increase from 75
  critical: 90     # Keep
```

### Impact
- Affects silver layer: `pm_flex_chronic_tools` table
- Affects gold layer: `chronic_tools_count`, `chronic_tools_pct`, `fact_chronic_tools_history`
- Affects Power BI: Chronic tools dashboard page, filters

---

## 3. Data Retention Policies

**Location in config.yaml:** `retention` section

### What It Does
Controls how long data is kept in each layer before automatic cleanup.

### Current Rules
```yaml
retention:
  copper_weeks: 26      # 26 weeks (6 months) in copper layer
  silver_weeks: 104     # 104 weeks (2 years) in silver layer
  gold_weeks: 156       # 156 weeks (3 years) in gold layer
```

### Examples

**Keep less historical data (save storage):**
```yaml
retention:
  copper_weeks: 13      # 3 months
  silver_weeks: 52      # 1 year
  gold_weeks: 104       # 2 years
```

**Keep more historical data (more trending):**
```yaml
retention:
  copper_weeks: 52      # 1 year
  silver_weeks: 156     # 3 years
  gold_weeks: 260       # 5 years
```

### Impact
- Affects database storage requirements
- Affects how far back Power BI can show trends
- Cleanup runs automatically via stored procedure `sp_cleanup_pm_flex_copper`

---

## 4. Data Quality Rules

**Location in config.yaml:** `data_quality` section

### What It Does
Validates data quality during ingestion and alerts on issues.

### Current Rules
```yaml
data_quality:
  critical_columns:     # Must have data
    - ENTITY
    - FACILITY
    - CEID
    - YEARWW
  max_null_pct: 50      # Fail if >50% nulls in critical columns
  row_count_tolerance: 0.20  # ±20% from previous week
```

### Examples

**More strict quality checks:**
```yaml
data_quality:
  max_null_pct: 25              # Fail if >25% nulls
  row_count_tolerance: 0.10     # ±10% tolerance
```

**Less strict (allow more variance):**
```yaml
data_quality:
  max_null_pct: 75              # Allow up to 75% nulls
  row_count_tolerance: 0.30     # ±30% tolerance
```

### Impact
- Affects bronze layer data validation
- Triggers warnings/errors in pipeline logs
- Affects `data_quality_score` column in silver layer

---

## 5. Rolling Averages

**Location in config.yaml:** `etl.gold` section

### What It Does
Calculates N-week rolling averages for trending.

### Current Rules
```yaml
etl:
  gold:
    rolling_window_weeks: 4     # 4-week rolling averages
```

### Examples

**8-week rolling averages (smoother trends):**
```yaml
rolling_window_weeks: 8
```

**2-week rolling averages (more responsive):**
```yaml
rolling_window_weeks: 2
```

### Impact
- Affects `rolling_4wk_avg_pm_life`, `rolling_4wk_pm_count`, `rolling_4wk_downtime_hours` in gold layer
- Affects trend lines in Power BI

---

## 6. ETL Behavior Settings

**Location in config.yaml:** `etl` section

### Bronze Layer
```yaml
etl:
  bronze:
    max_weeks_back: 4           # Search up to 4 weeks for files
    validate_schema: true       # Validate CSV schema
    add_altair_flag: true       # Add Altair classification
    batch_size: 1000            # Rows per SQL insert batch
```

### Silver Layer
```yaml
etl:
  silver:
    incremental_load: true      # Only process new data
```

### Impact
- Controls pipeline behavior
- Affects performance and error handling

---

## Testing Your Changes

After modifying `config.yaml`:

### 1. Validate YAML Syntax
```bash
python -c "import yaml; yaml.safe_load(open('config/config.yaml'))"
```

### 2. Test with Small Dataset
```bash
# Process single week to test changes
python run_etl_pipeline.py --work-week 2025WW22
```

### 3. Full Refresh
```bash
# Reprocess all data with new rules
python run_etl_pipeline.py --full-refresh
```

### 4. Verify in Power BI
- Refresh Power BI dataset
- Check chronic tool counts
- Check PM timing distribution
- Verify trends look correct

---

## Common Scenarios

### "Management wants stricter chronic tool criteria"

**Change:**
```yaml
chronic_tools:
  chronic_tool_threshold:
    unscheduled_pm_rate: 0.25   # Was 0.30
    pm_life_variance: 0.30      # Was 0.40
```

**Result:** Fewer tools flagged as chronic, only the worst cases

### "We want to focus on downtime more"

**Change:**
```yaml
chronic_tools:
  score_weights:
    downtime_hours: 0.50        # Was 0.20
    unscheduled_pm_rate: 0.25   # Was 0.35
    # Adjust others to total 1.0
```

**Result:** Chronic score emphasizes downtime impact

### "Early PMs are too aggressive, give more margin"

**Change:**
```yaml
pm_timing:
  early_threshold: -20          # Was -15
  on_time_min: -20              # Was -15
```

**Result:** Only PMs done >20% early are flagged

### "Need more historical data for trending"

**Change:**
```yaml
retention:
  gold_weeks: 260               # Was 156 (3 years → 5 years)
```

**Result:** Keep 5 years of gold layer data

---

## Validation & Monitoring

After changing rules, monitor:

### Check Logs
```bash
tail -f logs/pm_flex_pipeline.log
```

Look for:
- "PM Timing distribution"
- "Chronic tools: X of Y"
- "Severity distribution"

### Query Database
```sql
-- Check PM timing distribution
SELECT 
    pm_timing_classification,
    COUNT(*) as count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) as pct
FROM pm_flex_enriched
GROUP BY pm_timing_classification;

-- Check chronic tool counts
SELECT 
    chronic_severity,
    COUNT(*) as count
FROM pm_flex_chronic_tools
WHERE chronic_flag = 1
GROUP BY chronic_severity;
```

### Power BI Dashboard
- Check "Chronic Tools" page - does count look reasonable?
- Check "PM Timing" visuals - does distribution make sense?
- Check trend lines - are they smooth?

---

## Backup & Version Control

**Before making major changes:**

1. **Backup current config:**
   ```bash
   cp config/config.yaml config/config.yaml.backup
   ```

2. **Document changes:**
   - Add comments in YAML
   - Note date and reason for change
   - Keep old values in comments

3. **Commit to Git:**
   ```bash
   git commit -m "Changed chronic tool threshold from 0.30 to 0.25 per management request"
   ```

---

## Getting Help

If you're unsure about a change:

1. **Test on small dataset first** (`--work-week 2025WW22`)
2. **Check the YAML comments** - they have examples
3. **Review this document** for common scenarios
4. **Check logs** for validation errors
5. **Ask the data engineering team**

---

## Summary

✅ **All business rules in one place**: `config/config.yaml`  
✅ **Easy to change**: Edit file, save, run pipeline  
✅ **Well documented**: Comments and examples in YAML  
✅ **Version controlled**: Track changes in Git  
✅ **Testable**: Use `--work-week` for quick tests  
✅ **Validated**: Pipeline checks for invalid values  

**Remember:** Always test changes on a single week before running `--full-refresh` on all data!
