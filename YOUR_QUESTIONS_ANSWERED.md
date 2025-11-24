# Your Questions Answered

## Question 1: Single Gold Table vs Multiple PBI Sources?

### **TL;DR Recommendation: Build a Second ETL Pipeline → Unified Gold Table**

### Why This Approach?

**Current State (What We Built):**
```
PM_Flex Pipeline
└─ Gold Tables: fact_pm_kpis_by_site_ww, fact_pm_kpis_by_ceid_ww, etc.
```

**Future State (Recommended):**
```
PM_Flex Pipeline (Phase 1 - DONE)
├─ Gold Tables: fact_pm_kpis_by_site_ww, etc.

Unified Intelligence Pipeline (Phase 2 - FUTURE)
├─ Sources:
│  ├─ fact_pm_kpis_by_site_ww (from PM_Flex pipeline)
│  ├─ PM_all table
│  ├─ Entity_Reset table
│  ├─ Cost tables (part costs, labor costs)
│  └─ Other relevant sources
└─ Output:
   └─ fact_unified_pm_intelligence (ONE master table)
      └─ Power BI connects here
```

### Comparison: Unified Table vs Multiple Sources

| Aspect | Unified Gold Table ✅ | Multiple PBI Sources ❌ |
|--------|----------------------|------------------------|
| **Performance** | Fast (pre-joined) | Slow (PBI joins at query time) |
| **Cost Data** | Properly joined in ETL | Complex DAX relationships |
| **PM_all Integration** | Reconcile duplicates in ETL | Manual filtering in PBI |
| **DAX Complexity** | Simple SUM/AVG | Complex CALCULATE across tables |
| **Business Logic** | Centralized in ETL | Scattered across PBI |
| **Data Governance** | Single source of truth | Multiple versions of metrics |
| **Testing** | Validate one output | Test many relationships |
| **Maintenance** | Update one pipeline | Update PBI model + DAX |
| **Scalability** | Handles growth well | Performance degrades |
| **Troubleshooting** | One place to fix issues | Check ETL + PBI + relationships |

### Real-World Example

**Scenario:** Calculate "ROI per PM with actual part costs"

**With Unified Table (Easy):**
```sql
-- ETL creates this column once
SELECT 
    ENTITY,
    (cost_saved - part_cost - labor_cost) / total_pms as roi_per_pm
FROM fact_unified_pm_intelligence
```

```dax
// Simple DAX in PBI
Avg ROI per PM = AVERAGE(fact_unified_pm_intelligence[roi_per_pm])
```

**With Multiple Sources (Complex):**
```dax
// Complex DAX with multiple relationships
Avg ROI per PM = 
    CALCULATE(
        AVERAGE(
            DIVIDE(
                RELATED(cost_table[cost_saved]) - 
                RELATED(part_costs[part_cost]) - 
                RELATED(labor_costs[labor_cost]),
                fact_pm_kpis[total_pms]
            )
        ),
        // Hope the relationships work...
        USERELATIONSHIP(...),
        CROSSFILTER(...)
    )
```

### Implementation Timeline

**Phase 1 (Current - DONE):**
- ✅ PM_Flex pipeline complete
- ✅ Gold tables created
- ✅ Power BI can connect now

**Phase 2 (1-2 weeks after Phase 1 is stable):**
- Build second ETL pipeline
- Join PM_Flex gold + PM_all + Entity_Reset + Cost tables
- Create `fact_unified_pm_intelligence`
- Migrate Power BI to new table

**Phase 3 (Ongoing):**
- Add more sources as needed
- Evolve unified table
- Deprecate old PM_Flex gold tables (optional)

### Cost Data Challenge

You mentioned cost tables - this is a **key reason** to use unified approach:

**Problem:**
- PM_Flex has `PART_COST_PER_PM` column but it's inaccurate (all ~$1000)
- Real cost data is in separate tables
- Different granularity (part costs vs labor costs vs overhead)

**Solution in Unified Pipeline:**
```python
# ETL properly joins cost data
pm_data = load_pm_flex_gold()
part_costs = load_part_costs()  # Actual costs by part number
labor_costs = load_labor_costs()  # Actual labor rates

# Join on ENTITY + ATTRIBUTE_NAME
unified = pm_data.merge(part_costs, on=['ENTITY', 'ATTRIBUTE_NAME'])
unified = unified.merge(labor_costs, on=['FACILITY', 'CEID'])

# Calculate accurate ROI
unified['actual_roi'] = (
    unified['wafers_saved'] * unified['wafer_value'] -
    unified['actual_part_cost'] -
    unified['actual_labor_cost']
)
```

This is **much better** than trying to do this in Power BI DAX!

### My Strong Recommendation

**Do this:**
1. ✅ Use current PM_Flex pipeline as-is (you're done!)
2. ✅ Test it for 1-2 weeks
3. ✅ Understand your cost data structure
4. ✅ Build Phase 2: Unified Intelligence Pipeline
5. ✅ Migrate Power BI to unified table

**Don't do this:**
❌ Try to manage all relationships in Power BI
❌ Write complex DAX for cross-table calculations
❌ Deal with performance issues later

### When to Build Phase 2

**Wait until:**
- Current pipeline is stable (1-2 weeks)
- You understand PM_all and Entity_Reset structure
- You've mapped out cost table relationships
- Management approves unified approach

**Then we can build it together** - same structure as current pipeline!

---

## Question 2: Easily Editable Business Rules

### **TL;DR: All Business Rules in config.yaml + Validation Script**

### What We Built for You

**1. Centralized Configuration File**
```
config/config.yaml
```
**ALL thresholds and business rules are here:**
- PM timing classification (-15%, +15%, +30%)
- Chronic tool criteria (30% unscheduled, 40% variance)
- Chronic severity levels (Low/Med/High/Critical)
- Chronic score weights (35%, 25%, 20%, 10%, 10%)
- Data retention (26/104/156 weeks)
- Rolling averages (4-week window)

**2. Comprehensive Documentation**
```
docs/BUSINESS_RULES.md
```
- Explains every rule with examples
- Shows how to change each setting
- Provides common scenarios
- Includes before/after examples

**3. Validation Script**
```
validate_config.py
```
- Checks YAML syntax
- Validates threshold values
- Warns about extreme settings
- Run BEFORE deploying changes

### How to Change Rules (Simple!)

**Example: Change "Early" from <-15% to <-10%**

1. **Edit** `config/config.yaml`:
```yaml
pm_timing:
  early_threshold: -10      # Changed from -15
  on_time_min: -10          # Must match
  on_time_max: 15
```

2. **Validate**:
```bash
python validate_config.py
```

3. **Run** pipeline:
```bash
python run_etl_pipeline.py --full-refresh
```

4. **Done!** New data classified with -10% threshold

### All Configurable Settings

| Setting | Location | Example Change |
|---------|----------|----------------|
| **PM Timing** | | |
| Early threshold | `pm_timing.early_threshold` | -15 → -10 |
| Late threshold | `pm_timing.late_threshold` | 15 → 20 |
| Overdue threshold | `pm_timing.overdue_threshold` | 30 → 40 |
| | | |
| **Chronic Tools** | | |
| Unscheduled PM rate | `chronic_tools.chronic_tool_threshold.unscheduled_pm_rate` | 0.30 → 0.25 |
| PM life variance | `chronic_tools.chronic_tool_threshold.pm_life_variance` | 0.40 → 0.30 |
| Minimum PM events | `chronic_tools.chronic_tool_threshold.min_pm_events` | 5 → 10 |
| | | |
| **Chronic Severity** | | |
| Low severity | `chronic_tools.severity_thresholds.low` | 25 → 30 |
| Medium severity | `chronic_tools.severity_thresholds.medium` | 50 → 60 |
| High severity | `chronic_tools.severity_thresholds.high` | 75 → 80 |
| Critical severity | `chronic_tools.severity_thresholds.critical` | 90 → 95 |
| | | |
| **Chronic Score Weights** | | |
| Unscheduled PM weight | `chronic_tools.score_weights.unscheduled_pm_rate` | 0.35 → 0.25 |
| Variance weight | `chronic_tools.score_weights.pm_life_variance` | 0.25 → 0.15 |
| Downtime weight | `chronic_tools.score_weights.downtime_hours` | 0.20 → 0.40 |
| | | |
| **Data Retention** | | |
| Copper layer | `retention.copper_weeks` | 26 → 52 weeks |
| Silver layer | `retention.silver_weeks` | 104 → 156 weeks |
| Gold layer | `retention.gold_weeks` | 156 → 260 weeks |
| | | |
| **Other Settings** | | |
| Rolling avg window | `etl.gold.rolling_window_weeks` | 4 → 8 weeks |
| Max null % | `data_quality.max_null_pct` | 50 → 25% |

### Common Scenarios

**Management says: "Too many chronic tools"**
```yaml
chronic_tools:
  chronic_tool_threshold:
    unscheduled_pm_rate: 0.35    # Increase from 0.30
    pm_life_variance: 0.50       # Increase from 0.40
```

**Management says: "Focus more on downtime"**
```yaml
chronic_tools:
  score_weights:
    downtime_hours: 0.50         # Increase from 0.20
    unscheduled_pm_rate: 0.25    # Decrease from 0.35
    pm_life_variance: 0.15       # Decrease from 0.25
    # Must total 1.0
```

**Management says: "Give more leeway for Late PMs"**
```yaml
pm_timing:
  late_threshold: 20             # Increase from 15
  overdue_threshold: 40          # Increase from 30
```

### Why This Approach Works

✅ **One File** - All rules in config.yaml  
✅ **Well Documented** - Comments explain every setting  
✅ **Validated** - Script checks for errors  
✅ **Testable** - Test on single week first  
✅ **Version Controlled** - Track changes in Git  
✅ **No Code Changes** - Just edit YAML  

### What You DON'T Have to Do

❌ **Don't** edit Python code  
❌ **Don't** modify SQL queries  
❌ **Don't** change DAX in Power BI  
❌ **Don't** redeploy applications  

**Just edit config.yaml and rerun the pipeline!**

---

## Summary

### Question 1: ETL Architecture
**Recommendation:** Build second ETL pipeline for unified gold table

**Benefits:**
- Faster Power BI performance
- Easier cost data integration
- Simpler DAX
- Better maintainability
- Enterprise best practice

**Timeline:** After current pipeline is stable (1-2 weeks)

### Question 2: Configurable Rules
**Solution:** ALL rules in config.yaml + validation script

**Benefits:**
- Easy to change (edit YAML file)
- No code changes needed
- Validated before deployment
- Well documented with examples
- Management can request changes anytime

**Location:**
- Rules: `config/config.yaml`
- Guide: `docs/BUSINESS_RULES.md`
- Validator: `validate_config.py`

---

## Ready to Test!

Your pipeline is **fully functional** with:
✅ Complete Bronze → Silver → Gold layers  
✅ ALL business rules configurable  
✅ Comprehensive documentation  
✅ Validation tools  
✅ Ready for Phase 2 when you are  

**Next steps:**
1. Download the project
2. Test the pipeline
3. Use it for 1-2 weeks
4. Then we can build Phase 2 (unified table) together!
