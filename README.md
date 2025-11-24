# Parts Counter Dashboard Remodel - ETL Pipeline

## Project Overview

This project ingests weekly PM_Flex CSV files from a network share and loads them into SQL Server for analytics and Power BI reporting.

**Complete data pipeline:** Bronze (Raw) → Silver (Enriched) → Gold (KPI Fact Tables)

## Quick Start

```bash
# 1. Setup
git clone <repository>
cp .env.template .env
# Edit .env with your credentials

# 2. Install dependencies
pip install -r requirements.txt

# 3. Create database tables
python -m etl.setup_database

# 4. Run complete pipeline
python run_etl_pipeline.py

# 5. Validate configuration (optional)
python validate_config.py
```

## Configuration & Business Rules

**ALL business rules are in ONE file:** `config/config.yaml`

### Common Configuration Changes

| What to Change | Location | Default | How to Change |
|----------------|----------|---------|---------------|
| PM timing thresholds | `pm_timing` section | Early<-15%, Late>15% | Change threshold values |
| Chronic tool criteria | `chronic_tools.chronic_tool_threshold` | 30% unscheduled | Adjust rates and thresholds |
| Chronic severity levels | `chronic_tools.severity_thresholds` | Low/Med/High/Critical | Change score cutoffs |
| Data retention | `retention` section | 26/104/156 weeks | Adjust weeks per layer |

**See detailed guide:** [`docs/BUSINESS_RULES.md`](docs/BUSINESS_RULES.md)

### Changing Business Rules

1. **Edit** `config/config.yaml`
2. **Validate** changes: `python validate_config.py`
3. **Run** with full refresh: `python run_etl_pipeline.py --full-refresh`

**All thresholds have detailed comments and examples in config.yaml**

## Key Information

- **SQL Server**: TEHAUSTELSQL1
- **Database**: MAData_Output_Production
- **Schema**: dbo
- **Schedule**: Weekly, Wednesday 5:00 AM PST
- **Power BI Refresh**: Weekly, Wednesday 7:00 AM PST

## Pipeline Layers

### Bronze (Copper) - Raw Data
- Discovers PM_Flex.csv files on network share
- Validates schema and loads to `pm_flex_raw`
- Adds Altair classification

**Run:** `python -m etl.bronze.run_bronze_etl`

### Silver - Enriched Data
- Classifies PM timing (Early/On-Time/Late/Overdue)
- Identifies chronic tools with scoring
- Creates aggregated summaries
- Loads to `pm_flex_enriched`, `pm_flex_chronic_tools`

**Run:** `python -m etl.silver.run_silver_etl`

### Gold - KPI Fact Tables
- Creates site-level and CEID-level KPIs
- Calculates 4-week rolling averages
- Tracks chronic tools history
- Optimized for Power BI

**Run:** `python -m etl.gold.run_gold_etl`

### Complete Pipeline
**Run all three layers:** `python run_etl_pipeline.py`

## Documentation

- **Business Rules Guide**: [`docs/BUSINESS_RULES.md`](docs/BUSINESS_RULES.md) - How to change thresholds
- **Quick Start**: [`QUICK_START.md`](QUICK_START.md) - Getting started guide
- **Bronze Layer**: [`etl/bronze/README.md`](etl/bronze/README.md) - File ingestion
- **Silver Layer**: [`etl/silver/README.md`](etl/silver/README.md) - Enrichment logic
- **Gold Layer**: [`etl/gold/README.md`](etl/gold/README.md) - KPI aggregation
- **SQL Schemas**: [`sql/ddl/README.md`](sql/ddl/README.md) - Database setup

## Security Notice

⚠️ The file `data/altair_tools.csv` contains proprietary information and must NOT be shared externally.
