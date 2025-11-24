# Quick Start Guide - PM Flex Pipeline

## Initial Setup (One-Time)

### 1. Install Dependencies
```bash
cd etl_pm_flex_ingestion
pip install -r requirements.txt
```

### 2. Configure Environment
```bash
# Copy template
cp .env.template .env

# Edit .env with your credentials:
# - SQL Server username/password
# - Network share path
nano .env  # or use any text editor
```

### 3. Create Database Schema
```bash
python -m etl.setup_database
```

## Running the Pipeline

### Bronze Layer (Raw Data Ingestion)
```bash
# Automatic - finds latest file
python -m etl.bronze.run_bronze_etl

# Or for specific week
python -m etl.bronze.run_bronze_etl --work-week 2025WW22 --no-find-latest
```

### Silver Layer (Coming in Chunk 4)
```bash
python -m etl.silver.run_silver_etl
```

### Gold Layer (Coming in Chunk 5)
```bash
python -m etl.gold.run_gold_etl
```

## Testing Connectivity

### Test SQL Server Connection
```bash
python -c "from connectors import SQLServerConnector; c = SQLServerConnector(); print('✓ Connected')"
```

### Test Network Share Access
```bash
python -c "from etl.bronze import PMFlexFileDiscovery; d = PMFlexFileDiscovery(); print(d.list_available_weeks())"
```

## Monitoring

### Check Logs
```bash
tail -f logs/pm_flex_pipeline.log
```

### Check Load Status (SQL)
```sql
SELECT TOP 10 * 
FROM dbo.pm_flex_load_log 
ORDER BY load_timestamp DESC;
```

### Check Row Count (SQL)
```sql
SELECT COUNT(*) FROM dbo.pm_flex_raw;
```

## Common Commands

### View Help
```bash
python -m etl.bronze.run_bronze_etl --help
```

### Search Further Back
```bash
python -m etl.bronze.run_bronze_etl --max-weeks-back 8
```

### Check Intel WW Calendar
```python
from utils.helpers import get_current_ww
print(get_current_ww())  # Current work week
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| File not found | Check network share path in .env |
| Database connection failed | Verify credentials in .env |
| Schema validation failed | Contact data provider about changes |
| Import errors | Run `pip install -r requirements.txt` |

## File Structure
```
etl_pm_flex_ingestion/
├── .env                  ← Your credentials (not in git)
├── config/config.yaml    ← Pipeline settings
├── data/altair_tools.csv ← Tool classification (PRIVATE)
├── etl/
│   ├── setup_database.py ← Create tables
│   └── bronze/
│       └── run_bronze_etl.py ← Run ingestion
└── logs/                 ← Execution logs
```

## What's Next?

After bronze layer runs successfully:
1. Commit your code to GitHub
2. Wait for Chunk 4 (Silver Layer)
3. Then Chunk 5 (Gold Layer)
4. Finally: Power BI Dashboard!

## Need Help?

Check the detailed documentation:
- Bronze Layer: `etl/bronze/README.md`
- SQL Schema: `sql/ddl/README.md`
- Main README: `README.md`
