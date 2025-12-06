-- Drop existing table if it exists
IF OBJECT_ID('dbo.fact_pm_kpis_by_site_ww', 'U') IS NOT NULL
    DROP TABLE dbo.fact_pm_kpis_by_site_ww;
GO

CREATE TABLE dbo.fact_pm_kpis_by_site_ww (
    site_kpi_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Dimensions
    FACILITY VARCHAR(50) NOT NULL,
    YEARWW VARCHAR(20) NOT NULL,
    ww_year INT,
    ww_number INT,
    
    -- PM Event Counts
    total_pm_events INT,
    scheduled_pm_count INT,
    unscheduled_pm_count INT,
    early_pm_count INT,
    on_time_pm_count INT,
    late_pm_count INT,
    overdue_pm_count INT,
    
    -- PM Life Statistics
    avg_pm_life FLOAT,
    median_pm_life FLOAT,
    pm_life_std_dev FLOAT,
    
    -- Downtime Statistics
    total_downtime_hours FLOAT,
    avg_downtime_hours FLOAT,
    median_downtime_hours FLOAT,
    
    -- PM Rates
    unscheduled_pm_rate FLOAT,
    early_pm_rate FLOAT,
    overdue_pm_rate FLOAT,
    
    -- Reclean Metrics
    reclean_count INT,
    reclean_rate FLOAT,
    
    -- Sympathy PM Metrics
    sympathy_pm_count INT,
    
    -- Metadata
    calculation_timestamp DATETIME DEFAULT GETDATE(),
    
    -- Unique constraint to prevent duplicates
    CONSTRAINT UQ_fact_pm_kpis_by_site_ww UNIQUE (FACILITY, YEARWW)
);
GO









-- Drop existing table if it exists
IF OBJECT_ID('dbo.fact_pm_kpis_by_ceid_ww', 'U') IS NOT NULL
    DROP TABLE dbo.fact_pm_kpis_by_ceid_ww;
GO

CREATE TABLE dbo.fact_pm_kpis_by_ceid_ww (
    ceid_kpi_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Dimensions
    FACILITY VARCHAR(50) NOT NULL,
    CEID VARCHAR(100) NOT NULL,
    YEARWW VARCHAR(20) NOT NULL,
    ww_year INT,
    ww_number INT,
    AltairFlag VARCHAR(20),
    
    -- PM Event Counts
    total_pm_events INT,
    scheduled_pm_count INT,
    unscheduled_pm_count INT,
    early_pm_count INT,
    on_time_pm_count INT,
    late_pm_count INT,
    overdue_pm_count INT,
    
    -- PM Life Statistics
    avg_pm_life FLOAT,
    median_pm_life FLOAT,
    pm_life_std_dev FLOAT,
    
    -- Downtime Statistics
    total_downtime_hours FLOAT,
    avg_downtime_hours FLOAT,
    median_downtime_hours FLOAT,
    
    -- PM Rates
    unscheduled_pm_rate FLOAT,
    
    -- Reclean Metrics
    reclean_count INT,
    reclean_rate FLOAT,
    
    -- Metadata
    calculation_timestamp DATETIME DEFAULT GETDATE(),
    
    -- Unique constraint to prevent duplicates (different name than site table)
    CONSTRAINT UQ_fact_pm_kpis_by_ceid_ww UNIQUE (FACILITY, CEID, YEARWW)
);
GO
