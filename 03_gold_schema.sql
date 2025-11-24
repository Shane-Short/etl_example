-- =====================================================
-- PM Flex Pipeline - Gold Layer (KPI/Fact Tables)
-- =====================================================
-- This script creates the analytics-ready fact tables
-- Database: MAData_Output_Production
-- Schema: dbo
-- =====================================================

USE [MAData_Output_Production];
GO

-- =====================================================
-- Table: fact_pm_kpis_by_site_ww
-- Description: KPIs aggregated by site and work week
-- =====================================================

IF OBJECT_ID('dbo.fact_pm_kpis_by_site_ww', 'U') IS NOT NULL
    DROP TABLE dbo.fact_pm_kpis_by_site_ww;
GO

CREATE TABLE dbo.fact_pm_kpis_by_site_ww (
    fact_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Dimensions
    FACILITY NVARCHAR(50),
    ww_year INT,
    ww_number INT,
    YEARWW NVARCHAR(20),
    
    -- PM Event Counts
    total_pm_events INT,
    scheduled_pm_events INT,
    unscheduled_pm_events INT,
    
    -- PM Timing
    early_pm_count INT,
    on_time_pm_count INT,
    late_pm_count INT,
    overdue_pm_count INT,
    
    -- Downtime Hours
    total_downtime_hours FLOAT,
    scheduled_downtime_hours FLOAT,
    unscheduled_downtime_hours FLOAT,
    avg_downtime_per_pm FLOAT,
    
    -- PM Life Metrics
    avg_pm_life FLOAT,
    median_pm_life FLOAT,
    target_pm_life FLOAT,
    pm_life_variance FLOAT,
    
    -- Rates (%)
    unscheduled_pm_rate FLOAT,
    early_pm_rate FLOAT,
    on_time_pm_rate FLOAT,
    overdue_pm_rate FLOAT,
    
    -- Chronic Tools
    chronic_tools_count INT,
    total_tools_count INT,
    chronic_tools_pct FLOAT,
    
    -- Rolling Averages (4-week)
    rolling_4wk_avg_pm_life FLOAT,
    rolling_4wk_pm_count FLOAT,
    rolling_4wk_downtime_hours FLOAT,
    
    -- Metadata
    calculation_timestamp DATETIME2 NOT NULL DEFAULT GETDATE()
);
GO

CREATE NONCLUSTERED INDEX IX_fact_site_ww_facility
ON dbo.fact_pm_kpis_by_site_ww(FACILITY, ww_year, ww_number);
GO

CREATE NONCLUSTERED INDEX IX_fact_site_ww_yearww
ON dbo.fact_pm_kpis_by_site_ww(YEARWW);
GO

-- =====================================================
-- Table: fact_pm_kpis_by_ceid_ww
-- Description: KPIs aggregated by CEID and work week
-- =====================================================

IF OBJECT_ID('dbo.fact_pm_kpis_by_ceid_ww', 'U') IS NOT NULL
    DROP TABLE dbo.fact_pm_kpis_by_ceid_ww;
GO

CREATE TABLE dbo.fact_pm_kpis_by_ceid_ww (
    fact_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Dimensions
    CEID NVARCHAR(50),
    FACILITY NVARCHAR(50),
    AltairFlag NVARCHAR(20),
    ww_year INT,
    ww_number INT,
    YEARWW NVARCHAR(20),
    
    -- PM Event Counts
    total_pm_events INT,
    scheduled_pm_events INT,
    unscheduled_pm_events INT,
    
    -- Downtime
    total_downtime_hours FLOAT,
    unscheduled_downtime_hours FLOAT,
    
    -- PM Life
    avg_pm_life FLOAT,
    median_pm_life FLOAT,
    pm_life_std_dev FLOAT,
    
    -- Rates
    unscheduled_pm_rate FLOAT,
    early_pm_rate FLOAT,
    overdue_pm_rate FLOAT,
    
    -- Tool Counts
    total_chambers INT,
    chronic_chambers INT,
    
    -- Metadata
    calculation_timestamp DATETIME2 NOT NULL DEFAULT GETDATE()
);
GO

CREATE NONCLUSTERED INDEX IX_fact_ceid_ww_ceid
ON dbo.fact_pm_kpis_by_ceid_ww(CEID, ww_year, ww_number);
GO

CREATE NONCLUSTERED INDEX IX_fact_ceid_ww_altair
ON dbo.fact_pm_kpis_by_ceid_ww(AltairFlag, ww_year, ww_number);
GO

-- =====================================================
-- Table: fact_part_replacement_summary
-- Description: Part replacement trends and analysis
-- =====================================================

IF OBJECT_ID('dbo.fact_part_replacement_summary', 'U') IS NOT NULL
    DROP TABLE dbo.fact_part_replacement_summary;
GO

CREATE TABLE dbo.fact_part_replacement_summary (
    fact_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Dimensions
    ATTRIBUTE_NAME NVARCHAR(200),  -- Part name
    ENTITY NVARCHAR(100),
    FACILITY NVARCHAR(50),
    CEID NVARCHAR(50),
    ww_year INT,
    ww_number INT,
    YEARWW NVARCHAR(20),
    
    -- Replacement Metrics
    replacement_count INT,
    avg_wafers_at_replacement FLOAT,
    median_wafers_at_replacement FLOAT,
    min_wafers_at_replacement FLOAT,
    max_wafers_at_replacement FLOAT,
    
    -- Part Life Analysis
    avg_part_life_days FLOAT,
    part_life_variance FLOAT,
    
    -- Early/Late Replacements
    early_replacement_count INT,
    late_replacement_count INT,
    
    -- Metadata
    calculation_timestamp DATETIME2 NOT NULL DEFAULT GETDATE()
);
GO

CREATE NONCLUSTERED INDEX IX_fact_part_summary_part
ON dbo.fact_part_replacement_summary(ATTRIBUTE_NAME, ww_year, ww_number);
GO

CREATE NONCLUSTERED INDEX IX_fact_part_summary_entity
ON dbo.fact_part_replacement_summary(ENTITY, ATTRIBUTE_NAME);
GO

-- =====================================================
-- Table: fact_chronic_tools_history
-- Description: Historical chronic tool tracking
-- =====================================================

IF OBJECT_ID('dbo.fact_chronic_tools_history', 'U') IS NOT NULL
    DROP TABLE dbo.fact_chronic_tools_history;
GO

CREATE TABLE dbo.fact_chronic_tools_history (
    fact_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Tool identification
    ENTITY NVARCHAR(100),
    FACILITY NVARCHAR(50),
    CEID NVARCHAR(50),
    AltairFlag NVARCHAR(20),
    
    -- Time period
    ww_year INT,
    ww_number INT,
    YEARWW NVARCHAR(20),
    
    -- Chronic status
    chronic_flag BIT,
    chronic_score FLOAT,
    chronic_severity NVARCHAR(20),
    
    -- Contributing metrics
    unscheduled_pm_count INT,
    unscheduled_pm_rate FLOAT,
    pm_life_variance FLOAT,
    total_downtime_hours FLOAT,
    
    -- Week-over-week change
    chronic_score_change FLOAT,
    status_changed BIT,  -- 1 if chronic status changed from previous week
    
    -- Metadata
    calculation_timestamp DATETIME2 NOT NULL DEFAULT GETDATE()
);
GO

CREATE NONCLUSTERED INDEX IX_fact_chronic_history_entity
ON dbo.fact_chronic_tools_history(ENTITY, ww_year, ww_number);
GO

CREATE NONCLUSTERED INDEX IX_fact_chronic_history_flag
ON dbo.fact_chronic_tools_history(chronic_flag, chronic_score DESC);
GO

-- =====================================================
-- View: vw_executive_dashboard_kpis
-- Description: Pre-aggregated KPIs for executive dashboard
-- =====================================================

CREATE OR ALTER VIEW dbo.vw_executive_dashboard_kpis
AS
SELECT 
    f.FACILITY,
    f.YEARWW,
    f.ww_year,
    f.ww_number,
    
    -- PM Counts
    f.total_pm_events,
    f.scheduled_pm_events,
    f.unscheduled_pm_events,
    
    -- Key Rates
    f.unscheduled_pm_rate,
    f.on_time_pm_rate,
    f.chronic_tools_pct,
    
    -- Downtime
    f.total_downtime_hours,
    f.avg_downtime_per_pm,
    
    -- PM Life
    f.avg_pm_life,
    f.target_pm_life,
    CASE 
        WHEN f.target_pm_life > 0 
        THEN ((f.avg_pm_life - f.target_pm_life) / f.target_pm_life) * 100
        ELSE NULL
    END AS pm_life_vs_target_pct,
    
    -- Trends
    f.rolling_4wk_avg_pm_life,
    f.rolling_4wk_pm_count,
    
    -- Chronic Tools
    f.chronic_tools_count,
    f.total_tools_count,
    
    -- Metadata
    f.calculation_timestamp
FROM dbo.fact_pm_kpis_by_site_ww f;
GO

-- =====================================================
-- View: vw_chronic_tools_current
-- Description: Current chronic tool status (latest calculation)
-- =====================================================

CREATE OR ALTER VIEW dbo.vw_chronic_tools_current
AS
WITH latest_calc AS (
    SELECT MAX(calculation_timestamp) AS latest_timestamp
    FROM dbo.pm_flex_chronic_tools
)
SELECT 
    c.ENTITY,
    c.FACILITY,
    c.CEID,
    c.AltairFlag,
    c.chronic_flag,
    c.chronic_score,
    c.chronic_severity,
    c.unscheduled_pm_rate,
    c.pm_life_variance,
    c.total_pm_events,
    c.unscheduled_pm_count,
    c.calculation_timestamp
FROM dbo.pm_flex_chronic_tools c
CROSS JOIN latest_calc l
WHERE c.calculation_timestamp = l.latest_timestamp
  AND c.chronic_flag = 1;
GO

-- =====================================================
-- Stored Procedure: Calculate Rolling Averages
-- =====================================================

CREATE OR ALTER PROCEDURE dbo.sp_calculate_rolling_averages
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        -- Update 4-week rolling averages in fact_pm_kpis_by_site_ww
        UPDATE f
        SET 
            rolling_4wk_avg_pm_life = r.avg_pm_life_4wk,
            rolling_4wk_pm_count = r.total_pm_4wk,
            rolling_4wk_downtime_hours = r.total_downtime_4wk
        FROM dbo.fact_pm_kpis_by_site_ww f
        CROSS APPLY (
            SELECT 
                AVG(f2.avg_pm_life) AS avg_pm_life_4wk,
                SUM(f2.total_pm_events) AS total_pm_4wk,
                SUM(f2.total_downtime_hours) AS total_downtime_4wk
            FROM dbo.fact_pm_kpis_by_site_ww f2
            WHERE f2.FACILITY = f.FACILITY
              AND f2.ww_year = f.ww_year
              AND f2.ww_number BETWEEN (f.ww_number - 3) AND f.ww_number
        ) r;
        
        -- Log success
        INSERT INTO dbo.pm_flex_load_log (
            source_file,
            rows_loaded,
            load_status,
            layer
        )
        VALUES (
            'ROLLING_AVERAGES',
            @@ROWCOUNT,
            'SUCCESS',
            'GOLD'
        );
        
    END TRY
    BEGIN CATCH
        -- Log failure
        INSERT INTO dbo.pm_flex_load_log (
            source_file,
            load_status,
            error_message,
            layer
        )
        VALUES (
            'ROLLING_AVERAGES',
            'FAILED',
            ERROR_MESSAGE(),
            'GOLD'
        );
        
        THROW;
    END CATCH
END;
GO

-- =====================================================
-- Stored Procedure: Refresh All Gold Layer Tables
-- =====================================================

CREATE OR ALTER PROCEDURE dbo.sp_refresh_gold_layer
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        EXEC dbo.sp_calculate_rolling_averages;
        
        PRINT 'Gold layer refresh completed successfully';
        
    END TRY
    BEGIN CATCH
        PRINT 'Gold layer refresh failed: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

PRINT 'Gold layer schema created successfully';
GO
