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
    
    -- Unique constraint to prevent duplicates
    CONSTRAINT UQ_fact_pm_kpis_by_ceid_ww UNIQUE (FACILITY, CEID, YEARWW)
);
GO

CREATE NONCLUSTERED INDEX IX_fact_ceid_ww_ceid
ON dbo.fact_pm_kpis_by_ceid_ww(CEID, ww_year, ww_number);
GO

CREATE NONCLUSTERED INDEX IX_fact_ceid_ww_altair
ON dbo.fact_pm_kpis_by_ceid_ww(AltairFlag, ww_year, ww_number);
GO

CREATE NONCLUSTERED INDEX IX_fact_ceid_ww_facility
ON dbo.fact_pm_kpis_by_ceid_ww(FACILITY);
GO

CREATE NONCLUSTERED INDEX IX_fact_ceid_ww_yearww
ON dbo.fact_pm_kpis_by_ceid_ww(YEARWW);
GO

-- =====================================================
-- Table: fact_part_replacement_summary
-- Description: Part replacement trends and analysis
-- =====================================================

IF OBJECT_ID('dbo.fact_part_replacement_summary', 'U') IS NOT NULL
    DROP TABLE dbo.fact_part_replacement_summary;
GO

CREATE TABLE dbo.fact_part_replacement_summary (
    part_summary_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Dimensions
    FACILITY VARCHAR(50) NOT NULL,
    CEID VARCHAR(100) NOT NULL,
    YEARWW VARCHAR(20) NOT NULL,
    ww_year INT,
    ww_number INT,
    
    -- Part Cost Metrics
    total_part_cost FLOAT,
    avg_part_cost FLOAT,
    part_replacement_count INT,
    total_part_savings FLOAT,
    
    -- Metadata
    calculation_timestamp DATETIME DEFAULT GETDATE(),
    
    -- Unique constraint
    CONSTRAINT UQ_fact_part_replacement UNIQUE (FACILITY, CEID, YEARWW)
);
GO

CREATE NONCLUSTERED INDEX IX_fact_part_summary_facility
ON dbo.fact_part_replacement_summary(FACILITY);
GO

CREATE NONCLUSTERED INDEX IX_fact_part_summary_yearww
ON dbo.fact_part_replacement_summary(YEARWW);
GO

-- =====================================================
-- Table: fact_chronic_tools_history
-- Description: Historical chronic tool tracking
-- =====================================================

IF OBJECT_ID('dbo.fact_chronic_tools_history', 'U') IS NOT NULL
    DROP TABLE dbo.fact_chronic_tools_history;
GO

CREATE TABLE dbo.fact_chronic_tools_history (
    chronic_history_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Dimensions
    ENTITY VARCHAR(100),
    FACILITY VARCHAR(50),
    CEID VARCHAR(100),
    YEARWW VARCHAR(20),
    ww_year INT,
    ww_number INT,
    AltairFlag VARCHAR(20),
    
    -- PM Metrics
    total_pm_events INT,
    unscheduled_pm_count INT,
    unscheduled_pm_rate FLOAT,
    avg_pm_life FLOAT,
    
    -- Downtime Metrics
    total_downtime_hours FLOAT,
    avg_downtime_hours_per_pm FLOAT,
    
    -- PM Type Rates
    reclean_rate FLOAT,
    sympathy_pm_rate FLOAT,
    
    -- Metadata
    calculation_timestamp DATETIME DEFAULT GETDATE(),
    
    -- Unique constraint
    CONSTRAINT UQ_fact_chronic_history UNIQUE (ENTITY, FACILITY, CEID, YEARWW)
);
GO

CREATE NONCLUSTERED INDEX IX_fact_chronic_history_entity
ON dbo.fact_chronic_tools_history(ENTITY, ww_year, ww_number);
GO

CREATE NONCLUSTERED INDEX IX_fact_chronic_history_facility
ON dbo.fact_chronic_tools_history(FACILITY);
GO

CREATE NONCLUSTERED INDEX IX_fact_chronic_history_ceid
ON dbo.fact_chronic_tools_history(CEID);
GO

CREATE NONCLUSTERED INDEX IX_fact_chronic_history_yearww
ON dbo.fact_chronic_tools_history(YEARWW);
GO

PRINT 'Gold layer schema created successfully';
GO
