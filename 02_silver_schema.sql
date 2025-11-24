-- =====================================================
-- PM Flex Pipeline - Silver Layer (Enriched Data)
-- =====================================================
-- This script creates the enriched and aggregated tables
-- Database: MAData_Output_Production
-- Schema: dbo
-- =====================================================

USE [MAData_Output_Production];
GO

-- =====================================================
-- Table: pm_flex_enriched
-- Description: Enriched PM data with business logic applied
-- Retention: Cumulative (2+ years)
-- =====================================================

IF OBJECT_ID('dbo.pm_flex_enriched', 'U') IS NOT NULL
    DROP TABLE dbo.pm_flex_enriched;
GO

CREATE TABLE dbo.pm_flex_enriched (
    -- Primary key
    pm_flex_enriched_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    source_pm_flex_raw_id BIGINT,  -- Reference to copper table
    
    -- All original columns from copper (abbreviated for space)
    -- Copy all 88 columns from pm_flex_raw here...
    ENTITY NVARCHAR(100),
    FACILITY NVARCHAR(50),
    UNIQUE_ENTITY_ID NVARCHAR(150),
    CEID NVARCHAR(50),
    YEARWW NVARCHAR(20),
    TXN_DATE DATETIME2,
    PM_NAME NVARCHAR(200),
    ATTRIBUTE_NAME NVARCHAR(200),
    CUSTOM_DELTA FLOAT,
    COUNTER_UPPER_VALUE FLOAT,
    DOWNTIME_TYPE NVARCHAR(50),
    DOWNTIME_CLASS NVARCHAR(100),
    DOWNTIME_SUBCLASS NVARCHAR(100),
    DOWN_WINDOW_DURATION_HR FLOAT,
    PM_FREQUENCY NVARCHAR(50),
    Sympathy_PM FLOAT,
    
    -- NEW ENRICHED COLUMNS
    
    -- Altair Classification
    AltairFlag NVARCHAR(20),  -- ALTAIR, NON-ALTAIR, MIX, UNKNOWN
    
    -- Time-based fields
    ww_year INT,
    ww_number INT,
    fiscal_quarter INT,
    fiscal_month INT,
    
    -- PM Timing Classification
    pm_timing_classification NVARCHAR(50),  -- Early, On-Time, Late, Overdue
    pm_life_vs_target FLOAT,  -- Wafer count - target
    pm_life_vs_target_pct FLOAT,  -- Percentage deviation from target
    
    -- Scheduled vs Unscheduled
    scheduled_flag BIT,  -- 1 = Scheduled, 0 = Unscheduled
    scheduled_category NVARCHAR(50),  -- Scheduled, Unscheduled, Unknown
    
    -- Chronic Tool Flag
    chronic_tool_flag BIT,  -- 1 = Chronic, 0 = Normal
    chronic_score FLOAT,  -- Composite score (0-100)
    
    -- Downtime Categories
    downtime_category NVARCHAR(100),  -- Combined Type + Class
    downtime_primary_reason NVARCHAR(200),  -- Primary root cause
    
    -- PM Cycle Metrics
    pm_cycle_efficiency FLOAT,  -- pm_cycle_utilization if available
    pm_duration_outlier_flag BIT,  -- Based on outlier detection
    reclean_event_flag BIT,  -- Based on Reclean_Label
    sympathy_pm_flag BIT,  -- Based on Sympathy_PM
    
    -- Metadata
    enrichment_timestamp DATETIME2 NOT NULL DEFAULT GETDATE(),
    data_quality_score FLOAT,  -- 0-100 based on completeness
    
    CONSTRAINT FK_pm_flex_enriched_raw 
        FOREIGN KEY (source_pm_flex_raw_id) 
        REFERENCES dbo.pm_flex_raw(pm_flex_raw_id)
);
GO

-- Indexes for pm_flex_enriched
CREATE NONCLUSTERED INDEX IX_pm_flex_enriched_entity
ON dbo.pm_flex_enriched(ENTITY, FACILITY, CEID);
GO

CREATE NONCLUSTERED INDEX IX_pm_flex_enriched_date
ON dbo.pm_flex_enriched(TXN_DATE)
INCLUDE (ENTITY, DOWNTIME_TYPE, chronic_tool_flag);
GO

CREATE NONCLUSTERED INDEX IX_pm_flex_enriched_ww
ON dbo.pm_flex_enriched(ww_year, ww_number)
INCLUDE (ENTITY, CEID, FACILITY);
GO

CREATE NONCLUSTERED INDEX IX_pm_flex_enriched_altair
ON dbo.pm_flex_enriched(AltairFlag)
INCLUDE (ENTITY, CEID);
GO

CREATE NONCLUSTERED INDEX IX_pm_flex_enriched_chronic
ON dbo.pm_flex_enriched(chronic_tool_flag, chronic_score DESC);
GO

-- =====================================================
-- Table: pm_flex_downtime_summary
-- Description: Aggregated downtime metrics by site, CEID, and WW
-- =====================================================

IF OBJECT_ID('dbo.pm_flex_downtime_summary', 'U') IS NOT NULL
    DROP TABLE dbo.pm_flex_downtime_summary;
GO

CREATE TABLE dbo.pm_flex_downtime_summary (
    downtime_summary_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Dimensions
    FACILITY NVARCHAR(50),
    CEID NVARCHAR(50),
    ww_year INT,
    ww_number INT,
    YEARWW NVARCHAR(20),
    AltairFlag NVARCHAR(20),
    
    -- PM Counts
    total_pm_events INT,
    scheduled_pm_count INT,
    unscheduled_pm_count INT,
    early_pm_count INT,
    on_time_pm_count INT,
    late_pm_count INT,
    overdue_pm_count INT,
    
    -- Downtime Hours
    total_downtime_hours FLOAT,
    scheduled_downtime_hours FLOAT,
    unscheduled_downtime_hours FLOAT,
    
    -- PM Life Metrics
    avg_pm_life FLOAT,
    median_pm_life FLOAT,
    pm_life_std_dev FLOAT,
    pm_life_variance FLOAT,
    
    -- Rates
    unscheduled_pm_rate FLOAT,  -- unscheduled / total
    early_pm_rate FLOAT,
    overdue_pm_rate FLOAT,
    
    -- Chronic Tools
    chronic_tools_count INT,
    avg_chronic_score FLOAT,
    
    -- Metadata
    calculation_timestamp DATETIME2 NOT NULL DEFAULT GETDATE(),
    record_count INT  -- Number of raw records aggregated
);
GO

CREATE NONCLUSTERED INDEX IX_downtime_summary_facility_ww
ON dbo.pm_flex_downtime_summary(FACILITY, ww_year, ww_number);
GO

CREATE NONCLUSTERED INDEX IX_downtime_summary_ceid_ww
ON dbo.pm_flex_downtime_summary(CEID, ww_year, ww_number);
GO

-- =====================================================
-- Table: pm_flex_chronic_tools
-- Description: Entity-level chronic tool identification
-- =====================================================

IF OBJECT_ID('dbo.pm_flex_chronic_tools', 'U') IS NOT NULL
    DROP TABLE dbo.pm_flex_chronic_tools;
GO

CREATE TABLE dbo.pm_flex_chronic_tools (
    chronic_tools_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Tool identification
    ENTITY NVARCHAR(100),
    FACILITY NVARCHAR(50),
    CEID NVARCHAR(50),
    AltairFlag NVARCHAR(20),
    
    -- Analysis period
    analysis_start_date DATE,
    analysis_end_date DATE,
    weeks_analyzed INT,
    
    -- Chronic indicators
    chronic_flag BIT,
    chronic_score FLOAT,  -- Composite score (0-100)
    
    -- Contributing factors
    unscheduled_pm_rate FLOAT,
    pm_life_variance FLOAT,
    avg_downtime_hours_per_pm FLOAT,
    reclean_rate FLOAT,
    sympathy_pm_rate FLOAT,
    
    -- Counts
    total_pm_events INT,
    unscheduled_pm_count INT,
    
    -- Severity classification
    chronic_severity NVARCHAR(20),  -- Low, Medium, High, Critical
    
    -- Metadata
    calculation_timestamp DATETIME2 NOT NULL DEFAULT GETDATE(),
    
    CONSTRAINT UQ_chronic_tools_entity 
        UNIQUE (ENTITY, analysis_start_date, analysis_end_date)
);
GO

CREATE NONCLUSTERED INDEX IX_chronic_tools_flag
ON dbo.pm_flex_chronic_tools(chronic_flag, chronic_score DESC);
GO

CREATE NONCLUSTERED INDEX IX_chronic_tools_entity
ON dbo.pm_flex_chronic_tools(ENTITY)
INCLUDE (chronic_flag, chronic_score, chronic_severity);
GO

-- =====================================================
-- Dimension Tables
-- =====================================================

-- DimDate (Work Week Calendar)
IF OBJECT_ID('dbo.DimDate', 'U') IS NOT NULL
    DROP TABLE dbo.DimDate;
GO

CREATE TABLE dbo.DimDate (
    date_key INT PRIMARY KEY,  -- YYYYMMDD
    date_value DATE NOT NULL,
    day_of_year INT,
    fiscal_year INT,
    fiscal_quarter INT,
    fiscal_month INT,
    fiscal_week INT,
    work_week NVARCHAR(20),  -- YYYYWWnn
    day_of_week INT,  -- 0=Monday, 6=Sunday
    day_name NVARCHAR(20),
    is_weekend BIT,
    
    CONSTRAINT UQ_DimDate_date UNIQUE (date_value)
);
GO

CREATE NONCLUSTERED INDEX IX_DimDate_ww
ON dbo.DimDate(work_week);
GO

CREATE NONCLUSTERED INDEX IX_DimDate_fy
ON dbo.DimDate(fiscal_year, fiscal_quarter, fiscal_month);
GO

-- DimEntity
IF OBJECT_ID('dbo.DimEntity', 'U') IS NOT NULL
    DROP TABLE dbo.DimEntity;
GO

CREATE TABLE dbo.DimEntity (
    entity_key INT IDENTITY(1,1) PRIMARY KEY,
    ENTITY NVARCHAR(100) NOT NULL,
    FACILITY NVARCHAR(50),
    CEID NVARCHAR(50),
    TOOLSET NVARCHAR(50),
    AltairFlag NVARCHAR(20),
    is_active BIT DEFAULT 1,
    created_date DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT UQ_DimEntity UNIQUE (ENTITY, FACILITY)
);
GO

-- =====================================================
-- Stored Procedures
-- =====================================================

-- Procedure to refresh pm_flex_enriched from copper
CREATE OR ALTER PROCEDURE dbo.sp_refresh_pm_flex_enriched
    @start_date DATE = NULL,
    @end_date DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Default to last 7 days if no dates provided
    IF @start_date IS NULL
        SET @start_date = DATEADD(DAY, -7, GETDATE());
    IF @end_date IS NULL
        SET @end_date = GETDATE();
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Insert/update enriched records
        -- (Full transformation logic will be in Python ETL)
        -- This is a simplified version for manual refresh
        
        INSERT INTO dbo.pm_flex_enriched (
            source_pm_flex_raw_id,
            ENTITY,
            FACILITY,
            CEID,
            YEARWW,
            TXN_DATE,
            DOWNTIME_TYPE,
            ww_year,
            ww_number,
            scheduled_flag
        )
        SELECT 
            r.pm_flex_raw_id,
            r.ENTITY,
            r.FACILITY,
            r.CEID,
            r.YEARWW,
            r.TXN_DATE,
            r.DOWNTIME_TYPE,
            LEFT(r.YEARWW, 4) AS ww_year,
            CAST(RIGHT(r.YEARWW, 2) AS INT) AS ww_number,
            CASE 
                WHEN r.DOWNTIME_TYPE = 'Scheduled' THEN 1
                ELSE 0
            END AS scheduled_flag
        FROM dbo.pm_flex_raw r
        WHERE r.TXN_DATE >= @start_date
          AND r.TXN_DATE < @end_date
          AND NOT EXISTS (
              SELECT 1 
              FROM dbo.pm_flex_enriched e 
              WHERE e.source_pm_flex_raw_id = r.pm_flex_raw_id
          );
        
        COMMIT TRANSACTION;
        
        -- Log success
        INSERT INTO dbo.pm_flex_load_log (
            source_file,
            rows_loaded,
            load_status,
            layer
        )
        VALUES (
            'REFRESH_ENRICHED',
            @@ROWCOUNT,
            'SUCCESS',
            'SILVER'
        );
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        -- Log failure
        INSERT INTO dbo.pm_flex_load_log (
            source_file,
            load_status,
            error_message,
            layer
        )
        VALUES (
            'REFRESH_ENRICHED',
            'FAILED',
            ERROR_MESSAGE(),
            'SILVER'
        );
        
        THROW;
    END CATCH
END;
GO

PRINT 'Silver layer schema created successfully';
GO
