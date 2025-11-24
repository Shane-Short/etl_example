-- =====================================================
-- PM Flex Pipeline - Copper Layer (Bronze/Raw Data)
-- =====================================================
-- This script creates the raw data tables for PM_Flex ingestion
-- Database: MAData_Output_Production
-- Schema: dbo
-- =====================================================

USE [MAData_Output_Production];
GO

-- Drop table if exists (for redeployment)
IF OBJECT_ID('dbo.pm_flex_raw', 'U') IS NOT NULL
    DROP TABLE dbo.pm_flex_raw;
GO

-- =====================================================
-- Table: pm_flex_raw
-- Description: Raw PM_Flex data with all 88 columns
-- Retention: 26 weeks (matches source file)
-- =====================================================

CREATE TABLE dbo.pm_flex_raw (
    -- Primary key and metadata
    pm_flex_raw_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    load_timestamp DATETIME2 NOT NULL DEFAULT GETDATE(),
    source_file NVARCHAR(500),
    source_ww NVARCHAR(20),
    
    -- Entity and facility information
    ENTITY NVARCHAR(100),
    FACILITY NVARCHAR(50),
    UNIQUE_ENTITY_ID NVARCHAR(150),
    SUPPLIER NVARCHAR(50),
    FUNCTIONAL_AREA NVARCHAR(100),
    TOOLSET NVARCHAR(50),
    CEID NVARCHAR(50),
    VFMFGID NVARCHAR(100),
    CUSTOM_MODULE_GROUP NVARCHAR(200),
    Dominant_Tech_Node FLOAT,
    
    -- PM information
    PM_NAME NVARCHAR(200),
    ATTRIBUTE_NAME NVARCHAR(200),
    YEARWW NVARCHAR(20),
    TXN_DATE DATETIME2,
    PREV_ATTRIBUTE_VALUE FLOAT,
    ATTRIBUTE_VALUE FLOAT,
    NEXT_ATTRIBUTE_VALUE FLOAT,
    CUSTOM_DELTA FLOAT,
    Lower_IQR_Limit_Delta FLOAT,
    Median_Delta FLOAT,
    Delta_75th_Percentile FLOAT,
    COUNTER_UPPER_VALUE FLOAT,
    UPPER_LIMIT_FACILITY NVARCHAR(50),
    upper_limit_perc_target FLOAT,
    Met_Upper_Limit FLOAT,
    PM_Label NVARCHAR(100),
    GVB_PMCycle_Label FLOAT,
    PMCycle_Counter_by_UEI_AttrName FLOAT,
    
    -- Checklist information
    CHECKLIST_NAME NVARCHAR(200),
    CKL_START_TIME DATETIME2,
    CKL_END_TIME DATETIME2,
    CKL_DURATION_IN_HOURS FLOAT,
    NUM_STEPS_IN_CL FLOAT,
    MIN_OF_CKL_START_END_DISTANCE_TO_TXN_DATE FLOAT,
    CL_NAME_SIMILARITY_SCORE FLOAT,
    MOST_FREQUENT_CHECKLIST_FACILITY_CMG_TECHNODE NVARCHAR(200),
    DURATION_IN_HOURS_75TH_PERCENTILE FLOAT,
    pm_cycle_utilization FLOAT,
    reliable_upper_limit_insight FLOAT,
    
    -- Parent entity relationships
    TECHNODE_CEID_VFMFGID NVARCHAR(200),
    PARENT_ENTITY NVARCHAR(100),
    UNIQUE_PARENT_FAB NVARCHAR(150),
    SUB_ENTITY_ASSOCIATED_TO_ATTR NVARCHAR(100),
    ATTRIBUTE_NAME_ASSOCIATED_ENTITY NVARCHAR(200),
    num_of_resets_on_parent_txndate FLOAT,
    multiple_or_single_pm NVARCHAR(100),
    Sympathy_PM FLOAT,
    most_common_pm_grouping NVARCHAR(200),
    MOST_COMMON_PM_TYPE NVARCHAR(200),
    PM_FREQUENCY NVARCHAR(50),
    
    -- Downtime window information
    WINDOW_ID NVARCHAR(200),
    DOWN_WINDOW_START_TXN_DATE DATETIME2,
    DOWN_WINDOW_END_TXN_DATE DATETIME2,
    DOWN_WINDOW_DURATION_HR FLOAT,
    DOWN_WINDOW_DETAILS NVARCHAR(MAX),
    DOWN_WINDOW_COMMENTS NVARCHAR(MAX),
    ALL_STATES_IN_WINDOW NVARCHAR(500),
    WINDOW_TYPE NVARCHAR(50),
    DOWNTIME_TYPE NVARCHAR(50),
    DOWNTIME_CLASS NVARCHAR(100),
    DOWNTIME_SUBCLASS NVARCHAR(100),
    OLD_ENTITY_STATE NVARCHAR(100),
    NEW_ENTITY_STATE NVARCHAR(100),
    Reclean_Label FLOAT,
    Down_Window_Reclean_Rate FLOAT,
    DOWN_WINDOW_DURATION_OUTLIER_THRESHOLD_FOR_TOOLSET FLOAT,
    DOWN_WINDOW_DURATION_OUTLIER_LABEL_FOR_PMCYCLE FLOAT,
    
    -- Work order information
    WORKORDERID NVARCHAR(500),
    WO_TOOLNAME NVARCHAR(200),
    WO_DESCRIPTION NVARCHAR(MAX),
    PM_Reason_Deepdive NVARCHAR(200),
    DOWNTIME_SUBCLASS_DETAILS NVARCHAR(MAX),
    
    -- ROI and cost information
    UPPER_LIMIT_THRESHOLD FLOAT,
    UPPER_VALUE_THRESHOLD FLOAT,
    VALUE_LOSS_AT_PMRESET FLOAT,
    PM_REDUCTION_ROI FLOAT,
    NORMALIZING_FACTOR FLOAT,
    PM_REDUCTION_ROI_NORMALIZED FLOAT,
    G2G_PER_PM FLOAT,
    EQUIPMENT_DOWNTIME_ROI_Hrs FLOAT,  -- Note: Removed parentheses from column name
    PART_COST_PER_PM FLOAT,
    PART_COST_SAVING_ROI FLOAT,  -- Note: Removed $ from column name
    PM_DURATION FLOAT,
    MTS_NEEDED FLOAT,
    LABORHOUR_PER_PM FLOAT,
    LABORHOUR_PER_PM_2 FLOAT,  -- Note: Changed .1 to _2
    LABOR_HOUR_ROI_Hrs FLOAT,  -- Note: Removed parentheses from column name
    HEADCOUNT_ROI FLOAT  -- Note: Removed (#) from column name
);
GO

-- =====================================================
-- Indexes for performance
-- =====================================================

-- Index on ENTITY for joins with Altair classification
CREATE NONCLUSTERED INDEX IX_pm_flex_raw_entity
ON dbo.pm_flex_raw(ENTITY)
INCLUDE (FACILITY, CEID, YEARWW);
GO

-- Index on date for time-based queries
CREATE NONCLUSTERED INDEX IX_pm_flex_raw_txn_date
ON dbo.pm_flex_raw(TXN_DATE)
INCLUDE (ENTITY, YEARWW, DOWNTIME_TYPE);
GO

-- Index on work week for filtering
CREATE NONCLUSTERED INDEX IX_pm_flex_raw_yearww
ON dbo.pm_flex_raw(YEARWW)
INCLUDE (ENTITY, CEID, FACILITY);
GO

-- Index on load timestamp for data management
CREATE NONCLUSTERED INDEX IX_pm_flex_raw_load_timestamp
ON dbo.pm_flex_raw(load_timestamp DESC);
GO

-- Composite index for common filter combinations
CREATE NONCLUSTERED INDEX IX_pm_flex_raw_facility_ceid
ON dbo.pm_flex_raw(FACILITY, CEID, YEARWW);
GO

-- =====================================================
-- Table: pm_flex_load_log
-- Description: Track each ETL load execution
-- =====================================================

IF OBJECT_ID('dbo.pm_flex_load_log', 'U') IS NOT NULL
    DROP TABLE dbo.pm_flex_load_log;
GO

CREATE TABLE dbo.pm_flex_load_log (
    load_log_id INT IDENTITY(1,1) PRIMARY KEY,
    load_timestamp DATETIME2 NOT NULL DEFAULT GETDATE(),
    source_file NVARCHAR(500),
    source_ww NVARCHAR(20),
    rows_loaded INT,
    load_status NVARCHAR(50), -- SUCCESS, FAILED, PARTIAL
    error_message NVARCHAR(MAX),
    execution_time_seconds FLOAT,
    layer VARCHAR(20) -- COPPER, SILVER, GOLD
);
GO

CREATE NONCLUSTERED INDEX IX_pm_flex_load_log_timestamp
ON dbo.pm_flex_load_log(load_timestamp DESC);
GO

-- =====================================================
-- Cleanup procedure for data retention
-- =====================================================

CREATE OR ALTER PROCEDURE dbo.sp_cleanup_pm_flex_copper
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @retention_weeks INT = 26;
    DECLARE @cutoff_date DATETIME2;
    DECLARE @rows_deleted INT;
    
    -- Calculate cutoff date (26 weeks ago)
    SET @cutoff_date = DATEADD(WEEK, -@retention_weeks, GETDATE());
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Delete old records
        DELETE FROM dbo.pm_flex_raw
        WHERE load_timestamp < @cutoff_date;
        
        SET @rows_deleted = @@ROWCOUNT;
        
        -- Log the cleanup
        INSERT INTO dbo.pm_flex_load_log (
            source_file,
            rows_loaded,
            load_status,
            error_message,
            execution_time_seconds,
            layer
        )
        VALUES (
            'CLEANUP',
            @rows_deleted,
            'SUCCESS',
            CONCAT('Cleaned up records older than ', @cutoff_date),
            0,
            'COPPER'
        );
        
        COMMIT TRANSACTION;
        
        PRINT CONCAT('Successfully deleted ', @rows_deleted, ' old records');
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        DECLARE @error_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        
        -- Log the failure
        INSERT INTO dbo.pm_flex_load_log (
            source_file,
            rows_loaded,
            load_status,
            error_message,
            layer
        )
        VALUES (
            'CLEANUP',
            0,
            'FAILED',
            @error_msg,
            'COPPER'
        );
        
        THROW;
    END CATCH
END;
GO

-- =====================================================
-- Grant permissions (adjust as needed)
-- =====================================================

-- Grant permissions to your service account
-- GRANT SELECT, INSERT, UPDATE, DELETE ON dbo.pm_flex_raw TO [YOUR_SERVICE_ACCOUNT];
-- GRANT EXECUTE ON dbo.sp_cleanup_pm_flex_copper TO [YOUR_SERVICE_ACCOUNT];

PRINT 'Copper layer schema created successfully';
GO
