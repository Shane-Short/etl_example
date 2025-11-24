-- =====================================================
-- PM Flex Pipeline - Master Database Setup Script
-- =====================================================
-- This script runs all DDL scripts to create the complete
-- database schema (Copper, Silver, Gold layers)
-- 
-- Database: MAData_Output_Production
-- Server: TEHAUSTELSQL1
-- =====================================================

USE [MAData_Output_Production];
GO

PRINT '========================================';
PRINT 'PM Flex Pipeline - Database Setup';
PRINT 'Started: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '========================================';
PRINT '';

-- =====================================================
-- STEP 1: Create Copper Layer (Bronze/Raw)
-- =====================================================

PRINT 'STEP 1: Creating Copper Layer...';
:r 01_copper_schema.sql
PRINT 'Copper Layer: COMPLETE';
PRINT '';

-- =====================================================
-- STEP 2: Create Silver Layer (Enriched)
-- =====================================================

PRINT 'STEP 2: Creating Silver Layer...';
:r 02_silver_schema.sql
PRINT 'Silver Layer: COMPLETE';
PRINT '';

-- =====================================================
-- STEP 3: Create Gold Layer (KPIs/Facts)
-- =====================================================

PRINT 'STEP 3: Creating Gold Layer...';
:r 03_gold_schema.sql
PRINT 'Gold Layer: COMPLETE';
PRINT '';

-- =====================================================
-- STEP 4: Verify Schema Creation
-- =====================================================

PRINT '========================================';
PRINT 'Schema Verification';
PRINT '========================================';

-- Check Copper tables
IF OBJECT_ID('dbo.pm_flex_raw', 'U') IS NOT NULL
    PRINT '✓ pm_flex_raw';
ELSE
    PRINT '✗ pm_flex_raw - MISSING!';

IF OBJECT_ID('dbo.pm_flex_load_log', 'U') IS NOT NULL
    PRINT '✓ pm_flex_load_log';
ELSE
    PRINT '✗ pm_flex_load_log - MISSING!';

-- Check Silver tables
IF OBJECT_ID('dbo.pm_flex_enriched', 'U') IS NOT NULL
    PRINT '✓ pm_flex_enriched';
ELSE
    PRINT '✗ pm_flex_enriched - MISSING!';

IF OBJECT_ID('dbo.pm_flex_downtime_summary', 'U') IS NOT NULL
    PRINT '✓ pm_flex_downtime_summary';
ELSE
    PRINT '✗ pm_flex_downtime_summary - MISSING!';

IF OBJECT_ID('dbo.pm_flex_chronic_tools', 'U') IS NOT NULL
    PRINT '✓ pm_flex_chronic_tools';
ELSE
    PRINT '✗ pm_flex_chronic_tools - MISSING!';

IF OBJECT_ID('dbo.DimDate', 'U') IS NOT NULL
    PRINT '✓ DimDate';
ELSE
    PRINT '✗ DimDate - MISSING!';

-- Check Gold tables
IF OBJECT_ID('dbo.fact_pm_kpis_by_site_ww', 'U') IS NOT NULL
    PRINT '✓ fact_pm_kpis_by_site_ww';
ELSE
    PRINT '✗ fact_pm_kpis_by_site_ww - MISSING!';

IF OBJECT_ID('dbo.fact_pm_kpis_by_ceid_ww', 'U') IS NOT NULL
    PRINT '✓ fact_pm_kpis_by_ceid_ww';
ELSE
    PRINT '✗ fact_pm_kpis_by_ceid_ww - MISSING!';

PRINT '';
PRINT '========================================';
PRINT 'Database Setup Complete';
PRINT 'Completed: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '========================================';
GO
