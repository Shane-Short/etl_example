CREATE TABLE dbo.pm_flex_enriched (
    -- Auto-generated ID
    pm_flex_enriched_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Foreign key to Bronze layer
    source_pm_flex_raw_id BIGINT,
    
    -- =====================================================
    -- ALL 88 ORIGINAL COLUMNS FROM PM_FLEX_RAW
    -- =====================================================
    ENTITY VARCHAR(100),
    FACILITY VARCHAR(50),
    UNIQUE_ENTITY_ID VARCHAR(100),
    SUPPLIER VARCHAR(100),
    FUNCTIONAL_AREA VARCHAR(100),
    TOOLSET VARCHAR(100),
    CEID VARCHAR(100),
    VFMFGID VARCHAR(100),
    CUSTOM_MODULE_GROUP VARCHAR(100),
    Dominant_Tech_Node VARCHAR(50),
    PM_NAME VARCHAR(200),
    ATTRIBUTE_NAME VARCHAR(200),
    YEARWW VARCHAR(20),
    TXN_DATE DATETIME,
    PREV_ATTRIBUTE_VALUE FLOAT,
    ATTRIBUTE_VALUE FLOAT,
    NEXT_ATTRIBUTE_VALUE FLOAT,
    CUSTOM_DELTA FLOAT,
    Lower_IQR_Limit_Delta FLOAT,
    Median_Delta FLOAT,
    Delta_75th_Percentile FLOAT,
    COUNTER_UPPER_VALUE FLOAT,
    UPPER_LIMIT_FACILITY FLOAT,
    upper_limit_perc_target FLOAT,
    Met_Upper_Limit VARCHAR(10),
    PM_Label VARCHAR(100),
    GVB_PMCycle_Label VARCHAR(100),
    PMCycle_Counter_by_UEI_AttrName INT,
    CHECKLIST_NAME VARCHAR(200),
    CKL_START_TIME DATETIME,
    CKL_END_TIME DATETIME,
    CKL_DURATION_IN_HOURS FLOAT,
    NUM_STEPS_IN_CL INT,
    MIN_OF_CKL_START_END_DISTANCE_TO_TXN_DATE FLOAT,
    CL_NAME_SIMILARITY_SCORE FLOAT,
    MOST_FREQUENT_CHECKLIST_FACILITY_CMG_TECHNODE VARCHAR(200),
    DURATION_IN_HOURS_75TH_PERCENTILE FLOAT,
    pm_cycle_utilization FLOAT,
    reliable_upper_limit_insight VARCHAR(50),
    TECHNODE_CEID_VFMFGID VARCHAR(200),
    PARENT_ENTITY VARCHAR(100),
    UNIQUE_PARENT_FAB VARCHAR(100),
    SUB_ENTITY_ASSOCIATED_TO_ATTR VARCHAR(100),
    ATTRIBUTE_NAME_ASSOCIATED_ENTITY VARCHAR(200),
    num_of_resets_on_parent_txndate INT,
    multiple_or_single_pm VARCHAR(50),
    Sympathy_PM VARCHAR(10),
    most_common_pm_grouping VARCHAR(100),
    MOST_COMMON_PM_TYPE VARCHAR(100),
    PM_FREQUENCY VARCHAR(50),
    WINDOW_ID VARCHAR(100),
    DOWN_WINDOW_START_TXN_DATE DATETIME,
    DOWN_WINDOW_END_TXN_DATE DATETIME,
    DOWN_WINDOW_DURATION_HR FLOAT,
    DOWN_WINDOW_DETAILS VARCHAR(MAX),
    DOWN_WINDOW_COMMENTS VARCHAR(MAX),
    ALL_STATES_IN_WINDOW VARCHAR(MAX),
    WINDOW_TYPE VARCHAR(100),
    DOWNTIME_TYPE VARCHAR(100),
    DOWNTIME_CLASS VARCHAR(100),
    DOWNTIME_SUBCLASS VARCHAR(200),
    OLD_ENTITY_STATE VARCHAR(100),
    NEW_ENTITY_STATE VARCHAR(100),
    Reclean_Label VARCHAR(50),
    Down_Window_Reclean_Rate FLOAT,
    DOWN_WINDOW_DURATION_OUTLIER_THRESHOLD_FOR_TOOLSET FLOAT,
    DOWN_WINDOW_DURATION_OUTLIER_LABEL_FOR_PMCYCLE VARCHAR(50),
    WORKORDERID VARCHAR(100),
    WO_TOOLNAME VARCHAR(100),
    WO_DESCRIPTION VARCHAR(MAX),
    PM_Reason_Deepdive VARCHAR(MAX),
    DOWNTIME_SUBCLASS_DETAILS VARCHAR(MAX),
    UPPER_LIMIT_THRESHOLD FLOAT,
    UPPER_VALUE_THRESHOLD FLOAT,
    VALUE_LOSS_AT_PMRESET FLOAT,
    PM_REDUCTION_ROI FLOAT,
    NORMALIZING_FACTOR FLOAT,
    PM_REDUCTION_ROI_NORMALIZED FLOAT,
    G2G_PER_PM FLOAT,
    EQUIPMENT_DOWNTIME_ROI_Hrs FLOAT,
    PART_COST_PER_PM FLOAT,
    PART_COST_SAVING_ROI FLOAT,
    PM_DURATION FLOAT,
    MTS_NEEDED FLOAT,
    LABORHOUR_PER_PM FLOAT,
    LABOR_HOUR_ROI_Hrs FLOAT,
    HEADCOUNT_ROI FLOAT,
    
    -- =====================================================
    -- CALCULATED COLUMNS ADDED BY SILVER LAYER
    -- =====================================================
    
    -- PM Timing Classification
    pm_life_vs_target FLOAT,
    pm_timing_classification VARCHAR(20),
    
    -- PM Life Statistics
    avg_pm_life FLOAT,
    median_pm_life FLOAT,
    pm_life_variance FLOAT,
    pm_life_std_dev FLOAT,
    
    -- Downtime Metrics
    total_downtime_hours FLOAT,
    avg_downtime_hours FLOAT,
    median_downtime_hours FLOAT,
    downtime_variance FLOAT,
    scheduled_downtime_hours FLOAT,
    unscheduled_downtime_hours FLOAT,
    
    -- PM Type Rates
    unscheduled_pm_rate FLOAT,
    scheduled_pm_rate FLOAT,
    reclean_rate FLOAT,
    sympathy_pm_rate FLOAT,
    
    -- Chronic Tool Flags
    chronic_flag BIT,
    chronic_score FLOAT,
    chronic_severity VARCHAR(20),
    
    -- Work Week Metrics
    ww_year INT,
    ww_number INT,
    
    -- Data Quality
    data_quality_score FLOAT,
    
    -- Altair Flag (from Bronze)
    AltairFlag VARCHAR(20),
    
    -- =====================================================
    -- METADATA
    -- =====================================================
    enrichment_timestamp DATETIME DEFAULT GETDATE(),
    source_file VARCHAR(500),
    source_ww VARCHAR(20),
    load_timestamp DATETIME
);








CREATE TABLE dbo.pm_flex_chronic_tools (
    -- Auto-generated ID
    chronic_tool_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Tool identification
    ENTITY VARCHAR(100),
    FACILITY VARCHAR(50),
    CEID VARCHAR(100),
    YEARWW VARCHAR(20),
    
    -- PM Event Counts
    total_pm_events INT,
    scheduled_pm_count INT,
    unscheduled_pm_count INT,
    unscheduled_pm_rate FLOAT,
    
    -- PM Life Statistics
    avg_pm_life FLOAT,
    median_pm_life FLOAT,
    pm_life_variance FLOAT,
    pm_life_std_dev FLOAT,
    
    -- Downtime Metrics
    total_downtime_hours FLOAT,
    avg_downtime_hours FLOAT,
    median_downtime_hours FLOAT,
    scheduled_downtime_hours FLOAT,
    unscheduled_downtime_hours FLOAT,
    
    -- Reclean Metrics
    reclean_count INT,
    reclean_rate FLOAT,
    
    -- Sympathy PM Metrics
    sympathy_pm_count INT,
    sympathy_pm_rate FLOAT,
    
    -- Chamber/Tool Metrics
    total_chambers INT,
    chronic_chambers INT,
    
    -- Chronic Scoring
    chronic_score FLOAT,
    chronic_severity VARCHAR(20),
    chronic_flag BIT,
    
    -- Altair Classification
    AltairFlag VARCHAR(20),
    
    -- Metadata
    calculation_timestamp DATETIME DEFAULT GETDATE(),
    
    -- Unique constraint
    CONSTRAINT UQ_chronic_tools UNIQUE (ENTITY, FACILITY, CEID, YEARWW)
);





CREATE TABLE dbo.pm_flex_downtime_summary (
    -- Auto-generated ID
    downtime_summary_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Grouping keys
    ENTITY VARCHAR(100),
    FACILITY VARCHAR(50),
    CEID VARCHAR(100),
    YEARWW VARCHAR(20),
    DOWNTIME_TYPE VARCHAR(100),
    DOWNTIME_CLASS VARCHAR(100),
    
    -- Downtime Aggregations
    total_downtime_hours FLOAT,
    avg_downtime_hours FLOAT,
    median_downtime_hours FLOAT,
    min_downtime_hours FLOAT,
    max_downtime_hours FLOAT,
    downtime_variance FLOAT,
    downtime_std_dev FLOAT,
    downtime_count INT,
    
    -- Scheduled vs Unscheduled
    scheduled_downtime_hours FLOAT,
    unscheduled_downtime_hours FLOAT,
    
    -- Reclean Analysis
    reclean_events INT,
    reclean_rate FLOAT,
    
    -- Metadata
    calculation_timestamp DATETIME DEFAULT GETDATE(),
    
    -- Unique constraint
    CONSTRAINT UQ_downtime_summary UNIQUE (ENTITY, FACILITY, CEID, YEARWW, DOWNTIME_TYPE, DOWNTIME_CLASS)
);





CREATE TABLE dbo.DimDate (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    week_of_year INT NOT NULL,
    day_of_year INT NOT NULL,
    day_of_month INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    is_weekend BIT NOT NULL,
    is_holiday BIT DEFAULT 0,
    fiscal_year INT,
    fiscal_quarter INT,
    fiscal_week VARCHAR(20),
    work_week VARCHAR(20),
    ww_year INT,
    ww_number INT
);






CREATE TABLE dbo.DimEntity (
    entity_key INT IDENTITY(1,1) PRIMARY KEY,
    ENTITY VARCHAR(100) NOT NULL UNIQUE,
    FACILITY VARCHAR(50),
    CEID VARCHAR(100),
    TOOLSET VARCHAR(100),
    FUNCTIONAL_AREA VARCHAR(100),
    Dominant_Tech_Node VARCHAR(50),
    AltairFlag VARCHAR(20),
    
    -- Metadata
    first_seen_date DATE,
    last_seen_date DATE,
    is_active BIT DEFAULT 1
);
