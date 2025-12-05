if full_refresh:
    logger.info("Full refresh requested - truncating Bronze layer tables")
    from connectors.sqlserver_connector import SQLServerConnector
    connector = SQLServerConnector()
    try:
        # Drop foreign key constraints first
        drop_fk_sql = """
        DECLARE @sql NVARCHAR(MAX) = N'';
        SELECT @sql = @sql + 'ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + '.' 
            + QUOTENAME(OBJECT_NAME(parent_object_id)) + ' DROP CONSTRAINT ' + QUOTENAME(name) + '; '
        FROM sys.foreign_keys
        WHERE referenced_object_id = OBJECT_ID('dbo.pm_flex_raw');
        EXEC sp_executesql @sql;
        """
        connector.execute_query(drop_fk_sql)
        logger.info("  Dropped foreign key constraints")
        
        # Now truncate
        connector.truncate_table('pm_flex_raw', 'dbo')
        logger.info("  Truncated pm_flex_raw")
    except Exception as e:
        logger.warning(f"Could not truncate pm_flex_raw: {e}")
    finally:
        connector.close()
