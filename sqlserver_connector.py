"""
SQL Server connector for PM Flex ETL pipeline.

Provides connection management, query execution, and data loading capabilities.
"""

import pyodbc
import pandas as pd
from typing import Optional, Dict, Any, List
from contextlib import contextmanager
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from utils.env import get_connection_string, load_environment
from utils.exceptions import DatabaseConnectionError


class SQLServerConnector:
    """
    SQL Server database connector with connection pooling and error handling.
    """
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize SQL Server connector.
        
        Args:
            connection_string: ODBC connection string (defaults to environment config)
        """
        self.logger = logging.getLogger("pm_flex_pipeline.sqlserver")
        
        if connection_string is None:
            connection_string = get_connection_string()
        
        self.connection_string = connection_string
        self.engine = self._create_engine()
        
    def _create_engine(self) -> Engine:
        """
        Create SQLAlchemy engine with connection pooling.
        
        Returns:
            SQLAlchemy engine
        """
        try:
            # Build SQLAlchemy connection string from pyodbc connection string
            conn_str_sqlalchemy = f"mssql+pyodbc:///?odbc_connect={self.connection_string}"
            
            engine = create_engine(
                conn_str_sqlalchemy,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,  # Verify connections before using
                echo=False
            )
            
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.logger.info("SQL Server connection established successfully")
            return engine
            
        except Exception as e:
            self.logger.error(f"Failed to create SQL Server engine: {str(e)}")
            raise DatabaseConnectionError(f"Cannot connect to SQL Server: {str(e)}")
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections.
        
        Usage:
            with connector.get_connection() as conn:
                conn.execute("SELECT * FROM table")
        """
        connection = self.engine.connect()
        try:
            yield connection
        except Exception as e:
            self.logger.error(f"Database operation failed: {str(e)}")
            raise
        finally:
            connection.close()
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> None:
        """
        Execute a SQL query (INSERT, UPDATE, DELETE, DDL).
        
        Args:
            query: SQL query string
            params: Optional parameters for parameterized queries
        """
        try:
            with self.get_connection() as conn:
                if params:
                    conn.execute(text(query), params)
                else:
                    conn.execute(text(query))
                conn.commit()
            
            self.logger.debug(f"Query executed successfully")
            
        except Exception as e:
            self.logger.error(f"Query execution failed: {str(e)}")
            raise DatabaseConnectionError(f"Failed to execute query: {str(e)}")
    
    def fetch_dataframe(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        Execute a SELECT query and return results as DataFrame.
        
        Args:
            query: SQL SELECT query
            params: Optional parameters for parameterized queries
            
        Returns:
            DataFrame with query results
        """
        try:
            if params:
                df = pd.read_sql(text(query), self.engine, params=params)
            else:
                df = pd.read_sql(text(query), self.engine)
            
            self.logger.debug(f"Fetched {len(df)} rows")
            return df
            
        except Exception as e:
            self.logger.error(f"Query failed: {str(e)}")
            raise DatabaseConnectionError(f"Failed to fetch data: {str(e)}")
    
    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = "dbo",
        if_exists: str = "append",
        chunksize: int = 1000
    ) -> int:
        """
        Load a DataFrame into a SQL Server table.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            schema: Schema name (default: dbo)
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            chunksize: Number of rows per batch
            
        Returns:
            Number of rows loaded
        """
        try:
            row_count = len(df)
            
            self.logger.info(
                f"Loading {row_count} rows to {schema}.{table_name} "
                f"(mode: {if_exists})"
            )
            
            df.to_sql(
                name=table_name,
                con=self.engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                chunksize=chunksize,
                method="multi"
            )
            
            self.logger.info(f"Successfully loaded {row_count} rows")
            return row_count
            
        except Exception as e:
            self.logger.error(f"Failed to load data: {str(e)}")
            raise DatabaseConnectionError(f"Data load failed: {str(e)}")
    
    def table_exists(self, table_name: str, schema: str = "dbo") -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            table_name: Table name to check
            schema: Schema name (default: dbo)
            
        Returns:
            True if table exists, False otherwise
        """
        query = """
        SELECT COUNT(*) as count
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = :schema
        AND TABLE_NAME = :table_name
        """
        
        result = self.fetch_dataframe(
            query,
            params={"schema": schema, "table_name": table_name}
        )
        
        return result.iloc[0]["count"] > 0
    
    def get_row_count(self, table_name: str, schema: str = "dbo") -> int:
        """
        Get the number of rows in a table.
        
        Args:
            table_name: Table name
            schema: Schema name (default: dbo)
            
        Returns:
            Row count
        """
        query = f"SELECT COUNT(*) as count FROM {schema}.{table_name}"
        result = self.fetch_dataframe(query)
        return result.iloc[0]["count"]
    
    def truncate_table(self, table_name: str, schema: str = "dbo") -> None:
        """
        Truncate a table (remove all rows).
        
        Args:
            table_name: Table name to truncate
            schema: Schema name (default: dbo)
        """
        query = f"TRUNCATE TABLE {schema}.{table_name}"
        self.execute_query(query)
        self.logger.info(f"Truncated table {schema}.{table_name}")
    
    def execute_script_file(self, script_path: str) -> None:
        """
        Execute a SQL script file.
        
        Args:
            script_path: Path to SQL script file
        """
        with open(script_path, "r") as f:
            script = f.read()
        
        # Split by GO statements and execute each batch
        batches = [batch.strip() for batch in script.split("GO") if batch.strip()]
        
        for i, batch in enumerate(batches):
            try:
                self.execute_query(batch)
                self.logger.debug(f"Executed batch {i+1}/{len(batches)}")
            except Exception as e:
                self.logger.error(f"Failed on batch {i+1}: {str(e)}")
                raise
        
        self.logger.info(f"Script executed successfully: {script_path}")
    
    def close(self):
        """Close the database connection."""
        if self.engine:
            self.engine.dispose()
            self.logger.info("SQL Server connection closed")
