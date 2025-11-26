"""
Environment variable management for PM Flex ETL pipeline.

Loads and validates environment variables from .env file.
"""

import os
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv
from .exceptions import ConfigurationError


def load_environment() -> Dict[str, Any]:
    """
    Load environment variables from .env file.
    
    Returns:
        Dictionary containing all environment configuration
        
    Raises:
        ConfigurationError: If required environment variables are missing
    """
    # Find .env file (check current dir and parent dirs)
    env_path = Path(".env")
    if not env_path.exists():
        # Try parent directory
        env_path = Path("../.env")
    
    if env_path.exists():
        load_dotenv(env_path)
    else:
        raise ConfigurationError(
            ".env file not found. Please copy .env.template to .env and configure it."
        )
    
    # Required environment variables
    required_vars = [
        "SQL_SERVER",
        "SQL_DATABASE",
        "SQL_USERNAME",
        "SQL_PASSWORD",
        "NETWORK_SHARE_PATH",
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ConfigurationError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )
    
    # Build configuration dictionary
    config = {
        "sql_server": os.getenv("SQL_SERVER"),
        "sql_database": os.getenv("SQL_DATABASE"),
        "sql_username": os.getenv("SQL_USERNAME"),
        "sql_password": os.getenv("SQL_PASSWORD"),
        "sql_driver": os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server"),
        "network_share_path": os.getenv("NETWORK_SHARE_PATH"),
        "environment": os.getenv("ENVIRONMENT", "production"),
        "log_level": os.getenv("LOG_LEVEL", "INFO"),
        "log_file": os.getenv("LOG_FILE", "logs/pm_flex_pipeline.log"),
        "copper_retention_weeks": int(os.getenv("COPPER_RETENTION_WEEKS", "26")),
        "silver_retention_weeks": int(os.getenv("SILVER_RETENTION_WEEKS", "104")),
        "gold_retention_weeks": int(os.getenv("GOLD_RETENTION_WEEKS", "156")),
    }

# Normalize network paths for Windows (convert / to \)
    if config.get('network_share_path'):
        path = config['network_share_path']
        # Convert forward slashes to backslashes for Windows UNC paths
        if path.startswith('//'):
            path = '\\\\' + path[2:].replace('/', '\\')
        config['network_share_path'] = path

    return config


def get_connection_string() -> str:
    """
    Build SQL Server connection string from environment variables.
    
    Returns:
        ODBC connection string
    """
    config = load_environment()
    
    connection_string = (
        f"DRIVER={{{config['sql_driver']}}};"
        f"SERVER={config['sql_server']};"
        f"DATABASE={config['sql_database']};"
        f"UID={config['sql_username']};"
        f"PWD={config['sql_password']};"
        "TrustServerCertificate=yes;"
    )
    
    return connection_string
