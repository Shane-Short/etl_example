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
        "SQL_SERVER": os.getenv("SQL_SERVER"),
        "SQL_DATABASE": os.getenv("SQL_DATABASE"),
        "SQL_USERNAME": os.getenv("SQL_USERNAME"),
        "SQL_PASSWORD": os.getenv("SQL_PASSWORD"),
        "SQL_DRIVER": os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server"),
        "sql_server": os.getenv("SQL_SERVER"),
        "sql_database": os.getenv("SQL_DATABASE"),
        "sql_username": os.getenv("SQL_USERNAME"),
        "sql_password": os.getenv("SQL_PASSWORD"),
        "sql_driver": os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server"),
        "network_share_path": os.getenv("NETWORK_SHARE_PATH"),
        "NETWORK_SHARE_PATH": os.getenv("NETWORK_SHARE_PATH"),
        "NETWORK_SHARE_ROOT": os.getenv("NETWORK_SHARE_ROOT", os.getenv("NETWORK_SHARE_PATH")),
        "environment": os.getenv("ENVIRONMENT", "production"),
        "log_level": os.getenv("LOG_LEVEL", "INFO"),
        "LOG_LEVEL": os.getenv("LOG_LEVEL", "INFO"),
        "log_file": os.getenv("LOG_FILE", "logs/pm_flex_pipeline.log"),
        "LOG_FILE": os.getenv("LOG_FILE", "logs/pm_flex_pipeline.log"),
        "copper_retention_weeks": int(os.getenv("COPPER_RETENTION_WEEKS", "26")),
        "silver_retention_weeks": int(os.getenv("SILVER_RETENTION_WEEKS", "104")),
        "gold_retention_weeks": int(os.getenv("GOLD_RETENTION_WEEKS", "156")),
    }
    
    # ========================================
    # NORMALIZE NETWORK PATHS FOR WINDOWS
    # ========================================
    # Convert forward slashes to backslashes
    # //server/share → \\server\share
    
    print("DEBUG: Before normalization:")
    print(f"  network_share_path: {config.get('network_share_path')}")
    
    # Normalize all network path keys
    for key in ['network_share_path', 'NETWORK_SHARE_PATH', 'NETWORK_SHARE_ROOT']:
        if key in config and config[key]:
            original_path = config[key]
            
            # Convert // to \\ and / to \
            if original_path.startswith('//'):
                normalized_path = '\\\\' + original_path[2:].replace('/', '\\')
                config[key] = normalized_path
                print(f"DEBUG: Normalized {key}: {original_path} → {normalized_path}")
            elif '/' in original_path:
                normalized_path = original_path.replace('/', '\\')
                config[key] = normalized_path
                print(f"DEBUG: Normalized {key}: {original_path} → {normalized_path}")
    
    print("DEBUG: After normalization:")
    print(f"  network_share_path: {config.get('network_share_path')}")
    
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
