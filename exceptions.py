"""
Custom exceptions for PM Flex ETL pipeline.

These exceptions provide specific error handling for different
failure scenarios in the pipeline.
"""


class PMFlexError(Exception):
    """Base exception for PM Flex pipeline errors."""
    pass


class FileNotFoundError(PMFlexError):
    """Raised when expected PM_Flex file is not found."""
    pass


class SchemaValidationError(PMFlexError):
    """Raised when PM_Flex file schema doesn't match expected format."""
    pass


class DatabaseConnectionError(PMFlexError):
    """Raised when database connection fails."""
    pass


class DataQualityError(PMFlexError):
    """Raised when data quality checks fail."""
    pass


class ConfigurationError(PMFlexError):
    """Raised when configuration is invalid or missing."""
    pass


class TransformationError(PMFlexError):
    """Raised when data transformation fails."""
    pass
