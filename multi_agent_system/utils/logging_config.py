"""
Logging configuration for the Multi-Agent System.
"""

import logging
import sys
from typing import Optional


def setup_logging(level: str = "INFO", 
                 format_string: Optional[str] = None,
                 include_timestamp: bool = True) -> logging.Logger:
    """
    Set up structured logging for the Multi-Agent System.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_string: Custom format string (optional)
        include_timestamp: Whether to include timestamps in log messages
        
    Returns:
        Configured logger instance
    """
    # Convert string level to logging constant
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    # Default format string
    if format_string is None:
        if include_timestamp:
            format_string = (
                "%(asctime)s - %(name)s - %(levelname)s - "
                "%(funcName)s:%(lineno)d - %(message)s"
            )
        else:
            format_string = (
                "%(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
            )
    
    # Configure root logger
    logging.basicConfig(
        level=numeric_level,
        format=format_string,
        stream=sys.stdout,
        force=True  # Override any existing configuration
    )
    
    # Create and return logger for this package
    logger = logging.getLogger('multi_agent_system')
    logger.setLevel(numeric_level)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name under the multi_agent_system namespace.
    
    Args:
        name: Logger name (will be prefixed with 'multi_agent_system')
        
    Returns:
        Logger instance
    """
    return logging.getLogger(f'multi_agent_system.{name}')