"""Logging configuration module."""

import logging
import sys
from config import LOG_LEVEL, LOG_FORMAT

def setup_logger(name: str) -> logging.Logger:
    """Setup and return a logger instance."""
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)
    
    # Console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(LOG_LEVEL)
    
    # Formatter
    formatter = logging.Formatter(LOG_FORMAT)
    handler.setFormatter(formatter)
    
    # Add handler if not already present
    if not logger.handlers:
        logger.addHandler(handler)
    
    return logger

# Create module logger
logger = setup_logger(__name__)
