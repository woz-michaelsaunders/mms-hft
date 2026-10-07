"""
Logging configuration for Database Monitor
Separates logs into different files for better organization
"""

import logging
import logging.handlers
import os
from pathlib import Path
from datetime import datetime

# Create logs directory if it doesn't exist
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Log file paths
APP_LOG_FILE = LOG_DIR / "app.log"
ACCESS_LOG_FILE = LOG_DIR / "access.log"
ERROR_LOG_FILE = LOG_DIR / "error.log"
KDB_LOG_FILE = LOG_DIR / "kdb.log"

# Log format
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def setup_logging():
    """Configure logging with separate files for different log types"""
    
    # Root logger configuration
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    root_logger.handlers.clear()
    
    # Application logger (general app logs)
    app_logger = logging.getLogger("app")
    app_handler = logging.handlers.RotatingFileHandler(
        APP_LOG_FILE,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    app_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
    app_handler.setLevel(logging.INFO)
    app_logger.addHandler(app_handler)
    app_logger.setLevel(logging.INFO)
    
    # Error logger (errors and exceptions)
    error_logger = logging.getLogger("error")
    error_handler = logging.handlers.RotatingFileHandler(
        ERROR_LOG_FILE,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=10,  # Keep more error logs
        encoding='utf-8'
    )
    error_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
    error_handler.setLevel(logging.ERROR)
    error_logger.addHandler(error_handler)
    error_logger.setLevel(logging.ERROR)
    
    # kdb+ connection logger (database queries and connections)
    kdb_logger = logging.getLogger("kdb")
    kdb_handler = logging.handlers.RotatingFileHandler(
        KDB_LOG_FILE,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    kdb_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
    kdb_handler.setLevel(logging.INFO)
    kdb_logger.addHandler(kdb_handler)
    kdb_logger.setLevel(logging.INFO)
    
    # Access logger (HTTP requests) - will be configured in FastAPI middleware
    access_logger = logging.getLogger("access")
    access_handler = logging.handlers.RotatingFileHandler(
        ACCESS_LOG_FILE,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    access_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
    access_handler.setLevel(logging.INFO)
    access_logger.addHandler(access_handler)
    access_logger.setLevel(logging.INFO)
    
    # Also log to console for development
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
    console_handler.setLevel(logging.WARNING)  # Only warnings and errors to console
    root_logger.addHandler(console_handler)
    
    return {
        "app": app_logger,
        "error": error_logger,
        "kdb": kdb_logger,
        "access": access_logger
    }

# Initialize logging on import
loggers = setup_logging()

