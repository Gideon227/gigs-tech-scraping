import logging
from logging.handlers import RotatingFileHandler
import sys
from datetime import datetime

def setup_scraping_logger(name, log_file='scraping_job.log', level=logging.INFO):
    """
    Set up a logger for web scraping jobs with file and console output.
    
    Args:
        name (str): Name of the logger (usually __name__)
        log_file (str): Path to the log file
        level: Logging level (e.g., logging.INFO, logging.DEBUG)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create a custom logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Create handlers
    c_handler = logging.StreamHandler(sys.stdout)  # Console handler
    f_handler = RotatingFileHandler(               # File handler with rotation
        log_file, 
        maxBytes=1024*1024*5,  # 5MB
        backupCount=3
    )
    
    # Create formatters and add them to handlers
    log_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    c_handler.setFormatter(log_format)
    f_handler.setFormatter(log_format)
    
    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    
    return logger

