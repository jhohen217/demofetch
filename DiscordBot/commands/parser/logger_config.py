"""
Logger configuration for the parser module.
"""

import logging
import os

def configure_loggers():
    """
    Configure loggers for the parser module.
    
    Sets up:
    - Main discord_bot logger (console and file output)
    - Debug logger (file output only)
    """
    # Get the main discord_bot logger
    main_logger = logging.getLogger('discord_bot')
    
    # Create a debug logger that only logs to file, not console
    debug_logger = logging.getLogger('debug_discord_bot')
    debug_logger.setLevel(logging.DEBUG)
    
    # Create logs directory if it doesn't exist
    logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    # Create a file handler for debug logs
    debug_log_path = os.path.join(logs_dir, 'debug.log')
    debug_handler = logging.FileHandler(debug_log_path, encoding='utf-8')
    debug_handler.setLevel(logging.DEBUG)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    debug_handler.setFormatter(formatter)
    
    # Add the handler to the debug logger
    debug_logger.addHandler(debug_handler)
    
    # Ensure debug logger doesn't propagate to parent (main logger)
    debug_logger.propagate = False
    
    return main_logger, debug_logger
