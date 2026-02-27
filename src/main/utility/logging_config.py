"""
logging_config.py
=================

Purpose:
This module sets up a basic logging configuration for the ETL project.
It allows consistent logging across all modules for info, debug, warning, and error messages.
"""

import logging

# --------------------------------------------------------------------
# Basic Logging Configuration
# --------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,  # Set default logging level to INFO
    format='%(asctime)s - %(levelname)s - %(message)s'  # Format: timestamp - log level - message
)

# Logger object to be imported and used in other modules
logger = logging.getLogger(__name__)
