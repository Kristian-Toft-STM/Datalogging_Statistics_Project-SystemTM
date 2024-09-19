import os
import logging

class InfoFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == logging.INFO

class ErrorFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == logging.ERROR

def setup_logging():
    # Ensure the log directories exist
    os.makedirs('C:\\Users\\Admin-STM\\logs\\error', exist_ok=True)
    os.makedirs('C:\\Users\\Admin-STM\\logs\\data-logs', exist_ok=True)

    # Create a logger object
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Set the base logging level for the logger

    # Create handler for error logging
    error_handler = logging.FileHandler('C:\\Users\\Admin-STM\\logs\\error\\error.log')
    error_handler.setLevel(logging.ERROR)  # Only log errors
    error_handler.addFilter(ErrorFilter())

    # Create handler for info logging
    info_handler = logging.FileHandler("C:\\Users\\Admin-STM\\logs\\data-logs\\data-log.log")
    info_handler.setLevel(logging.INFO)  # Log everything from INFO level and above
    info_handler.addFilter(InfoFilter())

    # Create a common formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Attach formatter to handlers
    error_handler.setFormatter(formatter)
    info_handler.setFormatter(formatter)

    # Attach handlers to the logger
    logger.addHandler(error_handler)
    logger.addHandler(info_handler)

    return logger

# Call the setup function only once
logger = setup_logging()