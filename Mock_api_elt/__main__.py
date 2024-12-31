""" Main script entry point """

import logging
from .practice import execute
try:  # pragma: no cover - driver
    setup_logging()
    execute()
    logging.info("bi-mock_api_elt job Completed")
except Exception as err:  # pragma: no cover - driver
    logging.error("Error: %s", err)
    raise err
