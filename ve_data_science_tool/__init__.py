"""Global objects for the maintenance tool."""

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)

LOGGER = logging.getLogger("ve_data_science")

# # Add a stream handler
# handler = logging.StreamHandler()
# handler.name = "sdv_stream_log"
# LOGGER.addHandler(handler)
