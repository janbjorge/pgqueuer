import logging
import os
from typing import Final

logging.basicConfig()
logger: Final = logging.getLogger("pgqueuer")
logger.setLevel(level=os.environ.get("LOGLEVEL", "INFO").upper())
