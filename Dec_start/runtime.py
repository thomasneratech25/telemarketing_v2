import os
import time
import logging
from pathlib import Path
from dotenv import load_dotenv

# Load MongoDB API Key 
load_dotenv("/Users/nera_thomas/Desktop/Telemarketing/api/mongodb/.env")
MONGODB_URI = os.getenv("MONGODB_API_KEY")

# Logging configuration
LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("log_files")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(LOG_DIR / "CONVERSION_errors.log", encoding="utf-8")
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

# Safe Call, use for when Error, retry again start with the current error
def safe_call(func, *args, description=None, retries=500, delay=60, **kwargs):
    """
    Call a function safely with retries.

    Parameters:
        func: callable to execute.
        description: Optional string used in logs for readability.
        retries: Number of attempts before giving up.
        delay: Seconds to wait between retries.
    """
    attempt = 1
    label = description or getattr(func, "__name__", "callable")
    while True:
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            raise
        except Exception:
            logger.exception("Error during %s (attempt %s/%s)", label, attempt, retries)
            if attempt >= retries:
                logger.error("Giving up on %s after %s attempts.", label, retries)
                return None
            attempt += 1
            time.sleep(delay)
