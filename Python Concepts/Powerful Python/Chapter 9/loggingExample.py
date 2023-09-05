import logging

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

logger.info("Info")
logger.error("error")
logger.info("info")

logging.basicConfig(
    format = "Log level: %(levelname), msg: %(message)s")
logger.warning("Collision imminent")
