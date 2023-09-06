import logging

logger = logging.getLogger()
print(logger.name)

logger.info('this is info')
logger.warning('this is a warning')
logger.critical('this message is critical')
logger.error('this was an error')
logger.debug('basic troubleshooting')