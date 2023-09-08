import logging

logger = logging.getLogger()

# handlers
console_handler = logging.StreamHandler()
logfile_handler = logging.FileHandler('multipleLogs.txt')

# add handlers
logger.addHandler(logfile_handler)

# set the log levels
logger.setLevel(logging.DEBUG)
console_handler.setLevel(logging.INFO)

logger.addHandler(console_handler)
logger.warning('this will output to console and file')
logger.error('this is an error')
logger.debug('debug this') # only goes to file since console has a higher level than debug

