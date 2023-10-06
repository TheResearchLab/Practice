import logging
import sys 

logger = logging.getLogger()
#logger.hasHandlers() #False

#log to file
log_file_handler = logging.FileHandler('log.txt')
logger.addHandler(log_file_handler) 
logger.debug("A little detail")
logger.warning('Boo!')

#log to stdout
out_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(out_handler)

logger.warning('Boo')

stderr_handler = logging.StreamHandler()
logger.addHandler(stderr_handler)
logger.warning('Boo!')


