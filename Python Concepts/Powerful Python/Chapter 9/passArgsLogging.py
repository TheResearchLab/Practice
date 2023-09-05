import logging

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

param1 = 1
param2 = 'the parameters'

#Can be done in different ways but this is the preferred method
logger.info('this is the message: and this %d messasge is from %s',param1,param2)