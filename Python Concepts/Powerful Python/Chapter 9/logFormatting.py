import logging 

logger = logging.getLogger()
my_handler = logging.StreamHandler() 

fmt = logging.Formatter("This message goes to: %(message)s")
my_handler.setFormatter(fmt)
logger.addHandler(my_handler)
logger.warning('You! Wake UP!!!')
