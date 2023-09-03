import logging 
logging.warning('This is a warning being logged')
logging.error('This is a error being logged')
logging.critical('this is a critical message')
logging.info('this is info')

# Not everything got output
logging.basicConfig(level=logging.INFO)
logging.info('This is informative')
logging.error('Uh oh. Something went wrong')