import logging 

logger = logging.getLogger('VoltStream')
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)
logger.propagate = False

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

console_handler.setFormatter(formatter)

logger.info("VoltStream logger initializer successfully")