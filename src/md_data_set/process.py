import logging


def md_process(func):
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(__name__)
        logger.info("pre")
        value = func(*args, **kwargs)
        logger.info("post")
        return value
    return wrapper

