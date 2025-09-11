import logging

logging.getLogger(__name__).addHandler(logging.NullHandler())

__author__ = "Brendan Spinks"
__email__ = "brendan@massdynamics.com"
__version__ = "0.1.0"

# Export the R error extraction function
from .process import extract_clean_r_error

__all__ = ["extract_clean_r_error"]