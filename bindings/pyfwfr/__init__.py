from pyfwfr.fwfr import *
from os import path

# -----------------------------------

def get_library_dir():
    """
    Return absolute path to libfwfr.so
    """
    return path.dirname(__file__) + '/../../..'
