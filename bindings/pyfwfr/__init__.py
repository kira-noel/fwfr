import pyarrow  # to help fill symbol table for CPP extension

from pyfwfr.fwfr import *
from os import path

# -----------------------------------

def get_library_dir():
    """
    Return absolute path to pyfwfr package.
    """
    return path.dirname(__file__)
