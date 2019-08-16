#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the license.
from pyfwfr.fwfr import *
from os import path


def get_library_dir():
    """
    Return absolute path to libfwfr.so
    """
    return path.dirname(__file__) + '/../../..'


def get_include():
    """
    Return absolute path to header home dir.
    """
    return path.dirname(__file__) + '/include'
