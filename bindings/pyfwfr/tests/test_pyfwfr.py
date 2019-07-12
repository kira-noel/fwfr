#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the license.
#
# To ensure the Cython library works

import pyarrow as pa
import pyfwfr as pf
import os

def run_pyfwfr_test():
    print('Running pyfwfr tests...')

    print('test 1 ... ', end='')
    try:
        parse_options = pf.ParseOptions([6, 6, 6, 4])
    except:
        raise Exception('Unable to set ParseOptions.')
    print('ok')

    print('test 2 ... ', end='')
    try:
        table = pf.read_fwf(os.path.join(os.path.dirname(__file__), 'testdata/data.small.fwf'), parse_options)
    except:
        raise Exception('Table read failed.')
    print('ok')

    print('test 3 ... ', end='')
    assert(table.num_columns == 4)
    print('ok')

    print('test 4 ... ', end='')
    assert(table.num_rows == 100)
    print('ok')

    print('test 5 ... ', end='')
    assert(table.column(0).type == 'int64')
    print('ok')

    print('All tests pass.')
