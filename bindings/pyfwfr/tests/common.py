#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the license.
#import ebcdic
import io
import itertools
import string
import numpy as np
import pyarrow as pa

from pyfwfr import read_fwf, ParseOptions, ReadOptions

def generate_col_names():
    letters = string.ascii_lowercase
    for first in letters:
        for second in letters:
            yield first + second

def make_random_fwf(num_cols=2, num_rows=10, linesep=u'\r\n', encoding='utf8'):
    arr = np.random.randint(1000, 9999, size=(num_cols, num_rows))
    col_names = list(itertools.islice(generate_col_names(), num_cols))
    fwf = io.StringIO()
    for col in range(num_cols):
        fwf.write(u''.join(col_names[col] + '  '))
    fwf.write(linesep)
    for row in arr.T:
        for col in range(num_cols):
            fwf.write(u''.join(str(row[col])))
        fwf.write(linesep)
    fwf = fwf.getvalue().encode(encoding)
    columns = [pa.array(a, type=pa.int64()) for a in arr]
    expected = pa.Table.from_arrays(columns, col_names)
    return fwf, expected

def read_bytes(b, parse_options, **kwargs):
    return read_fwf(pa.py_buffer(b), parse_options, **kwargs)
