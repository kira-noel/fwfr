#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the license.
import pyarrow as pa
import pyfwfr as pf
import os
import unittest
import warnings

from pyfwfr.tests.common import make_random_fwf, read_bytes

# Ignore deprecated numpy warning. Official fix from numpy is to ignore the warning, but 
# unittest warning settings override it and shows the warning.
def ignore_numpy_warning(test_func):
    def do_test(self, *args, **kwargs):
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', message='numpy.ufunc size changed')
            test_func(self, *args, **kwargs)
    return do_test

class TestPyfwfr(unittest.TestCase):   
    def test_read_options(self):
        cls = pf.ReadOptions
        opts = cls()

        assert opts.encoding == ""
        opts.encoding = 'cp1047,swaplfnl'
        assert opts.encoding == 'cp1047,swaplfnl'

        assert opts.buffer_safety_factor > 1
        opts.buffer_safety_factor = 1.4
        assert opts.buffer_safety_factor == 1.4

        assert opts.use_threads is True
        opts.use_threads = False
        assert opts.use_threads is False

        assert opts.block_size > 0
        opts.block_size = 12345
        assert opts.block_size == 12345

        opts = cls(encoding='cp1047,swaplfnl', use_threads=False)
        assert opts.encoding == 'cp1047,swaplfnl'
        assert opts.use_threads is False

    def test_parse_options(self):
        cls = pf.ParseOptions
        with self.assertRaises(Exception):
            opts = cls()
        opts = cls([1, 2])

        assert opts.field_widths == [1, 2]
        opts.field_widths = [1, 2, 3]
        assert opts.field_widths == [1, 2, 3]

        assert opts.header_rows == 1
        opts.header_rows = 2
        assert opts.header_rows == 2

        assert opts.ignore_empty_lines is True
        opts.ignore_empty_lines = False
        assert opts.ignore_empty_lines is False

    def test_convert_options(self):
        cls = pf.ConvertOptions
        opts = cls()

        opts.column_types = {'a': pa.int64(), 'b': pa.string()}
        assert opts.column_types == {'a': pa.int64(), 'b': pa.string()}

        assert opts.strings_can_be_null is False
        opts.strings_can_be_null = True
        assert opts.strings_can_be_null is True

    @ignore_numpy_warning
    def test_small(self):
        try:
            parse_options = pf.ParseOptions([4, 4])
        except:
            raise Exception('unable to set ParseOptions')

        try:
            fwf, expected = make_random_fwf()  # generates 2 col, width 4 by default
            table = read_bytes(fwf, parse_options)
        except:
            raise Exception('table read failed')

        assert table.schema == expected.schema
        assert table.equals(expected)
        assert table.to_pydict() == expected.to_pydict()

    @ignore_numpy_warning
    def test_big(self):
        field_widths = []
        try:
            for i in range(30):
                field_widths.append(4)
                parse_options = pf.ParseOptions(field_widths)
        except:
            raise Exception('unable to set ParseOptions')

        try:
            fwf, expected = make_random_fwf(num_cols=30, num_rows=10000)
            table = read_bytes(fwf, parse_options)
        except:
            raise Exception('table read failed')

        assert table.schema == expected.schema
        assert table.equals(expected)
        assert table.to_pydict() == expected.to_pydict() 

    @ignore_numpy_warning
    def test_small_encoded(self):
        try:
            parse_options = pf.ParseOptions([4, 4])
        except:
            raise Exception('unable to set ParseOptions')

        try:
            read_options = pf.ReadOptions(encoding='Big5')
            #read_options = pf.ReadOptions(encoding='cp1047,swaplfnl')
        except:
            raise Exception('unable to set ReadOptions')

        try:
            fwf, expected = make_random_fwf(encoding='big5')
            #fwf, expected = make_random_fwf(encoding='cp1047')
            table = read_bytes(fwf, parse_options, read_options=read_options)
        except:
            raise Exception('read failed')

        assert table.schema == expected.schema
        assert table.equals(expected)
        assert table.to_pydict() == expected.to_pydict()

    @ignore_numpy_warning
    def test_big_encoded(self):
        field_widths = []
        try:
            for i in range(30):
                field_widths.append(4)
            parse_options = pf.ParseOptions(field_widths)
        except:
            raise Exception('unable to set ParseOptions')

        try:
            read_options = pf.ReadOptions(encoding='Big5')
            #read_options = pf.ReadOptions(encoding='cp1047,swaplfnl')
        except:
            raise Exception('unable to set ReadOptions')

        try:
            fwf, expected = make_random_fwf(num_cols=30, num_rows=10000, encoding='big5')
            #fwf, expected = make_random_fwf(num_cols=30, num_rows=100000, encoding='cp1047')
            table = read_bytes(fwf, parse_options, read_options=read_options)
        except:
            raise Exception('table read failed')

        assert table.schema == expected.schema
        assert table.equals(expected)
        assert table.to_pydict() == expected.to_pydict()
