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
import unittest
import warnings

from pyfwfr.tests.common import make_random_fwf, read_bytes


def ignore_numpy_warning(test_func):
    """
    Ignore deprecated numpy warning. Official fix from numpy is to ignore the
    warning, but unittest warning settings override it and shows the warning.
    """
    def do_test(self, *args, **kwargs):
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore',
                                    message='numpy.ufunc size changed')
            test_func(self, *args, **kwargs)
    return do_test


class TestPyfwfr(unittest.TestCase):
    @ignore_numpy_warning
    def test_big(self):
        field_widths = []
        for i in range(30):
            field_widths.append(4)
        parse_options = pf.ParseOptions(field_widths)
        fwf, expected = make_random_fwf(num_cols=30, num_rows=10000)
        table = read_bytes(fwf, parse_options)

        assert table.schema == expected.schema
        assert table.equals(expected)
        assert table.to_pydict() == expected.to_pydict()

    @ignore_numpy_warning
    def test_big_encoded(self):
        field_widths = []
        for i in range(30):
            field_widths.append(4)
        parse_options = pf.ParseOptions(field_widths)
        read_options = pf.ReadOptions(encoding='Big5')
        fwf, expected = make_random_fwf(num_cols=30, num_rows=10000,
                                        encoding='big5')
        table = read_bytes(fwf, parse_options, read_options=read_options)

        assert table.schema == expected.schema
        assert table.equals(expected)
        assert table.to_pydict() == expected.to_pydict()

    def test_cobol(self):
        rows = b'a  b  c \r\n1A ab 12\r\n33Jcde34\r\n6}  fg56\r\n 3Dhij78'
        parse_options = pf.ParseOptions([3, 3, 2])
        convert_options = pf.ConvertOptions(is_cobol=True)
        table = read_bytes(rows, parse_options,
                           convert_options=convert_options)

        assert isinstance(table, pa.Table)
        assert table.to_pydict() == {'a': [11, -331, -60, 34],
                                     'b': ['ab', 'cde', 'fg', 'hij'],
                                     'c': [12, 34, 56, 78]}
        assert table.column(0).type == 'int64'

    def test_convert_options(self):
        cls = pf.ConvertOptions
        opts = cls()

        opts.column_types = {'a': pa.int64(), 'b': pa.float32()}
        assert opts.column_types == {'a': pa.int64(), 'b': pa.float32()}
        opts.column_types = {'c': 'int16', 'd': 'null'}
        assert opts.column_types == {'c': pa.int16(), 'd': pa.null()}

        schema = pa.schema([('a', pa.int32()), ('b', pa.string())])
        opts.column_types = schema
        assert opts.column_types == {'a': pa.int32(), 'b': pa.string()}

        opts.column_types = [('a', pa.binary())]
        assert opts.column_types == {'a': pa.binary()}

        assert opts.strings_can_be_null is False
        opts.strings_can_be_null = True
        assert opts.strings_can_be_null is True

        assert isinstance(opts.null_values, list)
        assert '' in opts.null_values
        assert 'N/A' in opts.null_values
        opts.null_values = ['a', 'b']
        assert opts.null_values == ['a', 'b']

        assert isinstance(opts.true_values, list)
        opts.true_values = ['a', 'b']
        assert opts.true_values == ['a', 'b']

        assert isinstance(opts.false_values, list)
        opts.false_values = ['a', 'b']
        assert opts.false_values == ['a', 'b']

        assert opts.is_cobol is False
        opts.is_cobol = True
        assert opts.is_cobol is True

        opts.pos_values = {'a': '1', 'b': '2'}
        assert opts.pos_values == {'a': '1', 'b': '2'}

        opts.neg_values = {'a': 'b', '3': '4'}
        assert opts.neg_values == {'a': 'b', '3': '4'}

        opts = cls(column_types={'a': pa.null()}, is_cobol=True,
                   pos_values={'a': '1'}, neg_values={'b': '2'},
                   null_values=['N', 'nn'], true_values=['T', 'tt'],
                   false_values=['F', 'ff'], strings_can_be_null=True)
        assert opts.column_types == {'a': pa.null()}
        assert opts.is_cobol is True
        assert opts.pos_values == {'a': '1'}
        assert opts.neg_values == {'b': '2'}
        assert opts.null_values == ['N', 'nn']
        assert opts.true_values == ['T', 'tt']
        assert opts.false_values == ['F', 'ff']
        assert opts.strings_can_be_null is True

    def test_header(self):
        rows = b'abcdef'
        parse_options = pf.ParseOptions([2, 3, 1])
        table = read_bytes(rows, parse_options)
        assert isinstance(table, pa.Table)
        assert table.num_columns == 3
        assert table.column_names == ['ab', 'cde', 'f']
        assert table.num_rows == 0

    def test_no_header(self):
        rows = b'123456789'
        parse_options = pf.ParseOptions([1, 2, 3, 3])
        read_options = pf.ReadOptions(column_names=['a', 'b', 'c', 'd'])
        table = read_bytes(rows, parse_options, read_options=read_options)
        assert table.to_pydict() == {'a': [1], 'b': [23],
                                     'c': [456], 'd': [789]}

    def test_nulls_bools(self):
        rows = b'a     b     \r\n null N/A   \r\n123456  true'
        parse_options = pf.ParseOptions([6, 6])
        table = read_bytes(rows, parse_options)

        assert(table.column(0).type == 'int64')
        assert(table.column(1).type == 'bool')
        assert table.to_pydict() == {'a': [None, 123456], 'b': [None, True]}

    def test_parse_options(self):
        cls = pf.ParseOptions
        with self.assertRaises(Exception):
            opts = cls()
        opts = cls([1, 2])

        assert opts.field_widths == [1, 2]
        opts.field_widths = [1, 2, 3]
        assert opts.field_widths == [1, 2, 3]

        assert opts.ignore_empty_lines is True
        opts.ignore_empty_lines = False
        assert opts.ignore_empty_lines is False

        opts = cls([1, 2], ignore_empty_lines=False)
        assert opts.field_widths == [1, 2]
        assert opts.ignore_empty_lines is False

    def test_read_options(self):
        cls = pf.ReadOptions
        opts = cls()

        assert opts.encoding == ""
        opts.encoding = 'cp1047,swaplfnl'
        assert opts.encoding == 'cp1047,swaplfnl'

        assert opts.use_threads is True
        opts.use_threads = False
        assert opts.use_threads is False

        assert opts.block_size > 0
        opts.block_size = 12345
        assert opts.block_size == 12345

        assert opts.skip_rows == 0
        opts.skip_rows = 5
        assert opts.skip_rows == 5

        assert opts.column_names == []
        opts.column_names = ['ab', 'cd']
        assert opts.column_names == ['ab', 'cd']

        opts = cls(encoding='abcd', use_threads=False, block_size=1234,
                   skip_rows=1, column_names=['a', 'b', 'c'])
        assert opts.encoding == 'abcd'
        assert opts.use_threads is False
        assert opts.block_size == 1234
        assert opts.skip_rows == 1
        assert opts.column_names == ['a', 'b', 'c']

    def test_serial_read(self):
        parse_options = pf.ParseOptions([4, 4])
        read_options = pf.ReadOptions(use_threads=False)
        fwf, expected = make_random_fwf()  # generate 2 col, width 4 by default
        table = read_bytes(fwf, parse_options, read_options=read_options)

        assert table.schema == expected.schema
        assert table.equals(expected)
        assert table.to_pydict() == expected.to_pydict()

    def test_skip_columns(self):
        rows = b'a  b  c  \r\n11 ab 123\r\n33 cde456\r\n-60 fg789'
        parse_options = pf.ParseOptions([3, 3, 3], skip_columns=[0, 2])
        table = read_bytes(rows, parse_options)

        assert isinstance(table, pa.Table)
        assert table.to_pydict() == {'b': ['ab', 'cde', 'fg']}

    def test_small(self):
        parse_options = pf.ParseOptions([4, 4])
        fwf, expected = make_random_fwf()  # generate 2 col, width 4 by default
        table = read_bytes(fwf, parse_options)

        assert table.schema == expected.schema
        assert table.equals(expected)
        assert table.to_pydict() == expected.to_pydict()

    def test_small_encoded(self):
        parse_options = pf.ParseOptions([4, 4])
        read_options = pf.ReadOptions(encoding='Big5')
        fwf, expected = make_random_fwf(encoding='big5')
        table = read_bytes(fwf, parse_options, read_options=read_options)

        assert table.schema == expected.schema
        assert table.equals(expected)
        assert table.to_pydict() == expected.to_pydict()
