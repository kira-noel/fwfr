# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the license.

# distutils: language = c++
# cython: language_level = 3

from pyfwfr.includes.libfwfr cimport *

from pyarrow.compat import frombytes, tobytes
from collections.abc import Mapping
from pyarrow.includes.common cimport CStatus
from pyarrow.includes.libarrow cimport CDataType, CMemoryPool, CTable, InputStream
from pyarrow.lib cimport (pyarrow_wrap_data_type, pyarrow_unwrap_data_type, check_status,
                          pyarrow_wrap_table, get_input_stream, maybe_unbox_memory_pool,
                          ensure_type, Field, MemoryPool)

cdef class ReadOptions:
    """
    Options for reading fixed-width files.

    Parameters
    ----------
    encoding : strings, optional (default none)
        Encoding of input data. Input is assumed to be UTF8 unless
        otherwise specified.
    buffer_safety_factor : double, optional (deafult 1.3)
        Multiplier for allocating conversion buffer memory:
        X * encoded size
    use_threads : bool, optional (default True)
        Whether to use multiple threads to accelerate reading
    block_size : int, optional
        How many bytes to process at a time from the input stream.
        This will determine multi-threading granularity as well as 
        the size of individual chunks in the Table.
    """
    cdef:
        CFWFReadOptions options

    # Avoid mistakenly creating attributes
    __slots__ = ()

    def __init__(self, encoding=None, use_threads=None):
        self.options = CFWFReadOptions.Defaults()
        if encoding is not None:
            self.encoding = encoding
        if use_threads is not None:
            self.use_threads = use_threads

    @property
    def encoding(self):
        """
        Encoding of input data. Input is assumed to be UTF8 unless
        otherwise specified.
        """
        return frombytes(self.options.encoding)

    @encoding.setter
    def encoding(self, value):
        self.options.encoding = tobytes(value)

    @property
    def buffer_safety_factor(self):
        """
        Multiplier for allocting conversion buffer memory: X * encoded size
        """
        return self.options.buffer_safety_factor

    @buffer_safety_factor.setter
    def buffer_safety_factor(self, value):
        self.options.buffer_safety_factor = value

    @property
    def use_threads(self):
        """
        Whether to use multiple threads to accelerate reading.
        """
        return self.options.use_threads

    @use_threads.setter
    def use_threads(self, value):
        self.options.use_threads = value

    '''
    @property
    def block_size(self):
        """
        How many bytes to process at a time from the input stream.
        This will determine multi-threading granularity as well as
        the size of individual chunks in the Table.
        """
        return self.options.block_size

    @block_size.setter
    def block_size(self, value):
        self.options.block_size = value
    '''

cdef class ParseOptions:
    """
    Options for parsing fixed-width files.

    Parameters
    ----------
    field widths : int list, required
        The number of bytes in each field in a column of FWF data.
    header_rows : int, optional (default 1)
        The number of rows to skip at the start of the FWF data.
    ignore_empty_lines : bool, optional (default True)
        Whether empty lines are ignored in FWF input.
    """
    cdef:
        CFWFParseOptions options

    # Avoid mistakenly creating new attributes
    __slots__ = ()

    def __init__(self, field_widths, header_rows=None, 
                 ignore_empty_lines=None):
        self.options = CFWFParseOptions.Defaults()
        self.field_widths = field_widths
        if header_rows is not None:
            self.header_rows = header_rows
        if ignore_empty_lines is not None:
            self.ignore_empty_lines = ignore_empty_lines

    @property
    def field_widths(self):
        """
        The number of bytes in each field in a column of FWF data.
        """
        return self.options.field_widths

    @field_widths.setter
    def field_widths(self, value):
        self.options.field_widths = value
        #self.options.field_widths = [x for x in value]

    @property
    def header_rows(self):
        """
        The number of rows ro skip at the start of the FWF data.
        """
        return self.options.header_rows

    @header_rows.setter
    def header_rows(self, value):
        self.options.header_rows = value

    @property
    def ignore_empty_lines(self):
        """
        Whether empty lines are ignored in FWF input.
        """
        return self.options.ignore_empty_lines

    @ignore_empty_lines.setter
    def ignore_empty_lines(self, value):
        self.options.ignore_empty_lines = value


cdef class ConvertOptions:
    """
    Options for converting fixed-width file data.

    Parameters
    ----------
    check_utf8 : bool, optional (default True)
        Whether to check UTF8 validity of string columns.
    column_types : dict, optional
        Map column names to column types
        (disables type inferencing on those columns).
    null_values : list, optional
        A sequence of strings that denote nulls in the data
        (defaults are approproate in most cases).
    true_values : list, optional
        A sequence of strings that denote true booleans in the data
        (defaults are appropriate in most cases).
    false_values : list, optional
        A sequence of strings that denote false booleans in the data
        (defaults are appropriate in most cases).
    strings_can_be_null : bool, optional (default False)
        Whether string / binary columns can have null values.
        If true, then strings in null_values are considered null for
        string columns.
        If false, then all strings are valid string values.
    """
    cdef:
        CFWFConvertOptions options

    # Avoid mistakenly creating attributes
    __slots__ = ()

    def __init__(self, check_utf8=None, column_types=None, null_values=None,
                 true_values=None, false_values=None, strings_can_be_null=None):
        self.options = CFWFConvertOptions.Defaults()
        if check_utf8 is not None:
            self.check_utf8 = check_utf8
        if column_types is not None:
            self.column_types = column_types
        if null_values is not None:
            self.null_values = null_values
        if true_values is not None:
            self.true_values = true_values
        if false_values is not None:
            self.false_values = false_values
        if strings_can_be_null is not None:
            self.strings_can_be_null = strings_can_be_null

    @property
    def check_utf8(self):
        """
        Whether to check UTF8 validity of string columns.
        """
        return self.options.check_utf8

    @check_utf8.setter
    def check_utf8(self, value):
        self.options.check_utf8 = value

    @property
    def column_types(self):
        """
        Map column names to column types.
        """
        d = {frombytes(item.first): pyarrow_wrap_data_type(item.second)
             for item in self.options.column_types}
        return d

    @column_types.setter
    def column_types(self, value):
        cdef:
            shared_ptr[CDataType] typ

        if isinstance(value, Mapping):
            value = value.items()

        self.options.column_types.clear()
        for item in value:
            if isinstance(item, Field):
                k = item.name
                v = item.type
            else:
                k, v = item
            typ = pyarrow_unwrap_data_type(ensure_type(v))
            assert typ != NULL
            self.options.column_types[tobytes(k)] = typ

    @property
    def null_values(self):
        """
        A sequence of strings that denote nulls in the data.
        """
        return [frombytes(x) for x in self.options.null_values]

    @null_values.setter
    def null_values(self, value):
        self.options.null_values = [tobytes(x) for x in value]

    @property
    def true_values(self):
        """
        A sequence of string that denote true booleans in the data.
        """
        return [frombytes(x) for x in self.options.true_values]

    @true_values.setter
    def true_values(self, value):
        self.options.true_values = [tobytes(x) for x in value]

    @property
    def false_values(self):
        """
        A sequence of strings that denote false booleans in the data.
        """
        return [frombytes(x) for x in self.options.false_values]

    @false_values.setter
    def false_values(self, value):
        self.options.false_values = [tobytes(x) for x in value]

    @property
    def strings_can_be_null(self):
        """
        Whether string / binary columns can have null values.
        """
        return self.options.strings_can_be_null

    @strings_can_be_null.setter
    def strings_can_be_null(self, value):
        self.options.strings_can_be_null = value


cdef _get_reader(input_file, shared_ptr[InputStream]* out):
    use_memory_map = False
    get_input_stream(input_file, use_memory_map, out)


cdef _get_read_options(ReadOptions read_options, CFWFReadOptions* out):
    if read_options is None:
        out[0] = CFWFReadOptions.Defaults()
    else:
        out[0] = read_options.options

cdef _get_parse_options(ParseOptions parse_options, CFWFParseOptions* out):
    if parse_options is None:
        out[0] = CFWFParseOptions.Defaults()
    else:
        out[0] = parse_options.options


cdef _get_convert_options(ConvertOptions convert_options, CFWFConvertOptions* out):
    if convert_options is None:
        out[0] = CFWFConvertOptions.Defaults()
    else:
        out[0] = convert_options.options


def read_fwf(input_file, parse_options, read_options=None,
             convert_options=None, MemoryPool memory_pool=None):
    """
    Read a Table from a stream of fixed_width data.
    Must set parse_options.field_widths!

    Parameters
    ----------
    input_file : string, path or file-like object
        The location of the FWF data. If a string or path, and if it ends with a recognized
        compressed file extension (e.g. ".gz" or ".bz2"),
        the data is automatically decompressed when reading.
    parse_options : fwfr.ParseOptions, required
        Options for the FWF parser
        (see fwfr.ParseOptions for more details).
    read_options : fwfr.ReadOptions, optional
        Options for the FWF reader
        (see fwfr.ReadOptions for more details).
    convert_options : fwfr.ConvertOptions, optional
        Options for the FWF converter
        (see fwfr.ConvertOptions for more details).
    memory_pool : MemoryPool, optional
        Pool to allocate Table memory from.

    Returns
    -------
    :class:`pyarrow.Table`
        Contents of the FWF file as an in-memory table.
    """
    cdef:
        shared_ptr[InputStream] stream
        CFWFReadOptions c_read_options
        CFWFParseOptions c_parse_options
        CFWFConvertOptions c_convert_options
        shared_ptr[CFWFReader] reader
        shared_ptr[CTable] table

    _get_reader(input_file, &stream)
    _get_read_options(read_options, &c_read_options)
    _get_parse_options(parse_options, &c_parse_options)
    _get_convert_options(convert_options, &c_convert_options)

    check_status(CFWFReader.Make(maybe_unbox_memory_pool(memory_pool),
                                 stream, c_read_options, c_parse_options,
                                 c_convert_options, &reader))
    with nogil:
        check_status(reader.get().Read(&table))

    return pyarrow_wrap_table(table)
    