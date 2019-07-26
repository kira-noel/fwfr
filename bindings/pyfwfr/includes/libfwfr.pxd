# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the license.

# distutils: language = c++

from libc.stdint cimport int32_t, uint32_t
from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr
from libcpp.string cimport string as c_string
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map

from pyarrow.compat import frombytes, tobytes, Mapping
from pyarrow.includes.common cimport CStatus
from pyarrow.includes.libarrow cimport CDataType, CMemoryPool, CTable, InputStream

cdef extern from "../include/fwfr/api.h" namespace "arrow::fwfr" nogil:
    cdef cppclass CFWFReadOptions" arrow::fwfr::ReadOptions":
        c_string encoding
        double buffer_safety_factor
        c_bool use_threads
        int32_t block_size
        
        @staticmethod
        CFWFReadOptions Defaults()    
        
    cdef cppclass CFWFParseOptions" arrow::fwfr::ParseOptions":
        vector[uint32_t] field_widths
        int32_t header_rows
        c_bool ignore_empty_lines

        @staticmethod
        CFWFParseOptions Defaults()

    cdef cppclass CFWFConvertOptions" arrow::fwfr::ConvertOptions":
        unordered_map[c_string, shared_ptr[CDataType]] column_types
        vector[c_string] null_values
        vector[c_string] true_values
        vector[c_string] false_values
        c_bool strings_can_be_null

        @staticmethod
        CFWFConvertOptions Defaults()

    cdef cppclass CFWFReader" arrow::fwfr::TableReader":
        @staticmethod
        CStatus Make(CMemoryPool*, shared_ptr[InputStream],
                    CFWFReadOptions, CFWFParseOptions, CFWFConvertOptions,
                    shared_ptr[CFWFReader]* out)

        CStatus Read(shared_ptr[CTable]* out)
