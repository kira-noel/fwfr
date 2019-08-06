// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
// 
// Modified from Apache Arrow's CSV reader by Kira Noël.
// 
// Copyright © Her Majesty the Queen in Right of Canada, as represented
// by the Minister of Statistics Canada, 2019.
//
// Distributed under terms of the license.

#ifndef FWFR_OPTIONS_H
#define FWFR_OPTIONS_H

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/util/visibility.h>

namespace arrow {
    class DataType;
}

namespace fwfr {

struct ARROW_EXPORT ParseOptions {
  // Parsing options  

  // Field widths by bytes -- REQUIRED
  std::vector<uint32_t> field_widths;
  // Whether values are allowed to contain CR (0x0d) and LF (0x0a) characters
  bool newlines_in_values = false;
  // Whether empty lines are ignored.  If false, an empty line represents
  // a single empty value (assuming a one-column FWF file).
  bool ignore_empty_lines = true;

  static ParseOptions Defaults();
};

struct ARROW_EXPORT ConvertOptions {
  // Conversion options

  // Optional per-column types (disabling type inference on those columns)
  std::unordered_map<std::string, std::shared_ptr<arrow::DataType>> column_types;
  // Whether to treat as COBOL data
  bool is_cobol = false;
  // Optional, positive numbers for COBOL datasets. Use last character to change field meaning.
  std::unordered_map<char, char> pos_values = {
          {'{', '0'}, {'A', '1'}, {'B', '2'}, {'C', '3'}, {'D', '4'},
          {'E', '5'}, {'F', '6'}, {'G', '7'}, {'H', '8'}, {'I', '9'}
  };
  // Optional, negative numbers for COBOL datasets. Use last character to change field meaning.
  std::unordered_map<char, char> neg_values = {
          {'}', '0'}, {'J', '1'}, {'K', '2'}, {'L', '3'}, {'M', '4'},
          {'E', '5'}, {'O', '6'}, {'P', '7'}, {'Q', '8'}, {'R', '9'}
  };
  // Recognized spellings for null values
  std::vector<std::string> null_values;
  // Recognized spellings for boolean values
  std::vector<std::string> true_values;
  std::vector<std::string> false_values;
  // Whether string / binary columns can have null values.
  // If true, then strings in "null_values" are considered null for string columns.
  // If false, then all strings are valid string values.
  bool strings_can_be_null = false;

  static ConvertOptions Defaults();
};

struct ARROW_EXPORT ReadOptions {
  // Reader options

  // Encoding type on input data, if any
  std::string encoding = "";
  // Whether to use the global CPU thread pool
  bool use_threads = true;
  // Block size we request from the IO layer; also determines the size of
  // chunks when use_threads is true
  int32_t block_size = 1 << 20;  // 1 MB

  // Number of header rows to skip (not including the row of column names, if any)
  int32_t skip_rows = 0;
  // Column names (if empty, will be read from first row after 'skip_rows')
  std::vector<std::string> column_names;

  static ReadOptions Defaults();
};

}  // namespace fwfr

#endif  // FWFR_OPTIONS_H
