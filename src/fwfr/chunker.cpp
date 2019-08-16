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

#include <fwfr/chunker.h>

namespace fwfr {

namespace {

// Find the last newline character in the given data block.
// nullptr is returned if not found (like memchr()).
const char* FindNewlineReverse(const char* data, uint32_t size) {
  if (size == 0) {
    return nullptr;
  }
  const char* s = data + size - 1;
  while (size > 0) {
    if (*s == '\r' || *s == '\n') {
      return s;
    }
    --s;
    --size;
  }
  return nullptr;
}

}  // namespace

Chunker::Chunker(ParseOptions options) 
    : options_(options) {}

inline const char* Chunker::ReadLine(const char* data, const char* data_end) {
  // The parsing state machine
  char c;
  int32_t cur_field_index = 0;
  int32_t cur_field = 0;

FieldStart:
  // At the start of a field
  goto InField;

InField:
  // Inside a field
  if (data == data_end) {
    goto AbortLine;
  }
  c = *data++;
  ++cur_field_index;

  if (cur_field_index >= options_.field_widths[cur_field]) {
    goto FieldEnd;
  }
  goto InField;

FieldEnd:
  // At the end of a field
  if (cur_field >= options_.field_widths.size() - 1) {
    goto LineEnd;
  }
  ++cur_field;
  cur_field_index = 0;
  goto FieldStart;

LineEnd:
  return data;

AbortLine:
  // Truncated line at end of block
  return nullptr;
}

arrow::Status Chunker::Process(const char* start, uint32_t size, uint32_t* out_size) {
  if (!options_.newlines_in_values) {
    // If newlines are not accepted in FWF values, we can simply search for
    // the last newline character.
    // For common block sizes and FWF row sizes, this avoids reading
    // most of the data block, making the chunker extremely fast compared
    // to the rest of the FWF reading pipeline.
    const char* nl = FindNewlineReverse(start, size);
    if (nl == nullptr) {
      *out_size = 0;
    } else {
      *out_size = static_cast<uint32_t>(nl - start + 1);
    }
    return arrow::Status::OK();
  }
  const char* data = start;
  const char* data_end = start + size;

  while (data < data_end) {
    const char* line_end = ReadLine(data, data_end);
    if (line_end == nullptr) {
        // Cannot read any further
        break;
    }
    data = line_end;
  }
  *out_size = static_cast<uint32_t>(data - start);
  return arrow::Status::OK();
}

}  // namespace fwfr
