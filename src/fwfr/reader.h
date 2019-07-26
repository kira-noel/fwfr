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

#ifndef FWFR_READER_H
#define FWFR_READER_H

#include <memory>
#include <vector>

#include <fwfr/options.h>

#include <arrow/status.h>
#include <arrow/util/visibility.h>

#include <cstdio>
#include <cstring>
#include <cassert>
#include <unicode/ucnv.h>
#include <unicode/uclean.h>

namespace arrow {

class MemoryPool;
class Table;

namespace io {
class InputStream;
}  // namespace io

namespace fwfr {

class ARROW_EXPORT TableReader {
 public:
  virtual ~TableReader() = default;

  virtual Status Read(std::shared_ptr<Table>* out) = 0;
    
  static int add(int a, int b);

  static Status Make(MemoryPool* pool, std::shared_ptr<io::InputStream> input,
                     const ReadOptions&, const ParseOptions&, const ConvertOptions&,
                     std::shared_ptr<TableReader>* out);
};

}  // namespace fwfr
}  // namespace arrow

#endif  // FWFR_READER_H
