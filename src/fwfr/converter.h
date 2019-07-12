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
// Modified from Apache Arrow's CSV reader.
// 
// Copyright © Her Majesty the Queen in Right of Canada, as represented
// by the Minister of Statistics Canada, 2019.
//
// Distributed under terms of the license.

#ifndef FWFR_CONVERTER_H
#define FWFR_CONVERTER_H

#include <cstdint>
#include <memory>

#include <fwfr/options.h>

#include <arrow/util/macros.h>
#include <arrow/util/visibility.h>

namespace arrow {

class Array;
class DataType;
class MemoryPool;
class Status;

namespace fwfr {

class BlockParser;

class ARROW_EXPORT Converter {
 public:
  Converter(const std::shared_ptr<DataType>& type, const ConvertOptions& options,
            MemoryPool* pool);
  virtual ~Converter() = default;

  virtual Status Convert(const BlockParser& parser, int32_t col_index,
                         std::shared_ptr<Array>* out) = 0;

  std::shared_ptr<DataType> type() const { return type_; }

  static Status Make(const std::shared_ptr<DataType>& type, const ConvertOptions& options,
                     std::shared_ptr<Converter>* out);
  static Status Make(const std::shared_ptr<DataType>& type, const ConvertOptions& options,
                     MemoryPool* pool, std::shared_ptr<Converter>* out);

 protected:
  ARROW_DISALLOW_COPY_AND_ASSIGN(Converter);

  virtual Status Initialize() = 0;

  const ConvertOptions options_;
  MemoryPool* pool_;
  std::shared_ptr<DataType> type_;
};

}  // namespace fwfr
}  // namespace arrow

#endif  // FWFR_CONVERTER_H
