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

#ifndef FWFR_COLUMN_BUILDER_H
#define FWFR_COLUMN_BUILDER_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <fwfr/converter.h>
#include <fwfr/options.h>

#include <arrow/array.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/util/logging.h>
#include <arrow/util/task-group.h>
#include <arrow/util/visibility.h>

namespace arrow {
    class ChunkedArray;
    class DataType;
    
    namespace internal {
        class TaskGroup;
    }
}

namespace fwfr {

class BlockParser;
struct ConvertOptions;

class ColumnBuilder {
 public:
  virtual ~ColumnBuilder() = default;

  /// Spawn a task that will try to convert and append the given FWF block.
  /// All calls to Append() should happen on the same thread, otherwise
  /// call Insert() instead.
  virtual void Append(const std::shared_ptr<BlockParser>& parser);

  /// Spawn a task that will try to convert and insert the given FWF block
  virtual void Insert(int64_t block_index,
                      const std::shared_ptr<BlockParser>& parser) = 0;

  /// Return the final chunked array.  The TaskGroup _must_ have finished!
  virtual arrow::Status Finish(std::shared_ptr<arrow::ChunkedArray>* out) = 0;

  /// Change the task group.  The previous TaskGroup _must_ have finished!
  void SetTaskGroup(const std::shared_ptr<arrow::internal::TaskGroup>& task_group);

  std::shared_ptr<arrow::internal::TaskGroup> task_group() { return task_group_; }

  /// Construct a strictly-typed ColumnBuilder.
  static arrow::Status Make(const std::shared_ptr<arrow::DataType>& type, 
                            int32_t col_index, const ConvertOptions& options,
                            const std::shared_ptr<arrow::internal::TaskGroup>& task_group,
                            std::shared_ptr<ColumnBuilder>* out);

  /// Construct a type-inferring ColumnBuilder.
  static arrow::Status Make(int32_t col_index, const ConvertOptions& options,
                            const std::shared_ptr<arrow::internal::TaskGroup>& task_group,
                            std::shared_ptr<ColumnBuilder>* out);

 protected:
  explicit ColumnBuilder(const std::shared_ptr<arrow::internal::TaskGroup>& task_group)
      : task_group_(task_group) {}

  std::shared_ptr<arrow::internal::TaskGroup> task_group_;
  arrow::ArrayVector chunks_;
};

}  // namespace fwfr

#endif  // FWFR_COLUMN_BUILDER_H
