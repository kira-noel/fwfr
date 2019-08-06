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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <fwfr/column-builder.h>
#include <fwfr/converter.h>
#include <fwfr/options.h>

#include <arrow/array.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/util/logging.h>
#include <arrow/util/task-group.h>

namespace fwfr {

class BlockParser;

using arrow::internal::TaskGroup;

void ColumnBuilder::SetTaskGroup(const std::shared_ptr<arrow::internal::TaskGroup>& task_group) {
  task_group_ = task_group;
}

void ColumnBuilder::Append(const std::shared_ptr<BlockParser>& parser) {
  Insert(static_cast<int64_t>(chunks_.size()), parser);
}

//////////////////////////////////////////////////////////////////////////
// Pre-typed column builder implementation

class TypedColumnBuilder : public ColumnBuilder {
 public:
  TypedColumnBuilder(const std::shared_ptr<arrow::DataType>& type, int32_t col_index,
                     const ConvertOptions& options, arrow::MemoryPool* pool,
                     const std::shared_ptr<arrow::internal::TaskGroup>& task_group)
      : ColumnBuilder(task_group),
        type_(type),
        col_index_(col_index),
        options_(options),
        pool_(pool) {}

  arrow::Status Init();

  void Insert(int64_t block_index, const std::shared_ptr<BlockParser>& parser) override;
  arrow::Status Finish(std::shared_ptr<arrow::ChunkedArray>* out) override;

 protected:
  arrow::Status WrapConversionError(const arrow::Status& st) {
    if (st.ok()) {
      return st;
    } else {
      std::stringstream ss;
      ss << "In column #" << col_index_ << ": " << st.message();
      return arrow::Status(st.code(), ss.str());
    }
  }

  std::mutex mutex_;

  std::shared_ptr<arrow::DataType> type_;
  int32_t col_index_;
  ConvertOptions options_;
  arrow::MemoryPool* pool_;

  std::shared_ptr<Converter> converter_;
};

arrow::Status TypedColumnBuilder::Init() {
  return Converter::Make(type_, options_, pool_, &converter_);
}

void TypedColumnBuilder::Insert(int64_t block_index,
                                const std::shared_ptr<BlockParser>& parser) {
  DCHECK_NE(converter_, nullptr);

  // Create a null Array pointer at the back at the list
  // and spawn a task to initialize it after conversion
  size_t chunk_index = static_cast<size_t>(block_index);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (chunks_.size() <= chunk_index) {
      chunks_.resize(chunk_index + 1);
    }
  }

  // We're careful that all references in the closure outlive the Append() call
  task_group_->Append([=]() -> arrow::Status {
    std::shared_ptr<arrow::Array> res;
    RETURN_NOT_OK(WrapConversionError(converter_->Convert(*parser, col_index_, &res)));

    std::lock_guard<std::mutex> lock(mutex_);
    // Should not insert an already converted chunk
    DCHECK_EQ(chunks_[chunk_index], nullptr);
    chunks_[chunk_index] = std::move(res);
    return arrow::Status::OK();
  });
}

arrow::Status TypedColumnBuilder::Finish(std::shared_ptr<arrow::ChunkedArray>* out) {
  // Unnecessary iff all tasks have finished
  std::lock_guard<std::mutex> lock(mutex_);

  for (const auto& chunk : chunks_) {
    if (chunk == nullptr) {
      return arrow::Status::Invalid("a chunk failed converting for an unknown reason");
    }
  }
  *out = std::make_shared<arrow::ChunkedArray>(chunks_, type_);
  return arrow::Status::OK();
}

//////////////////////////////////////////////////////////////////////////
// Type-inferring column builder implementation

class InferringColumnBuilder : public ColumnBuilder {
 public:
  InferringColumnBuilder(int32_t col_index, const ConvertOptions& options,
                         arrow::MemoryPool* pool,
                         const std::shared_ptr<arrow::internal::TaskGroup>& task_group)
      : ColumnBuilder(task_group),
        col_index_(col_index),
        options_(options),
        pool_(pool) {}

  arrow::Status Init();

  void Insert(int64_t block_index, const std::shared_ptr<BlockParser>& parser) override;
  arrow::Status Finish(std::shared_ptr<arrow::ChunkedArray>* out) override;

 protected:
  arrow::Status LoosenType();
  arrow::Status UpdateType();
  arrow::Status TryConvertChunk(size_t chunk_index);
  // This must be called unlocked!
  void ScheduleConvertChunk(size_t chunk_index);

  std::mutex mutex_;

  int32_t col_index_;
  ConvertOptions options_;
  arrow::MemoryPool* pool_;
  std::shared_ptr<Converter> converter_;

  // Current inference status
  enum class InferKind { Null, Integer, Boolean, Real, Timestamp, Text, Binary };

  std::shared_ptr<arrow::DataType> infer_type_;
  InferKind infer_kind_;
  bool can_loosen_type_;

  // The parsers corresponding to each chunk (for reconverting)
  std::vector<std::shared_ptr<BlockParser>> parsers_;
};

arrow::Status InferringColumnBuilder::Init() {
  infer_kind_ = InferKind::Null;
  return UpdateType();
}

arrow::Status InferringColumnBuilder::LoosenType() {
  // We are locked

  DCHECK(can_loosen_type_);
  switch (infer_kind_) {
    case InferKind::Null:
      infer_kind_ = InferKind::Integer;
      break;
    case InferKind::Integer:
      infer_kind_ = InferKind::Boolean;
      break;
    case InferKind::Boolean:
      infer_kind_ = InferKind::Timestamp;
      break;
    case InferKind::Timestamp:
      infer_kind_ = InferKind::Real;
      break;
    case InferKind::Real:
      infer_kind_ = InferKind::Text;
      break;
    case InferKind::Text:
      infer_kind_ = InferKind::Binary;
      break;
    case InferKind::Binary:
      return arrow::Status::UnknownError("Shouldn't come here");
  }
  return UpdateType();
}

arrow::Status InferringColumnBuilder::UpdateType() {
  // We are locked

  switch (infer_kind_) {
    case InferKind::Null:
      infer_type_ = arrow::null();
      can_loosen_type_ = true;
      break;
    case InferKind::Integer:
      infer_type_ = arrow::int64();
      can_loosen_type_ = true;
      break;
    case InferKind::Boolean:
      infer_type_ = arrow::boolean();
      can_loosen_type_ = true;
      break;
    case InferKind::Timestamp:
      // We don't support parsing second fractions for now
      infer_type_ = arrow::timestamp(arrow::TimeUnit::SECOND);
      can_loosen_type_ = true;
      break;
    case InferKind::Real:
      infer_type_ = arrow::float64();
      can_loosen_type_ = true;
      break;
    case InferKind::Text:
      infer_type_ = arrow::utf8();
      can_loosen_type_ = true;
      break;
    case InferKind::Binary:
      infer_type_ = arrow::binary();
      can_loosen_type_ = false;
      break;
  }
  return Converter::Make(infer_type_, options_, pool_, &converter_);
}

void InferringColumnBuilder::ScheduleConvertChunk(size_t chunk_index) {
  // We're careful that all values in the closure outlive the Append() call
  task_group_->Append([=]() { return TryConvertChunk(chunk_index); });
}

arrow::Status InferringColumnBuilder::TryConvertChunk(size_t chunk_index) {
  std::unique_lock<std::mutex> lock(mutex_);
  std::shared_ptr<Converter> converter = converter_;
  std::shared_ptr<BlockParser> parser = parsers_[chunk_index];
  std::shared_ptr<arrow::Array> res;
  InferKind kind = infer_kind_;

  DCHECK_NE(parser, nullptr);

  lock.unlock();
  arrow::Status st = converter->Convert(*parser, col_index_, &res);
  lock.lock();

  if (kind != infer_kind_) {
    // infer_kind_ was changed by another task, reconvert
    lock.unlock();
    ScheduleConvertChunk(chunk_index);
    return arrow::Status::OK();
  }

  if (st.ok()) {
    // Conversion succeeded
    chunks_[chunk_index] = std::move(res);
    if (!can_loosen_type_) {
      // We won't try to reconvert anymore
      parsers_[chunk_index].reset();
    }
    return arrow::Status::OK();
  } else if (can_loosen_type_) {
    // Conversion failed, try another type
    RETURN_NOT_OK(LoosenType());

    // Reconvert past finished chunks
    // (unfinished chunks will notice by themselves if they need reconverting)
    size_t nchunks = chunks_.size();
    for (size_t i = 0; i < nchunks; ++i) {
      if (i != chunk_index && chunks_[i]) {
        // We're assuming the chunk was converted using the wrong type
        // (which should be true unless the executor reorders tasks)
        chunks_[i].reset();
        lock.unlock();
        ScheduleConvertChunk(i);
        lock.lock();
      }
    }

    // Reconvert this chunk
    lock.unlock();
    ScheduleConvertChunk(chunk_index);

    return arrow::Status::OK();
  } else {
    // Conversion failed but cannot loosen more
    return st;
  }
}

void InferringColumnBuilder::Insert(int64_t block_index,
                                    const std::shared_ptr<BlockParser>& parser) {
  // Create a slot for the new chunk and spawn a task to convert it
  size_t chunk_index = static_cast<size_t>(block_index);
  {
    std::lock_guard<std::mutex> lock(mutex_);

    DCHECK_NE(converter_, nullptr);
    if (chunks_.size() <= chunk_index) {
      chunks_.resize(chunk_index + 1);
    }
    if (parsers_.size() <= chunk_index) {
      parsers_.resize(chunk_index + 1);
    }
    // Should not insert an already converting chunk
    DCHECK_EQ(parsers_[chunk_index], nullptr);
    parsers_[chunk_index] = parser;
  }

  ScheduleConvertChunk(chunk_index);
}

arrow::Status InferringColumnBuilder::Finish(std::shared_ptr<arrow::ChunkedArray>* out) {
  // Unnecessary iff all tasks have finished
  std::lock_guard<std::mutex> lock(mutex_);

  for (const auto& chunk : chunks_) {
    if (chunk == nullptr) {
      return arrow::Status::Invalid("A chunk failed converting for an unknown reason");
    }
    DCHECK_EQ(chunk->type()->id(), infer_type_->id())
        << "Inference didn't equalize types!";
  }
  *out = std::make_shared<arrow::ChunkedArray>(chunks_, infer_type_);
  chunks_.clear();
  parsers_.clear();

  return arrow::Status::OK();
}

////////////////////////////////////////////////////////////////////////
// Factory functions

arrow::Status ColumnBuilder::Make(const std::shared_ptr<arrow::DataType>& type, int32_t col_index,
                                  const ConvertOptions& options,
                                  const std::shared_ptr<TaskGroup>& task_group,
                                  std::shared_ptr<ColumnBuilder>* out) {
    auto ptr = new TypedColumnBuilder(type, col_index, options, arrow::default_memory_pool(), task_group);
    auto res = std::shared_ptr<ColumnBuilder>(ptr);
    RETURN_NOT_OK(ptr->Init());
    *out = res;
    return arrow::Status::OK();
}

arrow::Status ColumnBuilder::Make(int32_t col_index, const ConvertOptions& options,
                                  const std::shared_ptr<TaskGroup>& task_group,
                                  std::shared_ptr<ColumnBuilder>* out) {
    auto ptr = new InferringColumnBuilder(col_index, options, arrow::default_memory_pool(), task_group);
    auto res = std::shared_ptr<ColumnBuilder>(ptr);
    RETURN_NOT_OK(ptr->Init());
    *out = res;
    return arrow::Status::OK();
}

}  // namespace fwfr
