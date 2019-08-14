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

#include <fwfr/reader.h>

#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fwfr/chunker.h>
#include <fwfr/column-builder.h>
#include <fwfr/options.h>
#include <fwfr/parser.h>
#include <fwfr/SkipUTF8BOM.h>  // TODO temporary

#include <arrow/buffer.h>
#include <arrow/io/readahead.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/util/logging.h>
#include <arrow/util/macros.h>
#include <arrow/util/task-group.h>
#include <arrow/util/thread-pool.h>

#include <iostream>

#include <cstdio>
#include <cstring>
#include <cassert>
#include <unicode/ucnv.h>
#include <unicode/uclean.h>

namespace arrow {
    class MemoryPool;

    namespace io {
        class InputStream;
    }
}

namespace fwfr {

using arrow::internal::GetCpuThreadPool;
using arrow::internal::ThreadPool;
using arrow::io::internal::ReadaheadBuffer;
using arrow::io::internal::ReadaheadSpooler;

static constexpr int64_t kDefaultLeftPadding = 2048;  // 2 kB
static constexpr int64_t kDefaultRightPadding = 16;

inline bool IsWhitespace(uint8_t c) {
  if (ARROW_PREDICT_TRUE(c > ' ')) {
    return false;
  }
  return c == ' ' || c == '\t';
}

/////////////////////////////////////////////////////////////////////////
// Base class for common functionality
class BaseTableReader : public fwfr::TableReader {
 public:
  BaseTableReader(arrow::MemoryPool* pool, const ReadOptions& read_options,
                  const ParseOptions& parse_options,
                  const ConvertOptions& convert_options)
      : pool_(pool),
        read_options_(read_options),
        parse_options_(parse_options),
        convert_options_(convert_options) {}

 protected:
  arrow::Status ReadFirstBlock() {
    RETURN_NOT_OK(ReadNextBlock());
    const uint8_t* data;
    
    // TODO arrow::util::SkipUTF8BOM not in latest release; manual for now
    RETURN_NOT_OK(extras::SkipUTF8BOM(cur_data_, cur_size_, &data));
    cur_size_ -= data - cur_data_;
    cur_data_ = data;
    return arrow::Status::OK();
  }

  // Read a next data block, stitch it to trailing data
  arrow::Status ReadNextBlock() {
    bool trailing_data = cur_size_ > 0;
    ReadaheadBuffer rh;

    if (trailing_data) {
      if (readahead_->GetLeftPadding() < cur_size_) {
        // Growth heuristic to try and ensure sufficient left padding
        // in subsequent reads
        readahead_->SetLeftPadding(cur_size_ * 3 / 2);
      }
    }

    RETURN_NOT_OK(readahead_->Read(&rh));
    if (!rh.buffer) {
      // EOF, let caller finish with existing data
      eof_ = true;
      return arrow::Status::OK();
    }

    std::shared_ptr<arrow::Buffer> new_block = rh.buffer;
    uint8_t* new_data = rh.buffer->mutable_data() + rh.left_padding;
    int64_t new_size = rh.buffer->size() - rh.left_padding - rh.right_padding;
    DCHECK_GT(new_size, 0);  // ensured by ReadaheadSpooler

    // Convert input data's encoding to UTF8 if read_options_.encoding is set
    // Notes:
    //   * for accepted codesets: https://demo.icu-project.org/icu-bin/convexp
    //   * EBCDIC encodings need ",lfnl" appended to codeset name ("cp1047,lfnl")
    //     to properly handle newlines 
    if (read_options_.encoding != "") {
        int64_t encoded_size = new_size;
        new_size = ucnv_toAlgorithmic(UCNV_UTF8, ucnv_, reinterpret_cast<char*>(new_data), 
                                      new_size,  
                                      reinterpret_cast<const char*>(new_data), 
                                      new_size, &uerr_);
        if (U_FAILURE(uerr_)) {
            return arrow::Status::Invalid(u_errorName(uerr_));
        }
        if (encoded_size < new_size) {
            return arrow::Status::Invalid("Decoded size larger than encoded size.",
                    " Possible memory errors. ", encoded_size, " vs ", new_size);
        }
    }

    if (trailing_cr_ && new_data[0] == '\n') {
      // Skip '\r\n' line separator that started at the end of previous block
      ++new_data;
      --new_size;
    }
    trailing_cr_ = (new_data[new_size - 1] == '\r');

    if (trailing_data) {
      // Try to copy trailing data at the beginning of new block
      if (cur_size_ <= rh.left_padding) {
        // Can left-extend new block inside padding area
        new_data -= cur_size_;
        new_size += cur_size_;
        std::memcpy(new_data, cur_data_, cur_size_);
      } else {
        // Need to allocate bigger block and concatenate trailing + present data
        RETURN_NOT_OK(
            AllocateBuffer(pool_, cur_size_ + new_size + rh.right_padding, &new_block));
        std::memcpy(new_block->mutable_data(), cur_data_, cur_size_);
        std::memcpy(new_block->mutable_data() + cur_size_, new_data, new_size);
        std::memset(new_block->mutable_data() + cur_size_ + new_size, 0,
                    rh.right_padding);
        new_data = new_block->mutable_data();
        new_size = cur_size_ + new_size;
      }
    }
    cur_block_ = new_block;
    cur_data_ = new_data;
    cur_size_ = new_size;
    return arrow::Status::OK();
  }

  // Read header and column names from current block, create column builders
  arrow::Status ProcessHeader() {
    DCHECK_GT(cur_size_, 0);
    
    if (read_options_.skip_rows) {
        // Skip initial rows (potentially invalid FWF data)
        auto data = cur_data_;
        auto num_skipped_rows = SkipRows(cur_data_, static_cast<uint32_t>(cur_size_),
                                         read_options_.skip_rows, &data);
        cur_size_ -= data - cur_data_;
            cur_data_ = data;
            if (num_skipped_rows < read_options_.skip_rows) {
                return arrow::Status::Invalid(
                        "Could not skip initial ", read_options_.skip_rows,
                        " rows from FWF data, "
                        "either file is too short or header is larger than block size");
            }
    }

    if (read_options_.column_names.empty()) {
        // Read one row with column names
        BlockParser parser(pool_, parse_options_, num_cols_, 1);
        uint32_t parsed_size = 0;
        RETURN_NOT_OK(parser.Parse(reinterpret_cast<const char*>(cur_data_),
                      static_cast<uint32_t>(cur_size_), &parsed_size));
        if (parser.num_cols() == 0) {
            return arrow::Status::Invalid("No columns in FWF");
        } 
        
        if (parser.num_rows() != 1) {
            return arrow::Status::Invalid(
                    "Could not read column names from FWF data, either "
                    "file is too short or header is larger than block size");
        }
        // Read column names from last header row
        auto visit = [&](const uint8_t* data, uint32_t size) -> arrow::Status {
            // Skip trailing whitespace
            if (ARROW_PREDICT_TRUE(size > 0) && ARROW_PREDICT_FALSE(IsWhitespace(data[size - 1]))) {
                const uint8_t* p = data + size - 1;
                while (size > 0 && IsWhitespace(*p)) {
                    --size;
                    --p;
                }
            }
            // Skip leading whitespace
            if (ARROW_PREDICT_TRUE(size > 0) && ARROW_PREDICT_FALSE(IsWhitespace(data[0]))) {
                while (size > 0 && IsWhitespace(*data)) {
                    --size;
                    ++data;
                }
            } 
            column_names_.emplace_back(reinterpret_cast<const char*>(data), size);
            return arrow::Status::OK();
        };
        RETURN_NOT_OK(parser.VisitLastRow(visit));
        DCHECK_EQ(static_cast<size_t>(parser.num_cols()), column_names_.size());
        // Skip parsed header row
        cur_data_ += parsed_size;
        cur_size_ -= parsed_size;
    } else {
        column_names_ = read_options_.column_names;
    }

    num_cols_ = static_cast<int32_t>(column_names_.size());
    DCHECK_GT(num_cols_, 0);

    // Construct column builders
    for (int32_t col_index = 0; col_index < num_cols_; ++col_index) {
      std::shared_ptr<ColumnBuilder> builder;
      // Does the named column have a fixed type?
      auto it = convert_options_.column_types.find(column_names_[col_index]);
      if (it == convert_options_.column_types.end()) {
        RETURN_NOT_OK(
            ColumnBuilder::Make(col_index, convert_options_, task_group_, &builder));
      } else {
        RETURN_NOT_OK(ColumnBuilder::Make(it->second, col_index, convert_options_,
                                          task_group_, &builder));
      }
      column_builders_.push_back(builder);
    }
    return arrow::Status::OK();
  }

  // Trigger conversion of parsed block data
  arrow::Status ProcessData(const std::shared_ptr<BlockParser>& parser, int64_t block_index) {
    for (auto& builder : column_builders_) {
      builder->Insert(block_index, parser);
    }
    return arrow::Status::OK();
  }

  arrow::Status MakeTable(std::shared_ptr<arrow::Table>* out) {
    DCHECK_GT(num_cols_, 0);
    DCHECK_EQ(column_names_.size(), static_cast<uint32_t>(num_cols_));
    DCHECK_EQ(column_builders_.size(), static_cast<uint32_t>(num_cols_));

    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Column>> columns;

    for (int32_t i = 0; i < num_cols_; ++i) {
      std::shared_ptr<arrow::ChunkedArray> array;
      RETURN_NOT_OK(column_builders_[i]->Finish(&array));
      columns.push_back(std::make_shared<arrow::Column>(column_names_[i], array));
      fields.push_back(columns.back()->field());
    }
    *out = arrow::Table::Make(schema(fields), columns);
    return arrow::Status::OK();
  }

  arrow::MemoryPool* pool_;
  ReadOptions read_options_;
  ParseOptions parse_options_;
  ConvertOptions convert_options_;

  // Note: UConverter objects must be declared in one step; no forward declaration
  UErrorCode uerr_ = U_ZERO_ERROR;
  UConverter *ucnv_ = ucnv_open(read_options_.encoding.c_str(), &uerr_); 
  
  int32_t num_cols_ = -1;
  std::shared_ptr<arrow::io::internal::ReadaheadSpooler> readahead_;
  // Column names
  std::vector<std::string> column_names_;
  std::shared_ptr<arrow::internal::TaskGroup> task_group_;
  std::vector<std::shared_ptr<ColumnBuilder>> column_builders_;

  // Current block and data pointer
  std::shared_ptr<arrow::Buffer> cur_block_;
  const uint8_t* cur_data_ = nullptr;
  int64_t cur_size_ = 0;
  // Index of current block inside data stream
  int64_t cur_block_index_ = 0;
  // Whether there was a trailing CR at the end of last parsed line
  bool trailing_cr_ = false;
  // Whether we reached input stream EOF.  There may still be data left to
  // process in current block.
  bool eof_ = false;
};

/////////////////////////////////////////////////////////////////////////
// Serial TableReader implementation
class SerialTableReader : public BaseTableReader {
 public:
  SerialTableReader(arrow::MemoryPool* pool, std::shared_ptr<arrow::io::InputStream> input,
                    const ReadOptions& read_options, const ParseOptions& parse_options,
                    const ConvertOptions& convert_options)
      : BaseTableReader(pool, read_options, parse_options, convert_options) {
    // Since we're converting serially, no need to readahead more than one block
    int32_t block_queue_size = 1;
    readahead_ = std::make_shared<arrow::io::internal::ReadaheadSpooler>(
        pool_, input, read_options_.block_size, block_queue_size, kDefaultLeftPadding,
        kDefaultRightPadding);
  }

  arrow::Status Read(std::shared_ptr<arrow::Table>* out) {
    task_group_ = arrow::internal::TaskGroup::MakeSerial();

    // First block
    RETURN_NOT_OK(ReadFirstBlock());
    if (eof_) {
      return arrow::Status::Invalid("Empty FWF file");
    }
    RETURN_NOT_OK(ProcessHeader());

    static constexpr int32_t max_num_rows = std::numeric_limits<int32_t>::max();
    auto parser =
        std::make_shared<BlockParser>(pool_, parse_options_, num_cols_, max_num_rows);
    while (!eof_) {
      // Consume current block
      uint32_t parsed_size = 0;
      RETURN_NOT_OK(parser->Parse(reinterpret_cast<const char*>(cur_data_),
                                  static_cast<uint32_t>(cur_size_), &parsed_size));
      if (parser->num_rows() > 0) {
        // Got some data
        RETURN_NOT_OK(ProcessData(parser, cur_block_index_++));
        cur_data_ += parsed_size;
        cur_size_ -= parsed_size;
        if (!task_group_->ok()) {
          // Conversion error => early exit
          break;
        }
      } else {
        // Need to fetch more data to get at least one row
        RETURN_NOT_OK(ReadNextBlock());
      }
    }
    if (eof_ && cur_size_ > 0) {
      // Parse remaining data
      uint32_t parsed_size = 0;
      RETURN_NOT_OK(parser->ParseFinal(reinterpret_cast<const char*>(cur_data_),
                                       static_cast<uint32_t>(cur_size_), &parsed_size));
      if (parser->num_rows() > 0) {
        RETURN_NOT_OK(ProcessData(parser, cur_block_index_++));
      }
    }

    // Finish conversion, create schema and table
    RETURN_NOT_OK(task_group_->Finish());

    // Clean up ICU
    ucnv_close(ucnv_);
    u_cleanup();
    
    return MakeTable(out);
  }
};

/////////////////////////////////////////////////////////////////////////
// Parallel TableReader implementation
class ThreadedTableReader : public BaseTableReader {
 public:
  ThreadedTableReader(arrow::MemoryPool* pool, std::shared_ptr<arrow::io::InputStream> input,
                      arrow::internal::ThreadPool* thread_pool, const ReadOptions& read_options,
                      const ParseOptions& parse_options,
                      const ConvertOptions& convert_options)
      : BaseTableReader(pool, read_options, parse_options, convert_options),
        thread_pool_(thread_pool) {
    // Readahead one block per worker thread
    int32_t block_queue_size = thread_pool->GetCapacity();
    readahead_ = std::make_shared<arrow::io::internal::ReadaheadSpooler>(
        pool_, input, read_options_.block_size, block_queue_size, kDefaultLeftPadding,
        kDefaultRightPadding);
  }

  ~ThreadedTableReader() {
    if (task_group_) {
      // In case of error, make sure all pending tasks are finished before
      // we start destroying BaseTableReader members
      ARROW_UNUSED(task_group_->Finish());
    }
  }

  arrow::Status Read(std::shared_ptr<arrow::Table>* out) {
    task_group_ = arrow::internal::TaskGroup::MakeThreaded(thread_pool_);
    static constexpr int32_t max_num_rows = std::numeric_limits<int32_t>::max();
    Chunker chunker(parse_options_);

    // Get first block and process header serially
    RETURN_NOT_OK(ReadFirstBlock());
    if (eof_) {
      return arrow::Status::Invalid("Empty FWF file");
    }
    RETURN_NOT_OK(ProcessHeader());

    while (!eof_ && task_group_->ok()) {
      // Consume current chunk
      uint32_t chunk_size = 0;
      RETURN_NOT_OK(chunker.Process(reinterpret_cast<const char*>(cur_data_),
                                    static_cast<uint32_t>(cur_size_), &chunk_size));
      if (chunk_size > 0) {
        // Got a chunk of rows
        const uint8_t* chunk_data = cur_data_;
        std::shared_ptr<arrow::Buffer> chunk_buffer = cur_block_;
        int64_t chunk_index = cur_block_index_;

        // "mutable" allows to modify captured by-copy chunk_buffer
        task_group_->Append([=]() mutable -> arrow::Status {
          auto parser = std::make_shared<BlockParser>(pool_, parse_options_, 
                                                      num_cols_, max_num_rows);
          uint32_t parsed_size = 0;
          RETURN_NOT_OK(parser->Parse(reinterpret_cast<const char*>(chunk_data),
                                      chunk_size, &parsed_size));
          if (parsed_size != chunk_size && parse_options_.skip_columns.size() == 0) {
            return arrow::Status::Invalid("Chunker and parser disagree on block size: ",
                                   chunk_size, " vs ", parsed_size);
          }
          RETURN_NOT_OK(ProcessData(parser, chunk_index));
          // Keep chunk buffer alive within closure and release it at the end
          chunk_buffer.reset();
          return arrow::Status::OK();
        });
        cur_data_ += chunk_size;
        cur_size_ -= chunk_size;
        cur_block_index_++;
      } else {
        // Need to fetch more data to get at least one row
        RETURN_NOT_OK(ReadNextBlock());
      }
    }

    // Finish all pending parallel tasks
    RETURN_NOT_OK(task_group_->Finish());

    if (eof_ && cur_size_ > 0) {
      // Parse remaining data (serial)
      task_group_ = arrow::internal::TaskGroup::MakeSerial();
      for (auto& builder : column_builders_) {
        builder->SetTaskGroup(task_group_);
      }
      auto parser =
          std::make_shared<BlockParser>(pool_, parse_options_, num_cols_, max_num_rows);
      uint32_t parsed_size = 0;
      RETURN_NOT_OK(parser->ParseFinal(reinterpret_cast<const char*>(cur_data_),
                                       static_cast<uint32_t>(cur_size_), &parsed_size));
      if (parser->num_rows() > 0) {
        RETURN_NOT_OK(ProcessData(parser, cur_block_index_++));
      }
      RETURN_NOT_OK(task_group_->Finish());
    }

    // Clean up ICU
    ucnv_close(ucnv_);
    u_cleanup();

    // Create schema and table
    return MakeTable(out);
  }

 protected:
  arrow::internal::ThreadPool* thread_pool_;
};

/////////////////////////////////////////////////////////////
// TableReader factory function

arrow::Status TableReader::Make(arrow::MemoryPool* pool, std::shared_ptr<arrow::io::InputStream> input,
                                const ReadOptions& read_options,
                                const ParseOptions& parse_options,
                                const ConvertOptions& convert_options,
                                std::shared_ptr<TableReader>* out) {
    std::shared_ptr<TableReader> result;
    if (read_options.use_threads) {
        result = std::make_shared<ThreadedTableReader>(pool, input, arrow::internal::GetCpuThreadPool(), 
                                                       read_options, parse_options, 
                                                       convert_options);
        *out = result;
        return arrow::Status::OK();
    } else {
        result = std::make_shared<SerialTableReader>(pool, input, read_options, parse_options, 
                                                     convert_options);
        *out = result;
        return arrow::Status::OK();
    }
}

}  // namespace fwfr
