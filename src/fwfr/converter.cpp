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

#include <fwfr/converter.h>
#include <fwfr/parser.h>

#include <cstring>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

#include <arrow/builder.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <arrow/util/parsing.h>  // IWYU pragma: keep
#include <arrow/util/trie.h>

namespace fwfr {

using arrow::internal::StringConverter;
using arrow::internal::Trie;
using arrow::internal::TrieBuilder;

namespace {

arrow::Status GenericConversionError(const std::shared_ptr<arrow::DataType>& type, const uint8_t* data,
                              uint32_t size) {
  return arrow::Status::Invalid("FWF conversion error to ", type->ToString(),
                                ": invalid value '",
                                std::string(reinterpret_cast<const char*>(data), size), "'");
}

inline bool IsWhitespace(uint8_t c) {
  if (ARROW_PREDICT_TRUE(c > ' ')) {
    return false;
  }
  return c == ' ' || c == '\t';
}

arrow::Status InitializeTrie(const std::vector<std::string>& inputs, arrow::internal::Trie* trie) {
  arrow::internal::TrieBuilder builder;
  for (const auto& s : inputs) {
    RETURN_NOT_OK(builder.Append(s, true /* allow_duplicates */));
  }
  *trie = builder.Finish();
  return arrow::Status::OK();
}

class ConcreteConverter : public Converter {
 public:
  using Converter::Converter;

 protected:
  arrow::Status Initialize() override;
  inline bool IsNull(const uint8_t* data, uint32_t size);

  arrow::internal::Trie null_trie_;
};

arrow::Status ConcreteConverter::Initialize() {
  return InitializeTrie(options_.null_values, &null_trie_);
}

bool ConcreteConverter::IsNull(const uint8_t* data, uint32_t size) {
  return null_trie_.Find(arrow::util::string_view(reinterpret_cast<const char*>(data), size)) >=
         0;
}

/////////////////////////////////////////////////////////////////////////
// Concrete Converter for null values

class NullConverter : public ConcreteConverter {
 public:
  using ConcreteConverter::ConcreteConverter;

  arrow::Status Convert(const BlockParser& parser, int32_t col_index,
                 std::shared_ptr<arrow::Array>* out) override;
};

arrow::Status NullConverter::Convert(const BlockParser& parser, int32_t col_index,
                                     std::shared_ptr<arrow::Array>* out) {
  arrow::NullBuilder builder(pool_);

  auto visit = [&](const uint8_t* data, uint32_t size) -> arrow::Status {
    if (ARROW_PREDICT_TRUE(IsNull(data, size))) {
      return builder.AppendNull();
    } else {
      return GenericConversionError(type_, data, size);
    }
  };
  RETURN_NOT_OK(parser.VisitColumn(col_index, visit));
  RETURN_NOT_OK(builder.Finish(out));

  return arrow::Status::OK();
}

/////////////////////////////////////////////////////////////////////////
// Concrete Converter for var-sized binary strings

template <typename T>
class VarSizeBinaryConverter : public ConcreteConverter {
 public:
  using ConcreteConverter::ConcreteConverter;

  arrow::Status Convert(const BlockParser& parser, int32_t col_index,
                        std::shared_ptr<arrow::Array>* out) override {
    using BuilderType = typename arrow::TypeTraits<T>::BuilderType;
    BuilderType builder(pool_);

    auto visit_non_null = [&](const uint8_t* data, uint32_t size) -> arrow::Status {
      /*if (CheckUTF8 && ARROW_PREDICT_FALSE(!arrow::util::ValidateUTF8(data, size))) {
        return arrow::Status::Invalid("FWF conversion error to ", type_->ToString(),
                               ": invalid UTF8 data");
      }*/
      // Skip trailing whitespace
      if (ARROW_PREDICT_TRUE(size > 0) &&
          ARROW_PREDICT_FALSE(IsWhitespace(data[size - 1]))) {
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
      builder.UnsafeAppend(data, size);
      return arrow::Status::OK();
    };

    RETURN_NOT_OK(builder.Resize(parser.num_rows()));
    RETURN_NOT_OK(builder.ReserveData(parser.num_bytes()));

    if (options_.strings_can_be_null) {
      auto visit = [&](const uint8_t* data, uint32_t size) -> arrow::Status {
        // Skip trailing whitespace
        if (ARROW_PREDICT_TRUE(size > 0) &&
            ARROW_PREDICT_FALSE(IsWhitespace(data[size - 1]))) {
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
        if (IsNull(data, size)) {
          builder.UnsafeAppendNull();
          return arrow::Status::OK();
        } else {
          return visit_non_null(data, size);
        }
      };
      RETURN_NOT_OK(parser.VisitColumn(col_index, visit));
    } else {
      RETURN_NOT_OK(parser.VisitColumn(col_index, visit_non_null));
    }

    RETURN_NOT_OK(builder.Finish(out));

    return arrow::Status::OK();
  }

 protected:
    arrow::Status Initialize() override {
    //arrow::util::InitializeUTF8();
    return ConcreteConverter::Initialize();
  }
};

/////////////////////////////////////////////////////////////////////////
// Concrete Converter for fixed-sized binary strings

class FixedSizeBinaryConverter : public ConcreteConverter {
 public:
  using ConcreteConverter::ConcreteConverter;

  arrow::Status Convert(const BlockParser& parser, int32_t col_index,
                        std::shared_ptr<arrow::Array>* out) override;
};

arrow::Status FixedSizeBinaryConverter::Convert(const BlockParser& parser, int32_t col_index,
                                                std::shared_ptr<arrow::Array>* out) {
  arrow::FixedSizeBinaryBuilder builder(type_, pool_);
  const uint32_t byte_width = static_cast<uint32_t>(builder.byte_width());

  auto visit = [&](const uint8_t* data, uint32_t size) -> arrow::Status {
    if (ARROW_PREDICT_FALSE(size != byte_width)) {
      return arrow::Status::Invalid("FWF conversion error to ", type_->ToString(), ": got a ",
                                    size, "-byte long string");
    }
    return builder.Append(data);
  };
  RETURN_NOT_OK(builder.Resize(parser.num_rows()));
  RETURN_NOT_OK(parser.VisitColumn(col_index, visit));
  RETURN_NOT_OK(builder.Finish(out));

  return arrow::Status::OK();
}

/////////////////////////////////////////////////////////////////////////
// Concrete Converter for booleans

class BooleanConverter : public ConcreteConverter {
 public:
  using ConcreteConverter::ConcreteConverter;

  arrow::Status Convert(const BlockParser& parser, int32_t col_index,
                        std::shared_ptr<arrow::Array>* out) override;

 protected:
  arrow::Status Initialize() override {
    RETURN_NOT_OK(InitializeTrie(options_.true_values, &true_trie_));
    RETURN_NOT_OK(InitializeTrie(options_.false_values, &false_trie_));
    return ConcreteConverter::Initialize();
  }

  arrow::internal::Trie true_trie_;
  arrow::internal::Trie false_trie_;
};

arrow::Status BooleanConverter::Convert(const BlockParser& parser, int32_t col_index,
                                        std::shared_ptr<arrow::Array>* out) {
    arrow::BooleanBuilder builder(type_, pool_);

  auto visit = [&](const uint8_t* data, uint32_t size) -> arrow::Status {
    // Skip trailing whitespace
    if (ARROW_PREDICT_TRUE(size > 0) &&
        ARROW_PREDICT_FALSE(IsWhitespace(data[size - 1]))) {
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
    if (IsNull(data, size)) {
      builder.UnsafeAppendNull();
      return arrow::Status::OK();
    }
    if (false_trie_.Find(arrow::util::string_view(reinterpret_cast<const char*>(data), size)) >=
        0) {
      builder.UnsafeAppend(false);
      return arrow::Status::OK();
    }
    if (true_trie_.Find(arrow::util::string_view(reinterpret_cast<const char*>(data), size)) >=
        0) {
      builder.UnsafeAppend(true);
      return arrow::Status::OK();
    }
    return GenericConversionError(type_, data, size);
  };
  RETURN_NOT_OK(builder.Resize(parser.num_rows()));
  RETURN_NOT_OK(parser.VisitColumn(col_index, visit));
  RETURN_NOT_OK(builder.Finish(out));

  return arrow::Status::OK();
}

/////////////////////////////////////////////////////////////////////////
// Concrete Converter for numbers

template <typename T>
class NumericConverter : public ConcreteConverter {
 public:
  using ConcreteConverter::ConcreteConverter;

  arrow::Status Convert(const BlockParser& parser, int32_t col_index,
                        std::shared_ptr<arrow::Array>* out) override;
};

template <typename T>
arrow::Status NumericConverter<T>::Convert(const BlockParser& parser, int32_t col_index,
                                           std::shared_ptr<arrow::Array>* out) {
  using BuilderType = typename arrow::TypeTraits<T>::BuilderType;
  using value_type = typename StringConverter<T>::value_type;

  BuilderType builder(type_, pool_);
  StringConverter<T> converter;

  auto visit = [&](const uint8_t* data, uint32_t size) -> arrow::Status {
    value_type value;
    // Skip trailing whitespace
    if (ARROW_PREDICT_TRUE(size > 0) &&
        ARROW_PREDICT_FALSE(IsWhitespace(data[size - 1]))) {
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
    if (IsNull(data, size)) {
      builder.UnsafeAppendNull();
      return arrow::Status::OK();
    } 
    // Handle COBOL data format
    if (options_.is_cobol) {
      bool is_modified = false;
      const char last = *(data + size - 1);
      char new_data[size + 1];  // the existing data object is read-only

      // Check positive map values
      // If the last character maps to ConvertOptions::pos_values,
      // switch last character to corresponding value.
      auto it = options_.pos_values.find(last);
      if (it != options_.pos_values.end()) {
        is_modified = true;
        for (int i=0; i < size; i++) {
          new_data[i] = data[i];
        }
        new_data[size - 1] = it->second;
      }

      // Check negative map values
      // If the last character maps to ConvertOptions::neg_values,
      // switch last character to corresponding value and add negative sign.
      it = options_.neg_values.find(last);
      if (it != options_.neg_values.end()) {
        is_modified = true;
        size++;
        for (int i=size-2; i >= 0; i--) {
          new_data[i + 1] = *(data + i);
        }
        new_data[size - 1] = it->second;
        new_data[0] = '-';
      }
      
      // Determine datatype and value of data
      if (is_modified) {
        if (ARROW_PREDICT_FALSE(
                    !converter(reinterpret_cast<const char*>(new_data), size, &value))) {
          return GenericConversionError(type_, reinterpret_cast<const uint8_t*>(new_data), size);
        }
        builder.UnsafeAppend(value);
        return arrow::Status::OK();
      }
    }
    if (ARROW_PREDICT_FALSE(
            !converter(reinterpret_cast<const char*>(data), size, &value))) {
      return GenericConversionError(type_, data, size);
    }
    builder.UnsafeAppend(value);
    return arrow::Status::OK();
  };
  RETURN_NOT_OK(builder.Resize(parser.num_rows()));
  RETURN_NOT_OK(parser.VisitColumn(col_index, visit));
  RETURN_NOT_OK(builder.Finish(out));

  return arrow::Status::OK();
}

/////////////////////////////////////////////////////////////////////////
// Concrete Converter for timestamps

class TimestampConverter : public ConcreteConverter {
 public:
  using ConcreteConverter::ConcreteConverter;

  arrow::Status Convert(const BlockParser& parser, int32_t col_index,
                        std::shared_ptr<arrow::Array>* out) override {
    using value_type = arrow::TimestampType::c_type;

    arrow::TimestampBuilder builder(type_, pool_);
    StringConverter<arrow::TimestampType> converter(type_);

    auto visit = [&](const uint8_t* data, uint32_t size) -> arrow::Status {
      value_type value = 0;
      // Skip trailing whitespace
      if (ARROW_PREDICT_TRUE(size > 0) &&
          ARROW_PREDICT_FALSE(IsWhitespace(data[size - 1]))) {
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
      if (IsNull(data, size)) {
        builder.UnsafeAppendNull();
        return arrow::Status::OK();
      }
      if (ARROW_PREDICT_FALSE(
              !converter(reinterpret_cast<const char*>(data), size, &value))) {
        return GenericConversionError(type_, data, size);
      }
      builder.UnsafeAppend(value);
      return arrow::Status::OK();
    };
    RETURN_NOT_OK(builder.Resize(parser.num_rows()));
    RETURN_NOT_OK(parser.VisitColumn(col_index, visit));
    RETURN_NOT_OK(builder.Finish(out));

    return arrow::Status::OK();
  }
};

}  // namespace

/////////////////////////////////////////////////////////////////////////
// Base Converter class implementation

Converter::Converter(const std::shared_ptr<arrow::DataType>& type, const ConvertOptions& options,
                     arrow::MemoryPool* pool)
    : options_(options), pool_(pool), type_(type) {}

arrow::Status Converter::Make(const std::shared_ptr<arrow::DataType>& type,
                              const ConvertOptions& options, arrow::MemoryPool* pool,
                              std::shared_ptr<Converter>* out) {
  Converter* result;

  switch (type->id()) {
#define CONVERTER_CASE(TYPE_ID, CONVERTER_TYPE)       \
  case TYPE_ID:                                       \
    result = new CONVERTER_TYPE(type, options, pool); \
    break;

    CONVERTER_CASE(arrow::Type::NA, NullConverter)
    CONVERTER_CASE(arrow::Type::INT8, NumericConverter<arrow::Int8Type>)
    CONVERTER_CASE(arrow::Type::INT16, NumericConverter<arrow::Int16Type>)
    CONVERTER_CASE(arrow::Type::INT32, NumericConverter<arrow::Int32Type>)
    CONVERTER_CASE(arrow::Type::INT64, NumericConverter<arrow::Int64Type>)
    CONVERTER_CASE(arrow::Type::UINT8, NumericConverter<arrow::UInt8Type>)
    CONVERTER_CASE(arrow::Type::UINT16, NumericConverter<arrow::UInt16Type>)
    CONVERTER_CASE(arrow::Type::UINT32, NumericConverter<arrow::UInt32Type>)
    CONVERTER_CASE(arrow::Type::UINT64, NumericConverter<arrow::UInt64Type>)
    CONVERTER_CASE(arrow::Type::FLOAT, NumericConverter<arrow::FloatType>)
    CONVERTER_CASE(arrow::Type::DOUBLE, NumericConverter<arrow::DoubleType>)
    CONVERTER_CASE(arrow::Type::BOOL, BooleanConverter)
    CONVERTER_CASE(arrow::Type::TIMESTAMP, TimestampConverter)
    CONVERTER_CASE(arrow::Type::BINARY, (VarSizeBinaryConverter<arrow::BinaryType>))
    CONVERTER_CASE(arrow::Type::FIXED_SIZE_BINARY, FixedSizeBinaryConverter)

  case arrow::Type::STRING:
      result = new VarSizeBinaryConverter<arrow::StringType>(type, options, pool);
      break;

    default: {
      return arrow::Status::NotImplemented("FWF conversion to ", type->ToString(),
                                           " is not supported");
    }

#undef CONVERTER_CASE
  }
  out->reset(result);
  return result->Initialize();
}

arrow::Status Converter::Make(const std::shared_ptr<arrow::DataType>& type,
                       const ConvertOptions& options, std::shared_ptr<Converter>* out) {
  return Make(type, options, arrow::default_memory_pool(), out);
}

}  // namespace fwfr
