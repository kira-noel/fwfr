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
// Excerpt from Apache Arrow's <arrow/util/utf8.h>.
// 
// Copyright © Her Majesty the Queen in Right of Canada, as represented
// by the Minister of Statistics Canada, 2019.
//
// Distributed under terms of the license.

#include <fwfr/SkipUTF8BOM.h>

namespace extras {

static const uint8_t kBOM[] = { 0xEF, 0xBB, 0xBF };

arrow::Status SkipUTF8BOM(const uint8_t* data, int64_t size, const uint8_t** out) {
    int64_t i;
    for (i = 0; i < static_cast<uint64_t>(sizeof(kBOM)); ++i) {
        if (size == 0) {
            if (i == 0) {
                // Empty string
                *out = data;
                return arrow::Status::OK();
            } else {
                return arrow::Status::Invalid("UTF8 string too short (truncated byte order mark?)");
            }
        }
        if (data[i] != kBOM[i]) {
            // BOM not found
            *out = data;
            return arrow::Status::OK();
        }
        --size;
    }
    // BOM found
    *out = data + i;
    return arrow::Status::OK();
} 

}  // namespace extras
