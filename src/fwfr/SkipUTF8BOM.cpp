/* -*- coding: utf-8 -*-
 * vim:fenc=utf-8
 *
 * Copyright © Her Majesty the Queen in Right of Canada, as represented
 * by the Minister of Statistics Canada, 2019.
 *
 * Distributed under terms of the license.
 */

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

}
