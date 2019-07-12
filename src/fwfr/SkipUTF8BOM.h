/* -*- coding: utf-8 -*-
 * vim:fenc=utf-8
 *
 * Copyright © Her Majesty the Queen in Right of Canada, as represented
 * by the Minister of Statistics Canada, 2019.
 *
 * Distributed under terms of the license.
 */

/* NOTE: copy of part of Apache Arrow's current build (not yet released). 
 * To be removed when Apache Arrow adds to the next release.
 * https://github.com/apache/arrow/cpp/src/arrow/ --> util/utf8.cc::SkipUTF8BOM
 */

#ifndef FWFR_SKIPUTF8BOM_H
#define FWFR_SKIPUTF8BOM_H

#include <arrow/status.h>
#include <memory>
#include <cstdint>

namespace extras {

arrow::Status SkipUTF8BOM(const uint8_t* data, int64_t size, const uint8_t** out);

}  // namespace extras

#endif  // FWFR_SKIPUTF8BOM_H
