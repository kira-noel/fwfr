/* -*- coding: utf-8 -*-
 * vim:fenc=utf-8
 *
 * Copyright © Her Majesty the Queen in Right of Canada, as represented
 * by the Minister of Statistics Canada, 2019.
 *
 * Distributed under terms of the license.
 */

#ifndef FWF_TEST_READER_H
#define FWF_TEST_READER_H

#include <iostream>
#include <vector>
#include <string>
#include <cstdint>

#include <fwfr/api.h>
#include <arrow/api.h>
#include <arrow/io/api.h>

namespace fwfr {
namespace test {
std::shared_ptr<arrow::Table> test_read(const char* fname, 
                                       std::vector<uint32_t> field_widths,
                                       std::string encoding, bool verbose=true);
}  // namespace test
}  // namespace fwfr

#endif
