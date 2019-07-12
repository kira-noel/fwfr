/* -*- coding: utf-8 -*-
 * vim:fenc=utf-8
 *
 * Copyright © Her Majesty the Queen in Right of Canada, as represented
 * by the Minister of Statistics Canada, 2019.
 *
 * Distributed under terms of the license.
 */
#ifndef TEST_H
#define TEST_H

#include <cstdint>
#include <iostream>
#include <vector>

#include <fwfr/test_reader.h>

void test(const char* fname, std::vector<uint32_t>,
          const char* encoding=NULL, bool verbose=true);

#endif
