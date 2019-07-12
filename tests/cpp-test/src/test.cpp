/* -*- coding: utf-8 -*-
 * vim:fenc=utf-8
 *
 * Copyright © Her Majesty the Queen in Right of Canada, as represented
 * by the Minister of Statistics Canada, 2019.
 *
 * Distributed under terms of the license.
 */

#include "test.h"

void test(const char* fname, std::vector<uint32_t> field_widths,
          const char* encoding, bool verbose) {
    fwfr::test::test_read(fname, field_widths, encoding, verbose);
}

int main() {
    std::vector<uint32_t> field_widths { 6, 6, 6, 4 };
    const char* encoding = "cp1047,swaplfnl";
    test("/home/kira.noel/workdir/fwfr/data/encoded.cp1047.strings.bin",
         field_widths, encoding);
    return 0;
}
