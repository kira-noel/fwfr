/* -*- coding: utf-8 -*-
 * vim:fenc=utf-8
 *
 * Copyright © Her Majesty the Queen in Right of Canada, as represented
 * by the Minister of Statistics Canada, 2019.
 *
 * Distributed under terms of the license.
 */

// gist.github.com/yangacer/1069052

#include <fstream>
#include <string>
#include <cstdio>
#include <cassert>
#include <cstring>
#include <iostream>

#include <unicode/ucsdet.h>
#include <unicode/uclean.h>
#include <unicode/ucnv.h>

#define FROM_CODE "cp1047"
#define TO_CODE "UTF-8"

int main() {
    UErrorCode u_glob_status = U_ZERO_ERROR;
    size_t size = 1024;
    char buf[size + 1];
    FILE *fin = fopen("/home/kira.noel/workdir/fwf-dev/generator/encoded.cp1047.strings.bin", "rb");
    assert(fin);

    size_t read = 0;
    char dest[4096] = {};
    size_t dest_cnt(0);
    size_t converted(0);

    u_init(&u_glob_status);
    assert(U_SUCCESS(u_glob_status));
    
    UErrorCode uerr = U_ZERO_ERROR;
    UConverter *uconv = ucnv_open(FROM_CODE, &uerr);

    size_t boundary = 0;
    size_t left = 0;
    while (0 < (read = fread (buf + left, 1, 1024 - left, fin))) {
        buf[read] = 0;
        boundary = read;

        // find the last escape character to prevent boundary slicing problem
        while (buf[boundary] >= 0)
            boundary--;

        left = read - boundary;

        dest_cnt = ucnv_toAlgorithmic(
                UCNV_UTF8, uconv, dest, 4096, buf, boundary, &uerr);

        std::cout << dest << std::endl;

        std::memcpy(buf, &buf[boundary], left);

        if (U_FAILURE(uerr)) {
            std::cerr << "Error: " << u_errorName(uerr) << std::endl;
        }
    }

    ucnv_close(uconv);
    fclose(fin);
    u_cleanup();
    return 0;
}
