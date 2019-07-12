/* -*- coding: utf-8 -*-
 * vim:fenc=utf-8
 *
 * Copyright © Her Majesty the Queen in Right of Canada, as represented
 * by the Minister of Statistics Canada, 2019.
 *
 * Distributed under terms of the license.
 */

// gist.github.com/yangacer/1067311

#include <fstream>
#include <string>
#include <cstdio>
#include <cassert>

#include <unicode/ucsdet.h>
#include <unicode/uclean.h>
#include <unicode/ucnv.h>

int main() {
    UErrorCode u_glob_status = U_ZERO_ERROR;
    size_t size = 64;
    char buf[size];
    std::ifstream fin("/home/kira.noel/workdir/fwf-dev/generator/encoded.cp1047.strings.bin", std::ios::binary);
    std::string data;

    u_init(&u_glob_status);
    assert(U_SUCCESS(u_glob_status));
    assert(fin.is_open());

    while (fin.read(buf, size)) {
        data.append(buf, size);
    }
    fin.close();

    UErrorCode status = U_ZERO_ERROR;
    UCharsetDetector *ucd = ucsdet_open(&status);
    ucsdet_setText(ucd, data.c_str(), data.size(), &status);
    UCharsetMatch const *match = ucsdet_detect(ucd, &status);
    
    for (int i=0; i < ucnv_countAvailable(); i++) {
        printf("%s\n", ucnv_getAvailableName(i));
    }
    printf("Name: %s\n", ucsdet_getName(match, &status));
    printf("Lang: %s\n", ucsdet_getLanguage(match, &status));
    printf("Confidence: %u\n", ucsdet_getConfidence(match, &status));

    ucsdet_close(ucd);
    u_cleanup();
    return 0;
}
