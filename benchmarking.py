#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the license.

import pyfwfr as pf
from pyfwfr.tests.common import *

import argparse
import numpy as np
import timeit
import sys

parser = argparse.ArgumentParser(description='Run benchmarks for pyfwfr.')
parser.add_argument('-n', '--n', default=5, type=int, help='repetitions')
parser.add_argument('-c', '--cols', default=10, type=int, help='number of columns')
parser.add_argument('-r', '--rows', default=100000, type=int, help='number of rows')
parser.add_argument('-e', '--encoding', default='utf8', type=str, help='encoding')

def run_benchmark(num_cols, num_rows, encoding='utf8'):
    fwf, expected = make_random_fwf(num_cols, num_rows, encoding=encoding)
 
    field_widths = []
    for i in range(num_cols):
        field_widths.append(4)
    parse_options = pf.ParseOptions(field_widths)

    if encoding != 'utf8':
        read_options = pf.ReadOptions(encoding=encoding)
    else:
        read_options = pf.ReadOptions()

    start = timeit.default_timer()
    table = read_bytes(fwf, parse_options, read_options=read_options)
    elapsed = timeit.default_timer() - start

    assert table.schema == expected.schema
    assert table.equals(expected)

    return elapsed, sys.getsizeof(fwf)/1000000

# Run benchmark n times, combine stats
def run_n_benchmark(num_cols, num_rows, n, encoding='utf8'):
    times = np.empty((n))
    sizes = np.empty((n))
    mb_s = np.empty((n))

    for i in range(n):
        time, size = run_benchmark(num_cols, num_rows, encoding) 
        times[i] = time
        sizes[i] = size
        mb_s[i] = size/time

    return times, sizes, mb_s

if __name__ == '__main__':
    args = parser.parse_args()
    times, sizes, mb_s = run_n_benchmark(
            args.cols, args.rows, args.n, encoding=args.encoding)
    
    print("%d rows, %d columns, %d total fields" %(args.rows, args.cols, args.rows*args.cols))
    print("mean: %f MB/s, standard deviation: %f" %(np.mean(mb_s), np.std(mb_s)))
    print("mean: %f MB, standard deviation: %f" %(np.mean(sizes), np.std(sizes)))
    print("mean: %f s, standard deviation: %f" %(np.mean(times), np.std(times)))
