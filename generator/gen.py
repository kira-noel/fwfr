#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the license.
import itertools
import string

import ebcdic

from faker import Faker
import numpy as np
import pandas as pd
from tabulate import tabulate

num_cols = 4
num_records = 10
max_val = 9999

outfile = 'data.strings'

def gen_col_names():
    letters = string.ascii_lowercase
    for first in letters:
        for second in letters:
            yield first + second

def gen_data():
    data = []
    fake = Faker()
    
    for i_record in range(num_records):
        data.append([])
        for i_col in range(num_cols):
            if i_col is 0:
                data[i_record].append(fake.name())
            elif i_col is 1:
                data[i_record].append(fake.job())
            elif i_col is 2:
                data[i_record].append(fake.phone_number())
            else:
                data[i_record].append(fake.date())
    return data

def to_fwf(df, fname):
    content = tabulate(df.values.tolist(), list(df.columns), tablefmt='plain')
    open(fname, 'w').write(content)

def gen_fwf(fname, data='gen'):
    if data is 'gen':
        df = pd.DataFrame.from_records(gen_data(), columns=list(itertools.islice(gen_col_names(), num_cols)))
    else:
        df = pd.DataFrame(np.random.randint(max_val, size=(num_records, num_cols)), columns=list(itertools.islice(gen_col_names(), num_cols)))
    to_fwf(df, 'data.' + fname + '.fwf')

def encode_file(fname, encoding, cur_encoding='utf8'):
    with open('data.' + fname + '.fwf', 'rb') as fin:
        with open('encoded.' + encoding + '.' + fname + '.bin', 'wb') as fout:
            for line in fin:
                fout.write((line.decode(cur_encoding)).encode(encoding))

def decode_file(fname, encoding):
    with open('encoded.' + encoding + '.' + fname + '.bin', 'rb') as f_encoded:
        with open('decoded.' + encoding + '.' + fname + '.bin', 'w') as f_decoded:
            for line in f_encoded:
                f_decoded.write(line.decode(encoding))

if __name__ == '__main__':
    gen_fwf('bigstrings', 'num')
    encode_file('bigstrings', 'cp1047')
    #decode_file('bigstrings', 'cp1047')
