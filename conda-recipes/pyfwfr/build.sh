#! /bin/bash
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the license.

set -e
set -x

echo "$(pwd)"
mkdir conda-build && cd conda-build
cmake -DCMAKE_INSTALL_PREFIX=$PREFIX ..
make install

cd ..
rm -r conda-build
cd bindings
$PYTHON setup.py \
    build \
    install
