#! /bin/bash
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the license.

set -e  # exit immediately on any error

if test $# -gt 0; then
    case "$1" in
        -h|--help)
            echo "./install [options]"
            echo ""
            echo "options:"
            echo "-h, --help        show help"
            echo "-c, --conda       use existing local channel to install pyfwfr"
            echo "-l, --local       build local conda channel and install pyfwfr from tar"
            echo "-s, --source      build from source"
            exit 0
            ;;
        -c|--conda)
            echo "Use existing local channel to install pyfwfr."
            if [ -z "$CONDA_PREFIX" ]; then
                echo "error - activate a conda environment"
                exit 1;
            fi
            if [ ! -f local-channel.tar.gz ]; then
                echo "local-channel.tar.gz not found"
                exit 1;
            fi

            # Downloading the archive from GitLab compresses it weirdly.
            { 
                tar -zxf local-channel.tar.gz &>/dev/null
            } || {
                gzip -d local-channel.tar.gz &&
                tar -zxf local-channel.tar
            }
            conda install -y pyfwfr -c file://$(pwd)/local-channel
            rm -r local-channel
            python -m unittest pyfwfr.tests.test_fwf -v
            exit 0
            ;;
        -l|--local)
            echo "Build local conda channel and install pyfwfr from tar."
            if [ -z "$CONDA_PREFIX" ]; then
                echo "error - activate a conda environment"
                exit 1;
            fi
            conda install -y conda-build
            mkdir -p local-channel/linux-64 && cp pyfwfr*.tar.bz2 local-channel/linux-64
            conda index local-channel
            conda install -y pyfwfr -c file://$(pwd)/local-channel
            rm -r local-channel
            conda remove -y conda-build
            python -m unittest pyfwfr.tests.test_fwf -v
            exit 0
            ;;
        -s|--source)
            echo "Building from source in current conda environment."
            if [ -z "$CONDA_PREFIX" ]; then
                echo "error - activate a conda environment"
                exit 1;
            fi
            conda install -y gcc_linux-64\>=5.1 gxx_linux-64\>=5.1 cmake\>=3.2 make \
                python=3.7 arrow-cpp\>=0.14 pyarrow\>=0.14 pkg-config cython\>=0.29 \
                -c conda-forge
            source $CONDA_PREFIX/../../bin/activate $CONDA_DEFAULT_ENV  # pull in new $CC $CXX
            mkdir -p build && cd build
            cmake \
                -DCMAKE_INSTALL_PREFIX=$CONDA_PREFIX \
                ..
            make install
            cd ..
            rm -r build
            cd bindings
            python setup.py \
                build \
                install
            #make wheel-quiet
            #pip install dist/pyfwfr*.whl
            make cleanest
            cd ..
            python -m unittest pyfwfr.tests.test_fwf -v
            exit 0
            ;;
    esac
else
    echo "Open help menu with ./install.sh -h"
fi
