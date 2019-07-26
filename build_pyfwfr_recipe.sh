#! /bin/bash
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the license.

set -e

if test $# -gt 0; then
    case "$1" in
        -h|--help)
            echo "./build_pyfwfr_recipe.sh [OPTIONS]"
            echo ""
            echo "Usage:"
            echo "-h, --help      show this menu"
            echo "-s, --simple    build conda recipe and store archive in repo"
            echo "-c, --channel   build local channel, index and archive"
            exit 0
            ;;
        -s|--simple)
            echo "Build conda recipe, store archive in repo"
            conda install -y conda-build
            conda build conda-recipes/pyfwfr
            mv $CONDA_PREFIX/conda-bld/linux-64/pyfwfr*.tar.bz2 ./
            exit 0
            ;;
        -c|--channel)
            echo "Build local channel, index and archive"
            mkdir -p local-channel/linux-64
            conda install -y conda-build
            conda build conda-recipes/pyfwfr
            mv $CONDA_PREFIX/conda-bld/linux-64/pyfwfr*.tar.bz2 ./local-channel/linux-64/
            conda index local-channel
            tar -zcf local-channel.tar.gz local-channel
            rm -r local-channel
            exit 0
            ;;
    esac
else
    echo "Access help menu with ./build_pyfwfr_recipe.sh -h"
fi
