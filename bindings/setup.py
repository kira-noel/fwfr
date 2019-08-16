#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the license.
import os
import numpy as np
import pyarrow as pa

from setuptools import setup, Extension, find_packages
from setuptools.command.build_ext import build_ext
from Cython.Build import cythonize


def main():
    env = os.environ['CONDA_PREFIX']
    if env == "":
        raise Exception("CONDA_PREFIX environment variable not set")

    # Configure extension
    ext = Extension(name='pyfwfr._fwfr', sources=['./pyfwfr/_fwfr.pyx'])
    ext.include_dirs.append(np.get_include())
    ext.include_dirs.append(pa.get_include())
    ext.include_dirs.append('./pyfwfr/include')
    ext.include_dirs.append(env + '/include')
    ext.library_dirs.extend([env + '/lib'])
    ext.libraries.extend(['fwfr'])
    ext.extra_compile_args.append('-w')  # quiet warnings

    # Set rpath
    ext.extra_link_args.append('-Wl,-rpath,$ORIGIN/../../..')

    # Included libraries use features from C++11
    if os.name == 'posix':
        ext.extra_compile_args.append('-std=c++11')

    setup(
        name='pyfwfr',
        version='0.1',
        author='Kira Noel',
        description='Module to read fixed-width files into Arrow tables.',
        url='https://gitlab.k8s.cloud.statcan.ca/stcdatascience/fwfr',
        ext_modules=cythonize([ext]),
        cmdclass={'build_ext': build_ext_},
        packages=find_packages(),
        python_requires='>=3.7,<3.8',
        package_data={
            '': ['includes/*.pxd', 'include/fwfr/*.h', '*.pyx'],
        },
        include_package_data=True,
    )


class build_ext_(build_ext):
    """
    Silence -Wstrict-prototypes warning

    build_ext bug where if $CC is not defined as g++, -Wstrict-prototypes is
    passed to the compiler. This compiler flag is not supported in C++. Remove
    flag manually to avoid warning.
    """
    def build_extensions(self):
        if '-Wstrict-prototypes' in self.compiler.compiler_so:
            self.compiler.compiler_so.remove('-Wstrict-prototypes')
        super().build_extensions()


if __name__ == '__main__':
    main()
