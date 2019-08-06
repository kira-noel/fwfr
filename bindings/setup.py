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

env = os.environ['CONDA_PREFIX']
if env is "":
    raise Exception("CONDA_PREFIX environment variable not set")

ext_modules = [Extension(name='pyfwfr._fwfr', sources=['./pyfwfr/_fwfr.pyx'])]

for ext in ext_modules:
    ext.include_dirs.append(np.get_include())
    ext.include_dirs.append(pa.get_include())
    ext.include_dirs.append('./pyfwfr/include')
    ext.include_dirs.append(env + '/include')

    ext.library_dirs.extend([env + '/lib'])  # uses libfwfr.so rpath to find other libraries
    
    ext.libraries.extend(['fwfr'])  # including arrow, icui18n, icuuc, double-conversion

    ext.extra_compile_args.append('-w')  # quiet warnings

    # Set rpath
    ext.extra_link_args.append('-Wl,-rpath,$ORIGIN/../../..')

    # Included libraries use features from C++11
    if os.name == 'posix':
        ext.extra_compile_args.append('-std=c++11')

# Silence -Wstrict-prototypes warning
#
# build_ext bug where if $CC is not defined as g++, -Wstrict-prototypes is
# passed to the compiler. This compiler flag is not supported in C++. Remove
# flag manually to avoid warning.
class my_build_ext(build_ext):
    def build_extensions(self):
        if '-Wstrict-prototypes' in self.compiler.compiler_so:
            self.compiler.compiler_so.remove('-Wstrict-prototypes')
        super().build_extensions()

setup(
    name='pyfwfr',
    version='0.1',
    author='Kira Noel',
    description='An extension to Apache Arrow for reading fixed-width files into Arrow tables.',
    url='https://gitlab.k8s.cloud.statcan.ca/stcdatascience/fwfr',
    ext_modules=cythonize(ext_modules),
    cmdclass={'build_ext': my_build_ext},
    packages=find_packages(),
    python_requires='>=3.7,<3.8',
    package_data={
        '':['*.fwf', '*.bin', '*.so', '*.pxd', '*.h', '*.pyx'],
    },
    include_package_data=True,
)
