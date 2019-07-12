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
from Cython.Build import cythonize, build_ext

env = os.environ['CONDA_PREFIX']

ext_modules = [Extension(name='_fwfr', sources=['pyfwfr/_fwfr.pyx'])]

for ext in ext_modules:
    ext.include_dirs.append(np.get_include())
    ext.include_dirs.append(pa.get_include())
    ext.include_dirs.append(env + '/include')
    ext.include_dirs.append('pyfwfr/include')

    ext.library_dirs.extend(pa.get_library_dirs())
    ext.library_dirs.extend(['pyfwfr/lib', env + '/lib'])
    
    ext.libraries.extend(['fwfr', 'arrow', 'icui18n', 'icuuc', 'double-conversion'])

    ext.extra_compile_args.append('-w')

    if os.name == 'posix':
        ext.extra_compile_args.append('-std=c++11')

setup(
    name='pyfwfr',
    version='0.0.1',
    author='Kira Noel',
    description='An extension to Apache Arrow for reading fixed-width files into Arrow tables.',
    url='https://gitlab.com/stcdatascience/fwfr',
    ext_modules=cythonize(ext_modules),
    packages=find_packages(),
    build_ext=build_ext,
    install_requires=['pyarrow', 'numpy'],
    python_requires='>=3.5',
    package_data={
        '':['*.fwf', '*.so', '*.pxd', '*.h', '*.pyx'],
    },
    include_package_data=True,
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: complicated',
        'Operating System :: RHEL',
        'Development Status :: 3 - Alpha',
    ],
)
