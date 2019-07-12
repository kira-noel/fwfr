# To build and install the fixed-width file reader (FWFR) C++ libraries for Cython
cmake_minimum_required (VERSION 2.8)

# Project settings
project (fwf-lib)

set (VENV $ENV{CONDA_PREFIX})

# Must use C++11
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -g -fPIC")

### Build the fwfr library
add_subdirectory (${PROJECT_SOURCE_DIR}/src)
