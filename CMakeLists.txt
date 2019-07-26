# To build and install the fixed-width file reader (FWFR) C++ libraries for Cython

cmake_minimum_required (VERSION 3.2)
project (libfwfr)

# PROJECT SETTINGS
if ("$ENV{CONDA_PREFIX}" STREQUAL "")
    message (FATAL_ERROR "CONDA_PREFIX not set. Exiting now...")
endif ()
set (VENV $ENV{CONDA_PREFIX})
# Include cmake helper modules
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake-modules")

# COMPILE SETTINGS
# Must use C++11
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
# Whether to build for Python - writes headers inside package, as well as in Conda env
option (FWFR_PYTHON "Whether to build for Python extension" ON)
if (FWFR_PYTHON)
    message (STATUS "Building for pyfwfr package, and C++.")
    set (CMAKE_INSTALL_RPATH "\$ORIGIN")
endif()

# BUILD FWFR
add_subdirectory (${PROJECT_SOURCE_DIR}/src)
