cmake_minimum_required(VERSION 2.8)

project(cpp-test)

set(VENV $ENV{CONDA_PREFIX})

find_package (PkgConfig REQUIRED)
pkg_check_modules (ICUUC REQUIRED icu-uc)
pkg_check_modules (ICUI18N REQUIRED icu-i18n)

find_library (FWFR_LIB fwfr PATHS ${PROJECT_SOURCE_DIR}/../../lib NO_DEFAULT_PATH)
set (FWFR_INCLUDE_DIRS ${PROJECT_SOURCE_DIR}/../../include)

find_library (ARROW_LIB arrow PATHS ${VENV}/lib/python3.7/site-packages/pyarrow NO_DEFAULT_PATH)
set (ARROW_INCLUDE_DIRS ${VENV}/lib/python3.7/site-packages/pyarrow/include)

set (ICU_INCLUDE_DIRS ${ICUUC_INCLUDE_DIRS} ${ICUU18N_INCLUDE_DIRS})

find_library (DOUBLE_CONVERSION_LIB double-conversion PATHS ${VENV}/lib)
set (DOUBLE_CONVERSION_INCLUDE_DIRS ${VENV}/include)

find_library (ICUUC_LIB icuuc PATHS ${ICUUC_LIBRARY_DIRS})
find_library (ICUI18N_LIB icui18n PATHS ${ICUI18N_LIBRARY_DIRS})
#set (ICU_LIBS ${ICUI18N_LIB} ${ICUUC_LIB})

include_directories (${ARROW_INCLUDE_DIRS} ${ICU_INCLUDE_DIRS} ${DOUBLE_CONVERSION_INCLUDE_DIRS} ${FWFR_INCLUDE_DIRS})
link_directories (${ICUUC_LIBRARY_DIRS})

set(LIBS ${FWFR_LIB} ${ARROW_LIB} ${ICU_LIBS} ${DOUBLE_CONVERSION_LIB})
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -g")

add_executable(run ${PROJECT_SOURCE_DIR}/src/test.cpp)
target_link_libraries(run ${LIBS})
