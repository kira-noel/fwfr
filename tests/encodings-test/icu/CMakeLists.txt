cmake_minimum_required (VERSION 3.1)

project (icu-conv)
set (VENV $ENV{CONDA_PREFIX})

find_package (PkgConfig REQUIRED)
pkg_check_modules (ICU-UC REQUIRED icu-uc)  # init and cleanup functions
pkg_check_modules (ICU-I18N REQUIRED icu-i18n)  # conversion functions

message ("icu library dirs: ${ICU-UC_LIBRARIES}")

include_directories (${ICU-UC_INCLUDE_DIRS} ${ICU-I18N_INCLUDE_DIRS})
link_directories (${ICU-UC_LIBRARY_DIRS} ${ICU-I18N_LIBRARY_DIRS})

set (LIBS ${ICU-UC_LIBRARIES} ${ICU-I18N_LIBRARIES})

add_executable (convert convert.cpp)
target_link_libraries (convert ${LIBS})
