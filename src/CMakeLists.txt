set (VENV $ENV{CONDA_PREFIX})

find_package (PkgConfig REQUIRED)
pkg_check_modules (ICUUC REQUIRED icu-uc)
pkg_check_modules (ICUI18N REQUIRED icu-i18n)
set (ICU_INCLUDE_DIRS ${ICUUC_INCLUDE_DIRS} ${ICUI18N_INCLUDE_DIRS})

find_library (DOUBLE_CONVERSION_LIB double-conversion PATHS ${VENV}/lib)
set (DOUBLE_CONVERSION_INCLUDE_DIRS ${VENV}/include)

find_library (ARROW_LIB arrow PATHS ${VENV}/lib/python3.7/site-packages/pyarrow NO_DEFAULT_PATH)
set (ARROW_INCLUDE_DIRS ${VENV}/lib/python3.7/site-packages/pyarrow/include)

include_directories (${ARROW_INCLUDE_DIRS} ${ICU_INCLUDE_DIRS} ${DOUBLE_CONVERSION_INCLUDE_DIRS})
link_directories (${ICUI18N_LIBRARY_DIRS} ${ICUUC_LIBRARY_DIRS})

set (LIBS ${ARROW_LIB} ${ICUI18N_LIBRARIES} ${ICUUC_LIBRARIES} ${DOUBLE_CONVERSION_LIB})

include_directories (${PROJECT_SOURCE_DIR}/src)

file (GLOB FWFR_SOURCES "fwfr/*.cpp")
add_library (fwfr SHARED ${FWFR_SOURCES})

#target_link_libraries (fwfr ${LIBS})

# Install
install (TARGETS fwfr DESTINATION ${VENV}/lib)
install (TARGETS fwfr DESTINATION ${PROJECT_SOURCE_DIR}/lib)
install (TARGETS fwfr DESTINATION ${PROJECT_SOURCE_DIR}/bindings/pyfwfr)
file (GLOB FWFR_HEADERS "fwfr/*.h")
install (FILES ${FWFR_HEADERS} DESTINATION ${VENV}/include/fwfr)
install (FILES ${FWFR_HEADERS} DESTINATION ${PROJECT_SOURCE_DIR}/include/fwfr)
install (FILES ${FWFR_HEADERS} DESTINATION ${PROJECT_SOURCE_DIR}/bindings/pyfwfr/include/fwfr)
