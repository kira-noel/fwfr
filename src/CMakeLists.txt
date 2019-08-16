# Gather dependencies and build the fwfr library

# Find fwfr's dependencies on the system
find_package (PkgConfig REQUIRED)

pkg_check_modules (ARROW REQUIRED arrow)
message (STATUS "\tarrow library - ${ARROW_LIBRARY_DIRS}")
message (STATUS "\tarrow include - ${ARROW_INCLUDE_DIRS}")

pkg_check_modules (ICUUC REQUIRED icu-uc)
message (STATUS "\ticu-uc library - ${ICUUC_LIBRARY_DIRS}")
message (STATUS "\ticu-uc include - ${ICUUC_INCLUDE_DIRS}")

pkg_check_modules (ICUI18N REQUIRED icu-i18n)
message (STATUS "\ticu-i18n library - ${ICUI18N_LIBRARY_DIRS}")
message (STATUS "\ticu-i18n include - ${ICUI18N_INCLUDE_DIRS}")

find_package (DoubleConversion REQUIRED)
message (STATUS "\tdouble-conversion library - ${DoubleConversion_LIB}")
message (STATUS "\tdouble-conversion include - ${DoubleConversion_INCLUDE_DIR}")

set (FWFR_INCLUDE_DIRS
     ${ARROW_INCLUDE_DIRS}
     ${ICUUC_INCLUDE_DIRS}
     ${ICUI18N_INCLUDE_DIRS}
     ${DoubleConversion_INCLUDE_DIR}
)
set (FWFR_LIBS 
     ${ARROW_LIBRARIES} 
     ${ICUUC_LIBRARIES} 
     ${ICUI18N_LIBRARIES} 
     ${DoubleConversion_LIB}
)
set (FWFR_LINK_DIRS
    ${ARROW_LIBRARY_DIRS}
    ${ICUUC_LIBRARY_DIRS}
    ${ICUI18N_LIBRARY_DIRS}
)

# Create fwfr library
include_directories (${PROJECT_SOURCE_DIR}/src  # source files
                     ${FWFR_INCLUDE_DIRS}
)
link_directories (${FWFR_LINK_DIRS})

file (GLOB FWFR_SOURCES "fwfr/*.cpp")
file (GLOB FWFR_HEADERS "fwfr/*.h")

add_library (fwfr SHARED ${FWFR_SOURCES})
target_link_libraries (fwfr ${FWFR_LIBS})

# Install
set (PY_INCLUDE_DEST ${PROJECT_SOURCE_DIR}/bindings/pyfwfr/include/fwfr)
set (LIB_DEST ${CMAKE_INSTALL_PREFIX}/lib)
set (INCLUDE_DEST ${CMAKE_INSTALL_PREFIX}/include/fwfr)

install (TARGETS fwfr DESTINATION ${LIB_DEST})
install (FILES ${FWFR_HEADERS} DESTINATION ${INCLUDE_DEST})

if (FWFR_PYTHON)
    install (FILES ${FWFR_HEADERS} DESTINATION ${PY_INCLUDE_DEST})
endif()
