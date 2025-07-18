cmake_minimum_required(VERSION 3.10)
project(emu-jobtap LANGUAGES C)

# Core settings
set(CMAKE_C_STANDARD 99)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)


# Options for system vs local Flux Core
option(USE_SYSTEM_FLUX "Link against a system-installed flux-core" ON)
set(FLUX_INSTALL_PREFIX "" CACHE PATH
    "Prefix where flux-core is installed (e.g., /opt/flux)")

# Prepare search paths for system install
set(_SYS_INCLUDE_PATHS /usr/include /usr/local/include)
set(_SYS_LIB_PATHS     /usr/lib     /usr/local/lib)
if(FLUX_INSTALL_PREFIX)
    list(APPEND _SYS_INCLUDE_PATHS "${FLUX_INSTALL_PREFIX}/include")
    list(APPEND _SYS_LIB_PATHS     "${FLUX_INSTALL_PREFIX}/lib")
endif()

# Try to find system install
if(USE_SYSTEM_FLUX)
    find_path(FLUX_INCLUDE_DIR
        NAMES flux/core.h
        PATHS ${_SYS_INCLUDE_PATHS}
        NO_DEFAULT_PATH)
    find_library(FLUX_CORE_LIBRARY
        NAMES flux-core
        PATHS ${_SYS_LIB_PATHS}
        NO_DEFAULT_PATH)
    if(FLUX_INCLUDE_DIR AND FLUX_CORE_LIBRARY)
        message(STATUS "Using system flux-core: include=${FLUX_INCLUDE_DIR}, lib=${FLUX_CORE_LIBRARY}")
    else()
        message(WARNING "Cannot find system flux-core; falling back to local clone")
        set(USE_SYSTEM_FLUX OFF CACHE BOOL "Link against a system-installed flux-core" FORCE)
    endif()
endif()

# Fallback to local clone
if(NOT USE_SYSTEM_FLUX)
    set(FLUXCLONE_ROOT "${CMAKE_SOURCE_DIR}/../flux-core")
    find_path(FLUX_INCLUDE_DIR
        NAMES flux/core.h
        PATHS "${FLUXCLONE_ROOT}/src/include")
    find_library(FLUX_CORE_LIBRARY
        NAMES flux-core
        PATHS "${FLUXCLONE_ROOT}/src/.libs")
    if(NOT FLUX_INCLUDE_DIR OR NOT FLUX_CORE_LIBRARY)
        message(FATAL_ERROR
            "flux-core not found in local clone at ${FLUXCLONE_ROOT}."
            " Install system flux-core or adjust FLUX_INSTALL_PREFIX/USE_SYSTEM_FLUX.")
    endif()
    message(STATUS "Using local flux-core from ${FLUXCLONE_ROOT}")
endif()

# Derive lib directory and include/link
get_filename_component(FLUX_LIB_DIR ${FLUX_CORE_LIBRARY} DIRECTORY)
include_directories(${FLUX_INCLUDE_DIR})
link_directories(${FLUX_LIB_DIR})

# Build the jobtap plugin
set(JOBTAP_SRC_DIR "${CMAKE_SOURCE_DIR}/src/jobtap")
add_library(emu_jobtap SHARED
    ${JOBTAP_SRC_DIR}/emu-jobtap.c)
target_compile_options(emu_jobtap PRIVATE -Wall -Wextra)
target_link_libraries(emu_jobtap PRIVATE flux-core)


# Embed RPATH for local clone builds only
if(NOT USE_SYSTEM_FLUX)
    set_target_properties(emu_jobtap PROPERTIES
        BUILD_WITH_INSTALL_RPATH TRUE
        INSTALL_RPATH "$ORIGIN:${FLUX_LIB_DIR}"
        INSTALL_RPATH_USE_LINK_PATH TRUE)
endif()

# Name output "emu-jobtap.so"
set_target_properties(emu_jobtap PROPERTIES
    OUTPUT_NAME emu-jobtap
    PREFIX "")


# Optional install rules
install(TARGETS emu_jobtap
        LIBRARY DESTINATION lib/emu-jobtap)
