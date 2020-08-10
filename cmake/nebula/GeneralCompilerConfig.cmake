set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if (!CMAKE_CXX_COMPILER)
    message(FATAL_ERROR "No C++ compiler found")
endif()

include(CheckCXXCompilerFlag)

add_compile_options(-Wall)
add_compile_options(-Wextra)
add_compile_options(-Wpedantic)
add_compile_options(-Wunused-parameter)
add_compile_options(-Wshadow)
add_compile_options(-Wnon-virtual-dtor)
add_compile_options(-Woverloaded-virtual)
add_compile_options(-Wignored-qualifiers)

add_definitions(-DS2_USE_GLOG)

include_directories(AFTER ${CMAKE_SOURCE_DIR}/src)
include_directories(AFTER ${CMAKE_CURRENT_BINARY_DIR}/src)


if(ENABLE_WERROR)
    add_compile_options(-Werror)
endif()

if(NOT ENABLE_STRICT_ALIASING)
    add_compile_options(-fno-strict-aliasing)
else()
    add_compile_options(-fstrict-aliasing)
endif()

if(ENABLE_FRAME_POINTER)
    add_compile_options(-fno-omit-frame-pointer)
else()
    add_compile_options(-fomit-frame-pointer)
endif()

if(NOT ${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    add_definitions(-D_FORTIFY_SOURCE=2)
else()
    if (NOT ${NEBULA_USE_LINKER} STREQUAL "gold")
        # gold linker is buggy for `--gc-sections', disabled for now
        add_compile_options(-ffunction-sections)
        add_compile_options(-fdata-sections)
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--gc-sections")
    endif()
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--compress-debug-sections=zlib")
    if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        add_compile_options(-fno-limit-debug-info)
    endif()
endif()

if(ENABLE_TESTING AND ENABLE_COVERAGE)
    add_compile_options(--coverage)
    add_compile_options(-g)
    add_compile_options(-O0)
endif()

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    add_compile_options(-Wsuggest-override)
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    MESSAGE(STATUS "NEBULA_CLANG_USE_GCC_TOOLCHAIN: ${NEBULA_CLANG_USE_GCC_TOOLCHAIN}")
    # Here we need to specify the path of libstdc++ used by Clang
    if(NOT ${NEBULA_CLANG_USE_GCC_TOOLCHAIN} STREQUAL "")
        execute_process(
            COMMAND ${NEBULA_CLANG_USE_GCC_TOOLCHAIN}/bin/gcc -dumpmachine
            OUTPUT_VARIABLE gcc_target_triplet
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        add_compile_options("--gcc-toolchain=${NEBULA_CLANG_USE_GCC_TOOLCHAIN}")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --gcc-toolchain=${NEBULA_CLANG_USE_GCC_TOOLCHAIN}")
        if(NOT "${gcc_target_triplet}" STREQUAL "")
            add_compile_options(--target=${gcc_target_triplet})
            set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --target=${gcc_target_triplet}")
        endif()
    endif()
    add_compile_options(-Wno-self-assign-overloaded)
    add_compile_options(-Wno-self-move)
    add_compile_options(-Wno-format-pedantic)
    add_compile_options(-Wno-gnu-zero-variadic-macro-arguments)
    set(libatomic_link_flags "-Wl,-Bstatic -latomic -Wl,-Bdynamic")
    set(CMAKE_REQUIRED_FLAGS "${libatomic_link_flags}")
    check_cxx_compiler_flag("${libatomic_link_flags}" has_static_libatomic)
    if(NOT has_static_libatomic)
        set(libatomic_link_flags "-latomic")
    endif()
endif()
