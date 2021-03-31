set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

print_config(CMAKE_CXX_STANDARD)
print_config(CMAKE_CXX_COMPILER)
print_config(CMAKE_CXX_COMPILER_ID)

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

if(ENABLE_PIC)
    add_compile_options(-fPIC)
endif()

if(ENABLE_TESTING AND ENABLE_COVERAGE)
    add_compile_options(--coverage)
    add_compile_options(-g)
    add_compile_options(-O0)
endif()

# TODO(doodle) Add option suggest-override for gnu
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    # Here we need to specify the path of libstdc++ used by Clang
    if(NOT ${NEBULA_CLANG_USE_GCC_TOOLCHAIN} STREQUAL "")
        print_config(NEBULA_CLANG_USE_GCC_TOOLCHAIN)
        execute_process(
            COMMAND ${NEBULA_CLANG_USE_GCC_TOOLCHAIN}/bin/gcc -dumpmachine
            OUTPUT_VARIABLE gcc_target_triplet
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        add_compile_options("--gcc-toolchain=${NEBULA_CLANG_USE_GCC_TOOLCHAIN}")
        nebula_add_exe_linker_flag(--gcc-toolchain=${NEBULA_CLANG_USE_GCC_TOOLCHAIN})
        nebula_add_shared_linker_flag(--gcc-toolchain=${NEBULA_CLANG_USE_GCC_TOOLCHAIN})
        if(NOT "${gcc_target_triplet}" STREQUAL "")
            add_compile_options(--target=${gcc_target_triplet})
            nebula_add_exe_linker_flag(--target=${gcc_target_triplet})
            nebula_add_shared_linker_flag(--target=${gcc_target_triplet})
        endif()
    endif()
#add_compile_options(-Wno-mismatched-tags)
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
