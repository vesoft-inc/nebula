macro(nebula_add_executable)
    cmake_parse_arguments(
        nebula_exec                 # prefix
        ""                          # <options>
        "NAME"                      # <one_value_args>
        "SOURCES;OBJECTS;LIBRARIES" # <multi_value_args>
        ${ARGN}
    )
    add_executable(
        ${nebula_exec_NAME}
        ${nebula_exec_SOURCES}
        ${nebula_exec_OBJECTS}
    )
    nebula_link_libraries(
        ${nebula_exec_NAME}
        ${nebula_exec_LIBRARIES}
    )

    if(${nebula_exec_NAME} MATCHES "_test$")
        set_target_properties(
            ${nebula_exec_NAME}
            PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/test
        )
    elseif(${nebula_exec_NAME} MATCHES "_bm$")
        set_target_properties(
            ${nebula_exec_NAME}
            PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/bench
        )
    endif()
    if(TARGET common_project)
        add_dependencies(
            ${nebula_exec_NAME}
            common_project
        )
    endif()
endmacro()

macro(nebula_add_test)
    cmake_parse_arguments(
        nebula_test                 # prefix
        "DISABLED;FUZZER"           # <options>
        "NAME"                      # <one_value_args>
        "SOURCES;OBJECTS;LIBRARIES" # <multi_value_args>
        ${ARGN}
    )

    nebula_add_executable(
        NAME ${nebula_test_NAME}
        SOURCES ${nebula_test_SOURCES}
        OBJECTS ${nebula_test_OBJECTS}
        LIBRARIES ${nebula_test_LIBRARIES}
    )

    if (${nebula_test_FUZZER})
        #Currently only Clang supports fuzz test
        if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
            set_target_properties(${nebula_test_NAME} PROPERTIES COMPILE_FLAGS "-g -fsanitize=fuzzer")
            set_target_properties(${nebula_test_NAME} PROPERTIES LINK_FLAGS "-fsanitize=fuzzer")
        endif()
    elseif (NOT ${nebula_test_DISABLED})
        string(REGEX REPLACE "${CMAKE_SOURCE_DIR}/src/(.*)/test" "\\1" test_group ${CMAKE_CURRENT_SOURCE_DIR})
        add_test(NAME ${nebula_test_NAME} COMMAND ${nebula_test_NAME})
        set_tests_properties(${nebula_test_NAME} PROPERTIES LABELS ${test_group})
        # e.g. cmake -DNEBULA_ASAN_PRELOAD=/path/to/libasan.so
        # or,  cmake -DNEBULA_ASAN_PRELOAD=`/path/to/gcc --print-file-name=libasan.so`
        if (NEBULA_ASAN_PRELOAD)
            set_property(
                TEST ${nebula_test_NAME}
                PROPERTY ENVIRONMENT LD_PRELOAD=${NEBULA_ASAN_PRELOAD}
            )
        endif()
    endif()
endmacro()

# A wrapper for target_link_libraries()
macro(nebula_link_libraries target)
    target_link_libraries(
        ${target}
        ${ARGN}
        folly
        fmt
        glog
        gflags
        boost_context
        boost_system
        boost_regex
        boost_filesystem
        boost_program_options
        event
        double-conversion
        s2
        ${OPENSSL_SSL_LIBRARY}
        ${OPENSSL_CRYPTO_LIBRARY}
        ${KRB5_LIBRARIES}
        ${COMPRESSION_LIBRARIES}
        ${JEMALLOC_LIB}
        ${LIBUNWIND_LIBRARIES}
        keyutils
        resolv
        dl
        ${GETTIME_LIB}
        ${libatomic_link_flags}
        -pthread
        z
        ${COVERAGES}
    )
endmacro(nebula_link_libraries)

function(nebula_add_subdirectory dir_name)
    if ((NOT ENABLE_TESTING) AND (${dir_name} MATCHES test))
        add_subdirectory(${dir_name} EXCLUDE_FROM_ALL)
        return()
    endif()
    add_subdirectory(${dir_name})
endfunction()

macro(nebula_add_exe_linker_flag flag)
    string(FIND "${CMAKE_EXE_LINKER_FLAGS}" "${flag}" position)
    if(${position} EQUAL -1)
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${flag}")
    endif()
endmacro()

macro(nebula_remove_exe_linker_flag flag)
    string(REPLACE "${flag}" "" output "${CMAKE_EXE_LINKER_FLAGS}")
    set(CMAKE_EXE_LINKER_FLAGS "${output}")
endmacro()

macro(nebula_add_shared_linker_flag flag)
    string(FIND "${CMAKE_SHARED_LINKER_FLAGS}" "${flag}" position)
    if(${position} EQUAL -1)
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${flag}")
    endif()
endmacro()

macro(nebula_remove_shared_linker_flag flag)
    string(REPLACE "${flag}" "" output "${CMAKE_SHARED_LINKER_FLAGS}")
    set(CMAKE_SHARED_LINKER_FLAGS "${output}")
endmacro()

function(nebula_string_rpad output width fill_char value)
    string(LENGTH ${value} length)
    math(EXPR delta "${width} - ${length}")
    set(pads "")
    if(${delta} GREATER 0)
        math(EXPR delta "${delta} - 1")
        foreach(loop_var RANGE ${delta})
            set(pads "${pads}${fill_char}")
        endforeach()
    endif()
    set(${output} "${value}${pads}" PARENT_SCOPE)
endfunction()

macro(print_option name)
    get_property(helpstring CACHE ${name} PROPERTY HELPSTRING)
    nebula_string_rpad(padded 32 " " "${name}")
    string(FIND "${helpstring}" "No help" position)
    if(helpstring AND ${position} EQUAL -1)
        message(STATUS "${padded}: ${${name}} (${helpstring})")
    else()
        message(STATUS "${padded}: ${${name}}")
    endif()
endmacro()

macro(print_config name)
    print_option(${name})
endmacro()
