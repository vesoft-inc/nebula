# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

if(NOT ${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})
    find_program(PYTHON_EXECUTABLE python3
        /usr/bin /usr/local/bin
        NO_CMAKE_ENVIRONMENT_PATH
        NO_SYSTEM_ENVIRONMENT_PATH
    )
    find_program(PYTHON_EXECUTABLE python)

    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/ntr.out-of-source
        ${CMAKE_CURRENT_BINARY_DIR}/nebula-test-run.py
        @ONLY
    )
endif()

execute_process(
    COMMAND chmod +x nebula-test-run.py
    COMMAND ${CMAKE_COMMAND} -E create_symlink ./nebula-test-run.py ntr
    COMMAND ${CMAKE_COMMAND} -E create_symlink ./nebula-test-run.py nebula-test-run
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)
