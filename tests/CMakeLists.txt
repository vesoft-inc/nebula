# Copyright (c) 2020 vesoft inc. All rights reserved.
IF(NOT ${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})
    FIND_PROGRAM(PYTHON_EXECUTABLE python3
    /usr/bin /usr/local/bin
    NO_CMAKE_ENVIRONMENT_PATH
    NO_SYSTEM_ENVIRONMENT_PATH)
    FIND_PROGRAM(PYTHON_EXECUTABLE python)

    CONFIGURE_FILE(
        ${CMAKE_CURRENT_SOURCE_DIR}/ntr.out-of-source
        ${CMAKE_CURRENT_BINARY_DIR}/nebula-test-run.py
        @ONLY
    )
ENDIF()

EXECUTE_PROCESS(
  COMMAND chmod +x  nebula-test-run.py
  COMMAND ${CMAKE_COMMAND} -E create_symlink 
   ./nebula-test-run.py ntr
  COMMAND ${CMAKE_COMMAND} -E create_symlink 
   ./nebula-test-run.py nebula-test-run
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)