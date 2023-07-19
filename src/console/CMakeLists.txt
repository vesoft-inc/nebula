# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

if(ENABLE_CONSOLE_COMPILATION)
  execute_process(
    COMMAND ${GIT_EXECUTABLE} symbolic-ref --short HEAD 
    OUTPUT_VARIABLE git_branch
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  )

  add_custom_command(
    OUTPUT ${CMAKE_BINARY_DIR}/nebula-console
    COMMAND bash ${CMAKE_SOURCE_DIR}/src/console/compile-console.sh ${git_branch} ${CMAKE_BINARY_DIR}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  )

  add_custom_target(GenerateConsole ALL
    DEPENDS ${CMAKE_BINARY_DIR}/nebula-console
  )

  add_dependencies(nebula-graphd GenerateConsole)

  install(
    FILES
        ${CMAKE_BINARY_DIR}/nebula-console
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        bin
    COMPONENT
        nebula-console
    OPTIONAL
)
endif()
