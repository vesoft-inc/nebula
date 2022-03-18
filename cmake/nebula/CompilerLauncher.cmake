option(ENABLE_COMPILER_LAUNCHER "Using compiler launcher if available" ON)
if(NOT ENABLE_COMPILER_LAUNCHER)
  return()
endif()

set(COMPILER_LAUNCHER_OPTION
    "ccache"
    CACHE STRING "Compiler cache to be used")
set(COMPILER_LAUNCHER_OPTION_VALUES "ccache" "sccache" "distcc")
set_property(CACHE COMPILER_LAUNCHER_OPTION PROPERTY STRINGS ${COMPILER_LAUNCHER_OPTION_VALUES})
list(
  FIND
  COMPILER_LAUNCHER_OPTION_VALUES
  ${COMPILER_LAUNCHER_OPTION}
  COMPILER_LAUNCHER_OPTION_INDEX)

if(${COMPILER_LAUNCHER_OPTION_INDEX} EQUAL -1)
  message(
    STATUS
      "Using custom compiler launcher system: '${COMPILER_LAUNCHER_OPTION}', explicitly supported entries are ${COMPILER_LAUNCHER_OPTION_VALUES}")
endif()

find_program(COMPILER_LAUNCHER_BINARY ${COMPILER_LAUNCHER_OPTION})
if(COMPILER_LAUNCHER_BINARY)
  message(STATUS "${COMPILER_LAUNCHER_OPTION} found and enabled")
  set(CMAKE_CXX_COMPILER_LAUNCHER ${COMPILER_LAUNCHER_BINARY})
else()
  message(WARNING "${COMPILER_LAUNCHER_OPTION} is enabled but was not found. Not using it")
endif()
