# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

if(ENABLE_ERROR)
  if(ENABLE_VERBOSE_BISON)
      set(bison_flags "-Werror -v")
  else()
      set(bison_flags "-Werror")
  endif()
else()
  if(ENABLE_VERBOSE_BISON)
      set(bison_flags "-v")
  else()
      set(bison_flags "")
  endif()
endif()
bison_target(Parser datetime_parser.yy ${CMAKE_CURRENT_BINARY_DIR}/DatetimeParser.cpp COMPILE_FLAGS ${bison_flags})
flex_target(Scanner datetime_scanner.lex ${CMAKE_CURRENT_BINARY_DIR}/DatetimeScanner.cpp)

add_custom_target(datetime_parser_target DEPENDS ${FLEX_Scanner_OUTPUTS} ${BISON_Parser_OUTPUTS})

add_flex_bison_dependency(Scanner Parser)

add_compile_options(-Wno-sign-compare -Wno-conversion-null -Wno-pedantic -Wno-extra)

nebula_add_library(
    datetime_parser_obj OBJECT
    ${FLEX_Scanner_OUTPUTS}
    ${BISON_Parser_OUTPUTS}
    DatetimeReader.cpp
)

nebula_add_subdirectory(test)
