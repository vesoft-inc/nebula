# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

if(ENABLE_VERBOSE_BISON)
    set(bison_flags "-Werror -v")
else()
    set(bison_flags "-Werror")
endif()
bison_target(Parser wkt/wkt_parser.yy ${CMAKE_CURRENT_BINARY_DIR}/wkt/WKTParser.cpp COMPILE_FLAGS ${bison_flags})
flex_target(Scanner wkt/wkt_scanner.lex ${CMAKE_CURRENT_BINARY_DIR}/wkt/WKTScanner.cpp)

add_custom_target(wkt_parser_target DEPENDS ${FLEX_Scanner_OUTPUTS} ${BISON_Parser_OUTPUTS})

add_flex_bison_dependency(Scanner Parser)

add_compile_options(-Wno-sign-compare -Wno-conversion-null -Wno-pedantic -Wno-extra)

nebula_add_library(
    wkt_wkb_io_obj OBJECT
    ${FLEX_Scanner_OUTPUTS}
    ${BISON_Parser_OUTPUTS}
    wkt/WKTReader.cpp
    wkt/WKTWriter.cpp
    wkb/WKBReader.cpp
    wkb/WKBWriter.cpp
)

nebula_add_subdirectory(wkt)
# nebula_add_subdirectory(wkb)
