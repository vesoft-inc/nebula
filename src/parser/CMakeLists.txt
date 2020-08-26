# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

if(ENABLE_VERBOSE_BISON)
    set(bison_flags "-Werror -v")
else()
    set(bison_flags "-Werror")
endif()
bison_target(Parser parser.yy ${CMAKE_CURRENT_BINARY_DIR}/GraphParser.cpp COMPILE_FLAGS ${bison_flags})
flex_target(Scanner scanner.lex ${CMAKE_CURRENT_BINARY_DIR}/GraphScanner.cpp)

add_custom_target(parser_target DEPENDS ${FLEX_Scanner_OUTPUTS} ${BISON_Parser_OUTPUTS})

add_flex_bison_dependency(Scanner Parser)

add_compile_options(-Wno-sign-compare -Wno-conversion-null -Wno-pedantic -Wno-extra)

nebula_add_library(
    parser_obj OBJECT
    ${FLEX_Scanner_OUTPUTS}
    ${BISON_Parser_OUTPUTS}
    Clauses.cpp
    EdgeKey.cpp
    SequentialSentences.cpp
    MaintainSentences.cpp
    TraverseSentences.cpp
    MutateSentences.cpp
    AdminSentences.cpp
    UserSentences.cpp
    ProcessControlSentences.cpp
    ExplainSentence.cpp
    MatchSentence.cpp
)

nebula_add_subdirectory(test)
