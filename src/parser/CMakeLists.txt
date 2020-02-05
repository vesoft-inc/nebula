include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

bison_target(Parser parser.yy ${CMAKE_CURRENT_BINARY_DIR}/GraphParser.cpp COMPILE_FLAGS "-Werror")
flex_target(Scanner scanner.lex ${CMAKE_CURRENT_BINARY_DIR}/GraphScanner.cpp)

add_custom_target(parser_target DEPENDS ${FLEX_Scanner_OUTPUTS} ${BISON_Parser_OUTPUTS})

add_flex_bison_dependency(Scanner Parser)

add_compile_options(-Wno-sign-compare -Wno-conversion-null -Wno-pedantic -Wno-extra)

nebula_add_library(
    parser_obj OBJECT
    ${FLEX_Scanner_OUTPUTS}
    ${BISON_Parser_OUTPUTS}
    Clauses.cpp
    SequentialSentences.cpp
    MaintainSentences.cpp
    TraverseSentences.cpp
    MutateSentences.cpp
    AdminSentences.cpp
    UserSentences.cpp
    ProcessControlSentences.cpp
)

nebula_add_subdirectory(test)
