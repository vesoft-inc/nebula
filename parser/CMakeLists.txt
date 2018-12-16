include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

bison_target(Parser parser.yy ${CMAKE_CURRENT_BINARY_DIR}/VGraphParser.cpp COMPILE_FLAGS "-Werror")
flex_target(Scanner scanner.lex ${CMAKE_CURRENT_BINARY_DIR}/VGraphScanner.cpp)

add_flex_bison_dependency(Scanner Parser)

add_library(
        parser_obj
        OBJECT
        ${FLEX_Scanner_OUTPUTS}
        ${BISON_Parser_OUTPUTS}
        Expressions.cpp
        Clauses.cpp
        CompoundSentence.cpp
        MaintainSentences.cpp
        MutateSentences.cpp
        TraverseSentences.cpp
)

add_dependencies(parser_obj base_obj)

add_subdirectory(test)
