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
        Statement.cpp
        MaintainSentences.cpp
        MutateSentences.cpp
        TraverseSentences.cpp
)

target_include_directories(parser_obj SYSTEM BEFORE PUBLIC ${FLEX_INCLUDE_DIRS})

add_dependencies(parser_obj base_obj)

add_subdirectory(test)
