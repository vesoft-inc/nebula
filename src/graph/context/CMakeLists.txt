nebula_add_library(
    graph_context_obj OBJECT
    QueryContext.cpp
    QueryExpressionContext.cpp
    ExecutionContext.cpp
    Iterator.cpp
    Result.cpp
    Symbols.cpp
)


nebula_add_subdirectory(test)
