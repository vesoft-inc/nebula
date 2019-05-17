add_library(
        filter_obj
        OBJECT
        Expressions.cpp
        FunctionManager.cpp
)

add_dependencies(filter_obj base_obj)

add_subdirectory(test)
