nebula_add_library(
        filter_obj
        OBJECT
        Expressions.cpp
        FunctionManager.cpp
)

add_subdirectory(test)
