add_library(
        filter_obj
        OBJECT
        Expressions.cpp
        FunctionManager.cpp
)
nebula_add_base_obj(filter_obj)

add_subdirectory(test)
