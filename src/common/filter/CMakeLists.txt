nebula_add_library(
        filter_obj
        OBJECT
        Expressions.cpp
        FunctionManager.cpp
)

nebula_add_subdirectory(test)
