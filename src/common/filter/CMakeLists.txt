nebula_add_library(
    filter_obj
    OBJECT
    Expressions.cpp
    FunctionManager.cpp
    geo/GeoFilter.cpp
    geo/GeoIndex.cpp
)

nebula_add_subdirectory(test)
