nebula_add_all_library(
    filter
    OBJECT
    Expressions.cpp
    FunctionManager.cpp
    geo/GeoFilter.cpp
    geo/GeoIndex.cpp
    geo/GeoParams.cpp
)

nebula_add_subdirectory(test)
