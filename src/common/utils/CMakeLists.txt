# The base_obj is a basic library. Other libraries are not allowed to depend on it.

nebula_add_library(
    keyutils_obj OBJECT
    NebulaKeyUtils.cpp
    IndexKeyUtils.cpp
    OperationKeyUtils.cpp
)

add_dependencies(module_common
    keyutils_obj)

nebula_add_subdirectory(test)
