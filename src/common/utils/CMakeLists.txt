nebula_add_library(
    kvstore_storage_utils_obj OBJECT
    NebulaKeyUtils.cpp
)

nebula_add_library(
    convert_time_type_utils_obj OBJECT
    ConvertTimeType.cpp
)

nebula_add_subdirectory(test)
