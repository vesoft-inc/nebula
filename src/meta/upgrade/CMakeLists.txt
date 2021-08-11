nebula_add_library(
    meta_data_upgrade_obj OBJECT
    MetaDataUpgrade.cpp
    thrift/MetaServiceUtilsV1.cpp
)

add_dependencies(
    meta_data_upgrade_obj
    old_meta_thrift_obj
)

add_subdirectory(thrift)
