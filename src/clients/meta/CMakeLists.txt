nebula_add_library(
    meta_client_obj OBJECT
    MetaClient.cpp
)

nebula_add_library(
    file_based_cluster_id_man_obj OBJECT
    FileBasedClusterIdMan.cpp
)

nebula_add_subdirectory(test)

