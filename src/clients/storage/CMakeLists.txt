nebula_add_library(
    graph_storage_client_obj OBJECT
    GraphStorageClient.cpp
)


nebula_add_library(
    general_storage_client_obj OBJECT
    GeneralStorageClient.cpp
)


nebula_add_library(
    storage_client_gflags_obj OBJECT
    GFlags.cpp
)
