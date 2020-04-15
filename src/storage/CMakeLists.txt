nebula_add_library(
    storage_common_obj OBJECT
    StorageFlags.cpp
    CommonUtils.cpp
)

nebula_add_library(
    storage_admin_service_handler OBJECT
    StorageAdminServiceHandler.cpp
    admin/CreateCheckpointProcessor.cpp
    admin/DropCheckpointProcessor.cpp
    admin/SendBlockSignProcessor.cpp
    # admin/RebuildTagIndexProcessor.cpp
    # admin/RebuildEdgeIndexProcessor.cpp
)

nebula_add_library(
    graph_storage_service_handler OBJECT
    GraphStorageServiceHandler.cpp
)


nebula_add_library(
    storage_http_handler OBJECT
    http/StorageHttpIngestHandler.cpp
    http/StorageHttpDownloadHandler.cpp
    http/StorageHttpAdminHandler.cpp
)

nebula_add_library(
    storage_server OBJECT
    StorageServer.cpp
)

# nebula_add_subdirectory(test)
