nebula_add_library(
    storage_service_handler OBJECT
    StorageServiceHandler.cpp
    StorageFlags.cpp
    CommonUtils.cpp
    query/QueryBaseProcessor.cpp
    query/QueryBoundProcessor.cpp
    query/QueryVertexPropsProcessor.cpp
    query/QueryEdgePropsProcessor.cpp
    query/QueryStatsProcessor.cpp
    query/ScanEdgeProcessor.cpp
    query/ScanVertexProcessor.cpp
    mutate/AddVerticesProcessor.cpp
    mutate/AddEdgesProcessor.cpp
    mutate/DeleteEdgesProcessor.cpp
    mutate/DeleteVerticesProcessor.cpp
    mutate/UpdateVertexProcessor.cpp
    mutate/UpdateEdgeProcessor.cpp
    kv/PutProcessor.cpp
    kv/GetProcessor.cpp
    admin/CreateCheckpointProcessor.cpp
    admin/DropCheckpointProcessor.cpp
    admin/SendBlockSignProcessor.cpp
    admin/RebuildTagIndexProcessor.cpp
    admin/RebuildEdgeIndexProcessor.cpp
    index/IndexPolicyMaker.cpp
    index/IndexExecutor.cpp
    index/LookUpIndexProcessor.cpp
)

nebula_add_library(
    storage_http_handler OBJECT
    http/StorageHttpIngestHandler.cpp
    http/StorageHttpDownloadHandler.cpp
    http/StorageHttpAdminHandler.cpp
    http/SysInfoHandler.cpp
)

nebula_add_library(
    storage_client OBJECT
    client/StorageClient.cpp
)

nebula_add_library(
    storage_server OBJECT
    StorageServer.cpp
)

nebula_add_subdirectory(test)
