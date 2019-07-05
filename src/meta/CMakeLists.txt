add_library(
    schema_obj OBJECT
    SchemaManager.cpp
    ServerBasedSchemaManager.cpp
    NebulaSchemaProvider.cpp
)
add_dependencies(
    schema_obj
    common_thrift_obj
    meta_thrift_obj
    base_obj)


add_library(
    meta_service_handler OBJECT
    MetaServiceHandler.cpp
    MetaServiceUtils.cpp
    ActiveHostsMan.cpp
    processors/partsMan/AddHostsProcessor.cpp
    processors/partsMan/ListHostsProcessor.cpp
    processors/partsMan/RemoveHostsProcessor.cpp
    processors/partsMan/CreateSpaceProcessor.cpp
    processors/partsMan/GetSpaceProcessor.cpp
    processors/partsMan/ListSpacesProcessor.cpp
    processors/partsMan/DropSpaceProcessor.cpp
    processors/partsMan/GetPartsAllocProcessor.cpp
    processors/schemaMan/CreateTagProcessor.cpp
    processors/schemaMan/AlterTagProcessor.cpp
    processors/schemaMan/GetTagProcessor.cpp
    processors/schemaMan/ListTagsProcessor.cpp
    processors/schemaMan/DropTagProcessor.cpp
    processors/schemaMan/CreateEdgeProcessor.cpp
    processors/schemaMan/AlterEdgeProcessor.cpp
    processors/schemaMan/GetEdgeProcessor.cpp
    processors/schemaMan/ListEdgesProcessor.cpp
    processors/schemaMan/DropEdgeProcessor.cpp
    processors/indexMan/CreateTagIndexProcessor.cpp
    processors/indexMan/DropTagIndexProcessor.cpp
    processors/indexMan/GetTagIndexProcessor.cpp
    processors/indexMan/ListTagIndexesProcessor.cpp
    processors/indexMan/CreateEdgeIndexProcessor.cpp
    processors/indexMan/DropEdgeIndexProcessor.cpp
    processors/indexMan/GetEdgeIndexProcessor.cpp
    processors/indexMan/ListEdgeIndexesProcessor.cpp
    processors/customKV/GetProcessor.cpp
    processors/customKV/MultiGetProcessor.cpp
    processors/customKV/MultiPutProcessor.cpp
    processors/customKV/RemoveProcessor.cpp
    processors/customKV/RemoveRangeProcessor.cpp
    processors/customKV/ScanProcessor.cpp
    processors/admin/HBProcessor.cpp
    processors/usersMan/AuthenticationProcessor.cpp
    processors/admin/BalanceProcessor.cpp
    processors/admin/Balancer.cpp
    processors/admin/BalancePlan.cpp
    processors/admin/BalanceTask.cpp
    processors/admin/AdminClient.cpp
)

add_dependencies(
    meta_service_handler
    meta_thrift_obj
    common_thrift_obj
    kvstore_obj
    thread_obj
    stats_obj
    base_obj
)

add_library(
    meta_http_handler OBJECT
    MetaHttpHandler.cpp
)
add_dependencies(
    meta_http_handler
    base_obj
)

add_library(
    meta_client OBJECT
    client/MetaClient.cpp
)
add_dependencies(
    meta_client
    meta_thrift_obj
    common_thrift_obj
    schema_obj
    base_obj)

add_subdirectory(test)
