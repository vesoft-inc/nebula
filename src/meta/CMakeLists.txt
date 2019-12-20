nebula_add_library(
    schema_obj OBJECT
    SchemaManager.cpp
    ServerBasedSchemaManager.cpp
    NebulaSchemaProvider.cpp
)

nebula_add_library(
    meta_service_handler OBJECT
    MetaServiceHandler.cpp
    MetaServiceUtils.cpp
    ActiveHostsMan.cpp
    processors/partsMan/ListHostsProcessor.cpp
    processors/partsMan/ListPartsProcessor.cpp
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
    processors/admin/CreateSnapshotProcessor.cpp
    processors/admin/DropSnapshotProcessor.cpp
    processors/admin/ListSnapshotsProcessor.cpp
    processors/admin/Balancer.cpp
    processors/admin/BalancePlan.cpp
    processors/admin/BalanceTask.cpp
    processors/admin/AdminClient.cpp
    processors/admin/LeaderBalanceProcessor.cpp
    processors/admin/SnapShot.cpp
    processors/configMan/RegConfigProcessor.cpp
    processors/configMan/GetConfigProcessor.cpp
    processors/configMan/ListConfigsProcessor.cpp
    processors/configMan/SetConfigProcessor.cpp
)

nebula_add_library(
    meta_http_handler OBJECT
    MetaHttpIngestHandler.cpp
    MetaHttpDownloadHandler.cpp
    MetaHttpStatusHandler.cpp
)

nebula_add_library(
    meta_client OBJECT
    client/MetaClient.cpp
)

nebula_add_library(
    gflags_man_obj OBJECT
    GflagsManager.cpp
)

nebula_add_library(
    meta_gflags_man_obj OBJECT
    ClientBasedGflagsManager.cpp
)

nebula_add_subdirectory(test)
