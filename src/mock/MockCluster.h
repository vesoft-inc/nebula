/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef MOCK_MOCKCLUSTER_H_
#define MOCK_MOCKCLUSTER_H_

#include "common/base/Base.h"
#include "common/clients/storage/GraphStorageClient.h"
#include "common/clients/storage/GeneralStorageClient.h"
#include "mock/RpcServer.h"
#include "kvstore/KVStore.h"
#include "kvstore/PartManager.h"
#include "kvstore/NebulaStore.h"
#include "storage/GraphStorageServiceHandler.h"
#include "storage/StorageAdminServiceHandler.h"
#include "storage/BaseProcessor.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include <thrift/lib/cpp/concurrency/ThreadManager.h>
#include <folly/synchronization/Baton.h>
#include <folly/executors/ThreadPoolExecutor.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <thrift/lib/cpp/concurrency/ThreadManager.h>

namespace nebula {
namespace mock {

class MockCluster {
public:
    void startAll();

    void startMeta(int32_t port, const std::string& rootPath, std::string hostname = "");

    void startStorage(HostAddr addr,
                      const std::string& rootPath,
                      bool isGeneralService = false,
                      SchemaVer schemaVerCount = 1);

    /**
     * Init a meta client connect to current meta server.
     * The meta server should be started before calling this method.
     * */
    void initMetaClient(meta::MetaClientOptions options = meta::MetaClientOptions());

    /*
     * Init a storage client connect to graphStorageServer
     * The meta server, and meta client must started first
     * */
    storage::GraphStorageClient* initGraphStorageClient();

    storage::GeneralStorageClient* initGeneralStorageClient();

    std::unique_ptr<meta::SchemaManager> memSchemaMan(SchemaVer schemaVerCount = 1,
                                                      GraphSpaceID spaceId = 1);

    std::unique_ptr<meta::IndexManager> memIndexMan(GraphSpaceID spaceId = 1);

    static void waitUntilAllElected(kvstore::NebulaStore* kvstore,
                                    GraphSpaceID spaceId,
                                    const std::vector<PartitionID>& partIds);

    static std::unique_ptr<kvstore::MemPartManager>
    memPartMan(GraphSpaceID spaceId, const std::vector<PartitionID>& parts);

    static std::unique_ptr<kvstore::NebulaStore>
    initKV(kvstore::KVOptions options, HostAddr localHost = HostAddr("", 0));

    static std::unique_ptr<kvstore::NebulaStore>
    initMetaKV(const char* dataPath, HostAddr localHost = HostAddr("", 0));

    void initStorageKV(const char* dataPath,
                       HostAddr localHost = HostAddr("", 0),
                       SchemaVer schemaVerCount = 1);

    static std::string localIP();

    int32_t getTotalParts() {
        return totalParts_;
    }

public:
    std::unique_ptr<RpcServer>                      metaServer_{nullptr};
    std::unique_ptr<meta::MetaClient>               metaClient_{nullptr};
    std::unique_ptr<storage::GraphStorageClient>    storageClient_{nullptr};
    std::unique_ptr<storage::GeneralStorageClient>  generalClient_{nullptr};
    std::unique_ptr<kvstore::NebulaStore>           metaKV_{nullptr};

    std::unique_ptr<RpcServer>                      storageAdminServer_{nullptr};
    std::unique_ptr<RpcServer>                      graphStorageServer_{nullptr};
    std::unique_ptr<RpcServer>                      generalStorageServer_{nullptr};
    std::unique_ptr<kvstore::NebulaStore>           storageKV_{nullptr};
    std::unique_ptr<storage::StorageEnv>            storageEnv_{nullptr};

    std::unique_ptr<meta::SchemaManager>            schemaMan_;
    std::unique_ptr<meta::IndexManager>             indexMan_;
    nebula::ClusterID                               clusterId_ = 10;
    int32_t                                         totalParts_;
};

}  // namespace mock
}  // namespace nebula

#endif  // MOCK_MOCKCLUSTER_H_
