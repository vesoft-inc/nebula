/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef MOCK_MOCKCLUSTER_H_
#define MOCK_MOCKCLUSTER_H_

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/ThreadPoolExecutor.h>
#include <folly/synchronization/Baton.h>
#include <thrift/lib/cpp/concurrency/ThreadManager.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>

#include "clients/storage/StorageClient.h"
#include "common/base/Base.h"
#include "common/base/ObjectPool.h"
#include "kvstore/KVStore.h"
#include "kvstore/NebulaStore.h"
#include "kvstore/PartManager.h"
#include "mock/LocalServer.h"
#include "mock/RpcServer.h"
#include "storage/BaseProcessor.h"
#include "storage/GraphStorageServiceHandler.h"
#include "storage/StorageAdminServiceHandler.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace mock {

class MockCluster {
 public:
  MockCluster() = default;

  ~MockCluster() {
    stop();
  }

  void startAll();

  void startMeta(const std::string& rootPath, HostAddr addr = HostAddr("127.0.0.1", 0));

  void startStorage(HostAddr addr, const std::string& rootPath, SchemaVer schemaVerCount = 1);

  /**
   * Init a meta client connect to current meta server.
   * The meta server should be started before calling this method.
   * */
  meta::MetaClient* initMetaClient(meta::MetaClientOptions options = meta::MetaClientOptions());

  /*
   * Init a storage client connect to graphStorageServer
   * The meta server, and meta client must started first
   * */
  storage::StorageClient* initGraphStorageClient();

  std::unique_ptr<meta::SchemaManager> memSchemaMan(SchemaVer schemaVerCount = 1,
                                                    GraphSpaceID spaceId = 1,
                                                    bool hasProp = true);

  std::unique_ptr<meta::IndexManager> memIndexMan(GraphSpaceID spaceId = 1, bool hasProp = true);

  static void waitUntilAllElected(kvstore::NebulaStore* kvstore,
                                  GraphSpaceID spaceId,
                                  const std::vector<PartitionID>& partIds);

  static std::unique_ptr<kvstore::MemPartManager> memPartMan(GraphSpaceID spaceId,
                                                             const std::vector<PartitionID>& parts);

  static std::unique_ptr<kvstore::NebulaStore> initKV(kvstore::KVOptions options,
                                                      HostAddr localHost = HostAddr("", 0));

  static std::unique_ptr<kvstore::NebulaStore> initMetaKV(const char* dataPath,
                                                          HostAddr localHost = HostAddr("", 0));

  void initStorageKV(const char* dataPath,
                     HostAddr localHost = HostAddr("", 0),
                     SchemaVer schemaVerCount = 1,
                     bool hasProp = true,
                     bool hasListener = false,
                     const std::vector<meta::cpp2::ServiceClient>& clients = {},
                     bool needCffBuilder = false);

  std::shared_ptr<apache::thrift::concurrency::PriorityThreadManager> getWorkers();

  void initListener(const char* dataPath, const HostAddr& addr);

  static std::string localIP();

  int32_t getTotalParts() {
    return totalParts_;
  }

  void stop() {
    if (storageKV_) {
      storageKV_->stop();
    }
    if (metaKV_) {
      metaKV_->stop();
    }

    storageAdminServer_.reset();
    graphStorageServer_.reset();

    if (metaClient_) {
      metaClient_->notifyStop();
      metaClient_->stop();
    }
    if (lMetaClient_) {
      metaClient_->notifyStop();
      lMetaClient_->stop();
    }
    if (esListener_) {
      esListener_->stop();
    }
  }

 public:
  std::unique_ptr<RpcServer> metaServer_{nullptr};
  std::unique_ptr<meta::MetaClient> metaClient_{nullptr};
  std::unique_ptr<storage::StorageClient> storageClient_{nullptr};
  std::unique_ptr<kvstore::NebulaStore> metaKV_{nullptr};

  std::unique_ptr<RpcServer> storageAdminServer_{nullptr};
#ifndef BUILD_STANDALONE
  std::unique_ptr<RpcServer> graphStorageServer_{nullptr};
#else
  std::unique_ptr<LocalServer> graphStorageServer_{nullptr};
#endif
  std::unique_ptr<kvstore::NebulaStore> storageKV_{nullptr};
  std::unique_ptr<storage::StorageEnv> storageEnv_{nullptr};

  std::unique_ptr<meta::SchemaManager> schemaMan_;
  std::unique_ptr<meta::IndexManager> indexMan_;
  nebula::ClusterID clusterId_ = 10;
  int32_t totalParts_;
  std::unique_ptr<kvstore::NebulaStore> esListener_{nullptr};
  std::unique_ptr<meta::SchemaManager> lSchemaMan_;
  std::unique_ptr<meta::MetaClient> lMetaClient_{nullptr};
  std::unique_ptr<storage::TransactionManager> txnMan_{nullptr};

  ObjectPool pool_;
};

}  // namespace mock
}  // namespace nebula

#endif  // MOCK_MOCKCLUSTER_H_
