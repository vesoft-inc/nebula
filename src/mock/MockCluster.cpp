/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "mock/MockCluster.h"

#include "clients/meta/MetaClient.h"
#include "common/meta/ServerBasedIndexManager.h"
#include "common/meta/ServerBasedSchemaManager.h"
#include "meta/MetaServiceHandler.h"
#include "mock/AdHocIndexManager.h"
#include "mock/AdHocSchemaManager.h"
#include "mock/MockData.h"
#include "storage/CompactionFilter.h"
#include "storage/GraphStorageServiceHandler.h"
#include "storage/StorageAdminServiceHandler.h"
#include "storage/transaction/TransactionManager.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace mock {

// static
void MockCluster::waitUntilAllElected(kvstore::NebulaStore* kvstore,
                                      GraphSpaceID spaceId,
                                      const std::vector<PartitionID>& partIds) {
  while (true) {
    size_t readyNum = 0;
    for (auto partId : partIds) {
      auto retLeader = kvstore->partLeader(spaceId, partId);
      if (ok(retLeader)) {
        auto leader = value(std::move(retLeader));
        if (leader != HostAddr("", 0)) {
          readyNum++;
        }
      }
    }
    if (readyNum == partIds.size()) {
      LOG(INFO) << "All leaders have been elected!";
      break;
    }
    usleep(100000);
  }
}

// static
std::unique_ptr<kvstore::MemPartManager> MockCluster::memPartMan(
    GraphSpaceID spaceId, const std::vector<PartitionID>& parts) {
  auto memPartMan = std::make_unique<kvstore::MemPartManager>();
  // GraphSpaceID =>  {PartitionIDs}
  auto& partsMap = memPartMan->partsMap();
  for (auto partId : parts) {
    partsMap[spaceId][partId] = meta::PartHosts();
  }
  return memPartMan;
}

// static
std::string MockCluster::localIP() {
  return "127.0.0.1";
}

// static
std::unique_ptr<kvstore::NebulaStore> MockCluster::initKV(kvstore::KVOptions options,
                                                          HostAddr localHost) {
  auto ioPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
  auto workers = apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(1);
  workers->setNamePrefix("executor");
  workers->start();
  if (localHost.host == 0) {
    localHost.host = localIP();
  }
  if (localHost.port == 0) {
    localHost.port = network::NetworkUtils::getAvailablePort();
  }
  auto store =
      std::make_unique<kvstore::NebulaStore>(std::move(options), ioPool, localHost, workers);
  store->init();
  return store;
}

// static
std::unique_ptr<kvstore::NebulaStore> MockCluster::initMetaKV(const char* dataPath, HostAddr addr) {
  kvstore::KVOptions options;
  options.partMan_ = memPartMan(0, {0});
  std::vector<std::string> paths;
  paths.emplace_back(folly::stringPrintf("%s/disk1", dataPath));
  options.dataPaths_ = std::move(paths);
  auto kv = initKV(std::move(options), addr);
  waitUntilAllElected(kv.get(), 0, {0});
  return kv;
}

void MockCluster::startMeta(const std::string& rootPath, HostAddr addr) {
  metaKV_ = initMetaKV(rootPath.c_str(), addr);
  metaServer_ = std::make_unique<RpcServer>();
  auto handler = std::make_shared<meta::MetaServiceHandler>(metaKV_.get(), clusterId_);
  // initKV would replace hostname and port if necessary, so we need to change
  // to the real addr
  metaServer_->start("meta", metaKV_->address().port, handler);
  LOG(INFO) << "The Meta Daemon started on port " << metaServer_->port_;
}

std::shared_ptr<apache::thrift::concurrency::PriorityThreadManager> MockCluster::getWorkers() {
  auto worker = apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(1);
  worker->setNamePrefix("executor");
  worker->start();
  return worker;
}

void MockCluster::initListener(const char* dataPath, const HostAddr& addr) {
  CHECK(metaServer_ != nullptr);
  auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
  auto localhosts = std::vector<HostAddr>{HostAddr(localIP(), metaServer_->port_)};
  lMetaClient_ =
      std::make_unique<meta::MetaClient>(threadPool, localhosts, meta::MetaClientOptions());
  lMetaClient_->waitForMetadReady();
  LOG(INFO) << "Listener meta client has been ready!";
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
  kvstore::KVOptions KVOpt;
  KVOpt.listenerPath_ = folly::stringPrintf("%s/listener", dataPath);
  KVOpt.partMan_ = std::make_unique<kvstore::MetaServerBasedPartManager>(addr, lMetaClient_.get());
  lSchemaMan_ = meta::ServerBasedSchemaManager::create(lMetaClient_.get());
  KVOpt.schemaMan_ = lSchemaMan_.get();
  esListener_ =
      std::make_unique<kvstore::NebulaStore>(std::move(KVOpt), ioThreadPool, addr, getWorkers());
  esListener_->init();
  sleep(FLAGS_heartbeat_interval_secs + 1);
}

void MockCluster::initStorageKV(const char* dataPath,
                                HostAddr addr,
                                SchemaVer schemaVerCount,
                                bool hasProp,
                                bool hasListener,
                                const std::vector<meta::cpp2::ServiceClient>& clients,
                                bool needCffBuilder) {
  FLAGS_heartbeat_interval_secs = 1;
  const std::vector<PartitionID> parts{1, 2, 3, 4, 5, 6};
  totalParts_ = 6;  // don't not delete this...
  kvstore::KVOptions options;
  if (metaClient_ != nullptr) {
    LOG(INFO) << "Pull meta information from meta server";
    nebula::meta::cpp2::SpaceDesc spaceDesc;
    spaceDesc.space_name_ref() = "test_space";
    spaceDesc.partition_num_ref() = 6;
    spaceDesc.replica_factor_ref() = 1;
    spaceDesc.charset_name_ref() = "utf8";
    spaceDesc.collate_name_ref() = "utf8_bin";
    meta::cpp2::ColumnTypeDef type;
    type.type_ref() = nebula::cpp2::PropertyType::FIXED_STRING;
    type.type_length_ref() = 32;
    spaceDesc.vid_type_ref() = std::move(type);
    auto ret = metaClient_->createSpace(spaceDesc).get();
    if (!ret.ok()) {
      LOG(FATAL) << "can't create space";
    }
    GraphSpaceID spaceId = ret.value();
    LOG(INFO) << "spaceId = " << spaceId;
    if (hasListener) {
      HostAddr listenerHost(localIP(), network::NetworkUtils::getAvailablePort());
      ret =
          metaClient_->addListener(spaceId, meta::cpp2::ListenerType::ELASTICSEARCH, {listenerHost})
              .get();
      if (!ret.ok()) {
        LOG(FATAL) << "listener init failed";
      }
      if (clients.empty()) {
        LOG(FATAL) << "full text client list is empty";
      }
      ret =
          metaClient_->signInService(meta::cpp2::ExternalServiceType::ELASTICSEARCH, clients).get();
      if (!ret.ok()) {
        LOG(FATAL) << "full text client sign in failed";
      }
      sleep(FLAGS_heartbeat_interval_secs + 1);
      options.listenerPath_ = folly::stringPrintf("%s/listener", dataPath);
      initListener(dataPath, {std::move(listenerHost)});
    }
    options.partMan_ =
        std::make_unique<kvstore::MetaServerBasedPartManager>(addr, metaClient_.get());
    schemaMan_ = meta::ServerBasedSchemaManager::create(metaClient_.get());
    indexMan_ = meta::ServerBasedIndexManager::create(metaClient_.get());
  } else {
    LOG(INFO) << "Use meta in memory!";
    schemaMan_ = memSchemaMan(schemaVerCount, 1, hasProp);
    indexMan_ = memIndexMan(1, hasProp);
    options.partMan_ = memPartMan(1, parts);
    options.schemaMan_ = schemaMan_.get();
  }
  std::vector<std::string> paths;
  paths.emplace_back(folly::stringPrintf("%s/disk1", dataPath));
  paths.emplace_back(folly::stringPrintf("%s/disk2", dataPath));

  // Prepare KVStore
  options.dataPaths_ = std::move(paths);
  if (needCffBuilder) {
    std::unique_ptr<kvstore::CompactionFilterFactoryBuilder> cffBuilder(
        new storage::StorageCompactionFilterFactoryBuilder(schemaMan_.get(), indexMan_.get()));
    options.cffBuilder_ = std::move(cffBuilder);
  }
  storageKV_ = initKV(std::move(options), addr);
  waitUntilAllElected(storageKV_.get(), 1, parts);

  storageEnv_ = std::make_unique<storage::StorageEnv>();
  storageEnv_->schemaMan_ = schemaMan_.get();
  storageEnv_->indexMan_ = indexMan_.get();
  storageEnv_->kvstore_ = storageKV_.get();
  storageEnv_->rebuildIndexGuard_ = std::make_unique<storage::IndexGuard>();
  storageEnv_->verticesML_ = std::make_unique<storage::VerticesMemLock>();
  storageEnv_->edgesML_ = std::make_unique<storage::EdgesMemLock>();

  txnMan_ = std::make_unique<storage::TransactionManager>(storageEnv_.get());
  storageEnv_->txnMan_ = txnMan_.get();
  txnMan_->start();
}

void MockCluster::startStorage(HostAddr addr,
                               const std::string& rootPath,
                               SchemaVer schemaVerCount) {
  initStorageKV(rootPath.c_str(), addr, schemaVerCount);

  auto* env = storageEnv_.get();
  storageAdminServer_ = std::make_unique<RpcServer>();
  auto adminHandler = std::make_shared<storage::StorageAdminServiceHandler>(env);
  storageAdminServer_->start("admin-storage", addr.port - 1, adminHandler);
  LOG(INFO) << "The admin storage daemon started on port " << storageAdminServer_->port_;

#ifndef BUILD_STANDALONE
  graphStorageServer_ = std::make_unique<RpcServer>();
  auto graphHandler = std::make_shared<storage::GraphStorageServiceHandler>(env);
  graphStorageServer_->start("graph-storage", addr.port, graphHandler);
  LOG(INFO) << "The graph storage daemon started on port " << graphStorageServer_->port_;
#else
  graphStorageServer_ = std::make_unique<LocalServer>();
  auto graphHandler = std::make_shared<storage::GraphStorageServiceHandler>(env);
  graphStorageServer_->start("graph-storage", addr.port, graphHandler);
  LOG(INFO) << "The graph storage daemon started on Local server.";
#endif
}

std::unique_ptr<meta::SchemaManager> MockCluster::memSchemaMan(SchemaVer schemaVerCount,
                                                               GraphSpaceID spaceId,
                                                               bool hasProp) {
  auto schemaMan = std::make_unique<AdHocSchemaManager>(6);
  // if have multi version schema, need to add from oldest to newest
  for (SchemaVer ver = 0; ver < schemaVerCount; ver++) {
    // Vertex has two tags: players and teams
    // When tagId is 1, use players data
    schemaMan->addTagSchema(spaceId, 1, MockData::mockPlayerTagSchema(&pool_, ver, hasProp));
    // When tagId is 2, use teams data
    schemaMan->addTagSchema(spaceId, 2, MockData::mockTeamTagSchema(ver, hasProp));

    // Edge has two type: serve and teammate
    // When edgeType is 101, use serve data
    schemaMan->addEdgeSchema(spaceId, 101, MockData::mockServeEdgeSchema(&pool_, ver, hasProp));
    // When edgeType is 102, use teammate data
    schemaMan->addEdgeSchema(spaceId, 102, MockData::mockTeammateEdgeSchema(ver, hasProp));
  }

  schemaMan->addTagSchema(spaceId, 3, MockData::mockGeneralTagSchemaV1());

  schemaMan->addTagSchema(spaceId, 3, MockData::mockGeneralTagSchemaV2());

  return schemaMan;
}

std::unique_ptr<meta::IndexManager> MockCluster::memIndexMan(GraphSpaceID spaceId, bool hasProp) {
  auto indexMan = std::make_unique<AdHocIndexManager>();
  if (hasProp) {
    indexMan->addTagIndex(spaceId, 1, 1, MockData::mockPlayerTagIndexColumns());
    indexMan->addTagIndex(spaceId, 2, 2, MockData::mockTeamTagIndexColumns());
    indexMan->addTagIndex(spaceId, 3, 3, MockData::mockGeneralTagIndexColumns());
    indexMan->addEdgeIndex(spaceId, 101, 101, MockData::mockServeEdgeIndexColumns());
    indexMan->addEdgeIndex(spaceId, 102, 102, MockData::mockTeammateEdgeIndexColumns());
  }

  indexMan->addTagIndex(spaceId, 1, 4, {});
  indexMan->addTagIndex(spaceId, 2, 5, {});
  indexMan->addEdgeIndex(spaceId, 101, 103, {});
  indexMan->addEdgeIndex(spaceId, 102, 104, {});
  return indexMan;
}

meta::MetaClient* MockCluster::initMetaClient(meta::MetaClientOptions options) {
  CHECK(metaServer_ != nullptr);
  auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
  auto localhosts = std::vector<HostAddr>{HostAddr(localIP(), metaServer_->port_)};
  metaClient_ = std::make_unique<meta::MetaClient>(threadPool, localhosts, options);
  metaClient_->waitForMetadReady();
  LOG(INFO) << "Meta client has been ready!";
  return metaClient_.get();
}

storage::StorageClient* MockCluster::initGraphStorageClient() {
  auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
  storageClient_ = std::make_unique<storage::StorageClient>(threadPool, metaClient_.get());
  return storageClient_.get();
}

}  // namespace mock
}  // namespace nebula
