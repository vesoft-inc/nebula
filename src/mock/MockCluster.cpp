/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "mock/MockCluster.h"
#include "mock/AdHocIndexManager.h"
#include "mock/AdHocSchemaManager.h"
#include "mock/MockData.h"
#include "meta/MetaServiceHandler.h"
#include "meta/ServerBasedSchemaManager.h"
#include "clients/meta/MetaClient.h"
#include "storage/StorageAdminServiceHandler.h"
#include "storage/GraphStorageServiceHandler.h"

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
                if (leader != HostAddr(0, 0)) {
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
std::unique_ptr<kvstore::MemPartManager>
MockCluster::memPartMan(GraphSpaceID spaceId, const std::vector<PartitionID>& parts) {
    auto memPartMan = std::make_unique<kvstore::MemPartManager>();
    // GraphSpaceID =>  {PartitionIDs}
    auto& partsMap = memPartMan->partsMap();
    for (auto partId : parts) {
        partsMap[spaceId][partId] = meta::PartHosts();
    }
    return memPartMan;
}

// static
IPv4 MockCluster::localIP() {
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    return localIp;
}

// static
std::unique_ptr<kvstore::NebulaStore>
MockCluster::initKV(kvstore::KVOptions options, HostAddr localHost) {
    auto ioPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
    auto workers = apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(
                             1, true /*stats*/);
    workers->setNamePrefix("executor");
    workers->start();
    if (localHost.ip == 0) {
        localHost.ip = localIP();
    }
    if (localHost.port == 0) {
        localHost.port = network::NetworkUtils::getAvailablePort();
    }
    auto store = std::make_unique<kvstore::NebulaStore>(std::move(options),
                                                        ioPool,
                                                        localHost,
                                                        workers);
    store->init();
    return store;
}

// static
std::unique_ptr<kvstore::NebulaStore>
MockCluster::initMetaKV(const char* dataPath, HostAddr addr) {
    kvstore::KVOptions options;
    options.partMan_ = memPartMan(0, {0});
    std::vector<std::string> paths;
    paths.emplace_back(folly::stringPrintf("%s/disk1", dataPath));
    options.dataPaths_ = std::move(paths);
    auto kv = initKV(std::move(options), addr);
    waitUntilAllElected(kv.get(), 0, {0});
    return kv;
}

void MockCluster::startMeta(int32_t port, const std::string& rootPath) {
    metaKV_ = initMetaKV(rootPath.c_str(), {0, port});
    metaServer_ = std::make_unique<RpcServer>();
    auto handler = std::make_shared<meta::MetaServiceHandler>(metaKV_.get(),
                                                              clusterId_);
    metaServer_->start("meta", port, handler);
    LOG(INFO) << "The Meta Daemon started on port " << metaServer_->port_;
}

void MockCluster::startStorage(HostAddr addr, const std::string& rootPath) {
    const std::vector<PartitionID> parts{1, 2, 3, 4, 5, 6};
    kvstore::KVOptions options;
    if (metaClient_ != nullptr) {
        LOG(INFO) << "Pull meta information from meta server";
        options.partMan_ = std::make_unique<kvstore::MetaServerBasedPartManager>(
                                            addr,
                                            metaClient_.get());
        schemaMan_ = meta::SchemaManager::create(metaClient_.get());
        indexMan_ = meta::IndexManager::create();
        indexMan_->init(metaClient_.get());
    } else {
        LOG(INFO) << "Use meta in memory!";
        options.partMan_ = memPartMan(1, parts);;
        indexMan_ = memIndexMan();
        schemaMan_ = memSchemaMan();
    }
    std::vector<std::string> paths;
    paths.emplace_back(folly::stringPrintf("%s/disk1", rootPath.c_str()));
    paths.emplace_back(folly::stringPrintf("%s/disk2", rootPath.c_str()));
    // Prepare KVStore
    options.dataPaths_ = std::move(paths);
    // options.cffBuilder_ = std::move(cffBuilder);
    storageKV_ = initKV(std::move(options), addr);
    waitUntilAllElected(storageKV_.get(), 1, parts);

    storageEnv_ = std::make_unique<storage::StorageEnv>();
    storageEnv_->indexMan_ = indexMan_.get();
    storageEnv_->schemaMan_ = schemaMan_.get();
    storageEnv_->kvstore_ = storageKV_.get();

    storageAdminServer_ = std::make_unique<RpcServer>();
    auto handler1 = std::make_shared<storage::StorageAdminServiceHandler>(storageEnv_.get());
    storageAdminServer_->start("admin-storage", addr.port, handler1);
    LOG(INFO) << "The admin storage daemon started on port " << storageAdminServer_->port_;

    graphStorageServer_ = std::make_unique<RpcServer>();
    auto handler2 = std::make_shared<storage::GraphStorageServiceHandler>(storageEnv_.get());
    auto port = addr.port == 0 ? addr.port : addr.port + 10;
    graphStorageServer_->start("graph-storage", port, handler2);
    LOG(INFO) << "The graph storage daemon started on port " << graphStorageServer_->port_;
}

std::unique_ptr<meta::SchemaManager>
MockCluster::memSchemaMan() {
    auto schemaMan = std::make_unique<AdHocSchemaManager>();
    // Vertex has two tags: players and teams
    // When tagId is 1, use players data
    schemaMan->addTagSchema(1, 1, MockData::mockPlayerTagSchema());
    // When tagId is 2, use teams data
    schemaMan->addTagSchema(1, 2, MockData::mockTeamTagSchema());

    // When edgeType is 101, use serve data
    schemaMan->addEdgeSchema(1, 101, MockData::mockEdgeSchema());
    return schemaMan;
}

std::unique_ptr<meta::IndexManager>
MockCluster::memIndexMan() {
    auto indexMan = std::make_unique<AdHocIndexManager>();
    return indexMan;
}

void MockCluster::initMetaClient(meta::MetaClientOptions options) {
    CHECK(metaServer_ != nullptr);
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto localhosts = std::vector<HostAddr>{HostAddr(localIP(), metaServer_->port_)};
    metaClient_ = std::make_unique<meta::MetaClient>(threadPool, localhosts, options);
    metaClient_->waitForMetadReady();
    LOG(INFO) << "Meta client has been ready!";
}

}  // namespace mock
}  // namespace nebula
