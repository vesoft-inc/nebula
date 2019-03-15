/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "network/NetworkUtils.h"
#include "storage/StorageServiceHandler.h"
#include "storage/StorageHttpHandler.h"
#include "kvstore/KVStore.h"
#include "kvstore/PartManager.h"
#include "storage/test/TestUtils.h"
#include "webservice/WebService.h"

DEFINE_int32(port, 44500, "Storage daemon listening port");
DEFINE_string(data_path, "", "Root data path, multi paths should be split by comma."
                             "For rocksdb engine, one path one instance.");
DEFINE_string(local_ip, "", "Local ip");
DEFINE_bool(mock_server, true, "start mock server");

// Get local IPv4 address. You could specify it by set FLAGS_local_ip, otherwise
// it will use the first ip exclude "127.0.0.1"
namespace nebula {

StatusOr<std::string> getLocalIP() {
    if (!FLAGS_local_ip.empty()) {
        return FLAGS_local_ip;
    }
    auto result = network::NetworkUtils::listDeviceAndIPv4s();
    if (!result.ok()) {
        return std::move(result).status();
    }
    for (auto& deviceIP : result.value()) {
        if (deviceIP.second != "127.0.0.1") {
            return deviceIP.second;
        }
    }
    return Status::Error("No IPv4 address found!");
}

}  // namespace nebula

int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    using nebula::HostAddr;
    using nebula::storage::StorageServiceHandler;
    using nebula::kvstore::KVStore;
    using nebula::meta::SchemaManager;
    using nebula::network::NetworkUtils;
    using nebula::getLocalIP;

    LOG(INFO) << "Starting the storage Daemon on port " << FLAGS_port
              << ", dataPath " << FLAGS_data_path;

    std::vector<std::string> paths;
    folly::split(",", FLAGS_data_path, paths, true);
    std::transform(paths.begin(), paths.end(), paths.begin(), [](auto& p) {
        return folly::trimWhitespace(p).str();
    });
    auto result = getLocalIP();
    CHECK(result.ok()) << result.status();
    uint32_t localIP;
    CHECK(NetworkUtils::ipv4ToInt(result.value(), localIP));
    if (FLAGS_mock_server) {
        nebula::kvstore::MemPartManager* partMan
            = reinterpret_cast<nebula::kvstore::MemPartManager*>(
                    nebula::kvstore::PartManager::instance());
        // GraphSpaceID =>  {PartitionIDs}
        // 0 => {0, 1, 2, 3, 4, 5}
        for (auto partId = 0; partId < 6; partId++) {
            partMan->addPart(0, partId);
        }
        nebula::meta::AdHocSchemaManager::addEdgeSchema(
           0 /*space id*/, 101 /*edge type*/,
           nebula::storage::TestUtils::genEdgeSchemaProvider(10, 10));
        for (auto tagId = 3001; tagId < 3010; tagId++) {
            nebula::meta::AdHocSchemaManager::addTagSchema(
               0 /*space id*/, tagId,
               nebula::storage::TestUtils::genTagSchemaProvider(tagId, 3, 3));
        }
    }
    std::unique_ptr<KVStore> kvstore;
    nebula::kvstore::KVOptions options;
    options.local_ = HostAddr(localIP, FLAGS_port);
    options.dataPaths_ = std::move(paths);
    kvstore.reset(KVStore::instance(std::move(options)));

    LOG(INFO) << "Starting Storage HTTP Service";
    nebula::WebService::registerHandler("/storage", [] {
        return new nebula::storage::StorageHttpHandler();
    });
    nebula::WebService::start();

    auto handler = std::make_shared<StorageServiceHandler>(kvstore.get());
    auto server = std::make_shared<apache::thrift::ThriftServer>();
    CHECK(!!server) << "Failed to create the thrift server";

    server->setInterface(handler);
    server->setPort(FLAGS_port);

    server->serve();  // Will wait until the server shuts down

    LOG(INFO) << "The storage Daemon on port " << FLAGS_port << " stopped";
}

