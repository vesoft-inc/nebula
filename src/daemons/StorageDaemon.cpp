/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "network/NetworkUtils.h"
#include "storage/StorageServiceHandler.h"
#include "kvstore/include/KVStore.h"
#include "meta/SchemaManager.h"

DEFINE_int32(port, 44500, "Storage daemon listening port");
DEFINE_string(data_path, "", "Root data path, multi paths should be split by comma."
                             "For rocksdb engine, one path one instance.");
DEFINE_string(local_ip, "", "Local ip");

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
            return deviceIP.first;
        }
    }
    return Status::Error("No IPv4 address found!");
}

}  // namespace nebula

int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);

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

    std::unique_ptr<KVStore> kvstore;
    kvstore.reset(KVStore::instance(HostAddr(localIP, FLAGS_port), std::move(paths)));
    std::unique_ptr<SchemaManager> schemaMan(SchemaManager::instance());

    auto handler = std::make_shared<StorageServiceHandler>(kvstore.get(), schemaMan.get());
    auto server = std::make_shared<apache::thrift::ThriftServer>();
    CHECK(!!server) << "Failed to create the thrift server";

    server->setInterface(handler);
    server->setPort(FLAGS_port);

    server->serve();  // Will wait until the server shuts down

    LOG(INFO) << "The storage Daemon on port " << FLAGS_port << " stopped";
}

