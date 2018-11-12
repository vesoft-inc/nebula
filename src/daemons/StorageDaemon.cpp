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
DEFINE_string(dataPath, "", "multi paths should be split by comma");


int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);

    using namespace nebula;
    using namespace nebula::storage;

    LOG(INFO) << "Starting the storage Daemon on port " << FLAGS_port
              << ", dataPath " << FLAGS_dataPath;

    std::vector<std::string> paths;
    folly::split(",", FLAGS_dataPath, paths, true);
    std::transform(paths.begin(), paths.end(), paths.begin(), [](auto& p) {
        return folly::trimWhitespace(p).str();
    });
    auto result = network::NetworkUtils::getLocalIP();
    CHECK(result.ok()) << result.status();
    uint32_t localIP;
    CHECK(network::NetworkUtils::ipv4ToInt(result.value(), localIP));

    std::unique_ptr<kvstore::KVStore> kvstore(
            kvstore::KVStore::instance(HostAddr(localIP, FLAGS_port), std::move(paths)));
    std::unique_ptr<meta::SchemaManager> schemaMan(meta::SchemaManager::instance());

    auto handler = std::make_shared<StorageServiceHandler>(kvstore.get(), schemaMan.get());
    auto server = std::make_shared<apache::thrift::ThriftServer>();
    CHECK(!!server) << "Failed to create the thrift server";

    server->setInterface(handler);
    server->setPort(FLAGS_port);

    server->serve();  // Will wait until the server shuts down

    LOG(INFO) << "The storage Daemon on port " << FLAGS_port << " stopped";
}

