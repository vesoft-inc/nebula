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
DEFINE_string(local_ip, "", "Local ip speicified for NetworkUtils::getLocalIP");
DEFINE_bool(mock_server, true, "start mock server");


int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    using nebula::HostAddr;
    using nebula::storage::StorageServiceHandler;
    using nebula::kvstore::KVStore;
    using nebula::meta::SchemaManager;
    using nebula::network::NetworkUtils;

    if (FLAGS_data_path.empty()) {
        LOG(FATAL) << "Storage Data Path should not empty";
        return -1;
    }
    LOG(INFO) << "Starting the storage Daemon on port " << FLAGS_port
              << ", dataPath " << FLAGS_data_path;

    std::vector<std::string> paths;
    folly::split(",", FLAGS_data_path, paths, true);
    std::transform(paths.begin(), paths.end(), paths.begin(), [](auto& p) {
        return folly::trimWhitespace(p).str();
    });
    auto result = nebula::network::NetworkUtils::getLocalIP(FLAGS_local_ip);
    CHECK(result.ok()) << result.status();
    uint32_t localIP;
    CHECK(NetworkUtils::ipv4ToInt(result.value(), localIP));

    nebula::kvstore::KVOptions options;
    options.local_ = HostAddr(localIP, FLAGS_port);
    options.dataPaths_ = std::move(paths);
    options.partMan_
        = std::make_unique<nebula::kvstore::MetaServerBasedPartManager>(options.local_);
    std::unique_ptr<nebula::kvstore::KVStore> kvstore(
            nebula::kvstore::KVStore::instance(std::move(options)));

    LOG(INFO) << "Starting Storage HTTP Service";
    nebula::WebService::registerHandler("/storage", [] {
        return new nebula::storage::StorageHttpHandler();
    });

    auto status = nebula::WebService::start();
    if (!status.ok()) {
        LOG(ERROR) << "Failed to start web service: " << status;
        return EXIT_FAILURE;
    }

    auto handler = std::make_shared<StorageServiceHandler>(kvstore.get());
    auto server = std::make_shared<apache::thrift::ThriftServer>();
    CHECK(!!server) << "Failed to create the thrift server";

    server->setInterface(handler);
    server->setPort(FLAGS_port);

    server->serve();  // Will wait until the server shuts down

    LOG(INFO) << "The storage Daemon on port " << FLAGS_port << " stopped";
}

