/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "meta/MetaServiceHandler.h"
#include "network/NetworkUtils.h"

DEFINE_int32(port, 45500, "Meta daemon listening port");
DEFINE_string(data_path, "", "Root data path");
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

    LOG(INFO) << "Starting the meta Daemon on port " << FLAGS_port;
    auto result = nebula::getLocalIP();
    CHECK(result.ok()) << result.status();
    uint32_t localIP;
    CHECK(nebula::network::NetworkUtils::ipv4ToInt(result.value(), localIP));
    std::unique_ptr<nebula::kvstore::KVStore> kvstore;
    nebula::kvstore::KVOptions options;
    options.local_ = nebula::HostAddr(localIP, FLAGS_port);
    options.dataPaths_ = {FLAGS_data_path};
    kvstore.reset(nebula::kvstore::KVStore::instance(std::move(options)));

    auto handler = std::make_shared<nebula::meta::MetaServiceHandler>(kvstore.get());

    auto server = std::make_shared<apache::thrift::ThriftServer>();
    CHECK(!!server) << "Failed to create the thrift server";

    server->setInterface(handler);
    server->setPort(FLAGS_port);

    server->serve();  // Will wait until the server shuts down

    LOG(INFO) << "The storage Daemon on port " << FLAGS_port << " stopped";
}

