/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "meta/MetaServiceHandler.h"
#include "meta/MetaHttpHandler.h"
#include "webservice/WebService.h"
#include "network/NetworkUtils.h"
#include "kvstore/PartManager.h"

DEFINE_int32(port, 45500, "Meta daemon listening port");
DEFINE_string(data_path, "", "Root data path");
DEFINE_string(peers, "", "It is a list of IPs split by comma,"
                         "the ips number equals replica number."
                         "If empty, it means replica is 1");
DEFINE_string(local_ip, "", "Local ip speicified for NetworkUtils::getLocalIP");
DECLARE_string(part_man_type);

namespace nebula {

std::vector<HostAddr> toHosts(const std::string& peersStr) {
    std::vector<HostAddr> hosts;
    std::vector<std::string> peers;
    folly::split(",", peersStr, peers, true);
    std::transform(peers.begin(), peers.end(), hosts.begin(), [](auto& p) {
        return network::NetworkUtils::toHostAddr(folly::trimWhitespace(p));
    });
    return hosts;
}

}  // namespace nebula


int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);

    LOG(INFO) << "Starting Meta HTTP Service";
    nebula::WebService::registerHandler("/meta", [] {
        return new nebula::meta::MetaHttpHandler();
    });
    auto status = nebula::WebService::start();
    if (!status.ok()) {
        LOG(ERROR) << "Failed to start web service: " << status;
        return EXIT_FAILURE;
    }

    LOG(INFO) << "Starting the meta Daemon on port " << FLAGS_port;
    auto result = nebula::network::NetworkUtils::getLocalIP(FLAGS_local_ip);
    CHECK(result.ok()) << result.status();
    uint32_t localIP;
    CHECK(nebula::network::NetworkUtils::ipv4ToInt(result.value(), localIP));

    CHECK_EQ("memory", FLAGS_part_man_type);
    nebula::kvstore::MemPartManager* partMan
        = reinterpret_cast<nebula::kvstore::MemPartManager*>(
                nebula::kvstore::PartManager::instance());
    // The meta server has only one space, one part.
    partMan->addPart(0, 0, nebula::toHosts(FLAGS_peers));

    nebula::kvstore::KVOptions options;
    options.local_ = nebula::HostAddr(localIP, FLAGS_port);
    options.dataPaths_ = {FLAGS_data_path};
    std::unique_ptr<nebula::kvstore::KVStore> kvstore(
            nebula::kvstore::KVStore::instance(std::move(options)));

    auto handler = std::make_shared<nebula::meta::MetaServiceHandler>(kvstore.get());

    auto server = std::make_shared<apache::thrift::ThriftServer>();
    CHECK(!!server) << "Failed to create the thrift server";

    server->setInterface(handler);
    server->setPort(FLAGS_port);

    server->serve();  // Will wait until the server shuts down

    LOG(INFO) << "The storage Daemon on port " << FLAGS_port << " stopped";
}
