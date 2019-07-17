/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "base/Base.h"
#include "meta/SchemaManager.h"
#include "DbDumper.h"

DEFINE_string(path_prefix, "", "when read data_path from config file, this is the prefix of the "
                                "path");
DEFINE_string(data_path, "", "Root data path, multi paths should be split by comma."
                             "For rocksdb engine, one path one instance.");
DEFINE_string(meta_server_addrs, "", "list of meta server addresses,"
                                     "the format looks like ip1:port1, ip2:port2, ip3:port3");

using nebula::meta::SchemaManager;

int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::FATAL);

    auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);

    // Meta client
    auto metaAddrsRet = nebula::network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
    if (!metaAddrsRet.ok() || metaAddrsRet.value().empty()) {
        LOG(ERROR) << "meta server's ip and port is wrong";
        return 0;
    }
    auto metaClient = std::make_unique<nebula::meta::MetaClient>(ioThreadPool,
                                                                 std::move(metaAddrsRet.value()));
    if (!metaClient->waitForMetadReady()) {
        LOG(ERROR) << "waitForMetadReady error!";
        return 0;
    }

    // schema manager init
    LOG(INFO) << "Init schema manager";
    auto schemaMan = nebula::meta::SchemaManager::create();
    schemaMan->init(metaClient.get());

    std::vector<std::string> paths;
    folly::split(",", FLAGS_data_path, paths, true);
    std::transform(paths.begin(), paths.end(), paths.begin(), [](auto& p) {
        return FLAGS_path_prefix+"/"+folly::trimWhitespace(p).str();
    });
    if (paths.empty()) {
         LOG(ERROR) << "Bad data_path format:" << FLAGS_data_path;
         return 0;
    }

    // dump data
    nebula::DbDumper dumper;
    dumper.init(schemaMan.get(), std::move(paths));
    dumper.dump();
    return 0;
}
