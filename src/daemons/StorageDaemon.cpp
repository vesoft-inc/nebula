/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "network/NetworkUtils.h"
#include "storage/StorageServiceHandler.h"
#include "storage/StorageHttpHandler.h"
#include "kvstore/KVStore.h"
#include "kvstore/PartManager.h"
#include "process/ProcessUtils.h"
#include "storage/test/TestUtils.h"
#include "webservice/WebService.h"
#include "meta/SchemaManager.h"
#include "meta/client/MetaClient.h"

DEFINE_int32(port, 44500, "Storage daemon listening port");
DEFINE_bool(reuse_port, true, "Whether to turn on the SO_REUSEPORT option");
DEFINE_string(data_path, "", "Root data path, multi paths should be split by comma."
                             "For rocksdb engine, one path one instance.");
DEFINE_string(local_ip, "", "Local ip speicified for NetworkUtils::getLocalIP");
DEFINE_bool(mock_server, true, "start mock server");
DEFINE_bool(daemonize, true, "Whether to run the process as a daemon");
DEFINE_string(pid_file, "pids/nebula-storaged.pid", "File to hold the process id");
DEFINE_string(meta_server_addrs, "", "list of meta server addresses,"
                                     "the format looks like ip1:port1, ip2:port2, ip3:port3");
DEFINE_int32(io_handlers, 10, "io handlers");

using nebula::Status;
using nebula::HostAddr;
using nebula::storage::StorageServiceHandler;
using nebula::kvstore::KVStore;
using nebula::meta::SchemaManager;
using nebula::meta::MetaClient;
using nebula::network::NetworkUtils;
using nebula::ProcessUtils;

static std::unique_ptr<apache::thrift::ThriftServer> gServer;

static void signalHandler(int sig);
static Status setupSignalHandler();


int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);
    if (FLAGS_daemonize) {
        google::SetStderrLogging(google::FATAL);
    } else {
        google::SetStderrLogging(google::INFO);
    }

    // Detect if the server has already been started
    auto pidPath = FLAGS_pid_file;
    auto status = ProcessUtils::isPidAvailable(pidPath);
    if (!status.ok()) {
        LOG(ERROR) << status;
        return EXIT_FAILURE;
    }

    if (FLAGS_daemonize) {
        status = ProcessUtils::daemonize(pidPath);
        if (!status.ok()) {
            LOG(ERROR) << status;
            return EXIT_FAILURE;
        }
    } else {
        // Write the current pid into the pid file
        status = ProcessUtils::makePidFile(pidPath);
        if (!status.ok()) {
            LOG(ERROR) << status;
            return EXIT_FAILURE;
        }
    }

    if (FLAGS_data_path.empty()) {
        LOG(ERROR) << "Storage Data Path should not empty";
        return EXIT_FAILURE;
    }

    auto result = nebula::network::NetworkUtils::getLocalIP(FLAGS_local_ip);
    if (!result.ok()) {
        LOG(ERROR) << "Get localIp failed, ip " << FLAGS_local_ip
                   << ", status:" << result.status();
        return EXIT_FAILURE;
    }
    auto hostRet = nebula::network::NetworkUtils::toHostAddr(result.value(), FLAGS_port);
    if (!hostRet.ok()) {
        LOG(ERROR) << "Bad local host addr, status:" << hostRet.status();
        return EXIT_FAILURE;
    }
    auto& localHost = hostRet.value();
    auto metaAddrsRet = nebula::network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
    if (!metaAddrsRet.ok() || metaAddrsRet.value().empty()) {
        LOG(ERROR) << "Can't get metaServer address, status:" << metaAddrsRet.status()
                   << ", FLAGS_meta_server_addrs:" << FLAGS_meta_server_addrs;
        return EXIT_FAILURE;
    }

    std::vector<std::string> paths;
    folly::split(",", FLAGS_data_path, paths, true);
    std::transform(paths.begin(), paths.end(), paths.begin(), [](auto& p) {
        return folly::trimWhitespace(p).str();
    });
    if (paths.empty()) {
         LOG(ERROR) << "Bad data_path format:" << FLAGS_data_path;
         return EXIT_FAILURE;
    }

    auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_io_handlers);
    auto metaClient = std::make_unique<nebula::meta::MetaClient>(ioThreadPool,
                                                                 std::move(metaAddrsRet.value()),
                                                                 true);
    metaClient->init();

    nebula::kvstore::KVOptions options;
    options.local_ = localHost;
    options.dataPaths_ = std::move(paths);
    options.partMan_ = std::make_unique<nebula::kvstore::MetaServerBasedPartManager>(
            options.local_, metaClient.get());
    std::unique_ptr<nebula::kvstore::KVStore> kvstore(
            nebula::kvstore::KVStore::instance(std::move(options)));
    auto schemaMan = nebula::meta::SchemaManager::create();
    schemaMan->init(metaClient.get());

    LOG(INFO) << "Starting Storage HTTP Service";
    nebula::WebService::registerHandler("/storage", [] {
        return new nebula::storage::StorageHttpHandler();
    });

    status = nebula::WebService::start();
    if (!status.ok()) {
        LOG(ERROR) << "Failed to start web service: " << status;
        return EXIT_FAILURE;
    }
    // Setup the signal handlers
    status = setupSignalHandler();
    if (!status.ok()) {
        LOG(ERROR) << status;
        return EXIT_FAILURE;
    }

    auto handler = std::make_shared<StorageServiceHandler>(kvstore.get(), std::move(schemaMan));
    try {
        nebula::operator<<(operator<<(LOG(INFO), "The storage deamon start on "), localHost);
        gServer = std::make_unique<apache::thrift::ThriftServer>();
        gServer->setInterface(std::move(handler));
        gServer->setPort(FLAGS_port);
        gServer->setReusePort(FLAGS_reuse_port);
        gServer->setIdleTimeout(std::chrono::seconds(0));  // No idle timeout on client connection
        gServer->setIOThreadPool(ioThreadPool);
        gServer->serve();  // Will wait until the server shuts down
    } catch (const std::exception& e) {
        LOG(ERROR) << "Start thrift server failed, error:" << e.what();
        return EXIT_FAILURE;
    }

    LOG(INFO) << "The storage Daemon stopped";
}


Status setupSignalHandler() {
    ::signal(SIGPIPE, SIG_IGN);
    ::signal(SIGINT, signalHandler);
    ::signal(SIGTERM, signalHandler);
    return Status::OK();
}


void signalHandler(int sig) {
    switch (sig) {
        case SIGINT:
        case SIGTERM:
            FLOG_INFO("Signal %d(%s) received, stopping this server", sig, ::strsignal(sig));
            nebula::WebService::stop();
            gServer->stop();
            break;
        default:
            FLOG_ERROR("Signal %d(%s) received but ignored", sig, ::strsignal(sig));
    }
}
