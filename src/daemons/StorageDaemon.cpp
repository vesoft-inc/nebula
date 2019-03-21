/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "network/NetworkUtils.h"
#include "thread/GenericThreadPool.h"
#include "storage/StorageServiceHandler.h"
#include "storage/StorageHttpHandler.h"
#include "kvstore/NebulaStore.h"
#include "kvstore/PartManager.h"
#include "process/ProcessUtils.h"
#include "storage/test/TestUtils.h"
#include "webservice/WebService.h"
#include "meta/SchemaManager.h"
#include "meta/client/MetaClient.h"

DEFINE_int32(port, 44500, "Storage daemon listening port");
DEFINE_string(local_ip, "", "Local ip speicified for NetworkUtils::getLocalIP");
DEFINE_string(data_path, "", "Root data path, multi paths should be split by comma."
                             "For rocksdb engine, one path one instance.");
DEFINE_bool(mock_server, true, "start mock server");
DEFINE_bool(daemonize, true, "Whether to run the process as a daemon");
DEFINE_string(pid_file, "pids/nebula-storaged.pid", "");
DEFINE_string(store_type, "nebula",
              "Which type of KVStore to be used by the storage daemon."
              " Options can be \"nebula\", \"hbase\", etc.");
DEFINE_int32(num_workers, 4, "Number of worker threads");
DEFINE_int32(num_io_threads, 16, "Number of IO threads");

using nebula::Status;

static std::unique_ptr<apache::thrift::ThriftServer> gServer;

static void signalHandler(int sig);
static Status setupSignalHandler();


std::unique_ptr<nebula::kvstore::KVStore> getStoreInstance(
        uint32_t localIP,
        std::vector<std::string> paths,
        std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
        std::shared_ptr<nebula::thread::GenericThreadPool> workers,
        nebula::meta::MetaClient* metaClient) {
    nebula::HostAddr localhost = {localIP, FLAGS_port};
    nebula::kvstore::KVOptions options;
    options.dataPaths_ = std::move(paths);
    options.partMan_ = std::make_unique<nebula::kvstore::MetaServerBasedPartManager>(
        localhost,
        metaClient);

    if (FLAGS_store_type == "nebula") {
        return std::make_unique<nebula::kvstore::NebulaStore>(std::move(options),
                                                              ioPool,
                                                              workers,
                                                              localhost);
    } else if (FLAGS_store_type == "hbase") {
        LOG(FATAL) << "HBase store has not been implemented";
    } else {
        LOG(FATAL) << "Unknown store type \"" << FLAGS_store_type << "\"";
    }
}


int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);
    if (FLAGS_daemonize) {
        google::SetStderrLogging(google::FATAL);
    } else {
        google::SetStderrLogging(google::INFO);
    }
    using nebula::HostAddr;
    using nebula::storage::StorageServiceHandler;
    using nebula::kvstore::KVStore;
    using nebula::meta::SchemaManager;
    using nebula::meta::MetaClient;
    using nebula::network::NetworkUtils;
    using nebula::ProcessUtils;

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

    // Figure out the local ip
    auto result = nebula::network::NetworkUtils::getLocalIP(FLAGS_local_ip);
    CHECK(result.ok()) << result.status();
    uint32_t localIP;
    CHECK(NetworkUtils::ipv4ToInt(result.value(), localIP));

    auto metaClient = std::make_unique<nebula::meta::MetaClient>();
    metaClient->init();

    // Generic thread pool
    auto workers = std::make_shared<nebula::thread::GenericThreadPool>();
    workers->start(FLAGS_num_workers);

    // folly IOThreadPoolExecutor
    auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_num_io_threads);

    std::unique_ptr<KVStore> kvstore = getStoreInstance(localIP,
                                                        std::move(paths),
                                                        ioThreadPool,
                                                        workers,
                                                        metaClient.get());

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

    // Thrift server
    auto handler = std::make_shared<StorageServiceHandler>(kvstore.get(), std::move(schemaMan));
    gServer = std::make_unique<apache::thrift::ThriftServer>();
    CHECK(!!gServer) << "Failed to create the thrift server";
    gServer->setInterface(handler);
    gServer->setPort(FLAGS_port);
    gServer->setIOThreadPool(ioThreadPool);

    // Setup the signal handlers
    status = setupSignalHandler();
    if (!status.ok()) {
        LOG(ERROR) << status;
        return EXIT_FAILURE;
    }

    gServer->setInterface(std::move(handler));
    gServer->setPort(FLAGS_port);
    gServer->setIdleTimeout(std::chrono::seconds(0));  // No idle timeout on client connection

    gServer->serve();  // Will wait until the server shuts down

    LOG(INFO) << "The storage Daemon on port " << FLAGS_port << " stopped";
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
