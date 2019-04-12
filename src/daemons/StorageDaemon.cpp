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
#include "storage/StorageFlags.h"
#include "kvstore/KVStore.h"
#include "kvstore/PartManager.h"
#include "process/ProcessUtils.h"
#include "storage/test/TestUtils.h"
#include "webservice/WebService.h"
#include "meta/SchemaManager.h"


using nebula::Status;
using nebula::HostAddr;
using nebula::storage::StorageServiceHandler;
using nebula::kvstore::KVStore;
using nebula::meta::SchemaManager;
using nebula::network::NetworkUtils;
using nebula::ProcessUtils;

static std::unique_ptr<apache::thrift::ThriftServer> gServer;

static void signalHandler(int sig);
static Status setupSignalHandler();


int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);
    if (FLAGS_storage_daemonize) {
        google::SetStderrLogging(google::FATAL);
    } else {
        google::SetStderrLogging(google::INFO);
    }

    // Detect if the server has already been started
    auto pidPath = FLAGS_storage_pid_file;
    auto status = ProcessUtils::isPidAvailable(pidPath);
    if (!status.ok()) {
        LOG(ERROR) << status;
        return EXIT_FAILURE;
    }

    if (FLAGS_storage_daemonize) {
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

    if (FLAGS_storage_data_path.empty()) {
        LOG(FATAL) << "Storage Data Path should not empty";
        return -1;
    }
    LOG(INFO) << "Starting the storage Daemon on port " << FLAGS_storage_port
              << ", dataPath " << FLAGS_storage_data_path;

    std::vector<std::string> paths;
    folly::split(",", FLAGS_storage_data_path, paths, true);
    std::transform(paths.begin(), paths.end(), paths.begin(), [](auto& p) {
        return folly::trimWhitespace(p).str();
    });
    auto result = nebula::network::NetworkUtils::getLocalIP(FLAGS_storage_local_ip);
    CHECK(result.ok()) << result.status();
    uint32_t localIP;
    CHECK(NetworkUtils::ipv4ToInt(result.value(), localIP));

    nebula::kvstore::KVOptions options;
    options.local_ = HostAddr(localIP, FLAGS_storage_port);
    options.dataPaths_ = std::move(paths);
    options.partMan_
        = std::make_unique<nebula::kvstore::MetaServerBasedPartManager>(options.local_);
    std::unique_ptr<nebula::kvstore::KVStore> kvstore(
            nebula::kvstore::KVStore::instance(std::move(options)));
    auto schemaMan = nebula::meta::SchemaManager::create();
    schemaMan->init();

    LOG(INFO) << "Starting Storage HTTP Service";
    nebula::WebService::registerHandler("/storage", [] {
        return new nebula::storage::StorageHttpHandler();
    });

    status = nebula::WebService::start();
    if (!status.ok()) {
        LOG(ERROR) << "Failed to start web service: " << status;
        return EXIT_FAILURE;
    }

    auto handler = std::make_shared<StorageServiceHandler>(kvstore.get(), std::move(schemaMan));
    gServer = std::make_unique<apache::thrift::ThriftServer>();
    CHECK(!!gServer) << "Failed to create the thrift server";

    // Setup the signal handlers
    status = setupSignalHandler();
    if (!status.ok()) {
        LOG(ERROR) << status;
        return EXIT_FAILURE;
    }

    gServer->setInterface(std::move(handler));
    gServer->setPort(FLAGS_storage_port);
    gServer->setIdleTimeout(std::chrono::seconds(0));  // No idle timeout on client connection

    gServer->serve();  // Will wait until the server shuts down

    LOG(INFO) << "The storage Daemon on port " << FLAGS_storage_port << " stopped";
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
