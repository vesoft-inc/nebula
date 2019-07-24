/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "common/base/SignalHandler.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "network/NetworkUtils.h"
#include "thread/GenericThreadPool.h"
#include "storage/StorageServiceHandler.h"
#include "storage/StorageHttpStatusHandler.h"
#include "storage/StorageHttpDownloadHandler.h"
#include "storage/StorageHttpAdminHandler.h"
#include "kvstore/NebulaStore.h"
#include "kvstore/PartManager.h"
#include "process/ProcessUtils.h"
#include "storage/test/TestUtils.h"
#include "webservice/WebService.h"
#include "meta/SchemaManager.h"
#include "meta/client/MetaClient.h"
#include "meta/ClientBasedGflagsManager.h"
#include "storage/CompactionFilter.h"
#include "hdfs/HdfsHelper.h"
#include "hdfs/HdfsCommandHelper.h"

DEFINE_int32(port, 44500, "Storage daemon listening port");
DEFINE_bool(reuse_port, true, "Whether to turn on the SO_REUSEPORT option");
DEFINE_string(data_path, "", "Root data path, multi paths should be split by comma."
                             "For rocksdb engine, one path one instance.");
DEFINE_string(local_ip, "", "IP address which is used to identify this server, "
                            "combined with the listen port");
DEFINE_bool(mock_server, true, "start mock server");
DEFINE_bool(daemonize, true, "Whether to run the process as a daemon");
DEFINE_string(pid_file, "pids/nebula-storaged.pid", "File to hold the process id");
DEFINE_string(clusterid_file, "data/storage.cluster.id", "File to hold the cluster id");
DEFINE_string(meta_server_addrs, "", "list of meta server addresses,"
                                     "the format looks like ip1:port1, ip2:port2, ip3:port3");
DEFINE_string(store_type, "nebula",
              "Which type of KVStore to be used by the storage daemon."
              " Options can be \"nebula\", \"hbase\", etc.");
DEFINE_int32(num_io_threads, 16, "Number of IO threads");
DEFINE_int32(num_worker_threads, 32, "Number of workers");

using nebula::operator<<;
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


std::unique_ptr<nebula::kvstore::KVStore> getStoreInstance(
        HostAddr localhost,
        std::vector<std::string> paths,
        std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
        nebula::meta::MetaClient* metaClient,
        nebula::meta::SchemaManager* schemaMan) {
    nebula::kvstore::KVOptions options;
    options.dataPaths_ = std::move(paths);
    options.partMan_ = std::make_unique<nebula::kvstore::MetaServerBasedPartManager>(
        localhost,
        metaClient);
    options.cfFactory_ = std::shared_ptr<nebula::kvstore::KVCompactionFilterFactory>(
            new nebula::storage::NebulaCompactionFilterFactory(schemaMan));
    if (FLAGS_store_type == "nebula") {
        auto nbStore = std::make_unique<nebula::kvstore::NebulaStore>(std::move(options),
                                                                      ioPool,
                                                                      localhost);
        if (!(nbStore->init())) {
            LOG(ERROR) << "nebula store init failed";
            return nullptr;
        }

        return nbStore;
    } else if (FLAGS_store_type == "hbase") {
        LOG(FATAL) << "HBase store has not been implemented";
    } else {
        LOG(FATAL) << "Unknown store type \"" << FLAGS_store_type << "\"";
    }

    return nullptr;
}


int main(int argc, char *argv[]) {
    google::SetVersionString(nebula::versionString());
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
    auto& localhost = hostRet.value();
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

    auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_num_io_threads);

    auto clusterMan
        = std::make_unique<nebula::meta::ClusterManager>("", FLAGS_clusterid_file);
    if (!clusterMan->loadClusterId()) {
        LOG(ERROR) << "storaged clusterId load error!";
    }

    // Meta client
    auto metaClient = std::make_unique<nebula::meta::MetaClient>(ioThreadPool,
                                                                 std::move(metaAddrsRet.value()),
                                                                 localhost,
                                                                 clusterMan.get(),
                                                                 true);
    if (!metaClient->waitForMetadReady()) {
        LOG(ERROR) << "waitForMetadReady error!";
        return EXIT_FAILURE;
    }
    auto gflagsManager = std::make_unique<nebula::meta::ClientBasedGflagsManager>(metaClient.get());
    gflagsManager->init();

    LOG(INFO) << "Init schema manager";
    auto schemaMan = nebula::meta::SchemaManager::create();
    schemaMan->init(metaClient.get());

    LOG(INFO) << "Init kvstore";
    std::unique_ptr<KVStore> kvstore = getStoreInstance(localhost,
                                                        std::move(paths),
                                                        ioThreadPool,
                                                        metaClient.get(),
                                                        schemaMan.get());

    if (nullptr == kvstore) {
        return EXIT_FAILURE;
    }

    std::unique_ptr<nebula::hdfs::HdfsHelper> helper =
        std::make_unique<nebula::hdfs::HdfsCommandHelper>();
    auto* helperPtr = helper.get();

    LOG(INFO) << "Starting Storage HTTP Service";
    nebula::WebService::registerHandler("/status", [] {
        return new nebula::storage::StorageHttpStatusHandler();
    });
    nebula::WebService::registerHandler("/download", [helperPtr] {
        auto* handler = new nebula::storage::StorageHttpDownloadHandler();
        handler->init(helperPtr);
        return handler;
    });
    nebula::WebService::registerHandler("/admin", [&] {
        return new nebula::storage::StorageHttpAdminHandler(schemaMan.get(), kvstore.get());
    });
    status = nebula::WebService::start();
    if (!status.ok()) {
        return EXIT_FAILURE;
    }

    // Setup the signal handlers
    status = setupSignalHandler();
    if (!status.ok()) {
        LOG(ERROR) << status;
        nebula::WebService::stop();
        return EXIT_FAILURE;
    }

    auto handler = std::make_shared<StorageServiceHandler>(kvstore.get(), schemaMan.get());
    try {
        LOG(INFO) << "The storage deamon start on " << localhost;
        gServer = std::make_unique<apache::thrift::ThriftServer>();
        gServer->setInterface(std::move(handler));
        gServer->setPort(FLAGS_port);
        gServer->setReusePort(FLAGS_reuse_port);
        gServer->setIdleTimeout(std::chrono::seconds(0));  // No idle timeout on client connection
        gServer->setIOThreadPool(ioThreadPool);
        gServer->setNumCPUWorkerThreads(FLAGS_num_worker_threads);
        gServer->setCPUWorkerThreadName("executor");
        gServer->serve();  // Will wait until the server shuts down
    } catch (const std::exception& e) {
        nebula::WebService::stop();
        LOG(ERROR) << "Start thrift server failed, error:" << e.what();
        return EXIT_FAILURE;
    }

    nebula::WebService::stop();
    LOG(INFO) << "The storage Daemon stopped";
    return EXIT_SUCCESS;
}


Status setupSignalHandler() {
    return nebula::SignalHandler::install(
        {SIGINT, SIGTERM},
        [](nebula::SignalHandler::GeneralSignalInfo *info) {
            signalHandler(info->sig());
        });
}


void signalHandler(int sig) {
    switch (sig) {
        case SIGINT:
        case SIGTERM:
            FLOG_INFO("Signal %d(%s) received, stopping this server", sig, ::strsignal(sig));
            gServer->stop();
            break;
        default:
            FLOG_ERROR("Signal %d(%s) received but ignored", sig, ::strsignal(sig));
    }
}
