/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "common/base/SignalHandler.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "meta/MetaServiceHandler.h"
#include "meta/MetaHttpIngestHandler.h"
#include "meta/MetaHttpCompactHandler.h"
#include "meta/MetaHttpStatusHandler.h"
#include "meta/MetaHttpDownloadHandler.h"
#include "webservice/WebService.h"
#include "network/NetworkUtils.h"
#include "process/ProcessUtils.h"
#include "hdfs/HdfsHelper.h"
#include "hdfs/HdfsCommandHelper.h"
#include "thread/GenericThreadPool.h"
#include "kvstore/PartManager.h"
#include "meta/ClusterIdMan.h"
#include "kvstore/NebulaStore.h"
#include "meta/ActiveHostsMan.h"
#include "meta/KVBasedGflagsManager.h"

using nebula::operator<<;
using nebula::ProcessUtils;
using nebula::Status;

DEFINE_int32(port, 45500, "Meta daemon listening port");
DEFINE_bool(reuse_port, true, "Whether to turn on the SO_REUSEPORT option");
DEFINE_string(data_path, "", "Root data path");
DEFINE_string(meta_server_addrs, "", "It is a list of IPs split by comma,"
                                     "the ips number equals replica number."
                                     "If empty, it means replica is 0");
DEFINE_string(local_ip, "", "Local ip speicified for NetworkUtils::getLocalIP");
DEFINE_int32(num_io_threads, 16, "Number of IO threads");
DEFINE_int32(meta_http_thread_num, 3, "Number of meta daemon's http thread");
DEFINE_int32(num_worker_threads, 32, "Number of workers");
DECLARE_string(part_man_type);

DEFINE_string(pid_file, "pids/nebula-metad.pid", "File to hold the process id");
DEFINE_bool(daemonize, true, "Whether run as a daemon process");

static std::unique_ptr<apache::thrift::ThriftServer> gServer;
static void signalHandler(int sig);
static Status setupSignalHandler();

namespace nebula {
namespace meta {
const std::string kClusterIdKey = "__meta_cluster_id_key__";  // NOLINT
}  // namespace meta
}  // namespace nebula

nebula::ClusterID gClusterId = 0;

std::unique_ptr<nebula::kvstore::KVStore> initKV(std::vector<nebula::HostAddr> peers,
                                                 nebula::HostAddr localhost) {
    auto partMan
        = std::make_unique<nebula::kvstore::MemPartManager>();
    // The meta server has only one space (0), one part (0)
    partMan->addPart(nebula::meta::kDefaultSpaceId,
                     nebula::meta::kDefaultPartId,
                     std::move(peers));
    // folly IOThreadPoolExecutor
    auto ioPool = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_num_io_threads);
    std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager(
        apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(
                                 FLAGS_num_worker_threads, true /*stats*/));
    threadManager->setNamePrefix("executor");
    threadManager->start();
    nebula::kvstore::KVOptions options;
    options.dataPaths_ = {FLAGS_data_path};
    options.partMan_ = std::move(partMan);
    auto kvstore = std::make_unique<nebula::kvstore::NebulaStore>(
                                                        std::move(options),
                                                        ioPool,
                                                        localhost,
                                                        threadManager);
    if (!(kvstore->init())) {
        LOG(ERROR) << "Nebula store init failed";
        return nullptr;
    }

    LOG(INFO) << "Waiting for the leader elected...";
    nebula::HostAddr leader;
    while (true) {
        auto ret = kvstore->partLeader(nebula::meta::kDefaultSpaceId,
                                       nebula::meta::kDefaultPartId);
        if (!nebula::ok(ret)) {
            LOG(ERROR) << "Nebula store init failed";
            return nullptr;
        }
        leader = nebula::value(ret);
        if (leader != nebula::HostAddr(0, 0)) {
            break;
        }
        LOG(INFO) << "Leader has not been elected, sleep 1s";
        sleep(1);
    }

    gClusterId = nebula::meta::ClusterIdMan::getClusterIdFromKV(kvstore.get(),
                                                                nebula::meta::kClusterIdKey);
    if (gClusterId == 0) {
        if (leader == localhost) {
            LOG(INFO) << "I am leader, create cluster Id";
            gClusterId = nebula::meta::ClusterIdMan::create(FLAGS_meta_server_addrs);
            if (!nebula::meta::ClusterIdMan::persistInKV(kvstore.get(),
                                                         nebula::meta::kClusterIdKey,
                                                         gClusterId)) {
                LOG(ERROR) << "Persist cluster failed!";
                return nullptr;
            }
        } else {
            LOG(INFO) << "I am follower, wait for the leader's clusterId";
            while (gClusterId == 0) {
                LOG(INFO) << "Waiting for the leader's clusterId";
                sleep(1);
                gClusterId = nebula::meta::ClusterIdMan::getClusterIdFromKV(
                                                kvstore.get(),
                                                nebula::meta::kClusterIdKey);
            }
        }
    }
    LOG(INFO) << "Nebula store init succeeded, clusterId " << gClusterId;
    return kvstore;
}

bool initWebService(nebula::kvstore::KVStore* kvstore,
                    nebula::hdfs::HdfsCommandHelper* helper,
                    nebula::thread::GenericThreadPool* pool) {
    LOG(INFO) << "Starting Meta HTTP Service";
    nebula::WebService::registerHandler("/status", [] {
        return new nebula::meta::MetaHttpStatusHandler();
    });
    nebula::WebService::registerHandler("/download-dispatch", [kvstore, helper, pool] {
        auto handler = new nebula::meta::MetaHttpDownloadHandler();
        handler->init(kvstore, helper, pool);
        return handler;
    });
    nebula::WebService::registerHandler("/ingest-dispatch", [kvstore, pool] {
        auto handler = new nebula::meta::MetaHttpIngestHandler();
        handler->init(kvstore, pool);
        return handler;
    });
    nebula::WebService::registerHandler("/compact-dispatch", [kvstore, pool] {
        auto handler = new nebula::meta::MetaHttpCompactHandler();
        handler->init(kvstore, pool);
        return handler;
    });
    auto status = nebula::WebService::start();
    if (!status.ok()) {
        LOG(ERROR) << "Failed to start web service: " << status;
        return false;
    }
    return true;
}

bool initComponents(nebula::kvstore::KVStore* kvstore) {
    auto gflagsManager = std::make_unique<nebula::meta::KVBasedGflagsManager>(kvstore);
    gflagsManager->init();
    return true;
}

int main(int argc, char *argv[]) {
    google::SetVersionString(nebula::versionString());
    folly::init(&argc, &argv, true);
    if (FLAGS_data_path.empty()) {
        LOG(ERROR) << "Meta Data Path should not empty";
        return EXIT_FAILURE;
    }

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
        status = ProcessUtils::makePidFile(pidPath);
        if (!status.ok()) {
            LOG(ERROR) << status;
            return EXIT_FAILURE;
        }
    }

    auto result = nebula::network::NetworkUtils::getLocalIP(FLAGS_local_ip);
    if (!result.ok()) {
        LOG(ERROR) << "Get local ip failed! status:" << result.status();
        return EXIT_FAILURE;
    }
    auto hostAddrRet = nebula::network::NetworkUtils::toHostAddr(result.value(), FLAGS_port);
    if (!hostAddrRet.ok()) {
        LOG(ERROR) << "Bad local host addr, status:" << hostAddrRet.status();
        return EXIT_FAILURE;
    }
    auto& localhost = hostAddrRet.value();
    auto peersRet = nebula::network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
    if (!peersRet.ok()) {
        LOG(ERROR) << "Can't get peers address, status:" << peersRet.status();
        return EXIT_FAILURE;
    }

    auto kvstore = initKV(peersRet.value(), hostAddrRet.value());
    if (kvstore == nullptr) {
        LOG(ERROR) << "Init kv failed!";
        return EXIT_FAILURE;
    }

    if (!initComponents(kvstore.get())) {
        LOG(ERROR) << "Init components failed";
        return EXIT_FAILURE;
    }

    LOG(INFO) << "Start http service";
    auto helper = std::make_unique<nebula::hdfs::HdfsCommandHelper>();
    auto pool = std::make_unique<nebula::thread::GenericThreadPool>();
    pool->start(FLAGS_meta_http_thread_num, "http thread pool");
    if (!initWebService(kvstore.get(), helper.get(), pool.get())) {
        LOG(ERROR) << "Init web service failed";
        return EXIT_FAILURE;
    }

    // Setup the signal handlers
    status = setupSignalHandler();
    if (!status.ok()) {
        LOG(ERROR) << status;
        nebula::WebService::stop();
        return EXIT_FAILURE;
    }

    auto handler = std::make_shared<nebula::meta::MetaServiceHandler>(kvstore.get(), gClusterId);
    LOG(INFO) << "The meta deamon start on " << localhost;
    try {
        gServer = std::make_unique<apache::thrift::ThriftServer>();
        gServer->setPort(FLAGS_port);
        gServer->setReusePort(FLAGS_reuse_port);
        gServer->setIdleTimeout(std::chrono::seconds(0));  // No idle timeout on client connection
        gServer->setInterface(std::move(handler));
        gServer->serve();  // Will wait until the server shuts down
    } catch (const std::exception &e) {
        nebula::WebService::stop();
        LOG(ERROR) << "Exception thrown: " << e.what();
        return EXIT_FAILURE;
    }

    nebula::WebService::stop();
    LOG(INFO) << "The meta Daemon stopped";
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
            if (gServer) {
                gServer->stop();
            }
            break;
        default:
            FLOG_ERROR("Signal %d(%s) received but ignored", sig, ::strsignal(sig));
    }
}
