/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "meta/MetaServiceHandler.h"
#include "meta/MetaHttpStatusHandler.h"
#include "meta/MetaHttpDownloadHandler.h"
#include "webservice/WebService.h"
#include "network/NetworkUtils.h"
#include "process/ProcessUtils.h"
#include "hdfs/HdfsHelper.h"
#include "hdfs/HdfsCommandHelper.h"
#include "thread/GenericThreadPool.h"
#include "kvstore/PartManager.h"
#include "kvstore/NebulaStore.h"
#include "meta/ActiveHostsMan.h"
#include "meta/KVBasedGflagsManager.h"

using nebula::operator<<;
using nebula::ProcessUtils;
using nebula::Status;

DEFINE_int32(port, 45500, "Meta daemon listening port");
DEFINE_bool(reuse_port, true, "Whether to turn on the SO_REUSEPORT option");
DEFINE_string(data_path, "", "Root data path");
DEFINE_string(peers, "", "It is a list of IPs split by comma,"
                         "the ips number equals replica number."
                         "If empty, it means replica is 1");
DEFINE_string(local_ip, "", "Local ip speicified for NetworkUtils::getLocalIP");
DEFINE_int32(num_io_threads, 16, "Number of IO threads");
DECLARE_string(part_man_type);

DEFINE_string(pid_file, "pids/nebula-metad.pid", "File to hold the process id");
DEFINE_bool(daemonize, true, "Whether run as a daemon process");

static std::unique_ptr<apache::thrift::ThriftServer> gServer;

static void signalHandler(int sig);
static void setupSignalHandler();

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
    auto peersRet = nebula::network::NetworkUtils::toHosts(FLAGS_peers);
    if (!peersRet.ok()) {
        LOG(ERROR) << "Can't get peers address, status:" << peersRet.status();
        return EXIT_FAILURE;
    }

    auto partMan
        = std::make_unique<nebula::kvstore::MemPartManager>();
    // The meta server has only one space, one part.
    partMan->addPart(0, 0, std::move(peersRet.value()));

    // folly IOThreadPoolExecutor
    auto ioPool = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_num_io_threads);

    nebula::kvstore::KVOptions options;
    options.dataPaths_ = {FLAGS_data_path};
    options.partMan_ = std::move(partMan);
    std::unique_ptr<nebula::kvstore::KVStore> kvstore =
        std::make_unique<nebula::kvstore::NebulaStore>(std::move(options),
                                                       ioPool,
                                                       localhost);

    auto *kvstore_ = kvstore.get();

    std::unique_ptr<nebula::hdfs::HdfsHelper> helper =
        std::make_unique<nebula::hdfs::HdfsCommandHelper>();
    auto *helperPtr = helper.get();

    LOG(INFO) << "Starting Meta HTTP Service";
    nebula::WebService::registerHandler("/status", [] {
        return new nebula::meta::MetaHttpStatusHandler();
    });
    nebula::WebService::registerHandler("/download-dispatch", [kvstore_, helperPtr] {
        auto handler = new nebula::meta::MetaHttpDownloadHandler();
        handler->init(kvstore_, helperPtr);
        return handler;
    });
    status = nebula::WebService::start();
    if (!status.ok()) {
        LOG(ERROR) << "Failed to start web service: " << status;
        return EXIT_FAILURE;
    }

    // Setup the signal handlers
    setupSignalHandler();
    auto handler = std::make_shared<nebula::meta::MetaServiceHandler>(kvstore_);
    nebula::meta::ActiveHostsMan::instance(kvstore_);
    nebula::meta::KVBasedGflagsManager::instance(kvstore.get());

    LOG(INFO) << "The meta deamon start on " << localhost;
    try {
        gServer = std::make_unique<apache::thrift::ThriftServer>();
        gServer->setInterface(std::move(handler));
        gServer->setPort(FLAGS_port);
        gServer->setReusePort(FLAGS_reuse_port);
        gServer->setIdleTimeout(std::chrono::seconds(0));  // No idle timeout on client connection
        gServer->setIOThreadPool(ioPool);
        gServer->serve();  // Will wait until the server shuts down
    } catch (const std::exception &e) {
        LOG(ERROR) << "Exception thrown: " << e.what();
        return EXIT_FAILURE;
    }

    LOG(INFO) << "The meta Daemon stopped";
}


void setupSignalHandler() {
    ::signal(SIGPIPE, SIG_IGN);
    ::signal(SIGINT, signalHandler);
    ::signal(SIGTERM, signalHandler);
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
