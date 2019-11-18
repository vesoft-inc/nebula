/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "storage/StorageServer.h"
#include "common/base/SignalHandler.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "network/NetworkUtils.h"
#include "process/ProcessUtils.h"

DEFINE_string(data_path, "", "Root data path, multi paths should be split by comma."
                             "For rocksdb engine, one path one instance.");
DEFINE_string(local_ip, "", "IP address which is used to identify this server, "
                            "combined with the listen port");
DEFINE_bool(daemonize, true, "Whether to run the process as a daemon");
DEFINE_string(pid_file, "pids/nebula-storaged.pid", "File to hold the process id");
DEFINE_string(meta_server_addrs, "", "list of meta server addresses,"
                                     "the format looks like ip1:port1, ip2:port2, ip3:port3");
DECLARE_int32(port);

using nebula::operator<<;
using nebula::Status;
using nebula::HostAddr;
using nebula::network::NetworkUtils;
using nebula::ProcessUtils;

static void signalHandler(int sig);
static Status setupSignalHandler();

std::unique_ptr<nebula::storage::StorageServer> gStorageServer;

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

    // Setup the signal handlers
    status = setupSignalHandler();
    if (!status.ok()) {
        LOG(ERROR) << status;
        return EXIT_FAILURE;
    }

    gStorageServer = std::make_unique<nebula::storage::StorageServer>(hostRet.value(),
                                                                      metaAddrsRet.value(),
                                                                      paths);
    if (!gStorageServer->start()) {
        LOG(ERROR) << "Storage server start failed";
        gStorageServer->stop();
        return EXIT_FAILURE;
    }

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
            if (gStorageServer) {
                gStorageServer->stop();
            }
            break;
        default:
            FLOG_ERROR("Signal %d(%s) received but ignored", sig, ::strsignal(sig));
    }
}
