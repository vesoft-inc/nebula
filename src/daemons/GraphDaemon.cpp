/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/base/SignalHandler.h"
#include "common/network/NetworkUtils.h"
#include <signal.h>
#include <errno.h>
#include <string.h>
#include "common/base/Status.h"
#include "common/fs/FileUtils.h"
#include "common/process/ProcessUtils.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "service/GraphService.h"
#include "service/GraphFlags.h"
#include "common/webservice/WebService.h"
#include "common/time/TimeUtils.h"
#include "version/Version.h"
#include "stats/StatsDef.h"

using nebula::Status;
using nebula::ProcessUtils;
using nebula::graph::GraphService;
using nebula::network::NetworkUtils;
using nebula::fs::FileUtils;

static std::unique_ptr<apache::thrift::ThriftServer> gServer;

static void signalHandler(int sig);
static Status setupSignalHandler();
static Status setupLogging();
static void printHelp(const char *prog);
static void setupThreadManager();

DECLARE_string(flagfile);

int main(int argc, char *argv[]) {
    google::SetVersionString(nebula::versionString());
    if (argc == 1) {
        printHelp(argv[0]);
        return EXIT_FAILURE;
    }
    if (argc == 2) {
        if (::strcmp(argv[1], "-h") == 0) {
            printHelp(argv[0]);
            return EXIT_SUCCESS;
        }
    }

    folly::init(&argc, &argv, true);
    nebula::initCounters();

    if (FLAGS_flagfile.empty()) {
        printHelp(argv[0]);
        return EXIT_FAILURE;
    }

    // Setup logging
    auto status = setupLogging();
    if (!status.ok()) {
        LOG(ERROR) << status;
        return EXIT_FAILURE;
    }

    // Detect if the server has already been started
    auto pidPath = FLAGS_pid_file;
    status = ProcessUtils::isPidAvailable(pidPath);
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

    // Get the IPv4 address the server will listen on
    std::string localIP;
    {
        auto result = NetworkUtils::getIPv4FromDevice(FLAGS_listen_netdev);
        if (!result.ok()) {
            LOG(ERROR) << result.status();
            return EXIT_FAILURE;
        }
        localIP = std::move(result).value();
    }

    // Initialize the global timezone, it's only used for datetime type compute
    // won't affect the process timezone.
    status = nebula::time::TimeUtils::initializeGlobalTimezone();
    if (!status.ok()) {
        LOG(ERROR) << status;
        return EXIT_FAILURE;
    }

    LOG(INFO) << "Starting Graph HTTP Service";
    auto webSvc = std::make_unique<nebula::WebService>();
    status = webSvc->start();
    if (!status.ok()) {
        return EXIT_FAILURE;
    }

    if (FLAGS_num_netio_threads == 0) {
        FLAGS_num_netio_threads = std::thread::hardware_concurrency();
    }
    if (FLAGS_num_netio_threads <= 0) {
        LOG(WARNING) << "Number of networking IO threads should be greater than zero";
        return EXIT_FAILURE;
    }
    LOG(INFO) << "Number of networking IO threads: " << FLAGS_num_netio_threads;

    if (FLAGS_num_worker_threads == 0) {
        FLAGS_num_worker_threads = std::thread::hardware_concurrency();
    }
    if (FLAGS_num_worker_threads <= 0) {
        LOG(WARNING) << "Number of worker threads should be greater than zero";
        return EXIT_FAILURE;
    }
    LOG(INFO) << "Number of worker threads: " << FLAGS_num_worker_threads;

    auto threadFactory = std::make_shared<folly::NamedThreadFactory>("graph-netio");
    auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(
                            FLAGS_num_netio_threads, std::move(threadFactory));
    gServer = std::make_unique<apache::thrift::ThriftServer>();
    gServer->setIOThreadPool(ioThreadPool);

    auto interface = std::make_shared<GraphService>();
    status = interface->init(ioThreadPool);
    if (!status.ok()) {
        LOG(ERROR) << status;
        return EXIT_FAILURE;
    }

    gServer->setInterface(std::move(interface));
    gServer->setAddress(localIP, FLAGS_port);
    gServer->setReusePort(FLAGS_reuse_port);
    gServer->setIdleTimeout(std::chrono::seconds(FLAGS_client_idle_timeout_secs));
    gServer->setNumAcceptThreads(FLAGS_num_accept_threads);
    gServer->setListenBacklog(FLAGS_listen_backlog);
    gServer->setThreadStackSizeMB(5);
    setupThreadManager();

    // Setup the signal handlers
    status = setupSignalHandler();
    if (!status.ok()) {
        LOG(ERROR) << status;
        return EXIT_FAILURE;
    }

    FLOG_INFO("Starting nebula-graphd on %s:%d\n", localIP.c_str(), FLAGS_port);
    try {
        gServer->serve();  // Blocking wait until shut down via gServer->stop()
    } catch (const std::exception &e) {
        FLOG_ERROR("Exception thrown while starting the RPC server: %s", e.what());
        return EXIT_FAILURE;
    }

    FLOG_INFO("nebula-graphd on %s:%d has been stopped", localIP.c_str(), FLAGS_port);

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


Status setupLogging() {
    // If the log directory does not exist, try to create
    if (!FileUtils::exist(FLAGS_log_dir)) {
        if (!FileUtils::makeDir(FLAGS_log_dir)) {
            return Status::Error("Failed to create log directory `%s'",
                                 FLAGS_log_dir.c_str());
        }
    }

    if (!FLAGS_redirect_stdout) {
        return Status::OK();
    }

    auto dup = [] (const std::string &filename, FILE *stream) -> Status {
        auto path = FLAGS_log_dir + "/" + filename;
        auto fd = ::open(path.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
        if (fd == -1) {
            return Status::Error("Failed to create or open `%s': %s",
                                 path.c_str(), ::strerror(errno));
        }
        if (::dup2(fd, ::fileno(stream)) == -1) {
            ::close(fd);
            return Status::Error("Failed to ::dup2 from `%s' to stdout: %s",
                                 path.c_str(), ::strerror(errno));
        }
        ::close(fd);
        return Status::OK();
    };

    NG_RETURN_IF_ERROR(dup(FLAGS_stdout_log_file, stdout));
    NG_RETURN_IF_ERROR(dup(FLAGS_stderr_log_file, stderr));
    return Status::OK();
}


void printHelp(const char *prog) {
    fprintf(stderr, "%s --flagfile <config_file>\n", prog);
}

void setupThreadManager() {
    int numThreads =
        FLAGS_num_worker_threads > 0 ? FLAGS_num_worker_threads : gServer->getNumIOWorkerThreads();
    std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager(
        PriorityThreadManager::newPriorityThreadManager(numThreads, false /*stats*/));
    threadManager->setNamePrefix("executor");
    threadManager->start();
    gServer->setThreadManager(threadManager);
}
