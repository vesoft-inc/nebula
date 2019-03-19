/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "network/NetworkUtils.h"
#include <signal.h>
#include "base/Status.h"
#include "fs/FileUtils.h"
#include "process/ProcessUtils.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "graph/GraphService.h"
#include "graph/GraphHttpHandler.h"
#include "graph/GraphFlags.h"
#include "webservice/WebService.h"

using nebula::Status;
using nebula::ProcessUtils;
using nebula::graph::GraphService;
using nebula::network::NetworkUtils;

static std::unique_ptr<apache::thrift::ThriftServer> gServer;

static void signalHandler(int sig);
static Status setupSignalHandler();
static Status setupLogging();
static void printHelp(const char *prog);
static void printVersion();

DECLARE_string(flagfile);

int main(int argc, char *argv[]) {
    if (argc == 1) {
        printHelp(argv[0]);
        return EXIT_FAILURE;
    }
    if (argc == 2) {
        if (::strcmp(argv[1], "-h") == 0) {
            printHelp(argv[0]);
            return EXIT_SUCCESS;
        }
        if (::strcmp(argv[1], "-v") == 0) {
            printVersion();
            return EXIT_SUCCESS;
        }
    }

    folly::init(&argc, &argv, true);

    if (FLAGS_flagfile.empty()) {
        printHelp(argv[0]);
        return EXIT_FAILURE;
    }

    if (FLAGS_daemonize) {
        google::SetStderrLogging(google::FATAL);
    } else {
        google::SetStderrLogging(google::INFO);
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

    // Setup the signal handlers
    status = setupSignalHandler();
    if (!status.ok()) {
        LOG(ERROR) << status;
        return EXIT_FAILURE;
    }

    LOG(INFO) << "Starting Graph HTTP Service";
    nebula::WebService::registerHandler("/graph", [] {
        return new nebula::graph::GraphHttpHandler();
    });
    status = nebula::WebService::start();
    if (!status.ok()) {
        LOG(ERROR) << "Failed to start web service: " << status;
        return EXIT_FAILURE;
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

    gServer = std::make_unique<apache::thrift::ThriftServer>();
    auto interface = std::make_shared<GraphService>(gServer->getIOThreadPool());

    gServer->setInterface(std::move(interface));
    gServer->setAddress(localIP, FLAGS_port);
    gServer->setReusePort(FLAGS_reuse_port);
    gServer->setIdleTimeout(std::chrono::seconds(FLAGS_client_idle_timeout_secs));

    // TODO(dutor) This only take effects on NORMAL priority threads
    gServer->setNumCPUWorkerThreads(1);

    gServer->setCPUWorkerThreadName("executor");
    gServer->setNumAcceptThreads(FLAGS_num_accept_threads);
    gServer->setListenBacklog(FLAGS_listen_backlog);
    gServer->setThreadStackSizeMB(5);
    if (FLAGS_num_netio_threads != 0) {
        gServer->setNumIOWorkerThreads(FLAGS_num_netio_threads);
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


Status setupLogging() {
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
            return Status::Error("Failed to ::dup2 from `%s' to stdout: %s",
                                 path.c_str(), ::strerror(errno));
        }
        ::close(fd);
        return Status::OK();
    };

    Status status = Status::OK();
    do {
        status = dup(FLAGS_stdout_log_file, stdout);
        if (!status.ok()) {
            break;
        }
        status = dup(FLAGS_stderr_log_file, stderr);
        if (!status.ok()) {
            break;
        }
    } while (false);

    return status;
}


void printHelp(const char *prog) {
    fprintf(stderr, "%s -flagfile config_file\n", prog);
    fprintf(stderr, "%s -h\n", prog);
    fprintf(stderr, "%s -v\n", prog);
}


void printVersion() {
    // TODO(dutor)
}
