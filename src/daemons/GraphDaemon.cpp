/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include <signal.h>
#include "base/Base.h"
#include "base/Status.h"
#include "fs/FileUtils.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "graph/GraphService.h"
#include "graph/GraphFlags.h"

using namespace nebula;
using namespace nebula::graph;
using namespace nebula::fs;

static std::unique_ptr<apache::thrift::ThriftServer> gServer;

static void signalHandler(int sig);
static Status setupSignalHandler();
static Status setupLogging();

int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);

    auto status = setupLogging();
    if (!status.ok()) {
        LOG(ERROR) << status;
        return EXIT_FAILURE;
    }

    status = setupSignalHandler();
    if (!status.ok()) {
        LOG(ERROR) << status;
        return EXIT_FAILURE;
    }

    auto interface = std::make_shared<GraphService>();
    gServer = std::make_unique<apache::thrift::ThriftServer>();

    gServer->setInterface(interface);
    gServer->setPort(FLAGS_port);
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

    FLOG_INFO("Starting nebula-graphd on port %d\n", FLAGS_port);
    try {
        gServer->serve();  // Blocking wait until shut down via gServer->stop()
    } catch (const std::exception &e) {
        FLOG_ERROR("Exception thrown while starting the RPC server: %s", e.what());
        return EXIT_FAILURE;
    }

    FLOG_INFO("nebula-graphd on port %d has been stopped", FLAGS_port);

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
            gServer->stop();
            break;
        default:
            FLOG_ERROR("Signal %d(%s) received but ignored", sig, ::strsignal(sig));
    }
}


Status setupLogging() {
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
