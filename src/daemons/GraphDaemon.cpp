/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/init/Init.h>  // for init
#include <folly/ssl/Init.h>
#include <gflags/gflags.h>  // for SetVersionString, DECLARE_bool
#include <signal.h>         // for SIGINT, SIGTERM
#include <stdio.h>          // for fprintf, stderr
#include <stdlib.h>         // for EXIT_FAILURE, EXIT_SUCCESS
#include <string.h>         // for strsignal, strcmp

#include <memory>   // for make_unique, unique_ptr
#include <ostream>  // for operator<<, basic_ostream::...
#include <thread>   // for thread

#include "common/base/Base.h"             // for FLOG_ERROR, FLOG_INFO
#include "common/base/Logging.h"          // for LogMessage, LOG, _LOG_ERROR
#include "common/base/SignalHandler.h"    // for SignalHandler, SignalHandle...
#include "common/base/Status.h"           // for operator<<, Status
#include "common/datatypes/HostAddr.h"    // for HostAddr
#include "common/network/NetworkUtils.h"  // for NetworkUtils
#include "common/process/ProcessUtils.h"  // for ProcessUtils
#include "common/ssl/SSLConfig.h"         // for FLAGS_enable_graph_ssl, FLA...
#include "common/time/TimezoneInfo.h"
#include "daemons/SetupLogging.h"       // for setupLogging
#include "graph/service/GraphFlags.h"   // for FLAGS_num_netio_threads
#include "graph/service/GraphServer.h"  // for GraphServer
#include "graph/stats/GraphStats.h"     // for initGraphStats
#include "version/Version.h"            // for versionString
#include "webservice/WebService.h"      // for WebService

namespace nebula {
namespace graph {
class GraphService;
}  // namespace graph
template <typename T>
class StatusOr;

namespace fs {
class FileUtils;

class FileUtils;
}  // namespace fs
namespace graph {
class GraphService;
}  // namespace graph
template <typename T>
class StatusOr;
}  // namespace nebula

using nebula::ProcessUtils;
using nebula::Status;
using nebula::StatusOr;
using nebula::fs::FileUtils;
using nebula::graph::GraphService;
using nebula::network::NetworkUtils;

static void signalHandler(nebula::graph::GraphServer *graphServer, int sig);
static Status setupSignalHandler(nebula::graph::GraphServer *graphServer);
static void printHelp(const char *prog);
#if defined(__x86_64__)
extern Status setupBreakpad();
#endif

DECLARE_string(flagfile);
DECLARE_bool(containerized);

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
  if (FLAGS_enable_ssl || FLAGS_enable_graph_ssl || FLAGS_enable_meta_ssl) {
    folly::ssl::init();
  }

  if (FLAGS_flagfile.empty()) {
    printHelp(argv[0]);
    return EXIT_FAILURE;
  }

  // Setup logging
  auto status = setupLogging(argv[0]);
  if (!status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }

#if defined(__x86_64__)
  status = setupBreakpad();
  if (!status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }
#endif

  // Detect if the server has already been started
  auto pidPath = FLAGS_pid_file;
  status = ProcessUtils::isPidAvailable(pidPath);
  if (!status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }

  nebula::initGraphStats();

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

  // Validate the IPv4 address or hostname
  status = NetworkUtils::validateHostOrIp(FLAGS_local_ip);
  if (!status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }
  ::nebula::HostAddr localhost{FLAGS_local_ip, FLAGS_port};

  // load the time zone data
  status = nebula::time::Timezone::init();
  if (!status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }

  // Initialize the global timezone, it's only used for datetime type compute
  // won't affect the process timezone.
  status = nebula::time::Timezone::initializeGlobalTimezone();
  if (!status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }

  LOG(INFO) << "Starting Graph HTTP Service";
  auto webSvc = std::make_unique<::nebula::WebService>();
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

  auto graphServer = std::make_unique<nebula::graph::GraphServer>(localhost);
  // Setup the signal handlers
  status = setupSignalHandler(graphServer.get());
  if (!status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }

  if (!graphServer->start()) {
    LOG(ERROR) << "The graph server start failed";
    return EXIT_FAILURE;
  }

  graphServer->waitUntilStop();
  LOG(INFO) << "The graph Daemon stopped";
  return EXIT_SUCCESS;
}

Status setupSignalHandler(nebula::graph::GraphServer *graphServer) {
  return nebula::SignalHandler::install(
      {SIGINT, SIGTERM}, [graphServer](nebula::SignalHandler::GeneralSignalInfo *info) {
        signalHandler(graphServer, info->sig());
      });
}

void signalHandler(nebula::graph::GraphServer *graphServer, int sig) {
  switch (sig) {
    case SIGINT:
    case SIGTERM:
      FLOG_INFO("Signal %d(%s) received, stopping this server", sig, ::strsignal(sig));
      graphServer->notifyStop();
      break;
    default:
      FLOG_ERROR("Signal %d(%s) received but ignored", sig, ::strsignal(sig));
  }
}

void printHelp(const char *prog) {
  fprintf(stderr, "%s --flagfile <config_file>\n", prog);
}
