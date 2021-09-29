/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <errno.h>
#include <folly/ssl/Init.h>
#include <signal.h>
#include <string.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>

#include "common/base/Base.h"
#include "common/base/SignalHandler.h"
#include "common/fs/FileUtils.h"
#include "common/network/NetworkUtils.h"
#include "common/process/ProcessUtils.h"
#include "common/ssl/SSLConfig.h"
#include "common/time/TimezoneInfo.h"
#include "graph/service/GraphFlags.h"
#include "graph/service/GraphService.h"
#include "graph/stats/StatsDef.h"
#include "version/Version.h"
#include "webservice/WebService.h"

using nebula::ProcessUtils;
using nebula::Status;
using nebula::StatusOr;
using nebula::fs::FileUtils;
using nebula::graph::GraphService;
using nebula::network::NetworkUtils;

static std::unique_ptr<apache::thrift::ThriftServer> gServer;

static void signalHandler(int sig);
static Status setupSignalHandler();
extern Status setupLogging();
static void printHelp(const char *prog);
static void setupThreadManager();
#if defined(__x86_64__)
extern Status setupBreakpad();
#endif

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
  if (FLAGS_enable_ssl || FLAGS_enable_graph_ssl || FLAGS_enable_meta_ssl) {
    folly::ssl::init();
  }
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
  if (FLAGS_local_ip.empty()) {
    LOG(ERROR) << "local_ip is empty, need to config it through config file";
    return EXIT_FAILURE;
  }
  // TODO: Check the ip is valid
  nebula::HostAddr localhost{FLAGS_local_ip, FLAGS_port};

  // Initialize the global timezone, it's only used for datetime type compute
  // won't affect the process timezone.
  status = nebula::time::Timezone::initializeGlobalTimezone();
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
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_num_netio_threads,
                                                                    std::move(threadFactory));
  gServer = std::make_unique<apache::thrift::ThriftServer>();
  gServer->setIOThreadPool(ioThreadPool);

  auto interface = std::make_shared<GraphService>();
  status = interface->init(ioThreadPool, localhost);
  if (!status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }

  gServer->setPort(localhost.port);
  gServer->setInterface(std::move(interface));
  gServer->setReusePort(FLAGS_reuse_port);
  gServer->setIdleTimeout(std::chrono::seconds(FLAGS_client_idle_timeout_secs));
  gServer->setNumAcceptThreads(FLAGS_num_accept_threads);
  gServer->setListenBacklog(FLAGS_listen_backlog);
  if (FLAGS_enable_ssl || FLAGS_enable_graph_ssl) {
    gServer->setSSLConfig(nebula::sslContextConfig());
  }
  setupThreadManager();

  // Setup the signal handlers
  status = setupSignalHandler();
  if (!status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }

  FLOG_INFO("Starting nebula-graphd on %s:%d\n", localhost.host.c_str(), localhost.port);
  try {
    gServer->serve();  // Blocking wait until shut down via gServer->stop()
  } catch (const std::exception &e) {
    FLOG_ERROR("Exception thrown while starting the RPC server: %s", e.what());
    return EXIT_FAILURE;
  }

  FLOG_INFO("nebula-graphd on %s:%d has been stopped", localhost.host.c_str(), localhost.port);

  return EXIT_SUCCESS;
}

Status setupSignalHandler() {
  return nebula::SignalHandler::install(
      {SIGINT, SIGTERM},
      [](nebula::SignalHandler::GeneralSignalInfo *info) { signalHandler(info->sig()); });
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

void printHelp(const char *prog) { fprintf(stderr, "%s --flagfile <config_file>\n", prog); }

void setupThreadManager() {
  int numThreads =
      FLAGS_num_worker_threads > 0 ? FLAGS_num_worker_threads : gServer->getNumIOWorkerThreads();
  std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager(
      PriorityThreadManager::newPriorityThreadManager(numThreads, false /*stats*/));
  threadManager->setNamePrefix("executor");
  threadManager->start();
  gServer->setThreadManager(threadManager);
}
