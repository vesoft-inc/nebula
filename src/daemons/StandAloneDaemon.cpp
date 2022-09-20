/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/ssl/Init.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>

#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstring>

#include "MetaDaemonInit.h"
#include "common/base/Base.h"
#include "common/base/SignalHandler.h"
#include "common/fs/FileUtils.h"
#include "common/hdfs/HdfsCommandHelper.h"
#include "common/network/NetworkUtils.h"
#include "common/process/ProcessUtils.h"
#include "common/ssl/SSLConfig.h"
#include "common/time/TimezoneInfo.h"
#include "common/utils/MetaKeyUtils.h"
#include "daemons/SetupLogging.h"
#include "folly/ScopeGuard.h"
#include "graph/service/GraphFlags.h"
#include "graph/service/GraphService.h"
#include "graph/stats/GraphStats.h"
#include "meta/MetaServiceHandler.h"
#include "meta/MetaVersionMan.h"
#include "meta/RootUserMan.h"
#include "meta/http/MetaHttpReplaceHostHandler.h"
#include "meta/processors/job/JobManager.h"
#include "meta/stats/MetaStats.h"
#include "storage/StorageServer.h"
#include "storage/stats/StorageStats.h"
#include "version/Version.h"
#include "webservice/WebService.h"

using nebula::fs::FileUtils;
using nebula::graph::GraphService;
using nebula::operator<<;
using nebula::HostAddr;
using nebula::ProcessUtils;
using nebula::Status;
using nebula::StatusOr;
using nebula::network::NetworkUtils;

void setupThreadManager();
void printHelp(const char *prog);
void stopAllDaemon();
static void signalHandler(int sig);
static Status setupSignalHandler();
#if defined(ENABLE_BREAKPAD)
extern Status setupBreakpad();
#endif

std::unique_ptr<nebula::storage::StorageServer> gStorageServer;
static std::unique_ptr<apache::thrift::ThriftServer> gServer;
static std::unique_ptr<apache::thrift::ThriftServer> gMetaServer;
static std::unique_ptr<nebula::kvstore::KVStore> gMetaKVStore;
std::mutex gServerGuard;

// common flags
DECLARE_string(flagfile);
DECLARE_bool(containerized);
DECLARE_bool(reuse_port);
DECLARE_string(meta_server_addrs);

// storage gflags
DEFINE_string(data_path,
              "",
              "Root data path, multi paths should be split by comma."
              "For rocksdb engine, one path one instance.");
DEFINE_string(wal_path,
              "",
              "Nebula wal path. By default, wal will be stored as a sibling of "
              "rocksdb data.");
DEFINE_string(listener_path,
              "",
              "Path for listener, only wal will be saved."
              "if it is not empty, data_path will not take effect.");
DECLARE_int32(storage_port);

// meta gflags
DEFINE_int32(meta_http_thread_num, 3, "Number of meta daemon's http thread");
DEFINE_int32(meta_port, 45500, "Meta daemon listening port");

int main(int argc, char *argv[]) {
  google::SetVersionString(nebula::versionString());
  google::SetUsageMessage("Usage: " + std::string(argv[0]) + " [options]");
  gflags::ParseCommandLineFlags(&argc, &argv, false);

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

  // Setup logging
  auto status = setupLogging(argv[0]);
  if (!status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }

#if defined(ENABLE_BREAKPAD)
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
  nebula::initMetaStats();
  nebula::initStorageStats();

  if (FLAGS_daemonize) {
    google::SetStderrLogging(google::FATAL);
  } else {
    google::SetStderrLogging(google::INFO);
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

  // Validate the IPv4 address or hostname
  status = NetworkUtils::validateHostOrIp(FLAGS_local_ip);
  if (!status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }

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

  // Setup the signal handlers
  status = setupSignalHandler();
  if (!status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }

  if (FLAGS_data_path.empty()) {
    LOG(ERROR) << "Storage Data Path should not empty";
    return EXIT_FAILURE;
  }

  bool metaReady = false;
  int32_t metaRet = EXIT_FAILURE;
  std ::unique_ptr<std::thread> metaThread = std::make_unique<std::thread>([&] {
    SCOPE_EXIT {
      stopAllDaemon();
    };
    nebula::HostAddr metaLocalhost{FLAGS_local_ip, FLAGS_meta_port};
    LOG(INFO) << "metalocalhost = " << metaLocalhost;
    auto peersRet = nebula::network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
    if (!peersRet.ok()) {
      LOG(ERROR) << "Can't get peers address, status:" << peersRet.status();
      return;
    }
    gMetaKVStore = initKV(peersRet.value(), metaLocalhost);
    if (gMetaKVStore == nullptr) {
      LOG(ERROR) << "Init kv failed!";
      return;
    }
    LOG(INFO) << "Start http service";
    auto webSvc = std::make_unique<nebula::WebService>();
    status = initWebService(webSvc.get(), gMetaKVStore.get());
    if (!status.ok()) {
      LOG(ERROR) << "Init web service failed: " << status;
      return;
    }

    {
      nebula::meta::JobManager *jobMgr = nebula::meta::JobManager::getInstance();
      if (!jobMgr->init(gMetaKVStore.get())) {
        LOG(ERROR) << "Init job manager failed";
        return;
      }
    }

    auto godInit = initGodUser(gMetaKVStore.get(), metaLocalhost);
    if (godInit != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "Init god user failed";
      return;
    }

    auto handler =
        std::make_shared<nebula::meta::MetaServiceHandler>(gMetaKVStore.get(), metaClusterId());
    LOG(INFO) << "The meta deamon start on " << metaLocalhost;
    try {
      gMetaServer = std::make_unique<apache::thrift::ThriftServer>();
      gMetaServer->setPort(FLAGS_meta_port);
      gMetaServer->setIdleTimeout(std::chrono::seconds(0));  // No idle timeout on client connection
      gMetaServer->setInterface(std::move(handler));
      if (FLAGS_enable_ssl || FLAGS_enable_meta_ssl) {
        gMetaServer->setSSLConfig(nebula::sslContextConfig());
      }
      metaReady = true;
      gMetaServer->serve();  // Will wait until the server shuts down
    } catch (const std::exception &e) {
      LOG(ERROR) << "Exception thrown: " << e.what();
      return;
    }

    LOG(INFO) << "The meta Daemon stopped";
    metaRet = EXIT_SUCCESS;
    return;
  });

  constexpr int metaWaitTimeoutInSec = 15;
  constexpr int metaWaitIntervalInSec = 1;
  int32_t metaWaitCount = 0;

  while (!metaReady && metaWaitIntervalInSec * metaWaitCount++ < metaWaitTimeoutInSec) {
    sleep(metaWaitIntervalInSec);
  }

  if (!metaReady) {
    LOG(ERROR) << "Meta not ready in time";
    return EXIT_FAILURE;
  }

  // start graph server
  int32_t graphRet = EXIT_FAILURE;
  std ::unique_ptr<std::thread> graphThread = std::make_unique<std::thread>([&] {
    SCOPE_EXIT {
      stopAllDaemon();
    };
    nebula::HostAddr localhost{FLAGS_local_ip, FLAGS_port};
    LOG(INFO) << "Starting Graph HTTP Service";
    auto webSvc = std::make_unique<nebula::WebService>();
    status = webSvc->start(FLAGS_ws_http_port);
    if (!status.ok()) {
      LOG(WARNING) << "Failed to start graph HTTP service";
      return;
    }

    if (FLAGS_num_netio_threads == 0) {
      FLAGS_num_netio_threads = std::thread::hardware_concurrency();
    }
    if (FLAGS_num_netio_threads <= 0) {
      LOG(WARNING) << "Number of networking IO threads should be greater than zero";
      return;
    }
    LOG(INFO) << "Number of networking IO threads: " << FLAGS_num_netio_threads;

    if (FLAGS_num_worker_threads == 0) {
      FLAGS_num_worker_threads = std::thread::hardware_concurrency();
    }
    if (FLAGS_num_worker_threads <= 0) {
      LOG(WARNING) << "Number of worker threads should be greater than zero";
      return;
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
      return;
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
    // Modify two blocking service
    FLOG_INFO("Starting nebula-graphd on %s:%d\n", localhost.host.c_str(), localhost.port);
    try {
      gServer->serve();  // Blocking wait until shut down via gServer->stop()
    } catch (const std::exception &e) {
      FLOG_ERROR("Exception thrown while starting the RPC server: %s", e.what());
      return;
    }
    FLOG_INFO("nebula-graphd on %s:%d has been stopped", localhost.host.c_str(), localhost.port);
    graphRet = EXIT_SUCCESS;
    return;
  });

  int32_t storageRet = EXIT_FAILURE;
  std ::unique_ptr<std::thread> storageThread = std::make_unique<std::thread>([&] {
    SCOPE_EXIT {
      stopAllDaemon();
    };
    HostAddr host(FLAGS_local_ip, FLAGS_storage_port);
    LOG(INFO) << "host = " << host;
    auto metaAddrsRet = nebula::network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
    if (!metaAddrsRet.ok() || metaAddrsRet.value().empty()) {
      LOG(ERROR) << "Can't get metaServer address, status:" << metaAddrsRet.status()
                 << ", FLAGS_meta_server_addrs:" << FLAGS_meta_server_addrs;
      return;
    }

    std::vector<std::string> paths;
    folly::split(",", FLAGS_data_path, paths, true);
    std::transform(paths.begin(), paths.end(), paths.begin(), [](auto &p) {
      return folly::trimWhitespace(p).str();
    });
    if (paths.empty()) {
      LOG(ERROR) << "Bad data_path format:" << FLAGS_data_path;
      return;
    }
    gStorageServer = std::make_unique<nebula::storage::StorageServer>(
        host, metaAddrsRet.value(), paths, FLAGS_wal_path, FLAGS_listener_path);
    if (!gStorageServer->start()) {
      LOG(ERROR) << "Storage server start failed";
      gStorageServer->stop();
      return;
    }
    gStorageServer->waitUntilStop();
    LOG(INFO) << "The storage Daemon stopped";
    storageRet = EXIT_SUCCESS;
    return;
  });

  metaThread->join();
  graphThread->join();
  storageThread->join();
  if (metaRet != EXIT_SUCCESS || graphRet != EXIT_SUCCESS || storageRet != EXIT_SUCCESS) {
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}

Status setupSignalHandler() {
  return nebula::SignalHandler::install(
      {SIGINT, SIGTERM},
      [](nebula::SignalHandler::GeneralSignalInfo *info) { signalHandler(info->sig()); });
}

void stopAllDaemon() {
  std::lock_guard<std::mutex> guard(gServerGuard);
  if (gServer) {
    gServer->stop();
    gServer.reset();
  }
  if (gStorageServer) {
    gStorageServer->stop();
    gStorageServer.reset();
  }
  if (gMetaServer) {
    gMetaServer->stop();
    gMetaServer.reset();
  }
  {
    auto gJobMgr = nebula::meta::JobManager::getInstance();
    if (gJobMgr) {
      gJobMgr->shutDown();
    }
  }
  if (gMetaKVStore) {
    gMetaKVStore->stop();
    gMetaKVStore.reset();
  }
}

void signalHandler(int sig) {
  switch (sig) {
    case SIGINT:
    case SIGTERM:
      FLOG_INFO("Signal %d(%s) received, stopping this server", sig, ::strsignal(sig));
      stopAllDaemon();
      break;
    default:
      FLOG_ERROR("Signal %d(%s) received but ignored", sig, ::strsignal(sig));
  }
}

void printHelp(const char *prog) {
  fprintf(stderr, "%s --flagfile <config_file>\n", prog);
}

void setupThreadManager() {
  int numThreads =
      FLAGS_num_worker_threads > 0 ? FLAGS_num_worker_threads : gServer->getNumIOWorkerThreads();
  std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager(
      PriorityThreadManager::newPriorityThreadManager(numThreads));
  threadManager->setNamePrefix("executor");
  threadManager->start();
  gServer->setThreadManager(threadManager);
}
