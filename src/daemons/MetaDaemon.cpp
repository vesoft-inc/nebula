/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/ssl/Init.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>

#include "MetaDaemonInit.h"
#include "clients/meta/MetaClient.h"
#include "common/base/Base.h"
#include "common/base/SignalHandler.h"
#include "common/fs/FileUtils.h"
#include "common/hdfs/HdfsCommandHelper.h"
#include "common/hdfs/HdfsHelper.h"
#include "common/network/NetworkUtils.h"
#include "common/process/ProcessUtils.h"
#include "common/ssl/SSLConfig.h"
#include "common/thread/GenericThreadPool.h"
#include "common/time/TimezoneInfo.h"
#include "common/utils/MetaKeyUtils.h"
#include "daemons/SetupLogging.h"
#include "kvstore/NebulaStore.h"
#include "kvstore/PartManager.h"
#include "meta/ActiveHostsMan.h"
#include "meta/KVBasedClusterIdMan.h"
#include "meta/MetaServiceHandler.h"
#include "meta/MetaVersionMan.h"
#include "meta/http/MetaHttpReplaceHostHandler.h"
#include "meta/processors/job/JobManager.h"
#include "meta/stats/MetaStats.h"
#include "version/Version.h"
#include "webservice/Router.h"
#include "webservice/WebService.h"

using nebula::operator<<;
using nebula::ProcessUtils;
using nebula::Status;
using nebula::StatusOr;
using nebula::network::NetworkUtils;

DEFINE_string(local_ip, "", "Local ip specified for NetworkUtils::getLocalIP");
DEFINE_int32(port, 45500, "Meta daemon listening port");
DEFINE_bool(reuse_port, true, "Whether to turn on the SO_REUSEPORT option");
DECLARE_string(data_path);
DECLARE_string(meta_server_addrs);

DEFINE_int32(meta_http_thread_num, 3, "Number of meta daemon's http thread");
DEFINE_string(pid_file, "pids/nebula-metad.pid", "File to hold the process id");
DEFINE_bool(daemonize, true, "Whether run as a daemon process");

static std::unique_ptr<nebula::kvstore::KVStore> gKVStore;

static void signalHandler(apache::thrift::ThriftServer* metaServer, int sig);
static void waitForStop();
static Status setupSignalHandler(apache::thrift::ThriftServer* metaServer);
#if defined(ENABLE_BREAKPAD)
extern Status setupBreakpad();
#endif

int main(int argc, char* argv[]) {
  google::SetVersionString(nebula::versionString());
  google::SetUsageMessage("Usage: " + std::string(argv[0]) + " [options]");
  // Detect if the server has already been started
  // Check pid before glog init, in case of user may start daemon twice
  // the 2nd will make the 1st failed to output log anymore
  gflags::ParseCommandLineFlags(&argc, &argv, false);

  Status status;

  auto pidPath = FLAGS_pid_file;
  status = ProcessUtils::isPidAvailable(pidPath);
  if (!status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }

  folly::init(&argc, &argv, true);
  if (FLAGS_enable_ssl || FLAGS_enable_meta_ssl) {
    folly::ssl::init();
  }
  if (FLAGS_data_path.empty()) {
    LOG(ERROR) << "Meta Data Path should not empty";
    return EXIT_FAILURE;
  }

  // Setup logging
  status = setupLogging(argv[0]);
  if (!status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }

  if (FLAGS_daemonize) {
    google::SetStderrLogging(google::FATAL);
  } else {
    google::SetStderrLogging(google::INFO);
  }

#if defined(ENABLE_BREAKPAD)
  status = setupBreakpad();
  if (!status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }
#endif

  // Init stats
  nebula::initMetaStats();

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

  std::string hostName;
  if (FLAGS_local_ip.empty()) {
    hostName = nebula::network::NetworkUtils::getHostname();
  } else {
    status = NetworkUtils::validateHostOrIp(FLAGS_local_ip);
    if (!status.ok()) {
      LOG(ERROR) << status;
      return EXIT_FAILURE;
    }
    hostName = FLAGS_local_ip;
  }
  nebula::HostAddr localhost{hostName, FLAGS_port};
  LOG(INFO) << "localhost = " << localhost;
  auto peersRet = nebula::network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
  if (!peersRet.ok()) {
    LOG(ERROR) << "Can't get peers address, status:" << peersRet.status();
    return EXIT_FAILURE;
  }

  gKVStore = initKV(peersRet.value(), localhost);
  if (gKVStore == nullptr) {
    LOG(ERROR) << "Init kv failed!";
    return EXIT_FAILURE;
  }

  LOG(INFO) << "Start http service";
  auto helper = std::make_unique<nebula::hdfs::HdfsCommandHelper>();
  auto pool = std::make_unique<nebula::thread::GenericThreadPool>();
  pool->start(FLAGS_meta_http_thread_num, "http thread pool");

  auto webSvc = std::make_unique<nebula::WebService>();
  status = initWebService(webSvc.get(), gKVStore.get());
  if (!status.ok()) {
    LOG(ERROR) << "Init web service failed: " << status;
    return EXIT_FAILURE;
  }

  auto godInit = initGodUser(gKVStore.get(), localhost);
  if (godInit != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Init god user failed";
    return EXIT_FAILURE;
  }

  auto metaServer = std::make_unique<apache::thrift::ThriftServer>();
  // Setup the signal handlers
  status = setupSignalHandler(metaServer.get());
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

  auto handler =
      std::make_shared<nebula::meta::MetaServiceHandler>(gKVStore.get(), metaClusterId());
  LOG(INFO) << "The meta daemon start on " << localhost;
  {
    nebula::meta::JobManager* jobMgr = nebula::meta::JobManager::getInstance();
    if (!jobMgr->init(gKVStore.get(), handler->getAdminClient())) {
      LOG(ERROR) << "Init job manager failed";
      return EXIT_FAILURE;
    }
  }
  try {
    metaServer->setPort(FLAGS_port);
    metaServer->setIdleTimeout(std::chrono::seconds(0));  // No idle timeout on client connection
    metaServer->setInterface(std::move(handler));
    if (FLAGS_enable_ssl || FLAGS_enable_meta_ssl) {
      metaServer->setSSLConfig(nebula::sslContextConfig());
    }
    metaServer->serve();  // Will wait until the server shuts down
    waitForStop();
  } catch (const std::exception& e) {
    LOG(ERROR) << "Exception thrown: " << e.what();
    return EXIT_FAILURE;
  }

  LOG(INFO) << "The meta Daemon stopped";
  return EXIT_SUCCESS;
}

Status setupSignalHandler(apache::thrift::ThriftServer* metaServer) {
  return nebula::SignalHandler::install(
      {SIGINT, SIGTERM}, [metaServer](nebula::SignalHandler::GeneralSignalInfo* info) {
        signalHandler(metaServer, info->sig());
      });
}

void signalHandler(apache::thrift::ThriftServer* metaServer, int sig) {
  switch (sig) {
    case SIGINT:
    case SIGTERM:
      FLOG_INFO("Signal %d(%s) received, stopping this server", sig, ::strsignal(sig));
      if (metaServer) {
        metaServer->stop();
      }
      break;
    default:
      FLOG_ERROR("Signal %d(%s) received but ignored", sig, ::strsignal(sig));
  }
}

void waitForStop() {
  auto jobMan = nebula::meta::JobManager::getInstance();
  if (jobMan) {
    jobMan->shutDown();
  }

  if (gKVStore) {
    gKVStore->stop();
    gKVStore.reset();
  }
}
