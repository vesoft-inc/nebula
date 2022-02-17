/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/init/Init.h>                      // for init
#include <folly/ssl/Init.h>                       // for init
#include <gflags/gflags.h>                        // for ParseCommandLineFlags
#include <glog/logging.h>                         // for FATAL, INFO
#include <signal.h>                               // for SIGINT, SIGTERM
#include <stdlib.h>                               // for EXIT_FAILURE, EXIT_...
#include <string.h>                               // for strsignal
#include <thrift/lib/cpp2/server/ThriftServer.h>  // for ThriftServer

#include <chrono>     // for seconds
#include <exception>  // for exception
#include <memory>     // for unique_ptr, make_un...
#include <string>     // for string, char_traits
#include <utility>    // for move

#include "common/base/ErrorOr.h"        // for ok, value
#include "common/base/Logging.h"        // for LogMessage, LOG
#include "common/base/SignalHandler.h"  // for operator<<, Status
#include "common/base/Status.h"         // for operator<<, Status
#include "common/base/StatusOr.h"       // for StatusOr
#include "common/datatypes/HostAddr.h"  // for HostAddr, operator<<
#include "common/hdfs/HdfsCommandHelper.h"
#include "common/hdfs/HdfsHelper.h"
#include "common/network/NetworkUtils.h"  // for NetworkUtils
#include "common/process/ProcessUtils.h"  // for operator<<, Status
#include "common/ssl/SSLConfig.h"         // for FLAGS_enable_meta_ssl
#include "common/time/TimezoneInfo.h"     // for operator<<, Status
#include "daemons/MetaDaemonInit.h"       // for initMetaStats
#include "daemons/SetupLogging.h"         // for setupLogging
#include "kvstore/KVStore.h"              // for KVStore
#include "meta/MetaServiceHandler.h"      // for MetaServiceHandler
#include "meta/RootUserMan.h"             // for RootUserMan
#include "meta/stats/MetaStats.h"         // for initMetaStats
#include "version/Version.h"              // for versionString
#include "webservice/WebService.h"        // for WebService

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
#if defined(__x86_64__)
extern Status setupBreakpad();
#endif

int main(int argc, char* argv[]) {
  google::SetVersionString(nebula::versionString());
  // Detect if the server has already been started
  // Check pid before glog init, in case of user may start daemon twice
  // the 2nd will make the 1st failed to output log anymore
  gflags::ParseCommandLineFlags(&argc, &argv, false);

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

  auto pidPath = FLAGS_pid_file;
  status = ProcessUtils::isPidAvailable(pidPath);
  if (!status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }

  // Init stats
  nebula::initMetaStats();

  folly::init(&argc, &argv, true);
  if (FLAGS_enable_ssl || FLAGS_enable_meta_ssl) {
    folly::ssl::init();
  }
  if (FLAGS_data_path.empty()) {
    LOG(ERROR) << "Meta Data Path should not empty";
    return EXIT_FAILURE;
  }

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
  status = initWebService(webSvc.get(), gKVStore.get(), helper.get(), pool.get());
  if (!status.ok()) {
    LOG(ERROR) << "Init web service failed: " << status;
    return EXIT_FAILURE;
  }

  {
    nebula::meta::JobManager* jobMgr = nebula::meta::JobManager::getInstance();
    if (!jobMgr->init(gKVStore.get())) {
      LOG(ERROR) << "Init job manager failed";
      return EXIT_FAILURE;
    }
  }

  {
    /**
     *  Only leader part needed.
     */
    auto ret = gKVStore->partLeader(nebula::kDefaultSpaceId, nebula::kDefaultPartId);
    if (!nebula::ok(ret)) {
      LOG(ERROR) << "Part leader get failed";
      return EXIT_FAILURE;
    }
    if (nebula::value(ret) == localhost) {
      LOG(INFO) << "Check and init root user";
      if (!nebula::meta::RootUserMan::isUserExists(gKVStore.get())) {
        if (!nebula::meta::RootUserMan::initRootUser(gKVStore.get())) {
          LOG(ERROR) << "Init root user failed";
          return EXIT_FAILURE;
        }
      }
    }
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
