/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Range.h>      // for StringPiece
#include <folly/String.h>     // for split, trimWhitespace
#include <folly/init/Init.h>  // for init
#include <folly/ssl/Init.h>   // for init
#include <gflags/gflags.h>    // for DEFINE_string, ParseComma...
#include <glog/logging.h>     // for FATAL, INFO
#include <signal.h>           // for SIGINT, SIGTERM
#include <stdlib.h>           // for EXIT_FAILURE, EXIT_SUCCESS
#include <string.h>           // for strsignal

#include <algorithm>                        // for transform
#include <boost/filesystem/operations.hpp>  // for absolute
#include <boost/filesystem/path.hpp>        // for path
#include <memory>                           // for unique_ptr, allocator
#include <ostream>                          // for operator<<, basic_ostream
#include <string>                           // for string, operator<<, basic...
#include <vector>                           // for vector

#include "common/base/Base.h"             // for FLOG_ERROR, FLOG_INFO
#include "common/base/Logging.h"          // for LogMessage, LOG, _LOG_ERROR
#include "common/base/SignalHandler.h"    // for SignalHandler, SignalHand...
#include "common/base/Status.h"           // for operator<<, Status
#include "common/base/StatusOr.h"         // for StatusOr
#include "common/datatypes/HostAddr.h"    // for operator<<, HostAddr
#include "common/network/NetworkUtils.h"  // for NetworkUtils
#include "common/process/ProcessUtils.h"  // for ProcessUtils
#include "common/ssl/SSLConfig.h"         // for FLAGS_enable_meta_ssl
#include "common/time/TimezoneInfo.h"     // for Timezone
#include "daemons/SetupLogging.h"         // for setupLogging
#include "storage/StorageServer.h"        // for StorageServer
#include "storage/stats/StorageStats.h"   // for initStorageStats
#include "version/Version.h"              // for versionString

DEFINE_string(local_ip, "", "IP address which is used to identify this server");
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
DEFINE_bool(daemonize, true, "Whether to run the process as a daemon");
DEFINE_string(pid_file, "pids/nebula-storaged.pid", "File to hold the process id");
DEFINE_string(meta_server_addrs,
              "",
              "list of meta server addresses,"
              "the format looks like ip1:port1, ip2:port2, ip3:port3");
DECLARE_int32(port);

using nebula::operator<<;
using nebula::HostAddr;
using nebula::ProcessUtils;
using nebula::Status;
using nebula::StatusOr;
using nebula::network::NetworkUtils;

static void signalHandler(nebula::storage::StorageServer *storageServer, int sig);
static Status setupSignalHandler(nebula::storage::StorageServer *storageServer);
#if defined(__x86_64__)
extern Status setupBreakpad();
#endif

int main(int argc, char *argv[]) {
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
  nebula::initStorageStats();

  folly::init(&argc, &argv, true);
  if (FLAGS_enable_ssl || FLAGS_enable_meta_ssl) {
    folly::ssl::init();
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
  auto metaAddrsRet = nebula::network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
  if (!metaAddrsRet.ok() || metaAddrsRet.value().empty()) {
    LOG(ERROR) << "Can't get metaServer address, status:" << metaAddrsRet.status()
               << ", FLAGS_meta_server_addrs:" << FLAGS_meta_server_addrs;
    return EXIT_FAILURE;
  }

  std::vector<std::string> paths;
  folly::split(",", FLAGS_data_path, paths, true);
  // make the paths absolute
  std::transform(
      paths.begin(), paths.end(), paths.begin(), [](const std::string &p) -> std::string {
        auto path = folly::trimWhitespace(p).str();
        path = boost::filesystem::absolute(path).string();
        LOG(INFO) << "data path= " << path;
        return path;
      });
  if (paths.empty()) {
    LOG(ERROR) << "Bad data_path format:" << FLAGS_data_path;
    return EXIT_FAILURE;
  }

  auto storageServer = std::make_unique<nebula::storage::StorageServer>(
      localhost, metaAddrsRet.value(), paths, FLAGS_wal_path, FLAGS_listener_path);
  // Setup the signal handlers
  status = setupSignalHandler(storageServer.get());
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

  if (!storageServer->start()) {
    LOG(ERROR) << "Storage server start failed";
    storageServer->stop();
    return EXIT_FAILURE;
  }

  storageServer->waitUntilStop();
  LOG(INFO) << "The storage Daemon stopped";
  return EXIT_SUCCESS;
}

Status setupSignalHandler(nebula::storage::StorageServer *storageServer) {
  return nebula::SignalHandler::install(
      {SIGINT, SIGTERM}, [storageServer](nebula::SignalHandler::GeneralSignalInfo *info) {
        signalHandler(storageServer, info->sig());
      });
}

void signalHandler(nebula::storage::StorageServer *storageServer, int sig) {
  switch (sig) {
    case SIGINT:
    case SIGTERM:
      FLOG_INFO("Signal %d(%s) received, stopping this server", sig, ::strsignal(sig));
      if (storageServer) {
        storageServer->notifyStop();
      }
      break;
    default:
      FLOG_ERROR("Signal %d(%s) received but ignored", sig, ::strsignal(sig));
  }
}
