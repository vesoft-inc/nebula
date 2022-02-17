/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "MetaDaemonInit.h"

#include <folly/executors/IOThreadPoolExecutor.h>      // for IOThreadPoolEx...
#include <gflags/gflags.h>                             // for DEFINE_int32
#include <stdint.h>                                    // for int32_t
#include <thrift/lib/cpp/concurrency/ThreadManager.h>  // for ThreadManager
#include <thrift/lib/cpp2/server/Cpp2ConnContext.h>    // for PriorityThread...
#include <unistd.h>                                    // for sleep

#include <boost/filesystem/operations.hpp>  // for absolute
#include <boost/filesystem/path.hpp>        // for path
#include <string>                           // for string, basic_...
#include <utility>                          // for move

#include "common/base/ErrorOr.h"                   // for ok, value
#include "common/base/Logging.h"                   // for LOG, LogMessage
#include "common/datatypes/HostAddr.h"             // for HostAddr
#include "common/hdfs/HdfsCommandHelper.h"         // for HdfsCommandHelper
#include "kvstore/KVStore.h"                       // for KVStore (ptr o...
#include "kvstore/NebulaStore.h"                   // for NebulaStore
#include "kvstore/Part.h"                          // for Part
#include "meta/KVBasedClusterIdMan.h"              // for ClusterIdMan
#include "meta/MetaVersionMan.h"                   // for MetaVersion
#include "meta/http/MetaHttpDownloadHandler.h"     // for MetaHttpDownlo...
#include "meta/http/MetaHttpIngestHandler.h"       // for MetaHttpIngest...
#include "meta/http/MetaHttpReplaceHostHandler.h"  // for MetaHttpIngest...
#include "webservice/Router.h"
#include "webservice/WebService.h"  // for WebService

namespace nebula {
namespace thread {
class GenericThreadPool;

class GenericThreadPool;
}  // namespace thread
}  // namespace nebula

#ifndef BUILD_STANDALONE
DEFINE_int32(num_io_threads, 16, "Number of IO threads");
DEFINE_int32(num_worker_threads, 32, "Number of workers");
DEFINE_string(data_path, "", "Root data path");
DEFINE_string(meta_server_addrs,
              "",
              "It is a list of IPs split by comma, used in cluster deployment"
              "the ips number is equal to the replica number."
              "If empty, it means it's a single node");
#else
DEFINE_int32(meta_num_io_threads, 16, "Number of IO threads");
DEFINE_int32(meta_num_worker_threads, 32, "Number of workers");
DEFINE_string(meta_data_path, "", "Root data path");
DECLARE_string(meta_server_addrs);  // use define from grap flags.
DECLARE_int32(ws_meta_http_port);
#endif

using nebula::web::PathParams;

namespace nebula::meta {
const std::string kClusterIdKey = "__meta_cluster_id_key__";  // NOLINT
}  // namespace nebula::meta

nebula::ClusterID gClusterId = 0;
nebula::ClusterID& metaClusterId() {
  return gClusterId;
}

std::unique_ptr<nebula::kvstore::KVStore> initKV(std::vector<nebula::HostAddr> peers,
                                                 nebula::HostAddr localhost) {
  auto partMan = std::make_unique<nebula::kvstore::MemPartManager>();
  // The meta server has only one space (0), one part (0)
  partMan->addPart(nebula::kDefaultSpaceId, nebula::kDefaultPartId, std::move(peers));
#ifndef BUILD_STANDALONE
  int32_t numMetaIoThreads = FLAGS_num_io_threads;
  int32_t numMetaWorkerThreads = FLAGS_num_worker_threads;
#else
  int32_t numMetaIoThreads = FLAGS_meta_num_io_threads;
  int32_t numMetaWorkerThreads = FLAGS_meta_num_worker_threads;
#endif
  // folly IOThreadPoolExecutor
  auto ioPool = std::make_shared<folly::IOThreadPoolExecutor>(numMetaIoThreads);
  std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager(
      apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(
          numMetaWorkerThreads));
  threadManager->setNamePrefix("executor");
  threadManager->start();
  nebula::kvstore::KVOptions options;
#ifndef BUILD_STANDALONE
  auto absolute = boost::filesystem::absolute(FLAGS_data_path);
#else
  auto absolute = boost::filesystem::absolute(FLAGS_meta_data_path);
#endif
  options.dataPaths_ = {absolute.string()};
  options.partMan_ = std::move(partMan);
  auto kvstore = std::make_unique<nebula::kvstore::NebulaStore>(
      std::move(options), ioPool, localhost, threadManager);
  if (!(kvstore->init())) {
    LOG(ERROR) << "Nebula store init failed";
    return nullptr;
  }

  auto engineRet = kvstore->part(nebula::kDefaultSpaceId, nebula::kDefaultPartId);
  if (!nebula::ok(engineRet)) {
    LOG(ERROR) << "Get nebula store engine failed";
    return nullptr;
  }

  auto engine = nebula::value(engineRet)->engine();
  LOG(INFO) << "Waiting for the leader elected...";
  nebula::HostAddr leader;
  while (true) {
    auto ret = kvstore->partLeader(nebula::kDefaultSpaceId, nebula::kDefaultPartId);
    if (!nebula::ok(ret)) {
      LOG(ERROR) << "Nebula store init failed";
      return nullptr;
    }
    leader = nebula::value(ret);
    if (leader != nebula::HostAddr("", 0)) {
      break;
    }
    LOG(INFO) << "Leader has not been elected, sleep 1s";
    sleep(1);
  }

  gClusterId =
      nebula::meta::ClusterIdMan::getClusterIdFromKV(kvstore.get(), nebula::meta::kClusterIdKey);
  if (gClusterId == 0) {
    if (leader == localhost) {
      LOG(INFO) << "I am leader, create cluster Id";
      gClusterId = nebula::meta::ClusterIdMan::create(FLAGS_meta_server_addrs);
      if (!nebula::meta::ClusterIdMan::persistInKV(
              kvstore.get(), nebula::meta::kClusterIdKey, gClusterId)) {
        LOG(ERROR) << "Persist cluster failed!";
        return nullptr;
      }
    } else {
      LOG(INFO) << "I am follower, wait for the leader's clusterId";
      while (gClusterId == 0) {
        LOG(INFO) << "Waiting for the leader's clusterId";
        sleep(1);
        gClusterId = nebula::meta::ClusterIdMan::getClusterIdFromKV(kvstore.get(),
                                                                    nebula::meta::kClusterIdKey);
      }
    }
  }

  auto version = nebula::meta::MetaVersionMan::getMetaVersionFromKV(kvstore.get());
  LOG(INFO) << "Get meta version is " << static_cast<int32_t>(version);
  if (version == nebula::meta::MetaVersion::UNKNOWN) {
    LOG(ERROR) << "Meta version is invalid";
    return nullptr;
  } else if (version == nebula::meta::MetaVersion::V1) {
    LOG(ERROR) << "Can't upgrade meta from V1 to V3";
    return nullptr;
  } else if (version == nebula::meta::MetaVersion::V2) {
    auto ret = nebula::meta::MetaVersionMan::updateMetaV2ToV3(engine);
    if (!ret.ok()) {
      LOG(ERROR) << "Update meta from V2 to V3 failed " << ret;
      return nullptr;
    }

    nebula::meta::MetaVersionMan::setMetaVersionToKV(engine, nebula::meta::MetaVersion::V3);
  }

  LOG(INFO) << "Nebula store init succeeded, clusterId " << gClusterId;
  return kvstore;
}

nebula::Status initWebService(nebula::WebService* svc,
                              nebula::kvstore::KVStore* kvstore,
                              nebula::hdfs::HdfsCommandHelper* helper,
                              nebula::thread::GenericThreadPool* pool) {
  LOG(INFO) << "Starting Meta HTTP Service";
  auto& router = svc->router();
  router.get("/download-dispatch").handler([kvstore, helper, pool](PathParams&&) {
    auto handler = new nebula::meta::MetaHttpDownloadHandler();
    handler->init(kvstore, helper, pool);
    return handler;
  });
  router.get("/ingest-dispatch").handler([kvstore, pool](PathParams&&) {
    auto handler = new nebula::meta::MetaHttpIngestHandler();
    handler->init(kvstore, pool);
    return handler;
  });
  router.get("/replace").handler([kvstore](PathParams&&) {
    auto handler = new nebula::meta::MetaHttpReplaceHostHandler();
    handler->init(kvstore);
    return handler;
  });
#ifndef BUILD_STANDALONE
  return svc->start();
#else
  return svc->start(FLAGS_ws_meta_http_port);
#endif
}
