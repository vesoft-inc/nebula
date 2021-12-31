/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "MetaDaemonInit.h"

#include <folly/ssl/Init.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>

#include "common/base/Base.h"
#include "common/base/SignalHandler.h"
#include "common/fs/FileUtils.h"
#include "common/hdfs/HdfsCommandHelper.h"
#include "common/hdfs/HdfsHelper.h"
#include "common/network/NetworkUtils.h"
#include "common/ssl/SSLConfig.h"
#include "common/thread/GenericThreadPool.h"
#include "common/utils/MetaKeyUtils.h"
#include "kvstore/NebulaStore.h"
#include "kvstore/PartManager.h"
#include "meta/ActiveHostsMan.h"
#include "meta/KVBasedClusterIdMan.h"
#include "meta/MetaServiceHandler.h"
#include "meta/MetaVersionMan.h"
#include "meta/http/MetaHttpDownloadHandler.h"
#include "meta/http/MetaHttpIngestHandler.h"
#include "meta/http/MetaHttpReplaceHostHandler.h"
#include "meta/processors/job/JobManager.h"
#include "meta/stats/MetaStats.h"
#include "webservice/Router.h"
#include "webservice/WebService.h"

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
DECLARE_int32(ws_meta_h2_port);
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
    if (leader == localhost) {
      LOG(INFO) << "I am leader, begin upgrade meta data";
      // need to upgrade the v1.0 meta data format to v2.0 meta data format
      auto ret = nebula::meta::MetaVersionMan::updateMetaV1ToV2(kvstore.get());
      if (!ret.ok()) {
        LOG(ERROR) << ret;
        return nullptr;
      }
    } else {
      LOG(INFO) << "I am follower, wait for leader to sync upgrade";
      while (version != nebula::meta::MetaVersion::V2) {
        VLOG(1) << "Waiting for leader to upgrade";
        sleep(1);
        version = nebula::meta::MetaVersionMan::getMetaVersionFromKV(kvstore.get());
      }
    }
  }

  if (leader == localhost) {
    nebula::meta::MetaVersionMan::setMetaVersionToKV(kvstore.get());
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
  return svc->start(FLAGS_ws_meta_http_port, FLAGS_ws_meta_h2_port);
#endif
}
