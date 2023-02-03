/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/StorageServer.h"

#include <thrift/lib/cpp/concurrency/ThreadManager.h>

#include <boost/filesystem.hpp>

#include "common/hdfs/HdfsCommandHelper.h"
#include "common/memory/MemoryUtils.h"
#include "common/meta/ServerBasedIndexManager.h"
#include "common/meta/ServerBasedSchemaManager.h"
#include "common/network/NetworkUtils.h"
#include "common/ssl/SSLConfig.h"
#include "common/thread/GenericThreadPool.h"
#include "common/time/TimezoneInfo.h"
#include "common/utils/Utils.h"
#include "kvstore/PartManager.h"
#include "kvstore/RocksEngine.h"
#include "storage/BaseProcessor.h"
#include "storage/CompactionFilter.h"
#include "storage/GraphStorageLocalServer.h"
#include "storage/GraphStorageServiceHandler.h"
#include "storage/StorageAdminServiceHandler.h"
#include "storage/StorageFlags.h"
#include "storage/http/StorageHttpAdminHandler.h"
#include "storage/http/StorageHttpPropertyHandler.h"
#include "storage/http/StorageHttpStatsHandler.h"
#include "storage/transaction/TransactionManager.h"
#include "version/Version.h"
#include "webservice/Router.h"
#include "webservice/WebService.h"

#ifndef BUILD_STANDALONE
DEFINE_int32(port, 44500, "Storage daemon listening port");
DEFINE_int32(num_worker_threads, 32, "Number of workers");
DEFINE_bool(local_config, true, "meta client will not retrieve latest configuration from meta");
#else
DEFINE_int32(storage_port, 44501, "Storage daemon listening port");
DEFINE_int32(storage_num_worker_threads, 32, "Number of workers");
DECLARE_bool(local_config);
DEFINE_bool(add_local_host, true, "Whether add localhost automatically");
DECLARE_string(local_ip);
#endif
DEFINE_bool(storage_kv_mode, false, "True for kv mode");
DEFINE_int32(num_io_threads, 16, "Number of IO threads");
DEFINE_uint32(num_max_connections,
              0,
              "Max active connections for all networking threads. 0 means no limit. Max active "
              "connections for each networking thread = num_max_connections / num_netio_threads");
DEFINE_int32(storage_http_thread_num, 3, "Number of storage daemon's http thread");
DEFINE_int32(check_memory_interval_in_secs, 1, "Memory check interval in seconds");

namespace nebula {
namespace storage {

StorageServer::StorageServer(HostAddr localHost,
                             std::vector<HostAddr> metaAddrs,
                             std::vector<std::string> dataPaths,
                             std::string walPath,
                             std::string listenerPath)
    : localHost_(localHost),
      metaAddrs_(std::move(metaAddrs)),
      dataPaths_(std::move(dataPaths)),
      walPath_(std::move(walPath)),
      listenerPath_(std::move(listenerPath)) {}

Status StorageServer::setupMemoryMonitorThread() {
  memoryMonitorThread_ = std::make_unique<thread::GenericWorker>();
  if (!memoryMonitorThread_ || !memoryMonitorThread_->start("storage-memory-monitor")) {
    return Status::Error("Fail to start storage server background thread.");
  }

  auto updateMemoryWatermark = []() -> Status {
    auto status = memory::MemoryUtils::hitsHighWatermark();
    NG_RETURN_IF_ERROR(status);
    memory::MemoryUtils::kHitMemoryHighWatermark.store(std::move(status).value());
    return Status::OK();
  };

  // Just to test whether to get the right memory info
  NG_RETURN_IF_ERROR(updateMemoryWatermark());

  auto ms = FLAGS_check_memory_interval_in_secs * 1000;
  memoryMonitorThread_->addRepeatTask(ms, updateMemoryWatermark);

  return Status::OK();
}

std::unique_ptr<kvstore::KVStore> StorageServer::getStoreInstance() {
  kvstore::KVOptions options;
  options.dataPaths_ = dataPaths_;
  options.walPath_ = walPath_;
  options.listenerPath_ = listenerPath_;
  options.partMan_ =
      std::make_unique<kvstore::MetaServerBasedPartManager>(localHost_, metaClient_.get());
  if (!FLAGS_storage_kv_mode) {
    options.cffBuilder_ =
        std::make_unique<StorageCompactionFilterFactoryBuilder>(schemaMan_.get(), indexMan_.get());
  }
  options.schemaMan_ = schemaMan_.get();
  if (FLAGS_store_type == "nebula") {
    auto nbStore = std::make_unique<kvstore::NebulaStore>(
        std::move(options), ioThreadPool_, localHost_, workers_);
    if (!(nbStore->init())) {
      LOG(ERROR) << "nebula store init failed";
      return nullptr;
    }
    return nbStore;
  } else if (FLAGS_store_type == "hbase") {
    DLOG(FATAL) << "HBase store has not been implemented";
    return nullptr;
  } else {
    DLOG(FATAL) << "Unknown store type \"" << FLAGS_store_type << "\"";
    return nullptr;
  }
  return nullptr;
}

bool StorageServer::initWebService() {
  LOG(INFO) << "Starting Storage HTTP Service";
  hdfsHelper_ = std::make_unique<hdfs::HdfsCommandHelper>();
  webWorkers_ = std::make_unique<nebula::thread::GenericThreadPool>();
  webWorkers_->start(FLAGS_storage_http_thread_num, "http thread pool");
  LOG(INFO) << "Http Thread Pool started";
  webSvc_ = std::make_unique<WebService>();
  auto& router = webSvc_->router();

  router.get("/admin").handler([this](web::PathParams&&) {
    return new storage::StorageHttpAdminHandler(schemaMan_.get(), kvstore_.get());
  });
  router.get("/rocksdb_stats").handler([](web::PathParams&&) {
    return new storage::StorageHttpStatsHandler();
  });
  router.get("/rocksdb_property").handler([this](web::PathParams&&) {
    return new storage::StorageHttpPropertyHandler(schemaMan_.get(), kvstore_.get());
  });

#ifndef BUILD_STANDALONE
  auto status = webSvc_->start();
#else
  auto status = webSvc_->start(FLAGS_ws_storage_http_port);
#endif
  return status.ok();
}

std::unique_ptr<kvstore::KVEngine> StorageServer::getAdminStoreInstance() {
  int32_t vIdLen = NebulaKeyUtils::adminTaskKey(-1, 0, 0, 0).size();
  std::unique_ptr<kvstore::KVEngine> re(
      new kvstore::RocksEngine(0, vIdLen, dataPaths_[0], walPath_));
  return re;
}

int32_t StorageServer::getAdminStoreSeqId() {
  std::string key = NebulaKeyUtils::adminTaskKey(-1, 0, 0, 0);
  std::string val;
  nebula::cpp2::ErrorCode rc = env_->adminStore_->get(key, &val);
  int32_t curSeqId = 1;
  if (rc == nebula::cpp2::ErrorCode::SUCCEEDED) {
    int32_t lastSeqId = *reinterpret_cast<const int32_t*>(val.data());
    curSeqId = lastSeqId + 1;
  }
  std::string newVal;
  newVal.append(reinterpret_cast<char*>(&curSeqId), sizeof(int32_t));
  auto ret = env_->adminStore_->put(key, newVal);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    DLOG(FATAL) << "Write put in admin-storage seq id " << curSeqId << " failed.";
    return -1;
  }
  return curSeqId;
}

bool StorageServer::start() {
  ioThreadPool_ = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_num_io_threads);
#ifndef BUILD_STANDALONE
  const int32_t numWorkerThreads = FLAGS_num_worker_threads;
#else
  const int32_t numWorkerThreads = FLAGS_storage_num_worker_threads;
#endif
  workers_ = apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(
      numWorkerThreads);
  workers_->setNamePrefix("executor");
  workers_->start();

  // Meta client
  meta::MetaClientOptions options;
  options.localHost_ = localHost_;
  options.serviceName_ = "";
  options.skipConfig_ = FLAGS_local_config;
  options.role_ = nebula::meta::cpp2::HostRole::STORAGE;
  // If listener path is specified, it will start as a listener
  if (!listenerPath_.empty()) {
    options.role_ = nebula::meta::cpp2::HostRole::STORAGE_LISTENER;
  }
  options.gitInfoSHA_ = gitInfoSha();
  options.rootPath_ = boost::filesystem::current_path().string();
  options.dataPaths_ = dataPaths_;

  metaClient_ = std::make_unique<meta::MetaClient>(ioThreadPool_, metaAddrs_, options);

#ifdef BUILD_STANDALONE
  if (FLAGS_add_local_host) {
    // meta already ready when standalone.
    auto ret = metaClient_->checkLocalMachineRegistered();
    if (ret.ok()) {
      if (!ret.value()) {
        std::vector<HostAddr> hosts = {{FLAGS_local_ip, FLAGS_storage_port}};
        folly::Baton<> baton;
        bool isAdded = false;
        metaClient_->addHosts(hosts).thenValue([&isAdded, &baton](StatusOr<bool> resp) {
          if (!resp.ok() || !resp.value()) {
            LOG(ERROR) << "Add hosts for standalone failed.";
          } else {
            LOG(INFO) << "Add hosts for standalone succeed.";
            isAdded = true;
          }
          baton.post();
        });

        baton.wait();
        if (!isAdded) {
          return false;
        }
      }
    } else {
      return false;
    }
  }
#endif

  if (!metaClient_->waitForMetadReady()) {
    LOG(ERROR) << "waitForMetadReady error!";
    return false;
  }

  // Wait meta to sync the timezone configuration
  // Initialize the global timezone, it's only used for datetime type compute
  // won't affect the process timezone.
  auto status = nebula::time::Timezone::initializeGlobalTimezone();
  if (!status.ok()) {
    LOG(ERROR) << status;
    return false;
  }

  LOG(INFO) << "Init schema manager";
  schemaMan_ = meta::ServerBasedSchemaManager::create(metaClient_.get());

  LOG(INFO) << "Init index manager";
  indexMan_ = meta::ServerBasedIndexManager::create(metaClient_.get());

  LOG(INFO) << "Init kvstore";
  kvstore_ = getStoreInstance();

  LOG(INFO) << "Init LogMonitor";
  logMonitor_ = std::make_unique<LogMonitor>();

  if (nullptr == kvstore_) {
    LOG(ERROR) << "Init kvstore failed";
    return false;
  }

  if (!initWebService()) {
    LOG(ERROR) << "Init webservice failed!";
    return false;
  }

  env_ = std::make_unique<storage::StorageEnv>();
  env_->kvstore_ = kvstore_.get();
  env_->indexMan_ = indexMan_.get();
  env_->schemaMan_ = schemaMan_.get();
  env_->rebuildIndexGuard_ = std::make_unique<IndexGuard>();
  env_->metaClient_ = metaClient_.get();

  env_->verticesML_ = std::make_unique<VerticesMemLock>();
  env_->edgesML_ = std::make_unique<EdgesMemLock>();
  env_->adminStore_ = getAdminStoreInstance();
  env_->adminSeqId_ = getAdminStoreSeqId();
  if (env_->adminSeqId_ < 0) {
    LOG(ERROR) << "Get admin store seq id failed!";
    return false;
  }

  taskMgr_ = AdminTaskManager::instance(env_.get());
  if (!taskMgr_->init()) {
    LOG(ERROR) << "Init task manager failed!";
    return false;
  }

  storageServer_ = getStorageServer();
  adminServer_ = getAdminServer();
  if (!storageServer_ || !adminServer_) {
    return false;
  }

  {
    std::lock_guard<std::mutex> lkStop(muStop_);
    if (serverStatus_ != STATUS_UNINITIALIZED) {
      // stop() called during start()
      return false;
    }
    serverStatus_ = STATUS_RUNNING;
  }

  return setupMemoryMonitorThread().ok();
}

void StorageServer::waitUntilStop() {
  {
    std::unique_lock<std::mutex> lkStop(muStop_);
    while (serverStatus_ == STATUS_RUNNING) {
      cvStop_.wait(lkStop);
    }
  }

  this->stop();
}

void StorageServer::notifyStop() {
  std::unique_lock<std::mutex> lkStop(muStop_);
  if (serverStatus_ == STATUS_RUNNING) {
    serverStatus_ = STATUS_STOPPED;
    cvStop_.notify_one();
  }
  if (metaClient_) {
    metaClient_->notifyStop();
  }
}

void StorageServer::stop() {
  // Stop http service
  webSvc_.reset();

  // Stop all thrift server: raft/storage/admin
  if (kvstore_) {
    // stop kvstore background job and raft services
    kvstore_->stop();
  }
  if (adminServer_) {
    adminServer_->cleanUp();
  }
  if (storageServer_) {
#ifndef BUILD_STANDALONE
    storageServer_->cleanUp();
#endif
  }

  // Stop all interface related to kvstore
  if (taskMgr_) {
    taskMgr_->shutdown();
  }
  if (metaClient_) {
    metaClient_->stop();
  }

  // Stop kvstore
  if (kvstore_) {
    kvstore_.reset();
  }
}

#ifndef BUILD_STANDALONE
std::unique_ptr<apache::thrift::ThriftServer> StorageServer::getStorageServer() {
  try {
    auto handler = std::make_shared<GraphStorageServiceHandler>(env_.get());
    auto server = std::make_unique<apache::thrift::ThriftServer>();
    server->setPort(FLAGS_port);
    server->setIdleTimeout(std::chrono::seconds(0));
    server->setIOThreadPool(ioThreadPool_);
    server->setThreadManager(workers_);
    server->setMaxConnections(FLAGS_num_max_connections);
    if (FLAGS_enable_ssl) {
      server->setSSLConfig(nebula::sslContextConfig());
    }
    server->setInterface(std::move(handler));
    server->setup();
    return server;
  } catch (const std::exception& e) {
    LOG(ERROR) << "Start storage server failed: " << e.what();
    return nullptr;
  } catch (...) {
    LOG(ERROR) << "Start storage server failed";
    return nullptr;
  }
}
#else
std::shared_ptr<GraphStorageLocalServer> StorageServer::getStorageServer() {
  auto handler = std::make_shared<GraphStorageServiceHandler>(env_.get());
  auto server = GraphStorageLocalServer::getInstance();
  server->setThreadManager(workers_);
  server->setInterface(std::move(handler));
  return server;
}
#endif

std::unique_ptr<apache::thrift::ThriftServer> StorageServer::getAdminServer() {
  try {
    auto handler = std::make_shared<StorageAdminServiceHandler>(env_.get());
    auto adminAddr = Utils::getAdminAddrFromStoreAddr(localHost_);
    auto server = std::make_unique<apache::thrift::ThriftServer>();
    server->setPort(adminAddr.port);
    server->setIdleTimeout(std::chrono::seconds(0));
    server->setIOThreadPool(ioThreadPool_);
    server->setThreadManager(workers_);
    if (FLAGS_enable_ssl) {
      server->setSSLConfig(nebula::sslContextConfig());
    }
    server->setInterface(std::move(handler));
    server->setup();
    return server;
  } catch (const std::exception& e) {
    LOG(ERROR) << "Start amdin server failed: " << e.what();
    return nullptr;
  } catch (...) {
    LOG(ERROR) << "Start amdin server failed";
    return nullptr;
  }
}

}  // namespace storage
}  // namespace nebula
