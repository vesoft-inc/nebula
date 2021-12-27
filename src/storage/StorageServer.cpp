/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/StorageServer.h"

#include <thrift/lib/cpp/concurrency/ThreadManager.h>

#include <boost/filesystem.hpp>

#include "clients/storage/InternalStorageClient.h"
#include "common/hdfs/HdfsCommandHelper.h"
#include "common/meta/ServerBasedIndexManager.h"
#include "common/meta/ServerBasedSchemaManager.h"
#include "common/network/NetworkUtils.h"
#include "common/ssl/SSLConfig.h"
#include "common/thread/GenericThreadPool.h"
#include "common/utils/Utils.h"
#include "kvstore/PartManager.h"
#include "kvstore/RocksEngine.h"
#include "storage/BaseProcessor.h"
#include "storage/CompactionFilter.h"
#include "storage/GraphStorageLocalServer.h"
#include "storage/GraphStorageServiceHandler.h"
#include "storage/InternalStorageServiceHandler.h"
#include "storage/StorageAdminServiceHandler.h"
#include "storage/StorageFlags.h"
#include "storage/http/StorageHttpAdminHandler.h"
#include "storage/http/StorageHttpDownloadHandler.h"
#include "storage/http/StorageHttpIngestHandler.h"
#include "storage/http/StorageHttpPropertyHandler.h"
#include "storage/http/StorageHttpStatsHandler.h"
#include "storage/transaction/TransactionManager.h"
#include "version/Version.h"
#include "webservice/Router.h"
#include "webservice/WebService.h"

#ifndef BUILD_STANDALONE
DEFINE_int32(port, 44500, "Storage daemon listening port");
DEFINE_int32(num_worker_threads, 32, "Number of workers");
DEFINE_bool(local_config, false, "meta client will not retrieve latest configuration from meta");
#else
DEFINE_int32(storage_port, 44501, "Storage daemon listening port");
DEFINE_int32(storage_num_worker_threads, 32, "Number of workers");
DECLARE_bool(local_config);
#endif
DEFINE_bool(storage_kv_mode, false, "True for kv mode");
DEFINE_int32(num_io_threads, 16, "Number of IO threads");
DEFINE_int32(storage_http_thread_num, 3, "Number of storage daemon's http thread");

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

StorageServer::~StorageServer() {
  stop();
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
    LOG(FATAL) << "HBase store has not been implemented";
  } else {
    LOG(FATAL) << "Unknown store type \"" << FLAGS_store_type << "\"";
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

  router.get("/download").handler([this](web::PathParams&&) {
    auto* handler = new storage::StorageHttpDownloadHandler();
    handler->init(hdfsHelper_.get(), webWorkers_.get(), kvstore_.get(), dataPaths_);
    return handler;
  });
  router.get("/ingest").handler([this](web::PathParams&&) {
    auto handler = new nebula::storage::StorageHttpIngestHandler();
    handler->init(kvstore_.get());
    return handler;
  });
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
  auto status = webSvc_->start(FLAGS_ws_storage_http_port, FLAGS_ws_storage_h2_port);
#endif
  return status.ok();
}

std::unique_ptr<kvstore::KVEngine> StorageServer::getAdminStoreInstance() {
  int32_t vIdLen = NebulaKeyUtils::adminTaskKey(-1, 0, 0).size();
  std::unique_ptr<kvstore::KVEngine> re(
      new kvstore::RocksEngine(0, vIdLen, dataPaths_[0], walPath_));
  return re;
}

int32_t StorageServer::getAdminStoreSeqId() {
  std::string key = NebulaKeyUtils::adminTaskKey(-1, 0, 0);
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
    LOG(FATAL) << "Write put in admin-storage seq id " << curSeqId << " failed.";
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
    options.role_ = nebula::meta::cpp2::HostRole::LISTENER;
  }
  options.gitInfoSHA_ = gitInfoSha();
  options.rootPath_ = boost::filesystem::current_path().string();
  options.dataPaths_ = dataPaths_;

  metaClient_ = std::make_unique<meta::MetaClient>(ioThreadPool_, metaAddrs_, options);
  if (!metaClient_->waitForMetadReady()) {
    LOG(ERROR) << "waitForMetadReady error!";
    return false;
  }

  LOG(INFO) << "Init schema manager";
  schemaMan_ = meta::ServerBasedSchemaManager::create(metaClient_.get());

  LOG(INFO) << "Init index manager";
  indexMan_ = meta::ServerBasedIndexManager::create(metaClient_.get());

  LOG(INFO) << "Init kvstore";
  kvstore_ = getStoreInstance();

  if (nullptr == kvstore_) {
    LOG(ERROR) << "Init kvstore failed";
    return false;
  }

  if (!initWebService()) {
    LOG(ERROR) << "Init webservice failed!";
    return false;
  }

  interClient_ = std::make_unique<InternalStorageClient>(ioThreadPool_, metaClient_.get());

  env_ = std::make_unique<storage::StorageEnv>();
  env_->kvstore_ = kvstore_.get();
  env_->indexMan_ = indexMan_.get();
  env_->schemaMan_ = schemaMan_.get();
  env_->rebuildIndexGuard_ = std::make_unique<IndexGuard>();
  env_->metaClient_ = metaClient_.get();
  env_->interClient_ = interClient_.get();

  txnMan_ = std::make_unique<TransactionManager>(env_.get());
  if (!txnMan_->start()) {
    LOG(ERROR) << "Start transaction manager failed!";
    return false;
  }
  env_->txnMan_ = txnMan_.get();

  env_->verticesML_ = std::make_unique<VerticesMemLock>();
  env_->edgesML_ = std::make_unique<EdgesMemLock>();
  env_->adminStore_ = getAdminStoreInstance();
  env_->adminSeqId_ = getAdminStoreSeqId();
  taskMgr_ = AdminTaskManager::instance(env_.get());
  if (!taskMgr_->init()) {
    LOG(ERROR) << "Init task manager failed!";
    return false;
  }

  storageThread_.reset(new std::thread([this] {
    try {
      auto handler = std::make_shared<GraphStorageServiceHandler>(env_.get());
#ifndef BUILD_STANDALONE
      storageServer_ = std::make_unique<apache::thrift::ThriftServer>();
      storageServer_->setPort(FLAGS_port);
      storageServer_->setIdleTimeout(std::chrono::seconds(0));
      storageServer_->setIOThreadPool(ioThreadPool_);
      storageServer_->setStopWorkersOnStopListening(false);
      if (FLAGS_enable_ssl) {
        storageServer_->setSSLConfig(nebula::sslContextConfig());
      }
#else
      storageServer_ = GraphStorageLocalServer::getInstance();
#endif

      storageServer_->setThreadManager(workers_);
      storageServer_->setInterface(std::move(handler));
      ServiceStatus expected = STATUS_UNINITIALIZED;
      if (!storageSvcStatus_.compare_exchange_strong(expected, STATUS_RUNNING)) {
        LOG(ERROR) << "Impossible! How could it happen!";
        return;
      }
      LOG(INFO) << "The storage service start on " << localHost_;
      storageServer_->serve();  // Will wait until the server shuts down
    } catch (const std::exception& e) {
      LOG(ERROR) << "Start storage service failed, error:" << e.what();
    }
    storageSvcStatus_.store(STATUS_STOPPED);
    LOG(INFO) << "The storage service stopped";
  }));

  adminThread_.reset(new std::thread([this] {
    try {
      auto handler = std::make_shared<StorageAdminServiceHandler>(env_.get());
      auto adminAddr = Utils::getAdminAddrFromStoreAddr(localHost_);
      adminServer_ = std::make_unique<apache::thrift::ThriftServer>();
      adminServer_->setPort(adminAddr.port);
      adminServer_->setIdleTimeout(std::chrono::seconds(0));
      adminServer_->setIOThreadPool(ioThreadPool_);
      adminServer_->setThreadManager(workers_);
      adminServer_->setStopWorkersOnStopListening(false);
      adminServer_->setInterface(std::move(handler));
      if (FLAGS_enable_ssl) {
        adminServer_->setSSLConfig(nebula::sslContextConfig());
      }

      ServiceStatus expected = STATUS_UNINITIALIZED;
      if (!adminSvcStatus_.compare_exchange_strong(expected, STATUS_RUNNING)) {
        LOG(ERROR) << "Impossible! How could it happen!";
        return;
      }
      LOG(INFO) << "The admin service start on " << adminAddr;
      adminServer_->serve();  // Will wait until the server shuts down
    } catch (const std::exception& e) {
      LOG(ERROR) << "Start admin service failed, error:" << e.what();
    }
    adminSvcStatus_.store(STATUS_STOPPED);
    LOG(INFO) << "The admin service stopped";
  }));

  internalStorageThread_.reset(new std::thread([this] {
    try {
      auto handler = std::make_shared<InternalStorageServiceHandler>(env_.get());
      auto internalAddr = Utils::getInternalAddrFromStoreAddr(localHost_);
      internalStorageServer_ = std::make_unique<apache::thrift::ThriftServer>();
      internalStorageServer_->setPort(internalAddr.port);
      internalStorageServer_->setIdleTimeout(std::chrono::seconds(0));
      internalStorageServer_->setIOThreadPool(ioThreadPool_);
      internalStorageServer_->setThreadManager(workers_);
      internalStorageServer_->setStopWorkersOnStopListening(false);
      internalStorageServer_->setInterface(std::move(handler));
      if (FLAGS_enable_ssl) {
        internalStorageServer_->setSSLConfig(nebula::sslContextConfig());
      }

      internalStorageSvcStatus_.store(STATUS_RUNNING);
      LOG(INFO) << "The internal storage service start(same with admin) on " << internalAddr;
      internalStorageServer_->serve();  // Will wait until the server shuts down
    } catch (const std::exception& e) {
      LOG(ERROR) << "Start internal storage service failed, error:" << e.what();
    }
    internalStorageSvcStatus_.store(STATUS_STOPPED);
    LOG(INFO) << "The internal storage  service stopped";
  }));

  while (storageSvcStatus_.load() == STATUS_UNINITIALIZED ||
         adminSvcStatus_.load() == STATUS_UNINITIALIZED ||
         internalStorageSvcStatus_.load() == STATUS_UNINITIALIZED) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }

  if (storageSvcStatus_.load() != STATUS_RUNNING || adminSvcStatus_.load() != STATUS_RUNNING ||
      internalStorageSvcStatus_.load() != STATUS_RUNNING) {
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
  return true;
}

void StorageServer::waitUntilStop() {
  {
    std::unique_lock<std::mutex> lkStop(muStop_);
    while (serverStatus_ == STATUS_RUNNING) {
      cvStop_.wait(lkStop);
    }
  }

  this->stop();

  adminThread_->join();
  storageThread_->join();
  internalStorageThread_->join();
}

void StorageServer::notifyStop() {
  std::unique_lock<std::mutex> lkStop(muStop_);
  if (serverStatus_ == STATUS_RUNNING) {
    serverStatus_ = STATUS_STOPPED;
    cvStop_.notify_one();
  }
}

void StorageServer::stop() {
  if (adminSvcStatus_.load() == ServiceStatus::STATUS_STOPPED &&
      storageSvcStatus_.load() == ServiceStatus::STATUS_STOPPED &&
      internalStorageSvcStatus_.load() == ServiceStatus::STATUS_STOPPED) {
    LOG(INFO) << "All services has been stopped";
    return;
  }

  ServiceStatus adminExpected = ServiceStatus::STATUS_RUNNING;
  adminSvcStatus_.compare_exchange_strong(adminExpected, STATUS_STOPPED);

  ServiceStatus storageExpected = ServiceStatus::STATUS_RUNNING;
  storageSvcStatus_.compare_exchange_strong(storageExpected, STATUS_STOPPED);

  ServiceStatus interStorageExpected = ServiceStatus::STATUS_RUNNING;
  internalStorageSvcStatus_.compare_exchange_strong(interStorageExpected, STATUS_STOPPED);

  // kvstore need to stop back ground job before http server dctor
  if (kvstore_) {
    kvstore_->stop();
  }

  webSvc_.reset();

  if (txnMan_) {
    txnMan_->stop();
  }
  if (taskMgr_) {
    taskMgr_->shutdown();
  }
  if (metaClient_) {
    metaClient_->stop();
  }
  if (kvstore_) {
    kvstore_.reset();
  }
  if (adminServer_) {
    adminServer_->stop();
  }
  if (internalStorageServer_) {
    internalStorageServer_->stop();
  }
  if (storageServer_) {
    storageServer_->stop();
  }
}

}  // namespace storage
}  // namespace nebula
