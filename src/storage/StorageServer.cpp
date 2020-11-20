/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/StorageServer.h"
#include "common/network/NetworkUtils.h"
#include "common/webservice/WebService.h"
#include "common/webservice/Router.h"
#include "common/meta/ServerBasedSchemaManager.h"
#include "common/hdfs/HdfsCommandHelper.h"
#include "common/thread/GenericThreadPool.h"
#include "storage/BaseProcessor.h"
#include "storage/CompactionFilter.h"
#include "storage/StorageFlags.h"
#include "storage/StorageAdminServiceHandler.h"
#include "storage/GraphStorageServiceHandler.h"
#include "storage/http/StorageHttpStatsHandler.h"
#include "storage/http/StorageHttpDownloadHandler.h"
#include "storage/http/StorageHttpIngestHandler.h"
#include "storage/http/StorageHttpAdminHandler.h"
#include "kvstore/PartManager.h"
#include "utils/Utils.h"
#include <thrift/lib/cpp/concurrency/ThreadManager.h>

DEFINE_int32(port, 44500, "Storage daemon listening port");
DEFINE_bool(reuse_port, true, "Whether to turn on the SO_REUSEPORT option");
DEFINE_int32(num_io_threads, 16, "Number of IO threads");
DEFINE_int32(num_worker_threads, 32, "Number of workers");
DEFINE_int32(storage_http_thread_num, 3, "Number of storage daemon's http thread");
DEFINE_bool(local_config, false, "meta client will not retrieve latest configuration from meta");

namespace nebula {
namespace storage {

StorageServer::StorageServer(HostAddr localHost,
                             std::vector<HostAddr> metaAddrs,
                             std::vector<std::string> dataPaths,
                             std::string listenerPath)
    : localHost_(localHost)
    , metaAddrs_(std::move(metaAddrs))
    , dataPaths_(std::move(dataPaths))
    , listenerPath_(std::move(listenerPath)) {}

StorageServer::~StorageServer() {
    stop();
}

std::unique_ptr<kvstore::KVStore> StorageServer::getStoreInstance() {
    kvstore::KVOptions options;
    options.dataPaths_ = dataPaths_;
    options.listenerPath_ = listenerPath_;
    options.partMan_ = std::make_unique<kvstore::MetaServerBasedPartManager>(
                                                localHost_,
                                                metaClient_.get());
    options.cffBuilder_ = std::make_unique<StorageCompactionFilterFactoryBuilder>(schemaMan_.get(),
                                                                                  indexMan_.get());
    options.schemaMan_ = schemaMan_.get();
    if (FLAGS_store_type == "nebula") {
        auto nbStore = std::make_unique<kvstore::NebulaStore>(std::move(options),
                                                              ioThreadPool_,
                                                              localHost_,
                                                              workers_);
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

    auto status = webSvc_->start();
    return status.ok();
}

bool StorageServer::start() {
    ioThreadPool_ = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_num_io_threads);
    workers_ = apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(
                                 FLAGS_num_worker_threads, true /*stats*/);
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
    options.gitInfoSHA_ = NEBULA_STRINGIFY(GIT_INFO_SHA);

    metaClient_ = std::make_unique<meta::MetaClient>(ioThreadPool_,
                                                     metaAddrs_,
                                                     options);
    if (!metaClient_->waitForMetadReady()) {
        LOG(ERROR) << "waitForMetadReady error!";
        return false;
    }

    LOG(INFO) << "Init schema manager";
    schemaMan_ = meta::SchemaManager::create(metaClient_.get());

    LOG(INFO) << "Init index manager";
    indexMan_ = meta::IndexManager::create();
    indexMan_->init(metaClient_.get());

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

    taskMgr_ = AdminTaskManager::instance();
    if (!taskMgr_->init()) {
        LOG(ERROR) << "Init task manager failed!";
        return false;
    }

    env_ = std::make_unique<storage::StorageEnv>();
    env_->kvstore_ = kvstore_.get();
    env_->indexMan_ = indexMan_.get();
    env_->schemaMan_ = schemaMan_.get();
    env_->rebuildIndexGuard_ = std::make_unique<IndexGuard>();

    storageThread_.reset(new std::thread([this] {
        try {
            auto handler = std::make_shared<GraphStorageServiceHandler>(env_.get());
            storageServer_ = std::make_unique<apache::thrift::ThriftServer>();
            storageServer_->setPort(FLAGS_port);
            storageServer_->setReusePort(FLAGS_reuse_port);
            storageServer_->setIdleTimeout(std::chrono::seconds(0));
            storageServer_->setIOThreadPool(ioThreadPool_);
            storageServer_->setThreadManager(workers_);
            storageServer_->setStopWorkersOnStopListening(false);
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
        storageSvcStatus_.store(STATUS_STTOPED);
        LOG(INFO) << "The storage service stopped";
    }));

    adminThread_.reset(new std::thread([this] {
        try {
            auto handler = std::make_shared<StorageAdminServiceHandler>(env_.get());
            auto adminAddr = Utils::getAdminAddrFromStoreAddr(localHost_);
            adminServer_ = std::make_unique<apache::thrift::ThriftServer>();
            adminServer_->setPort(adminAddr.port);
            adminServer_->setReusePort(FLAGS_reuse_port);
            adminServer_->setIdleTimeout(std::chrono::seconds(0));
            adminServer_->setIOThreadPool(ioThreadPool_);
            adminServer_->setThreadManager(workers_);
            adminServer_->setStopWorkersOnStopListening(false);
            adminServer_->setInterface(std::move(handler));

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
        adminSvcStatus_.store(STATUS_STTOPED);
        LOG(INFO) << "The admin service stopped";
    }));

    while (storageSvcStatus_.load() == STATUS_UNINITIALIZED ||
           adminSvcStatus_.load() == STATUS_UNINITIALIZED) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    if (storageSvcStatus_.load() != STATUS_RUNNING || adminSvcStatus_.load() != STATUS_RUNNING) {
        return false;
    }
    return true;
}

void StorageServer::waitUntilStop() {
    adminThread_->join();
    storageThread_->join();
}

void StorageServer::stop() {
    ServiceStatus adminExpected = ServiceStatus::STATUS_RUNNING;
    ServiceStatus storageExpected = ServiceStatus::STATUS_RUNNING;
    if (!adminSvcStatus_.compare_exchange_strong(adminExpected, STATUS_STTOPED) &&
        !storageSvcStatus_.compare_exchange_strong(storageExpected, STATUS_STTOPED)) {
        LOG(INFO) << "All services has been stopped";
        return;
    }
    stopped_ = true;

    if (kvstore_) {
        kvstore_->stop();
    }

    webSvc_.reset();

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
    if (storageServer_) {
        storageServer_->stop();
    }
}

}   // namespace storage
}   // namespace nebula
