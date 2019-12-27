/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/StorageServer.h"
#include "network/NetworkUtils.h"
#include "storage/StorageFlags.h"
#include "storage/StorageServiceHandler.h"
#include "storage/http/StorageHttpStatusHandler.h"
#include "storage/http/StorageHttpDownloadHandler.h"
#include "storage/http/StorageHttpIngestHandler.h"
#include "storage/http/StorageHttpAdminHandler.h"
#include "kvstore/PartManager.h"
#include "webservice/WebService.h"
#include "storage/CompactionFilter.h"
#include "hdfs/HdfsCommandHelper.h"
#include "thread/GenericThreadPool.h"
#include <thrift/lib/cpp/concurrency/ThreadManager.h>


DEFINE_int32(port, 44500, "Storage daemon listening port");
DEFINE_bool(reuse_port, true, "Whether to turn on the SO_REUSEPORT option");
DEFINE_int32(num_io_threads, 16, "Number of IO threads");
DEFINE_int32(num_worker_threads, 32, "Number of workers");
DEFINE_int32(storage_http_thread_num, 3, "Number of storage daemon's http thread");

namespace nebula {
namespace storage {

std::unique_ptr<kvstore::KVStore> StorageServer::getStoreInstance() {
    kvstore::KVOptions options;
    options.dataPaths_ = dataPaths_;
    options.partMan_ = std::make_unique<kvstore::MetaServerBasedPartManager>(
                                                localHost_,
                                                metaClient_.get());
    options.cffBuilder_ = std::make_unique<StorageCompactionFilterFactoryBuilder>(schemaMan_.get());
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

    WebService::registerHandler("/status", [] {
        return new StorageHttpStatusHandler();
    });
    WebService::registerHandler("/download", [this] {
        auto* handler = new storage::StorageHttpDownloadHandler();
        handler->init(hdfsHelper_.get(), webWorkers_.get(), kvstore_.get(), dataPaths_);
        return handler;
    });
    nebula::WebService::registerHandler("/ingest", [this] {
        auto handler = new nebula::storage::StorageHttpIngestHandler();
        handler->init(kvstore_.get());
        return handler;
    });
    WebService::registerHandler("/admin", [this] {
        return new storage::StorageHttpAdminHandler(schemaMan_.get(), kvstore_.get());
    });
    auto status = WebService::start();
    if (!status.ok()) {
        return false;
    }
    webStatus_ = Status::RUNNING;
    return true;
}

bool StorageServer::start() {
    ioThreadPool_ = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_num_io_threads);
    workers_ = apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(
                                 FLAGS_num_worker_threads, true /*stats*/);
    workers_->setNamePrefix("executor");
    workers_->start();

    // Meta client
    metaClient_ = std::make_unique<meta::MetaClient>(ioThreadPool_,
                                                     metaAddrs_,
                                                     localHost_,
                                                     0,
                                                     true);
    if (!metaClient_->waitForMetadReady()) {
        LOG(ERROR) << "waitForMetadReady error!";
        return false;
    }

    gFlagsMan_ = std::make_unique<meta::ClientBasedGflagsManager>(metaClient_.get());

    LOG(INFO) << "Init schema manager";
    schemaMan_ = meta::SchemaManager::create();
    schemaMan_->init(metaClient_.get());

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

    auto handler = std::make_shared<StorageServiceHandler>(kvstore_.get(),
                                                           schemaMan_.get(),
                                                           metaClient_.get());
    try {
        LOG(INFO) << "The storage deamon start on " << localHost_;
        tfServer_ = std::make_unique<apache::thrift::ThriftServer>();
        tfServer_->setPort(FLAGS_port);
        tfServer_->setReusePort(FLAGS_reuse_port);
        tfServer_->setIdleTimeout(std::chrono::seconds(0));  // No idle timeout on client connection
        tfServer_->setIOThreadPool(ioThreadPool_);
        tfServer_->setThreadManager(workers_);
        tfServer_->setInterface(std::move(handler));
        tfServer_->setStopWorkersOnStopListening(false);
        tfServer_->serve();  // Will wait until the server shuts down
    } catch (const std::exception& e) {
        LOG(ERROR) << "Start thrift server failed, error:" << e.what();
        return false;
    }
    return true;
}

void StorageServer::stop() {
    if (stopped_) {
        LOG(INFO) << "All services has been stopped";
        return;
    }
    stopped_ = true;
    if (webStatus_ == Status::RUNNING) {
        nebula::WebService::stop();
        webStatus_ = Status::STOPPED;
    }
    if (metaClient_) {
        metaClient_->stop();
    }
    if (kvstore_) {
        kvstore_.reset();
    }
    if (tfServer_) {
        tfServer_->stop();
    }
}

}   // namespace storage
}   // namespace nebula
