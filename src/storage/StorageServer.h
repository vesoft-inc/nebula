/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_STORAGESERVER_H_
#define STORAGE_STORAGESERVER_H_

#include "common/base/Base.h"
#include "common/meta/SchemaManager.h"
#include "common/meta/IndexManager.h"
#include "common/clients/meta/MetaClient.h"
#include "common/hdfs/HdfsHelper.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "kvstore/NebulaStore.h"
#include "storage/CommonUtils.h"
#include "storage/admin/AdminTaskManager.h"

namespace nebula {

class WebService;

namespace storage {

class StorageServer final {
public:
    StorageServer(HostAddr localHost,
                  std::vector<HostAddr> metaAddrs,
                  std::vector<std::string> dataPaths,
                  std::string walPath = "",
                  std::string listenerPath = "");

    ~StorageServer();

    // Return false if failed.
    bool start();

    void stop();

    void waitUntilStop();

private:
    enum ServiceStatus {
        STATUS_UNINITIALIZED = 0,
        STATUS_RUNNING       = 1,
        STATUS_STTOPED       = 2
    };

private:
    std::unique_ptr<kvstore::KVStore> getStoreInstance();

    bool initWebService();

    std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;
    std::shared_ptr<apache::thrift::concurrency::ThreadManager> workers_;

    std::unique_ptr<std::thread> storageThread_;
    std::unique_ptr<std::thread> adminThread_;
    std::atomic<ServiceStatus> storageSvcStatus_{STATUS_UNINITIALIZED};
    std::atomic<ServiceStatus> adminSvcStatus_{STATUS_UNINITIALIZED};

    std::unique_ptr<apache::thrift::ThriftServer> storageServer_;
    std::unique_ptr<apache::thrift::ThriftServer> adminServer_;

    std::unique_ptr<std::thread> internalStorageThread_;
    std::atomic<ServiceStatus> internalStorageSvcStatus_{STATUS_UNINITIALIZED};
    std::unique_ptr<apache::thrift::ThriftServer> internalStorageServer_;

    std::unique_ptr<nebula::WebService> webSvc_;
    std::unique_ptr<meta::MetaClient> metaClient_;
    std::unique_ptr<kvstore::KVStore> kvstore_;

    std::unique_ptr<nebula::hdfs::HdfsHelper> hdfsHelper_;
    std::unique_ptr<nebula::thread::GenericThreadPool> webWorkers_;
    std::unique_ptr<meta::SchemaManager> schemaMan_;
    std::unique_ptr<meta::IndexManager> indexMan_;
    std::unique_ptr<storage::StorageEnv> env_;

    HostAddr localHost_;
    std::vector<HostAddr> metaAddrs_;
    std::vector<std::string> dataPaths_;
    std::string walPath_;
    std::string listenerPath_;

    AdminTaskManager* taskMgr_{nullptr};
    std::unique_ptr<TransactionManager> txnMan_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_STORAGESERVER_H_
