/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_STORAGESERVER_H_
#define STORAGE_STORAGESERVER_H_

#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "base/Base.h"
#include "hdfs/HdfsHelper.h"
#include "kvstore/NebulaStore.h"
#include "meta/ClientBasedGflagsManager.h"
#include "meta/IndexManager.h"
#include "meta/SchemaManager.h"
#include "meta/client/MetaClient.h"

namespace nebula {

class WebService;

namespace storage {

class StorageServer final {
public:
    enum class Status {
        INIT,
        RUNNING,
        STOPPED,
    };

    StorageServer(network::InetAddress localHost,
                  std::vector<network::InetAddress> metaAddrs,
                  std::vector<std::string> dataPaths);

    ~StorageServer();

    // Return false if failed.
    bool start();

    void stop();

private:
    std::unique_ptr<kvstore::KVStore> getStoreInstance();

    bool initWebService();

    std::string statusStr(Status status) {
        switch (status) {
            case Status::INIT:
                return "init";
            case Status::RUNNING:
                return "running";
            case Status::STOPPED:
                return "stoppped";
        }
        LOG(FATAL) << "Unreached";
        return "";
    }

    std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;
    std::shared_ptr<apache::thrift::concurrency::ThreadManager> workers_;

    std::unique_ptr<apache::thrift::ThriftServer> tfServer_;
    std::unique_ptr<nebula::WebService> webSvc_;
    std::unique_ptr<meta::MetaClient> metaClient_;
    std::unique_ptr<kvstore::KVStore> kvstore_;

    std::unique_ptr<nebula::hdfs::HdfsHelper> hdfsHelper_;
    std::unique_ptr<nebula::thread::GenericThreadPool> webWorkers_;
    std::unique_ptr<meta::ClientBasedGflagsManager> gFlagsMan_;
    std::unique_ptr<meta::SchemaManager> schemaMan_;
    std::unique_ptr<meta::IndexManager> indexMan_;

    network::InetAddress localHost_;
    std::vector<network::InetAddress> metaAddrs_;
    std::vector<std::string> dataPaths_;

    std::atomic<Status> status_{Status::INIT};
};

}   // namespace storage
}   // namespace nebula
#endif   // STORAGE_STORAGESERVER_H_
