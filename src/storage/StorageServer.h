/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_STORAGESERVER_H_
#define STORAGE_STORAGESERVER_H_

#include "base/Base.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "kvstore/NebulaStore.h"
#include "meta/SchemaManager.h"
#include "meta/client/MetaClient.h"
#include "meta/ClientBasedGflagsManager.h"
#include "hdfs/HdfsHelper.h"

namespace nebula {
namespace storage {

class StorageServer final {
public:
    StorageServer(HostAddr localHost,
                  std::vector<HostAddr> metaAddrs,
                  std::vector<std::string> dataPaths)
        : localHost_(localHost)
        , metaAddrs_(std::move(metaAddrs))
        , dataPaths_(std::move(dataPaths)) {}

    ~StorageServer() {
        stop();
    }

    // Return false if failed.
    bool start();

    void stop();

private:
    std::unique_ptr<kvstore::KVStore> getStoreInstance();

    bool initWebService();

private:
    enum Status {
        RUNNING,
        STOPPED,
    };

    std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;
    std::shared_ptr<apache::thrift::concurrency::ThreadManager> workers_;

    std::unique_ptr<apache::thrift::ThriftServer> tfServer_;
    std::unique_ptr<meta::MetaClient> metaClient_;
    std::unique_ptr<kvstore::KVStore> kvstore_;

    std::unique_ptr<nebula::hdfs::HdfsHelper> hdfsHelper_;
    std::unique_ptr<nebula::thread::GenericThreadPool> webWorkers_;
    std::unique_ptr<meta::ClientBasedGflagsManager> gFlagsMan_;
    std::unique_ptr<meta::SchemaManager> schemaMan_;

    std::atomic_bool stopped_{false};
    HostAddr localHost_;
    std::vector<HostAddr> metaAddrs_;
    std::vector<std::string> dataPaths_;
    std::atomic<Status> webStatus_{Status::STOPPED};
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_STORAGESERVER_H_

