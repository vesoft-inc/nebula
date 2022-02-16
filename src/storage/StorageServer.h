/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_STORAGESERVER_H_
#define STORAGE_STORAGESERVER_H_

#include <stdint.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "clients/meta/MetaClient.h"
#include "common/base/Base.h"
#include "common/datatypes/HostAddr.h"
#include "common/hdfs/HdfsHelper.h"
#include "common/log/LogMonitor.h"
#include "common/meta/IndexManager.h"
#include "common/meta/SchemaManager.h"
#include "kvstore/NebulaStore.h"
#include "storage/CommonUtils.h"
#include "storage/GraphStorageLocalServer.h"
#include "storage/admin/AdminTaskManager.h"
#include "storage/transaction/TransactionManager.h"

namespace apache {
namespace thrift {
class ThriftServer;
namespace concurrency {
class ThreadManager;
}  // namespace concurrency
}  // namespace thrift
}  // namespace apache
namespace folly {
class IOThreadPoolExecutor;
}  // namespace folly

namespace nebula {

class WebService;
class LogMonitor;
namespace hdfs {
class HdfsHelper;
}  // namespace hdfs
namespace kvstore {
class KVEngine;
class KVStore;
}  // namespace kvstore
namespace meta {
class IndexManager;
class MetaClient;
class SchemaManager;
}  // namespace meta
namespace thread {
class GenericThreadPool;
}  // namespace thread

namespace storage {
class AdminTaskManager;
class InternalStorageClient;
class StorageEnv;

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

  // used for signal handler to set an internal stop flag
  void notifyStop();

  void waitUntilStop();

 private:
  enum ServiceStatus { STATUS_UNINITIALIZED = 0, STATUS_RUNNING = 1, STATUS_STOPPED = 2 };

 private:
  std::unique_ptr<kvstore::KVStore> getStoreInstance();

  std::unique_ptr<kvstore::KVEngine> getAdminStoreInstance();

  int32_t getAdminStoreSeqId();

  bool initWebService();

  std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;
  std::shared_ptr<apache::thrift::concurrency::ThreadManager> workers_;

  std::unique_ptr<std::thread> storageThread_;
  std::unique_ptr<std::thread> adminThread_;
  std::atomic<ServiceStatus> storageSvcStatus_{STATUS_UNINITIALIZED};
  std::atomic<ServiceStatus> adminSvcStatus_{STATUS_UNINITIALIZED};

#ifndef BUILD_STANDALONE
  std::unique_ptr<apache::thrift::ThriftServer> storageServer_;
#else
  std::shared_ptr<GraphStorageLocalServer> storageServer_;
#endif
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
  std::unique_ptr<TransactionManager> txnMan_{nullptr};
  // used for communicate between one storaged to another
  std::unique_ptr<InternalStorageClient> interClient_;

  std::unique_ptr<LogMonitor> logMonitor_;

  ServiceStatus serverStatus_{STATUS_UNINITIALIZED};
  std::mutex muStop_;
  std::condition_variable cvStop_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_STORAGESERVER_H_
