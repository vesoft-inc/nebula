/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_STORAGESERVER_H_
#define STORAGE_STORAGESERVER_H_

#include <thrift/lib/cpp2/server/ThriftServer.h>

#include "clients/meta/MetaClient.h"
#include "common/base/Base.h"
#include "common/hdfs/HdfsHelper.h"
#include "common/log/LogMonitor.h"
#include "common/meta/IndexManager.h"
#include "common/meta/SchemaManager.h"
#include "kvstore/NebulaStore.h"
#include "storage/CommonUtils.h"
#include "storage/GraphStorageLocalServer.h"
#include "storage/admin/AdminTaskManager.h"
#include "storage/transaction/TransactionManager.h"
#include "webservice/WebService.h"

namespace nebula {

namespace storage {

class StorageServer final {
 public:
  StorageServer(HostAddr localHost,
                std::vector<HostAddr> metaAddrs,
                std::vector<std::string> dataPaths,
                std::string walPath = "",
                std::string listenerPath = "");

  // Return false if failed.
  bool start();

  void stop();

  // used for signal handler to set an internal stop flag
  // not allow any wait() in this
  void notifyStop();

  void waitUntilStop();

 private:
  enum ServiceStatus { STATUS_UNINITIALIZED = 0, STATUS_RUNNING = 1, STATUS_STOPPED = 2 };

 private:
  std::unique_ptr<kvstore::KVStore> getStoreInstance();

  /**
   * @brief Get the Admin Store Instance object (used for task manager)
   *
   * @return std::unique_ptr<kvstore::KVEngine>
   */
  std::unique_ptr<kvstore::KVEngine> getAdminStoreInstance();

  /**
   * @brief Get the Admin Store Seq Id object
   *
   * @return int32_t
   */
  int32_t getAdminStoreSeqId();

  bool initWebService();

  /**
   * @brief storage thrift server, mainly for graph query
   */
#ifndef BUILD_STANDALONE
  std::unique_ptr<apache::thrift::ThriftServer> getStorageServer();
#else
  std::shared_ptr<GraphStorageLocalServer> getStorageServer();
#endif

  /**
   * @brief admin thrift server, mainly for meta to control storage
   */
  std::unique_ptr<apache::thrift::ThriftServer> getAdminServer();

  /**
   * @brief internal thrift server, mainly for toss now
   */
  std::unique_ptr<apache::thrift::ThriftServer> getInternalServer();

  /**
   * @brief used by all thrift client, and kvstore.
   *        default num is 16
   */
  std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;
  std::shared_ptr<apache::thrift::concurrency::ThreadManager> workers_;

#ifndef BUILD_STANDALONE
  std::unique_ptr<apache::thrift::ThriftServer> storageServer_;
#else
  std::shared_ptr<GraphStorageLocalServer> storageServer_;
#endif
  std::unique_ptr<apache::thrift::ThriftServer> adminServer_;
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
