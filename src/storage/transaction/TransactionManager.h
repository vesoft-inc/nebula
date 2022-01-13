/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_TRANSACTIONMANAGER_H
#define STORAGE_TRANSACTION_TRANSACTIONMANAGER_H

#include <folly/executors/Async.h>
#include <storage/transaction/ChainBaseProcessor.h>

#include "clients/meta/MetaClient.h"
#include "clients/storage/InternalStorageClient.h"
#include "common/meta/SchemaManager.h"
#include "common/thrift/ThriftTypes.h"
#include "common/utils/MemoryLockCore.h"
#include "common/utils/MemoryLockWrapper.h"
#include "interface/gen-cpp2/storage_types.h"
#include "kvstore/KVStore.h"
#include "kvstore/Part.h"
#include "storage/CommonUtils.h"
#include "storage/transaction/ConsistUtil.h"

namespace nebula {
namespace storage {
class TransactionManager {
 public:
  FRIEND_TEST(ChainUpdateEdgeTest, updateTest1);
  friend class FakeInternalStorageClient;
  using LockGuard = MemoryLockGuard<std::string>;
  using LockCore = MemoryLockCore<std::string>;
  using UPtrLock = std::unique_ptr<LockCore>;

 public:
  explicit TransactionManager(storage::StorageEnv* env);

  ~TransactionManager() {
    stop();
  }

  void addChainTask(ChainBaseProcessor* proc) {
    folly::async([=] {
      proc->prepareLocal()
          .via(exec_.get())
          .thenValue([=](auto&& code) { return proc->processRemote(code); })
          .thenValue([=](auto&& code) { return proc->processLocal(code); })
          .ensure([=]() { proc->finish(); });
    });
  }

  folly::Executor* getExecutor() {
    return exec_.get();
  }

  bool start();

  void stop();

  LockCore* getLockCore(GraphSpaceID spaceId, PartitionID partId, bool checkWhiteList = true);

  InternalStorageClient* getInternalClient() {
    return iClient_;
  }

  // get term of part from kvstore, may fail if this part is not exist
  std::pair<TermID, nebula::cpp2::ErrorCode> getTerm(GraphSpaceID spaceId, PartitionID partId);

  // check get term from local term cache
  // this is used by Chain...RemoteProcessor,
  // to avoid an old leader request overrider a newer leader's
  bool checkTermFromCache(GraphSpaceID spaceId, PartitionID partId, TermID termId);

  void reportFailed();

  // leave a record for (double)prime edge, to let resume processor there is one dangling edge
  void addPrime(GraphSpaceID spaceId, const std::string& edgeKey, ResumeType type);

  void delPrime(GraphSpaceID spaceId, const std::string& edgeKey);

  bool checkUnfinishedEdge(GraphSpaceID spaceId, const folly::StringPiece& key);

  folly::ConcurrentHashMap<std::string, ResumeType>* getDangleEdges();

  void scanPrimes(GraphSpaceID spaceId, PartitionID partId);

  void scanAll();

 protected:
  void resumeThread();

  std::string makeLockKey(GraphSpaceID spaceId, const std::string& edge);

  std::string getEdgeKey(const std::string& lockKey);

  // this is a callback register to NebulaStore on new part added.
  void onNewPartAdded(std::shared_ptr<kvstore::Part>& part);

  // this is a callback register to Part::onElected
  void onLeaderElectedWrapper(const ::nebula::kvstore::Part::CallbackOptions& options);

  void onLeaderLostWrapper(const ::nebula::kvstore::Part::CallbackOptions& options);

 protected:
  using PartUUID = std::pair<GraphSpaceID, PartitionID>;

  StorageEnv* env_{nullptr};
  std::shared_ptr<folly::IOThreadPoolExecutor> exec_;
  InternalStorageClient* iClient_;
  folly::ConcurrentHashMap<GraphSpaceID, UPtrLock> memLocks_;
  folly::ConcurrentHashMap<PartUUID, TermID> cachedTerms_;
  std::unique_ptr<thread::GenericWorker> resumeThread_;

  /**
   * edges need to recover will put into this,
   * resume processor will get edge from this then do resume.
   * */
  folly::ConcurrentHashMap<std::string, ResumeType> dangleEdges_;

  /**
   * @brief every raft part need to do a scan,
   *        only scanned part allowed to insert edges
   */
  folly::ConcurrentHashMap<std::pair<GraphSpaceID, PartitionID>, int> scannedParts_;
};

}  // namespace storage
}  // namespace nebula
#endif
