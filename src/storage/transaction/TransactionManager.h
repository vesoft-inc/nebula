/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

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

  ~TransactionManager() = default;

  void addChainTask(ChainBaseProcessor* proc) {
    folly::async([=] {
      proc->prepareLocal()
          .via(exec_.get())
          .thenValue([=](auto&& code) { return proc->processRemote(code); })
          .thenValue([=](auto&& code) { return proc->processLocal(code); })
          .ensure([=]() { proc->finish(); });
    });
  }

  folly::Executor* getExecutor() { return exec_.get(); }

  LockCore* getLockCore(GraphSpaceID spaceId, PartitionID partId, bool checkWhiteList = true);

  InternalStorageClient* getInternalClient() { return iClient_; }

  StatusOr<TermID> getTerm(GraphSpaceID spaceId, PartitionID partId);

  bool checkTerm(GraphSpaceID spaceId, PartitionID partId, TermID term);

  bool start();

  void stop();

  // leave a record for (double)prime edge, to let resume processor there is one dangling edge
  void addPrime(GraphSpaceID spaceId, const std::string& edgeKey, ResumeType type);

  void delPrime(GraphSpaceID spaceId, const std::string& edgeKey);

  bool checkUnfinishedEdge(GraphSpaceID spaceId, const folly::StringPiece& key);

  // return false if there is no "edge" in reserveTable_
  //        true if there is, and also erase the edge from reserveTable_.
  bool takeDanglingEdge(GraphSpaceID spaceId, const std::string& edge);

  folly::ConcurrentHashMap<std::string, ResumeType>* getReserveTable();

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
   * an update request may re-entered to an existing (double)prime key
   * and wants to have its own (double)prime.
   * also MVCC doesn't work.
   * because (double)prime can't judge if remote side succeeded.
   * to prevent insert/update re
   * */
  folly::ConcurrentHashMap<std::string, ResumeType> reserveTable_;

  /**
   * @brief only part in this white list allowed to get lock
   */
  folly::ConcurrentHashMap<std::pair<GraphSpaceID, PartitionID>, int> whiteListParts_;
};

}  // namespace storage
}  // namespace nebula
