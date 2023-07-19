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
  friend class TransactionManagerTester;
  using LockGuard = MemoryLockGuard<std::string>;
  using LockCore = MemoryLockCore<std::string>;
  using SPtrLock = std::shared_ptr<LockCore>;

 public:
  explicit TransactionManager(storage::StorageEnv* env);

  ~TransactionManager() {
    stop();
    join();
  }

  bool start();

  void stop();

  /**
   * @brief wait until stop
   */
  void join();

  /**
   * @brief add a new processor to do "chain" work,
   *        using the internal executor of transaction manager.
   *
   * @param proc
   */
  void addChainTask(ChainBaseProcessor* proc);

  /**
   * @brief Get the Lock Core object to set a memory lock for a key.
   *
   * @param spaceId
   * @param partId
   * @param termId
   * @param checkWhiteList caller outside TransactionManager have to set this true.
   * @return nullptr if failed.
   */
  SPtrLock getLockCore(GraphSpaceID spaceId,
                       PartitionID partId,
                       TermID termId,
                       bool checkWhiteList = true);

  // get term of part from kvstore, may fail if this part is not exist
  std::pair<TermID, nebula::cpp2::ErrorCode> getTermFromKVStore(GraphSpaceID spaceId,
                                                                PartitionID partId);

  // check get term from local term cache
  // this is used by Chain...RemoteProcessor,
  // to avoid an old leader request overrider a newer leader's
  bool checkTermFromCache(GraphSpaceID spaceId, PartitionID partId, TermID termId);

  // leave a record for (double)prime edge, to let resume processor there is one dangling edge
  void addPrime(GraphSpaceID spaceId,
                PartitionID partId,
                TermID termId,
                const std::string& edgeKey,
                ResumeType type);

  // delete a prime record when recover succeeded.
  void delPrime(GraphSpaceID spaceId,
                PartitionID partId,
                TermID termId,
                const std::string& edgeKey);

  /**
   * @brief need to do a scan to let all prime(double prime) set a memory lock,
   *        before a partition start to serve.
   *        otherwise, if a new request comes, it will overwrite the existing lock.
   * @param spaceId
   * @param partId
   */
  void scanPrimes(GraphSpaceID spaceId, PartitionID partId, TermID termId);

  /**
   * @brief Get the an Event Base object from its internal executor
   *
   * @return folly::EventBase*
   */
  folly::EventBase* getEventBase();

  /**
   * @brief stat thread, used for debug
   */
  void monitorPoolStat(folly::ThreadPoolExecutor* pool, const std::string& msg);
  void bgPrintPoolStat();
  std::string dumpPoolStat(folly::ThreadPoolExecutor* pool, const std::string& msg);

  bool stop_{false};
  std::vector<std::pair<folly::ThreadPoolExecutor*, std::string>> monPoolStats_;

 protected:
  // this is a callback register to NebulaStore on new part added.
  void onNewPartAdded(std::shared_ptr<kvstore::Part>& part);

  // this is a callback register to Part::onElected
  void onLeaderElectedWrapper(const ::nebula::kvstore::Part::CallbackOptions& options);

  // this is a callback register to Part::onLostLeadership
  void onLeaderLostWrapper(const ::nebula::kvstore::Part::CallbackOptions& options);

  void waitUntil(std::function<bool()>&& cond, folly::Promise<folly::Unit>&& p);

 protected:
  using SpacePart = std::pair<GraphSpaceID, PartitionID>;

  StorageEnv* env_{nullptr};
  // real executor to run recover / normal jobs
  std::shared_ptr<folly::IOThreadPoolExecutor> worker_;
  // only used for waiting some job stop.
  std::shared_ptr<folly::IOThreadPoolExecutor> controller_;

  /**
   * @brief used for a remote processor to record the term of its "local Processor"
   */
  folly::ConcurrentHashMap<SpacePart, TermID> cachedTerms_;

  using MemLockKey = std::tuple<GraphSpaceID, PartitionID, TermID>;
  folly::ConcurrentHashMap<MemLockKey, SPtrLock> memLocks_;

  /**
   * @brief every raft part need to do a scan,
   *        only scanned part allowed to insert edges
   */
  folly::ConcurrentHashMap<SpacePart, TermID> currTerm_;

  folly::ConcurrentHashMap<SpacePart, TermID> prevTerms_;
};

}  // namespace storage
}  // namespace nebula
#endif
