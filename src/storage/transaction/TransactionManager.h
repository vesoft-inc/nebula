/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_TRANSACTION_TRANSACTIONMGR_H_
#define STORAGE_TRANSACTION_TRANSACTIONMGR_H_

#include <folly/Function.h>
#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/Synchronized.h>

#include "common/clients/meta/MetaClient.h"
#include "common/clients/storage/GraphStorageClient.h"
#include "common/clients/storage/InternalStorageClient.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/meta/SchemaManager.h"
#include "common/thrift/ThriftTypes.h"
#include "kvstore/KVStore.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/transaction/TransactionUtils.h"
#include "utils/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

using KV = std::pair<std::string, std::string>;
using RawKeys = std::vector<std::string>;
using MemEdgeLocks = folly::ConcurrentHashMap<std::string, int64_t>;
using ResumedResult = std::shared_ptr<folly::Synchronized<KV>>;
using GetBatchFunc = std::function<folly::Optional<std::string>()>;

class TransactionManager {
public:
    explicit TransactionManager(storage::StorageEnv* env);

    ~TransactionManager() = default;

    /**
     * @brief edges have same localPart and remotePart will share
     *        one signle RPC request
     * @param localEdges
     *        <K, encodedValue>.
     * @param processor
     *        will set this if edge have index
     * @param optBatchGetter
     *        get a batch of raft operations.
     *        used by updateNode, need to run this func after edge locked
     * */
    folly::Future<nebula::cpp2::ErrorCode> addSamePartEdges(
        size_t vIdLen,
        GraphSpaceID spaceId,
        PartitionID localPart,
        PartitionID remotePart,
        std::vector<KV>& localEdges,
        AddEdgesProcessor* processor = nullptr,
        folly::Optional<GetBatchFunc> optBatchGetter = folly::none);

    /**
     * @brief update out-edge first, then in-edge
     * @param batchGetter
     *        need to update index & edge together, and exactly the same verion
     *        which means we need a lock before doing anything.
     *        as this method will call addSamePartEdges(),
     *        and addSamePartEdges() will set a lock to edge,
     *        I would like to forward this function to addSamePartEdges()
     * */
    folly::Future<nebula::cpp2::ErrorCode>
    updateEdgeAtomic(size_t vIdLen,
                     GraphSpaceID spaceId,
                     PartitionID partId,
                     const cpp2::EdgeKey& edgeKey,
                     GetBatchFunc batchGetter);

    /*
     * resume an unfinished add/update/upsert request
     * 1. if in-edge commited, will commit out-edge
     *       else, will remove lock
     * 2. if mvcc enabled, will commit the value of lock
     *       else, get props from in-edge, then re-check index and commit
     * */
    folly::Future<nebula::cpp2::ErrorCode>
    resumeTransaction(size_t vIdLen,
                      GraphSpaceID spaceId,
                      std::string lockKey,
                      ResumedResult result = nullptr);

    folly::SemiFuture<nebula::cpp2::ErrorCode>
    commitBatch(GraphSpaceID spaceId,
                PartitionID partId,
                std::string&& batch);

    bool enableToss(GraphSpaceID spaceId) {
        return nebula::meta::cpp2::IsolationLevel::TOSS == getSpaceIsolationLvel(spaceId);
    }

    folly::Executor* getExecutor() { return exec_.get(); }

    // used for perf trace, will remove finally
    std::unordered_map<std::string, std::list<int64_t>> timer_;

protected:
    folly::SemiFuture<nebula::cpp2::ErrorCode>
    commitEdgeOut(GraphSpaceID spaceId,
                  PartitionID partId,
                  std::string&& key,
                  std::string&& props);

    folly::SemiFuture<nebula::cpp2::ErrorCode>
    commitEdge(GraphSpaceID spaceId,
               PartitionID partId,
               std::string& key,
               std::string& encodedProp);

    folly::SemiFuture<nebula::cpp2::ErrorCode>
    eraseKey(GraphSpaceID spaceId, PartitionID partId, const std::string& key);

    void eraseMemoryLock(const std::string& rawKey, int64_t ver);

    nebula::meta::cpp2::IsolationLevel getSpaceIsolationLvel(GraphSpaceID spaceId);

    std::string encodeBatch(std::vector<KV>&& data);

protected:
    StorageEnv*                                         env_{nullptr};
    std::shared_ptr<folly::IOThreadPoolExecutor>        exec_;
    std::unique_ptr<storage::InternalStorageClient>     interClient_;
    MemEdgeLocks                                        memLock_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_TRANSACTION_TRANSACTIONMGR_H_
