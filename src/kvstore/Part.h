/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_PART_H_
#define KVSTORE_PART_H_

#include "base/Base.h"
#include "raftex/RaftPart.h"
#include "kvstore/Common.h"
#include "kvstore/KVEngine.h"
#include "kvstore/raftex/SnapshotManager.h"

namespace nebula {
namespace kvstore {


class Part : public raftex::RaftPart {
    friend class SnapshotManager;
public:
    Part(GraphSpaceID spaceId,
         PartitionID partId,
         HostAddr localAddr,
         const std::string& walPath,
         KVEngine* engine,
         std::shared_ptr<folly::IOThreadPoolExecutor> pool,
         std::shared_ptr<thread::GenericThreadPool> workers,
         std::shared_ptr<folly::Executor> handlers,
         std::shared_ptr<raftex::SnapshotManager> snapshotMan);

    virtual ~Part() {
        LOG(INFO) << idStr_ << "~Part()";
    }

    KVEngine* engine() {
        return engine_;
    }

    void asyncPut(folly::StringPiece key, folly::StringPiece value, KVCallback cb);
    void asyncMultiPut(const std::vector<KV>& keyValues, KVCallback cb);

    void asyncRemove(folly::StringPiece key, KVCallback cb);
    void asyncMultiRemove(const std::vector<std::string>& keys, KVCallback cb);
    void asyncRemovePrefix(folly::StringPiece prefix, KVCallback cb);
    void asyncRemoveRange(folly::StringPiece start,
                          folly::StringPiece end,
                          KVCallback cb);

    void asyncAtomicOp(raftex::AtomicOp op, KVCallback cb);

    void asyncAddLearner(const HostAddr& learner, KVCallback cb);

    void asyncTransferLeader(const HostAddr& target, KVCallback cb);

    void registerNewLeaderCb(NewLeaderCallback cb) {
        newLeaderCb_ = std::move(cb);
    }

    void unRegisterNewLeaderCb() {
        newLeaderCb_ = nullptr;
    }

private:
    /**
     * Methods inherited from RaftPart
     */
    std::pair<LogID, TermID> lastCommittedLogId() override;

    void onLostLeadership(TermID term) override;

    void onElected(TermID term) override;

    void onDiscoverNewLeader(HostAddr nLeader) override;

    bool commitLogs(std::unique_ptr<LogIterator> iter) override;

    bool preProcessLog(LogID logId,
                       TermID termId,
                       ClusterID clusterId,
                       const std::string& log) override;

    std::pair<int64_t, int64_t> commitSnapshot(const std::vector<std::string>& data,
                                               LogID committedLogId,
                                               TermID committedLogTerm,
                                               bool finished) override;

    ResultCode putCommitMsg(WriteBatch* batch, LogID committedLogId, TermID committedLogTerm);

    void cleanup() override {
        LOG(INFO) << idStr_ << "Clean up all data, not implement!";
    }

protected:
    GraphSpaceID spaceId_;
    PartitionID partId_;
    std::string walPath_;
    KVEngine* engine_ = nullptr;
    NewLeaderCallback newLeaderCb_ = nullptr;
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_PART_H_

