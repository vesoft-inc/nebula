/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_PART_H_
#define KVSTORE_PART_H_

#include "common/base/Base.h"
#include "utils/NebulaKeyUtils.h"
#include "raftex/RaftPart.h"
#include "kvstore/Common.h"
#include "kvstore/KVEngine.h"
#include "kvstore/raftex/SnapshotManager.h"
#include "kvstore/wal/FileBasedWal.h"

namespace nebula {
namespace kvstore {

using RaftClient = thrift::ThriftClientManager<raftex::cpp2::RaftexServiceAsyncClient>;

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
         std::shared_ptr<raftex::SnapshotManager> snapshotMan,
         std::shared_ptr<RaftClient> clientMan,
         std::shared_ptr<DiskManager> diskMan);

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
    void asyncRemoveRange(folly::StringPiece start,
                          folly::StringPiece end,
                          KVCallback cb);

    void asyncAppendBatch(std::string&& batch, KVCallback cb);

    void asyncAtomicOp(raftex::AtomicOp op, KVCallback cb);

    void asyncAddLearner(const HostAddr& learner, KVCallback cb);

    void asyncTransferLeader(const HostAddr& target, KVCallback cb);

    void asyncAddPeer(const HostAddr& peer, KVCallback cb);

    void asyncRemovePeer(const HostAddr& peer, KVCallback cb);

    void setBlocking(bool sign);

    // Sync the information committed on follower.
    void sync(KVCallback cb);

    void registerNewLeaderCb(NewLeaderCallback cb) {
        newLeaderCb_ = std::move(cb);
    }

    void unRegisterNewLeaderCb() {
        newLeaderCb_ = nullptr;
    }

    // clean up all data about this part.
    void resetPart() {
        std::lock_guard<std::mutex> g(raftLock_);
        reset();
    }

private:
    /**
     * Methods inherited from RaftPart
     */
    std::pair<LogID, TermID> lastCommittedLogId() override;

    void onLostLeadership(TermID term) override;

    void onElected(TermID term) override;

    void onDiscoverNewLeader(HostAddr nLeader) override;

    cpp2::ErrorCode commitLogs(std::unique_ptr<LogIterator> iter, bool wait) override;

    bool preProcessLog(LogID logId,
                       TermID termId,
                       ClusterID clusterId,
                       const std::string& log) override;

    std::pair<int64_t, int64_t> commitSnapshot(const std::vector<std::string>& data,
                                               LogID committedLogId,
                                               TermID committedLogTerm,
                                               bool finished) override;

    nebula::cpp2::ErrorCode
    putCommitMsg(WriteBatch* batch, LogID committedLogId, TermID committedLogTerm);

    void cleanup() override;

    nebula::cpp2::ErrorCode toResultCode(raftex::AppendLogResult res);

protected:
    GraphSpaceID spaceId_;
    PartitionID partId_;
    std::string walPath_;
    NewLeaderCallback newLeaderCb_ = nullptr;

private:
    KVEngine* engine_ = nullptr;
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_PART_H_

