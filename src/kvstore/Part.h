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
         std::shared_ptr<RaftClient> clientMan);

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
    void reset() {
        LOG(INFO) << idStr_ << "Clean up all wals";
        wal()->reset();
        ResultCode res = engine_->remove(NebulaKeyUtils::systemCommitKey(partId_));
        if (res != ResultCode::SUCCEEDED) {
            LOG(WARNING) << idStr_ << "Remove the committedLogId failed, error "
                         << static_cast<int32_t>(res);
        }
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
        LOG(INFO) << idStr_ << "Clean up all data, just reset the committedLogId!";
        auto batch = engine_->startBatchWrite();
        if (ResultCode::SUCCEEDED != putCommitMsg(batch.get(), 0, 0)) {
            LOG(ERROR) << idStr_ << "Put failed in commit";
            return;
        }
        if (ResultCode::SUCCEEDED != engine_->commitBatchWrite(std::move(batch))) {
            LOG(ERROR) << idStr_ << "Put failed in commit";
            return;
        }
        return;
    }


    ResultCode toResultCode(raftex::AppendLogResult res);

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

