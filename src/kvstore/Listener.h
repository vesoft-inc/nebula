/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_LISTENER_H_
#define KVSTORE_LISTENER_H_

#include "common/base/Base.h"
#include "common/meta/SchemaManager.h"
#include "kvstore/Common.h"
#include "kvstore/raftex/RaftPart.h"
#include "kvstore/raftex/Host.h"
#include "kvstore/wal/FileBasedWal.h"

DECLARE_int32(cluster_id);

namespace nebula {
namespace kvstore {

using RaftClient = thrift::ThriftClientManager<raftex::cpp2::RaftexServiceAsyncClient>;

/*
Listener is a special RaftPart, it will join the raft group as learner. The key difference between
learner (e.g. in balance data) and listener is that, listener won't be able to be promoted to
follower or leader.

Once it joins raft group, it can receive logs from leader and do whatever you like. When listener
receives logs from leader, it will just write wal and update committedLogId_, none of the logs is
applied to state machine, which is different from follower/learner. There will be another thread
trigger the apply within a certain interval (listener_commit_interval_secs), it will get the logs
in range of [lastApplyLogId_ + 1, committedLogId_], and decode these logs into kv, and apply them
to state machine.

If you want to add a new type of listener, just inherit from Listener. There are some interface you
need to implement, some of them has been implemented in Listener. Others need to impelemented in
derived class.

* Implemented in Listener, could override if necessary
    // Start listener as learner
    void start(std::vector<HostAddr>&& peers, bool asLearner)

    // Stop the listener
    void stop()

    void onLostLeadership(TermID term)

    void onElected(TermID term)

    void onDiscoverNewLeader(HostAddr nLeader)

    // For listener, we just return true directly. Another background thread trigger the actual
    // apply work, and do it in worker thread, and update lastApplyLogId_
    cpp2::Errorcode commitLogs(std::unique_ptr<LogIterator> iter, bool)

    // For most of the listeners, just return true is enough. However, if listener need to be aware
    // of membership change, some log type of wal need to be pre-processed, could do it here.
    bool preProcessLog(LogID logId, TermID termId, ClusterID clusterId, const std::string& log)

    // If listener falls far behind from leader, leader would send snapshot to listener. The
    // snapshot is a vector of kv, listener could decode them and treat them as normal logs until
    // all snapshot has been received.
    std::pair<int64_t, int64_t> commitSnapshot(const std::vector<std::string>& data,
                                               LogID committedLogId,
                                               TermID committedLogTerm,
                                               bool finished) override;

* Must implement in derived class
    // extra initialize work could do here
    void init()

    // read last commit log id and term from external storage, used in initialization
    std::pair<LogID, TermID> lastCommittedLogId()

    // read last apply id from external storage, used in initialization
    LogID lastApplyLogId()

    // apply the kv to state machine
    bool apply(const std::vector<KV>& data)

    // persist last commit log id/term and lastApplyId
    bool persist(LogID, TermID, LogID)

    // extra cleanup work, will be invoked when listener is about to be removed, or raft is reseted
    virtual void cleanup() = 0
*/

class Listener : public raftex::RaftPart {
public:
    Listener(GraphSpaceID spaceId,
             PartitionID partId,
             HostAddr localAddr,
             const std::string& walPath,
             std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
             std::shared_ptr<thread::GenericThreadPool> workers,
             std::shared_ptr<folly::Executor> handlers,
             std::shared_ptr<raftex::SnapshotManager> snapshotMan,
             std::shared_ptr<RaftClient> clientMan,
             std::shared_ptr<DiskManager> diskMan,
             meta::SchemaManager* schemaMan);

    // Initialize listener, all Listener must call this method
    void start(std::vector<HostAddr>&& peers, bool asLearner = true) override;

    // Stop listener
    void stop() override;

    LogID getApplyId() {
        return lastApplyLogId_;
    }

    void cleanup() override {
        CHECK(!raftLock_.try_lock());
        leaderCommitId_ = 0;
        lastApplyLogId_ = 0;
        persist(0, 0, lastApplyLogId_);
    }

    void resetListener();

    bool pursueLeaderDone();

protected:
    virtual void init() = 0;

    // Last apply id, need to be persisted, used in initialization
    virtual LogID lastApplyLogId() = 0;

    virtual bool apply(const std::vector<KV>& data) = 0;

    virtual bool persist(LogID, TermID, LogID) = 0;

    void onLostLeadership(TermID) override {
        LOG(FATAL) << "Should not reach here";
    }

    void onElected(TermID) override {
        LOG(FATAL) << "Should not reach here";
    }

    void onDiscoverNewLeader(HostAddr nLeader) override {
        LOG(INFO) << idStr_ << "Find the new leader " << nLeader;
    }

    raftex::cpp2::ErrorCode checkPeer(const HostAddr& candidate) override {
        CHECK(!raftLock_.try_lock());
        if (peers_.find(candidate) == peers_.end()) {
            LOG(WARNING) << idStr_ << "The candidate " << candidate << " is not in my peers";
            return raftex::cpp2::ErrorCode::E_WRONG_LEADER;
        }
        return raftex::cpp2::ErrorCode::SUCCEEDED;
    }

    // For listener, we just return true directly. Another background thread trigger the actual
    // apply work, and do it in worker thread, and update lastApplyLogId_
    cpp2::ErrorCode commitLogs(std::unique_ptr<LogIterator>, bool) override;

    // For most of the listeners, just return true is enough. However, if listener need to be aware
    // of membership change, some log type of wal need to be pre-processed, could do it here.
    bool preProcessLog(LogID logId,
                       TermID termId,
                       ClusterID clusterId,
                       const std::string& log) override;

    // If the listener falls behind way to much than leader, the leader will send all its data
    // in snapshot by batch, listener need to implement it if it need handle this case. The return
    // value is a pair of <logs count, logs size> of this batch.
    std::pair<int64_t, int64_t> commitSnapshot(const std::vector<std::string>& data,
                                               LogID committedLogId,
                                               TermID committedLogTerm,
                                               bool finished) override;

    void doApply();

protected:
    LogID leaderCommitId_ = 0;
    LogID lastApplyLogId_ = 0;
    int64_t lastApplyTime_ = 0;
    std::set<HostAddr> peers_;
    meta::SchemaManager* schemaMan_{nullptr};
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_LISTENER_H_
