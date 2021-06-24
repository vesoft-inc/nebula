/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef RAFTEX_RAFTPART_H_
#define RAFTEX_RAFTPART_H_

#include "common/base/Base.h"
#include "common/interface/gen-cpp2/raftex_types.h"
#include "common/interface/gen-cpp2/RaftexServiceAsyncClient.h"
#include "common/time/Duration.h"
#include "common/thread/GenericThreadPool.h"
#include "kvstore/Common.h"
#include "kvstore/raftex/SnapshotManager.h"
#include "kvstore/DiskManager.h"
#include <folly/futures/SharedPromise.h>
#include <folly/Function.h>
#include <gtest/gtest_prod.h>
#include "utils/LogIterator.h"

namespace folly {
class IOThreadPoolExecutor;
class EventBase;
}  // namespace folly

namespace nebula {

namespace wal {
class FileBasedWal;
}  // namespace wal


namespace raftex {

enum class AppendLogResult {
    SUCCEEDED = 0,
    E_ATOMIC_OP_FAILURE = -1,
    E_NOT_A_LEADER = -2,
    E_STOPPED = -3,
    E_NOT_READY = -4,
    E_BUFFER_OVERFLOW = -5,
    E_WAL_FAILURE = -6,
    E_TERM_OUT_OF_DATE = -7,
    E_SENDING_SNAPSHOT = -8,
    E_INVALID_PEER = -9,
    E_NOT_ENOUGH_ACKS = -10,
    E_WRITE_BLOCKING = -11,
};

enum class LogType {
    NORMAL      = 0x00,
    ATOMIC_OP   = 0x01,
    /**
      COMMAND is similar to AtomicOp, but not the same. There are two differences:
      1. Normal logs after AtomicOp could be committed together. In opposite, Normal logs
         after COMMAND should be hold until the COMMAND committed, but the logs before
         COMMAND could be committed together.
      2. AtomicOp maybe failed. So we use SinglePromise for it. But COMMAND not, so it could
         share one promise with the normal logs before it.
     * */
    COMMAND     = 0x02,
};

class Host;
class AppendLogsIterator;

/**
 * The operation will be atomic, if the operation failed, empty string will be returned,
 * otherwise it will return the new operation's encoded string whick should be applied atomically.
 * You could implement CAS, READ-MODIFY-WRITE operations though it.
 * */
using AtomicOp = folly::Function<folly::Optional<std::string>(void)>;

class RaftPart : public std::enable_shared_from_this<RaftPart> {
    friend class AppendLogsIterator;
    friend class Host;
    friend class SnapshotManager;
    FRIEND_TEST(MemberChangeTest, AddRemovePeerTest);
    FRIEND_TEST(MemberChangeTest, RemoveLeaderTest);

public:
    virtual ~RaftPart();

    bool isRunning() const {
        std::lock_guard<std::mutex> g(raftLock_);
        return status_ == Status::RUNNING;
    }

    bool isStopped() const {
        std::lock_guard<std::mutex> g(raftLock_);
        return status_ == Status::STOPPED;
    }

    bool isLeader() const {
        std::lock_guard<std::mutex> g(raftLock_);
        return role_ == Role::LEADER;
    }

    bool isFollower() const {
        std::lock_guard<std::mutex> g(raftLock_);
        return role_ == Role::FOLLOWER;
    }

    bool isLearner() const {
        std::lock_guard<std::mutex> g(raftLock_);
        return role_ == Role::LEARNER;
    }

    ClusterID clusterId() const {
        return clusterId_;
    }

    GraphSpaceID spaceId() const {
        return spaceId_;
    }

    PartitionID partitionId() const {
        return partId_;
    }

    const HostAddr& address() const {
        return addr_;
    }

    HostAddr leader() const {
        std::lock_guard<std::mutex> g(raftLock_);
        return leader_;
    }

    TermID termId() const {
        return term_;
    }

    std::shared_ptr<wal::FileBasedWal> wal() const {
        return wal_;
    }

    void addLearner(const HostAddr& learner);

    void commitTransLeader(const HostAddr& target);

    void preProcessTransLeader(const HostAddr& target);


    void preProcessRemovePeer(const HostAddr& peer);

    void commitRemovePeer(const HostAddr& peer);

    void addListenerPeer(const HostAddr& peer);

    void removeListenerPeer(const HostAddr& peer);

    // Change the partition status to RUNNING. This is called
    // by the inherited class, when it's ready to serve
    virtual void start(std::vector<HostAddr>&& peers, bool asLearner = false);

    // Change the partition status to STOPPED. This is called
    // by the inherited class, when it's about to stop
    virtual void stop();


    /*****************************************************************
     * Asynchronously append a log
     *
     * This is the **PUBLIC** Log Append API, used by storage
     * service
     *
     * The method will take the ownership of the log and returns
     * as soon as possible. Internally it will asynchronously try
     * to send the log to all followers. It will keep trying until
     * majority of followers accept the log, then the future will
     * be fulfilled
     *
     * If the source == -1, the current clusterId will be used
     ****************************************************************/
    folly::Future<AppendLogResult> appendAsync(ClusterID source, std::string log);

    /****************************************************************
     * Run the op atomically.
     ***************************************************************/
    folly::Future<AppendLogResult> atomicOpAsync(AtomicOp op);

    /**
     * Asynchronously send one command.
     * */
    folly::Future<AppendLogResult> sendCommandAsync(std::string log);

    /**
     * Check if the peer has catched up data from leader. If leader is sending the snapshot,
     * the method will return false.
     * */
    AppendLogResult isCatchedUp(const HostAddr& peer);

    bool linkCurrentWAL(const char* newPath);

    /**
     * Reset my peers if not equals the argument
     */
    void checkAndResetPeers(const std::vector<HostAddr>& peers);

    /**
     * Add listener into peers or remove from peers
     */
    void checkRemoteListeners(const std::set<HostAddr>& listeners);

    /*****************************************************
     *
     * Methods to process incoming raft requests
     *
     ****************************************************/
    // Process the incoming leader election request
    void processAskForVoteRequest(
        const cpp2::AskForVoteRequest& req,
        cpp2::AskForVoteResponse& resp);

    // Process appendLog request
    void processAppendLogRequest(
        const cpp2::AppendLogRequest& req,
        cpp2::AppendLogResponse& resp);

    // Process sendSnapshot request
    void processSendSnapshotRequest(
        const cpp2::SendSnapshotRequest& req,
        cpp2::SendSnapshotResponse& resp);

    void processHeartbeatRequest(
        const cpp2::HeartbeatRequest& req,
        cpp2::HeartbeatResponse& resp);

    bool leaseValid();

    bool needToCleanWal();

    // leader + follwers
    std::vector<HostAddr> peers() const;

    std::set<HostAddr> listeners() const;

    std::pair<LogID, TermID> lastLogInfo() const;

    // Reset the part, clean up all data and WALs.
    void reset();

protected:
    // Protected constructor to prevent from instantiating directly
    RaftPart(
         ClusterID clusterId,
         GraphSpaceID spaceId,
         PartitionID partId,
         HostAddr localAddr,
         const folly::StringPiece walRoot,
         std::shared_ptr<folly::IOThreadPoolExecutor> pool,
         std::shared_ptr<thread::GenericThreadPool> workers,
         std::shared_ptr<folly::Executor> executor,
         std::shared_ptr<SnapshotManager> snapshotMan,
         std::shared_ptr<thrift::ThriftClientManager<cpp2::RaftexServiceAsyncClient>> clientMan,
         std::shared_ptr<kvstore::DiskManager> diskMan);

    enum class Status {
        STARTING = 0,   // The part is starting, not ready for service
        RUNNING,        // The part is running
        STOPPED,        // The part has been stopped
        WAITING_SNAPSHOT  // Waiting for the snapshot.
    };

    enum class Role {
        LEADER = 1,     // the leader
        FOLLOWER,       // following a leader
        CANDIDATE,      // Has sent AskForVote request
        LEARNER         // It is the same with FOLLOWER,
                        // except it does not participate in leader election
    };

    const char* idStr() const {
        return idStr_.c_str();
    }

    // The method will be invoked by start()
    //
    // Inherited classes should implement this method to provide the last
    // committed log id
    virtual std::pair<LogID, TermID> lastCommittedLogId() = 0;

    // This method is called when this partition's leader term
    // is finished, either by receiving a new leader election
    // request, or a new leader heartbeat
    virtual void onLostLeadership(TermID term) = 0;

    // This method is called when this partition is elected as
    // a new leader
    virtual void onElected(TermID term) = 0;

    virtual void onDiscoverNewLeader(HostAddr nLeader) = 0;

    // Check if we can accept candidate's message
    virtual cpp2::ErrorCode checkPeer(const HostAddr& candidate);

    // The inherited classes need to implement this method to commit
    // a batch of log messages
    virtual nebula::cpp2::ErrorCode commitLogs(std::unique_ptr<LogIterator> iter, bool wait) = 0;

    virtual bool preProcessLog(LogID logId,
                               TermID termId,
                               ClusterID clusterId,
                               const std::string& log) = 0;

    // Return <size, count> committed;
    virtual std::pair<int64_t, int64_t> commitSnapshot(const std::vector<std::string>& data,
                                                       LogID committedLogId,
                                                       TermID committedLogTerm,
                                                       bool finished) = 0;

    // Clean up extra data about the part, usually related to state machine
    virtual void cleanup() = 0;

    void addPeer(const HostAddr& peer);

    void removePeer(const HostAddr& peer);

private:
    // A list of <idx, resp>
    // idx  -- the index of the peer
    // resp -- coresponding response of peer[index]
    using ElectionResponses = std::vector<std::pair<size_t, cpp2::AskForVoteResponse>>;
    using AppendLogResponses = std::vector<std::pair<size_t, cpp2::AppendLogResponse>>;
    using HeartbeatResponses = std::vector<std::pair<size_t, cpp2::HeartbeatResponse>>;

    // <source, logType, log>
    using LogCache = std::vector<
        std::tuple<ClusterID,
                   LogType,
                   std::string,
                   AtomicOp>>;


    /****************************************************
     *
     * Private methods
     *
     ***************************************************/
    const char* roleStr(Role role) const;

    template<typename REQ>
    cpp2::ErrorCode verifyLeader(const REQ& req);

    /*****************************************************************
     *
     * Asynchronously send a heartbeat
     *
     ****************************************************************/
    void sendHeartbeat();

    /****************************************************
     *
     * Methods used by the status polling logic
     *
     ***************************************************/
    bool needToSendHeartbeat();

    bool needToStartElection();

    void statusPolling(int64_t startTime);

    bool needToCleanupSnapshot();

    void cleanupSnapshot();

    // The method sends out AskForVote request
    // It return true if a leader is elected, otherwise returns false
    bool leaderElection();

    // The method will fill up the request object and return TRUE
    // if the election should continue. Otherwise the method will
    // return FALSE
    bool prepareElectionRequest(
        cpp2::AskForVoteRequest& req,
        std::vector<std::shared_ptr<Host>>& hosts);

    // The method returns the partition's role after the election
    Role processElectionResponses(const ElectionResponses& results,
                                  std::vector<std::shared_ptr<Host>> hosts,
                                  TermID proposedTerm);

    // Check whether new logs can be appended
    // Pre-condition: The caller needs to hold the raftLock_
    AppendLogResult canAppendLogs();

    // Also check if term has changed
    // Pre-condition: The caller needs to hold the raftLock_
    AppendLogResult canAppendLogs(TermID currTerm);

    folly::Future<AppendLogResult> appendLogAsync(ClusterID source,
                                                  LogType logType,
                                                  std::string log,
                                                  AtomicOp cb = nullptr);

    void appendLogsInternal(AppendLogsIterator iter, TermID termId);

    void replicateLogs(
        folly::EventBase* eb,
        AppendLogsIterator iter,
        TermID currTerm,
        LogID lastLogId,
        LogID committedId,
        TermID prevLogTerm,
        LogID prevLogId);

    void processAppendLogResponses(
        const AppendLogResponses& resps,
        folly::EventBase* eb,
        AppendLogsIterator iter,
        TermID currTerm,
        LogID lastLogId,
        LogID committedId,
        TermID prevLogTerm,
        LogID prevLogId,
        std::vector<std::shared_ptr<Host>> hosts);

    // followers return Host of which could vote, in other words, learner is not counted in
    std::vector<std::shared_ptr<Host>> followers() const;

    bool checkAppendLogResult(AppendLogResult res);

    void updateQuorum();

protected:
    template<class ValueType>
    class PromiseSet final {
    public:
        PromiseSet() = default;
        PromiseSet(const PromiseSet&) = delete;
        PromiseSet(PromiseSet&&) = default;

        ~PromiseSet()  = default;

        PromiseSet& operator=(const PromiseSet&) = delete;
        PromiseSet& operator=(PromiseSet&& right) = default;

        void reset() {
            sharedPromises_.clear();
            singlePromises_.clear();
            rollSharedPromise_ = true;
        }

        folly::Future<ValueType> getSharedFuture() {
            if (rollSharedPromise_) {
                sharedPromises_.emplace_back();
                rollSharedPromise_ = false;
            }

            return sharedPromises_.back().getFuture();
        }

        folly::Future<ValueType> getSingleFuture() {
            singlePromises_.emplace_back();
            rollSharedPromise_ = true;

            return singlePromises_.back().getFuture();
        }

        folly::Future<ValueType> getAndRollSharedFuture() {
            if (rollSharedPromise_) {
                sharedPromises_.emplace_back();
            }
            rollSharedPromise_ = true;
            return sharedPromises_.back().getFuture();
        }

        template<class VT>
        void setOneSharedValue(VT&& val) {
            CHECK(!sharedPromises_.empty());
            sharedPromises_.front().setValue(std::forward<VT>(val));
            sharedPromises_.pop_front();
        }

        template<class VT>
        void setOneSingleValue(VT&& val) {
            CHECK(!singlePromises_.empty());
            singlePromises_.front().setValue(std::forward<VT>(val));
            singlePromises_.pop_front();
        }

        void setValue(ValueType val) {
            for (auto& p : sharedPromises_) {
                p.setValue(val);
            }
            for (auto& p : singlePromises_) {
                p.setValue(val);
            }
        }


    private:
        // Whether the last future was returned from a shared promise
        bool rollSharedPromise_{true};

        // Promises shared by continuous non atomic op logs
        std::list<folly::SharedPromise<ValueType>> sharedPromises_;
        // A list of promises for atomic op logs
        std::list<folly::Promise<ValueType>> singlePromises_;
    };


    const std::string idStr_;

    const ClusterID clusterId_;
    const GraphSpaceID spaceId_;
    const PartitionID partId_;
    const HostAddr addr_;
    // hosts_ contains all connection, hosts_ = all peers and listeners
    std::vector<std::shared_ptr<Host>> hosts_;
    size_t quorum_{0};

    // all listener's role is learner (cannot promote to follower)
    std::set<HostAddr> listeners_;

    // The lock is used to protect logs_ and cachingPromise_
    mutable std::mutex logsLock_;
    std::atomic_bool replicatingLogs_{false};
    std::atomic_bool bufferOverFlow_{false};
    PromiseSet<AppendLogResult> cachingPromise_;
    LogCache logs_;

    // Partition level lock to synchronize the access of the partition
    mutable std::mutex raftLock_;

    PromiseSet<AppendLogResult> sendingPromise_;

    Status status_;
    Role role_;

    // When the partition is the leader, the leader_ is same as addr_
    HostAddr leader_;

    // After voted for somebody, it will not be empty anymore.
    // And it will be reset to empty after current election finished.
    HostAddr votedAddr_;

    // The current term id
    // the term id proposed by that candidate
    TermID term_{0};
    // During normal operation, proposedTerm_ is equal to term_,
    // when the partition becomes a candidate, proposedTerm_ will be
    // bumped up by 1 every time when sending out the AskForVote
    // Request

    // If voted for somebody, the proposeTerm will be reset to the candidate
    // propose term. So we could use it to prevent revote if someone else ask for
    // vote for current proposedTerm.

    // TODO(heng) We should persist it on the disk in the future
    // Otherwise, after restart the whole cluster, maybe the stale
    // leader still has the unsend log with larger term, and after other
    // replicas elected the new leader, the stale one will not join in the
    // Raft group any more.
    TermID proposedTerm_{0};

    // The id and term of the last-sent log
    LogID lastLogId_{0};
    TermID lastLogTerm_{0};
    // The id for the last globally committed log (from the leader)
    LogID committedLogId_{0};

    // To record how long ago when the last leader message received
    time::Duration lastMsgRecvDur_;
    // To record how long ago when the last log message or heartbeat was sent
    time::Duration lastMsgSentDur_;
    // To record when the last message was accepted by majority peers
    uint64_t lastMsgAcceptedTime_{0};
    // How long between last message was sent and was accepted by majority peers
    uint64_t lastMsgAcceptedCostMs_{0};
    // Make sure only one election is in progress
    std::atomic_bool inElection_{false};
    // Speed up first election when I don't know who is leader
    bool isBlindFollower_{true};
    // Check leader has commit log in this term (accepted by majority is not enough),
    // leader is not allowed to service until it is true.
    bool commitInThisTerm_{false};

    // Write-ahead Log
    std::shared_ptr<wal::FileBasedWal> wal_;

    // IO Thread pool
    std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;
    // Shared worker thread pool
    std::shared_ptr<thread::GenericThreadPool> bgWorkers_;
    // Workers pool
    std::shared_ptr<folly::Executor> executor_;

    std::shared_ptr<SnapshotManager> snapshot_;

    std::shared_ptr<thrift::ThriftClientManager<cpp2::RaftexServiceAsyncClient>> clientMan_;
    // Used in snapshot, record the last total count and total size received from request
    int64_t lastTotalCount_ = 0;
    int64_t lastTotalSize_ = 0;
    time::Duration lastSnapshotRecvDur_;

    // Check if disk has enough space before write wal
    std::shared_ptr<kvstore::DiskManager> diskMan_;

    // Used to bypass the stale command
    int64_t startTimeMs_ = 0;

    std::atomic<uint64_t> weight_;

    std::atomic<bool> blocking_{false};
};

}  // namespace raftex
}  // namespace nebula
#endif  // RAFTEX_RAFTPART_H_
