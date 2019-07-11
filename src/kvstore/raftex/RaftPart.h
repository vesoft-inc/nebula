/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef RAFTEX_RAFTPART_H_
#define RAFTEX_RAFTPART_H_

#include "base/Base.h"
#include <folly/futures/SharedPromise.h>
#include "gen-cpp2/raftex_types.h"
#include "time/Duration.h"
#include "thread/GenericThreadPool.h"
#include "base/LogIterator.h"

namespace folly {
class IOThreadPoolExecutor;
class EventBase;
}  // namespace folly

namespace nebula {

namespace wal {
class FileBasedWal;
class BufferFlusher;
}  // namespace wal


namespace raftex {

enum class AppendLogResult {
    SUCCEEDED = 0,
    E_CAS_FAILURE = -1,
    E_NOT_A_LEADER = -2,
    E_STOPPED = -3,
    E_NOT_READY = -4,
    E_BUFFER_OVERFLOW = -5,
    E_WAL_FAILURE = -6,
    E_TERM_OUT_OF_DATE = -7,
};

enum class LogType {
    NORMAL      = 0x00,
    CAS         = 0x01,
    /**
      COMMAND is similar to CAS, but not the same. There are two differences:
      1. Normal logs after CAS could be committed together. In opposite, Normal logs
         after COMMAND should be hold until the COMMAND committed, but the logs before
         COMMAND could be committed together.
      2. CAS maybe failed. So we use SinglePromise for it. But COMMAND not, so it could
         share one promise with the normal logs before it.
     * */
    COMMAND     = 0x02,
};

class Host;
class AppendLogsIterator;

class RaftPart : public std::enable_shared_from_this<RaftPart> {
    friend class AppendLogsIterator;
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

    std::shared_ptr<wal::FileBasedWal> wal() const {
        return wal_;
    }

    // Change the partition status to RUNNING. This is called
    // by the inherited class, when it's ready to serve
    virtual void start(std::vector<HostAddr>&& peers);

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
     * Asynchronously compare and set
     ***************************************************************/
    folly::Future<AppendLogResult> casAsync(std::string log);

    /**
     * Asynchronously send one command.
     * */
    folly::Future<AppendLogResult> sendCommandAsync(std::string log);

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


protected:
    // Protected constructor to prevent from instantiating directly
    RaftPart(ClusterID clusterId,
             GraphSpaceID spaceId,
             PartitionID partId,
             HostAddr localAddr,
             const folly::StringPiece walRoot,
             wal::BufferFlusher* flusher,
             std::shared_ptr<folly::IOThreadPoolExecutor> pool,
             std::shared_ptr<thread::GenericThreadPool> workers);

    const char* idStr() const {
        return idStr_.c_str();
    }

    // The method will be invoked by start()
    //
    // Inherited classes should implement this method to provide the last
    // committed log id
    virtual LogID lastCommittedLogId() = 0;

    // This method is called when this partition's leader term
    // is finished, either by receiving a new leader election
    // request, or a new leader heartbeat
    virtual void onLostLeadership(TermID term) = 0;

    // This method is called when this partition is elected as
    // a new leader
    virtual void onElected(TermID term) = 0;

    // This method is invoked when handling a CAS log. The inherited
    // class uses this method to do the comparison and decide whether
    // a log should be inserted
    //
    // The method will be guaranteed to execute in a single-threaded
    // manner, so no need for locks
    //
    // If CAS succeeded, the method should return the correct log content
    // that will be applied to the storage. Otherwise it returns an empty
    // string
    virtual std::string compareAndSet(const std::string& log) = 0;

    // The inherited classes need to implement this method to commit
    // a batch of log messages
    virtual bool commitLogs(std::unique_ptr<LogIterator> iter) = 0;


private:
    enum class Status {
        STARTING = 0,   // The part is starting, not ready for service
        RUNNING,        // The part is running
        STOPPED         // The part has been stopped
    };

    enum class Role {
        LEADER = 1,     // the leader
        FOLLOWER,       // following a leader
        CANDIDATE       // Has sent AskForVote request
    };

    // A list of <idx, resp>
    // idx  -- the index of the peer
    // resp -- AskForVoteResponse
    using ElectionResponses = std::vector<cpp2::AskForVoteResponse>;
    // A list of <idx, resp>
    // idx  -- the index of the peer
    // resp -- AppendLogResponse
    using AppendLogResponses = std::vector<cpp2::AppendLogResponse>;

    // <source, term, logType, log>
    using LogCache = std::vector<
        std::tuple<ClusterID,
                   TermID,
                   LogType,
                   std::string>>;


    /****************************************************
     *
     * Private methods
     *
     ***************************************************/
    const char* roleStr(Role role) const;

    cpp2::ErrorCode verifyLeader(const cpp2::AppendLogRequest& req,
                                 std::lock_guard<std::mutex>& lck);

    /*****************************************************************
     * Asynchronously send a heartbeat (An empty log entry)
     *
     ****************************************************************/
    folly::Future<AppendLogResult> sendHeartbeat();

    /****************************************************
     *
     * Methods used by the status polling logic
     *
     ***************************************************/
    bool needToSendHeartbeat();

    bool needToStartElection();

    void statusPolling();

    // The method sends out AskForVote request
    // It return true if a leader is elected, otherwise returns false
    bool leaderElection();

    // The method will fill up the request object and return TRUE
    // if the election should continue. Otherwise the method will
    // return FALSE
    bool prepareElectionRequest(
        cpp2::AskForVoteRequest& req,
        std::shared_ptr<std::unordered_map<HostAddr, std::shared_ptr<Host>>>& hosts);

    // The method returns the partition's role after the election
    Role processElectionResponses(const ElectionResponses& results);

    // Check whether new logs can be appended
    // Pre-condition: The caller needs to hold the raftLock_
    AppendLogResult canAppendLogs(std::lock_guard<std::mutex>& lck);

    folly::Future<AppendLogResult> appendLogAsync(ClusterID source,
                                                  LogType logType,
                                                  std::string log);

    void appendLogsInternal(AppendLogsIterator iter);

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
        LogID prevLogId);


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

        // Promises shared by continuous non-CAS logs
        std::list<folly::SharedPromise<ValueType>> sharedPromises_;
        // A list of promises for CAS logs
        std::list<folly::Promise<ValueType>> singlePromises_;
    };


    const std::string idStr_;

    const ClusterID clusterId_;
    const GraphSpaceID spaceId_;
    const PartitionID partId_;
    const HostAddr addr_;
    std::shared_ptr<std::unordered_map<HostAddr, std::shared_ptr<Host>>>
        peerHosts_;
    size_t quorum_{0};

    // Partition level lock to synchronize the access of the partition
    mutable std::mutex raftLock_;

    bool replicatingLogs_{false};
    PromiseSet<AppendLogResult> cachingPromise_;
    PromiseSet<AppendLogResult> sendingPromise_;
    LogCache logs_;

    Status status_;
    Role role_;

    // When the partition is the leader, the leader_ is same as addr_
    HostAddr leader_;

    // The current term id
    //
    // When the partition voted for someone, termId will be set to
    // the term id proposed by that candidate
    TermID term_{0};
    // During normal operation, proposedTerm_ is equal to term_,
    // when the partition becomes a candidate, proposedTerm_ will be
    // bumped up by 1 every time when sending out the AskForVote
    // Request
    TermID proposedTerm_{0};

    // The id and term of the last-sent log
    LogID lastLogId_{0};
    TermID lastLogTerm_{0};
    // The id for the last globally committed log (from the leader)
    LogID committedLogId_{0};

    // To record how long ago when the last leader message received
    time::Duration lastMsgRecvDur_;
    // To record how long ago when the last log message or heartbeat
    // was sent
    time::Duration lastMsgSentDur_;

    // Write-ahead Log
    std::shared_ptr<wal::FileBasedWal> wal_;

    // IO Thread pool
    std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;
    // Shared worker thread pool
    std::shared_ptr<thread::GenericThreadPool> workers_;
};

}  // namespace raftex
}  // namespace nebula
#endif  // RAFTEX_RAFTPART_H_
