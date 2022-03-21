/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef RAFTEX_RAFTPART_H_
#define RAFTEX_RAFTPART_H_

#include <folly/Function.h>
#include <folly/futures/SharedPromise.h>
#include <gtest/gtest_prod.h>

#include "common/base/Base.h"
#include "common/thread/GenericThreadPool.h"
#include "common/time/Duration.h"
#include "common/utils/LogIterator.h"
#include "interface/gen-cpp2/RaftexServiceAsyncClient.h"
#include "interface/gen-cpp2/common_types.h"
#include "interface/gen-cpp2/raftex_types.h"
#include "kvstore/Common.h"
#include "kvstore/DiskManager.h"
#include "kvstore/raftex/SnapshotManager.h"

namespace folly {
class IOThreadPoolExecutor;
class EventBase;
}  // namespace folly

namespace nebula {

namespace wal {
class FileBasedWal;
}  // namespace wal

namespace raftex {

/**
 * @brief Log type of raft log
 * NORMAL:    Normal log could be in any position of a batch
 * ATOMIC_OP: Atomic op should be the first log in a batch (a batch with only one atomic op log is
              legal as well), there won't be more than one atomic op log in the same batch
 * COMMAND:   Command should only be the last log in a batch (a batch with only one command log is
              legal as well), there won't be more than one atomic op log in the same batch
 */
enum class LogType {
  NORMAL = 0x00,
  ATOMIC_OP = 0x01,
  /**
    COMMAND is similar to AtomicOp, but not the same. There are two differences:
    1. Normal logs after AtomicOp could be committed together. In opposite,
   Normal logs after COMMAND should be hold until the COMMAND committed, but the
   logs before COMMAND could be committed together.
    2. AtomicOp maybe failed. So we use SinglePromise for it. But COMMAND not,
   so it could share one promise with the normal logs before it.
   * */
  COMMAND = 0x02,
};

class Host;
class AppendLogsIterator;

/**
 * The operation will be atomic, if the operation failed, empty string will be
 * returned, otherwise it will return the new operation's encoded string whick
 * should be applied atomically. You could implement CAS, READ-MODIFY-WRITE
 * operations though it.
 * */
using AtomicOp = folly::Function<std::optional<std::string>(void)>;

class RaftPart : public std::enable_shared_from_this<RaftPart> {
  friend class AppendLogsIterator;
  friend class Host;
  friend class SnapshotManager;
  FRIEND_TEST(MemberChangeTest, AddRemovePeerTest);
  FRIEND_TEST(MemberChangeTest, RemoveLeaderTest);

 public:
  virtual ~RaftPart();

  /**
   * @brief Return whether RaftPart is running
   */
  bool isRunning() const {
    std::lock_guard<std::mutex> g(raftLock_);
    return status_ == Status::RUNNING;
  }

  /**
   * @brief Return whether RaftPart is stopped
   */
  bool isStopped() const {
    std::lock_guard<std::mutex> g(raftLock_);
    return status_ == Status::STOPPED;
  }

  /**
   * @brief Return whether RaftPart is leader
   */
  bool isLeader() const {
    std::lock_guard<std::mutex> g(raftLock_);
    return role_ == Role::LEADER;
  }

  /**
   * @brief Return whether RaftPart is follower
   */
  bool isFollower() const {
    std::lock_guard<std::mutex> g(raftLock_);
    return role_ == Role::FOLLOWER;
  }

  /**
   * @brief Return whether RaftPart is learner
   */
  bool isLearner() const {
    std::lock_guard<std::mutex> g(raftLock_);
    return role_ == Role::LEARNER;
  }

  /**
   * @brief Return the cluster id of RaftPart
   */
  ClusterID clusterId() const {
    return clusterId_;
  }

  /**
   * @brief Return the space id of RaftPart
   */
  GraphSpaceID spaceId() const {
    return spaceId_;
  }

  /**
   * @brief Return the part id of RaftPart
   */
  PartitionID partitionId() const {
    return partId_;
  }

  /**
   * @brief Return the address of RaftPart
   */
  const HostAddr& address() const {
    return addr_;
  }

  /**
   * @brief Return the leader address of RaftPart
   */
  HostAddr leader() const {
    std::lock_guard<std::mutex> g(raftLock_);
    return leader_;
  }

  /**
   * @brief Return the term of RaftPart
   */
  TermID termId() const {
    return term_;
  }

  /**
   * @brief clean wal that before commitLogId
   */
  virtual void cleanWal();

  /**
   * @brief Return the wal
   */
  std::shared_ptr<wal::FileBasedWal> wal() const {
    return wal_;
  }

  /**
   * @brief Add a raft learner to its peers
   *
   * @param learner Learner address
   */
  void addLearner(const HostAddr& learner);

  /**
   * @brief When commit to state machine, old leader will step down as follower
   *
   * @param target Target new leader
   */
  void commitTransLeader(const HostAddr& target);

  /**
   * @brief Pre-process of transfer leader, target new leader will start election task to background
   * worker
   *
   * @param target Target new leader
   */
  void preProcessTransLeader(const HostAddr& target);

  /**
   * @brief Pre-process of remove a host from peers, follower will remove the peer in
   * preProcessRemovePeer, leader will remove in commitRemovePeer
   *
   * @param peer Target peer to remove
   */
  void preProcessRemovePeer(const HostAddr& peer);

  /**
   * @brief Commit of remove a host from peers, follower will remove the peer in
   * preProcessRemovePeer, leader will remove in commitRemovePeer
   *
   * @param peer Target peer to remove
   */
  void commitRemovePeer(const HostAddr& peer);

  // All learner and listener are raft learner. The difference between listener and learner is that
  // learner could be promoted to follower, but listener could not. (learner are added to hosts_,
  // but listener are added to listeners_)
  // todo(doodle): separate learner and listener into different raft role
  /**
   * @brief Add listener peer.
   *
   * @param peer Listener address
   */
  void addListenerPeer(const HostAddr& peer);

  /**
   * @brief Remove listener peer
   *
   * @param peer Listener address
   */
  void removeListenerPeer(const HostAddr& peer);

  /**
   * @brief Change the partition status to RUNNING. This is called by the inherited class, when it's
   * ready to serve
   *
   * @param peers All raft peers to add
   * @param asLearner Whether start as raft learner
   */
  virtual void start(std::vector<HostAddr>&& peers, bool asLearner = false);

  /**
   * @brief Change the partition status to STOPPED. This is called by the inherited class, when it's
   * about to stop
   */
  virtual void stop();

  /**
   * @brief Asynchronously append a log
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
   *
   * @param source Cluster id
   * @param log Log message to append
   * @return folly::Future<nebula::cpp2::ErrorCode>
   */
  folly::Future<nebula::cpp2::ErrorCode> appendAsync(ClusterID source, std::string log);

  /**
   * @brief Trigger the op atomically. If the atomic operation succeed, and append the output log
   * message of ATOMIC_OP type
   *
   * @param op Atomic operation, will output a log if succeed
   * @return folly::Future<nebula::cpp2::ErrorCode>
   */
  folly::Future<nebula::cpp2::ErrorCode> atomicOpAsync(AtomicOp op);

  /**
   * @brief Send a log of COMMAND type
   *
   * @param log Command log
   * @return folly::Future<nebula::cpp2::ErrorCode>
   */
  folly::Future<nebula::cpp2::ErrorCode> sendCommandAsync(std::string log);

  /**
   * @brief Check if the peer has catched up data from leader. If leader is sending the
   * snapshot, the method will return false.
   *
   * @param peer The peer to check if it has catched up
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode isCatchedUp(const HostAddr& peer);

  /**
   * @brief Hard link the wal files to a new path
   *
   * @param newPath New wal path
   * @return Whether link succeed
   */
  bool linkCurrentWAL(const char* newPath);

  /**
   * @brief Reset my peers if not equals the argument
   *
   * @param peers Expect peers
   */
  void checkAndResetPeers(const std::vector<HostAddr>& peers);

  /**
   * @brief Check my remote listeners is equal to the expected one, add/remove if necessary
   *
   * @param listeners Expect remote listeners
   */
  void checkRemoteListeners(const std::set<HostAddr>& listeners);

  /*****************************************************
   *
   * Methods to process incoming raft requests
   *
   ****************************************************/

  /**
   * @brief Check my remote listeners is equal to the expected one, add/remove if necessary
   *
   * @param listeners Expect remote listeners
   */
  void getState(cpp2::GetStateResponse& resp);

  /**
   * @brief Process the incoming leader election request
   *
   * @param req
   * @param resp
   */
  void processAskForVoteRequest(const cpp2::AskForVoteRequest& req, cpp2::AskForVoteResponse& resp);

  /**
   * @brief Process append log request
   *
   * @param req
   * @param resp
   */
  void processAppendLogRequest(const cpp2::AppendLogRequest& req, cpp2::AppendLogResponse& resp);

  /**
   * @brief Process send snapshot request
   *
   * @param req
   * @param resp
   */
  void processSendSnapshotRequest(const cpp2::SendSnapshotRequest& req,
                                  cpp2::SendSnapshotResponse& resp);

  /**
   * @brief Process heartbeat request
   *
   * @param req
   * @param resp
   */
  void processHeartbeatRequest(const cpp2::HeartbeatRequest& req, cpp2::HeartbeatResponse& resp);

  /**
   * @brief Return whether leader lease is still valid
   */
  bool leaseValid();

  /**
   * @brief Return whether we need to clean expired wal
   */
  bool needToCleanWal();

  /**
   * @brief Get the address of node which has the partition, local address and all peers address
   *
   * @return std::vector<HostAddr> local address and all peers address
   */
  std::vector<HostAddr> peers() const;

  /**
   * @brief All listeners address
   *
   * @return std::set<HostAddr>
   */
  std::set<HostAddr> listeners() const;

  /**
   * @brief Return last log id and last log term in wal
   *
   * @return std::pair<LogID, TermID> Pair of last log id and last log term in wal
   */
  std::pair<LogID, TermID> lastLogInfo() const;

  /**
   * @brief Reset the part, clean up all data and WALs.
   */
  void reset();

  /**
   * @brief Execution time of some operation, for statistics
   *
   * @return uint64_t Time in us
   */
  uint64_t execTime() const {
    return execTime_;
  }

 protected:
  /**
   * @brief Construct a new RaftPart
   *
   * @param clusterId
   * @param spaceId
   * @param partId
   * @param localAddr Listener ip/addr
   * @param walPath Listener's wal path
   * @param ioPool IOThreadPool for listener
   * @param workers Background thread for listener
   * @param executor Worker thread for listener
   * @param snapshotMan Snapshot manager
   * @param clientMan Client manager
   * @param diskMan Disk manager
   */
  RaftPart(ClusterID clusterId,
           GraphSpaceID spaceId,
           PartitionID partId,
           HostAddr localAddr,
           const folly::StringPiece walPath,
           std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
           std::shared_ptr<thread::GenericThreadPool> workers,
           std::shared_ptr<folly::Executor> executor,
           std::shared_ptr<SnapshotManager> snapshotMan,
           std::shared_ptr<thrift::ThriftClientManager<cpp2::RaftexServiceAsyncClient>> clientMan,
           std::shared_ptr<kvstore::DiskManager> diskMan);

  using Status = cpp2::Status;
  using Role = cpp2::Role;

  /**
   * @brief The str of the RaftPart, used in logging
   */
  const char* idStr() const {
    return idStr_.c_str();
  }

  /**
   * @brief Inherited classes should implement this method to provide the last commit log id and
   * last commit log term. The method will be invoked by start()
   *
   * @return std::pair<LogID, TermID> Last commit log id and last commit log term
   */
  virtual std::pair<LogID, TermID> lastCommittedLogId() = 0;

  /**
   * @brief This method is called when this partition's leader term is finished, either by receiving
   * a new leader election request, or a new leader heartbeat
   *
   * @param term New term from peers
   */
  virtual void onLostLeadership(TermID term) = 0;

  /**
   * @brief This method is called when this partition is elected as a new leader
   *
   * @param term Term when elected as leader
   */
  virtual void onElected(TermID term) = 0;

  /**
   * @brief This method is called after leader committed first log (a little bit later onElected),
   * leader need to set some internal status after elected.
   *
   * @param term
   */
  virtual void onLeaderReady(TermID term) = 0;

  /**
   * @brief Callback when a raft node discover new leader
   *
   * @param nLeader New leader's address
   */
  virtual void onDiscoverNewLeader(HostAddr nLeader) = 0;

  /**
   * @brief Check if we can accept candidate's message
   *
   * @param candidate The sender of the message
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode checkPeer(const HostAddr& candidate);

  /**
   * @brief The inherited classes need to implement this method to commit a batch of log messages.
   *
   * @param iter Log iterator of all logs to commit
   * @param wait Whether wait until all logs has been applied to state machine
   * @return std::tuple<nebula::cpp2::ErrorCode, LogID, TermID>
   * Return {error code, last commit log id, last commit log term}. When no logs applied to state
   * machine or error occurs when calling commitLogs, kNoCommitLogId and kNoCommitLogTerm are
   * returned.
   */
  virtual std::tuple<nebula::cpp2::ErrorCode, LogID, TermID> commitLogs(
      std::unique_ptr<LogIterator> iter, bool wait) = 0;

  /**
   * @brief A interface to pre-process wal, mainly for membership change
   *
   * @param logId Log id to pre-process
   * @param termId Log term to pre-process
   * @param clusterId Cluster id in wal
   * @param log Log message in wal
   * @return True if succeed. False if failed.
   */
  virtual bool preProcessLog(LogID logId,
                             TermID termId,
                             ClusterID clusterId,
                             const std::string& log) = 0;

  /**
   * @brief If raft node falls behind way to much than leader, the leader will send all its data in
   * snapshot by batch, derived class need to implement this method to apply the batch to state
   * machine.
   *
   * @param data Data to apply
   * @param committedLogId Commit log id of snapshot
   * @param committedLogTerm Commit log term of snapshot
   * @param finished Whether spapshot is finished
   * @return std::tuple<nebula::cpp2::ErrorCode, int64_t, int64_t> Return {ok, count, size} if
   */
  virtual std::tuple<nebula::cpp2::ErrorCode, int64_t, int64_t> commitSnapshot(
      const std::vector<std::string>& data,
      LogID committedLogId,
      TermID committedLogTerm,
      bool finished) = 0;

  /**
   * @brief Clean up extra data about the partition, usually related to state machine
   *
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode cleanup() = 0;

  /**
   * @brief Add a host to my peers
   *
   * @param peer Address to add
   */
  void addPeer(const HostAddr& peer);

  /**
   * @brief Remove a host from my peers
   *
   * @param peer Address to remove
   */
  void removePeer(const HostAddr& peer);

 private:
  // A list of <idx, resp>
  // idx  -- the index of the peer
  // resp -- corresponding response of peer[index]
  using ElectionResponses = std::vector<std::pair<size_t, cpp2::AskForVoteResponse>>;
  using AppendLogResponses = std::vector<std::pair<size_t, cpp2::AppendLogResponse>>;
  using HeartbeatResponses = std::vector<std::pair<size_t, cpp2::HeartbeatResponse>>;

  // <source, logType, log>
  using LogCache = std::vector<std::tuple<ClusterID, LogType, std::string, AtomicOp>>;

  /****************************************************
   *
   * Private methods
   *
   ***************************************************/

  /**
   * @brief Return the role in string
   *
   * @param role Raft role
   * @return const char*
   */
  const char* roleStr(Role role) const;

  /**
   * @brief Verify if the request can be accepted when receiving a AppendLog or Heartbeat request
   *
   * @tparam REQ AppendLogRequest or HeartbeatRequest
   * @param req RPC requeset
   * @return nebula::cpp2::ErrorCode
   */
  template <typename REQ>
  nebula::cpp2::ErrorCode verifyLeader(const REQ& req);

  /****************************************************
   *
   * Methods used by the status polling logic
   *
   ***************************************************/

  /**
   * @brief Polling to check some status
   *
   * @param startTime Start time of the RaftPart, only used in test case
   */
  void statusPolling(int64_t startTime);

  /**
   * @brief Return whether need to send heartbeat
   */
  bool needToSendHeartbeat();

  /**
   * @brief Asynchronously send a heartbeat
   */
  void sendHeartbeat();

  /**
   * @brief Return whether need to trigger leader election
   */
  bool needToStartElection();

  /**
   * @brief Return whether need to clean snapshot when a node has not received the snapshot for a
   * period of time
   */
  bool needToCleanupSnapshot();

  /**
   * @brief Convert to follower when snapshot has been outdated
   */
  void cleanupSnapshot();

  /**
   * @brief The method sends out AskForVote request. Return true if I have been granted majority
   * votes on proposedTerm, no matter isPreVote or not
   *
   * @param isPreVote Whether this is a pre-vote
   * @return folly::Future<bool> Whether get majority votes
   */
  folly::Future<bool> leaderElection(bool isPreVote);

  /**
   * @brief The method will fill up the request object and return TRUE if the election should
   * continue. Otherwise the method will return FALSE
   *
   * @param req The request to send
   * @param hosts Raft peers
   * @param isPreVote Whether this is a pre-vote
   * @return Whether we have a valid request
   */
  bool prepareElectionRequest(cpp2::AskForVoteRequest& req,
                              std::vector<std::shared_ptr<Host>>& hosts,
                              bool isPreVote);

  /**
   * @brief Handle the leader election responses
   *
   * @param resps Leader election response
   * @param hosts Raft peers
   * @param proposedTerm Which term I proposed to be leader
   * @param isPreVote Whether this is a pre-vote
   * @return Return true if I have been granted majority votes on proposedTerm, no matter isPreVote
   * or not
   *
   */
  bool handleElectionResponses(const ElectionResponses& resps,
                               const std::vector<std::shared_ptr<Host>>& hosts,
                               TermID proposedTerm,
                               bool isPreVote);

  /**
   * @brief Check if have been granted from majority peers, no matter isPreVote or not. Convert to
   * leader if it is a formal election, and I have received majority votes.
   *
   * @param results Leader election response
   * @param hosts Raft peers
   * @param proposedTerm Which term I proposed to be leader
   * @param isPreVote Whether this is a pre-vote
   * @return Return true if I have been granted majority votes on proposedTerm, no matter isPreVote
   * or not
   */
  bool processElectionResponses(const ElectionResponses& results,
                                std::vector<std::shared_ptr<Host>> hosts,
                                TermID proposedTerm,
                                bool isPreVote);

  /**
   * @brief Check whether new logs can be appended
   * @pre The caller needs to hold the raftLock_
   *
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode canAppendLogs();

  /**
   * @brief Check if term has changed, and check if new logs can be appended
   * @pre The caller needs to hold the raftLock_
   *
   * @param currTerm Current term
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode canAppendLogs(TermID currTerm);

  /**
   * @brief The main interfaces to append log
   *
   * @param source Log cluster id
   * @param logType Log type
   * @param log Log message
   * @param cb Callback when log is replicated
   * @return folly::Future<nebula::cpp2::ErrorCode>
   */
  folly::Future<nebula::cpp2::ErrorCode> appendLogAsync(ClusterID source,
                                                        LogType logType,
                                                        std::string log,
                                                        AtomicOp cb = nullptr);

  /**
   * @brief Append the logs in iterator
   *
   * @param iter Log iterator to replicate
   * @param termId The term when building the iterator
   */
  void appendLogsInternal(AppendLogsIterator iter, TermID termId);

  /**
   * @brief Replicate the logs to peers by sending RPC
   *
   * @param eb The eventbase to send request
   * @param iter Log iterator to send
   * @param currTerm The term when building the iterator
   * @param lastLogId The last log id in iterator
   * @param committedId The commit log id
   * @param prevLogTerm The last log term which has been sent
   * @param prevLogId The last log id which has been sent
   */
  void replicateLogs(folly::EventBase* eb,
                     AppendLogsIterator iter,
                     TermID currTerm,
                     LogID lastLogId,
                     LogID committedId,
                     TermID prevLogTerm,
                     LogID prevLogId);

  /**
   * @brief Handle the log append response, apply to state machine if necessary
   *
   * @param resps Responses of peers
   * @param eb The eventbase when sent request, used for retry and continue as well
   * @param iter Log iterator, also used for continue to replicate remaing logs
   * @param currTerm The term when building the iterator
   * @param lastLogId The last log id in iterator
   * @param committedId The commit log id
   * @param prevLogTerm The last log term which has been sent
   * @param prevLogId The last log id which has been sent
   * @param hosts The Host of raft peers
   */
  void processAppendLogResponses(const AppendLogResponses& resps,
                                 folly::EventBase* eb,
                                 AppendLogsIterator iter,
                                 TermID currTerm,
                                 LogID lastLogId,
                                 LogID committedId,
                                 TermID prevLogTerm,
                                 LogID prevLogId,
                                 std::vector<std::shared_ptr<Host>> hosts);

  /**
   * @brief Return Host of which could vote, in other words, learner is not counted in
   *
   * @return std::vector<std::shared_ptr<Host>>
   */
  std::vector<std::shared_ptr<Host>> followers() const;

  /**
   * @brief Check if we succeed in append log.
   *
   * @param res Errorcode to check
   * @return Return true directly if res is succeed, otherwise set the failed status.
   */
  bool checkAppendLogResult(nebula::cpp2::ErrorCode res);

  /**
   * @brief Update raft quorum when membership changes
   */
  void updateQuorum();

 protected:
  template <class ValueType>
  class PromiseSet final {
   public:
    PromiseSet() = default;
    PromiseSet(const PromiseSet&) = delete;
    PromiseSet(PromiseSet&&) = default;

    ~PromiseSet() = default;

    PromiseSet& operator=(const PromiseSet&) = delete;
    PromiseSet& operator=(PromiseSet&& right) = default;

    /**
     * @brief Clean all promises
     */
    void reset() {
      sharedPromises_.clear();
      singlePromises_.clear();
      rollSharedPromise_ = true;
    }

    /**
     * @brief Used for NORMAL raft log
     *
     * @return folly::Future<ValueType>
     */
    folly::Future<ValueType> getSharedFuture() {
      if (rollSharedPromise_) {
        sharedPromises_.emplace_back();
        rollSharedPromise_ = false;
      }

      return sharedPromises_.back().getFuture();
    }

    /**
     * @brief Used for ATOMIC_OP raft log
     *
     * @return folly::Future<ValueType>
     */
    folly::Future<ValueType> getSingleFuture() {
      singlePromises_.emplace_back();
      rollSharedPromise_ = true;

      return singlePromises_.back().getFuture();
    }

    /**
     * @brief Used for COMMAND raft log
     *
     * @return folly::Future<ValueType>
     */
    folly::Future<ValueType> getAndRollSharedFuture() {
      if (rollSharedPromise_) {
        sharedPromises_.emplace_back();
      }
      rollSharedPromise_ = true;
      return sharedPromises_.back().getFuture();
    }

    /**
     * @brief Set shared promise
     *
     * @tparam VT
     * @param val
     */
    template <class VT>
    void setOneSharedValue(VT&& val) {
      CHECK(!sharedPromises_.empty());
      sharedPromises_.front().setValue(std::forward<VT>(val));
      sharedPromises_.pop_front();
    }

    /**
     * @brief Set single promise
     *
     * @tparam VT
     * @param val
     */
    template <class VT>
    void setOneSingleValue(VT&& val) {
      CHECK(!singlePromises_.empty());
      singlePromises_.front().setValue(std::forward<VT>(val));
      singlePromises_.pop_front();
    }

    /**
     * @brief Set all promises to result, usually a failed result
     *
     * @param val
     */
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
  PromiseSet<nebula::cpp2::ErrorCode> cachingPromise_;
  LogCache logs_;

  // Partition level lock to synchronize the access of the partition
  mutable std::mutex raftLock_;

  PromiseSet<nebula::cpp2::ErrorCode> sendingPromise_;

  Status status_;
  Role role_;

  // When the partition is the leader, the leader_ is same as addr_
  HostAddr leader_;

  // After voted for somebody, it will not be empty anymore.
  HostAddr votedAddr_;

  // The current term id
  // the term id proposed by that candidate
  TermID term_{0};

  // Once we have voted some one in formal election, we will set votedTerm_ and votedAddr_.
  // To prevent we have voted more than once in a same term
  TermID votedTerm_{0};

  // As for leader lastLogId_ is the log id which has been replicated to majority peers.
  // As for follower lastLogId_ is only a latest log id from current leader or any leader of
  // previous term. Not all logs before lastLogId_ could be applied for follower.
  LogID lastLogId_{0};
  TermID lastLogTerm_{0};

  // The last id and term when logs has been applied to state machine
  LogID committedLogId_{0};
  TermID committedLogTerm_{0};
  static constexpr LogID kNoCommitLogId{-1};
  static constexpr TermID kNoCommitLogTerm{-1};
  static constexpr int64_t kNoSnapshotCount{-1};
  static constexpr int64_t kNoSnapshotSize{-1};

  // To record how long ago when the last leader message received
  time::Duration lastMsgRecvDur_;
  // To record how long ago when the last log message was sent
  time::Duration lastMsgSentDur_;
  // To record when the last message was accepted by majority peers
  uint64_t lastMsgAcceptedTime_{0};
  // How long between last message was sent and was accepted by majority peers
  uint64_t lastMsgAcceptedCostMs_{0};
  // Make sure only one election is in progress
  std::atomic_bool inElection_{false};
  // Speed up first election when I don't know who is leader
  bool isBlindFollower_{true};
  // Check leader has commit log in this term (accepted by majority is not
  // enough), leader is not allowed to service until it is true.
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
  // Used in snapshot, record the commitLogId and commitLogTerm of the snapshot, as well as
  // last total count and total size received from request
  LogID lastSnapshotCommitId_ = 0;
  TermID lastSnapshotCommitTerm_ = 0;
  int64_t lastTotalCount_ = 0;
  int64_t lastTotalSize_ = 0;
  time::Duration lastSnapshotRecvDur_;

  // Check if disk has enough space before write wal
  std::shared_ptr<kvstore::DiskManager> diskMan_;

  // Used to bypass the stale command
  int64_t startTimeMs_ = 0;

  std::atomic<bool> blocking_{false};

  // For stats info
  uint64_t execTime_{0};
};

}  // namespace raftex
}  // namespace nebula
#endif  // RAFTEX_RAFTPART_H_
