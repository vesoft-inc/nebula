/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_LISTENER_LISTENER_H_
#define KVSTORE_LISTENER_LISTENER_H_

#include "common/base/Base.h"
#include "common/meta/SchemaManager.h"
#include "kvstore/Common.h"
#include "kvstore/LogEncoder.h"
#include "kvstore/raftex/Host.h"
#include "kvstore/raftex/RaftPart.h"
#include "kvstore/wal/FileBasedWal.h"

DECLARE_int32(cluster_id);

namespace nebula {
namespace kvstore {

using RaftClient = thrift::ThriftClientManager<raftex::cpp2::RaftexServiceAsyncClient>;

/**
 * Listener is a special RaftPart, it will join the raft group as learner. The key difference
 * between learner (e.g. in balance data) and listener is that, listener won't be able to be
 * promoted to follower or leader.
 *
 * Once it joins raft group, it can receive logs from leader and do whatever you like. When listener
 * receives logs from leader, it will just write wal and update committedLogId_, none of the logs is
 * applied to state machine, which is different from follower/learner. There will be another thread
 * trigger the apply within a certain interval (listener_commit_interval_secs), it will get the logs
 * in range of [lastApplyLogId_ + 1, committedLogId_], and decode these logs into kv, and apply them
 * to state machine.
 *
 * If you want to add a new type of listener, just inherit from Listener. There are some interface
 * you need to implement, some of them has been implemented in Listener. Others need to implemented
 * in derived class.
 *
 * * Implemented in Listener, could override if necessary
 *   // Start listener as learner
 *   void start(std::vector<HostAddr>&& peers, bool asLearner)
 *
 *   // Stop the listener
 *   void stop()
 *
 *   void onLostLeadership(TermID term)
 *
 *   void onElected(TermID term)
 *
 *   void onDiscoverNewLeader(HostAddr nLeader)
 *
 *   // For listener, we just return true directly. Another background thread trigger the actual
 *   // apply work, and do it in worker thread, and update lastApplyLogId_
 *   std::tuple<nebula::cpp2::ErrorCode, LogID, TermID>
 *   commitLogs(std::unique_ptr<LogIterator> iter, bool)
 *
 *   // For most of the listeners, just return true is enough. However, if listener need to be
 *   // aware of membership change, some log type of wal need to be pre-processed, could do it
 *   // here.
 *   bool preProcessLog(LogID logId, TermID termId, ClusterID clusterId, const std::string& log)
 *
 *   // If listener falls far behind from leader, leader would send snapshot to listener. The
 *   // snapshot is a vector of kv, listener could decode them and treat them as normal logs until
 *   // all snapshot has been received.
 *   std::tuple<cpp2::ErrorCode, int64_t, int64_t> commitSnapshot(
 *        const std::vector<std::string>& data,
 *        LogID committedLogId,
 *        TermID committedLogTerm,
 *        bool finished) override;
 *
 *   // extra cleanup work, will be invoked when listener is about to be removed,
 *   // or raft is reset
 *   void cleanup()；
 *
 * * Must implement in derived class
 *   // extra initialize work could do here
 *   void init()
 *
 *   // Main interface to process logs, listener need to apply the committed log entry to their
 *   // state machine. Once apply succeeded, user should call persist() to make their progress
 *   // persisted.
 *   virtual void processLogs() = 0;
 *
 *   // read last commit log id and term from external storage, used in initialization
 *   std::pair<LogID, TermID> lastCommittedLogId()
 *
 *   // read last apply id from external storage, used in initialization
 *   LogID lastApplyLogId()
 *
 *   // persist last commit log id/term and lastApplyId
 *   bool persist(LogID, TermID, LogID)
 */
class Listener : public raftex::RaftPart {
 public:
  /**
   * @brief Construct a new Listener, it is a derived class of RaftPart
   *
   * @param spaceId
   * @param partId
   * @param localAddr Listener ip/addr
   * @param walPath Listener's wal path
   * @param ioPool IOThreadPool for listener
   * @param workers Background thread for listener
   * @param handlers Worker thread for listener
   */
  Listener(GraphSpaceID spaceId,
           PartitionID partId,
           HostAddr localAddr,
           const std::string& walPath,
           std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
           std::shared_ptr<thread::GenericThreadPool> workers,
           std::shared_ptr<folly::Executor> handlers);

  /**
   * @brief Initialize listener, all Listener must call this method
   *
   * @param peers Raft peers
   * @param asLearner Listener is always a raft learner, so by default true
   */
  void start(std::vector<HostAddr>&& peers, bool asLearner = true) override;

  /**
   * @brief Stop listener
   */
  void stop() override;

  /**
   * @brief Get listener's apply id
   *
   * @return LogID logId that has been applied to state machine
   */
  LogID getApplyId() {
    return lastApplyLogId_;
  }

  /**
   * @brief clean wal that before lastApplyLogId_
   */
  void cleanWal() override;

  /**
   * @brief clean up data in listener, called in RaftPart::reset
   *
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode cleanup() override {
    CHECK(!raftLock_.try_lock());
    leaderCommitId_ = 0;
    lastApplyLogId_ = 0;
    persist(0, 0, lastApplyLogId_);
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  /**
   * @brief Trigger RaftPart::reset, clean all data and reset states
   */
  void resetListener();

  /**
   * @brief Check whether listener has catchup leader
   */
  bool pursueLeaderDone();

 protected:
  /**
   * @brief extra initialize work could do here
   */
  virtual void init() = 0;

  /**
   * @brief Get last apply id from persistence storage, used in initialization
   *
   * @return LogID Last apply log id
   */
  virtual LogID lastApplyLogId() = 0;

  /**
   * @brief Persist commitLogId commitLogTerm and lastApplyLogId
   */
  virtual bool persist(LogID commitLogId, TermID commitLogTerm, LogID lastApplyLogId) = 0;

  /**
   * @brief Main interface to process logs, listener need to apply the committed log entry to their
   * state machine. Once apply succeeded, user should call persist() to make their progress
   * persisted.
   */
  virtual void processLogs() = 0;

  /**
   * @brief Callback when a raft node lost leadership on term, should not happen in listener
   *
   * @param term
   */
  void onLostLeadership(TermID term) override {
    UNUSED(term);
    LOG(FATAL) << "Should not reach here";
  }

  /**
   * @brief Callback when a raft node elected as leader on term, should not happen in listener
   *
   * @param term
   */
  void onElected(TermID term) override {
    UNUSED(term);
    LOG(FATAL) << "Should not reach here";
  }

  /**
   * @brief Callback when a raft node is ready to serve as leader, should not happen in listener
   *
   * @param term
   */
  void onLeaderReady(TermID term) override {
    UNUSED(term);
    LOG(FATAL) << "Should not reach here";
  }

  /**
   * @brief Callback when a raft node discover new leader
   *
   * @param nLeader New leader's address
   */
  void onDiscoverNewLeader(HostAddr nLeader) override {
    VLOG(2) << idStr_ << "Find the new leader " << nLeader;
  }

  /**
   * @brief Check if candidate is in my peer
   *
   * @param candidate Address when received a request
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode checkPeer(const HostAddr& candidate) override {
    CHECK(!raftLock_.try_lock());
    if (peers_.find(candidate) == peers_.end()) {
      VLOG(2) << idStr_ << "The candidate " << candidate << " is not in my peers";
      return nebula::cpp2::ErrorCode::E_RAFT_INVALID_PEER;
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  /**
   * @brief For listener, we just return true directly. Another background thread trigger the actual
   * apply work, and do it in worker thread, and update lastApplyLogId_
   *
   * @return std::tuple<SUCCEED, last log id, last log term>
   */
  std::tuple<nebula::cpp2::ErrorCode, LogID, TermID> commitLogs(std::unique_ptr<LogIterator>,
                                                                bool,
                                                                bool) override;

  /**
   * @brief For most of the listeners, just return true is enough. However, if listener need to be
   * aware of membership change, some log type of wal need to be pre-processed, could do it here.
   *
   * @param logId Log id to pre-process
   * @param termId Log term to pre-process
   * @param clusterId Cluster id in wal
   * @param log Log message in wal
   * @return True if succeed. False if failed.
   */
  bool preProcessLog(LogID logId,
                     TermID termId,
                     ClusterID clusterId,
                     folly::StringPiece log) override;

  /**
   * @brief Background job thread will trigger doApply to apply data into state machine periodically
   */
  void doApply();

 protected:
  LogID leaderCommitId_ = 0;
  LogID lastApplyLogId_ = 0;
  std::set<HostAddr> peers_;
  std::unique_ptr<folly::IOThreadPoolExecutor> applyPool_;
};

}  // namespace kvstore
}  // namespace nebula
#endif
