/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_PART_H_
#define KVSTORE_PART_H_

#include "common/base/Base.h"
#include "common/utils/NebulaKeyUtils.h"
#include "kvstore/Common.h"
#include "kvstore/KVEngine.h"
#include "kvstore/raftex/SnapshotManager.h"
#include "kvstore/wal/FileBasedWal.h"
#include "raftex/RaftPart.h"

namespace nebula {
namespace kvstore {

using RaftClient = thrift::ThriftClientManager<raftex::cpp2::RaftexServiceAsyncClient>;

/**
 * @brief A derived class of RaftPart, most of the interfaces are called from NebulaStore
 */
class Part : public raftex::RaftPart {
  friend class SnapshotManager;

 public:
  /**
   * @brief Construct a new Part object
   *
   * @param spaceId
   * @param partId
   * @param localAddr Local address of the Part
   * @param walPath Listener's wal path
   * @param engine Pointer of kv engine
   * @param pool IOThreadPool for listener
   * @param workers Background thread for listener
   * @param handlers Worker thread for listener
   * @param snapshotMan Snapshot manager
   * @param clientMan Client manager
   * @param diskMan Disk manager
   * @param vIdLen Vertex id length of space
   */
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
       std::shared_ptr<DiskManager> diskMan,
       int32_t vIdLen);

  virtual ~Part() {
    LOG(INFO) << idStr_ << "~Part()";
  }

  /**
   * @brief Return the related kv engine
   */
  KVEngine* engine() {
    return engine_;
  }

  /**
   * @brief Write single key/values to kvstore asynchronously
   *
   * @param key Key to put
   * @param value Value to put
   * @param cb Callback when has a result
   */
  void asyncPut(folly::StringPiece key, folly::StringPiece value, KVCallback cb);

  /**
   * @brief Write multiple key/values to kvstore asynchronously
   *
   * @param keyValues Key/values to put
   * @param cb Callback when has a result
   */
  void asyncMultiPut(const std::vector<KV>& keyValues, KVCallback cb);

  /**
   * @brief Remove a key from kvstore asynchronously
   *
   * @param key Key to remove
   * @param cb Callback when has a result
   */
  void asyncRemove(folly::StringPiece key, KVCallback cb);

  /**
   * @brief Remove multiple keys from kvstore asynchronously
   *
   * @param key Keys to remove
   * @param cb Callback when has a result
   */
  void asyncMultiRemove(const std::vector<std::string>& keys, KVCallback cb);

  /**
   * @brief Remove keys in range [start, end) asynchronously
   *
   * @param start Start key
   * @param end End key
   * @param cb Callback when has a result
   */
  void asyncRemoveRange(folly::StringPiece start, folly::StringPiece end, KVCallback cb);

  /**
   * @brief Async commit multi operation, difference between asyncMultiPut or asyncMultiRemove
   * is this method allow contains both put and remove together, difference between asyncAtomicOp is
   * that asyncAtomicOp may have CAS
   *
   * @param batch Encoded write batch
   * @param cb Callback when has a result
   */
  void asyncAppendBatch(std::string&& batch, KVCallback cb);

  /**
   * @brief Do some atomic operation on kvstore
   *
   * @param op Atomic operation
   * @param cb Callback when has a result
   */
  void asyncAtomicOp(MergeableAtomicOp op, KVCallback cb);

  /**
   * @brief Add a raft learner asynchronously by adding raft log
   *
   * @param learner Address of learner
   * @param cb Callback when has a result
   */
  void asyncAddLearner(const HostAddr& learner, KVCallback cb);

  /**
   * @brief Try to transfer raft leader to target host asynchronously, by adding raft log
   *
   * @param target Address of target new leader
   * @param cb Callback when has a result
   */
  void asyncTransferLeader(const HostAddr& target, KVCallback cb);

  /**
   * @brief Add a raft peer asynchronously by adding raft log
   *
   * @param peer Address of new peer
   * @param cb Callback when has a result
   */
  void asyncAddPeer(const HostAddr& peer, KVCallback cb);

  /**
   * @brief Remove a raft peer asynchronously by adding raft log
   *
   * @param peer Address of peer to be removed
   * @param cb Callback when has a result
   */
  void asyncRemovePeer(const HostAddr& peer, KVCallback cb);

  /**
   * @brief Set the write blocking flag, if blocked, only heartbeat can be replicated
   *
   * @param sign True to block. Falst to unblock
   */
  void setBlocking(bool sign);

  /**
   * @brief Synchronize the kvstore across multiple replica by add a empty log
   *
   * @param cb Callback when has a result
   */
  void sync(KVCallback cb);

  /**
   * @brief Register a callback when discover a new leader
   *
   * @param cb Callback when discovered a new leader
   */
  void registerNewLeaderCb(NewLeaderCallback cb) {
    newLeaderCb_ = std::move(cb);
  }

  /**
   * @brief Unregister the new leader callback
   */
  void unRegisterNewLeaderCb() {
    newLeaderCb_ = nullptr;
  }

  /**
   * @brief Clean up all data about this part.
   */
  void resetPart() {
    std::lock_guard<std::mutex> g(raftLock_);
    reset();
  }

  /**
   * @brief clean up data safely
   *
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode cleanupSafely() {
    std::lock_guard<std::mutex> g(raftLock_);
    return cleanup();
  }

 private:
  /**
   * Methods inherited from RaftPart
   */

  /**
   * @brief Read last commit log id and term from external storage, used in initialization
   *
   * @return std::pair<LogID, TermID> Last commit log id and last commit log term
   */
  std::pair<LogID, TermID> lastCommittedLogId() override;

  /**
   * @brief Callback when a raft node lost leadership on term
   *
   * @param term
   */
  void onLostLeadership(TermID term) override;

  /**
   * @brief Callback when a raft node elected as leader on term
   *
   * @param term
   */
  void onElected(TermID term) override;

  /**
   * @brief Callback when a raft node is ready to serve as leader
   *
   * @param term
   */
  void onLeaderReady(TermID term) override;

  /**
   * @brief Callback when a raft node discover new leader
   *
   * @param nLeader New leader's address
   */
  void onDiscoverNewLeader(HostAddr nLeader) override;

  /**
   * @brief Apply the logs in iterator to state machine, and return the commit log id and commit log
   * term if succeed
   *
   * @param iter Wal log iterator
   * @param wait Whether we should until all data applied to state machine
   * @param needLock Whether need to acquire raftLock_ before operations. When the raftLock_ has
   * been acquired before commitLogs is invoked, needLock is false (e.g. commitLogs by follower). If
   * the lock has not been acquired, needLock is true (e.g. commitLogs by leader).
   * @return std::tuple<nebula::cpp2::ErrorCode, LogID, TermID>
   *
   */
  std::tuple<nebula::cpp2::ErrorCode, LogID, TermID> commitLogs(std::unique_ptr<LogIterator> iter,
                                                                bool wait,
                                                                bool needLock) override;

  /**
   * @brief Some special log need to be pre-processed when appending to wal
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
   * @brief If a raft peer falls behind way to much than leader, the leader will send all its data
   * in snapshot by batch, Part need to implement this method to apply the batch to state machine.
   * The return value is a pair of <logs count, logs size> of this batch.
   *
   * @param data Data to apply
   * @param committedLogId Commit log id of snapshot
   * @param committedLogTerm Commit log term of snapshot
   * @param finished Whether spapshot is finished
   * @return std::tuple<nebula::cpp2::ErrorCode, int64_t, int64_t> Return {ok, count, size} if
   * succeed, else return {errorcode, -1, -1}
   */
  std::tuple<nebula::cpp2::ErrorCode, int64_t, int64_t> commitSnapshot(
      const std::vector<std::string>& data,
      LogID committedLogId,
      TermID committedLogTerm,
      bool finished) override;

  /**
   * @brief Encode the commit log id and commit log term to write batch
   *
   * @param batch Pointer of write batch
   * @param committedLogId Commit log id
   * @param committedLogTerm Commit log term
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode putCommitMsg(WriteBatch* batch,
                                       LogID committedLogId,
                                       TermID committedLogTerm);

  /**
   * @brief clean up data in storage part, called in RaftPart::reset
   *
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode cleanup() override;

  /**
   * @brief clean up data in meta part, called in RaftPart::reset
   *
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode metaCleanup();

 public:
  struct CallbackOptions {
    GraphSpaceID spaceId;
    PartitionID partId;
    TermID term;
  };

  using LeaderChangeCB = std::function<void(const CallbackOptions& opt)>;

  /**
   * @brief Register callback when raft node is ready to serve as leader
   */
  void registerOnLeaderReady(LeaderChangeCB cb);

  /**
   * @brief Register callback when raft node lost leadership
   */
  void registerOnLeaderLost(LeaderChangeCB cb);

 protected:
  GraphSpaceID spaceId_;
  PartitionID partId_;
  std::string walPath_;
  NewLeaderCallback newLeaderCb_ = nullptr;
  std::vector<LeaderChangeCB> leaderReadyCB_;
  std::vector<LeaderChangeCB> leaderLostCB_;

 private:
  KVEngine* engine_ = nullptr;
  int32_t vIdLen_;
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_PART_H_
