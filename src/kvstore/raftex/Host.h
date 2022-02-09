/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef RAFTEX_HOST_H_
#define RAFTEX_HOST_H_

#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>

#include "common/base/Base.h"
#include "common/base/ErrorOr.h"
#include "common/thrift/ThriftClientManager.h"
#include "interface/gen-cpp2/RaftexServiceAsyncClient.h"
#include "interface/gen-cpp2/raftex_types.h"

namespace folly {
class EventBase;
}  // namespace folly

namespace nebula {
namespace raftex {

class RaftPart;

/**
 * @brief Host is a class to monitor how many log has been sent to a raft peer. It will send logs or
 * start elelction to the remote peer by rpc
 */
class Host final : public std::enable_shared_from_this<Host> {
  friend class RaftPart;

 public:
  /**
   * @brief Construct a new Host
   *
   * @param addr Target peer address
   * @param part Related RaftPart
   * @param isLearner Whether target is a learner
   */
  Host(const HostAddr& addr, std::shared_ptr<RaftPart> part, bool isLearner = false);

  /**
   * @brief Destroy the Host
   */
  ~Host() {
    VLOG(1) << idStr_ << " The host has been destroyed!";
  }

  /**
   * @brief The str of the Host, used in logging
   */
  const char* idStr() const {
    return idStr_.c_str();
  }

  /**
   * @brief This will be called when the RaftPart lost its leadership
   */
  void pause() {
    std::lock_guard<std::mutex> g(lock_);
    paused_ = true;
  }

  /**
   * @brief This will be called when the RaftPart becomes the leader
   */
  void resume() {
    std::lock_guard<std::mutex> g(lock_);
    paused_ = false;
  }

  /**
   * @brief This will be called when the RaftPart is stopped
   */
  void stop() {
    std::lock_guard<std::mutex> g(lock_);
    stopped_ = true;
  }

  /**
   * @brief Reset all state, should be called when the RaftPart becomes the leader
   */
  void reset() {
    std::unique_lock<std::mutex> g(lock_);
    noMoreRequestCV_.wait(g, [this] { return !requestOnGoing_; });
    logIdToSend_ = 0;
    logTermToSend_ = 0;
    lastLogIdSent_ = 0;
    lastLogTermSent_ = 0;
    committedLogId_ = 0;
    sendingSnapshot_ = false;
    followerCommittedLogId_ = 0;
  }

  /**
   * @brief Wait for all requests in flight finish or timeout
   */
  void waitForStop();

  /**
   * @brief Return whether the target peer is a raft learner
   */
  bool isLearner() const {
    return isLearner_;
  }

  /**
   * @brief Set the state in Host of a raft peer as learner
   */
  void setLearner(bool isLearner) {
    isLearner_ = isLearner;
  }

  /**
   * @brief Send the leader election rpc to the peer
   *
   * @param req The RPC request
   * @param eb The eventbase to send rpc
   * @return folly::Future<cpp2::AskForVoteResponse>
   */
  folly::Future<cpp2::AskForVoteResponse> askForVote(const cpp2::AskForVoteRequest& req,
                                                     folly::EventBase* eb);

  /**
   * @brief Send the append log to the peer
   *
   * @param eb The eventbase to send rpc
   * @param term The term of RaftPart
   * @param logId The last log to be sent
   * @param committedLogId The last committed log id
   * @param lastLogTermSent The last log term being sent
   * @param lastLogIdSent The last log id being sent
   * @return folly::Future<cpp2::AppendLogResponse>
   */
  folly::Future<cpp2::AppendLogResponse> appendLogs(folly::EventBase* eb,
                                                    TermID term,
                                                    LogID logId,
                                                    LogID committedLogId,
                                                    TermID lastLogTermSent,
                                                    LogID lastLogIdSent);

  /**
   * @brief Send the heartbeat to the peer
   *
   * @param eb The eventbase to send rpc
   * @param term The term of RaftPart
   * @param logId The last log to be sent
   * @param committedLogId The last committed log id
   * @param lastLogTermSent The last log term being sent
   * @param lastLogIdSent The last log id being sent
   * @return folly::Future<cpp2::AppendLogResponse>
   */
  folly::Future<cpp2::HeartbeatResponse> sendHeartbeat(
      folly::EventBase* eb, TermID term, LogID commitLogId, TermID lastLogTerm, LogID lastLogId);

  /**
   * @brief Return the peer address
   */
  const HostAddr& address() const {
    return addr_;
  }

 private:
  /**
   * @brief Whether Host can send rpc to the peer
   */
  nebula::cpp2::ErrorCode canAppendLog() const;

  /**
   * @brief Send append log rpc
   *
   * @param eb The eventbase to send rpc
   * @param req The rpc request
   * @return folly::Future<cpp2::AppendLogResponse>
   */
  folly::Future<cpp2::AppendLogResponse> sendAppendLogRequest(
      folly::EventBase* eb, std::shared_ptr<cpp2::AppendLogRequest> req);

  /**
   * @brief Send the append log rpc and handle the response
   *
   * @param eb The eventbase to send rpc
   * @param req The rpc request
   */
  void appendLogsInternal(folly::EventBase* eb, std::shared_ptr<cpp2::AppendLogRequest> req);

  folly::Future<cpp2::HeartbeatResponse> sendHeartbeatRequest(
      folly::EventBase* eb, std::shared_ptr<cpp2::HeartbeatRequest> req);

  /**
   * @brief Build the append log request based on the log id
   *
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::shared_ptr<cpp2::AppendLogRequest>>
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::shared_ptr<cpp2::AppendLogRequest>>
  prepareAppendLogRequest();

  /**
   * @brief Begin to start snapshot when we don't have the log in wal file
   *
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode startSendSnapshot();

  /**
   * @brief Return true if there isn't a request in flight
   */
  bool noRequest() const;

  /**
   * @brief Notify the RaftPart the result of sending logs to peers
   *
   * @param resp RPC response
   */
  void setResponse(const cpp2::AppendLogResponse& resp);

  /**
   * @brief If there are more logs to send, build the append log request
   *
   * @param self Shared ptr of Host itself
   * @return std::shared_ptr<cpp2::AppendLogRequest> The request if there are logs to send, return
   * nullptr there are none
   */
  std::shared_ptr<cpp2::AppendLogRequest> getPendingReqIfAny(std::shared_ptr<Host> self);

 private:
  // <term, logId, committedLogId>
  using Request = std::tuple<TermID, LogID, LogID>;

  std::shared_ptr<RaftPart> part_;
  const HostAddr addr_;
  bool isLearner_ = false;
  const std::string idStr_;

  mutable std::mutex lock_;

  bool paused_{false};
  bool stopped_{false};

  // whether there is a batch of logs for target host in on going
  bool requestOnGoing_{false};
  // whether there is a snapshot for target host in on going
  bool sendingSnapshot_{false};

  std::condition_variable noMoreRequestCV_;
  folly::SharedPromise<cpp2::AppendLogResponse> promise_;
  folly::SharedPromise<cpp2::AppendLogResponse> cachingPromise_;

  Request pendingReq_{0, 0, 0};

  // These logId and term pointing to the latest log we need to send
  LogID logIdToSend_{0};
  TermID logTermToSend_{0};

  // The previous log before this batch
  LogID lastLogIdSent_{0};
  TermID lastLogTermSent_{0};

  LogID committedLogId_{0};

  // CommittedLogId of follower
  LogID followerCommittedLogId_{0};
};

}  // namespace raftex
}  // namespace nebula

#endif  // RAFTEX_HOST_H_
