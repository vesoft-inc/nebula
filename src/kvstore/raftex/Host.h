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

class Host final : public std::enable_shared_from_this<Host> {
  friend class RaftPart;

 public:
  Host(const HostAddr& addr, std::shared_ptr<RaftPart> part, bool isLearner = false);

  ~Host() {
    LOG(INFO) << idStr_ << " The host has been destroyed!";
  }

  const char* idStr() const {
    return idStr_.c_str();
  }

  void stop() {
    std::lock_guard<std::mutex> g(lock_);
    stopped_ = true;
  }

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

  void waitForStop();

  bool isLearner() const {
    return isLearner_;
  }

  void setLearner(bool isLearner) {
    isLearner_ = isLearner;
  }

  folly::Future<cpp2::AskForVoteResponse> askForVote(const cpp2::AskForVoteRequest& req,
                                                     folly::EventBase* eb);

  // When logId == lastLogIdSent, it is a heartbeat
  folly::Future<cpp2::AppendLogResponse> appendLogs(
      folly::EventBase* eb,
      TermID term,             // Current term
      LogID logId,             // The last log to be sent
      LogID committedLogId,    // The last committed log id
      TermID lastLogTermSent,  // The last log term being sent
      LogID lastLogIdSent);    // The last log id being sent

  folly::Future<cpp2::HeartbeatResponse> sendHeartbeat(folly::EventBase* eb,
                                                       TermID term,
                                                       LogID commitLogId,
                                                       TermID lastLogTerm,
                                                       LogID lastLogId);

  const HostAddr& address() const {
    return addr_;
  }

 private:
  cpp2::ErrorCode checkStatus() const;

  folly::Future<cpp2::AppendLogResponse> sendAppendLogRequest(
      folly::EventBase* eb, std::shared_ptr<cpp2::AppendLogRequest> req);

  void appendLogsInternal(folly::EventBase* eb, std::shared_ptr<cpp2::AppendLogRequest> req);

  folly::Future<cpp2::HeartbeatResponse> sendHeartbeatRequest(
      folly::EventBase* eb, std::shared_ptr<cpp2::HeartbeatRequest> req);

  ErrorOr<cpp2::ErrorCode, std::shared_ptr<cpp2::AppendLogRequest>> prepareAppendLogRequest();

  bool noRequest() const;

  void setResponse(const cpp2::AppendLogResponse& r);

  std::shared_ptr<cpp2::AppendLogRequest> getPendingReqIfAny(std::shared_ptr<Host> self);

 private:
  // <term, logId, committedLogId>
  using Request = std::tuple<TermID, LogID, LogID>;

  std::shared_ptr<RaftPart> part_;
  const HostAddr addr_;
  bool isLearner_ = false;
  const std::string idStr_;

  mutable std::mutex lock_;

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
