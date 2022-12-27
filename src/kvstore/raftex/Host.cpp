/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/raftex/Host.h"

#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/io/async/EventBase.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/network/NetworkUtils.h"
#include "common/stats/StatsManager.h"
#include "common/time/WallClock.h"
#include "kvstore/raftex/RaftPart.h"
#include "kvstore/stats/KVStats.h"
#include "kvstore/wal/FileBasedWal.h"

DEFINE_uint32(max_appendlog_batch_size,
              128,
              "The max number of logs in each appendLog request batch");
DEFINE_uint32(max_outstanding_requests, 1024, "The max number of outstanding appendLog requests");
DEFINE_int32(raft_rpc_timeout_ms, 1000, "rpc timeout for raft client");

DECLARE_bool(trace_raft);
DECLARE_uint32(raft_heartbeat_interval_secs);

namespace nebula {
namespace raftex {

using nebula::network::NetworkUtils;

Host::Host(const HostAddr& addr, std::shared_ptr<RaftPart> part, bool isLearner)
    : part_(std::move(part)),
      addr_(addr),
      isLearner_(isLearner),
      idStr_(folly::stringPrintf(
          "%s[Host: %s:%d] ", part_->idStr_.c_str(), addr_.host.c_str(), addr_.port)),
      cachingPromise_(folly::SharedPromise<cpp2::AppendLogResponse>()) {}

void Host::waitForStop() {
  std::unique_lock<std::mutex> g(lock_);

  CHECK(stopped_);
  noMoreRequestCV_.wait(g, [this] { return !requestOnGoing_; });
  VLOG(1) << idStr_ << "The host has been stopped!";
}

nebula::cpp2::ErrorCode Host::canAppendLog() const {
  CHECK(!lock_.try_lock());
  if (stopped_) {
    VLOG(3) << idStr_ << "The host is stopped, just return";
    return nebula::cpp2::ErrorCode::E_RAFT_HOST_STOPPED;
  }

  if (paused_) {
    VLOG(3) << idStr_ << "The host is paused, due to losing leadership";
    return nebula::cpp2::ErrorCode::E_RAFT_HOST_PAUSED;
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

folly::Future<cpp2::AskForVoteResponse> Host::askForVote(const cpp2::AskForVoteRequest& req,
                                                         folly::EventBase* eb) {
  {
    std::lock_guard<std::mutex> g(lock_);
    if (stopped_) {
      VLOG(3) << idStr_ << "The Host is not in a proper status, do not send";
      cpp2::AskForVoteResponse resp;
      resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_HOST_STOPPED;
      return resp;
    }
  }
  auto client = part_->clientMan_->client(addr_, eb, false, FLAGS_raft_rpc_timeout_ms);
  return client->future_askForVote(req);
}

folly::Future<cpp2::AppendLogResponse> Host::appendLogs(folly::EventBase* eb,
                                                        TermID term,
                                                        LogID logId,
                                                        LogID committedLogId,
                                                        TermID prevLogTerm,
                                                        LogID prevLogId) {
  VLOG(4) << idStr_ << "Entering Host::appendLogs()";

  auto ret = folly::Future<cpp2::AppendLogResponse>::makeEmpty();
  std::shared_ptr<cpp2::AppendLogRequest> req;
  {
    std::lock_guard<std::mutex> g(lock_);

    auto res = canAppendLog();

    if (UNLIKELY(sendingSnapshot_)) {
      VLOG_EVERY_N(2, 1000) << idStr_ << "The target host is waiting for a snapshot";
      res = nebula::cpp2::ErrorCode::E_RAFT_WAITING_SNAPSHOT;
    } else if (requestOnGoing_) {
      // buffer incoming request to pendingReq_
      if (cachingPromise_.size() <= FLAGS_max_outstanding_requests) {
        pendingReq_ = std::make_tuple(term, logId, committedLogId);
        return cachingPromise_.getFuture();
      } else {
        VLOG_EVERY_N(2, 1000) << idStr_ << "Too many requests are waiting, return error";
        res = nebula::cpp2::ErrorCode::E_RAFT_TOO_MANY_REQUESTS;
      }
    }

    if (res != nebula::cpp2::ErrorCode::SUCCEEDED) {
      VLOG(3) << idStr_ << "The host is not in a proper status, just return";
      cpp2::AppendLogResponse r;
      r.error_code_ref() = res;
      return r;
    }

    VLOG(4) << idStr_ << "About to send the AppendLog request";

    // No request is ongoing, let's send a new request
    if (UNLIKELY(lastLogIdSent_ == 0 && lastLogTermSent_ == 0)) {
      lastLogIdSent_ = prevLogId;
      lastLogTermSent_ = prevLogTerm;
      VLOG(2) << idStr_ << "This is the first time to send the logs to this host"
              << ", lastLogIdSent = " << lastLogIdSent_
              << ", lastLogTermSent = " << lastLogTermSent_;
    }
    logTermToSend_ = term;
    logIdToSend_ = logId;
    committedLogId_ = committedLogId;

    auto result = prepareAppendLogRequest();
    if (ok(result)) {
      VLOG_IF(1, FLAGS_trace_raft) << idStr_ << "Sending the pending request in the queue"
                                   << ", from " << lastLogIdSent_ + 1 << " to " << logIdToSend_;
      req = std::move(value(result));
      pendingReq_ = std::make_tuple(0, 0, 0);
      promise_ = std::move(cachingPromise_);
      cachingPromise_ = folly::SharedPromise<cpp2::AppendLogResponse>();
      ret = promise_.getFuture();
      requestOnGoing_ = true;
    } else {
      // target host is waiting for a snapshot or wal not found
      cpp2::AppendLogResponse r;
      r.error_code_ref() = error(result);
      return r;
    }
  }

  // Get a new promise
  appendLogsInternal(eb, std::move(req));

  return ret;
}

void Host::setResponse(const cpp2::AppendLogResponse& r) {
  CHECK(!lock_.try_lock());
  promise_.setValue(r);
  cachingPromise_.setValue(r);
  cachingPromise_ = folly::SharedPromise<cpp2::AppendLogResponse>();
  pendingReq_ = std::make_tuple(0, 0, 0);
  requestOnGoing_ = false;
  noMoreRequestCV_.notify_all();
}

void Host::appendLogsInternal(folly::EventBase* eb, std::shared_ptr<cpp2::AppendLogRequest> req) {
  using TransportException = apache::thrift::transport::TTransportException;
  auto beforeRpcUs = time::WallClock::fastNowInMicroSec();
  sendAppendLogRequest(eb, req)
      .via(eb)
      .thenValue([eb, beforeRpcUs, self = shared_from_this()](cpp2::AppendLogResponse&& resp) {
        stats::StatsManager::addValue(kAppendLogLatencyUs,
                                      time::WallClock::fastNowInMicroSec() - beforeRpcUs);
        VLOG_IF(1, FLAGS_trace_raft)
            << self->idStr_ << "AppendLogResponse "
            << "code " << apache::thrift::util::enumNameSafe(resp.get_error_code()) << ", currTerm "
            << resp.get_current_term() << ", lastLogTerm " << resp.get_last_matched_log_term()
            << ", commitLogId " << resp.get_committed_log_id() << ", lastLogIdSent_ "
            << self->lastLogIdSent_ << ", lastLogTermSent_ " << self->lastLogTermSent_;
        switch (resp.get_error_code()) {
          case nebula::cpp2::ErrorCode::SUCCEEDED:
          case nebula::cpp2::ErrorCode::E_RAFT_LOG_GAP:
          case nebula::cpp2::ErrorCode::E_RAFT_LOG_STALE: {
            VLOG(3) << self->idStr_ << "AppendLog request sent successfully";

            std::shared_ptr<cpp2::AppendLogRequest> newReq;
            {
              std::lock_guard<std::mutex> g(self->lock_);
              auto res = self->canAppendLog();
              if (res != nebula::cpp2::ErrorCode::SUCCEEDED) {
                cpp2::AppendLogResponse r;
                r.error_code_ref() = res;
                self->setResponse(r);
                return;
              }
              // Host is working
              self->lastLogIdSent_ = resp.get_last_matched_log_id();
              self->lastLogTermSent_ = resp.get_last_matched_log_term();
              self->followerCommittedLogId_ = resp.get_committed_log_id();
              if (self->lastLogIdSent_ < self->logIdToSend_) {
                // More to send
                VLOG(3) << self->idStr_ << "There are more logs to send";
                auto result = self->prepareAppendLogRequest();
                if (ok(result)) {
                  newReq = std::move(value(result));
                } else {
                  cpp2::AppendLogResponse r;
                  r.error_code_ref() = error(result);
                  self->setResponse(r);
                  return;
                }
              } else {
                // resp.get_last_matched_log_id() >= self->logIdToSend_
                // All logs up to logIdToSend_ has been sent, fulfill the promise
                self->promise_.setValue(resp);
                // Check if there are any pending request:
                // Either send pending request if any, or set Host to vacant
                newReq = self->getPendingReqIfAny(self);
              }
            }
            if (newReq) {
              self->appendLogsInternal(eb, newReq);
            }
            return;
          }
          // Usually the peer is not in proper state, for example:
          // E_RAFT_UNKNOWN_PART/E_RAFT_STOPPED/E_RAFT_NOT_READY/E_RAFT_WAITING_SNAPSHOT
          // In this case, nothing changed, just return the error
          default: {
            VLOG_EVERY_N(2, 1000) << self->idStr_ << "Failed to append logs to the host (Err: "
                                  << apache::thrift::util::enumNameSafe(resp.get_error_code())
                                  << ")";
            {
              std::lock_guard<std::mutex> g(self->lock_);
              self->setResponse(resp);
            }
            return;
          }
        }
      })
      .thenError(folly::tag_t<TransportException>{},
                 [self = shared_from_this(), req](TransportException&& ex) {
                   VLOG(4) << self->idStr_ << ex.what();
                   cpp2::AppendLogResponse r;
                   r.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_RPC_EXCEPTION;
                   {
                     std::lock_guard<std::mutex> g(self->lock_);
                     if (ex.getType() == TransportException::TIMED_OUT) {
                       VLOG_IF(1, FLAGS_trace_raft)
                           << self->idStr_ << "append log time out"
                           << ", space " << req->get_space() << ", part " << req->get_part()
                           << ", current term " << req->get_current_term() << ", committed_id "
                           << req->get_committed_log_id() << ", last_log_term_sent "
                           << req->get_last_log_term_sent() << ", last_log_id_sent "
                           << req->get_last_log_id_sent() << ", set lastLogIdSent_ to logIdToSend_ "
                           << self->logIdToSend_ << ", logs size "
                           << req->get_log_str_list().size();
                     }
                     self->setResponse(r);
                   }
                   // a new raft log or heartbeat will trigger another appendLogs in Host
                   return;
                 })
      .thenError(folly::tag_t<std::exception>{}, [self = shared_from_this()](std::exception&& ex) {
        VLOG(4) << self->idStr_ << ex.what();
        cpp2::AppendLogResponse r;
        r.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_RPC_EXCEPTION;
        {
          std::lock_guard<std::mutex> g(self->lock_);
          self->setResponse(r);
        }
        // a new raft log or heartbeat will trigger another appendLogs in Host
        return;
      });
}

ErrorOr<nebula::cpp2::ErrorCode, std::shared_ptr<cpp2::AppendLogRequest>>
Host::prepareAppendLogRequest() {
  CHECK(!lock_.try_lock());
  VLOG(3) << idStr_ << "Prepare AppendLogs request from Log " << lastLogIdSent_ + 1 << " to "
          << logIdToSend_;

  auto makeReq = [this]() -> std::shared_ptr<cpp2::AppendLogRequest> {
    auto req = std::make_shared<cpp2::AppendLogRequest>();
    req->space_ref() = part_->spaceId();
    req->part_ref() = part_->partitionId();
    req->current_term_ref() = logTermToSend_;
    req->committed_log_id_ref() = committedLogId_;
    req->leader_addr_ref() = part_->address().host;
    req->leader_port_ref() = part_->address().port;
    req->last_log_term_sent_ref() = lastLogTermSent_;
    req->last_log_id_sent_ref() = lastLogIdSent_;
    return req;
  };

  // We need to use lastLogIdSent_ + 1 to check whether need to send snapshot
  if (UNLIKELY(lastLogIdSent_ + 1 < part_->wal()->firstLogId())) {
    return startSendSnapshot();
  }

  if (lastLogIdSent_ == logIdToSend_) {
    auto req = makeReq();
    return req;
  }

  if (lastLogIdSent_ + 1 > part_->wal()->lastLogId()) {
    VLOG_IF(1, FLAGS_trace_raft) << idStr_ << "My lastLogId in wal is " << part_->wal()->lastLogId()
                                 << ", but you are seeking " << lastLogIdSent_ + 1
                                 << ", so i have nothing to send, logIdToSend_ = " << logIdToSend_;
    return nebula::cpp2::ErrorCode::E_RAFT_NO_WAL_FOUND;
  }

  auto it = part_->wal()->iterator(lastLogIdSent_ + 1, logIdToSend_);
  if (it->valid()) {
    auto req = makeReq();
    std::vector<cpp2::RaftLogEntry> logs;
    for (size_t cnt = 0; it->valid() && cnt < FLAGS_max_appendlog_batch_size; ++(*it), ++cnt) {
      cpp2::RaftLogEntry entry;
      entry.cluster_ref() = it->logSource();
      entry.log_str_ref() = it->logMsg().toString();
      entry.log_term_ref() = it->logTerm();
      logs.emplace_back(std::move(entry));
    }
    // the last log entry's id is (lastLogIdSent_ + cnt), when iterator is invalid and last log
    // entry's id is not logIdToSend_, which means the log has been rollbacked
    if (!it->valid() && (lastLogIdSent_ + static_cast<int64_t>(logs.size()) != logIdToSend_)) {
      VLOG_IF(1, FLAGS_trace_raft)
          << idStr_ << "Can't find log in wal, logIdToSend_ = " << logIdToSend_;
      return nebula::cpp2::ErrorCode::E_RAFT_NO_WAL_FOUND;
    }
    req->log_str_list_ref() = std::move(logs);
    return req;
  } else {
    return nebula::cpp2::ErrorCode::E_RAFT_NO_WAL_FOUND;
  }
}

nebula::cpp2::ErrorCode Host::startSendSnapshot() {
  CHECK(!lock_.try_lock());
  if (!sendingSnapshot_) {
    VLOG(1) << idStr_ << "Can't find log " << lastLogIdSent_ + 1 << " in wal, send the snapshot"
            << ", logIdToSend = " << logIdToSend_
            << ", firstLogId in wal = " << part_->wal()->firstLogId()
            << ", lastLogId in wal = " << part_->wal()->lastLogId();
    sendingSnapshot_ = true;
    stats::StatsManager::addValue(kNumSendSnapshot);
    part_->snapshot_->sendSnapshot(part_, addr_)
        .thenValue([self = shared_from_this()](auto&& status) {
          std::lock_guard<std::mutex> g(self->lock_);
          if (status.ok()) {
            auto commitLogIdAndTerm = status.value();
            self->lastLogIdSent_ = commitLogIdAndTerm.first;
            self->lastLogTermSent_ = commitLogIdAndTerm.second;
            self->followerCommittedLogId_ = commitLogIdAndTerm.first;
            VLOG(1) << self->idStr_ << "Send snapshot succeeded!"
                    << " commitLogId = " << commitLogIdAndTerm.first
                    << " commitLogTerm = " << commitLogIdAndTerm.second;
          } else {
            VLOG(1) << self->idStr_ << "Send snapshot failed!";
            // TODO(heng): we should tell the follower i am failed.
          }
          self->sendingSnapshot_ = false;
          self->noMoreRequestCV_.notify_all();
        });
  } else {
    VLOG_EVERY_N(2, 1000) << idStr_ << "The snapshot req is in queue, please wait for a moment";
  }
  return nebula::cpp2::ErrorCode::E_RAFT_WAITING_SNAPSHOT;
}

folly::Future<cpp2::AppendLogResponse> Host::sendAppendLogRequest(
    folly::EventBase* eb, std::shared_ptr<cpp2::AppendLogRequest> req) {
  VLOG(4) << idStr_ << "Entering Host::sendAppendLogRequest()";

  {
    std::lock_guard<std::mutex> g(lock_);
    auto res = canAppendLog();
    if (res != nebula::cpp2::ErrorCode::SUCCEEDED) {
      VLOG(3) << idStr_ << "The Host is not in a proper status, do not send";
      cpp2::AppendLogResponse resp;
      resp.error_code_ref() = res;
      return resp;
    }
  }

  VLOG_IF(1, FLAGS_trace_raft) << idStr_ << "Sending appendLog: space " << req->get_space()
                               << ", part " << req->get_part() << ", current term "
                               << req->get_current_term() << ", committed_id "
                               << req->get_committed_log_id() << ", last_log_term_sent "
                               << req->get_last_log_term_sent() << ", last_log_id_sent "
                               << req->get_last_log_id_sent() << ", logs in request "
                               << req->get_log_str_list().size();
  // Get client connection
  auto client = part_->clientMan_->client(addr_, eb, false, FLAGS_raft_rpc_timeout_ms);
  return client->future_appendLog(*req);
}

folly::Future<cpp2::HeartbeatResponse> Host::sendHeartbeat(
    folly::EventBase* eb, TermID term, LogID commitLogId, TermID lastLogTerm, LogID lastLogId) {
  auto req = std::make_shared<cpp2::HeartbeatRequest>();
  req->space_ref() = part_->spaceId();
  req->part_ref() = part_->partitionId();
  req->current_term_ref() = term;
  req->committed_log_id_ref() = commitLogId;
  req->leader_addr_ref() = part_->address().host;
  req->leader_port_ref() = part_->address().port;
  req->last_log_term_sent_ref() = lastLogTerm;
  req->last_log_id_sent_ref() = lastLogId;
  folly::Promise<cpp2::HeartbeatResponse> promise;
  auto future = promise.getFuture();
  sendHeartbeatRequest(eb, std::move(req))
      .via(eb)
      .then([self = shared_from_this(),
             pro = std::move(promise)](folly::Try<cpp2::HeartbeatResponse>&& t) mutable {
        VLOG(4) << self->idStr_ << "heartbeat call got response";
        if (t.hasException()) {
          cpp2::HeartbeatResponse resp;
          resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_RPC_EXCEPTION;
          pro.setValue(std::move(resp));
          return;
        } else {
          pro.setValue(std::move(t.value()));
        }
      });
  return future;
}

folly::Future<cpp2::HeartbeatResponse> Host::sendHeartbeatRequest(
    folly::EventBase* eb, std::shared_ptr<cpp2::HeartbeatRequest> req) {
  VLOG(4) << idStr_ << "Entering Host::sendHeartbeatRequest()";

  {
    std::lock_guard<std::mutex> g(lock_);
    auto res = canAppendLog();
    if (res != nebula::cpp2::ErrorCode::SUCCEEDED) {
      VLOG(3) << idStr_ << "The Host is not in a proper status, do not send";
      cpp2::HeartbeatResponse resp;
      resp.error_code_ref() = res;
      return resp;
    }
  }

  VLOG_IF(1, FLAGS_trace_raft) << idStr_ << "Sending heartbeat: space " << req->get_space()
                               << ", part " << req->get_part() << ", current term "
                               << req->get_current_term() << ", committed_id "
                               << req->get_committed_log_id() << ", last_log_term_sent "
                               << req->get_last_log_term_sent() << ", last_log_id_sent "
                               << req->get_last_log_id_sent();
  // Get client connection
  auto client = part_->clientMan_->client(addr_, eb, false, FLAGS_raft_rpc_timeout_ms);
  return client->future_heartbeat(*req);
}

bool Host::noRequest() const {
  CHECK(!lock_.try_lock());
  static auto emptyTup = std::make_tuple(0, 0, 0);
  return pendingReq_ == emptyTup;
}

std::shared_ptr<cpp2::AppendLogRequest> Host::getPendingReqIfAny(std::shared_ptr<Host> self) {
  CHECK(!self->lock_.try_lock());
  CHECK(self->requestOnGoing_) << self->idStr_;

  // Check if there are any pending request to send
  if (self->noRequest()) {
    self->noMoreRequestCV_.notify_all();
    self->requestOnGoing_ = false;
    return nullptr;
  }

  // there is pending request
  auto& tup = self->pendingReq_;
  self->logTermToSend_ = std::get<0>(tup);
  self->logIdToSend_ = std::get<1>(tup);
  self->committedLogId_ = std::get<2>(tup);

  VLOG_IF(1, FLAGS_trace_raft) << self->idStr_ << "Sending the pending request in the queue"
                               << ", from " << self->lastLogIdSent_ + 1 << " to "
                               << self->logIdToSend_;
  self->pendingReq_ = std::make_tuple(0, 0, 0);
  self->promise_ = std::move(self->cachingPromise_);
  self->cachingPromise_ = folly::SharedPromise<cpp2::AppendLogResponse>();

  auto result = self->prepareAppendLogRequest();
  if (ok(result)) {
    return value(result);
  } else {
    cpp2::AppendLogResponse r;
    r.error_code_ref() = error(result);
    self->setResponse(r);
    return nullptr;
  }
}

}  // namespace raftex
}  // namespace nebula
