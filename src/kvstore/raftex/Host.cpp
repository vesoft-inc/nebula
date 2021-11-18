/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/raftex/Host.h"

#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/io/async/EventBase.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/network/NetworkUtils.h"
#include "kvstore/raftex/RaftPart.h"
#include "kvstore/wal/FileBasedWal.h"

DEFINE_uint32(max_appendlog_batch_size,
              128,
              "The max number of logs in each appendLog request batch");
DEFINE_uint32(max_outstanding_requests, 1024, "The max number of outstanding appendLog requests");
DEFINE_int32(raft_rpc_timeout_ms, 500, "rpc timeout for raft client");

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
  LOG(INFO) << idStr_ << "The host has been stopped!";
}

cpp2::ErrorCode Host::checkStatus() const {
  CHECK(!lock_.try_lock());
  if (stopped_) {
    VLOG(2) << idStr_ << "The host is stopped, just return";
    return cpp2::ErrorCode::E_HOST_STOPPED;
  }

  return cpp2::ErrorCode::SUCCEEDED;
}

folly::Future<cpp2::AskForVoteResponse> Host::askForVote(const cpp2::AskForVoteRequest& req,
                                                         folly::EventBase* eb) {
  {
    std::lock_guard<std::mutex> g(lock_);
    auto res = checkStatus();
    if (res != cpp2::ErrorCode::SUCCEEDED) {
      VLOG(2) << idStr_ << "The Host is not in a proper status, do not send";
      cpp2::AskForVoteResponse resp;
      resp.set_error_code(res);
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
  VLOG(3) << idStr_ << "Entering Host::appendLogs()";

  auto ret = folly::Future<cpp2::AppendLogResponse>::makeEmpty();
  std::shared_ptr<cpp2::AppendLogRequest> req;
  {
    std::lock_guard<std::mutex> g(lock_);

    auto res = checkStatus();

    if (UNLIKELY(sendingSnapshot_)) {
      LOG_EVERY_N(INFO, 500) << idStr_ << "The target host is waiting for a snapshot";
      res = cpp2::ErrorCode::E_WAITING_SNAPSHOT;
    } else if (requestOnGoing_) {
      // buffer incoming request to pendingReq_
      if (cachingPromise_.size() <= FLAGS_max_outstanding_requests) {
        pendingReq_ = std::make_tuple(term, logId, committedLogId);
        return cachingPromise_.getFuture();
      } else {
        LOG_EVERY_N(INFO, 200) << idStr_ << "Too many requests are waiting, return error";
        res = cpp2::ErrorCode::E_TOO_MANY_REQUESTS;
      }
    }

    if (res != cpp2::ErrorCode::SUCCEEDED) {
      VLOG(2) << idStr_ << "The host is not in a proper status, just return";
      cpp2::AppendLogResponse r;
      r.set_error_code(res);
      return r;
    }

    VLOG(2) << idStr_ << "About to send the AppendLog request";

    // No request is ongoing, let's send a new request
    if (UNLIKELY(lastLogIdSent_ == 0 && lastLogTermSent_ == 0)) {
      lastLogIdSent_ = prevLogId;
      lastLogTermSent_ = prevLogTerm;
      LOG(INFO) << idStr_ << "This is the first time to send the logs to this host"
                << ", lastLogIdSent = " << lastLogIdSent_
                << ", lastLogTermSent = " << lastLogTermSent_;
    }
    logTermToSend_ = term;
    logIdToSend_ = logId;
    committedLogId_ = committedLogId;

    auto result = prepareAppendLogRequest();
    if (ok(result)) {
      LOG_IF(INFO, FLAGS_trace_raft) << idStr_ << "Sending the pending request in the queue"
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
      r.set_error_code(error(result));
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
  sendAppendLogRequest(eb, req)
      .via(eb)
      .thenValue([eb, self = shared_from_this()](cpp2::AppendLogResponse&& resp) {
        LOG_IF(INFO, FLAGS_trace_raft)
            << self->idStr_ << "AppendLogResponse "
            << "code " << apache::thrift::util::enumNameSafe(resp.get_error_code()) << ", currTerm "
            << resp.get_current_term() << ", lastLogId " << resp.get_last_log_id()
            << ", lastLogTerm " << resp.get_last_log_term() << ", commitLogId "
            << resp.get_committed_log_id() << ", lastLogIdSent_ " << self->lastLogIdSent_
            << ", lastLogTermSent_ " << self->lastLogTermSent_;
        switch (resp.get_error_code()) {
          case cpp2::ErrorCode::SUCCEEDED:
          case cpp2::ErrorCode::E_LOG_GAP:
          case cpp2::ErrorCode::E_LOG_STALE: {
            VLOG(2) << self->idStr_ << "AppendLog request sent successfully";

            std::shared_ptr<cpp2::AppendLogRequest> newReq;
            {
              std::lock_guard<std::mutex> g(self->lock_);
              auto res = self->checkStatus();
              if (res != cpp2::ErrorCode::SUCCEEDED) {
                cpp2::AppendLogResponse r;
                r.set_error_code(res);
                self->setResponse(r);
                return;
              }
              // Host is working
              self->lastLogIdSent_ = resp.get_last_log_id();
              self->lastLogTermSent_ = resp.get_last_log_term();
              self->followerCommittedLogId_ = resp.get_committed_log_id();
              if (self->lastLogIdSent_ < self->logIdToSend_) {
                // More to send
                VLOG(2) << self->idStr_ << "There are more logs to send";
                auto result = self->prepareAppendLogRequest();
                if (ok(result)) {
                  newReq = std::move(value(result));
                } else {
                  cpp2::AppendLogResponse r;
                  r.set_error_code(error(result));
                  self->setResponse(r);
                  return;
                }
              } else {
                // resp.get_last_log_id() >= self->logIdToSend_
                // All logs up to logIdToSend_ has been sent, fulfill the promise
                self->promise_.setValue(resp);
                // Check if there are any pending request:
                // Eithor send pending requst if any, or set Host to vacant
                newReq = self->getPendingReqIfAny(self);
              }
            }
            if (newReq) {
              self->appendLogsInternal(eb, newReq);
            }
            return;
          }
          // Usually the peer is not in proper state, for example:
          // E_UNKNOWN_PART/E_BAD_STATE/E_NOT_READY/E_WAITING_SNAPSHOT
          // In this case, nothing changed, just return the error
          default: {
            LOG_EVERY_N(ERROR, 100)
                << self->idStr_ << "Failed to append logs to the host (Err: "
                << apache::thrift::util::enumNameSafe(resp.get_error_code()) << ")";
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
                   VLOG(2) << self->idStr_ << ex.what();
                   cpp2::AppendLogResponse r;
                   r.set_error_code(cpp2::ErrorCode::E_RPC_EXCEPTION);
                   {
                     std::lock_guard<std::mutex> g(self->lock_);
                     if (ex.getType() == TransportException::TIMED_OUT) {
                       LOG_IF(INFO, FLAGS_trace_raft)
                           << self->idStr_ << "append log time out"
                           << ", space " << req->get_space() << ", part " << req->get_part()
                           << ", current term " << req->get_current_term() << ", last_log_id "
                           << req->get_last_log_id() << ", committed_id "
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
        VLOG(2) << self->idStr_ << ex.what();
        cpp2::AppendLogResponse r;
        r.set_error_code(cpp2::ErrorCode::E_RPC_EXCEPTION);
        {
          std::lock_guard<std::mutex> g(self->lock_);
          self->setResponse(r);
        }
        // a new raft log or heartbeat will trigger another appendLogs in Host
        return;
      });
}

ErrorOr<cpp2::ErrorCode, std::shared_ptr<cpp2::AppendLogRequest>> Host::prepareAppendLogRequest() {
  CHECK(!lock_.try_lock());
  VLOG(2) << idStr_ << "Prepare AppendLogs request from Log " << lastLogIdSent_ + 1 << " to "
          << logIdToSend_;
  if (lastLogIdSent_ + 1 > part_->wal()->lastLogId()) {
    LOG_IF(INFO, FLAGS_trace_raft)
        << idStr_ << "My lastLogId in wal is " << part_->wal()->lastLogId()
        << ", but you are seeking " << lastLogIdSent_ + 1
        << ", so i have nothing to send, logIdToSend_ = " << logIdToSend_;
    return cpp2::ErrorCode::E_NO_WAL_FOUND;
  }
  auto it = part_->wal()->iterator(lastLogIdSent_ + 1, logIdToSend_);
  if (it->valid()) {
    auto term = it->logTerm();
    auto req = std::make_shared<cpp2::AppendLogRequest>();
    req->set_space(part_->spaceId());
    req->set_part(part_->partitionId());
    req->set_current_term(logTermToSend_);
    req->set_last_log_id(logIdToSend_);
    req->set_leader_addr(part_->address().host);
    req->set_leader_port(part_->address().port);
    req->set_committed_log_id(committedLogId_);
    req->set_last_log_term_sent(lastLogTermSent_);
    req->set_last_log_id_sent(lastLogIdSent_);
    req->set_log_term(term);

    std::vector<cpp2::LogEntry> logs;
    for (size_t cnt = 0;
         it->valid() && it->logTerm() == term && cnt < FLAGS_max_appendlog_batch_size;
         ++(*it), ++cnt) {
      cpp2::LogEntry le;
      le.set_cluster(it->logSource());
      le.set_log_str(it->logMsg().toString());
      logs.emplace_back(std::move(le));
    }
    req->set_log_str_list(std::move(logs));
    return req;
  } else {
    if (!sendingSnapshot_) {
      LOG(INFO) << idStr_ << "Can't find log " << lastLogIdSent_ + 1 << " in wal, send the snapshot"
                << ", logIdToSend = " << logIdToSend_
                << ", firstLogId in wal = " << part_->wal()->firstLogId()
                << ", lastLogId in wal = " << part_->wal()->lastLogId();
      sendingSnapshot_ = true;
      part_->snapshot_->sendSnapshot(part_, addr_)
          .thenValue([self = shared_from_this()](auto&& status) {
            std::lock_guard<std::mutex> g(self->lock_);
            if (status.ok()) {
              auto commitLogIdAndTerm = status.value();
              self->lastLogIdSent_ = commitLogIdAndTerm.first;
              self->lastLogTermSent_ = commitLogIdAndTerm.second;
              self->followerCommittedLogId_ = commitLogIdAndTerm.first;
              LOG(INFO) << self->idStr_ << "Send snapshot succeeded!"
                        << " commitLogId = " << commitLogIdAndTerm.first
                        << " commitLogTerm = " << commitLogIdAndTerm.second;
            } else {
              LOG(INFO) << self->idStr_ << "Send snapshot failed!";
              // TODO(heng): we should tell the follower i am failed.
            }
            self->sendingSnapshot_ = false;
            self->noMoreRequestCV_.notify_all();
          });
    } else {
      LOG_EVERY_N(INFO, 100) << idStr_ << "The snapshot req is in queue, please wait for a moment";
    }
    return cpp2::ErrorCode::E_WAITING_SNAPSHOT;
  }
}

folly::Future<cpp2::AppendLogResponse> Host::sendAppendLogRequest(
    folly::EventBase* eb, std::shared_ptr<cpp2::AppendLogRequest> req) {
  VLOG(2) << idStr_ << "Entering Host::sendAppendLogRequest()";

  {
    std::lock_guard<std::mutex> g(lock_);
    auto res = checkStatus();
    if (res != cpp2::ErrorCode::SUCCEEDED) {
      LOG(WARNING) << idStr_ << "The Host is not in a proper status, do not send";
      cpp2::AppendLogResponse resp;
      resp.set_error_code(res);
      return resp;
    }
  }

  LOG_IF(INFO, FLAGS_trace_raft) << idStr_ << "Sending appendLog: space " << req->get_space()
                                 << ", part " << req->get_part() << ", current term "
                                 << req->get_current_term() << ", last_log_id "
                                 << req->get_last_log_id() << ", committed_id "
                                 << req->get_committed_log_id() << ", last_log_term_sent "
                                 << req->get_last_log_term_sent() << ", last_log_id_sent "
                                 << req->get_last_log_id_sent() << ", logs in request "
                                 << req->get_log_str_list().size();
  // Get client connection
  auto client = part_->clientMan_->client(addr_, eb, false, FLAGS_raft_rpc_timeout_ms);
  return client->future_appendLog(*req);
}

folly::Future<cpp2::HeartbeatResponse> Host::sendHeartbeat(folly::EventBase* eb,
                                                           TermID term,
                                                           LogID latestLogId,
                                                           LogID commitLogId,
                                                           TermID lastLogTerm,
                                                           LogID lastLogId) {
  auto req = std::make_shared<cpp2::HeartbeatRequest>();
  req->set_space(part_->spaceId());
  req->set_part(part_->partitionId());
  req->set_current_term(term);
  req->set_last_log_id(latestLogId);
  req->set_committed_log_id(commitLogId);
  req->set_leader_addr(part_->address().host);
  req->set_leader_port(part_->address().port);
  req->set_last_log_term_sent(lastLogTerm);
  req->set_last_log_id_sent(lastLogId);
  folly::Promise<cpp2::HeartbeatResponse> promise;
  auto future = promise.getFuture();
  sendHeartbeatRequest(eb, std::move(req))
      .via(eb)
      .then([self = shared_from_this(),
             pro = std::move(promise)](folly::Try<cpp2::HeartbeatResponse>&& t) mutable {
        VLOG(3) << self->idStr_ << "heartbeat call got response";
        if (t.hasException()) {
          cpp2::HeartbeatResponse resp;
          resp.set_error_code(cpp2::ErrorCode::E_RPC_EXCEPTION);
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
  VLOG(2) << idStr_ << "Entering Host::sendHeartbeatRequest()";

  {
    std::lock_guard<std::mutex> g(lock_);
    auto res = checkStatus();
    if (res != cpp2::ErrorCode::SUCCEEDED) {
      LOG(WARNING) << idStr_ << "The Host is not in a proper status, do not send";
      cpp2::HeartbeatResponse resp;
      resp.set_error_code(res);
      return resp;
    }
  }

  LOG_IF(INFO, FLAGS_trace_raft) << idStr_ << "Sending heartbeat: space " << req->get_space()
                                 << ", part " << req->get_part() << ", current term "
                                 << req->get_current_term() << ", last_log_id "
                                 << req->get_last_log_id() << ", committed_id "
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

  LOG_IF(INFO, FLAGS_trace_raft) << self->idStr_ << "Sending the pending request in the queue"
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
    r.set_error_code(error(result));
    self->setResponse(r);
    return nullptr;
  }
}

}  // namespace raftex
}  // namespace nebula
