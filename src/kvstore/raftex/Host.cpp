/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "kvstore/raftex/Host.h"
#include "kvstore/raftex/RaftPart.h"
#include "kvstore/wal/FileBasedWal.h"
#include "common/network/NetworkUtils.h"
#include <folly/io/async/EventBase.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

DEFINE_uint32(max_appendlog_batch_size, 128,
              "The max number of logs in each appendLog request batch");
DEFINE_uint32(max_outstanding_requests, 1024,
              "The max number of outstanding appendLog requests");
DEFINE_int32(raft_rpc_timeout_ms, 500, "rpc timeout for raft client");

DECLARE_bool(trace_raft);
DECLARE_uint32(raft_heartbeat_interval_secs);

namespace nebula {
namespace raftex {

using nebula::network::NetworkUtils;

Host::Host(const HostAddr& addr, std::shared_ptr<RaftPart> part, bool isLearner)
        : part_(std::move(part))
        , addr_(addr)
        , isLearner_(isLearner)
        , idStr_(folly::stringPrintf(
            "%s[Host: %s:%d] ",
            part_->idStr_.c_str(),
            addr_.host.c_str(),
            addr_.port))
        , cachingPromise_(folly::SharedPromise<cpp2::AppendLogResponse>()) {
}


void Host::waitForStop() {
    std::unique_lock<std::mutex> g(lock_);

    CHECK(stopped_);
    noMoreRequestCV_.wait(g, [this] {
        return !requestOnGoing_;
    });
    LOG(INFO) << idStr_ << "The host has been stopped!";
}


cpp2::ErrorCode Host::checkStatus() const {
    CHECK(!lock_.try_lock());
    if (stopped_) {
        VLOG(2) << idStr_ << "The host is stopped, just return";
        return cpp2::ErrorCode::E_HOST_STOPPED;
    }

    if (paused_) {
        VLOG(2) << idStr_
                << "The host is paused, due to losing leadership";
        return cpp2::ErrorCode::E_NOT_A_LEADER;
    }

    return cpp2::ErrorCode::SUCCEEDED;
}


folly::Future<cpp2::AskForVoteResponse> Host::askForVote(
        const cpp2::AskForVoteRequest& req,
        folly::EventBase* eb) {
    {
        std::lock_guard<std::mutex> g(lock_);
        auto res = checkStatus();
        if (res != cpp2::ErrorCode::SUCCEEDED) {
            VLOG(2) << idStr_
                    << "The Host is not in a proper status, do not send";
            cpp2::AskForVoteResponse resp;
            resp.set_error_code(res);
            return resp;
        }
    }
    auto client =
        part_->clientMan_->client(addr_, eb, false, FLAGS_raft_heartbeat_interval_secs * 1000);
    return client->future_askForVote(req);
}


folly::Future<cpp2::AppendLogResponse> Host::appendLogs(
        folly::EventBase* eb,
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
        if (logId <= lastLogIdSent_) {
            LOG(INFO) << idStr_ << "The log " << logId << " has been sended"
                      << ", lastLogIdSent " << lastLogIdSent_;
            cpp2::AppendLogResponse r;
            r.set_error_code(cpp2::ErrorCode::SUCCEEDED);
            return r;
        }

        if (requestOnGoing_ && res == cpp2::ErrorCode::SUCCEEDED) {
            if (cachingPromise_.size() <= FLAGS_max_outstanding_requests) {
                pendingReq_ = std::make_tuple(term,
                                              logId,
                                              committedLogId);
                return cachingPromise_.getFuture();
            } else {
                LOG_EVERY_N(INFO, 200) << idStr_ << "Too many requests are waiting, return error";
                cpp2::AppendLogResponse r;
                r.set_error_code(cpp2::ErrorCode::E_TOO_MANY_REQUESTS);
                return r;
            }
        }

        if (res != cpp2::ErrorCode::SUCCEEDED) {
            VLOG(2) << idStr_
                    << "The host is not in a proper status, just return";
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
        pendingReq_ = std::make_tuple(0, 0, 0);
        promise_ = std::move(cachingPromise_);
        cachingPromise_ = folly::SharedPromise<cpp2::AppendLogResponse>();
        ret = promise_.getFuture();

        requestOnGoing_ = true;

        req = prepareAppendLogRequest();
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
}

void Host::appendLogsInternal(folly::EventBase* eb,
                              std::shared_ptr<cpp2::AppendLogRequest> req) {
    sendAppendLogRequest(eb, std::move(req)).via(eb).then(
            [eb, self = shared_from_this()] (folly::Try<cpp2::AppendLogResponse>&& t) {
        VLOG(3) << self->idStr_ << "appendLogs() call got response";
        if (t.hasException()) {
            VLOG(2) << self->idStr_ << t.exception().what();
            cpp2::AppendLogResponse r;
            r.set_error_code(cpp2::ErrorCode::E_EXCEPTION);
            {
                std::lock_guard<std::mutex> g(self->lock_);
                self->setResponse(r);
                self->lastLogIdSent_ = self->logIdToSend_ - 1;
            }
            self->noMoreRequestCV_.notify_all();
            return;
        }

        cpp2::AppendLogResponse resp = std::move(t).value();
        LOG_IF(INFO, FLAGS_trace_raft)
            << self->idStr_ << "AppendLogResponse "
            << "code " << apache::thrift::util::enumNameSafe(resp.get_error_code())
            << ", currTerm " << resp.get_current_term()
            << ", lastLogId " << resp.get_last_log_id()
            << ", lastLogTerm " << resp.get_last_log_term()
            << ", commitLogId " << resp.get_committed_log_id()
            << ", lastLogIdSent_ " << self->lastLogIdSent_
            << ", lastLogTermSent_ " << self->lastLogTermSent_;
        switch (resp.get_error_code()) {
            case cpp2::ErrorCode::SUCCEEDED: {
                VLOG(2) << self->idStr_
                        << "AppendLog request sent successfully";

                std::shared_ptr<cpp2::AppendLogRequest> newReq;
                {
                    std::lock_guard<std::mutex> g(self->lock_);
                    auto res = self->checkStatus();
                    if (res != cpp2::ErrorCode::SUCCEEDED) {
                        VLOG(2) << self->idStr_
                                << "The host is not in a proper status,"
                                   " just return";
                        cpp2::AppendLogResponse r;
                        r.set_error_code(res);
                        self->setResponse(r);
                    } else if (self->lastLogIdSent_ >= resp.get_last_log_id()) {
                        VLOG(2) << self->idStr_
                                << "We send nothing in the last request"
                                << ", so we don't send the same logs again";
                        self->followerCommittedLogId_ = resp.get_committed_log_id();
                        cpp2::AppendLogResponse r;
                        r.set_error_code(res);
                        self->setResponse(r);
                    } else {
                        self->lastLogIdSent_ = resp.get_last_log_id();
                        self->lastLogTermSent_ = resp.get_last_log_term();
                        self->followerCommittedLogId_ = resp.get_committed_log_id();
                        if (self->lastLogIdSent_ < self->logIdToSend_) {
                            // More to send
                            VLOG(2) << self->idStr_
                                    << "There are more logs to send";
                            newReq = self->prepareAppendLogRequest();
                        } else {
                            VLOG(2) << self->idStr_
                                    << "Fulfill the promise, size = " << self->promise_.size();
                            // Fulfill the promise
                            self->promise_.setValue(resp);

                            if (self->noRequest()) {
                                VLOG(2) << self->idStr_ << "No request any more!";
                                self->requestOnGoing_ = false;
                            } else {
                                auto& tup = self->pendingReq_;
                                self->logTermToSend_ = std::get<0>(tup);
                                self->logIdToSend_ = std::get<1>(tup);
                                self->committedLogId_ = std::get<2>(tup);
                                VLOG(2) << self->idStr_
                                        << "Sending the pending request in the queue"
                                        << ", from " << self->lastLogIdSent_ + 1
                                        << " to " << self->logIdToSend_;
                                newReq = self->prepareAppendLogRequest();
                                self->promise_ = std::move(self->cachingPromise_);
                                self->cachingPromise_
                                    = folly::SharedPromise<cpp2::AppendLogResponse>();
                                self->pendingReq_ = std::make_tuple(0, 0, 0);
                            }  // self->noRequest()
                        }  // self->lastLogIdSent_ < self->logIdToSend_
                    }  // else
                }
                if (newReq) {
                    self->appendLogsInternal(eb, newReq);
                } else {
                    self->noMoreRequestCV_.notify_all();
                }
                return;
            }
            case cpp2::ErrorCode::E_LOG_GAP: {
                VLOG(2) << self->idStr_
                        << "The host's log is behind, need to catch up";
                std::shared_ptr<cpp2::AppendLogRequest> newReq;
                {
                    std::lock_guard<std::mutex> g(self->lock_);
                    auto res = self->checkStatus();
                    if (res != cpp2::ErrorCode::SUCCEEDED) {
                        VLOG(2) << self->idStr_
                                << "The host is not in a proper status,"
                                   " skip catching up the gap";
                        cpp2::AppendLogResponse r;
                        r.set_error_code(res);
                        self->setResponse(r);
                    } else if (self->lastLogIdSent_ == resp.get_last_log_id()) {
                        VLOG(2) << self->idStr_
                                << "We send nothing in the last request"
                                << ", so we don't send the same logs again";
                        self->lastLogIdSent_ = resp.get_last_log_id();
                        self->lastLogTermSent_ = resp.get_last_log_term();
                        self->followerCommittedLogId_ = resp.get_committed_log_id();
                        cpp2::AppendLogResponse r;
                        r.set_error_code(cpp2::ErrorCode::SUCCEEDED);
                        self->setResponse(r);
                    } else {
                        self->lastLogIdSent_ = std::min(resp.get_last_log_id(),
                                                        self->logIdToSend_ - 1);
                        self->lastLogTermSent_ = resp.get_last_log_term();
                        self->followerCommittedLogId_ = resp.get_committed_log_id();
                        newReq = self->prepareAppendLogRequest();
                    }
                }
                if (newReq) {
                    self->appendLogsInternal(eb, newReq);
                } else {
                    self->noMoreRequestCV_.notify_all();
                }
                return;
            }
            case cpp2::ErrorCode::E_WAITING_SNAPSHOT: {
                LOG(INFO) << self->idStr_
                          << "The host is waiting for the snapshot, so we need to send log from "
                          << "current committedLogId " << self->committedLogId_;
                std::shared_ptr<cpp2::AppendLogRequest> newReq;
                {
                    std::lock_guard<std::mutex> g(self->lock_);
                    auto res = self->checkStatus();
                    if (res != cpp2::ErrorCode::SUCCEEDED) {
                        VLOG(2) << self->idStr_
                                << "The host is not in a proper status,"
                                   " skip waiting the snapshot";
                        cpp2::AppendLogResponse r;
                        r.set_error_code(res);
                        self->setResponse(r);
                    } else {
                        self->lastLogIdSent_ = self->committedLogId_;
                        self->lastLogTermSent_ = self->logTermToSend_;
                        self->followerCommittedLogId_ = resp.get_committed_log_id();
                        newReq = self->prepareAppendLogRequest();
                    }
                }
                if (newReq) {
                    self->appendLogsInternal(eb, newReq);
                } else {
                    self->noMoreRequestCV_.notify_all();
                }
                return;
            }
            case cpp2::ErrorCode::E_LOG_STALE: {
                VLOG(2) << self->idStr_ << "Log stale, reset lastLogIdSent " << self->lastLogIdSent_
                        << " to the followers lastLodId " << resp.get_last_log_id();
                std::shared_ptr<cpp2::AppendLogRequest> newReq;
                {
                    std::lock_guard<std::mutex> g(self->lock_);
                    auto res = self->checkStatus();
                    if (res != cpp2::ErrorCode::SUCCEEDED) {
                        VLOG(2) << self->idStr_
                                << "The host is not in a proper status,"
                                   " skip waiting the snapshot";
                        cpp2::AppendLogResponse r;
                        r.set_error_code(res);
                        self->setResponse(r);
                    } else if (self->logIdToSend_ <= resp.get_last_log_id()) {
                        VLOG(2) << self->idStr_
                                << "It means the request has been received by follower";
                        self->lastLogIdSent_ = self->logIdToSend_ - 1;
                        self->lastLogTermSent_ = resp.get_last_log_term();
                        self->followerCommittedLogId_ = resp.get_committed_log_id();
                        cpp2::AppendLogResponse r;
                        r.set_error_code(cpp2::ErrorCode::SUCCEEDED);
                        self->setResponse(r);
                    } else {
                        self->lastLogIdSent_ = std::min(resp.get_last_log_id(),
                                                        self->logIdToSend_ - 1);
                        self->lastLogTermSent_ = resp.get_last_log_term();
                        self->followerCommittedLogId_ = resp.get_committed_log_id();
                        newReq = self->prepareAppendLogRequest();
                    }
                }
                if (newReq) {
                    self->appendLogsInternal(eb, newReq);
                } else {
                    self->noMoreRequestCV_.notify_all();
                }
                return;
            }
            default: {
                LOG_EVERY_N(ERROR, 100)
                           << self->idStr_
                           << "Failed to append logs to the host (Err: "
                           << apache::thrift::util::enumNameSafe(resp.get_error_code())
                           << ")";
                {
                    std::lock_guard<std::mutex> g(self->lock_);
                    self->setResponse(resp);
                    self->lastLogIdSent_ = self->logIdToSend_ - 1;
                }
                self->noMoreRequestCV_.notify_all();
                return;
            }
        }
    });
}


std::shared_ptr<cpp2::AppendLogRequest>
Host::prepareAppendLogRequest() {
    CHECK(!lock_.try_lock());
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

    VLOG(2) << idStr_ << "Prepare AppendLogs request from Log "
                      << lastLogIdSent_ + 1 << " to " << logIdToSend_;
    if (lastLogIdSent_ + 1 > part_->wal()->lastLogId()) {
        LOG(INFO) << idStr_ << "My lastLogId in wal is " << part_->wal()->lastLogId()
                  << ", but you are seeking " << lastLogIdSent_ + 1
                  << ", so i have nothing to send.";
        return req;
    }
    auto it = part_->wal()->iterator(lastLogIdSent_ + 1, logIdToSend_);
    if (it->valid()) {
        VLOG(2) << idStr_ << "Prepare the list of log entries to send";

        auto term = it->logTerm();
        req->set_log_term(term);

        std::vector<cpp2::LogEntry> logs;
        for (size_t cnt = 0;
             it->valid()
                && it->logTerm() == term
                && cnt < FLAGS_max_appendlog_batch_size;
             ++(*it), ++cnt) {
            cpp2::LogEntry le;
            le.set_cluster(it->logSource());
            le.set_log_str(it->logMsg().toString());
            logs.emplace_back(std::move(le));
        }
        req->set_log_str_list(std::move(logs));
        req->set_sending_snapshot(false);
    } else {
        req->set_sending_snapshot(true);
        if (!sendingSnapshot_) {
            LOG(INFO) << idStr_ << "Can't find log " << lastLogIdSent_ + 1
                      << " in wal, send the snapshot"
                      << ", logIdToSend = " << logIdToSend_
                      << ", firstLogId in wal = " << part_->wal()->firstLogId()
                      << ", lastLogId in wal = " << part_->wal()->lastLogId();
            sendingSnapshot_ = true;
            part_->snapshot_->sendSnapshot(part_, addr_)
                .thenValue([self = shared_from_this()] (Status&& status) {
                if (status.ok()) {
                    LOG(INFO) << self->idStr_ << "Send snapshot succeeded!";
                } else {
                    LOG(INFO) << self->idStr_ << "Send snapshot failed!";
                    // TODO(heng): we should tell the follower i am failed.
                }
                self->sendingSnapshot_ = false;
            });
        } else {
            LOG_EVERY_N(INFO, 30) << idStr_
                                   << "The snapshot req is in queue, please wait for a moment";
        }
    }

    return req;
}


folly::Future<cpp2::AppendLogResponse> Host::sendAppendLogRequest(
        folly::EventBase* eb,
        std::shared_ptr<cpp2::AppendLogRequest> req) {
    VLOG(2) << idStr_ << "Entering Host::sendAppendLogRequest()";

    {
        std::lock_guard<std::mutex> g(lock_);
        auto res = checkStatus();
        if (res != cpp2::ErrorCode::SUCCEEDED) {
            LOG(WARNING) << idStr_
                         << "The Host is not in a proper status, do not send";
            cpp2::AppendLogResponse resp;
            resp.set_error_code(res);
            return resp;
        }
    }

    LOG_IF(INFO, FLAGS_trace_raft) << idStr_
        << "Sending appendLog: space " << req->get_space()
        << ", part " << req->get_part()
        << ", current term " << req->get_current_term()
        << ", last_log_id " << req->get_last_log_id()
        << ", committed_id " << req->get_committed_log_id()
        << ", last_log_term_sent" << req->get_last_log_term_sent()
        << ", last_log_id_sent " << req->get_last_log_id_sent();
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
        .then([self = shared_from_this(), pro = std::move(promise)]
              (folly::Try<cpp2::HeartbeatResponse>&& t) mutable {
            VLOG(3) << self->idStr_ << "heartbeat call got response";
            if (t.hasException()) {
                cpp2::HeartbeatResponse resp;
                resp.set_error_code(cpp2::ErrorCode::E_EXCEPTION);
                pro.setValue(std::move(resp));
                return;
            } else {
                pro.setValue(std::move(t.value()));
            }
        });
    return future;
}

folly::Future<cpp2::HeartbeatResponse> Host::sendHeartbeatRequest(
        folly::EventBase* eb,
        std::shared_ptr<cpp2::HeartbeatRequest> req) {
    VLOG(2) << idStr_ << "Entering Host::sendHeartbeatRequest()";

    {
        std::lock_guard<std::mutex> g(lock_);
        auto res = checkStatus();
        if (res != cpp2::ErrorCode::SUCCEEDED) {
            LOG(WARNING) << idStr_
                         << "The Host is not in a proper status, do not send";
            cpp2::HeartbeatResponse resp;
            resp.set_error_code(res);
            return resp;
        }
    }

    LOG_IF(INFO, FLAGS_trace_raft) << idStr_
        << "Sending heartbeat: space " << req->get_space()
        << ", part " << req->get_part()
        << ", current term " << req->get_current_term()
        << ", last_log_id " << req->get_last_log_id()
        << ", committed_id " << req->get_committed_log_id()
        << ", last_log_term_sent " << req->get_last_log_term_sent()
        << ", last_log_id_sent " << req->get_last_log_id_sent();
    // Get client connection
    auto client = part_->clientMan_->client(addr_, eb, false, FLAGS_raft_rpc_timeout_ms);
    return client->future_heartbeat(*req);
}

bool Host::noRequest() const {
    CHECK(!lock_.try_lock());
    static auto emptyTup = std::make_tuple(0, 0, 0);
    return pendingReq_ == emptyTup;
}

}  // namespace raftex
}  // namespace nebula

