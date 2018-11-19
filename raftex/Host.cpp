/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "raftex/Host.h"
#include "raftex/RaftPart.h"
#include "raftex/FileBasedWal.h"
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>
#include <folly/io/async/EventBase.h>
#include "interface/gen-cpp2/RaftexServiceAsyncClient.h"
#include "network/ThriftSocketManager.h"
#include "network/NetworkUtils.h"

DEFINE_uint32(max_appendlog_batch_size, 128,
              "The max number of logs in each appendLog request batch");


namespace vesoft {
namespace raftex {

using namespace vesoft::network;

Host::Host(const HostAddr& addr, std::shared_ptr<RaftPart> part)
        : part_(std::move(part))
        , addr_(addr)
        , idStr_(folly::stringPrintf(
            "[Host: %s:%d] ",
            NetworkUtils::intToIPv4(addr_.first).c_str(),
            addr_.second))
        , future_(folly::Future<cpp2::AppendLogResponse>::makeEmpty()) {
}


void Host::waitForStop() {
    std::unique_lock<std::mutex> g(lock_);

    CHECK(stopped_);
    noMoreRequestCV_.wait(g, [this] {
        return !requestOnGoing_;
    });
}


folly::Future<cpp2::AppendLogResponse> Host::appendLogs(
        folly::EventBase* eb,
        TermID term,
        LogID logId,
        LogID committedLogId,
        TermID prevLogTerm,
        LogID prevLogId) {
    VLOG(3) << idStr_ << "Entering Host::appendLogs()";

    std::shared_ptr<cpp2::AppendLogRequest> req;
    {
        std::lock_guard<std::mutex> g(lock_);

        if (stopped_) {
            VLOG(2) << idStr_ << "The host is stopped, just return";
            cpp2::AppendLogResponse r;
            r.set_error_code(cpp2::ErrorCode::E_HOST_STOPPED);
            return r;
        }

        if (logId <= logIdToSend_) {
            // This could be a re-send or a heartbeat. If there
            // is no ongoing request, we will send a heartbeat out
            if (requestOnGoing_) {
                cpp2::AppendLogResponse r;
                r.set_error_code(cpp2::ErrorCode::SUCCEEDED);
                return r;
            }
        }

        logTermToSend_ = term;
        logIdToSend_ = logId;
        prevLogTerm_ = prevLogTerm;
        prevLogId_ = prevLogId;
        committedLogId_ = committedLogId;

        if (requestOnGoing_) {
            VLOG(2) << idStr_ << "Another request is ongoing, just return";
            cpp2::AppendLogResponse r;
            r.set_error_code(cpp2::ErrorCode::E_REQUEST_ONGOING);
            return r;
        } else {
            requestOnGoing_ = true;
        }

        req = prepareAppendLogRequest();
    }

    // Get a new promise
    promise_ = folly::Promise<cpp2::AppendLogResponse>();
    future_ = appendLogsInternal(eb, std::move(req));

    return promise_.getFuture();
}


folly::Future<cpp2::AppendLogResponse> Host::appendLogsInternal(
        folly::EventBase* eb,
        std::shared_ptr<cpp2::AppendLogRequest> req) {
    auto numLogs = req->get_log_str_list() == nullptr
                    ? 0
                    : req->get_log_str_list()->size();
    auto firstId = req->get_first_log_id();
    return folly::via(eb, [self = shared_from_this(), req] () {
        return self->sendAppendLogRequest(std::move(req));
    }).then([eb, self = shared_from_this(), numLogs, firstId] (
            folly::Try<cpp2::AppendLogResponse>&& t) {
        VLOG(2) << self->idStr_ << "appendLogs() call got response";
        if (t.hasException()) {
            LOG(ERROR) << self->idStr_ << t.exception().what();
            cpp2::AppendLogResponse r;
            r.set_error_code(cpp2::ErrorCode::E_EXCEPTION);
            self->promise_.setValue(r);
            {
                std::lock_guard<std::mutex> g(self->lock_);
                self->requestOnGoing_ = false;
            }
            self->noMoreRequestCV_.notify_all();
            return r;
        }

        folly::Future<cpp2::AppendLogResponse> newFuture =
            folly::Future<cpp2::AppendLogResponse>::makeEmpty();
        cpp2::AppendLogResponse resp = std::move(t).value();

        switch(resp.get_error_code()) {
            case cpp2::ErrorCode::SUCCEEDED: {
                std::shared_ptr<cpp2::AppendLogRequest> newReq;

                {
                    std::lock_guard<std::mutex> g(self->lock_);
                    if (numLogs > 0) {
                        self->lastLogIdSent_ = firstId + numLogs - 1;
                    }
                    if (self->lastLogIdSent_ < self->logIdToSend_) {
                        // More to send
                        VLOG(2) << self->idStr_
                                << "There are more logs to send";
                        newReq = self->prepareAppendLogRequest();
                    } else {
                        self->promise_.setValue(resp);
                        self->requestOnGoing_ = false;
                    }
                }

                if (newReq) {
                    newFuture = self->appendLogsInternal(eb, newReq);
                    std::swap(self->future_, newFuture);
                } else {
                    self->noMoreRequestCV_.notify_all();
                }
                return resp;
            }
            case cpp2::ErrorCode::E_LOG_GAP: {
                VLOG(2) << self->idStr_
                        << "The host's log is behind, need to catch up";
                std::shared_ptr<cpp2::AppendLogRequest> newReq;
                if (numLogs > 0) {
                    std::lock_guard<std::mutex> g(self->lock_);
                    self->lastLogIdSent_ = resp.get_last_log_id();
                    newReq = self->prepareAppendLogRequest();
                } else {
                    std::lock_guard<std::mutex> g(self->lock_);
                    newReq = self->prepareAppendLogRequest();
                }
                newFuture = self->appendLogsInternal(eb, newReq);
                std::swap(self->future_, newFuture);
                return resp;
            }
            default: {
                LOG(ERROR) << self->idStr_
                           << "Failed to append logs to the host (Err: "
                           << static_cast<int32_t>(resp.get_error_code())
                           << ")";
                {
                    std::lock_guard<std::mutex> g(self->lock_);
                    self->promise_.setValue(resp);
                    self->requestOnGoing_ = false;
                }
                self->noMoreRequestCV_.notify_all();
                return resp;
            }
        }
    });
}


/*
 * Hass to run under protection (owns the lock)
 */
std::shared_ptr<cpp2::AppendLogRequest>
Host::prepareAppendLogRequest() const {
    auto req = std::make_shared<cpp2::AppendLogRequest>();
    req->set_space(part_->spaceId());
    req->set_part(part_->partitionId());
    req->set_term(logTermToSend_);
    req->set_leader_ip(part_->address().first);
    req->set_leader_port(part_->address().second);
    req->set_committed_log_id(committedLogId_);
    req->set_previous_log_term(prevLogTerm_);
    req->set_previous_log_id(prevLogId_);

    LogID firstIdToSend = 0;
    if (lastLogIdSent_ > 0) {
        firstIdToSend = lastLogIdSent_ + 1;
    } else {
        // First time ever to send a log to the host
        firstIdToSend = logIdToSend_;
    }

    VLOG(2) << idStr_ << "Prepare AppendLogs request from Log "
            << firstIdToSend << " to " << logIdToSend_;
    auto it = part_->wal()->iterator(firstIdToSend, logIdToSend_);
    if (it->valid()) {
        VLOG(2) << idStr_ << "Prepare the list of log entries to send";

        auto term = it->logTerm();
        req->set_log_term(term);
        req->set_first_log_id(firstIdToSend);

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
    }

    return std::move(req);
}


folly::Future<cpp2::AppendLogResponse> Host::sendAppendLogRequest(
        std::shared_ptr<cpp2::AppendLogRequest> req) {
    VLOG(3) << idStr_ << "Entering Host::sendAppendLogRequest()";

    {
        std::lock_guard<std::mutex> g(lock_);
        if (stopped_) {
            VLOG(2) << idStr_ << "The Host is stopped";
            cpp2::AppendLogResponse resp;
            resp.set_error_code(cpp2::ErrorCode::E_HOST_STOPPED);
            return resp;
        }
    }

    // Get client connection
    auto socket = ThriftSocketManager::getSocket(addr_);
    if (!socket) {
        // Bad connection
        LOG(ERROR) << idStr_ << "Not connected";
        cpp2::AppendLogResponse resp;
        resp.set_error_code(cpp2::ErrorCode::E_HOST_DISCONNECTED);
        return resp;
    }

    auto client = std::make_unique<cpp2::RaftexServiceAsyncClient>(
        apache::thrift::HeaderClientChannel::newChannel(socket));
    return client->future_appendLog(*req);
}

}  // namespace raftex
}  // namespace vesoft

