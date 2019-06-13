/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "kvstore/raftex/Host.h"
#include "kvstore/raftex/RaftPart.h"
#include "kvstore/wal/FileBasedWal.h"
#include <folly/io/async/EventBase.h>
#include "network/NetworkUtils.h"

DEFINE_uint32(max_appendlog_batch_size, 128,
              "The max number of logs in each appendLog request batch");
DEFINE_uint32(max_outstanding_requests, 1024,
              "The max number of outstanding appendLog requests");


namespace nebula {
namespace raftex {

using nebula::network::NetworkUtils;

Host::Host(const HostAddr& addr, std::shared_ptr<RaftPart> part)
        : part_(std::move(part))
        , addr_(addr)
        , idStr_(folly::stringPrintf(
            "[Host: %s:%d] ",
            NetworkUtils::intToIPv4(addr_.first).c_str(),
            addr_.second)) {
}


void Host::waitForStop() {
    std::unique_lock<std::mutex> g(lock_);

    CHECK(stopped_);
    noMoreRequestCV_.wait(g, [this] {
        return !requestOnGoing_;
    });
}


cpp2::ErrorCode Host::checkStatus(std::lock_guard<std::mutex>& lck) const {
    UNUSED(lck);
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
        const cpp2::AskForVoteRequest& req) {
    auto client = tcManager().client(addr_);
    return client->future_askForVote(req);
}


folly::Future<cpp2::AppendLogResponse> Host::appendLogs(
        folly::EventBase* eb,
        TermID term,
        LogID logId,
        LogID committedLogId,
        TermID lastLogTermSent,
        LogID lastLogIdSent) {
    VLOG(3) << idStr_ << "Entering Host::appendLogs()";

    VLOG(2) << idStr_
            << "Append logs to the host [term = " << term
            << ", logId = " << logId
            << ", committedLogId = " << committedLogId
            << ", lastLogTermSent = " << lastLogTermSent
            << ", lastLogIdSent = " << lastLogIdSent
            << "]";

    auto ret = folly::Future<cpp2::AppendLogResponse>::makeEmpty();
    std::shared_ptr<cpp2::AppendLogRequest> req;
    {
        std::lock_guard<std::mutex> g(lock_);

        auto res = checkStatus(g);

        if (logId == 0 || logId == logIdToSend_) {
            // This is a re-send or a heartbeat. If there is an
            // ongoing request, we will just return SUCCEEDED
            if (requestOnGoing_) {
                VLOG(2) << idStr_ << "Another request is onging,"
                                     "ignore the re-send/heartbeat request";
                cpp2::AppendLogResponse r;
                r.set_error_code(cpp2::ErrorCode::SUCCEEDED);
                return r;
            }
        } else {
            // Otherwise, logId has to be greater
            CHECK_GT(logId, logIdToSend_);
        }

        if (requestOnGoing_ && res == cpp2::ErrorCode::SUCCEEDED) {
            // Another request is ongoing
            if (requests_.size() <= FLAGS_max_outstanding_requests) {
                VLOG(2) << idStr_
                        << "Another request is ongoing, wait in queue";
                requests_.push(std::make_pair(
                    folly::Promise<cpp2::AppendLogResponse>(),
                    std::make_tuple(term,
                                    logId,
                                    committedLogId,
                                    lastLogTermSent,
                                    lastLogIdSent)));
                return requests_.back().first.getFuture();
            } else {
                VLOG(2) << idStr_
                        << "Too many requests are waiting, return error";
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
        CHECK_GE(lastLogTermSent, lastLogTermSent_);
        CHECK_GE(lastLogIdSent, lastLogIdSent_);
        logTermToSend_ = term;
        logIdToSend_ = logId;
        lastLogTermSent_ = lastLogTermSent;
        lastLogIdSent_ = lastLogIdSent;
        committedLogId_ = committedLogId;

        promise_ = folly::Promise<cpp2::AppendLogResponse>();
        ret = promise_.getFuture();

        requestOnGoing_ = true;

        req = prepareAppendLogRequest(g);
    }

    // Get a new promise
    appendLogsInternal(eb, std::move(req));

    return ret;
}


folly::Future<cpp2::AppendLogResponse> Host::appendLogsInternal(
        folly::EventBase* eb,
        std::shared_ptr<cpp2::AppendLogRequest> req) {
    auto numLogs = req->get_log_str_list().size();
    auto firstId = req->get_last_log_id_sent() + 1;
    auto termSent = req->get_log_term();

    return folly::via(eb, [self = shared_from_this(), req] () {
        return self->sendAppendLogRequest(std::move(req));
    })
    .then([eb, self = shared_from_this(), numLogs, termSent, firstId] (
            folly::Try<cpp2::AppendLogResponse>&& t) {
        UNUSED(numLogs);
        UNUSED(termSent);
        UNUSED(firstId);
        VLOG(2) << self->idStr_ << "appendLogs() call got response";

        if (t.hasException()) {
            LOG(ERROR) << self->idStr_ << t.exception().what();
            cpp2::AppendLogResponse r;
            r.set_error_code(cpp2::ErrorCode::E_EXCEPTION);
            {
                std::lock_guard<std::mutex> g(self->lock_);
                self->promise_.setValue(r);
                self->requestOnGoing_ = false;
                // TODO We need to clear the requests_
            }
            self->noMoreRequestCV_.notify_all();
            return r;
        }

        cpp2::AppendLogResponse resp = std::move(t).value();
        switch (resp.get_error_code()) {
            case cpp2::ErrorCode::SUCCEEDED: {
                VLOG(2) << self->idStr_
                        << "AppendLog request sent successfully";

                std::shared_ptr<cpp2::AppendLogRequest> newReq;
                cpp2::AppendLogResponse r;
                {
                    std::lock_guard<std::mutex> g(self->lock_);

                    auto res = self->checkStatus(g);
                    if (res != cpp2::ErrorCode::SUCCEEDED) {
                        VLOG(2) << self->idStr_
                                << "The host is not in a proper status,"
                                   " just return";
                        r.set_error_code(res);
                        self->promise_.setValue(r);
                        self->requestOnGoing_ = false;

                        // Remove all requests in the queue
                        while (!self->requests_.empty()) {
                            self->requests_.front().first.setValue(r);
                            self->requests_.pop();
                        }
                    } else {
                        self->lastLogIdSent_ = resp.get_last_log_id();
                        self->lastLogTermSent_ = resp.get_last_log_term();
                        if (self->lastLogIdSent_ < self->logIdToSend_) {
                            // More to send
                            VLOG(2) << self->idStr_
                                    << "There are more logs to send";
                            newReq = self->prepareAppendLogRequest(g);
                        } else {
                            // Fulfill the promise
                            self->promise_.setValue(resp);

                            if (self->requests_.empty()) {
                                self->requestOnGoing_ = false;
                            } else {
                                VLOG(2) << self->idStr_
                                        << "Sending next request in the queue";
                                auto& tup = self->requests_.front().second;
                                self->logTermToSend_ = std::get<0>(tup);
                                self->logIdToSend_ = std::get<1>(tup);
                                self->committedLogId_ = std::get<2>(tup);
                                newReq = self->prepareAppendLogRequest(g);

                                self->promise_ =
                                    std::move(self->requests_.front().first);

                                // Remove the first request from the queue
                                self->requests_.pop();
                            }
                        }

                        r = std::move(resp);
                    }
                }

                if (newReq) {
                    self->appendLogsInternal(eb, newReq);
                } else {
                    self->noMoreRequestCV_.notify_all();
                }
                return r;
            }
            case cpp2::ErrorCode::E_LOG_GAP: {
                VLOG(2) << self->idStr_
                        << "The host's log is behind, need to catch up";
                std::shared_ptr<cpp2::AppendLogRequest> newReq;
                cpp2::AppendLogResponse r;
                {
                    std::lock_guard<std::mutex> g(self->lock_);
                    auto res = self->checkStatus(g);
                    if (res != cpp2::ErrorCode::SUCCEEDED) {
                        VLOG(2) << self->idStr_
                                << "The host is not in a proper status,"
                                   " skip catching up the gap";
                        r.set_error_code(res);
                        self->promise_.setValue(r);
                        self->requestOnGoing_ = false;

                        // Remove all requests in the queue
                        while (!self->requests_.empty()) {
                            self->requests_.front().first.setValue(r);
                            self->requests_.pop();
                        }
                    } else {
                        self->lastLogIdSent_ = resp.get_last_log_id();
                        self->lastLogTermSent_ = resp.get_last_log_term();
                        newReq = self->prepareAppendLogRequest(g);
                        r = std::move(resp);
                    }
                }
                if (newReq) {
                    self->appendLogsInternal(eb, newReq);
                } else {
                    self->noMoreRequestCV_.notify_all();
                }
                return r;
            }
            default: {
                LOG(ERROR) << self->idStr_
                           << "Failed to append logs to the host (Err: "
                           << static_cast<int32_t>(resp.get_error_code())
                           << ")";
                {
                    std::lock_guard<std::mutex> g(self->lock_);
                    self->promise_.setValue(resp);

                    // Remove all requests in the queue
                    while (!self->requests_.empty()) {
                        self->requests_.front().first.setValue(resp);
                        self->requests_.pop();
                    }

                    self->requestOnGoing_ = false;
                }
                self->noMoreRequestCV_.notify_all();
                return resp;
            }
        }
    });
}


std::shared_ptr<cpp2::AppendLogRequest>
Host::prepareAppendLogRequest(std::lock_guard<std::mutex>& lck) const {
    UNUSED(lck);
    auto req = std::make_shared<cpp2::AppendLogRequest>();
    req->set_space(part_->spaceId());
    req->set_part(part_->partitionId());
    req->set_current_term(logTermToSend_);
    req->set_last_log_id(logIdToSend_);
    req->set_leader_ip(part_->address().first);
    req->set_leader_port(part_->address().second);
    req->set_committed_log_id(committedLogId_);
    req->set_last_log_term_sent(lastLogTermSent_);
    req->set_last_log_id_sent(lastLogIdSent_);

    VLOG(2) << idStr_ << "Prepare AppendLogs request from Log "
            << lastLogIdSent_ + 1 << " to " << logIdToSend_;
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
    } else {
        req->set_log_term(0);
    }

    return req;
}


folly::Future<cpp2::AppendLogResponse> Host::sendAppendLogRequest(
        std::shared_ptr<cpp2::AppendLogRequest> req) {
    VLOG(3) << idStr_ << "Entering Host::sendAppendLogRequest()";

    {
        std::lock_guard<std::mutex> g(lock_);
        auto res = checkStatus(g);
        if (res != cpp2::ErrorCode::SUCCEEDED) {
            VLOG(2) << idStr_
                    << "The Host is not in a proper status, do not send";
            cpp2::AppendLogResponse resp;
            resp.set_error_code(res);
            return resp;
        }
    }

    // Get client connection
    auto client = tcManager().client(addr_);
    return client->future_appendLog(*req);
}

}  // namespace raftex
}  // namespace nebula

