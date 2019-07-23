/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "kvstore/raftex/RaftPart.h"
#include <folly/io/async/EventBaseManager.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/gen/Base.h>
#include "gen-cpp2/RaftexServiceAsyncClient.h"
#include "base/CollectNSucceeded.h"
#include "thrift/ThriftClientManager.h"
#include "network/NetworkUtils.h"
#include "thread/NamedThread.h"
#include "kvstore/wal/FileBasedWal.h"
#include "kvstore/wal/BufferFlusher.h"
#include "kvstore/raftex/LogStrListIterator.h"
#include "kvstore/raftex/Host.h"


DEFINE_bool(accept_log_append_during_pulling, false,
            "Whether to accept new logs during pulling the snapshot");
DEFINE_uint32(heartbeat_interval, 5,
             "Seconds between each heartbeat");
DEFINE_uint32(max_batch_size, 256, "The max number of logs in a batch");


namespace nebula {
namespace raftex {

using nebula::network::NetworkUtils;
using nebula::thrift::ThriftClientManager;
using nebula::wal::FileBasedWal;
using nebula::wal::FileBasedWalPolicy;
using nebula::wal::BufferFlusher;

class AppendLogsIterator final : public LogIterator {
public:
    AppendLogsIterator(LogID firstLogId,
                       TermID termId,
                       RaftPart::LogCache logs,
                       std::function<std::string(const std::string&)> casCB)
            : firstLogId_(firstLogId)
            , termId_(termId)
            , logId_(firstLogId)
            , logs_(std::move(logs))
            , casCB_(std::move(casCB)) {
        leadByCAS_ = processCAS();
        valid_ = idx_ < logs_.size();
        hasNonCASLogs_ = !leadByCAS_ && valid_;
        if (valid_) {
            currLogType_ = lastLogType_ = logType();
        }
    }

    AppendLogsIterator(const AppendLogsIterator&) = delete;
    AppendLogsIterator(AppendLogsIterator&&) = default;

    AppendLogsIterator& operator=(const AppendLogsIterator&) = delete;
    AppendLogsIterator& operator=(AppendLogsIterator&&) = default;

    bool leadByCAS() const {
        return leadByCAS_;
    }

    bool hasNonCASLogs() const {
        return hasNonCASLogs_;
    }

    LogID firstLogId() const {
        return firstLogId_;
    }

    // Return true if the current log is a CAS, otherwise return false
    bool processCAS() {
        while (idx_ < logs_.size()) {
            auto& tup = logs_.at(idx_);
            auto logType = std::get<1>(tup);
            if (logType != LogType::CAS) {
                // Not a CAS
                return false;
            }

            // Process CAS log
            CHECK(!!casCB_);
            casResult_ = casCB_(std::get<2>(tup));
            if (casResult_.size() > 0) {
                // CAS Succeeded
                return true;
            } else {
                // CAS failed, move to the next log, but do not increment the logId_
                ++idx_;
            }
        }

        // Reached the end
        return false;
    }

    LogIterator& operator++() override {
        ++idx_;
        ++logId_;
        if (idx_ < logs_.size()) {
            currLogType_ = logType();
            valid_ = currLogType_ != LogType::CAS;
            if (valid_) {
                hasNonCASLogs_ = true;
            }
            valid_ = valid_ && lastLogType_ != LogType::COMMAND;
            lastLogType_ = currLogType_;
        } else {
            valid_ = false;
        }
        return *this;
    }

    // The iterator becomes invalid when exhausting the logs
    // **OR** running into a CAS log
    bool valid() const override {
        return valid_;
    }

    LogID logId() const override {
        DCHECK(valid());
        return logId_;
    }

    TermID logTerm() const override {
        return termId_;
    }

    ClusterID logSource() const override {
        DCHECK(valid());
        return std::get<0>(logs_.at(idx_));
    }

    folly::StringPiece logMsg() const override {
        DCHECK(valid());
        if (currLogType_ == LogType::CAS) {
            return casResult_;
        } else {
            return std::get<2>(logs_.at(idx_));
        }
    }

    // Return true when there is no more log left for processing
    bool empty() const {
        return idx_ >= logs_.size();
    }

    // Resume the iterator so that we can continue to process the remaining logs
    void resume() {
        CHECK(!valid_);
        if (!empty()) {
            leadByCAS_ = processCAS();
            valid_ = idx_ < logs_.size();
            hasNonCASLogs_ = !leadByCAS_ && valid_;
            if (valid_) {
                currLogType_ = lastLogType_ = logType();
            }
        }
    }

    LogType logType() const {
        return  std::get<1>(logs_.at(idx_));
    }

private:
    size_t idx_{0};
    bool leadByCAS_{false};
    bool hasNonCASLogs_{false};
    bool valid_{true};
    LogType lastLogType_{LogType::NORMAL};
    LogType currLogType_{LogType::NORMAL};
    std::string casResult_;
    LogID firstLogId_;
    TermID termId_;
    LogID logId_;
    RaftPart::LogCache logs_;
    std::function<std::string(const std::string&)> casCB_;
};


/********************************************************
 *
 *  Implementation of RaftPart
 *
 *******************************************************/
RaftPart::RaftPart(ClusterID clusterId,
                   GraphSpaceID spaceId,
                   PartitionID partId,
                   HostAddr localAddr,
                   const folly::StringPiece walRoot,
                   BufferFlusher* flusher,
                   std::shared_ptr<folly::IOThreadPoolExecutor> pool,
                   std::shared_ptr<thread::GenericThreadPool> workers)
        : idStr_{folly::stringPrintf("[Port: %d, Space: %d, Part: %d] ",
                                     localAddr.second, spaceId, partId)}
        , clusterId_{clusterId}
        , spaceId_{spaceId}
        , partId_{partId}
        , addr_{localAddr}
        , status_{Status::STARTING}
        , role_{Role::FOLLOWER}
        , leader_{0, 0}
        , ioThreadPool_{pool}
        , workers_{workers} {
    // TODO Configure the wal policy
    wal_ = FileBasedWal::getWal(walRoot,
                                FileBasedWalPolicy(),
                                flusher,
                                [this] (LogID logId,
                                        TermID logTermId,
                                        ClusterID logClusterId,
                                        const std::string& log) {
                                    return this->preProcessLog(logId,
                                                               logTermId,
                                                               logClusterId,
                                                               log);
                                });
    lastLogId_ = wal_->lastLogId();
    term_ = proposedTerm_ = lastLogTerm_ = wal_->lastLogTerm();
}


RaftPart::~RaftPart() {
    std::lock_guard<std::mutex> g(raftLock_);

    // Make sure the partition has stopped
    CHECK(status_ == Status::STOPPED);
    LOG(INFO) << idStr_ << " The part has been destroyed...";
}


const char* RaftPart::roleStr(Role role) const {
    switch (role) {
    case Role::LEADER:
        return "Leader";
    case Role::FOLLOWER:
        return "Follower";
    case Role::CANDIDATE:
        return "Candidate";
    case Role::LEARNER:
        return "Learner";
    default:
        LOG(FATAL) << idStr_ << "Invalid role";
    }
    return nullptr;
}


void RaftPart::start(std::vector<HostAddr>&& peers, bool asLearner) {
    std::lock_guard<std::mutex> g(raftLock_);

    // Set the quorum number
    quorum_ = (peers.size() + 1) / 2;
    LOG(INFO) << idStr_ << "There are "
                        << peers.size()
                        << " peer hosts, and total "
                        << peers.size() + 1
                        << " copies. The quorum is " << quorum_ + 1
                        << ", as learner " << asLearner;

    committedLogId_ = lastCommittedLogId();

    // Start all peer hosts
    for (auto& addr : peers) {
        auto hostPtr = std::make_shared<Host>(addr, shared_from_this());
        hosts_.emplace_back(hostPtr);
    }

    // Change the status
    status_ = Status::RUNNING;
    if (asLearner) {
        role_ = Role::LEARNER;
    }
    // Set up a leader election task
    size_t delayMS = 100 + folly::Random::rand32(900);
    workers_->addDelayTask(delayMS, [self = shared_from_this()] {
        self->statusPolling();
    });
}


void RaftPart::stop() {
    VLOG(2) << idStr_ << "Stopping the partition";

    decltype(hosts_) hosts;
    {
        std::unique_lock<std::mutex> lck(raftLock_);
        status_ = Status::STOPPED;
        leader_ = {0, 0};
        role_ = Role::FOLLOWER;

        hosts = std::move(hosts_);
    }

    for (auto& h : hosts) {
        h->stop();
    }

    VLOG(2) << idStr_ << "Invoked stop() on all peer hosts";

    for (auto& h : hosts) {
        VLOG(2) << idStr_ << "Waiting " << h->idStr() << " to stop";
        h->waitForStop();
        VLOG(2) << idStr_ << h->idStr() << "has stopped";
    }
    hosts.clear();
    LOG(INFO) << idStr_ << "Partition has been stopped";
}


AppendLogResult RaftPart::canAppendLogs() {
    CHECK(!raftLock_.try_lock());
    if (status_ == Status::STARTING) {
        LOG(ERROR) << idStr_ << "The partition is still starting";
        return AppendLogResult::E_NOT_READY;
    }
    if (status_ == Status::STOPPED) {
        LOG(ERROR) << idStr_ << "The partition is stopped";
        return AppendLogResult::E_STOPPED;
    }
    if (role_ != Role::LEADER) {
        LOG(ERROR) << idStr_ << "The partition is not a leader";
        return AppendLogResult::E_NOT_A_LEADER;
    }

    return AppendLogResult::SUCCEEDED;
}

void RaftPart::addLearner(const HostAddr& addr) {
    CHECK(!raftLock_.try_lock());
    if (addr == addr_) {
        LOG(INFO) << idStr_ << "I am learner!";
        return;
    }
    auto it = std::find_if(hosts_.begin(), hosts_.end(), [&addr] (const auto& h) {
                return h->address() == addr;
            });
    if (it == hosts_.end()) {
        hosts_.emplace_back(std::make_shared<Host>(addr, shared_from_this(), true));
        LOG(INFO) << idStr_ << "Add learner " << addr;
    } else {
        LOG(INFO) << idStr_ << "The host " << addr << " has been existed as "
                  << ((*it)->isLearner() ? " learner " : " group member");
    }
}

folly::Future<AppendLogResult> RaftPart::appendAsync(ClusterID source,
                                                     std::string log) {
    if (source < 0) {
        source = clusterId_;
    }
    return appendLogAsync(source, LogType::NORMAL, std::move(log));
}


folly::Future<AppendLogResult> RaftPart::casAsync(std::string log) {
    return appendLogAsync(clusterId_, LogType::CAS, std::move(log));
}

folly::Future<AppendLogResult> RaftPart::sendCommandAsync(std::string log) {
    return appendLogAsync(clusterId_, LogType::COMMAND, std::move(log));
}

folly::Future<AppendLogResult> RaftPart::appendLogAsync(ClusterID source,
                                                        LogType logType,
                                                        std::string log) {
    LogCache swappedOutLogs;
    auto retFuture = folly::Future<AppendLogResult>::makeEmpty();

    {
        std::lock_guard<std::mutex> lck(logsLock_);

        VLOG(2) << idStr_ << "Checking whether buffer overflow";

        if (logs_.size() >= FLAGS_max_batch_size) {
            // Buffer is full
            LOG(WARNING) << idStr_
                         << "The appendLog buffer is full."
                            " Please slow down the log appending rate";
            return AppendLogResult::E_BUFFER_OVERFLOW;
        }

        VLOG(2) << idStr_ << "Appending logs to the buffer";

        // Append new logs to the buffer
        DCHECK_GE(source, 0);
        logs_.emplace_back(source, logType, std::move(log));
        switch (logType) {
            case LogType::CAS:
                retFuture = cachingPromise_.getSingleFuture();
                break;
            case LogType::COMMAND:
                retFuture = cachingPromise_.getAndRollSharedFuture();
                break;
            case LogType::NORMAL:
                retFuture = cachingPromise_.getSharedFuture();
                break;
        }

        bool expected = false;
        if (replicatingLogs_.compare_exchange_strong(expected, true)) {
            // We need to send logs to all followers
            VLOG(2) << idStr_ << "Preparing to send AppendLog request";
            sendingPromise_ = std::move(cachingPromise_);
            cachingPromise_.reset();
            std::swap(swappedOutLogs, logs_);
        } else {
            VLOG(2) << idStr_
                    << "Another AppendLogs request is ongoing,"
                       " just return";
            return retFuture;
        }
    }

    LogID firstId = 0;
    TermID termId = 0;
    {
        std::lock_guard<std::mutex> g(raftLock_);
        auto res = canAppendLogs();
        if (res != AppendLogResult::SUCCEEDED) {
            LOG(ERROR) << idStr_
                       << "Cannot append logs, clean the buffer";
            sendingPromise_.setValue(std::move(res));
            replicatingLogs_ = false;
            return res;
        }
        firstId = lastLogId_ + 1;
        termId = term_;
    }
    // Replicate buffered logs to all followers
    // Replication will happen on a separate thread and will block
    // until majority accept the logs, the leadership changes, or
    // the partition stops
    VLOG(2) << idStr_ << "Calling appendLogsInternal()";
    AppendLogsIterator it(
        firstId,
        termId,
        std::move(swappedOutLogs),
        [this] (const std::string& msg) -> std::string {
            auto res = compareAndSet(msg);
            if (res.empty()) {
                // Failed
                sendingPromise_.setOneSingleValue(AppendLogResult::E_CAS_FAILURE);
            }
            return res;
        });
    appendLogsInternal(std::move(it), termId);

    return retFuture;
}


void RaftPart::appendLogsInternal(AppendLogsIterator iter, TermID termId) {
    TermID currTerm = 0;
    LogID prevLogId = 0;
    TermID prevLogTerm = 0;
    LogID committed = 0;
    LogID lastId = 0;
    if (iter.valid()) {
        VLOG(2) << idStr_ << "Ready to append logs from id "
                << iter.logId() << " (Current term is "
                << currTerm << ")";
    } else {
        VLOG(2) << idStr_ << "Ready to send a heartbeat";
    }

    {
        std::lock_guard<std::mutex> g(raftLock_);
        if (status_ != Status::RUNNING) {
            // The partition is not running
            VLOG(2) << idStr_ << "The partition is stopped";
            sendingPromise_.setValue(AppendLogResult::E_STOPPED);
            replicatingLogs_ = false;
            return;
        }

        if (role_ != Role::LEADER) {
            // Is not a leader any more
            VLOG(2) << idStr_ << "The leader has changed";
            sendingPromise_.setValue(AppendLogResult::E_NOT_A_LEADER);
            replicatingLogs_ = false;
            return;
        }
        if (term_ != termId) {
            VLOG(2) << idStr_ << "Term has been updated, origin " << termId << ", new " << term_;
            sendingPromise_.setValue(AppendLogResult::E_TERM_OUT_OF_DATE);
            replicatingLogs_ = false;
            return;
        }
        currTerm = term_;
        prevLogId = lastLogId_;
        prevLogTerm = lastLogTerm_;
        committed = committedLogId_;
        // Step 1: Write WAL
        if (!wal_->appendLogs(iter)) {
            LOG(ERROR) << idStr_ << "Failed to write into WAL";
            sendingPromise_.setValue(AppendLogResult::E_WAL_FAILURE);
            replicatingLogs_ = false;
            return;
        }
        lastId = wal_->lastLogId();
        VLOG(2) << idStr_ << "Succeeded writing logs ["
                << iter.firstLogId() << ", " << lastId << "] to WAL";
    }

    // Step 2: Replicate to followers
    auto eb = ioThreadPool_->getEventBase();
    replicateLogs(eb,
                  std::move(iter),
                  currTerm,
                  lastId,
                  committed,
                  prevLogTerm,
                  prevLogId);
    return;
}


void RaftPart::replicateLogs(folly::EventBase* eb,
                             AppendLogsIterator iter,
                             TermID currTerm,
                             LogID lastLogId,
                             LogID committedId,
                             TermID prevLogTerm,
                             LogID prevLogId) {
    using namespace folly;  // NOLINT since the fancy overload of | operator

    decltype(hosts_) hosts;
    {
        std::lock_guard<std::mutex> g(raftLock_);

        if (status_ != Status::RUNNING) {
            // The partition is not running
            VLOG(2) << idStr_ << "The partition is stopped";
            sendingPromise_.setValue(AppendLogResult::E_STOPPED);
            replicatingLogs_ = false;
            return;
        }

        if (role_ != Role::LEADER) {
            // Is not a leader any more
            VLOG(2) << idStr_ << "The leader has changed";
            sendingPromise_.setValue(AppendLogResult::E_NOT_A_LEADER);
            replicatingLogs_ = false;
            return;
        }

        hosts = hosts_;
    }

    VLOG(2) << idStr_ << "About to replicate logs to all peer hosts";

    collectNSucceeded(
        gen::from(hosts)
        | gen::map([self = shared_from_this(),
                    eb,
                    currTerm,
                    lastLogId,
                    prevLogId,
                    prevLogTerm,
                    committedId] (std::shared_ptr<Host> hostPtr) {
            VLOG(2) << self->idStr_
                    << "Appending logs to "
                    << hostPtr->idStr();
            return via(
                eb,
                [=] () -> Future<cpp2::AppendLogResponse> {
                    return hostPtr->appendLogs(eb,
                                               currTerm,
                                               lastLogId,
                                               committedId,
                                               prevLogTerm,
                                               prevLogId);
                });
        })
        | gen::as<std::vector>(),
        // Number of succeeded required
        quorum_,
        // Result evaluator
        [hosts] (size_t index, cpp2::AppendLogResponse& resp) {
            return resp.get_error_code() == cpp2::ErrorCode::SUCCEEDED
                    && !hosts[index]->isLearner();
        })
        .then(eb, [self = shared_from_this(),
                   eb,
                   it = std::move(iter),
                   currTerm,
                   lastLogId,
                   committedId,
                   prevLogId,
                   prevLogTerm,
                   pHosts = std::move(hosts)] (folly::Try<AppendLogResponses>&& result) mutable {
            VLOG(2) << self->idStr_ << "Received enough response";
            // TODO: since we kill some peers in ut, this may fail, handle exception if necessary
            // CHECK(!result.hasException());

            self->processAppendLogResponses(*result,
                                            eb,
                                            std::move(it),
                                            currTerm,
                                            lastLogId,
                                            committedId,
                                            prevLogTerm,
                                            prevLogId,
                                            std::move(pHosts));

            return *result;
        });
}


void RaftPart::processAppendLogResponses(
        const AppendLogResponses& resps,
        folly::EventBase* eb,
        AppendLogsIterator iter,
        TermID currTerm,
        LogID lastLogId,
        LogID committedId,
        TermID prevLogTerm,
        LogID prevLogId,
        std::vector<std::shared_ptr<Host>> hosts) {
    // Make sure majority have succeeded
    size_t numSucceeded = 0;
    for (auto& res : resps) {
        if (!hosts[res.first]->isLearner()
                && res.second.get_error_code() == cpp2::ErrorCode::SUCCEEDED) {
            ++numSucceeded;
        }
    }

    if (numSucceeded >= quorum_) {
        // Majority have succeeded
        VLOG(2) << idStr_ << numSucceeded
                << " hosts have accepted the logs";

        LogID firstLogId = 0;
        {
            std::lock_guard<std::mutex> g(raftLock_);
            if (status_ != Status::RUNNING) {
                // The partition is not running
                VLOG(2) << idStr_ << "The partition is stopped";
                sendingPromise_.setValue(AppendLogResult::E_STOPPED);
                replicatingLogs_ = false;
                return;
            }
            if (role_ != Role::LEADER) {
                // Is not a leader any more
                VLOG(2) << idStr_ << "The leader has changed";
                sendingPromise_.setValue(AppendLogResult::E_NOT_A_LEADER);
                replicatingLogs_ = false;
                return;
            }
            if (currTerm != term_) {
                LOG(INFO) << idStr_ << "The leader has changed, ABA problem.";
                sendingPromise_.setValue(AppendLogResult::E_TERM_OUT_OF_DATE);
                replicatingLogs_ = false;
                return;
            }
            lastLogId_ = lastLogId;
            lastLogTerm_ = currTerm;

            lastMsgSentDur_.reset();

            auto walIt = wal_->iterator(committedId + 1, lastLogId);
            // Step 3: Commit the batch
            if (commitLogs(std::move(walIt))) {
                committedLogId_ = lastLogId;
                firstLogId = lastLogId_ + 1;
            } else {
                LOG(FATAL) << idStr_ << "Failed to commit logs";
            }
            VLOG(2) << idStr_ << "Leader succeeded in committing the logs "
                              << committedId + 1 << " to " << lastLogId;
        }
        // Step 4: Fulfill the promise
        if (iter.hasNonCASLogs()) {
            sendingPromise_.setOneSharedValue(AppendLogResult::SUCCEEDED);
        }
        if (iter.leadByCAS()) {
            sendingPromise_.setOneSingleValue(AppendLogResult::SUCCEEDED);
        }
        // Step 5: Check whether need to continue
        // the log replication
        CHECK(replicatingLogs_);
        // Continue to process the original AppendLogsIterator if necessary
        iter.resume();
        if (iter.empty()) {
            std::lock_guard<std::mutex> lck(logsLock_);
            if (logs_.size() > 0) {
                // continue to replicate the logs
                sendingPromise_ = std::move(cachingPromise_);
                cachingPromise_.reset();
                iter = AppendLogsIterator(
                    firstLogId,
                    currTerm,
                    std::move(logs_),
                    [this] (const std::string& log) -> std::string {
                        auto res = compareAndSet(log);
                        if (res.empty()) {
                            // Failed
                            sendingPromise_.setOneSingleValue(
                                AppendLogResult::E_CAS_FAILURE);
                        }
                        return res;
                    });
                logs_.clear();
            } else {
                replicatingLogs_ = false;
                VLOG(2) << idStr_ << "No more log to be replicated";
            }
        }
        if (!iter.empty()) {
            appendLogsInternal(std::move(iter), currTerm);
        }
    } else {
        // Not enough hosts accepted the log, re-try
        LOG(WARNING) << idStr_ << "Only " << numSucceeded
                     << " hosts succeeded, Need to try again";
        replicateLogs(eb,
                      std::move(iter),
                      currTerm,
                      lastLogId,
                      committedId,
                      prevLogTerm,
                      prevLogId);
    }
}


bool RaftPart::needToSendHeartbeat() {
    std::lock_guard<std::mutex> g(raftLock_);
    return status_ == Status::RUNNING &&
           role_ == Role::LEADER &&
           lastMsgSentDur_.elapsedInSec() >= FLAGS_heartbeat_interval * 2 / 5;
}


bool RaftPart::needToStartElection() {
    std::lock_guard<std::mutex> g(raftLock_);
    if (status_ == Status::RUNNING &&
        role_ == Role::FOLLOWER &&
        (lastMsgRecvDur_.elapsedInSec() >= FLAGS_heartbeat_interval ||
         term_ == 0)) {
        role_ = Role::CANDIDATE;
    }

    return role_ == Role::CANDIDATE;
}


bool RaftPart::prepareElectionRequest(
        cpp2::AskForVoteRequest& req,
        std::vector<std::shared_ptr<Host>>& hosts) {
    std::lock_guard<std::mutex> g(raftLock_);

    // Make sure the partition is running
    if (status_ != Status::RUNNING) {
        VLOG(2) << idStr_ << "The partition is not running";
        return false;
    }

    // Make sure the role is still CANDIDATE
    if (role_ != Role::CANDIDATE) {
        VLOG(2) << idStr_ << "A leader has been elected";
        return false;
    }

    req.set_space(spaceId_);
    req.set_part(partId_);
    req.set_candidate_ip(addr_.first);
    req.set_candidate_port(addr_.second);
    req.set_term(++proposedTerm_);  // Bump up the proposed term
    req.set_last_log_id(lastLogId_);
    req.set_last_log_term(lastLogTerm_);

    hosts = followers();

    return true;
}


typename RaftPart::Role RaftPart::processElectionResponses(
        const RaftPart::ElectionResponses& results) {
    std::lock_guard<std::mutex> g(raftLock_);

    // Make sure the partition is running
    if (status_ != Status::RUNNING) {
        VLOG(2) << idStr_ << "The partition is not running";
        return Role::FOLLOWER;
    }

    if (role_ != Role::CANDIDATE) {
        LOG(INFO) << idStr_ << "Partition's role has changed to "
                  << roleStr(role_)
                  << " during the election, so discard the results";
        return role_;
    }

    size_t numSucceeded = 0;
    for (auto& r : results) {
        if (r.second.get_error_code() == cpp2::ErrorCode::SUCCEEDED) {
            ++numSucceeded;
        }
    }

    CHECK(role_ == Role::CANDIDATE);

    if (numSucceeded >= quorum_) {
        LOG(INFO) << idStr_
                  << "Partition is elected as the new leader for term "
                  << proposedTerm_;
        term_ = proposedTerm_;
        role_ = Role::LEADER;
    }

    return role_;
}


bool RaftPart::leaderElection() {
    VLOG(2) << idStr_ << "Start leader election...";
    using namespace folly;  // NOLINT since the fancy overload of | operator

    cpp2::AskForVoteRequest voteReq;
    decltype(hosts_) hosts;
    if (!prepareElectionRequest(voteReq, hosts)) {
        return false;
    }

    // Send out the AskForVoteRequest
    VLOG(2) << idStr_ << "Sending out an election request "
            << "(space = " << voteReq.get_space()
            << ", part = " << voteReq.get_part()
            << ", term = " << voteReq.get_term()
            << ", lastLogId = " << voteReq.get_last_log_id()
            << ", lastLogTerm = " << voteReq.get_last_log_term()
            << ", candidateIP = "
            << NetworkUtils::intToIPv4(voteReq.get_candidate_ip())
            << ", candidatePort = " << voteReq.get_candidate_port()
            << ")";

    auto resps = ElectionResponses();
    if (hosts.empty()) {
        VLOG(2) << idStr_ << "No peer found, I will be the leader";
    } else {
        auto eb = ioThreadPool_->getEventBase();
        auto futures = collectNSucceeded(
            gen::from(hosts)
            | gen::map([eb, self = shared_from_this(), &voteReq] (auto& host) {
                VLOG(2) << self->idStr_
                        << "Sending AskForVoteRequest to "
                        << host->idStr();
                return via(
                    eb,
                    [&voteReq, &host] ()
                            -> Future<cpp2::AskForVoteResponse> {
                        return host->askForVote(voteReq);
                    });
            })
            | gen::as<std::vector>(),
            // Number of succeeded required
            quorum_,
            // Result evaluator
            [](size_t, cpp2::AskForVoteResponse& resp) {
                return resp.get_error_code()
                    == cpp2::ErrorCode::SUCCEEDED;
            });

        VLOG(2) << idStr_
                << "AskForVoteRequest has been sent to all peers"
                   ", waiting for responses";
        futures.wait();
        CHECK(!futures.hasException())
            << "Got exception -- "
            << futures.result().exception().what().toStdString();
        VLOG(2) << idStr_ << "Got AskForVote response back";

        resps = std::move(futures).get();
    }

    // Process the responses
    switch (processElectionResponses(resps)) {
        case Role::LEADER: {
            // Elected
            LOG(INFO) << idStr_
                      << "The partition is elected as the leader";
            {
                std::lock_guard<std::mutex> g(raftLock_);
                if (status_ == Status::RUNNING) {
                    leader_ = addr_;
                    workers_->addTask([self = shared_from_this(),
                                       term = voteReq.get_term()] {
                        self->onElected(term);
                    });
                }
            }
            sendHeartbeat();
            return true;
        }
        case Role::FOLLOWER: {
            // Someone was elected
            VLOG(2) << idStr_ << "Someone else was elected";
            return true;
        }
        case Role::CANDIDATE: {
            // No one has been elected
            VLOG(2) << idStr_
                    << "No one is elected, continue the election";
            return false;
        }
        case Role::LEARNER: {
            LOG(FATAL) << idStr_ << " Impossible! There must be some bugs!";
            return false;
        }
    }

    LOG(FATAL) << "Should not reach here";
    return false;
}


void RaftPart::statusPolling() {
    size_t delay = FLAGS_heartbeat_interval * 1000 / 3;
    if (needToStartElection()) {
        VLOG(2) << idStr_ << "Need to start leader election";
        if (leaderElection()) {
            VLOG(2) << idStr_ << "Stop the election";
        } else {
            // No leader has been elected, need to continue
            // (After sleeping a random period betwen [500ms, 2s])
            VLOG(2) << idStr_ << "Wait for a while and continue the leader election";
            delay = folly::Random::rand32(1500) + 500;
        }
    } else if (needToSendHeartbeat()) {
        VLOG(2) << idStr_ << "Need to send heartbeat";
        sendHeartbeat();
    }

    {
        std::lock_guard<std::mutex> g(raftLock_);
        if (status_ == Status::RUNNING) {
            workers_->addDelayTask(
                delay,
                [self = shared_from_this()] {
                    self->statusPolling();
                });
        }
    }
}


void RaftPart::processAskForVoteRequest(
        const cpp2::AskForVoteRequest& req,
        cpp2::AskForVoteResponse& resp) {
    VLOG(2) << idStr_
            << "Recieved a VOTING request"
            << ": space = " << req.get_space()
            << ", partition = " << req.get_part()
            << ", candidateAddr = "
            << NetworkUtils::intToIPv4(req.get_candidate_ip()) << ":"
            << req.get_candidate_port()
            << ", term = " << req.get_term()
            << ", lastLogId = " << req.get_last_log_id()
            << ", lastLogTerm = " << req.get_last_log_term();

    std::lock_guard<std::mutex> g(raftLock_);

    // Make sure the partition is running
    if (status_ != Status::RUNNING) {
        LOG(ERROR) << idStr_ << "The partition is not running";
        resp.set_error_code(cpp2::ErrorCode::E_NOT_READY);
        return;
    }

    VLOG(2) << idStr_ << "The partition currently is a "
            << roleStr(role_);

    // Check term id
    auto term = role_ == Role::CANDIDATE ? proposedTerm_ : term_;
    if (req.get_term() <= term) {
        VLOG(2) << idStr_
                << (role_ == Role::CANDIDATE
                    ? "The partition is currently proposing term "
                    : "The partition currently is on term ")
                << term
                << ". The term proposed by the candidate is"
                   " no greater, so it will be rejected";
        resp.set_error_code(cpp2::ErrorCode::E_TERM_OUT_OF_DATE);
        return;
    }

    // Check the last term to receive a log
    if (req.get_last_log_term() < lastLogTerm_) {
        VLOG(2) << idStr_
                << "The partition's last term to receive a log is "
                << lastLogTerm_
                << ", which is newer than the candidate's"
                   ". So the candidate will be rejected";
        resp.set_error_code(cpp2::ErrorCode::E_TERM_OUT_OF_DATE);
        return;
    }

    if (req.get_last_log_term() == lastLogTerm_) {
        // Check last log id
        if (req.get_last_log_id() < lastLogId_) {
            VLOG(2) << idStr_
                    << "The partition's last log id is " << lastLogId_
                    << ". The candidate's last log id is smaller"
                       ", so it will be rejected";
            resp.set_error_code(cpp2::ErrorCode::E_LOG_STALE);
            return;
        }
    }

    // Ok, no reason to refuse, we will vote for the candidate
    VLOG(2) << idStr_ << "The partition will vote for the candidate";
    resp.set_error_code(cpp2::ErrorCode::SUCCEEDED);

    Role oldRole = role_;
    TermID oldTerm = term_;
    role_ = Role::FOLLOWER;
    term_ = proposedTerm_ = req.get_term();
    leader_ = std::make_pair(req.get_candidate_ip(),
                             req.get_candidate_port());

    // Reset the last message time
    lastMsgRecvDur_.reset();

    // If the partition used to be a leader, need to fire the callback
    if (oldRole == Role::LEADER) {
        // Need to invoke the onLostLeadership callback
        VLOG(2) << idStr_ << "Was a leader, need to do some clean-up";
        workers_->addTask(
            [self = shared_from_this(), oldTerm] {
                self->onLostLeadership(oldTerm);
            });
    }

    return;
}


void RaftPart::processAppendLogRequest(
        const cpp2::AppendLogRequest& req,
        cpp2::AppendLogResponse& resp) {
    bool hasSnapshot = req.get_snapshot_uri() != nullptr;

    VLOG(2) << idStr_
            << "Received logAppend "
            << ": GraphSpaceId = " << req.get_space()
            << ", partition = " << req.get_part()
            << ", current_term = " << req.get_current_term()
            << ", committedLogId = " << req.get_committed_log_id()
            << ", leaderIp = " << req.get_leader_ip()
            << ", leaderPort = " << req.get_leader_port()
            << ", lastLogIdSent = " << req.get_last_log_id_sent()
            << ", lastLogTermSent = " << req.get_last_log_term_sent()
            << folly::stringPrintf(
                    ", num_logs = %ld, logTerm = %ld",
                    req.get_log_str_list().size(),
                    req.get_log_term())
            << (hasSnapshot
                ? ", SnapshotURI = " + *(req.get_snapshot_uri())
                : "");

    std::lock_guard<std::mutex> g(raftLock_);

    resp.set_current_term(term_);
    resp.set_leader_ip(leader_.first);
    resp.set_leader_port(leader_.second);
    resp.set_committed_log_id(committedLogId_);
    resp.set_last_log_id(lastLogId_);
    resp.set_last_log_term(lastLogTerm_);
    resp.set_pulling_snapshot(false);

    // Check status
    if (UNLIKELY(status_ == Status::STOPPED)) {
        VLOG(2) << idStr_
                << "The part has been stopped, skip the request";
        resp.set_error_code(cpp2::ErrorCode::E_BAD_STATE);
        return;
    }
    if (UNLIKELY(status_ == Status::STARTING)) {
        VLOG(2) << idStr_ << "The partition is still starting";
        resp.set_error_code(cpp2::ErrorCode::E_NOT_READY);
        return;
    }

    TermID oldTerm = term_;
    Role oldRole = role_;

    // Check leadership
    cpp2::ErrorCode err = verifyLeader(req, g);
    if (err != cpp2::ErrorCode::SUCCEEDED) {
        // Wrong leadership
        VLOG(2) << idStr_ << "Will not follow the leader";
        resp.set_error_code(err);
        return;
    }

    // Reset the timeout timer
    lastMsgRecvDur_.reset();

    // TODO Check snapshot pulling status
//      if (hasSnapshot && !isPullingSnapshot()) {
//          // We need to pull the snapshot
//          startSnapshotPullingThread(std::move(req.get_snapshot_uri()));
//      }
//      if (isPullingSnapshot()) {
//          CHECK_NE(oldRole, Role::LEADER);
//          resp.set_pulling_snapshot(true);
//          if (!FLAGS_accept_log_append_during_pulling) {
//              VLOG(2) << idStr_
//                      << "Pulling the snapshot and not allowed to accept"
//                         " the LogAppend Requests";
//              resp.set_error_code(cpp2::ErrorCode::E_PULLING_SNAPSHOT);
//              return;
//          }
//      }

    // Check the last log
    CHECK_GE(req.get_last_log_id_sent(), committedLogId_);
    if (lastLogTerm_ > 0 && req.get_last_log_term_sent() != lastLogTerm_) {
        VLOG(2) << idStr_ << "The local last log term is " << lastLogTerm_
                << ", which is different from the leader's prevLogTerm "
                << req.get_last_log_term_sent()
                << ". So need to rollback to last committedLogId_ " << committedLogId_;
        wal_->rollbackToLog(committedLogId_);
        lastLogId_ = wal_->lastLogId();
        lastLogTerm_ = wal_->lastLogTerm();
        resp.set_last_log_id(lastLogId_);
        resp.set_last_log_term(lastLogTerm_);
        resp.set_error_code(cpp2::ErrorCode::E_LOG_GAP);
        return;
    } else if (req.get_last_log_id_sent() > lastLogId_) {
        // There is a gap
        VLOG(2) << idStr_ << "Local is missing logs from id "
                << lastLogId_ << ". Need to catch up";
        resp.set_error_code(cpp2::ErrorCode::E_LOG_GAP);
        return;
    } else if (req.get_last_log_id_sent() < lastLogId_) {
        // Local has some extra logs, which need to be rolled back
        wal_->rollbackToLog(req.get_last_log_id_sent());
        lastLogId_ = wal_->lastLogId();
        lastLogTerm_ = wal_->lastLogTerm();
        resp.set_last_log_id(lastLogId_);
        resp.set_last_log_term(lastLogTerm_);
    }

    // Append new logs
    size_t numLogs = req.get_log_str_list().size();
    LogID firstId = req.get_last_log_id_sent() + 1;
    VLOG(2) << idStr_ << "Writing log [" << firstId
            << ", " << firstId + numLogs - 1 << "] to WAL";
    LogStrListIterator iter(firstId,
                            req.get_log_term(),
                            req.get_log_str_list());
    if (wal_->appendLogs(iter)) {
        CHECK_EQ(firstId + numLogs - 1, wal_->lastLogId());
        lastLogId_ = wal_->lastLogId();
        lastLogTerm_ = wal_->lastLogTerm();
        resp.set_last_log_id(lastLogId_);
        resp.set_last_log_term(lastLogTerm_);
    } else {
        LOG(ERROR) << idStr_ << "Failed to append logs to WAL";
        resp.set_error_code(cpp2::ErrorCode::E_WAL_FAIL);
        return;
    }

    if (req.get_committed_log_id() > committedLogId_) {
        // Commit some logs
        // We can only commit logs from firstId to min(lastLogId_, leader's commit log id),
        // follower can't always commit to leader's commit id because of lack of log
        LogID lastLogIdCanCommit = std::min(lastLogId_, req.get_committed_log_id());
        CHECK(committedLogId_ + 1 <= lastLogIdCanCommit);
        if (commitLogs(wal_->iterator(committedLogId_ + 1, lastLogIdCanCommit))) {
            VLOG(2) << idStr_ << "Follower succeeded committing log "
                              << committedLogId_ + 1 << " to "
                              << lastLogIdCanCommit;
            committedLogId_ = lastLogIdCanCommit;
            resp.set_committed_log_id(lastLogIdCanCommit);
        } else {
            LOG(ERROR) << idStr_ << "Failed to commit log "
                       << committedLogId_ + 1 << " to "
                       << req.get_committed_log_id();
            resp.set_error_code(cpp2::ErrorCode::E_WAL_FAIL);
            return;
        }
    }

    resp.set_error_code(cpp2::ErrorCode::SUCCEEDED);

    if (oldRole == Role::LEADER) {
        // Need to invoke onLostLeadership callback
        VLOG(2) << idStr_ << "Was a leader, need to do some clean-up";
        workers_->addTask([self = shared_from_this(), oldTerm] {
            self->onLostLeadership(oldTerm);
        });
    }
}


cpp2::ErrorCode RaftPart::verifyLeader(
        const cpp2::AppendLogRequest& req,
        std::lock_guard<std::mutex>& lck) {
    VLOG(2) << idStr_ << "The current role is " << roleStr(role_);
    UNUSED(lck);
    switch (role_) {
        case Role::LEARNER:
        case Role::FOLLOWER: {
            if (req.get_current_term() == term_ &&
                req.get_leader_ip() == leader_.first &&
                req.get_leader_port() == leader_.second) {
                VLOG(3) << idStr_ << "Same leader";
                return cpp2::ErrorCode::SUCCEEDED;
            }
            break;
        }
        case Role::LEADER: {
            // In this case, the remote term has to be newer
            // TODO optimize the case that the current partition is
            // isolated and the term keeps going up
            break;
        }
        case Role::CANDIDATE: {
            // Since the current partition is a candidate, the remote
            // term has to be newer so that it can be accepted
            break;
        }
    }

    // Make sure the remote term is greater than local's
    if (req.get_current_term() < term_) {
        LOG(ERROR) << idStr_ << "The local term is " << term_
                   << ". The remote term is not newer";
        return cpp2::ErrorCode::E_TERM_OUT_OF_DATE;
    }

    // Ok, no reason to refuse, just follow the leader
    VLOG(2) << idStr_ << "The current role is " << roleStr(role_)
            << ". Will follow the new leader "
            << network::NetworkUtils::intToIPv4(req.get_leader_ip())
            << ":" << req.get_leader_port()
            << " [Term: " << req.get_current_term() << "]";

    if (role_ != Role::LEARNER) {
        role_ = Role::FOLLOWER;
    }
    leader_ = std::make_pair(req.get_leader_ip(),
                             req.get_leader_port());
    term_ = proposedTerm_ = req.get_current_term();

    return cpp2::ErrorCode::SUCCEEDED;
}


folly::Future<AppendLogResult> RaftPart::sendHeartbeat() {
    std::string log = "";
    return appendLogAsync(clusterId_, LogType::NORMAL, std::move(log));
}

std::vector<std::shared_ptr<Host>> RaftPart::followers() const {
    CHECK(!raftLock_.try_lock());
    decltype(hosts_) hosts;
    for (auto& h : hosts_) {
        if (!h->isLearner()) {
            hosts.emplace_back(h);
        }
    }
    return hosts;
}


}  // namespace raftex
}  // namespace nebula

