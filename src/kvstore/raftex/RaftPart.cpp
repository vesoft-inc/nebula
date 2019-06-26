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
                       RaftPart::LogCache logs,
                       std::function<std::string(const std::string&)> casCB)
            : firstLogId_(firstLogId)
            , logId_(firstLogId)
            , logs_(std::move(logs))
            , casCB_(std::move(casCB)) {
        leadByCAS_ = processCAS();
        valid_ = idx_ < logs_.size();
        hasLogs_ = !leadByCAS_ && valid_;
    }

    AppendLogsIterator(const AppendLogsIterator&) = delete;
    AppendLogsIterator(AppendLogsIterator&&) = default;

    AppendLogsIterator& operator=(const AppendLogsIterator&) = delete;
    AppendLogsIterator& operator=(AppendLogsIterator&&) = default;

    bool leadByCAS() const {
        return leadByCAS_;
    }

    bool hasLogs() const {
        return hasLogs_;
    }

    LogID firstLogId() const {
        return firstLogId_;
    }

    // Return true if the current log is a CAS, otherwise return false
    bool processCAS() {
        while (idx_ < logs_.size()) {
            auto& tup = logs_.at(idx_);
            bool isCAS = std::get<2>(tup);
            if (!isCAS) {
                // Not a CAS
                return false;
            }

            // Process CAS log
            CHECK(!!casCB_);
            casResult_ = casCB_(std::get<3>(tup));
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
        valid_ = (idx_ < logs_.size()) && !isCAS();
        if (valid_) {
            hasLogs_ = true;
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
        DCHECK(valid());
        return std::get<1>(logs_.at(idx_));
    }

    ClusterID logSource() const override {
        DCHECK(valid());
        return std::get<0>(logs_.at(idx_));
    }

    folly::StringPiece logMsg() const override {
        DCHECK(valid());
        if (isCAS()) {
            return casResult_;
        } else {
            return std::get<3>(logs_.at(idx_));
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
            hasLogs_ = !leadByCAS_ && valid_;
        }
    }

private:
    bool isCAS() const {
        return std::get<2>(logs_.at(idx_));
    }

private:
    size_t idx_{0};
    bool leadByCAS_{false};
    bool hasLogs_{false};
    bool valid_{true};
    std::string casResult_;
    LogID firstLogId_;
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
    wal_ = FileBasedWal::getWal(walRoot, FileBasedWalPolicy(), flusher);
    lastLogId_ = wal_->lastLogId();
    term_ = proposedTerm_ = lastLogTerm_ = wal_->lastLogTerm();
}


RaftPart::~RaftPart() {
    std::lock_guard<std::mutex> g(raftLock_);

    // Make sure the partition has stopped
    CHECK(status_ == Status::STOPPED);
}


const char* RaftPart::roleStr(Role role) const {
    switch (role) {
    case Role::LEADER:
        return "Leader";
    case Role::FOLLOWER:
        return "Follower";
    case Role::CANDIDATE:
        return "Candidate";
    default:
        LOG(FATAL) << idStr_ << "Invalid role";
    }
    return nullptr;
}


void RaftPart::start(std::vector<HostAddr>&& peers) {
    std::lock_guard<std::mutex> g(raftLock_);

    // Set the quorum number
    quorum_ = (peers.size() + 1) / 2;
    VLOG(2) << idStr_ << "There are "
                      << peers.size()
                      << " peer hosts, and total "
                      << peers.size() + 1
                      << " copies. The quorum is " << quorum_ + 1;

    committedLogId_ = lastCommittedLogId();

    // Start all peer hosts
    peerHosts_ = std::make_shared<
        std::unordered_map<HostAddr, std::shared_ptr<Host>>
    >();
    for (auto& addr : peers) {
        peerHosts_->emplace(
            addr,
            std::make_shared<Host>(addr, shared_from_this()));
    }

    // Change the status
    status_ = Status::RUNNING;

    // Set up a leader election task
    size_t delayMS = 100 + folly::Random::rand32(900);
    workers_->addDelayTask(delayMS, [self = shared_from_this()] {
        self->statusPolling();
    });
}


void RaftPart::stop() {
    VLOG(2) << idStr_ << "Stopping the partition";

    decltype(peerHosts_) hosts;
    {
        std::unique_lock<std::mutex> lck(raftLock_);
        status_ = Status::STOPPED;

        hosts = std::move(peerHosts_);
    }

    for (auto& h : *hosts) {
        h.second->stop();
    }
    VLOG(2) << idStr_ << "Invoked stop() on all peer hosts";

    for (auto& h : *hosts) {
        VLOG(2) << idStr_ << "Waiting " << h.second->idStr() << " to stop";
        h.second->waitForStop();
        VLOG(2) << idStr_ << h.second->idStr() << "has stopped";
    }
    VLOG(2) << idStr_ << "All hosts are stopped";

    VLOG(2) << idStr_ << "Partition has been stopped";
}


AppendLogResult RaftPart::canAppendLogs(
        std::lock_guard<std::mutex>& lck) {
    UNUSED(lck);
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


folly::Future<AppendLogResult> RaftPart::appendAsync(ClusterID source,
                                                     std::string log) {
    if (source < 0) {
        source = clusterId_;
    }
    return appendLogAsync(source, false, std::move(log));
}


folly::Future<AppendLogResult> RaftPart::casAsync(std::string log) {
    return appendLogAsync(clusterId_, true, std::move(log));
}


folly::Future<AppendLogResult> RaftPart::appendLogAsync(ClusterID source,
                                                        bool isCAS,
                                                        std::string log) {
    LogCache swappedOutLogs;
    LogID firstId;
    auto retFuture = folly::Future<AppendLogResult>::makeEmpty();

    {
        std::lock_guard<std::mutex> lck(raftLock_);

        auto res = canAppendLogs(lck);
        if (res != AppendLogResult::SUCCEEDED) {
            LOG(ERROR) << idStr_
                       << "Cannot append logs, clean the buffer";
            cachingPromise_.setValue(std::move(res));
            cachingPromise_.reset();
            logs_.clear();
            return res;
        }

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
        if (isCAS) {
            logs_.emplace_back(source, term_, true, std::move(log));
            retFuture = cachingPromise_.getSingleFuture();
        } else {
            logs_.emplace_back(source, term_, false, std::move(log));
            retFuture = cachingPromise_.getSharedFuture();
        }

        if (replicatingLogs_) {
            VLOG(2) << idStr_
                    << "Another AppendLogs request is ongoing,"
                       " just return";
            return retFuture;
        } else {
            // We need to send logs to all followers
            VLOG(2) << idStr_ << "Preparing to send AppendLog request";
            replicatingLogs_ = true;
            sendingPromise_ = std::move(cachingPromise_);
            cachingPromise_.reset();
            std::swap(swappedOutLogs, logs_);
            firstId = lastLogId_ + 1;
        }
    }

    // Replicate buffered logs to all followers
    // Replication will happen on a separate thread and will block
    // until majority accept the logs, the leadership changes, or
    // the partition stops
    VLOG(2) << idStr_ << "Calling appendLogsInternal()";
    AppendLogsIterator it(
        firstId,
        std::move(swappedOutLogs),
        [this] (const std::string& msg) -> std::string {
            auto res = compareAndSet(msg);
            if (res.empty()) {
                // Failed
                sendingPromise_.setOneSingleValue(AppendLogResult::E_CAS_FAILURE);
            }
            return res;
        });
    appendLogsInternal(std::move(it));

    return retFuture;
}


void RaftPart::appendLogsInternal(AppendLogsIterator iter) {
    TermID currTerm = 0;
    LogID prevLogId = 0;
    TermID prevLogTerm = 0;
    LogID committed = 0;
    {
        std::lock_guard<std::mutex> g(raftLock_);
        currTerm = term_;
        prevLogId = lastLogId_;
        prevLogTerm = lastLogTerm_;
        committed = committedLogId_;
    }

    if (iter.valid()) {
        VLOG(2) << idStr_ << "Ready to append logs from id "
                << iter.logId() << " (Current term is "
                << currTerm << ")";
    } else {
        VLOG(2) << idStr_ << "Ready to send a heartbeat";
    }

    // Step 1: Write WAL
    if (!wal_->appendLogs(iter)) {
        LOG(ERROR) << idStr_ << "Failed to write into WAL";
        sendingPromise_.setValue(AppendLogResult::E_WAL_FAILURE);
        return;
    }
    LogID lastId = wal_->lastLogId();
    VLOG(2) << idStr_ << "Succeeded writing logs ["
            << iter.firstLogId() << ", " << lastId << "] to WAL";

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

    decltype(peerHosts_) hosts;
    {
        std::lock_guard<std::mutex> g(raftLock_);

        if (status_ != Status::RUNNING) {
            // The partition is not running
            VLOG(2) << idStr_ << "The partition is stopped";
            sendingPromise_.setValue(AppendLogResult::E_STOPPED);
            cachingPromise_.setValue(AppendLogResult::E_STOPPED);
            logs_.clear();
            replicatingLogs_ = false;
            return;
        }

        if (role_ != Role::LEADER) {
            // Is not a leader any more
            VLOG(2) << idStr_ << "The leader has changed";
            sendingPromise_.setValue(AppendLogResult::E_NOT_A_LEADER);
            cachingPromise_.setValue(AppendLogResult::E_NOT_A_LEADER);
            logs_.clear();
            replicatingLogs_ = false;
            return;
        }

        hosts = peerHosts_;
    }

    VLOG(2) << idStr_ << "About to replicate logs to all peer hosts";

    if (!hosts || hosts->empty()) {
        // No peer
        VLOG(2) << idStr_ << "The leader has no peer";
        processAppendLogResponses(AppendLogResponses(),
                                  eb,
                                  std::move(iter),
                                  currTerm,
                                  lastLogId,
                                  committedId,
                                  prevLogTerm,
                                  prevLogId);
        return;
    }

    using PeerHostEntry = typename decltype(peerHosts_)::element_type::value_type;
    collectNSucceeded(
        gen::from(*hosts)
        | gen::map([self = shared_from_this(),
                    eb,
                    currTerm,
                    lastLogId,
                    prevLogId,
                    prevLogTerm,
                    committedId] (PeerHostEntry& host) {
            VLOG(2) << self->idStr_
                    << "Appending logs to "
                    << NetworkUtils::intToIPv4(host.first.first)
                    << ":" << host.first.second;
            return via(
                eb,
                [=] () -> Future<cpp2::AppendLogResponse> {
                    return host.second->appendLogs(eb,
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
        [](cpp2::AppendLogResponse& resp) {
            return resp.get_error_code() == cpp2::ErrorCode::SUCCEEDED;
        })
        .then(eb, [self = shared_from_this(),
                   eb,
                   it = std::move(iter),
                   currTerm,
                   lastLogId,
                   committedId,
                   prevLogId,
                   prevLogTerm] (folly::Try<AppendLogResponses>&& result) mutable {
            VLOG(2) << self->idStr_ << "Received enough response";
            CHECK(!result.hasException());

            self->processAppendLogResponses(*result,
                                            eb,
                                            std::move(it),
                                            currTerm,
                                            lastLogId,
                                            committedId,
                                            prevLogTerm,
                                            prevLogId);

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
        LogID prevLogId) {
    // Make sure majority have succeeded
    size_t numSucceeded = 0;
    for (auto& res : resps) {
        if (res.get_error_code() == cpp2::ErrorCode::SUCCEEDED) {
            ++numSucceeded;
        }
    }

    if (numSucceeded >= quorum_) {
        // Majority have succeeded
        VLOG(2) << idStr_ << numSucceeded
                << " hosts have accepted the logs";

        std::unique_ptr<LogIterator> walIt;
        {
            std::lock_guard<std::mutex> g(raftLock_);

            lastLogId_ = lastLogId;
            lastLogTerm_ = currTerm;

            lastMsgSentDur_.reset();

            walIt = wal_->iterator(committedId + 1, lastLogId);
        }

        // Step 3: Commit the batch
        if (commitLogs(std::move(walIt))) {
            std::lock_guard<std::mutex> g(raftLock_);

            committedLogId_ = lastLogId;

            // Step 4: Fulfill the promise
            if (iter.hasLogs()) {
                sendingPromise_.setOneSharedValue(AppendLogResult::SUCCEEDED);
            }
            if (iter.leadByCAS()) {
                sendingPromise_.setOneSingleValue(AppendLogResult::SUCCEEDED);
            }
            VLOG(2) << idStr_ << "Succeeded in committing the logs";

            // Step 5: Check whether need to continue
            // the log replication
            CHECK(replicatingLogs_);
            // Continue to process the original AppendLogsIterator if necessary
            iter.resume();
            if (iter.empty()) {
                if (logs_.size() > 0) {
                    // continue to replicate the logs
                    sendingPromise_ = std::move(cachingPromise_);
                    cachingPromise_.reset();
                    iter = AppendLogsIterator(
                        lastLogId_ + 1,
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
        } else {
            LOG(FATAL) << idStr_ << "Failed to commit logs";
        }

        if (!iter.empty()) {
            appendLogsInternal(std::move(iter));
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
        std::shared_ptr<std::unordered_map<HostAddr, std::shared_ptr<Host>>>& hosts) {
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

    hosts = peerHosts_;

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
        if (r.get_error_code() == cpp2::ErrorCode::SUCCEEDED) {
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
    decltype(peerHosts_) hosts;
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
    if (!hosts || hosts->empty()) {
        VLOG(2) << idStr_ << "No peer found, I will be the leader";
    } else {
        auto eb = ioThreadPool_->getEventBase();
        auto futures = collectNSucceeded(
            gen::from(*hosts)
            | gen::map([eb, self = shared_from_this(), &voteReq] (
                    decltype(peerHosts_)::element_type::value_type& host) {
                VLOG(2) << self->idStr_
                        << "Sending AskForVoteRequest to "
                        << NetworkUtils::intToIPv4(host.first.first)
                        << ":" << host.first.second;
                return via(
                    eb,
                    [&voteReq, &host] ()
                            -> Future<cpp2::AskForVoteResponse> {
                        return host.second->askForVote(voteReq);
                    });
            })
            | gen::as<std::vector>(),
            // Number of succeeded required
            quorum_,
            // Result evaluator
            [](cpp2::AskForVoteResponse& resp) {
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
    }

    LOG(FATAL) << "Should not reach here";
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
    bool isHeartbeat = req.get_log_str_list().empty();
    bool hasSnapshot = req.get_snapshot_uri() != nullptr;

    VLOG(2) << idStr_
            << "Received a "
            << (isHeartbeat ? "Heartbeat" : "LogAppend")
            << ": GraphSpaceId = " << req.get_space()
            << ", partition = " << req.get_part()
            << ", current_term = " << req.get_current_term()
            << ", committedLogId = " << req.get_committed_log_id()
            << ", leaderIp = " << req.get_leader_ip()
            << ", leaderPort = " << req.get_leader_port()
            << ", lastLogIdSent = " << req.get_last_log_id_sent()
            << ", lastLogTermSent = " << req.get_last_log_term_sent()
            << (isHeartbeat
                ? ""
                : folly::stringPrintf(
                    ", num_logs = %ld, logTerm = %ld",
                    req.get_log_str_list().size(),
                    req.get_log_term()))
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
        VLOG(2) << idStr_ << "The local last log term is "
                << lastLogTerm_
                << ", which is different from the leader's"
                   ". So need to rollback";
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

    if (!isHeartbeat) {
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
    } else {
        VLOG(2) << idStr_
                << "Request is a heartbeat, nothing to put into WAL";
    }

    if (req.get_committed_log_id() > committedLogId_) {
        // Commit some logs
        if (commitLogs(wal_->iterator(committedLogId_ + 1,
                                      req.get_committed_log_id()))) {
            VLOG(2) << idStr_ << "Succeeded committing log "
                              << committedLogId_ + 1 << " to "
                              << req.get_committed_log_id();
            committedLogId_ = req.get_committed_log_id();
            resp.set_committed_log_id(committedLogId_);
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
    if (req.get_current_term() <= term_) {
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

    role_ = Role::FOLLOWER;
    leader_ = std::make_pair(req.get_leader_ip(),
                             req.get_leader_port());
    term_ = proposedTerm_ = req.get_current_term();

    return cpp2::ErrorCode::SUCCEEDED;
}


folly::Future<AppendLogResult> RaftPart::sendHeartbeat() {
    using namespace folly;  // NOLINT since the fancy overload of | operator

    VLOG(2) << idStr_ << "Sending heartbeat to all other hosts";

    TermID term = 0;
    LogID lastLogId = 0;
    TermID lastLogTerm = 0;
    LogID committed = 0;

    decltype(peerHosts_) hosts;
    {
        std::lock_guard<std::mutex> g(raftLock_);

        auto res = canAppendLogs(g);
        if (res != AppendLogResult::SUCCEEDED) {
            LOG(ERROR) << idStr_
                       << "Cannot send heartbeat, clean up the buffer";
            return res;
        }

        if (!logs_.empty()) {
            LOG(WARNING) << idStr_
                         << "There is logs in the buffer,"
                            " stop sending the heartbeat";
            return AppendLogResult::SUCCEEDED;
        }

        if (replicatingLogs_) {
            VLOG(2) << idStr_
                    << "Logs are being sent out."
                       " Stop sending the heartbeat";
            return AppendLogResult::SUCCEEDED;
        } else {
            // We need to send logs to all followers
            replicatingLogs_ = true;
        }

        // Prepare to send heartbeat to all followers
        term = term_;
        lastLogId = lastLogId_;
        lastLogTerm = lastLogTerm_;
        committed = committedLogId_;

        hosts = peerHosts_;
    }

    if (!hosts || hosts->empty()) {
        VLOG(2) << idStr_ << "No peer to send the heartbeat";
        doneHeartbeat();
        return AppendLogResult::SUCCEEDED;
    }

    auto eb = ioThreadPool_->getEventBase();

    using PeerHostEntry = typename decltype(peerHosts_)::element_type::value_type;
    return collectNSucceeded(
        gen::from(*hosts)
        | gen::map([=, self = shared_from_this()] (PeerHostEntry& host) {
            VLOG(2) << self->idStr_
                    << "Send a heartbeat to "
                    << NetworkUtils::intToIPv4(host.first.first)
                    << ":" << host.first.second;
            return via(
                eb,
                [=, &host] () -> Future<cpp2::AppendLogResponse> {
                    return host.second->appendLogs(eb,
                                                   term,
                                                   lastLogId,
                                                   committed,
                                                   lastLogTerm,
                                                   lastLogId);
                });
        })
        | gen::as<std::vector>(),
        // Number of succeeded required
        quorum_,
        // Result evaluator
        [](cpp2::AppendLogResponse& resp) {
            return resp.get_error_code() == cpp2::ErrorCode::SUCCEEDED;
        })
        .then([=, self = shared_from_this()] (
                folly::Try<AppendLogResponses>&& result)
                    -> folly::Future<AppendLogResult> {
            VLOG(2) << self->idStr_ << "Done with heartbeats";
            CHECK(!result.hasException());

            self->doneHeartbeat();
            return AppendLogResult::SUCCEEDED;
        });
}


void RaftPart::doneHeartbeat() {
    decltype(logs_) swappedOutLogs;
    LogID firstId = 0;
    {
        std::lock_guard<std::mutex> g(raftLock_);
        CHECK(replicatingLogs_);
        if (logs_.size() > 0) {
            // continue to replicate the logs
            sendingPromise_ = std::move(cachingPromise_);
            cachingPromise_.reset();
            std::swap(swappedOutLogs, logs_);
            firstId = lastLogId_ + 1;
        } else {
            replicatingLogs_ = false;
        }
    }
    if (!swappedOutLogs.empty()) {
        AppendLogsIterator it(
            firstId,
            std::move(swappedOutLogs),
            [this] (const std::string& msg) -> std::string {
                auto res = compareAndSet(msg);
                if (res.empty()) {
                    // Failed
                    sendingPromise_.setOneSingleValue(
                        AppendLogResult::E_CAS_FAILURE);
                }
                return res;
            });
        appendLogsInternal(std::move(it));
    }
}

}  // namespace raftex
}  // namespace nebula

