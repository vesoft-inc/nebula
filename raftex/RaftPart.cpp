/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "raftex/RaftPart.h"
#include "base/CollectNSucceeded.h"
#include "network/ThriftSocketManager.h"
#include <folly/io/async/EventBaseManager.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/gen/Base.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>
#include "interface/gen-cpp2/RaftexServiceAsyncClient.h"
#include "network/NetworkUtils.h"
#include "thread/NamedThread.h"
#include "raftex/FileBasedWal.h"
#include "raftex/BufferFlusher.h"
#include "raftex/LogStrListIterator.h"
#include "raftex/Host.h"


DEFINE_bool(accept_log_append_during_pulling, false,
            "Whether to accept new logs during pulling the snapshot");
DEFINE_uint32(heartbeat_interval, 10,
             "Seconds between each heartbeat");
DEFINE_uint32(num_worker_threads, 4,
              "The number of threads in the shared worker thread pool");
DEFINE_uint32(max_batch_size, 256, "The max number of logs in a batch");


namespace vesoft {
namespace raftex {

using namespace vesoft::network;
using namespace vesoft::thread;

class AppendLogsIterator final : public LogIterator {
public:
    AppendLogsIterator(
        LogID firstLogId,
        const std::vector<
            std::tuple<ClusterID, TermID, std::string>
        >& logs)
            : firstLogId_(firstLogId)
            , logs_(logs) {
    }

    LogIterator& operator++() override {
        ++idx_;
        return *this;
    }

    bool valid() const override {
        return idx_ < logs_.size();
    }

    LogID logId() const override {
        DCHECK(valid());
        return firstLogId_ + idx_;
    }

    TermID logTerm() const override {
        DCHECK(valid());
        return std::get<1>(logs_.at(idx_));
    }

    ClusterID logSource() const override {
        DCHECK(valid());
        return std::get<0>(logs_.at(idx_));
    }

    folly::StringPiece logMsg() const {
        DCHECK(valid());
        return std::get<2>(logs_.at(idx_));
    }

private:
    size_t idx_{0};
    LogID firstLogId_;
    const std::vector<
        std::tuple<ClusterID, TermID, std::string>
    >& logs_;
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
                   std::vector<HostAddr>&& peers,
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
        , peerAddresses_{std::move(peers)}
        , quorum_{(peerAddresses_.size() + 1) / 2}
        , appendLogFuture_{folly::Future<AppendLogResponses>::makeEmpty()}
        , status_{Status::STARTING}
        , role_{Role::FOLLOWER}
        , leader_{0, 0}
        , ioThreadPool_{pool}
        , workers_{workers} {
    // Initialize all peers
    VLOG(2) << idStr_ << "There are "
                      << peerAddresses_.size()
                      << " peer hosts, and total " << peerAddresses_.size() + 1
                      << " copies. The quorum is " << quorum_ + 1;
    // TODO Configure the wal policy
    wal_ = FileBasedWal::getWal(walRoot, FileBasedWalPolicy(), flusher);
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


void RaftPart::start() {
    std::lock_guard<std::mutex> g(raftLock_);
    status_ = Status::RUNNING;

    for (auto& addr : peerAddresses_) {
        peerHosts_.emplace(
            addr,
            std::make_shared<Host>(addr, shared_from_this()));
    }

    // Set up a leader election task
    size_t delayMS = 100 + folly::Random::rand32(900);
    statusPollingFuture_ = workers_->addDelayTask(
        delayMS,
        [self = shared_from_this()] {
            self->statusPolling();
        });
}


void RaftPart::stop() {
    VLOG(2) << idStr_ << "Stopping the partition";

    {
        std::lock_guard<std::mutex> g(raftLock_);
        status_ = Status::STOPPED;
    }

    for (auto& h: peerHosts_) {
        h.second->stop();
    }
    VLOG(2) << idStr_ << "Invoked stop() on all peer hosts";

    if (statusPollingFuture_.valid()) {
        statusPollingFuture_.wait();
    }
    VLOG(2) << idStr_ << "Status polling task is stopped";

    if (lostLeadershipFuture_.valid()) {
        lostLeadershipFuture_.wait();
    }
    VLOG(2) << idStr_ << "onLostLeadership callback is stopped";

    if (electedFuture_.valid()) {
        electedFuture_.wait();
    }
    VLOG(2) << idStr_ << "onElected callback is stopped";

    for (auto& h: peerHosts_) {
        h.second->waitForStop();
    }
    peerHosts_.clear();
    VLOG(2) << idStr_ << "All hosts are stopped";

    VLOG(2) << idStr_ << "Partition has been stopped";
}


typename RaftPart::AppendLogResult RaftPart::canAppendLogs(
        TermID& term) {
    std::lock_guard<std::mutex> g(raftLock_);
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

    term = term_;
    return AppendLogResult::SUCCEEDED;
}


folly::Future<typename RaftPart::AppendLogResult>
RaftPart::appendLogsAsync(ClusterID source,
                          std::vector<std::string>&& logMsgs) {
    CHECK_GT(logMsgs.size(), 0UL);

    std::vector<
        std::tuple<ClusterID, TermID, std::string>> swappedOutLogs;
    folly::Future<AppendLogResult> appendFut = cachingPromise_.getFuture();

    TermID term;
    auto res = canAppendLogs(term);
    if (res != AppendLogResult::SUCCEEDED) {
        LOG(ERROR) << idStr_ << "Cannot append logs, clean up the buffer";

        std::lock_guard<std::mutex> g(appendLogLock_);
        cachingPromise_.setValue(res);
        logs_.clear();
        return appendFut;
    } else {
        std::lock_guard<std::mutex> g(appendLogLock_);

        if (logs_.size() + logMsgs.size() >= FLAGS_max_batch_size) {
            // Buffer is full
            LOG(WARNING) << idStr_
                         << "The appendLog buffer is full."
                            " Please slow down the log appending rate";
            // TODO We might want to repolicate the logs in the buffer
            return AppendLogResult::E_BUFFER_OVERFLOW;
        }

        for (auto& m : logMsgs) {
            logs_.emplace_back(source, term, std::move(m));
        }

        if (replicatingLogs_) {
            CHECK(!cachingPromise_.isFulfilled());
            return appendFut;
        } else {
            // We need to send logs to all followers
            replicatingLogs_ = true;
            sendingPromise_ = std::move(cachingPromise_);
            cachingPromise_ = folly::SharedPromise<AppendLogResult>();
            swappedOutLogs = std::move(logs_);
            logs_.clear();
        }
    }

    // Replicate to all followers
    // The call blocks until majority accept the logs,
    // the leadership changes, or the partition stops
    appendLogsInternal(std::move(swappedOutLogs));

    return appendFut;
}


void RaftPart::appendLogsInternal(
        std::vector<std::tuple<ClusterID, TermID, std::string>>&& logs) {
    CHECK(!logs.empty());

    LogID firstId = 0;
    TermID currTerm = 0;
    LogID prevLogId = 0;
    TermID prevLogTerm = 0;
    LogID committed = 0;
    {
        std::lock_guard<std::mutex> g(raftLock_);
        firstId = prevLogId_ + 1;
        currTerm = term_;
        prevLogId = prevLogId_;
        prevLogTerm = prevLogTerm_;
        committed = committedLogId_;
    }
    LogID lastId = firstId + logs.size() - 1;
    TermID lastTerm = std::get<1>(logs.back());

    // Step 1: Write WAL
    AppendLogsIterator it(firstId, std::move(logs));
    if (!wal_->appendLogs(it)) {
        LOG(ERROR) << idStr_ << "Failed to write into WAL";
        sendingPromise_.setValue(AppendLogResult::E_WAL_FAILURE);
        return;
    }

    // Step 2: Replicate to followers
    auto eb = ioThreadPool_->getEventBase();
    appendLogFuture_ = replicateLogs(eb,
                                     currTerm,
                                     committed,
                                     prevLogId,
                                     prevLogTerm,
                                     lastId,
                                     lastTerm);

    return;
}


folly::Future<typename RaftPart::AppendLogResponses>
RaftPart::replicateLogs(
        folly::EventBase* eb,
        TermID currTerm,
        LogID committedId,
        LogID prevLogId,
        TermID prevLogTerm,
        LogID lastLogId,
        TermID lastLogTerm) {
    using namespace folly;

    if (isStopped()) {
        // The partition has stopped
        VLOG(2) << idStr_ << "The partition is stopped";
        sendingPromise_.setValue(AppendLogResult::E_STOPPED);
        return folly::Future<AppendLogResponses>::makeEmpty();
    }

    if (!isLeader()) {
        // Is not a leader any more
        VLOG(2) << idStr_ << "The leader has changed";
        sendingPromise_.setValue(AppendLogResult::E_NOT_A_LEADER);
        return folly::Future<AppendLogResponses>::makeEmpty();
    }

    using PeerHostEntry = typename decltype(peerHosts_)::value_type;
    return collectNSucceeded(
        gen::from(peerHosts_)
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
        2,
        // Result evaluator
        [](cpp2::AppendLogResponse& resp) {
            return resp.get_error_code() == cpp2::ErrorCode::SUCCEEDED;
        })
        .then(eb, [self = shared_from_this(),
                   eb,
                   currTerm,
                   committedId,
                   prevLogId,
                   prevLogTerm,
                   lastLogId,
                   lastLogTerm] (folly::Try<AppendLogResponses>&& result) {
            VLOG(2) << self->idStr_ << "Received enough response";
            CHECK(!result.hasException());

            self->processAppendLogResponses(*result,
                                            eb,
                                            currTerm,
                                            committedId,
                                            prevLogId,
                                            prevLogTerm,
                                            lastLogId,
                                            lastLogTerm);

            return *result;
        });
}


void RaftPart::processAppendLogResponses(
        const AppendLogResponses& resps,
        folly::EventBase* eb,
        TermID currTerm,
        LogID committedId,
        LogID prevLogId,
        TermID prevLogTerm,
        LogID lastLogId,
        TermID lastLogTerm) {
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

            prevLogId_ = lastLogId;
            prevLogTerm_ = lastLogTerm;

            lastMsgSentDur_.reset();

            walIt = wal_->iterator(committedId + 1, lastLogId);
        }

        decltype(logs_) swappedOutLogs;

        // Step 3: Commit the batch
        if (commitLogs(std::move(walIt))) {
            {
                std::lock_guard<std::mutex> g(raftLock_);

                myLastCommittedLogId_ = committedLogId_ = lastLogId;
            }

            // Step 4: Fulfill the promise
            AppendLogResult res = AppendLogResult::SUCCEEDED;
            sendingPromise_.setValue(std::move(res));

            // Step 5: Check whether need to continue
            // the log replication
            {
                std::lock_guard<std::mutex> g(appendLogLock_);
                CHECK(replicatingLogs_);
                if (logs_.size() > 0) {
                    // continue to replicate the logs
                    sendingPromise_ = std::move(cachingPromise_);
                    cachingPromise_ =
                        folly::SharedPromise<AppendLogResult>();
                    swappedOutLogs = std::move(logs_);
                    logs_.clear();
                } else {
                    replicatingLogs_ = false;
                }
            }
        } else {
            LOG(FATAL) << idStr_ << "Failed to commit logs";
        }

        if (!swappedOutLogs.empty()) {
            appendLogsInternal(std::move(swappedOutLogs));
        }
    } else {
        // Not enough hosts accepted the log, re-try
        LOG(WARNING) << idStr_ << "Only " << numSucceeded
                     << " hosts succeeded, Need to try again";
        appendLogFuture_ = replicateLogs(eb,
                                         currTerm,
                                         committedId,
                                         prevLogId,
                                         prevLogTerm,
                                         lastLogId,
                                         lastLogTerm);
    }
}


bool RaftPart::needToSendHeartbeat() const {
    std::lock_guard<std::mutex> g(raftLock_);
    return status_ == Status::RUNNING &&
           role_ == Role::LEADER &&
           lastMsgSentDur_.elapsedInSec() >= FLAGS_heartbeat_interval / 2;
}


bool RaftPart::needToStartElection() const {
    std::lock_guard<std::mutex> g(raftLock_);
    return status_ == Status::RUNNING &&
           role_ == Role::FOLLOWER &&
           (lastMsgRecvDur_.elapsedInSec() >= FLAGS_heartbeat_interval ||
            term_ == 0);
}


bool RaftPart::prepareElectionRequest(
        cpp2::AskForVoteRequest& req) {
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
    req.set_term(++term_);  // Bump up the term
    req.set_last_log_id(lastLogId_);
    req.set_last_log_term(lastLogTerm_);

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
                  << term_;
        role_ = Role::LEADER;
    }

    return role_;
}


void RaftPart::leaderElection(std::shared_ptr<RaftPart> self) {
    VLOG(2) << idStr_ << "Start leader election...";
    using namespace apache::thrift;
    using namespace folly;

    {
        std::lock_guard<std::mutex> g(raftLock_);

        if (role_ == Role::FOLLOWER) {
            // During the entire election, the role will be kept as
            // CANDIDATE, until a leader is elected
            role_ = Role::CANDIDATE;
        } else {
            VLOG(2) << idStr_
                    << "Not a follower any more, finish the election";
            return;
        }
    }

    // Loop until a leader is elected
    while (true) {
        cpp2::AskForVoteRequest voteReq;
        if (!prepareElectionRequest(voteReq)) {
            break;
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

        auto eb = ioThreadPool_->getEventBase();
        auto futures = collectNSucceeded(
            gen::from(peerAddresses_)
            | gen::map([eb, self = shared_from_this(), &voteReq] (
                    HostAddr& host) {
                VLOG(2) << self->idStr_
                        << "Sending AskForVoteRequest to "
                        << NetworkUtils::intToIPv4(host.first)
                        << ":" << host.second;
                return via(
                    eb,
                    [&voteReq,
                     &host,
                     self] () -> Future<cpp2::AskForVoteResponse> {
                        auto socket = ThriftSocketManager::getSocket(host);
                        if (!socket) {
                            LOG(ERROR)
                                << self->idStr_
                                << NetworkUtils::intToIPv4(host.first)
                                << ":" << host.second
                                << " is not connected";
                            cpp2::AskForVoteResponse resp;
                            resp.set_error_code(
                                cpp2::ErrorCode::E_HOST_DISCONNECTED);
                            return resp;
                        }

                        auto client =
                            std::make_unique<
                                cpp2::RaftexServiceAsyncClient
                            >(HeaderClientChannel::newChannel(socket));
                        return client->future_askForVote(voteReq);
                    });
            })
            | gen::as<std::vector>(),
            // Number of succeeded required
            quorum_,
            // Result evaluator
            [](cpp2::AskForVoteResponse& resp) {
                return resp.get_error_code() == cpp2::ErrorCode::SUCCEEDED;
            });

        VLOG(2) << idStr_
                << "AskForVoteRequest has been sent to all peers"
                   ", waiting for responses";
        futures.wait();
        CHECK(!futures.hasException())
            << "Got exception -- "
            << futures.result().exception().what().toStdString();

        // Process the responses
        bool stopElection = true;
        switch (processElectionResponses(std::move(futures).get())) {
            case Role::LEADER:
                // Elected
                LOG(INFO) << idStr_
                          << "The partition is elected as the leader";
                electedFuture_ = workers_->addTask(
                    [&voteReq, self = shared_from_this()] {
                        self->onElected(voteReq.get_term());
                    });
                sendHeartbeat();
                break;
            case Role::FOLLOWER:
                // Someone was elected
                VLOG(2) << idStr_ << "Someone else was elected";
                break;
            case Role::CANDIDATE:
                // No one has been elected
                VLOG(2) << idStr_
                        << "No one is elected, continue the election";
                stopElection = false;
                break;
        }
        if (stopElection) {
            break;
        }

        // No leader has been elected, need to continue
        // (After sleeping a random period betwen 500 milliseconds and 3 seconds)
        usleep(folly::Random::rand32(2500) * 1000 + 500000);
    }

    VLOG(2) << idStr_ << "Stop the election";
}


void RaftPart::startElection() {
    NamedThread electionThread(
        "leader-election",
        std::bind(&RaftPart::leaderElection, this, shared_from_this()));
    electionThread.detach();
}


void RaftPart::statusPolling() {
    if (needToStartElection()) {
        startElection();
    } else if (needToSendHeartbeat()) {
        sendHeartbeat();
    }

    if (isRunning()) {
        statusPollingFuture_ = workers_->addDelayTask(
            FLAGS_heartbeat_interval * 1000 / 3,
            [self = shared_from_this()] {
                self->statusPolling();
            });
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

    Role oldRole;
    TermID oldTerm;
    {
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
        if (req.get_term() <= term_) {
            VLOG(2) << idStr_
                    << "The partition currently is on term " << term_
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

        oldRole = role_;
        oldTerm = term_;
        role_ = Role::FOLLOWER;
        term_ = req.get_term();
        leader_ = std::make_pair(req.get_candidate_ip(),
                                 req.get_candidate_port());

        // Reset the last message time
        lastMsgRecvDur_.reset();
    }

    // If the partition used to be a leader, need to fire the callback
    if (oldRole == Role::LEADER) {
        // Need to invoke the onLostLeadership callback
        VLOG(2) << idStr_ << "Was a leader, need to do some clean-up";
        lostLeadershipFuture_ = workers_->addTask(
            [oldTerm, self = shared_from_this()] {
                self->onLostLeadership(oldTerm);
            });
    }

    return;
}


void RaftPart::processAppendLogRequest(
        const cpp2::AppendLogRequest& req,
        cpp2::AppendLogResponse& resp) {
    bool isHeartbeat = req.get_log_str_list() == nullptr;
    bool hasSnapshot = req.get_snapshot_uri() != nullptr;

    VLOG(2) << idStr_
            << "Received a "
            << (isHeartbeat ? "Heartbeat" : "LogAppend")
            << ": GraphSpaceId = " << req.get_space()
            << ", partition = " << req.get_part()
            << ", term = " << req.get_term()
            << ", committedLogId = " << req.get_committed_log_id()
            << ", leaderIp = " << req.get_leader_ip()
            << ", leaderPort = " << req.get_leader_port()
            << ", prevLogId = " << req.get_previous_log_id()
            << ", prevLogTerm = " << req.get_previous_log_term()
            << (isHeartbeat
                ? ""
                : ", num_logs = " + req.get_log_str_list()->size())
            << (hasSnapshot
                ? ", SnapshotURI = " + *(req.get_snapshot_uri())
                : "");

    Role oldRole;
    TermID oldTerm;
    {
        std::lock_guard<std::mutex> g(raftLock_);

        resp.set_term(term_);
        resp.set_leader_ip(leader_.first);
        resp.set_leader_port(leader_.second);
        resp.set_committed_log_id(committedLogId_);
        resp.set_last_log_id(lastLogId_);
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

        oldTerm = term_;
        oldRole = role_;

        // Check leadership
        cpp2::ErrorCode err = verifyLeader(req, oldRole);
        if (err != cpp2::ErrorCode::SUCCEEDED) {
            // Wrong leadership
            VLOG(2) << idStr_ << "Will not follow the leader";
            resp.set_error_code(err);
            return;
        }

        // Reset the timeout timer
        lastMsgRecvDur_.reset();

        // TODO Check snapshot pulling status
//        if (hasSnapshot && !isPullingSnapshot()) {
//            // We need to pull the snapshot
//            startSnapshotPullingThread(std::move(req->get_snapshot_uri()));
//        }
//        if (isPullingSnapshot()) {
//            CHECK_NE(oldRole, Role::LEADER);
//            resp.set_pulling_snapshot(true);
//            if (!FLAGS_accept_log_append_during_pulling) {
//                VLOG(2) << idStr_
//                        << "Pulling the snapshot and not allowed to accept"
//                           " the LogAppend Requests";
//                resp.set_error_code(cpp2::ErrorCode::E_PULLING_SNAPSHOT);
//                return;
//            }
//        }

        // Check the last log
        if (req.get_previous_log_term() == lastLogTerm_) {
            if (req.get_previous_log_id() == lastLogId_) {
                // Perfect! This should be the most common case.
                // Just need to append the new logs
            } else if (req.get_previous_log_id() < lastLogId_) {
                // Local has some extra logs, which need to be rolled back
                wal_->rollbackToLog(req.get_previous_log_id());
            } else {
                // Local is missing some logs, need to catch up
                VLOG(2) << idStr_ << "Local is missing logs from id "
                        << lastLogId_ << ". Need to catch up";
                resp.set_error_code(cpp2::ErrorCode::E_LOG_GAP);
                return;
            }
        } else {
            // req->get_previous_log_term() > lastLogTerm_
            // Need to rollback to the last committed log
            VLOG(2) << idStr_ << "Local's last log term is too old."
                                 " Need to catch up";
            CHECK_LT(committedLogId_, req.get_previous_log_id());
            wal_->rollbackToLog(committedLogId_);
            resp.set_last_log_id(committedLogId_);
            resp.set_error_code(cpp2::ErrorCode::E_LOG_GAP);
            return;
        }

        if (!isHeartbeat) {
            // Append new logs
            size_t numLogs = req.get_log_str_list()->size();
            LogStrListIterator iter(req.get_first_log_id(),
                                    req.get_log_term(),
                                    *(req.get_log_str_list()));
            if (wal_->appendLogs(iter)) {
                prevLogId_ = lastLogId_ =
                    req.get_first_log_id() + numLogs - 1;
                prevLogTerm_ = lastLogTerm_ = req.get_log_term();
                resp.set_last_log_id(lastLogId_);
            } else {
                LOG(ERROR) << idStr_ << "Failed to append logs to WAL";
                resp.set_error_code(cpp2::ErrorCode::E_WAL_FAIL);
                return;
            }
        }

        if (req.get_committed_log_id() > committedLogId_) {
            // Commit some logs
            if (commitLogs(
                    wal_->iterator(committedLogId_ + 1,
                                   req.get_committed_log_id()))) {
                committedLogId_ = req.get_committed_log_id();
                resp.set_committed_log_id(committedLogId_);
            } else {
                LOG(ERROR) << idStr_ << "Failed to commit logs ["
                           << committedLogId_ + 1 << ", "
                           << req.get_committed_log_id()
                           << "]";
                resp.set_error_code(cpp2::ErrorCode::E_WAL_FAIL);
                return;
            }
        }

        resp.set_error_code(cpp2::ErrorCode::SUCCEEDED);
    }

    if (oldRole == Role::LEADER) {
        // Need to invoke onLostLeadership callback
        VLOG(2) << idStr_ << "Was a leader, need to do some clean-up";
        lostLeadershipFuture_ = workers_->addTask(
            [oldTerm, self = shared_from_this()] {
                self->onLostLeadership(oldTerm);
            });
    }
}


cpp2::ErrorCode RaftPart::verifyLeader(const cpp2::AppendLogRequest& req,
                                       Role currRole) {
    VLOG(2) << idStr_ << "The current role is " << roleStr(currRole);

    switch (currRole) {
        case Role::FOLLOWER: {
            if (req.get_term() == term_ &&
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
    if (req.get_term() <= term_) {
        LOG(ERROR) << idStr_ << "The local term is " << term_
                   << ". The remote term is not newer";
        return cpp2::ErrorCode::E_TERM_OUT_OF_DATE;
    }

    // Make sure the remote term of the previous log is not older
    if (req.get_previous_log_term() < lastLogTerm_) {
        LOG(ERROR) << idStr_ << "The local term for the last log is "
                   << lastLogTerm_ << ". The remote one is older";
        return cpp2::ErrorCode::E_LAST_LOG_TERM_TOO_OLD;
    }

    // Ok, no reason to refuse, just follow the leader
    VLOG(2) << idStr_ << "The current role is " << roleStr(currRole)
            << ". Will follow the new leader "
            << network::NetworkUtils::intToIPv4(req.get_leader_ip())
            << ":" << req.get_leader_port()
            << " [Term: " << req.get_term() << "]";

    role_ = Role::FOLLOWER;
    leader_ = std::make_pair(req.get_leader_ip(),
                             req.get_leader_port());
    term_ = req.get_term();

    return cpp2::ErrorCode::SUCCEEDED;
}


folly::Future<RaftPart::AppendLogResult> RaftPart::sendHeartbeat() {
    using namespace folly;

    VLOG(2) << idStr_ << "Sending heartbeat to all other hosts";

    TermID term;
    auto res = canAppendLogs(term);
    if (res != AppendLogResult::SUCCEEDED) {
        LOG(ERROR) << idStr_
                   << "Cannot send heartbeat, clean up the buffer";
        return res;
    } else {
        std::lock_guard<std::mutex> g(appendLogLock_);

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
    }

    // Send heartbeat to all followers
    LogID prevLogId = 0;
    TermID prevLogTerm = 0;
    LogID committed = 0;
    {
        std::lock_guard<std::mutex> g(raftLock_);
        term = term_;
        prevLogId = prevLogId_;
        prevLogTerm = prevLogTerm_;
        committed = committedLogId_;
    }

    auto eb = ioThreadPool_->getEventBase();

    using PeerHostEntry = typename decltype(peerHosts_)::value_type;
    return collectNSucceeded(
        gen::from(peerHosts_)
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
                                                   0,
                                                   committed,
                                                   prevLogTerm,
                                                   prevLogId);
                });
        })
        | gen::as<std::vector>(),
        // Number of succeeded required
        2,
        // Result evaluator
        [](cpp2::AppendLogResponse& resp) {
            return resp.get_error_code() == cpp2::ErrorCode::SUCCEEDED;
        })
        .then([=, self = shared_from_this()] (
                folly::Try<AppendLogResponses>&& result)
                    -> folly::Future<AppendLogResult> {
            VLOG(2) << self->idStr_ << "Done with heartbeats";
            CHECK(!result.hasException());
            return AppendLogResult::SUCCEEDED;
        });

    VLOG(2) << idStr_ << "Done sending the heartbeat";
}

}  // namespace raftex
}  // namespace vesoft

