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
#include "kvstore/raftex/LogStrListIterator.h"
#include "kvstore/raftex/Host.h"
#include "time/WallClock.h"
#include "base/SlowOpTracker.h"


DEFINE_uint32(raft_heartbeat_interval_secs, 5,
             "Seconds between each heartbeat");

DEFINE_uint64(raft_snapshot_timeout, 60 * 5, "Max seconds between two snapshot requests");

DEFINE_uint32(max_batch_size, 256, "The max number of logs in a batch");

DEFINE_int32(wal_ttl, 86400, "Default wal ttl");
DEFINE_int64(wal_file_size, 16 * 1024 * 1024, "Default wal file size");
DEFINE_int32(wal_buffer_size, 8 * 1024 * 1024, "Default wal buffer size");
DEFINE_int32(wal_buffer_num, 2, "Default wal buffer number");


namespace nebula {
namespace raftex {

using nebula::network::NetworkUtils;
using nebula::thrift::ThriftClientManager;
using nebula::wal::FileBasedWal;
using nebula::wal::FileBasedWalPolicy;

class AppendLogsIterator final : public LogIterator {
public:
    AppendLogsIterator(LogID firstLogId,
                       TermID termId,
                       RaftPart::LogCache logs,
                       folly::Function<std::string(AtomicOp op)> opCB)
            : firstLogId_(firstLogId)
            , termId_(termId)
            , logId_(firstLogId)
            , logs_(std::move(logs))
            , opCB_(std::move(opCB)) {
        leadByAtomicOp_ = processAtomicOp();
        valid_ = idx_ < logs_.size();
        hasNonAtomicOpLogs_ = !leadByAtomicOp_ && valid_;
        if (valid_) {
            currLogType_ = lastLogType_ = logType();
        }
    }

    AppendLogsIterator(const AppendLogsIterator&) = delete;
    AppendLogsIterator(AppendLogsIterator&&) = default;

    AppendLogsIterator& operator=(const AppendLogsIterator&) = delete;
    AppendLogsIterator& operator=(AppendLogsIterator&&) = default;

    bool leadByAtomicOp() const {
        return leadByAtomicOp_;
    }

    bool hasNonAtomicOpLogs() const {
        return hasNonAtomicOpLogs_;
    }

    LogID firstLogId() const {
        return firstLogId_;
    }

    // Return true if the current log is a AtomicOp, otherwise return false
    bool processAtomicOp() {
        while (idx_ < logs_.size()) {
            auto& tup = logs_.at(idx_);
            auto logType = std::get<1>(tup);
            if (logType != LogType::ATOMIC_OP) {
                // Not a AtomicOp
                return false;
            }

            // Process AtomicOp log
            CHECK(!!opCB_);
            opResult_ = opCB_(std::move(std::get<3>(tup)));
            if (opResult_.size() > 0) {
                // AtomicOp Succeeded
                return true;
            } else {
                // AtomicOp failed, move to the next log, but do not increment the logId_
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
            valid_ = currLogType_ != LogType::ATOMIC_OP;
            if (valid_) {
                hasNonAtomicOpLogs_ = true;
            }
            valid_ = valid_ && lastLogType_ != LogType::COMMAND;
            lastLogType_ = currLogType_;
        } else {
            valid_ = false;
        }
        return *this;
    }

    // The iterator becomes invalid when exhausting the logs
    // **OR** running into a AtomicOp log
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
        if (currLogType_ == LogType::ATOMIC_OP) {
            return opResult_;
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
            leadByAtomicOp_ = processAtomicOp();
            valid_ = idx_ < logs_.size();
            hasNonAtomicOpLogs_ = !leadByAtomicOp_ && valid_;
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
    bool leadByAtomicOp_{false};
    bool hasNonAtomicOpLogs_{false};
    bool valid_{true};
    LogType lastLogType_{LogType::NORMAL};
    LogType currLogType_{LogType::NORMAL};
    std::string opResult_;
    LogID firstLogId_;
    TermID termId_;
    LogID logId_;
    RaftPart::LogCache logs_;
    folly::Function<std::string(AtomicOp op)> opCB_;
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
                   std::shared_ptr<folly::IOThreadPoolExecutor> pool,
                   std::shared_ptr<thread::GenericThreadPool> workers,
                   std::shared_ptr<folly::Executor> executor,
                   std::shared_ptr<SnapshotManager> snapshotMan)
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
        , bgWorkers_{workers}
        , executor_(executor)
        , snapshot_(snapshotMan)
        , weight_(1) {
    FileBasedWalPolicy policy;
    policy.ttl = FLAGS_wal_ttl;
    policy.fileSize = FLAGS_wal_file_size;
    policy.bufferSize = FLAGS_wal_buffer_size;
    policy.numBuffers = FLAGS_wal_buffer_num;
    wal_ = FileBasedWal::getWal(walRoot,
                                idStr_,
                                policy,
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
    lastLogTerm_ = wal_->lastLogTerm();
    logs_.reserve(FLAGS_max_batch_size);
    CHECK(!!executor_) << idStr_ << "Should not be nullptr";
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

    auto logIdAndTerm = lastCommittedLogId();
    committedLogId_ = logIdAndTerm.first;
    term_ = proposedTerm_ = logIdAndTerm.second;

    if (lastLogId_ < committedLogId_) {
        lastLogId_ = committedLogId_;
        lastLogTerm_ = term_;
        wal_->reset();
    }

    // Start all peer hosts
    for (auto& addr : peers) {
        LOG(INFO) << idStr_ << "Add peer " << addr;
        auto hostPtr = std::make_shared<Host>(addr, shared_from_this());
        hosts_.emplace_back(hostPtr);
    }

    // Change the status
    status_ = Status::RUNNING;
    if (asLearner) {
        role_ = Role::LEARNER;
    }
    startTimeMs_ = time::WallClock::fastNowInMilliSec();
    // Set up a leader election task
    size_t delayMS = 100 + folly::Random::rand32(900);
    bgWorkers_->addDelayTask(delayMS, [self = shared_from_this()] {
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
        PLOG_EVERY_N(ERROR, 100) << idStr_ << "The partition is not a leader";
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

void RaftPart::preProcessTransLeader(const HostAddr& target) {
    CHECK(!raftLock_.try_lock());
    LOG(INFO) << idStr_ << "Pre process transfer leader to " << target;
    switch (role_) {
        case Role::FOLLOWER: {
            if (target != addr_ && target != HostAddr(0, 0)) {
                LOG(INFO) << idStr_ << "I am follower, just wait for the new leader.";
            } else {
                LOG(INFO) << idStr_ << "I will be the new leader, trigger leader election now!";
                bgWorkers_->addTask([self = shared_from_this()] {
                    {
                        std::unique_lock<std::mutex> lck(self->raftLock_);
                        self->role_ = Role::CANDIDATE;
                        self->leader_ = HostAddr(0, 0);
                    }
                    self->leaderElection();
                });
            }
            break;
        }
        default: {
            LOG(INFO) << idStr_ << "My role is " << roleStr(role_)
                      << ", so do nothing when pre process transfer leader";
            break;
        }
    }
}

void RaftPart::commitTransLeader(const HostAddr& target) {
    CHECK(!raftLock_.try_lock());
    LOG(INFO) << idStr_ << "Commit transfer leader to " << target;
    switch (role_) {
        case Role::LEADER: {
            if (target != addr_ && !hosts_.empty()) {
                auto iter = std::find_if(hosts_.begin(), hosts_.end(), [] (const auto& h) {
                    return !h->isLearner();
                });
                if (iter != hosts_.end()) {
                    lastMsgRecvDur_.reset();
                    role_ = Role::FOLLOWER;
                    leader_ = HostAddr(0, 0);
                    LOG(INFO) << idStr_ << "Give up my leadership!";
                }
            } else {
                LOG(INFO) << idStr_ << "I am already the leader!";
            }
            break;
        }
        case Role::FOLLOWER:
        case Role::CANDIDATE: {
            LOG(INFO) << idStr_ << "I am " << roleStr(role_) << ", just wait for the new leader!";
            break;
        }
        case Role::LEARNER: {
            LOG(INFO) << idStr_ << "I am learner, not in the raft group, skip the log";
            break;
        }
    }
}

void RaftPart::updateQuorum() {
    CHECK(!raftLock_.try_lock());
    int32_t total = 0;
    for (auto& h : hosts_) {
        if (!h->isLearner()) {
            total++;
        }
    }
    quorum_ = (total + 1) / 2;
}

void RaftPart::addPeer(const HostAddr& peer) {
    CHECK(!raftLock_.try_lock());
    if (peer == addr_) {
        if (role_ == Role::LEARNER) {
            LOG(INFO) << idStr_ << "I am learner, promote myself to be follower";
            role_ = Role::FOLLOWER;
            updateQuorum();
        } else {
            LOG(INFO) << idStr_ << "I am already in the raft group!";
        }
        return;
    }
    auto it = std::find_if(hosts_.begin(), hosts_.end(), [&peer] (const auto& h) {
                return h->address() == peer;
            });
    if (it == hosts_.end()) {
        hosts_.emplace_back(std::make_shared<Host>(peer, shared_from_this()));
        updateQuorum();
        LOG(INFO) << idStr_ << "Add peer " << peer;
    } else {
        if ((*it)->isLearner()) {
            LOG(INFO) << idStr_ << "The host " << peer
                      << " has been existed as learner, promote it!";
            (*it)->setLearner(false);
            updateQuorum();
        } else {
            LOG(INFO) << idStr_ << "The host " << peer << " has been existed as follower!";
        }
    }
}

void RaftPart::removePeer(const HostAddr& peer) {
    CHECK(!raftLock_.try_lock());
    if (peer == addr_) {
        //    status_ = Status::STOPPED;
        LOG(INFO) << idStr_ << "Remove myself from the raft group.";
        return;
    }
    auto it = std::find_if(hosts_.begin(), hosts_.end(), [&peer] (const auto& h) {
                  return h->address() == peer;
              });
    if (it == hosts_.end()) {
        LOG(INFO) << idStr_ << "The peer " << peer << " not exist!";
    } else {
        if ((*it)->isLearner()) {
            LOG(INFO) << idStr_ << "The peer is learner, remove it directly!";
            hosts_.erase(it);
            return;
        }
        hosts_.erase(it);
        updateQuorum();
        LOG(INFO) << idStr_ << "Remove peer " << peer;
    }
}

void RaftPart::preProcessRemovePeer(const HostAddr& peer) {
    CHECK(!raftLock_.try_lock());
    if (role_ == Role::LEADER) {
        LOG(INFO) << idStr_ << "I am leader, skip remove peer in preProcessLog";
        return;
    }
    removePeer(peer);
}

void RaftPart::commitRemovePeer(const HostAddr& peer) {
    CHECK(!raftLock_.try_lock());
    if (role_ == Role::FOLLOWER || role_ == Role::LEARNER) {
        LOG(INFO) << idStr_ << "I am " << roleStr(role_)
                  << ", skip remove peer in commit";
        return;
    }
    CHECK(Role::LEADER == role_);
    removePeer(peer);
}

folly::Future<AppendLogResult> RaftPart::appendAsync(ClusterID source,
                                                     std::string log) {
    if (source < 0) {
        source = clusterId_;
    }
    return appendLogAsync(source, LogType::NORMAL, std::move(log));
}


folly::Future<AppendLogResult> RaftPart::atomicOpAsync(AtomicOp op) {
    return appendLogAsync(clusterId_, LogType::ATOMIC_OP, "", std::move(op));
}

folly::Future<AppendLogResult> RaftPart::sendCommandAsync(std::string log) {
    return appendLogAsync(clusterId_, LogType::COMMAND, std::move(log));
}

folly::Future<AppendLogResult> RaftPart::appendLogAsync(ClusterID source,
                                                        LogType logType,
                                                        std::string log,
                                                        AtomicOp op) {
    if (blocking_ && (logType == LogType::NORMAL || logType == LogType::ATOMIC_OP)) {
        return AppendLogResult::E_WRITE_BLOCKING;
    }

    LogCache swappedOutLogs;
    auto retFuture = folly::Future<AppendLogResult>::makeEmpty();

    if (bufferOverFlow_) {
        PLOG_EVERY_N(WARNING, 30) << idStr_
                     << "The appendLog buffer is full."
                        " Please slow down the log appending rate."
                     << "replicatingLogs_ :" << replicatingLogs_;
        return AppendLogResult::E_BUFFER_OVERFLOW;
    }
    {
        std::lock_guard<std::mutex> lck(logsLock_);

        VLOG(2) << idStr_ << "Checking whether buffer overflow";

        if (logs_.size() >= FLAGS_max_batch_size) {
            // Buffer is full
            LOG(WARNING) << idStr_
                         << "The appendLog buffer is full."
                            " Please slow down the log appending rate."
                         << "replicatingLogs_ :" << replicatingLogs_;
            bufferOverFlow_ = true;
            return AppendLogResult::E_BUFFER_OVERFLOW;
        }

        VLOG(2) << idStr_ << "Appending logs to the buffer";

        // Append new logs to the buffer
        DCHECK_GE(source, 0);
        logs_.emplace_back(source, logType, std::move(log), std::move(op));
        switch (logType) {
            case LogType::ATOMIC_OP:
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
            bufferOverFlow_ = false;
        } else {
            VLOG(2) << idStr_
                    << "Another AppendLogs request is ongoing,"
                       " just return";
            return retFuture;
        }
    }

    LogID firstId = 0;
    TermID termId = 0;
    AppendLogResult res;
    {
        std::lock_guard<std::mutex> g(raftLock_);
        res = canAppendLogs();
        if (res == AppendLogResult::SUCCEEDED) {
            firstId = lastLogId_ + 1;
            termId = term_;
        }
    }

    if (!checkAppendLogResult(res)) {
        // Mosy likely failed because the parttion is not leader
        PLOG_EVERY_N(ERROR, 100) << idStr_ << "Cannot append logs, clean the buffer";
        return res;
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
        [this] (AtomicOp opCB) -> std::string {
            CHECK(opCB != nullptr);
            auto opRet = opCB();
            if (opRet.empty()) {
                // Failed
                sendingPromise_.setOneSingleValue(AppendLogResult::E_ATOMIC_OP_FAILURE);
            }
            return opRet;
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
        LOG(ERROR) << idStr_ << "Only happend when Atomic op failed";
        replicatingLogs_ = false;
        return;
    }
    AppendLogResult res = AppendLogResult::SUCCEEDED;
    do {
        std::lock_guard<std::mutex> g(raftLock_);
        if (status_ != Status::RUNNING) {
            // The partition is not running
            VLOG(2) << idStr_ << "The partition is stopped";
            res = AppendLogResult::E_STOPPED;
            break;
        }

        if (role_ != Role::LEADER) {
            // Is not a leader any more
            VLOG(2) << idStr_ << "The leader has changed";
            res = AppendLogResult::E_NOT_A_LEADER;
            break;
        }
        if (term_ != termId) {
            VLOG(2) << idStr_ << "Term has been updated, origin "
                    << termId << ", new " << term_;
            res = AppendLogResult::E_TERM_OUT_OF_DATE;
            break;
        }
        currTerm = term_;
        prevLogId = lastLogId_;
        prevLogTerm = lastLogTerm_;
        committed = committedLogId_;
        // Step 1: Write WAL
        SlowOpTracker tracker;
        if (!wal_->appendLogs(iter)) {
            LOG(ERROR) << idStr_ << "Failed to write into WAL";
            res = AppendLogResult::E_WAL_FAILURE;
            break;
        }
        lastId = wal_->lastLogId();
        if (tracker.slow()) {
            tracker.output(idStr_, folly::stringPrintf("Write WAL, total %ld",
                                                       lastId - prevLogId + 1));
        }
        VLOG(2) << idStr_ << "Succeeded writing logs ["
                << iter.firstLogId() << ", " << lastId << "] to WAL";
    } while (false);

    if (!checkAppendLogResult(res)) {
        LOG(ERROR) << idStr_ << "Failed append logs";
        return;
    }
    // Step 2: Replicate to followers
    auto* eb = ioThreadPool_->getEventBase();
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
    AppendLogResult res = AppendLogResult::SUCCEEDED;
    do {
        std::lock_guard<std::mutex> g(raftLock_);

        if (status_ != Status::RUNNING) {
            // The partition is not running
            VLOG(2) << idStr_ << "The partition is stopped";
            res = AppendLogResult::E_STOPPED;
            break;
        }

        if (role_ != Role::LEADER) {
            // Is not a leader any more
            VLOG(2) << idStr_ << "The leader has changed";
            res = AppendLogResult::E_NOT_A_LEADER;
            break;
        }

        hosts = hosts_;
    } while (false);

    if (!checkAppendLogResult(res)) {
        LOG(ERROR) << idStr_ << "Replicate logs failed";
        return;
    }

    VLOG(2) << idStr_ << "About to replicate logs to all peer hosts";

    SlowOpTracker tracker;
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
            return via(eb, [=] () -> Future<cpp2::AppendLogResponse> {
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
        .via(executor_.get())
            .then([self = shared_from_this(),
                   eb,
                   it = std::move(iter),
                   currTerm,
                   lastLogId,
                   committedId,
                   prevLogId,
                   prevLogTerm,
                   pHosts = std::move(hosts),
                   tracker] (folly::Try<AppendLogResponses>&& result) mutable {
            VLOG(2) << self->idStr_ << "Received enough response";
            CHECK(!result.hasException());
            if (tracker.slow()) {
                tracker.output(self->idStr_, folly::stringPrintf("Total send logs: %ld",
                                                                  lastLogId - prevLogId + 1));
            }
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
        AppendLogResult res = AppendLogResult::SUCCEEDED;
        do {
            std::lock_guard<std::mutex> g(raftLock_);
            if (status_ != Status::RUNNING) {
                LOG(INFO) << idStr_ << "The partition is stopped";
                res = AppendLogResult::E_STOPPED;
                break;
            }
            if (role_ != Role::LEADER) {
                LOG(INFO) << idStr_ << "The leader has changed";
                res = AppendLogResult::E_NOT_A_LEADER;
                break;
            }
            if (currTerm != term_) {
                LOG(INFO) << idStr_ << "The leader has changed, ABA problem.";
                res = AppendLogResult::E_TERM_OUT_OF_DATE;
                break;
            }
            lastLogId_ = lastLogId;
            lastLogTerm_ = currTerm;

            lastMsgSentDur_.reset();

            auto walIt = wal_->iterator(committedId + 1, lastLogId);
            SlowOpTracker tracker;
            // Step 3: Commit the batch
            if (commitLogs(std::move(walIt))) {
                committedLogId_ = lastLogId;
                firstLogId = lastLogId_ + 1;
            } else {
                LOG(FATAL) << idStr_ << "Failed to commit logs";
            }
            if (tracker.slow()) {
                tracker.output(idStr_, folly::stringPrintf("Total commit: %ld",
                                                           committedLogId_ - committedId));
            }
            VLOG(2) << idStr_ << "Leader succeeded in committing the logs "
                              << committedId + 1 << " to " << lastLogId;
        } while (false);

        if (!checkAppendLogResult(res)) {
            LOG(ERROR) << idStr_ << "processAppendLogResponses failed!";
            return;
        }
        // Step 4: Fulfill the promise
        if (iter.hasNonAtomicOpLogs()) {
            sendingPromise_.setOneSharedValue(AppendLogResult::SUCCEEDED);
        }
        if (iter.leadByAtomicOp()) {
            sendingPromise_.setOneSingleValue(AppendLogResult::SUCCEEDED);
        }
        // Step 5: Check whether need to continue
        // the log replication
        {
            std::lock_guard<std::mutex> lck(logsLock_);
            CHECK(replicatingLogs_);
            // Continue to process the original AppendLogsIterator if necessary
            iter.resume();
            // If no more valid logs to be replicated in iter, create a new one if we have new log
            if (iter.empty()) {
                VLOG(2) << idStr_ << "logs size " << logs_.size();
                if (logs_.size() > 0) {
                    // continue to replicate the logs
                    sendingPromise_ = std::move(cachingPromise_);
                    cachingPromise_.reset();
                    iter = AppendLogsIterator(
                        firstLogId,
                        currTerm,
                        std::move(logs_),
                        [this] (AtomicOp op) -> std::string {
                            auto opRet = op();
                            if (opRet.empty()) {
                                // Failed
                                sendingPromise_.setOneSingleValue(
                                    AppendLogResult::E_ATOMIC_OP_FAILURE);
                            }
                            return opRet;
                        });
                    logs_.clear();
                    bufferOverFlow_ = false;
                }
                // Reset replicatingLogs_ one of the following is true:
                // 1. old iter is empty && logs_.size() == 0
                // 2. old iter is empty && logs_.size() > 0, but all logs in new iter is atomic op,
                //    and all of them failed, which would make iter is empty again
                if (iter.empty()) {
                    replicatingLogs_ = false;
                    VLOG(2) << idStr_ << "No more log to be replicated";
                    return;
                }
            }
        }
        this->appendLogsInternal(std::move(iter), currTerm);
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
           lastMsgSentDur_.elapsedInSec() >= FLAGS_raft_heartbeat_interval_secs * 2 / 5;
}


bool RaftPart::needToStartElection() {
    std::lock_guard<std::mutex> g(raftLock_);
    if (status_ == Status::RUNNING &&
        role_ == Role::FOLLOWER &&
        (lastMsgRecvDur_.elapsedInMSec() >= weight_ * FLAGS_raft_heartbeat_interval_secs * 1000 ||
         term_ == 0)) {
        LOG(INFO) << idStr_ << "Start leader election, reason: lastMsgDur "
                  << lastMsgRecvDur_.elapsedInMSec()
                  << ", term " << term_;
        role_ = Role::CANDIDATE;
        leader_ = HostAddr(0, 0);
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
    LOG(INFO) << idStr_ << "Sending out an election request "
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
            [hosts, this](size_t idx, cpp2::AskForVoteResponse& resp) {
                if (resp.get_error_code() == cpp2::ErrorCode::E_LOG_STALE) {
                    LOG(INFO) << idStr_ << "My last log id is less than " << hosts[idx]->address()
                              << ", double my election interval.";
                    uint64_t curWeight = weight_.load();
                    weight_.store(curWeight * 2);
                }
                return resp.get_error_code() == cpp2::ErrorCode::SUCCEEDED
                        && !hosts[idx]->isLearner();
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
                    bgWorkers_->addTask([self = shared_from_this(),
                                       term = voteReq.get_term()] {
                        self->onElected(term);
                    });
                }
            }
            weight_ = 1;
            sendHeartbeat();
            return true;
        }
        case Role::FOLLOWER: {
            // Someone was elected
            LOG(INFO) << idStr_ << "Someone else was elected";
            return true;
        }
        case Role::CANDIDATE: {
            // No one has been elected
            LOG(INFO) << idStr_
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
    size_t delay = FLAGS_raft_heartbeat_interval_secs * 1000 / 3;
    if (needToStartElection()) {
        if (leaderElection()) {
            VLOG(2) << idStr_ << "Stop the election";
        } else {
            // No leader has been elected, need to continue
            // (After sleeping a random period betwen [500ms, 2s])
            VLOG(2) << idStr_ << "Wait for a while and continue the leader election";
            delay = (folly::Random::rand32(1500) + 500) * weight_;
        }
    } else if (needToSendHeartbeat()) {
        VLOG(2) << idStr_ << "Need to send heartbeat";
        sendHeartbeat();
    }
    if (needToCleanupSnapshot()) {
        LOG(INFO) << idStr_ << "Clean up the snapshot";
        cleanupSnapshot();
    }
    if (needToCleanWal()) {
        wal_->cleanWAL(FLAGS_wal_ttl);
    }
    {
        std::lock_guard<std::mutex> g(raftLock_);
        if (status_ == Status::RUNNING || status_ == Status::WAITING_SNAPSHOT) {
            VLOG(3) << idStr_ << "Schedule new task";
            bgWorkers_->addDelayTask(
                delay,
                [self = shared_from_this()] {
                    self->statusPolling();
                });
        }
    }
}

bool RaftPart::needToCleanupSnapshot() {
    std::lock_guard<std::mutex> g(raftLock_);
    return status_ == Status::WAITING_SNAPSHOT &&
           role_ != Role::LEADER &&
           lastSnapshotRecvDur_.elapsedInSec() >= FLAGS_raft_snapshot_timeout;
}

void RaftPart::cleanupSnapshot() {
    LOG(INFO) << idStr_ << "Clean up the snapshot";
    std::lock_guard<std::mutex> g(raftLock_);
    reset();
    status_ = Status::RUNNING;
}

bool RaftPart::needToCleanWal() {
    std::lock_guard<std::mutex> g(raftLock_);
    if (status_ == Status::WAITING_SNAPSHOT) {
        return false;
    }
    for (auto& host : hosts_) {
        if (host->sendingSnapshot_) {
            return false;
        }
    }
    return true;
}

void RaftPart::processAskForVoteRequest(
        const cpp2::AskForVoteRequest& req,
        cpp2::AskForVoteResponse& resp) {
    LOG(INFO) << idStr_
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

    LOG(INFO) << idStr_ << "The partition currently is a "
              << roleStr(role_);
    if (role_ == Role::LEARNER) {
        resp.set_error_code(cpp2::ErrorCode::E_BAD_ROLE);
        return;
    }

    // Check term id
    auto term = role_ == Role::CANDIDATE ? proposedTerm_ : term_;
    if (req.get_term() <= term) {
        LOG(INFO) << idStr_
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
        LOG(INFO) << idStr_
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
            LOG(INFO) << idStr_
                      << "The partition's last log id is " << lastLogId_
                      << ". The candidate's last log id is smaller"
                         ", so it will be rejected";
            resp.set_error_code(cpp2::ErrorCode::E_LOG_STALE);
            return;
        }
    }

    auto candidate = HostAddr(req.get_candidate_ip(), req.get_candidate_port());
    auto hosts = followers();
    auto it = std::find_if(hosts.begin(), hosts.end(), [&candidate] (const auto& h){
                return h->address() == candidate;
            });
    if (it == hosts.end()) {
        LOG(INFO) << idStr_ << "The candidate " << candidate << " is not my peers";
        resp.set_error_code(cpp2::ErrorCode::E_WRONG_LEADER);
        return;
    }
    // Ok, no reason to refuse, we will vote for the candidate
    LOG(INFO) << idStr_ << "The partition will vote for the candidate";
    resp.set_error_code(cpp2::ErrorCode::SUCCEEDED);

    Role oldRole = role_;
    TermID oldTerm = term_;
    role_ = Role::FOLLOWER;
    term_ = proposedTerm_ = req.get_term();
    leader_ = std::make_pair(req.get_candidate_ip(),
                             req.get_candidate_port());

    // Reset the last message time
    lastMsgRecvDur_.reset();
    weight_ = 1;

    // If the partition used to be a leader, need to fire the callback
    if (oldRole == Role::LEADER) {
        LOG(INFO) << idStr_ << "Was a leader, need to do some clean-up";
        if (wal_->lastLogId() > lastLogId_) {
            LOG(INFO) << idStr_ << "There is one log " << wal_->lastLogId()
                      << " i did not commit when i was leader, rollback to " << lastLogId_;
            wal_->rollbackToLog(lastLogId_);
        }
        // Need to invoke the onLostLeadership callback
        bgWorkers_->addTask(
            [self = shared_from_this(), oldTerm] {
                self->onLostLeadership(oldTerm);
            });
    }

    LOG(INFO) << idStr_ << "I was " << roleStr(oldRole)
              << ", discover the new leader " << leader_;
    bgWorkers_->addTask([self = shared_from_this()] {
        self->onDiscoverNewLeader(self->leader_);
    });
    return;
}


void RaftPart::processAppendLogRequest(
        const cpp2::AppendLogRequest& req,
        cpp2::AppendLogResponse& resp) {
    VLOG(2) << idStr_
            << "Received logAppend "
            << ": GraphSpaceId = " << req.get_space()
            << ", partition = " << req.get_part()
            << ", current_term = " << req.get_current_term()
            << ", lastLogId = " << req.get_last_log_id()
            << ", committedLogId = " << req.get_committed_log_id()
            << ", leaderIp = " << req.get_leader_ip()
            << ", leaderPort = " << req.get_leader_port()
            << ", lastLogIdSent = " << req.get_last_log_id_sent()
            << ", lastLogTermSent = " << req.get_last_log_term_sent()
            << folly::stringPrintf(
                    ", num_logs = %ld, logTerm = %ld",
                    req.get_log_str_list().size(),
                    req.get_log_term())
            << ", sendingSnapshot = " << req.get_sending_snapshot()
            << ", local lastLogId = " << lastLogId_
            << ", local committedLogId = " << committedLogId_;

    std::lock_guard<std::mutex> g(raftLock_);

    resp.set_current_term(term_);
    resp.set_leader_ip(leader_.first);
    resp.set_leader_port(leader_.second);
    resp.set_committed_log_id(committedLogId_);
    resp.set_last_log_id(lastLogId_ < committedLogId_ ? committedLogId_ : lastLogId_);
    resp.set_last_log_term(lastLogTerm_);

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
    // Check leadership
    cpp2::ErrorCode err = verifyLeader(req);
    if (err != cpp2::ErrorCode::SUCCEEDED) {
        // Wrong leadership
        VLOG(2) << idStr_ << "Will not follow the leader";
        resp.set_error_code(err);
        return;
    }

    // Reset the timeout timer
    lastMsgRecvDur_.reset();

    if (req.get_sending_snapshot() && status_ != Status::WAITING_SNAPSHOT) {
        LOG(INFO) << idStr_ << "Begin to wait for the snapshot"
                  << " " << req.get_committed_log_id();
        reset();
        status_ = Status::WAITING_SNAPSHOT;
        resp.set_error_code(cpp2::ErrorCode::E_WAITING_SNAPSHOT);
        return;
    }

    if (UNLIKELY(status_ == Status::WAITING_SNAPSHOT)) {
        VLOG(2) << idStr_
                << "The part is receiving snapshot,"
                << "so just accept the new wals, but don't commit them."
                << "last_log_id_sent " << req.get_last_log_id_sent()
                << ", total log number " << req.get_log_str_list().size();
        if (lastLogId_ > 0 && req.get_last_log_id_sent() > lastLogId_) {
            // There is a gap
            LOG(INFO) << idStr_ << "Local is missing logs from id "
                      << lastLogId_ << ". Need to catch up";
            resp.set_error_code(cpp2::ErrorCode::E_LOG_GAP);
            return;
        }
        // TODO(heng): if we have 3 node, one is leader, one is wait snapshot and return success,
        // the other is follower, but leader replica log to follow failed,
        // How to deal with leader crash? At this time, no leader will be elected.
        size_t numLogs = req.get_log_str_list().size();
        LogID firstId = req.get_last_log_id_sent() + 1;

        VLOG(2) << idStr_ << "Writing log [" << firstId
                << ", " << firstId + numLogs - 1 << "] to WAL";
        LogStrListIterator iter(firstId,
                                req.get_log_term(),
                                req.get_log_str_list());
        if (wal_->appendLogs(iter)) {
            // When leader has been sending a snapshot already, sometimes it would send a request
            // with empty log list, and lastLogId in wal may be 0 because of reset.
            if (numLogs != 0) {
                CHECK_EQ(firstId + numLogs - 1, wal_->lastLogId());
            }
            lastLogId_ = wal_->lastLogId();
            lastLogTerm_ = wal_->lastLogTerm();
            resp.set_last_log_id(lastLogId_);
            resp.set_last_log_term(lastLogTerm_);
            resp.set_error_code(cpp2::ErrorCode::SUCCEEDED);
        } else {
            LOG(ERROR) << idStr_ << "Failed to append logs to WAL";
            resp.set_error_code(cpp2::ErrorCode::E_WAL_FAIL);
        }
        return;
    }

    if (req.get_last_log_id_sent() < committedLogId_) {
        LOG(INFO) << idStr_ << "Stale log! The log " << req.get_last_log_id_sent()
                  << " i had committed yet. My committedLogId is "
                  << committedLogId_;
        resp.set_error_code(cpp2::ErrorCode::E_LOG_STALE);
        return;
    }
    if (lastLogTerm_ > 0 && req.get_last_log_term_sent() != lastLogTerm_) {
        LOG(INFO) << idStr_ << "The local last log term is " << lastLogTerm_
                << ", which is different from the leader's prevLogTerm "
                << req.get_last_log_term_sent()
                << ". So need to rollback to last committedLogId_ " << committedLogId_;
        if (wal_->rollbackToLog(committedLogId_)) {
            lastLogId_ = wal_->lastLogId();
            lastLogTerm_ = wal_->lastLogTerm();
            resp.set_last_log_id(lastLogId_);
            resp.set_last_log_term(lastLogTerm_);
         }
         resp.set_error_code(cpp2::ErrorCode::E_LOG_GAP);
         return;
    } else if (req.get_last_log_id_sent() > lastLogId_) {
        // There is a gap
        LOG(INFO) << idStr_ << "Local is missing logs from id "
                << lastLogId_ << ". Need to catch up";
        resp.set_error_code(cpp2::ErrorCode::E_LOG_GAP);
        return;
    } else if (req.get_last_log_id_sent() < lastLogId_) {
        LOG(INFO) << idStr_ << "Stale log! Local lastLogId " << lastLogId_
                  << ", lastLogTerm " << lastLogTerm_
                  << ", lastLogIdSent " << req.get_last_log_id_sent()
                  << ", lastLogTermSent " << req.get_last_log_term_sent();
        resp.set_error_code(cpp2::ErrorCode::E_LOG_STALE);
        return;
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
            VLOG(1) << idStr_ << "Follower succeeded committing log "
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
}


cpp2::ErrorCode RaftPart::verifyLeader(
        const cpp2::AppendLogRequest& req) {
    CHECK(!raftLock_.try_lock());
    auto candidate = HostAddr(req.get_leader_ip(), req.get_leader_port());
    auto hosts = followers();
    auto it = std::find_if(hosts.begin(), hosts.end(), [&candidate] (const auto& h){
                return h->address() == candidate;
            });
    if (it == hosts.end()) {
        VLOG(2) << idStr_ << "The candidate leader " << candidate << " is not my peers";
        return cpp2::ErrorCode::E_WRONG_LEADER;
    }
    VLOG(2) << idStr_ << "The current role is " << roleStr(role_);
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
        PLOG_EVERY_N(ERROR, 100) << idStr_
                                 << "The current role is " << roleStr(role_)
                                 << ". The local term is " << term_
                                 << ". The remote term is not newer";
        return cpp2::ErrorCode::E_TERM_OUT_OF_DATE;
    }
    if (role_ == Role::FOLLOWER || role_ == Role::LEARNER) {
        if (req.get_current_term() == term_ && leader_ != std::make_pair(0, 0)) {
            LOG(ERROR) << idStr_ << "The local term is same as remote term " << term_
                       << ". But I believe leader exists.";
            return cpp2::ErrorCode::E_TERM_OUT_OF_DATE;
        }
    }

    Role oldRole = role_;
    TermID oldTerm = term_;
    // Ok, no reason to refuse, just follow the leader
    LOG(INFO) << idStr_ << "The current role is " << roleStr(role_)
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
    weight_ = 1;
    if (oldRole == Role::LEADER) {
        VLOG(2) << idStr_ << "Was a leader, need to do some clean-up";
        if (wal_->lastLogId() > lastLogId_) {
            LOG(INFO) << idStr_ << "There is one log " << wal_->lastLogId()
                      << " i did not commit when i was leader, rollback to " << lastLogId_;
            wal_->rollbackToLog(lastLogId_);
        }
        // Need to invoke onLostLeadership callback
        bgWorkers_->addTask([self = shared_from_this(), oldTerm] {
            self->onLostLeadership(oldTerm);
        });
    }

    bgWorkers_->addTask([self = shared_from_this()] {
        self->onDiscoverNewLeader(self->leader_);
    });
    return cpp2::ErrorCode::SUCCEEDED;
}

void RaftPart::processSendSnapshotRequest(const cpp2::SendSnapshotRequest& req,
                                          cpp2::SendSnapshotResponse& resp) {
    VLOG(1) << idStr_ << "Receive snapshot, total rows " << req.get_rows().size()
            << ", total count received " << req.get_total_count()
            << ", total size received " << req.get_total_size()
            << ", finished " << req.get_done();
    std::lock_guard<std::mutex> g(raftLock_);
    // Check status
    if (UNLIKELY(status_ == Status::STOPPED)) {
        LOG(ERROR) << idStr_
                  << "The part has been stopped, skip the request";
        resp.set_error_code(cpp2::ErrorCode::E_BAD_STATE);
        return;
    }
    if (UNLIKELY(status_ == Status::STARTING)) {
        LOG(ERROR) << idStr_ << "The partition is still starting";
        resp.set_error_code(cpp2::ErrorCode::E_NOT_READY);
        return;
    }
    if (UNLIKELY(role_ != Role::FOLLOWER && role_ != Role::LEARNER)) {
        LOG(ERROR) << idStr_ << "Bad role " << roleStr(role_);
        resp.set_error_code(cpp2::ErrorCode::E_BAD_STATE);
        return;
    }
    if (UNLIKELY(leader_ != HostAddr(req.get_leader_ip(), req.get_leader_port())
            || term_ != req.get_term())) {
        LOG(ERROR) << idStr_ << "Term out of date, current term " << term_
                   << ", received term " << req.get_term();
        resp.set_error_code(cpp2::ErrorCode::E_TERM_OUT_OF_DATE);
        return;
    }
    if (status_ != Status::WAITING_SNAPSHOT) {
        LOG(INFO) << idStr_ << "Begin to receive the snapshot";
        reset();
        status_ = Status::WAITING_SNAPSHOT;
    }
    lastSnapshotRecvDur_.reset();
    // TODO(heng): Maybe we should save them into one sst firstly?
    auto ret = commitSnapshot(req.get_rows(),
                              req.get_committed_log_id(),
                              req.get_committed_log_term(),
                              req.get_done());
    lastTotalCount_ += ret.first;
    lastTotalSize_ += ret.second;
    if (lastTotalCount_ != req.get_total_count()
            || lastTotalSize_ != req.get_total_size()) {
        LOG(ERROR) << idStr_ << "Bad snapshot, total rows received " << lastTotalCount_
                   << ", total rows sended " << req.get_total_count()
                   << ", total size received " << lastTotalSize_
                   << ", total size sended " << req.get_total_size();
        resp.set_error_code(cpp2::ErrorCode::E_PERSIST_SNAPSHOT_FAILED);
        return;
    }
    if (req.get_done()) {
        committedLogId_ = req.get_committed_log_id();
        if (lastLogId_ < committedLogId_) {
            lastLogId_ = committedLogId_;
            lastLogTerm_ = req.get_committed_log_term();
        }
        if (wal_->lastLogId() <= committedLogId_) {
            LOG(INFO) << "Reset invalid wal after snapshot received";
            wal_->reset();
        }
        status_ = Status::RUNNING;
        LOG(INFO) << idStr_ << "Receive all snapshot, committedLogId_ " << committedLogId_
                  << ", lastLodId " << lastLogId_ << ", lastLogTermId " << lastLogTerm_;
    }
    resp.set_error_code(cpp2::ErrorCode::SUCCEEDED);
    return;
}

folly::Future<AppendLogResult> RaftPart::sendHeartbeat() {
    VLOG(2) << idStr_ << "Send heartbeat";
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

bool RaftPart::checkAppendLogResult(AppendLogResult res) {
    if (res != AppendLogResult::SUCCEEDED) {
        {
            std::lock_guard<std::mutex> lck(logsLock_);
            logs_.clear();
            cachingPromise_.setValue(res);
            cachingPromise_.reset();
            bufferOverFlow_ = false;
        }
        sendingPromise_.setValue(res);
        replicatingLogs_ = false;
        return false;;
    }
    return true;
}

void RaftPart::reset() {
    CHECK(!raftLock_.try_lock());
    wal_->reset();
    cleanup();
    lastLogId_ = committedLogId_ = 0;
    lastTotalCount_ = 0;
    lastTotalSize_ = 0;
}

AppendLogResult RaftPart::isCatchedUp(const HostAddr& peer) {
    std::lock_guard<std::mutex> lck(raftLock_);
    LOG(INFO) << idStr_ << "Check whether I catch up";
    if (role_ != Role::LEADER) {
        LOG(INFO) << idStr_ << "I am not the leader";
        return AppendLogResult::E_NOT_A_LEADER;
    }
    if (peer == addr_) {
        LOG(INFO) << idStr_ << "I am the leader";
        return AppendLogResult::SUCCEEDED;
    }
    for (auto& host : hosts_) {
        if (host->addr_ == peer) {
            if (host->followerCommittedLogId_ < wal_->firstLogId()) {
                LOG(INFO) << idStr_ << "The committed log id of peer is "
                          << host->followerCommittedLogId_
                          << ", which is invalid or less than my first wal log id";
                return AppendLogResult::E_SENDING_SNAPSHOT;
            }
            return host->sendingSnapshot_ ? AppendLogResult::E_SENDING_SNAPSHOT
                                          : AppendLogResult::SUCCEEDED;
        }
    }
    return AppendLogResult::E_INVALID_PEER;
}

bool RaftPart::linkCurrentWAL(const char* newPath) {
    CHECK_NOTNULL(newPath);
    std::lock_guard<std::mutex> g(raftLock_);
    return wal_->linkCurrentWAL(newPath);
}

void RaftPart::checkAndResetPeers(const std::vector<HostAddr>& peers) {
    std::lock_guard<std::mutex> lck(raftLock_);
    // To avoid the iterator invalid, we use another container for it.
    decltype(hosts_) hosts = hosts_;
    for (auto& h : hosts) {
        LOG(INFO) << idStr_ << "Check host " << h->addr_;
        auto it = std::find(peers.begin(), peers.end(), h->addr_);
        if (it == peers.end()) {
            LOG(INFO) << idStr_ << "The peer " << h->addr_ << " should not exist in my peers";
            removePeer(h->addr_);
        }
    }
    for (auto& p : peers) {
        LOG(INFO) << idStr_ << "Add peer " << p << " if not exist!";
        addPeer(p);
    }
}

}  // namespace raftex
}  // namespace nebula

