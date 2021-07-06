/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/interface/gen-cpp2/RaftexServiceAsyncClient.h"
#include "common/base/CollectNSucceeded.h"
#include "common/thrift/ThriftClientManager.h"
#include "common/network/NetworkUtils.h"
#include "common/thread/NamedThread.h"
#include "common/time/WallClock.h"
#include "common/base/SlowOpTracker.h"
#include <folly/io/async/EventBaseManager.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/gen/Base.h>
#include "kvstore/wal/FileBasedWal.h"
#include "kvstore/raftex/LogStrListIterator.h"
#include "kvstore/raftex/Host.h"
#include "kvstore/raftex/RaftPart.h"
#include <thrift/lib/cpp/util/EnumUtils.h>

DEFINE_uint32(raft_heartbeat_interval_secs, 5,
             "Seconds between each heartbeat");

DEFINE_uint64(raft_snapshot_timeout, 60 * 5, "Max seconds between two snapshot requests");

DEFINE_uint32(max_batch_size, 256, "The max number of logs in a batch");

DEFINE_bool(trace_raft, false, "Enable trace one raft request");

DECLARE_int32(wal_ttl);
DECLARE_int64(wal_file_size);
DECLARE_int32(wal_buffer_size);
DECLARE_bool(wal_sync);

namespace nebula {
namespace raftex {

using nebula::network::NetworkUtils;
using nebula::thrift::ThriftClientManager;
using nebula::wal::FileBasedWal;
using nebula::wal::FileBasedWalPolicy;
using nebula::wal::FileBasedWalInfo;

using OpProcessor = folly::Function<folly::Optional<std::string>(AtomicOp op)>;

class AppendLogsIterator final : public LogIterator {
public:
    AppendLogsIterator(LogID firstLogId,
                       TermID termId,
                       RaftPart::LogCache logs,
                       OpProcessor opCB)
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
            if (opResult_.hasValue()) {
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
            CHECK(opResult_.hasValue());
            return opResult_.value();
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
    folly::Optional<std::string> opResult_;
    LogID firstLogId_;
    TermID termId_;
    LogID logId_;
    RaftPart::LogCache logs_;
    OpProcessor opCB_;
};


/********************************************************
 *
 *  Implementation of RaftPart
 *
 *******************************************************/
RaftPart::RaftPart(
           ClusterID clusterId,
           GraphSpaceID spaceId,
           PartitionID partId,
           HostAddr localAddr,
           const folly::StringPiece walRoot,
           std::shared_ptr<folly::IOThreadPoolExecutor> pool,
           std::shared_ptr<thread::GenericThreadPool> workers,
           std::shared_ptr<folly::Executor> executor,
           std::shared_ptr<SnapshotManager> snapshotMan,
           std::shared_ptr<thrift::ThriftClientManager<cpp2::RaftexServiceAsyncClient>> clientMan,
           std::shared_ptr<kvstore::DiskManager> diskMan)
        : idStr_{folly::stringPrintf("[Port: %d, Space: %d, Part: %d] ",
                                     localAddr.port, spaceId, partId)}
        , clusterId_{clusterId}
        , spaceId_{spaceId}
        , partId_{partId}
        , addr_{localAddr}
        , status_{Status::STARTING}
        , role_{Role::FOLLOWER}
        , leader_{"", 0}
        , ioThreadPool_{pool}
        , bgWorkers_{workers}
        , executor_(executor)
        , snapshot_(snapshotMan)
        , clientMan_(clientMan)
        , diskMan_(diskMan)
        , weight_(1) {
    FileBasedWalPolicy policy;
    policy.fileSize = FLAGS_wal_file_size;
    policy.bufferSize = FLAGS_wal_buffer_size;
    policy.sync = FLAGS_wal_sync;
    FileBasedWalInfo info;
    info.idStr_ = idStr_;
    info.spaceId_ = spaceId_;
    info.partId_ = partId_;
    wal_ = FileBasedWal::getWal(
        walRoot,
        std::move(info),
        std::move(policy),
        [this] (LogID logId, TermID logTermId, ClusterID logClusterId, const std::string& log) {
            return this->preProcessLog(logId, logTermId, logClusterId, log);
        },
        diskMan);
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

    lastLogId_ = wal_->lastLogId();
    lastLogTerm_ = wal_->lastLogTerm();
    term_ = proposedTerm_ = lastLogTerm_;

    // Set the quorum number
    quorum_ = (peers.size() + 1) / 2;

    auto logIdAndTerm = lastCommittedLogId();
    committedLogId_ = logIdAndTerm.first;

    if (lastLogId_ < committedLogId_) {
        LOG(INFO) << idStr_ << "Reset lastLogId " << lastLogId_
                  << " to be the committedLogId " << committedLogId_;
        lastLogId_ = committedLogId_;
        lastLogTerm_ = term_;
        wal_->reset();
    }
    LOG(INFO) << idStr_ << "There are "
                        << peers.size()
                        << " peer hosts, and total "
                        << peers.size() + 1
                        << " copies. The quorum is " << quorum_ + 1
                        << ", as learner " << asLearner
                        << ", lastLogId " << lastLogId_
                        << ", lastLogTerm " << lastLogTerm_
                        << ", committedLogId " << committedLogId_
                        << ", term " << term_;

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
    bgWorkers_->addDelayTask(delayMS, [self = shared_from_this(), startTime = startTimeMs_] {
        self->statusPolling(startTime);
    });
}


void RaftPart::stop() {
    VLOG(2) << idStr_ << "Stopping the partition";

    decltype(hosts_) hosts;
    {
        std::unique_lock<std::mutex> lck(raftLock_);
        status_ = Status::STOPPED;
        leader_ = {"", 0};
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
    DCHECK(!raftLock_.try_lock());
    if (UNLIKELY(status_ != Status::RUNNING)) {
        LOG(ERROR) << idStr_ << "The partition is not running";
        return AppendLogResult::E_STOPPED;
    }
    if (UNLIKELY(role_ != Role::LEADER)) {
        LOG_EVERY_N(WARNING, 1000) << idStr_ << "The partition is not a leader";
        return AppendLogResult::E_NOT_A_LEADER;
    }
    return AppendLogResult::SUCCEEDED;
}

AppendLogResult RaftPart::canAppendLogs(TermID termId) {
    DCHECK(!raftLock_.try_lock());
    if (UNLIKELY(term_ != termId)) {
        VLOG(2) << idStr_ << "Term has been updated, origin "
                << termId << ", new " << term_;
        return AppendLogResult::E_TERM_OUT_OF_DATE;
    }
    return canAppendLogs();
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
            if (target != addr_ && target != HostAddr("", 0)) {
                LOG(INFO) << idStr_ << "I am follower, just wait for the new leader.";
            } else {
                LOG(INFO) << idStr_ << "I will be the new leader, trigger leader election now!";
                bgWorkers_->addTask([self = shared_from_this()] {
                    {
                        std::unique_lock<std::mutex> lck(self->raftLock_);
                        self->role_ = Role::CANDIDATE;
                        self->leader_ = HostAddr("", 0);
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
    bool needToUnlock = raftLock_.try_lock();
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
                    leader_ = HostAddr("", 0);
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
    if (needToUnlock) {
        raftLock_.unlock();
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
        // The part will be removed in REMOVE_PART_ON_SRC phase
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

cpp2::ErrorCode RaftPart::checkPeer(const HostAddr& candidate) {
    CHECK(!raftLock_.try_lock());
    auto hosts = followers();
    auto it = std::find_if(hosts.begin(), hosts.end(), [&candidate](const auto& h) {
        return h->address() == candidate;
    });
    if (it == hosts.end()) {
        LOG(INFO) << idStr_ << "The candidate " << candidate << " is not in my peers";
        return cpp2::ErrorCode::E_WRONG_LEADER;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

void RaftPart::addListenerPeer(const HostAddr& listener) {
    std::lock_guard<std::mutex> guard(raftLock_);
    if (listener == addr_) {
        LOG(INFO) << idStr_ << "I am already in the raft group";
        return;
    }
    auto it = std::find_if(hosts_.begin(), hosts_.end(), [&listener] (const auto& h) {
        return h->address() == listener;
    });
    if (it == hosts_.end()) {
        // Add listener as a raft learner
        hosts_.emplace_back(std::make_shared<Host>(listener, shared_from_this(), true));
        listeners_.emplace(listener);
        LOG(INFO) << idStr_ << "Add listener " << listener;
    } else {
        LOG(INFO) << idStr_ << "The listener " << listener << " has joined raft group before";
    }
}

void RaftPart::removeListenerPeer(const HostAddr& listener) {
    std::lock_guard<std::mutex> guard(raftLock_);
    if (listener == addr_) {
        LOG(INFO) << idStr_ << "Remove myself from the raft group";
        return;
    }
    auto it = std::find_if(hosts_.begin(), hosts_.end(), [&listener] (const auto& h) {
        return h->address() == listener;
    });
    if (it == hosts_.end()) {
        LOG(INFO) << idStr_ << "The listener " << listener << " not found";
    } else {
        hosts_.erase(it);
        listeners_.erase(listener);
        LOG(INFO) << idStr_ << "Remove listener " << listener;
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
    bool needToUnlock = raftLock_.try_lock();
    SCOPE_EXIT {
        if (needToUnlock) {
            raftLock_.unlock();
        }
    };
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
    if (blocking_) {
        // No need to block heartbeats and empty log.
         if ((logType == LogType::NORMAL && !log.empty()) || logType == LogType::ATOMIC_OP) {
             return AppendLogResult::E_WRITE_BLOCKING;
         }
    }

    LogCache swappedOutLogs;
    auto retFuture = folly::Future<AppendLogResult>::makeEmpty();

    if (bufferOverFlow_) {
        LOG_EVERY_N(WARNING, 100) << idStr_
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
                    << "Another AppendLogs request is ongoing, just return";
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
        LOG_EVERY_N(WARNING, 1000) << idStr_ << "Cannot append logs, clean the buffer";
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
        [this] (AtomicOp opCB) -> folly::Optional<std::string> {
            CHECK(opCB != nullptr);
            auto opRet = opCB();
            if (!opRet.hasValue()) {
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
        res = canAppendLogs(termId);
        if (res != AppendLogResult::SUCCEEDED) {
            break;
        }
        currTerm = term_;
        prevLogId = lastLogId_;
        prevLogTerm = lastLogTerm_;
        committed = committedLogId_;
        // Step 1: Write WAL
        SlowOpTracker tracker;
        if (!wal_->appendLogs(iter)) {
            LOG_EVERY_N(WARNING, 100) << idStr_ << "Failed to write into WAL";
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
        LOG_EVERY_N(WARNING, 100) << idStr_ << "Failed to write wal";
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
        res = canAppendLogs(currTerm);
        if (res != AppendLogResult::SUCCEEDED) {
            break;
        }
        hosts = hosts_;
    } while (false);

    if (!checkAppendLogResult(res)) {
        LOG(WARNING) << idStr_ << "replicateLogs failed because of not leader or term changed";
        return;
    }

    VLOG(2) << idStr_ << "About to replicate logs to all peer hosts";

    lastMsgSentDur_.reset();
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
            res = canAppendLogs(currTerm);
            if (res != AppendLogResult::SUCCEEDED) {
                break;
            }
            lastLogId_ = lastLogId;
            lastLogTerm_ = currTerm;
        } while (false);

        if (!checkAppendLogResult(res)) {
            LOG(WARNING)
                << idStr_
                << "processAppendLogResponses failed because of not leader or term changed";
            return;
        }

        {
            auto walIt = wal_->iterator(committedId + 1, lastLogId);
            SlowOpTracker tracker;
            // Step 3: Commit the batch
            if (commitLogs(std::move(walIt), true) == nebula::cpp2::ErrorCode::SUCCEEDED) {
                std::lock_guard<std::mutex> g(raftLock_);
                committedLogId_ = lastLogId;
                firstLogId = lastLogId_ + 1;
                lastMsgAcceptedCostMs_ = lastMsgSentDur_.elapsedInMSec();
                lastMsgAcceptedTime_ = time::WallClock::fastNowInMilliSec();
                commitInThisTerm_ = true;
            } else {
                LOG(FATAL) << idStr_ << "Failed to commit logs";
            }
            if (tracker.slow()) {
                tracker.output(idStr_, folly::stringPrintf("Total commit: %ld",
                                                           committedLogId_ - committedId));
            }
            VLOG(2) << idStr_ << "Leader succeeded in committing the logs "
                              << committedId + 1 << " to " << lastLogId;
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
                        [this] (AtomicOp op) -> folly::Optional<std::string> {
                            auto opRet = op();
                            if (!opRet.hasValue()) {
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
        LOG_EVERY_N(WARNING, 100) << idStr_ << "Only " << numSucceeded
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
    return status_ == Status::RUNNING && role_ == Role::LEADER;
}


bool RaftPart::needToStartElection() {
    std::lock_guard<std::mutex> g(raftLock_);
    if (status_ == Status::RUNNING &&
        role_ == Role::FOLLOWER &&
        (lastMsgRecvDur_.elapsedInMSec() >= weight_ * FLAGS_raft_heartbeat_interval_secs * 1000 ||
         isBlindFollower_)) {
        LOG(INFO) << idStr_ << "Start leader election, reason: lastMsgDur "
                  << lastMsgRecvDur_.elapsedInMSec()
                  << ", term " << term_;
        role_ = Role::CANDIDATE;
        leader_ = HostAddr("", 0);
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

    if (UNLIKELY(status_ == Status::STOPPED)) {
        VLOG(2) << idStr_
                << "The part has been stopped, skip the request";
        return false;
    }

    if (UNLIKELY(status_ == Status::STARTING)) {
        VLOG(2) << idStr_ << "The partition is still starting";
        return false;
    }

    if (UNLIKELY(status_ == Status::WAITING_SNAPSHOT)) {
        VLOG(2) << idStr_ << "The partition is still waiting snapshot";
        return false;
    }

    // Make sure the role is still CANDIDATE
    if (role_ != Role::CANDIDATE) {
        VLOG(2) << idStr_ << "A leader has been elected";
        return false;
    }

    // Before start a new election, reset the votedAddr
    votedAddr_ = HostAddr("", 0);

    req.set_space(spaceId_);
    req.set_part(partId_);
    req.set_candidate_addr(addr_.host);
    req.set_candidate_port(addr_.port);
    req.set_term(++proposedTerm_);  // Bump up the proposed term
    req.set_last_log_id(lastLogId_);
    req.set_last_log_term(lastLogTerm_);

    hosts = followers();

    return true;
}


typename RaftPart::Role RaftPart::processElectionResponses(
        const RaftPart::ElectionResponses& results,
        std::vector<std::shared_ptr<Host>> hosts,
        TermID proposedTerm) {
    std::lock_guard<std::mutex> g(raftLock_);

    if (UNLIKELY(status_ == Status::STOPPED)) {
        LOG(INFO) << idStr_
                  << "The part has been stopped, skip the request";
        return role_;
    }

    if (UNLIKELY(status_ == Status::STARTING)) {
        LOG(INFO) << idStr_ << "The partition is still starting";
        return role_;
    }

    if (UNLIKELY(status_ == Status::WAITING_SNAPSHOT)) {
        LOG(INFO) << idStr_ << "The partition is still waitiong snapshot";
        return role_;
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
        } else if (r.second.get_error_code() == cpp2::ErrorCode::E_LOG_STALE) {
            LOG(INFO) << idStr_ << "My last log id is less than " << hosts[r.first]->address()
                      << ", double my election interval.";
            uint64_t curWeight = weight_.load();
            weight_.store(curWeight * 2);
        } else {
            LOG(ERROR) << idStr_ << "Receive response about askForVote from "
                       << hosts[r.first]->address() << ", error code is "
                       << apache::thrift::util::enumNameSafe(r.second.get_error_code());
        }
    }

    CHECK(role_ == Role::CANDIDATE);

    if (numSucceeded >= quorum_) {
        LOG(INFO) << idStr_
                  << "Partition is elected as the new leader for term "
                  << proposedTerm;
        term_ = proposedTerm;
        role_ = Role::LEADER;
        isBlindFollower_ = false;
    }

    return role_;
}


bool RaftPart::leaderElection() {
    VLOG(2) << idStr_ << "Start leader election...";
    using namespace folly;  // NOLINT since the fancy overload of | operator

    bool expected = false;
    if (!inElection_.compare_exchange_strong(expected, true)) {
        return true;
    }
    SCOPE_EXIT {
        inElection_ = false;
    };

    cpp2::AskForVoteRequest voteReq;
    decltype(hosts_) hosts;
    if (!prepareElectionRequest(voteReq, hosts)) {
        // Suppose we have three replicas A(leader), B, C, after A crashed,
        // B, C will begin the election. B win, and send hb, C has gap with B
        // and need the snapshot from B. Meanwhile C begin the election,
        // C will be Candidate, but because C is in WAITING_SNAPSHOT,
        // so prepareElectionRequest will return false and go on the election.
        // Becasue C is in Candidate, so it will reject the snapshot request from B.
        // Infinite loop begins.
        // So we neeed to go back to the follower state to avoid the case.
        std::lock_guard<std::mutex> g(raftLock_);
        role_ = Role::FOLLOWER;
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
              << voteReq.get_candidate_addr()
              << ", candidatePort = " << voteReq.get_candidate_port()
              << ")";

    auto proposedTerm = voteReq.get_term();
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
                    [&voteReq, &host, eb] ()
                            -> Future<cpp2::AskForVoteResponse> {
                        return host->askForVote(voteReq, eb);
                    });
            })
            | gen::as<std::vector>(),
            // Number of succeeded required
            quorum_,
            // Result evaluator
            [hosts] (size_t idx, cpp2::AskForVoteResponse& resp) {
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
    switch (processElectionResponses(resps, std::move(hosts), proposedTerm)) {
        case Role::LEADER: {
            // Elected
            LOG(INFO) << idStr_
                      << "The partition is elected as the leader";
            {
                std::lock_guard<std::mutex> g(raftLock_);
                if (status_ == Status::RUNNING) {
                    leader_ = addr_;
                    for (auto& host : hosts_) {
                        host->reset();
                    }
                    bgWorkers_->addTask([self = shared_from_this(),
                                       term = voteReq.get_term()] {
                        self->onElected(term);
                    });
                    lastMsgAcceptedTime_ = 0;
                }
                weight_ = 1;
                commitInThisTerm_ = false;
            }
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


void RaftPart::statusPolling(int64_t startTime) {
    {
        std::lock_guard<std::mutex> g(raftLock_);
        // If startTime is not same as the time when `statusPolling` is add to event loop,
        // it means the part has been restarted (it only happens in ut for now), so don't
        // add another `statusPolling`.
        if (startTime != startTimeMs_) {
            return;
        }
    }
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
    {
        std::lock_guard<std::mutex> g(raftLock_);
        if (status_ == Status::RUNNING || status_ == Status::WAITING_SNAPSHOT) {
            VLOG(3) << idStr_ << "Schedule new task";
            bgWorkers_->addDelayTask(
                delay,
                [self = shared_from_this(), startTime] {
                    self->statusPolling(startTime);
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
    if (status_ == Status::STARTING || status_ == Status::WAITING_SNAPSHOT) {
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
              << req.get_candidate_addr() << ":"
              << req.get_candidate_port()
              << ", term = " << req.get_term()
              << ", lastLogId = " << req.get_last_log_id()
              << ", lastLogTerm = " << req.get_last_log_term();

    std::lock_guard<std::mutex> g(raftLock_);

    // Make sure the partition is running
    if (UNLIKELY(status_ == Status::STOPPED)) {
        LOG(INFO) << idStr_
                  << "The part has been stopped, skip the request";
        resp.set_error_code(cpp2::ErrorCode::E_BAD_STATE);
        return;
    }

    if (UNLIKELY(status_ == Status::STARTING)) {
        LOG(INFO) << idStr_ << "The partition is still starting";
        resp.set_error_code(cpp2::ErrorCode::E_NOT_READY);
        return;
    }

    if (UNLIKELY(status_ == Status::WAITING_SNAPSHOT)) {
        LOG(INFO) << idStr_ << "The partition is still waiting snapshot";
        resp.set_error_code(cpp2::ErrorCode::E_NOT_READY);
        return;
    }

    LOG(INFO) << idStr_ << "The partition currently is a "
              << roleStr(role_) << ", lastLogId " << lastLogId_
              << ", lastLogTerm " << lastLogTerm_
              << ", committedLogId " << committedLogId_
              << ", term " << term_;
    if (role_ == Role::LEARNER) {
        resp.set_error_code(cpp2::ErrorCode::E_BAD_ROLE);
        return;
    }

    auto candidate = HostAddr(req.get_candidate_addr(), req.get_candidate_port());
    // Check term id
    auto term = term_;
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
                  << ", which is newer than the candidate's log "
                  << req.get_last_log_term()
                  << ". So the candidate will be rejected";
        resp.set_error_code(cpp2::ErrorCode::E_TERM_OUT_OF_DATE);
        return;
    }

    if (req.get_last_log_term() == lastLogTerm_) {
        // Check last log id
        if (req.get_last_log_id() < lastLogId_) {
            LOG(INFO) << idStr_
                      << "The partition's last log id is " << lastLogId_
                      << ". The candidate's last log id " << req.get_last_log_id()
                      << " is smaller, so it will be rejected, candidate is "
                      << candidate;
            resp.set_error_code(cpp2::ErrorCode::E_LOG_STALE);
            return;
        }
    }

    // If we have voted for somebody, we will reject other candidates under the proposedTerm.
    if (votedAddr_ != HostAddr("", 0)) {
        if (proposedTerm_ > req.get_term()
                || (proposedTerm_ == req.get_term() && votedAddr_ != candidate)) {
            LOG(INFO) << idStr_
                << "We have voted " << votedAddr_ << " on term " << proposedTerm_
                << ", so we should reject the candidate " << candidate
                << " request on term " << req.get_term();
            resp.set_error_code(cpp2::ErrorCode::E_TERM_OUT_OF_DATE);
            return;
        }
    }

    auto code = checkPeer(candidate);
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        resp.set_error_code(code);
        return;
    }
    // Ok, no reason to refuse, we will vote for the candidate
    LOG(INFO) << idStr_ << "The partition will vote for the candidate " << candidate;
    resp.set_error_code(cpp2::ErrorCode::SUCCEEDED);

    // Before change role from leader to follower, check the logs locally.
    if (role_ == Role::LEADER && wal_->lastLogId() > lastLogId_) {
        LOG(INFO) << idStr_ << "There is one log " << wal_->lastLogId()
                  << " i did not commit when i was leader, rollback to " << lastLogId_;
        wal_->rollbackToLog(lastLogId_);
    }
    role_ = Role::FOLLOWER;
    votedAddr_ = candidate;
    proposedTerm_ = req.get_term();
    leader_ = HostAddr("", 0);

    // Reset the last message time
    lastMsgRecvDur_.reset();
    weight_ = 1;
    isBlindFollower_ = false;
    return;
}


void RaftPart::processAppendLogRequest(
        const cpp2::AppendLogRequest& req,
        cpp2::AppendLogResponse& resp) {
    LOG_IF(INFO, FLAGS_trace_raft) << idStr_
        << "Received logAppend"
        << ": GraphSpaceId = " << req.get_space()
        << ", partition = " << req.get_part()
        << ", leaderIp = " << req.get_leader_addr()
        << ", leaderPort = " << req.get_leader_port()
        << ", current_term = " << req.get_current_term()
        << ", lastLogId = " << req.get_last_log_id()
        << ", committedLogId = " << req.get_committed_log_id()
        << ", lastLogIdSent = " << req.get_last_log_id_sent()
        << ", lastLogTermSent = " << req.get_last_log_term_sent()
        << ", num_logs = " << req.get_log_str_list().size()
        << ", logTerm = " << req.get_log_term()
        << ", sendingSnapshot = " << req.get_sending_snapshot()
        << ", local lastLogId = " << lastLogId_
        << ", local lastLogTerm = " << lastLogTerm_
        << ", local committedLogId = " << committedLogId_
        << ", local current term = " << term_;
    std::lock_guard<std::mutex> g(raftLock_);

    resp.set_current_term(term_);
    resp.set_leader_addr(leader_.host);
    resp.set_leader_port(leader_.port);
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
    cpp2::ErrorCode err = verifyLeader<cpp2::AppendLogRequest>(req);
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
                CHECK_EQ(firstId + numLogs - 1, wal_->lastLogId()) << "First Id is " << firstId;
            }
            lastLogId_ = wal_->lastLogId();
            lastLogTerm_ = wal_->lastLogTerm();
            resp.set_last_log_id(lastLogId_);
            resp.set_last_log_term(lastLogTerm_);
            resp.set_error_code(cpp2::ErrorCode::SUCCEEDED);
        } else {
            LOG_EVERY_N(WARNING, 100) << idStr_ << "Failed to append logs to WAL";
            resp.set_error_code(cpp2::ErrorCode::E_WAL_FAIL);
        }
        return;
    }

    if (req.get_last_log_id_sent() < committedLogId_ && req.get_last_log_term_sent() <= term_) {
        LOG(INFO) << idStr_ << "Stale log! The log " << req.get_last_log_id_sent()
                  << ", term " << req.get_last_log_term_sent()
                  << " i had committed yet. My committedLogId is "
                  << committedLogId_ << ", term is " << term_;
        resp.set_error_code(cpp2::ErrorCode::E_LOG_STALE);
        return;
    } else if (req.get_last_log_id_sent() < committedLogId_) {
        LOG(INFO) << idStr_ << "What?? How it happens! The log id is "
                  <<  req.get_last_log_id_sent()
                  << ", the log term is " << req.get_last_log_term_sent()
                  << ", but my committedLogId is " << committedLogId_
                  << ", my term is " << term_
                  << ", to make the cluster stable i will follow the high term"
                  << " candidate and clenaup my data";
        reset();
        resp.set_committed_log_id(committedLogId_);
        resp.set_last_log_id(lastLogId_);
        resp.set_last_log_term(lastLogTerm_);
        return;
    }

    // req.get_last_log_id_sent() >= committedLogId_
    if (lastLogTerm_ > 0 && req.get_last_log_term_sent() != lastLogTerm_) {
        LOG(INFO) << idStr_ << "The local last log term is " << lastLogTerm_
                << ", which is different from the leader's prevLogTerm "
                << req.get_last_log_term_sent()
                << ", the prevLogId is " << req.get_last_log_id_sent()
                << ". So need to rollback to last committedLogId_ " << committedLogId_;
        if (wal_->rollbackToLog(committedLogId_)) {
            lastLogId_ = wal_->lastLogId();
            lastLogTerm_ = wal_->lastLogTerm();
            resp.set_last_log_id(lastLogId_);
            resp.set_last_log_term(lastLogTerm_);
            LOG(INFO) << idStr_ << "Rollback succeeded! lastLogId is " << lastLogId_
                      << ", logLogTerm is " << lastLogTerm_
                      << ", committedLogId is " << committedLogId_
                      << ", term is " << term_;
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
        // TODO(doodle): This is a potential bug which would cause data not in consensus. In most
        // case, we would hit this path when leader append logs to follower and timeout (leader
        // would set lastLogIdSent_ = logIdToSend_ - 1 in Host). **But follower actually received
        // it successfully**. Which will explain when leader retry to append these logs, the LOG
        // belows is printed, and lastLogId_ == req.get_last_log_id_sent() + 1 in the LOG.
        //
        // In fact we should always rollback to req.get_last_log_id_sent(), and append the logs from
        // leader (we can't make promise that the logs in range [req.get_last_log_id_sent() + 1,
        // lastLogId_] is same with follower). However, this makes no difference in the above case.
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
        if (numLogs != 0) {
            CHECK_EQ(firstId + numLogs - 1, wal_->lastLogId()) << "First Id is " << firstId;
        }
        lastLogId_ = wal_->lastLogId();
        lastLogTerm_ = wal_->lastLogTerm();
        resp.set_last_log_id(lastLogId_);
        resp.set_last_log_term(lastLogTerm_);
    } else {
        LOG_EVERY_N(WARNING, 100) << idStr_ << "Failed to append logs to WAL";
        resp.set_error_code(cpp2::ErrorCode::E_WAL_FAIL);
        return;
    }

    LogID lastLogIdCanCommit = std::min(lastLogId_, req.get_committed_log_id());
    if (lastLogIdCanCommit > committedLogId_) {
        // Commit some logs
        // We can only commit logs from firstId to min(lastLogId_, leader's commit log id),
        // follower can't always commit to leader's commit id because of lack of log
        auto code = commitLogs(wal_->iterator(committedLogId_ + 1, lastLogIdCanCommit), false);
        if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
            VLOG(1) << idStr_ << "Follower succeeded committing log "
                              << committedLogId_ + 1 << " to "
                              << lastLogIdCanCommit;
            committedLogId_ = lastLogIdCanCommit;
            resp.set_committed_log_id(lastLogIdCanCommit);
            resp.set_error_code(cpp2::ErrorCode::SUCCEEDED);
        } else if (code == nebula::cpp2::ErrorCode::E_WRITE_STALLED) {
            VLOG(1) << idStr_ << "Follower delay committing log "
                              << committedLogId_ + 1 << " to "
                              << lastLogIdCanCommit;
            // Even if log is not applied to state machine, still regard as succeded:
            // 1. As a follower, upcoming request will try to commit them
            // 2. If it is elected as leader later, it will try to commit them as well
            resp.set_committed_log_id(committedLogId_);
            resp.set_error_code(cpp2::ErrorCode::SUCCEEDED);
        } else {
            LOG(ERROR) << idStr_ << "Failed to commit log "
                       << committedLogId_ + 1 << " to "
                       << req.get_committed_log_id();
            resp.set_error_code(cpp2::ErrorCode::E_WAL_FAIL);
        }
    } else {
        resp.set_error_code(cpp2::ErrorCode::SUCCEEDED);
    }

    // Reset the timeout timer again in case wal and commit takes longer time than expected
    lastMsgRecvDur_.reset();
}


template<typename REQ>
cpp2::ErrorCode RaftPart::verifyLeader(const REQ& req) {
    DCHECK(!raftLock_.try_lock());
    auto candidate = HostAddr(req.get_leader_addr(), req.get_leader_port());
    auto code = checkPeer(candidate);
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }

    VLOG(2) << idStr_ << "The current role is " << roleStr(role_);
    // Make sure the remote term is greater than local's
    if (req.get_current_term() < term_) {
        LOG_EVERY_N(INFO, 100) << idStr_
                               << "The current role is " << roleStr(role_)
                               << ". The local term is " << term_
                               << ". The remote term is not newer";
        return cpp2::ErrorCode::E_TERM_OUT_OF_DATE;
    } else if (req.get_current_term() > term_) {
        // Leader stickness, no matter the term in Request is larger or not.
        // TODO(heng) Maybe we should reconsider the logic
        if (leader_ != HostAddr("", 0) && leader_ != candidate
                && lastMsgRecvDur_.elapsedInMSec()
                        < FLAGS_raft_heartbeat_interval_secs * 1000) {
            LOG_EVERY_N(INFO, 100) << idStr_ << "I believe the leader " << leader_ << " exists. "
                                   << "Refuse to append logs of " << candidate;
            return cpp2::ErrorCode::E_WRONG_LEADER;
        }
    } else {
        // req.get_current_term() == term_
        do {
            if (role_ != Role::LEADER && leader_ == HostAddr("", 0)) {
                LOG_EVERY_N(INFO, 100) << idStr_
                                       << "I dont know who is leader for current term " << term_
                                       << ", so accept the candidate " << candidate;
                break;
            }
            // Same leader
            if (role_ != Role::LEADER
                    && candidate == leader_)  {
                return cpp2::ErrorCode::SUCCEEDED;
            } else {
                LOG_EVERY_N(INFO, 100) << idStr_
                                       << "The local term is same as remote term " << term_
                                       << ", my role is " << roleStr(role_) << ", reject it!";
                return cpp2::ErrorCode::E_TERM_OUT_OF_DATE;
            }
        } while (false);
    }

    // Update my state.
    Role oldRole = role_;
    TermID oldTerm = term_;
    // Ok, no reason to refuse, just follow the leader
    LOG(INFO) << idStr_ << "The current role is " << roleStr(role_)
              << ". Will follow the new leader "
              << req.get_leader_addr()
              << ":" << req.get_leader_port()
              << " [Term: " << req.get_current_term() << "]";

    if (role_ != Role::LEARNER) {
        role_ = Role::FOLLOWER;
    }
    leader_ = candidate;
    term_ = proposedTerm_ = req.get_current_term();
    votedAddr_ = HostAddr("", 0);
    weight_ = 1;
    isBlindFollower_ = false;
    // Before accept the logs from the new leader, check the logs locally.
    if (wal_->lastLogId() > lastLogId_) {
        LOG(INFO) << idStr_ << "There is one log " << wal_->lastLogId()
                  << " i did not commit when i was leader, rollback to " << lastLogId_;
        wal_->rollbackToLog(lastLogId_);
    }
    if (oldRole == Role::LEADER) {
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

void RaftPart::processHeartbeatRequest(
        const cpp2::HeartbeatRequest& req,
        cpp2::HeartbeatResponse& resp) {
    LOG_IF(INFO, FLAGS_trace_raft) << idStr_
        << "Received heartbeat"
        << ": GraphSpaceId = " << req.get_space()
        << ", partition = " << req.get_part()
        << ", leaderIp = " << req.get_leader_addr()
        << ", leaderPort = " << req.get_leader_port()
        << ", current_term = " << req.get_current_term()
        << ", lastLogId = " << req.get_last_log_id()
        << ", committedLogId = " << req.get_committed_log_id()
        << ", lastLogIdSent = " << req.get_last_log_id_sent()
        << ", lastLogTermSent = " << req.get_last_log_term_sent()
        << ", local lastLogId = " << lastLogId_
        << ", local lastLogTerm = " << lastLogTerm_
        << ", local committedLogId = " << committedLogId_
        << ", local current term = " << term_;
    std::lock_guard<std::mutex> g(raftLock_);

    resp.set_current_term(term_);
    resp.set_leader_addr(leader_.host);
    resp.set_leader_port(leader_.port);
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
    cpp2::ErrorCode err = verifyLeader<cpp2::HeartbeatRequest>(req);
    if (err != cpp2::ErrorCode::SUCCEEDED) {
        // Wrong leadership
        VLOG(2) << idStr_ << "Will not follow the leader";
        resp.set_error_code(err);
        return;
    }

    // Reset the timeout timer
    lastMsgRecvDur_.reset();

    // As for heartbeat, return ok after verifyLeader
    resp.set_error_code(cpp2::ErrorCode::SUCCEEDED);
    return;
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
    if (UNLIKELY(leader_ != HostAddr(req.get_leader_addr(), req.get_leader_port())
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
            LOG(INFO) << idStr_ << "Reset invalid wal after snapshot received";
            wal_->reset();
        }
        status_ = Status::RUNNING;
        LOG(INFO) << idStr_ << "Receive all snapshot, committedLogId_ " << committedLogId_
                  << ", lastLodId " << lastLogId_ << ", lastLogTermId " << lastLogTerm_;
    }
    resp.set_error_code(cpp2::ErrorCode::SUCCEEDED);
    return;
}

void RaftPart::sendHeartbeat() {
    // If leader has not commit any logs in this term, it must commit all logs in previous term,
    // so heartbeat is send by appending one empty log.
    if (!replicatingLogs_.load(std::memory_order_acquire)) {
        folly::via(executor_.get(), [this] {
            std::string log = "";
            appendLogAsync(clusterId_, LogType::NORMAL, std::move(log));
        });
    }

    using namespace folly;  // NOLINT since the fancy overload of | operator
    VLOG(2) << idStr_ << "Send heartbeat";
    TermID currTerm = 0;
    LogID latestLogId = 0;
    LogID commitLogId = 0;
    TermID prevLogTerm = 0;
    LogID prevLogId = 0;
    size_t replica = 0;
    decltype(hosts_) hosts;
    {
        std::lock_guard<std::mutex> g(raftLock_);
        currTerm = term_;
        latestLogId = wal_->lastLogId();
        commitLogId = committedLogId_;
        prevLogTerm = lastLogTerm_;
        prevLogId = lastLogId_;
        replica = quorum_;
        hosts = hosts_;
    }
    auto eb = ioThreadPool_->getEventBase();
    auto startMs = time::WallClock::fastNowInMilliSec();
    collectNSucceeded(
        gen::from(hosts)
        | gen::map([self = shared_from_this(), eb, currTerm, latestLogId, commitLogId,
                    prevLogId, prevLogTerm] (std::shared_ptr<Host> hostPtr) {
            VLOG(2) << self->idStr_ << "Send heartbeat to " << hostPtr->idStr();
            return via(eb, [=]() -> Future<cpp2::HeartbeatResponse> {
                return hostPtr->sendHeartbeat(
                    eb, currTerm, latestLogId, commitLogId, prevLogTerm, prevLogId);
            });
        })
        | gen::as<std::vector>(),
        // Number of succeeded required
        hosts.size(),
        // Result evaluator
        [hosts] (size_t index, cpp2::HeartbeatResponse& resp) {
            return resp.get_error_code() == cpp2::ErrorCode::SUCCEEDED
                    && !hosts[index]->isLearner();
    })
    .then([replica, hosts = std::move(hosts), startMs, this]
          (folly::Try<HeartbeatResponses>&& resps) {
        CHECK(!resps.hasException());
        size_t numSucceeded = 0;
        for (auto& resp : *resps) {
            if (!hosts[resp.first]->isLearner()
                    && resp.second.get_error_code() == cpp2::ErrorCode::SUCCEEDED) {
                ++numSucceeded;
            }
        }
        if (numSucceeded >= replica) {
            VLOG(2) << idStr_ << "Heartbeat is accepted by quorum";
            std::lock_guard<std::mutex> g(raftLock_);
            auto now = time::WallClock::fastNowInMilliSec();
            lastMsgAcceptedCostMs_ = now - startMs;
            lastMsgAcceptedTime_ = now;
        }
    });
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

std::vector<HostAddr> RaftPart::peers() const {
    std::lock_guard<std::mutex> lck(raftLock_);
    std::vector<HostAddr> peer{addr_};
    for (auto& host : hosts_) {
        peer.emplace_back(host->address());
    }
    return peer;
}

std::set<HostAddr> RaftPart::listeners() const {
    std::lock_guard<std::mutex> lck(raftLock_);
    return listeners_;
}

std::pair<LogID, TermID> RaftPart::lastLogInfo() const {
    return std::make_pair(wal_->lastLogId(), wal_->lastLogTerm());
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
        return false;
    }
    return true;
}

void RaftPart::reset() {
    CHECK(!raftLock_.try_lock());
    wal_->reset();
    cleanup();
    lastLogId_ = committedLogId_ = 0;
    lastLogTerm_ = 0;
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
            if (host->followerCommittedLogId_ == 0
                    || host->followerCommittedLogId_ < wal_->firstLogId()) {
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

void RaftPart::checkRemoteListeners(const std::set<HostAddr>& expected) {
    auto actual = listeners();
    for (const auto& host : actual) {
        auto it = std::find(expected.begin(), expected.end(), host);
        if (it == expected.end()) {
            LOG(INFO) << idStr_ << "The listener " << host << " should not exist in my peers";
            removeListenerPeer(host);
        }
    }
    for (const auto& host : expected) {
        auto it = std::find(actual.begin(), actual.end(), host);
        if (it == actual.end()) {
            LOG(INFO) << idStr_ << "Add listener " << host << " to my peers";
            addListenerPeer(host);
        }
    }
}

bool RaftPart::leaseValid() {
    std::lock_guard<std::mutex> g(raftLock_);
    if (hosts_.empty()) {
        return true;
    }
    if (!commitInThisTerm_) {
        return false;
    }
    // When majority has accepted a log, leader obtains a lease which last for heartbeat.
    // However, we need to take off the net io time. On the left side of the inequality is
    // the time duration since last time leader send a log (the log has been accepted as well)
    return time::WallClock::fastNowInMilliSec() - lastMsgAcceptedTime_
        < FLAGS_raft_heartbeat_interval_secs * 1000 - lastMsgAcceptedCostMs_;
}

}  // namespace raftex
}  // namespace nebula

