/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/time/WallClock.h"
#include "kvstore/Listener.h"
#include "kvstore/LogEncoder.h"
#include "codec/RowReaderWrapper.h"

DEFINE_int32(listener_commit_interval_secs, 1, "Listener commit interval");
DEFINE_int32(listener_commit_batch_size, 1000, "Max batch size when listener commit");
DEFINE_int32(ft_request_retry_times, 3, "Retry times if fulltext request failed");
DEFINE_int32(ft_bulk_batch_size, 100, "Max batch size when bulk insert");
DEFINE_int32(listener_pursue_leader_threshold, 1000, "Catch up with the leader's threshold");

namespace nebula {
namespace kvstore {

Listener::Listener(GraphSpaceID spaceId,
                   PartitionID partId,
                   HostAddr localAddr,
                   const std::string& walPath,
                   std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
                   std::shared_ptr<thread::GenericThreadPool> workers,
                   std::shared_ptr<folly::Executor> handlers,
                   std::shared_ptr<raftex::SnapshotManager> snapshotMan,
                   std::shared_ptr<RaftClient> clientMan,
                   std::shared_ptr<DiskManager> diskMan,
                   meta::SchemaManager* schemaMan)
    : RaftPart(FLAGS_cluster_id, spaceId, partId, localAddr, walPath,
               ioPool, workers, handlers, snapshotMan, clientMan, diskMan)
    , schemaMan_(schemaMan) {
}

void Listener::start(std::vector<HostAddr>&& peers, bool) {
    std::lock_guard<std::mutex> g(raftLock_);

    init();

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

    lastApplyLogId_ = lastApplyLogId();

    LOG(INFO) << idStr_ << "Listener start"
                        << ", there are " << peers.size() << " peer hosts"
                        << ", lastLogId " << lastLogId_
                        << ", lastLogTerm " << lastLogTerm_
                        << ", committedLogId " << committedLogId_
                        << ", lastApplyLogId " << lastApplyLogId_
                        << ", term " << term_;

    // As for listener, we don't need Host actually. However, listener need to be aware of
    // membership change, it can be handled in preProcessLog.
    for (auto& addr : peers) {
        peers_.emplace(addr);
    }

    status_ = Status::RUNNING;
    role_ = Role::LEARNER;

    size_t delayMS = 100 + folly::Random::rand32(900);
    bgWorkers_->addDelayTask(delayMS, &Listener::doApply, this);
}

void Listener::stop() {
    LOG(INFO) << "Stop listener [" << spaceId_ << ", " << partId_ << "] on " << addr_;
    {
        std::unique_lock<std::mutex> lck(raftLock_);
        status_ = Status::STOPPED;
        leader_ = {"", 0};
    }
}

bool Listener::preProcessLog(LogID logId,
                             TermID termId,
                             ClusterID clusterId,
                             const std::string& log) {
    UNUSED(logId); UNUSED(termId); UNUSED(clusterId);
    if (!log.empty()) {
        // todo(doodle): handle membership change
        switch (log[sizeof(int64_t)]) {
            case OP_ADD_LEARNER: {
                break;
            }
            case OP_ADD_PEER: {
                break;
            }
            case OP_REMOVE_PEER: {
                break;
            }
            default: {
                break;
            }
        }
    }
    return true;
}

cpp2::ErrorCode Listener::commitLogs(std::unique_ptr<LogIterator> iter, bool) {
    LogID lastId = -1;
    while (iter->valid()) {
        lastId = iter->logId();
        ++(*iter);
    }
    if (lastId > 0) {
        leaderCommitId_ = lastId;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

void Listener::doApply() {
    if (isStopped()) {
        return;
    }
    // todo(doodle): only put is handled, all remove is ignored for now
    folly::via(executor_.get(), [this] {
        SCOPE_EXIT {
            bgWorkers_->addDelayTask(FLAGS_listener_commit_interval_secs * 1000,
                                     &Listener::doApply, this);
        };

        std::unique_ptr<LogIterator> iter;
        {
            std::lock_guard<std::mutex> guard(raftLock_);
            if (lastApplyLogId_ >= committedLogId_) {
                return;
            }
            iter = wal_->iterator(lastApplyLogId_ + 1, committedLogId_);
        }

        LogID lastApplyId = -1;
        // the kv pair which can sync to remote safely
        std::vector<KV> data;
        while (iter->valid()) {
            lastApplyId = iter->logId();

            auto log = iter->logMsg();
            if (log.empty()) {
                // skip the heartbeat
                ++(*iter);
                continue;
            }

            DCHECK_GE(log.size(), sizeof(int64_t) + 1 + sizeof(uint32_t));
            switch (log[sizeof(int64_t)]) {
                case OP_PUT: {
                    auto pieces = decodeMultiValues(log);
                    DCHECK_EQ(2, pieces.size());
                    data.emplace_back(pieces[0], pieces[1]);
                    break;
                }
                case OP_MULTI_PUT: {
                    auto kvs = decodeMultiValues(log);
                    DCHECK_EQ((kvs.size() + 1) / 2, kvs.size() / 2);
                    for (size_t i = 0; i < kvs.size(); i += 2) {
                        data.emplace_back(kvs[i], kvs[i + 1]);
                    }
                    break;
                }
                case OP_REMOVE:
                case OP_REMOVE_RANGE:
                case OP_MULTI_REMOVE: {
                    break;
                }
                case OP_BATCH_WRITE: {
                    auto batch = decodeBatchValue(log);
                    for (auto& op : batch) {
                        // OP_BATCH_PUT and OP_BATCH_REMOVE_RANGE is igored
                        if (op.first == BatchLogType::OP_BATCH_PUT) {
                            data.emplace_back(op.second.first, op.second.second);
                        }
                    }
                    break;
                }
                case OP_TRANS_LEADER:
                case OP_ADD_LEARNER:
                case OP_ADD_PEER:
                case OP_REMOVE_PEER: {
                    break;
                }
                default: {
                    LOG(WARNING) << idStr_ << "Unknown operation: " << static_cast<int32_t>(log[0]);
                }
            }

            if (static_cast<int32_t>(data.size()) > FLAGS_listener_commit_batch_size) {
                break;
            }
            ++(*iter);
        }

        // apply to state machine
        if (apply(data)) {
            std::lock_guard<std::mutex> guard(raftLock_);
            lastApplyLogId_ = lastApplyId;
            persist(committedLogId_, term_, lastApplyLogId_);
            VLOG(1) << idStr_ << "Listener succeeded apply log to " << lastApplyLogId_;
            lastApplyTime_ = time::WallClock::fastNowInMilliSec();
            VLOG(1) << folly::sformat("Commit snapshot to : committedLogId={},"
                                      "committedLogTerm={}, lastApplyLogId_={}",
                                      committedLogId_, term_, lastApplyLogId_);
        }
    });
}

std::pair<int64_t, int64_t> Listener::commitSnapshot(const std::vector<std::string>& rows,
                                                     LogID committedLogId,
                                                     TermID committedLogTerm,
                                                     bool finished) {
    VLOG(1) << idStr_ << "Listener is committing snapshot.";
    int64_t count = 0;
    int64_t size = 0;
    std::vector<KV> data;
    data.reserve(rows.size());
    for (const auto& row : rows) {
        count++;
        size += row.size();
        auto kv = decodeKV(row);
        data.emplace_back(kv.first, kv.second);
    }
    if (!apply(data)) {
        LOG(ERROR) << idStr_ << "Failed to apply data while committing snapshot.";
        return std::make_pair(0, 0);
    }
    if (finished) {
        CHECK(!raftLock_.try_lock());
        leaderCommitId_ = committedLogId;
        lastApplyLogId_ = committedLogId;
        persist(committedLogId, committedLogTerm, lastApplyLogId_);
        LOG(INFO) << idStr_ << "Listener succeeded apply log to " << lastApplyLogId_;
        lastApplyTime_ = time::WallClock::fastNowInMilliSec();
        VLOG(3) << folly::sformat("Commit snapshot to : committedLogId={},"
                                  "committedLogTerm={}, lastApplyLogId_={}",
                                  committedLogId, committedLogTerm, lastApplyLogId_);
    }
    return std::make_pair(count, size);
}

void Listener::resetListener() {
    std::lock_guard<std::mutex> g(raftLock_);
    reset();
    VLOG(1) << folly::sformat("The listener has been reset : leaderCommitId={},"
                              "proposedTerm={}, lastLogTerm={}, term={},"
                              "lastApplyLogId={}",
                              leaderCommitId_, proposedTerm_, lastLogTerm_,
                              term_, lastApplyLogId_);
}

bool Listener::pursueLeaderDone() {
    std::lock_guard<std::mutex> g(raftLock_);
    if (status_ != Status::RUNNING) {
        return false;
    }
    // if the leaderCommitId_ and lastApplyLogId_ all are 0. means the snapshot gap.
    if (leaderCommitId_ == 0 && lastApplyLogId_ == 0) {
        return false;
    }
    VLOG(1) << folly::sformat("pursue leader : leaderCommitId={}, lastApplyLogId_={}",
                              leaderCommitId_, lastApplyLogId_);
    return (leaderCommitId_ - lastApplyLogId_) <= FLAGS_listener_pursue_leader_threshold;
}
}  // namespace kvstore
}  // namespace nebula
