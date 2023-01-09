/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/listener/Listener.h"

#include "codec/RowReaderWrapper.h"
#include "common/time/WallClock.h"
#include "kvstore/LogEncoder.h"

DEFINE_int32(listener_commit_interval_secs, 1, "Listener commit interval");
DEFINE_uint32(ft_request_retry_times, 3, "Retry times if fulltext request failed");
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
                   std::shared_ptr<folly::Executor> handlers)
    : RaftPart(FLAGS_cluster_id,
               spaceId,
               partId,
               localAddr,
               walPath,
               ioPool,
               workers,
               handlers,
               nullptr,
               nullptr,
               nullptr) {}

void Listener::start(std::vector<HostAddr>&& peers, bool) {
  std::lock_guard<std::mutex> g(raftLock_);

  init();

  lastLogId_ = wal_->lastLogId();
  lastLogTerm_ = wal_->lastLogTerm();
  term_ = lastLogTerm_;

  // Set the quorum number
  quorum_ = (peers.size() + 1) / 2;

  auto logIdAndTerm = lastCommittedLogId();
  committedLogId_ = logIdAndTerm.first;
  committedLogTerm_ = logIdAndTerm.second;

  if (lastLogId_ < committedLogId_) {
    LOG(INFO) << idStr_ << "Listener reset lastLogId " << lastLogId_ << " to be the committedLogId "
              << committedLogId_;
    lastLogId_ = committedLogId_;
    lastLogTerm_ = committedLogTerm_;
    wal_->reset();
  }

  lastApplyLogId_ = lastApplyLogId();

  LOG(INFO) << idStr_ << "Listener start"
            << ", there are " << peers.size() << " peer hosts"
            << ", lastLogId " << lastLogId_ << ", lastLogTerm " << lastLogTerm_
            << ", committedLogId " << committedLogId_ << ", lastApplyLogId " << lastApplyLogId_
            << ", term " << term_;

  // As for listener, we don't need Host actually. However, listener need to be
  // aware of membership change, it can be handled in preProcessLog.
  for (auto& addr : peers) {
    peers_.emplace(addr);
  }

  status_ = Status::RUNNING;
  role_ = Role::LEARNER;

  applyPool_ = std::make_unique<folly::IOThreadPoolExecutor>(
      1,
      std::make_shared<folly::NamedThreadFactory>(
          folly::stringPrintf("ListenerApplyPool-%d-%d", spaceId_, partId_)));
  applyPool_->add(std::bind(&Listener::doApply, this));
}

void Listener::stop() {
  LOG(INFO) << "Stop listener [" << spaceId_ << ", " << partId_ << "] on " << addr_;
  {
    std::unique_lock<std::mutex> lck(raftLock_);
    status_ = Status::STOPPED;
    leader_ = {"", 0};
  }
  applyPool_->stop();
  applyPool_->join();
}

bool Listener::preProcessLog(LogID logId,
                             TermID termId,
                             ClusterID clusterId,
                             folly::StringPiece log) {
  UNUSED(logId);
  UNUSED(termId);
  UNUSED(clusterId);
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

void Listener::cleanWal() {
  std::lock_guard<std::mutex> g(raftLock_);
  wal()->cleanWAL(lastApplyLogId_);
}

std::tuple<nebula::cpp2::ErrorCode, LogID, TermID> Listener::commitLogs(
    std::unique_ptr<LogIterator> iter, bool, bool) {
  LogID lastId = kNoCommitLogId;
  TermID lastTerm = kNoCommitLogTerm;
  while (iter->valid()) {
    lastId = iter->logId();
    lastTerm = iter->logTerm();
    ++(*iter);
  }
  if (lastId > 0) {
    leaderCommitId_ = lastId;
  }
  return {cpp2::ErrorCode::SUCCEEDED, lastId, lastTerm};
}

void Listener::doApply() {
  while (!isStopped()) {
    if (needToCleanupSnapshot()) {
      cleanupSnapshot();
    }
    // todo(doodle): only put is handled, all remove is ignored for now
    processLogs();
    sleep(FLAGS_listener_commit_interval_secs);
  }
}

void Listener::resetListener() {
  std::lock_guard<std::mutex> g(raftLock_);
  reset();
  LOG(INFO) << folly::sformat(
      "The listener has been reset : leaderCommitId={},"
      "lastLogTerm={}, term={},"
      "lastApplyLogId={}",
      leaderCommitId_,
      lastLogTerm_,
      term_,
      lastApplyLogId_);
}

bool Listener::pursueLeaderDone() {
  std::lock_guard<std::mutex> g(raftLock_);
  if (status_ != Status::RUNNING) {
    return false;
  }
  // if the leaderCommitId_ and lastApplyLogId_ all are 0. means the snapshot
  // gap.
  if (leaderCommitId_ == 0 && lastApplyLogId_ == 0) {
    return false;
  }
  VLOG(1) << folly::sformat(
      "pursue leader : leaderCommitId={}, lastApplyLogId_={}", leaderCommitId_, lastApplyLogId_);
  return (leaderCommitId_ - lastApplyLogId_) <= FLAGS_listener_pursue_leader_threshold;
}

}  // namespace kvstore
}  // namespace nebula
