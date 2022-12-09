/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/listener/Listener.h"

#include "codec/RowReaderWrapper.h"
#include "common/fs/FileUtils.h"
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
                   ListenerID listenerId,
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
               nullptr) {
  listenerId_ = listenerId;
  walPath_ = walPath;
}

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

nebula::cpp2::ErrorCode Listener::cleanup() {
  CHECK(!raftLock_.try_lock());
  leaderCommitId_ = 0;
  lastApplyLogId_ = 0;
  persist(0, 0, lastApplyLogId_);
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode Listener::checkPeer(const HostAddr& candidate) {
  CHECK(!raftLock_.try_lock());
  if (peers_.find(candidate) == peers_.end()) {
    VLOG(2) << idStr_ << "The candidate " << candidate << " is not in my peers";
    return nebula::cpp2::ErrorCode::E_RAFT_INVALID_PEER;
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
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
  if (isStopped()) {
    return;
  }

  if (needToCleanupSnapshot()) {
    cleanupSnapshot();
  }
  // todo(doodle): only put is handled, all remove is ignored for now
  folly::via(executor_.get(), [this] {
    SCOPE_EXIT {
      bgWorkers_->addDelayTask(
          FLAGS_listener_commit_interval_secs * 1000, &Listener::doApply, this);
    };
    processLogs();
  });
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

ListenerID Listener::getLisenerIdFromFile(const std::string& path) {
  if (!fs::FileUtils::exist(path)) {
    return -1;
  }
  auto fd = open(path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);
  if (fd < 0) {
    LOG(FATAL) << "Failed to open the file " << path << " (" << errno << "): " << strerror(errno);
  }
  // read listenerId from log file.
  ListenerID listenerId;
  auto ret = pread(fd, &listenerId, sizeof(ListenerID), 0);
  if (ret != static_cast<ssize_t>(sizeof(ListenerID))) {
    close(fd);
    VLOG(3) << "Read listenerId file failed, path " << path;
    return -1;
  }
  return listenerId;
}

bool Listener::writeListenerIdFile(const std::string& path, ListenerID listenerId) {
  auto fd = open(path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);
  if (fd < 0) {
    LOG(FATAL) << "Failed to open the file " << path << " (" << errno << "): " << strerror(errno);
  }
  std::string val;
  val.append(reinterpret_cast<const char*>(&listenerId), sizeof(ListenerID));
  ssize_t written = write(fd, val.c_str(), val.size());
  if (written != (ssize_t)val.size()) {
    VLOG(3) << "Written:" << path << "failed, error:" << strerror(errno);
    close(fd);
    return false;
  }
  close(fd);
  return true;
}

}  // namespace kvstore
}  // namespace nebula
