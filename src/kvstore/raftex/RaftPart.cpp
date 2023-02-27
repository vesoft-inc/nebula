/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/raftex/RaftPart.h"

#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/gen/Base.h>
#include <folly/io/async/EventBaseManager.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/base/Base.h"
#include "common/base/CollectNSucceeded.h"
#include "common/network/NetworkUtils.h"
#include "common/stats/StatsManager.h"
#include "common/thread/NamedThread.h"
#include "common/thrift/ThriftClientManager.h"
#include "common/time/ScopedTimer.h"
#include "common/time/WallClock.h"
#include "common/utils/LogStrListIterator.h"
#include "common/utils/MetaKeyUtils.h"
#include "interface/gen-cpp2/RaftexServiceAsyncClient.h"
#include "kvstore/LogEncoder.h"
#include "kvstore/raftex/Host.h"
#include "kvstore/raftex/RaftLogIterator.h"
#include "kvstore/stats/KVStats.h"
#include "kvstore/wal/FileBasedWal.h"

DEFINE_uint32(raft_heartbeat_interval_secs, 5, "Seconds between each heartbeat");

DEFINE_uint64(raft_snapshot_timeout, 60 * 5, "Max seconds between two snapshot requests");

DEFINE_uint32(max_batch_size, 256, "The max number of logs in a batch");

DEFINE_bool(trace_raft, false, "Enable trace one raft request");

DEFINE_int32(raft_num_worker_threads, 32, "Raft part number of workers");

DECLARE_int32(wal_ttl);
DECLARE_int64(wal_file_size);
DECLARE_int32(wal_buffer_size);
DECLARE_bool(wal_sync);

namespace nebula {
namespace raftex {

using nebula::network::NetworkUtils;
using nebula::thrift::ThriftClientManager;
using nebula::wal::FileBasedWal;
using nebula::wal::FileBasedWalInfo;
using nebula::wal::FileBasedWalPolicy;

using OpProcessor = folly::Function<std::optional<std::string>(AtomicOp op)>;

/**
 * @brief code to describe if a log can be merged with others
 *  NO_MERGE: can't merge with any other
 *  MERGE_NEXT: can't previous logs, can merge with next. (has to be head)
 *  MERGE_PREV: can merge with previous, can't merge any more.  (has to be tail)
 *  MERGE_BOTH: can merge with any other
 *
 *  Normal / heartbeat will always be MERGE_BOTH
 *  Command will always be MERGE_PREV
 *  ATOMIC_OP can be either MERGE_NEXT or MERGE_BOTH
 *                          depends on if it read a key in write set.
 *  no log type will judge as NO_MERGE
 */

enum class MergeAbleCode {
  NO_MERGE = 0,
  MERGE_NEXT = 1,
  MERGE_PREV = 2,
  MERGE_BOTH = 3,
};

/**
 * @brief this is an Iterator deal with memory lock.
 */
class AppendLogsIterator final : public LogIterator {
 public:
  AppendLogsIterator(LogID firstLogId, TermID termId, RaftPart::LogCache logs)
      : firstLogId_(firstLogId), termId_(termId), logId_(firstLogId), logs_(std::move(logs)) {}
  AppendLogsIterator(const AppendLogsIterator&) = delete;
  AppendLogsIterator(AppendLogsIterator&&) = default;

  AppendLogsIterator& operator=(const AppendLogsIterator&) = delete;
  AppendLogsIterator& operator=(AppendLogsIterator&&) = default;

  ~AppendLogsIterator() {
    if (!logs_.empty()) {
      for (auto& log : logs_) {
        auto& promiseRef = std::get<4>(log);
        if (!promiseRef.isFulfilled()) {
          // When a AppendLogsIterator destruct before calling commit, the promise it not fulfilled.
          // It only happens when exception is thrown, which make `commit` is never invoked.
          promiseRef.setValue(nebula::cpp2::ErrorCode::E_STORAGE_MEMORY_EXCEEDED);
        }
      }
    }
  }

  void commit(nebula::cpp2::ErrorCode code = nebula::cpp2::ErrorCode::SUCCEEDED) {
    for (auto it = logs_.begin(); it != logs_.end(); ++it) {
      auto& promiseRef = std::get<4>(*it);
      if (!promiseRef.isFulfilled()) {
        DCHECK(!promiseRef.isFulfilled());
        promiseRef.setValue(code);
      }
    }
  }

  LogIterator& operator++() override {
    ++idx_;
    ++logId_;
    return *this;
  }

  bool valid() const override {
    return idx_ < logs_.size();
  }

  bool empty() const {
    return logs_.empty();
  }

  LogID firstLogId() const {
    return firstLogId_;
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
    return std::get<2>(logs_.at(idx_));
  }

  LogType logType() const {
    return std::get<1>(logs_.at(idx_));
  }

 private:
  size_t idx_{0};
  LogID firstLogId_;
  TermID termId_;
  LogID logId_;
  RaftPart::LogCache logs_;
};

class AppendLogsIteratorFactory {
 public:
  AppendLogsIteratorFactory() = default;
  static void make(RaftPart::LogCache& cacheLogs, RaftPart::LogCache& sendLogs) {
    DCHECK(sendLogs.empty());
    std::unordered_set<std::string> memLock;
    std::list<std::pair<std::string, std::string>> ranges;
    for (auto& log : cacheLogs) {
      auto code = mergeAble(log, memLock, ranges);
      if (code == MergeAbleCode::MERGE_BOTH) {
        sendLogs.emplace_back();
        std::swap(cacheLogs.front(), sendLogs.back());
        cacheLogs.pop_front();
        continue;
      } else if (code == MergeAbleCode::MERGE_PREV) {
        sendLogs.emplace_back();
        std::swap(cacheLogs.front(), sendLogs.back());
        cacheLogs.pop_front();
        break;
      } else if (code == MergeAbleCode::NO_MERGE) {
        // if we meet some failed atomicOp, we can just skip it.
        cacheLogs.pop_front();
        continue;
      } else {  // MERGE_NEXT
        break;
      }
    }
  }

  /**
   * @brief check if a incoming log can be merged with previous logs
   *
   * @param logWrapper
   */
  static MergeAbleCode mergeAble(RaftPart::LogCacheItem& logWrapper,
                                 std::unordered_set<std::string>& memLock,
                                 std::list<std::pair<std::string, std::string>>& ranges) {
    // log type:
    switch (std::get<1>(logWrapper)) {
      case LogType::NORMAL: {
        std::vector<folly::StringPiece> updateSet;
        auto& log = std::get<2>(logWrapper);
        if (log.empty()) {
          return MergeAbleCode::MERGE_BOTH;
        }
        decode(log, updateSet, ranges);
        for (auto& key : updateSet) {
          memLock.insert(key.str());
        }
        return MergeAbleCode::MERGE_BOTH;
      }
      case LogType::COMMAND: {
        return MergeAbleCode::MERGE_PREV;
      }
      case LogType::ATOMIC_OP: {
        auto& atomOp = std::get<3>(logWrapper);
        auto [code, result, read, write] = atomOp();
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
          DLOG(INFO) << "===> OOPs, atomOp failed!!!, code = "
                     << apache::thrift::util::enumNameSafe(code);
          auto& promiseRef = std::get<4>(logWrapper);
          if (!promiseRef.isFulfilled()) {
            DCHECK(!promiseRef.isFulfilled());
            promiseRef.setValue(code);
          }
          return MergeAbleCode::NO_MERGE;
        }
        std::get<2>(logWrapper) = std::move(result);
        /**
         * @brief We accept same read/write key in a one log,
         *        but reject if same in different logs.
         */
        for (auto& key : read) {
          auto cit = memLock.find(key);
          // read after write is not acceptable.
          if (cit != memLock.end()) {
            return MergeAbleCode::MERGE_NEXT;
          }

          // if we try to read a key, in any range
          for (auto& it : ranges) {
            auto* begin = it.first.c_str();
            auto* end = it.second.c_str();
            auto* pKey = key.c_str();
            if ((std::strcmp(begin, pKey) <= 0) && (std::strcmp(pKey, end) <= 0)) {
              return MergeAbleCode::MERGE_NEXT;
            }
          }
        }

        for (auto& key : write) {
          // it doesn't matter if insert failed. (if write conflict, last write win)
          memLock.insert(key);
        }
        return MergeAbleCode::MERGE_BOTH;
      }
      default:
        LOG(ERROR) << "should not get here";
    }
    return MergeAbleCode::NO_MERGE;
  }

  static void decode(const std::string& log,
                     std::vector<folly::StringPiece>& updateSet,
                     std::list<std::pair<std::string, std::string>>& ranges) {
    switch (log[sizeof(int64_t)]) {
      case nebula::kvstore::OP_PUT: {
        auto pieces = nebula::kvstore::decodeMultiValues(log);
        updateSet.push_back(pieces[0]);
        break;
      }
      case nebula::kvstore::OP_MULTI_PUT: {
        auto kvs = nebula::kvstore::decodeMultiValues(log);
        // Make the number of values are an even number
        DCHECK_EQ((kvs.size() + 1) / 2, kvs.size() / 2);
        for (size_t i = 0; i < kvs.size(); i += 2) {
          updateSet.push_back(kvs[i]);
        }
        break;
      }
      case nebula::kvstore::OP_REMOVE: {
        auto key = nebula::kvstore::decodeSingleValue(log);
        updateSet.push_back(key);
        break;
      }
      case nebula::kvstore::OP_MULTI_REMOVE: {
        auto keys = nebula::kvstore::decodeMultiValues(log);
        for (auto k : keys) {
          updateSet.push_back(k);
        }
        break;
      }
      case nebula::kvstore::OP_REMOVE_RANGE: {
        auto range = nebula::kvstore::decodeMultiValues(log);
        auto item = std::make_pair(range[0].str(), range[1].str());
        ranges.emplace_back(std::move(item));
        break;
      }
      case nebula::kvstore::OP_BATCH_WRITE: {
        auto data = nebula::kvstore::decodeBatchValue(log);
        for (auto& op : data) {
          if (op.first == nebula::kvstore::BatchLogType::OP_BATCH_PUT) {
            updateSet.push_back(op.second.first);
          } else if (op.first == nebula::kvstore::BatchLogType::OP_BATCH_REMOVE) {
            updateSet.push_back(op.second.first);
          } else if (op.first == nebula::kvstore::BatchLogType::OP_BATCH_REMOVE_RANGE) {
            auto begin = op.second.first;
            auto end = op.second.second;
            ranges.emplace_back(std::make_pair(begin, end));
          }
        }
        break;
      }
      case nebula::kvstore::OP_ADD_PEER:
      case nebula::kvstore::OP_ADD_LEARNER:
      case nebula::kvstore::OP_TRANS_LEADER:
      case nebula::kvstore::OP_REMOVE_PEER: {
        break;
      }
      default: {
        VLOG(3) << "Unknown operation: " << static_cast<int32_t>(log[0]);
      }
    }
  }
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
    std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
    std::shared_ptr<thread::GenericThreadPool> workers,
    std::shared_ptr<folly::Executor> executor [[gnu::unused]],
    std::shared_ptr<SnapshotManager> snapshotMan,
    std::shared_ptr<thrift::ThriftClientManager<cpp2::RaftexServiceAsyncClient>> clientMan,
    std::shared_ptr<kvstore::DiskManager> diskMan)
    : idStr_{folly::stringPrintf(
          "[Port: %d, Space: %d, Part: %d] ", localAddr.port, spaceId, partId)},
      clusterId_{clusterId},
      spaceId_{spaceId},
      partId_{partId},
      addr_{localAddr},
      status_{Status::STARTING},
      role_{Role::FOLLOWER},
      leader_{"", 0},
      ioThreadPool_{ioPool},
      bgWorkers_{workers},
      snapshot_(snapshotMan),
      clientMan_(clientMan),
      diskMan_(diskMan) {
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
      [this](LogID logId, TermID logTermId, ClusterID logClusterId, folly::StringPiece log) {
        return this->preProcessLog(logId, logTermId, logClusterId, log);
      },
      diskMan);

  auto pool = apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(
      FLAGS_raft_num_worker_threads);
  pool->setNamePrefix("part-executor");
  pool->start();
  executor_ = std::move(pool);
  CHECK(!!executor_) << idStr_ << "Should not be nullptr";
}

RaftPart::~RaftPart() {
  std::lock_guard<std::mutex> g(raftLock_);

  // Make sure the partition has stopped
  CHECK(status_ == Status::STOPPED);
  VLOG(1) << idStr_ << " The part has been destroyed...";
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

  // There are some rare cases that the part start as learner, but wal is not empty. For example,
  // the node is dead, and one partition is removed from raft group (majority still alive). However,
  // the part is added back to raft group again as learner. So the wal may be not empty, what is
  // worse, there could be case that commitLogId is 0, but wal's lastLogId is not 0, which is
  // obviously not expected
  if (asLearner) {
    wal_->reset();
  }

  lastLogId_ = wal_->lastLogId();
  lastLogTerm_ = wal_->lastLogTerm();
  term_ = lastLogTerm_;

  // Set the quorum number
  quorum_ = (peers.size() + 1) / 2;

  auto logIdAndTerm = lastCommittedLogId();
  committedLogId_ = logIdAndTerm.first;
  committedLogTerm_ = logIdAndTerm.second;

  if (lastLogId_ < committedLogId_) {
    VLOG(1) << idStr_ << "Reset lastLogId " << lastLogId_ << " to be the committedLogId "
            << committedLogId_;
    lastLogId_ = committedLogId_;
    lastLogTerm_ = committedLogTerm_;
    wal_->reset();
  }
  VLOG(1) << idStr_ << "There are " << peers.size() << " peer hosts, and total " << peers.size() + 1
          << " copies. The quorum is " << quorum_ + 1 << ", as learner " << asLearner
          << ", lastLogId " << lastLogId_ << ", lastLogTerm " << lastLogTerm_ << ", committedLogId "
          << committedLogId_ << ", committedLogTerm " << committedLogTerm_ << ", term " << term_;

  // Start all peer hosts
  for (auto& addr : peers) {
    VLOG(1) << idStr_ << "Add peer " << addr;
    auto hostPtr = std::make_shared<Host>(addr, shared_from_this());
    hosts_.emplace_back(hostPtr);
  }

  // Change the status
  status_ = Status::RUNNING;
  if (asLearner) {
    role_ = Role::LEARNER;
  }
  leader_ = HostAddr("", 0);
  startTimeMs_ = time::WallClock::fastNowInMilliSec();
  // Set up a leader election task
  size_t delayMS = 100 + folly::Random::rand32(900);
  bgWorkers_->addDelayTask(delayMS, [self = shared_from_this(), startTime = startTimeMs_] {
    self->statusPolling(startTime);
  });
}

void RaftPart::stop() {
  VLOG(1) << idStr_ << "Stopping the partition";

  decltype(hosts_) hosts;
  {
    std::lock_guard<std::mutex> lck(raftLock_);
    status_ = Status::STOPPED;
    leader_ = {"", 0};
    role_ = Role::FOLLOWER;

    hosts = std::move(hosts_);
  }

  for (auto& h : hosts) {
    h->stop();
  }

  VLOG(1) << idStr_ << "Invoked stop() on all peer hosts";

  // Host::waitForStop will wait a callback executed in ioThreadPool, so make sure the
  // RaftPart::stop SHOULD NOT be executed in the same ioThreadPool
  for (auto& h : hosts) {
    VLOG(1) << idStr_ << "Waiting " << h->idStr() << " to stop";
    h->waitForStop();
    VLOG(1) << idStr_ << h->idStr() << "has stopped";
  }
  hosts.clear();
  VLOG(1) << idStr_ << "Partition has been stopped";
}

std::pair<TermID, RaftPart::Role> RaftPart::getTermAndRole() const {
  std::lock_guard<std::mutex> g(raftLock_);
  std::pair<TermID, RaftPart::Role> res = {term_, role_};
  return res;
}

void RaftPart::cleanWal() {
  std::lock_guard<std::mutex> g(raftLock_);
  wal()->cleanWAL(committedLogId_);
}

nebula::cpp2::ErrorCode RaftPart::canAppendLogs() {
  DCHECK(!raftLock_.try_lock());
  if (UNLIKELY(status_ != Status::RUNNING)) {
    VLOG(3) << idStr_ << "The partition is not running";
    return nebula::cpp2::ErrorCode::E_RAFT_STOPPED;
  }
  if (UNLIKELY(role_ != Role::LEADER)) {
    VLOG_EVERY_N(2, 1000) << idStr_ << "The partition is not a leader";
    return nebula::cpp2::ErrorCode::E_LEADER_CHANGED;
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode RaftPart::canAppendLogs(TermID termId) {
  DCHECK(!raftLock_.try_lock());
  nebula::cpp2::ErrorCode rc = canAppendLogs();
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return rc;
  }
  if (UNLIKELY(term_ != termId)) {
    VLOG(3) << idStr_ << "Term has been updated, origin " << termId << ", new " << term_;
    return nebula::cpp2::ErrorCode::E_RAFT_TERM_OUT_OF_DATE;
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

void RaftPart::addLearner(const HostAddr& addr, bool needLock) {
  auto addLearner = [&] {
    if (addr == addr_) {
      VLOG(1) << idStr_ << "I am learner!";
      return;
    }
    auto it = std::find_if(
        hosts_.begin(), hosts_.end(), [&addr](const auto& h) { return h->address() == addr; });
    if (it == hosts_.end()) {
      hosts_.emplace_back(std::make_shared<Host>(addr, shared_from_this(), true));
      VLOG(1) << idStr_ << "Add learner " << addr;
    } else {
      VLOG(1) << idStr_ << "The host " << addr << " has been existed as "
              << ((*it)->isLearner() ? " learner " : " group member");
    }
  };
  if (needLock) {
    std::lock_guard<std::mutex> guard(raftLock_);
    addLearner();
  } else {
    addLearner();
  }
}

void RaftPart::preProcessTransLeader(const HostAddr& target) {
  CHECK(!raftLock_.try_lock());
  VLOG(1) << idStr_ << "Pre process transfer leader to " << target;
  switch (role_) {
    case Role::FOLLOWER: {
      if (target != addr_ && target != HostAddr("", 0)) {
        VLOG(1) << idStr_ << "I am follower, just wait for the new leader.";
      } else {
        VLOG(1) << idStr_ << "I will be the new leader, trigger leader election now!";
        bgWorkers_->addTask([self = shared_from_this()] {
          {
            std::lock_guard<std::mutex> lck(self->raftLock_);
            self->role_ = Role::CANDIDATE;
            self->leader_ = HostAddr("", 0);
          }
          // skip prevote for transfer leader
          self->leaderElection(false).get();
        });
      }
      break;
    }
    default: {
      VLOG(1) << idStr_ << "My role is " << roleStr(role_)
              << ", so do nothing when pre process transfer leader";
      break;
    }
  }
}

void RaftPart::commitTransLeader(const HostAddr& target, bool needLock) {
  auto transfer = [&] {
    VLOG(1) << idStr_ << "Commit transfer leader to " << target;
    switch (role_) {
      case Role::LEADER: {
        if (target != addr_ && !hosts_.empty()) {
          auto iter = std::find_if(
              hosts_.begin(), hosts_.end(), [](const auto& h) { return !h->isLearner(); });
          if (iter != hosts_.end()) {
            lastMsgRecvDur_.reset();
            role_ = Role::FOLLOWER;
            leader_ = HostAddr("", 0);
            for (auto& host : hosts_) {
              host->pause();
            }
            VLOG(1) << idStr_ << "Give up my leadership!";
          }
        } else {
          VLOG(1) << idStr_ << "I am already the leader!";
        }
        break;
      }
      case Role::FOLLOWER:
      case Role::CANDIDATE: {
        VLOG(1) << idStr_ << "I am " << roleStr(role_) << ", just wait for the new leader!";
        break;
      }
      case Role::LEARNER: {
        VLOG(1) << idStr_ << "I am learner, not in the raft group, skip the log";
        break;
      }
    }
  };
  if (needLock) {
    std::lock_guard<std::mutex> guard(raftLock_);
    transfer();
  } else {
    transfer();
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

bool RaftPart::checkAlive(const HostAddr& addr) {
  static const int64_t kTimeoutInMs = FLAGS_raft_heartbeat_interval_secs * 1000 * 2;
  int64_t now = time::WallClock::fastNowInMilliSec();
  auto it = std::find_if(
      hosts_.begin(), hosts_.end(), [&addr](const auto& h) { return h->address() == addr; });
  if (it == hosts_.end()) {
    return false;
  }
  auto last = it->get()->getLastHeartbeatTime();
  if (now - last > kTimeoutInMs) {
    return false;
  }
  return true;
}

void RaftPart::addPeer(const HostAddr& peer) {
  CHECK(!raftLock_.try_lock());
  if (peer == addr_) {
    if (role_ == Role::LEARNER) {
      VLOG(1) << idStr_ << "I am learner, promote myself to be follower";
      role_ = Role::FOLLOWER;
      updateQuorum();
    } else {
      VLOG(1) << idStr_ << "I am already in the raft group!";
    }
    return;
  }
  auto it = std::find_if(
      hosts_.begin(), hosts_.end(), [&peer](const auto& h) { return h->address() == peer; });
  if (it == hosts_.end()) {
    hosts_.emplace_back(std::make_shared<Host>(peer, shared_from_this()));
    updateQuorum();
    VLOG(1) << idStr_ << "Add peer " << peer;
  } else {
    if ((*it)->isLearner()) {
      VLOG(1) << idStr_ << "The host " << peer << " has been existed as learner, promote it!";
      (*it)->setLearner(false);
      updateQuorum();
    } else {
      VLOG(1) << idStr_ << "The host " << peer << " has been existed as follower!";
    }
  }
}

void RaftPart::removePeer(const HostAddr& peer) {
  CHECK(!raftLock_.try_lock());
  if (peer == addr_) {
    // The part will be removed in REMOVE_PART_ON_SRC phase
    VLOG(1) << idStr_ << "Remove myself from the raft group.";
    return;
  }
  auto it = std::find_if(
      hosts_.begin(), hosts_.end(), [&peer](const auto& h) { return h->address() == peer; });
  if (it == hosts_.end()) {
    VLOG(1) << idStr_ << "The peer " << peer << " not exist!";
  } else {
    if ((*it)->isLearner()) {
      VLOG(1) << idStr_ << "The peer is learner, remove it directly!";
      hosts_.erase(it);
      return;
    }
    hosts_.erase(it);
    updateQuorum();
    VLOG(1) << idStr_ << "Remove peer " << peer;
  }
}

nebula::cpp2::ErrorCode RaftPart::checkPeer(const HostAddr& candidate) {
  CHECK(!raftLock_.try_lock());
  auto hosts = followers();
  auto it = std::find_if(hosts.begin(), hosts.end(), [&candidate](const auto& h) {
    return h->address() == candidate;
  });
  if (it == hosts.end()) {
    VLOG(2) << idStr_ << "The candidate " << candidate << " is not in my peers";
    return nebula::cpp2::ErrorCode::E_RAFT_INVALID_PEER;
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

void RaftPart::addListenerPeer(const HostAddr& listener) {
  std::lock_guard<std::mutex> guard(raftLock_);
  if (listener == addr_) {
    VLOG(1) << idStr_ << "I am already in the raft group";
    return;
  }
  auto it = std::find_if(hosts_.begin(), hosts_.end(), [&listener](const auto& h) {
    return h->address() == listener;
  });
  if (it == hosts_.end()) {
    // Add listener as a raft learner
    hosts_.emplace_back(std::make_shared<Host>(listener, shared_from_this(), true));
    listeners_.emplace(listener);
    VLOG(1) << idStr_ << "Add listener " << listener;
  } else {
    VLOG(1) << idStr_ << "The listener " << listener << " has joined raft group before";
  }
}

void RaftPart::removeListenerPeer(const HostAddr& listener) {
  std::lock_guard<std::mutex> guard(raftLock_);
  if (listener == addr_) {
    VLOG(1) << idStr_ << "Remove myself from the raft group";
    return;
  }
  auto it = std::find_if(hosts_.begin(), hosts_.end(), [&listener](const auto& h) {
    return h->address() == listener;
  });
  if (it == hosts_.end()) {
    VLOG(1) << idStr_ << "The listener " << listener << " not found";
  } else {
    hosts_.erase(it);
    listeners_.erase(listener);
    VLOG(1) << idStr_ << "Remove listener " << listener;
  }
}

void RaftPart::preProcessRemovePeer(const HostAddr& peer) {
  CHECK(!raftLock_.try_lock());
  if (role_ == Role::LEADER) {
    VLOG(1) << idStr_ << "I am leader, skip remove peer in preProcessLog";
    return;
  }
  removePeer(peer);
}

void RaftPart::commitRemovePeer(const HostAddr& peer, bool needLock) {
  auto remove = [&] {
    if (role_ == Role::FOLLOWER || role_ == Role::LEARNER) {
      VLOG(1) << idStr_ << "I am " << roleStr(role_) << ", skip remove peer in commit";
      return;
    }
    CHECK(Role::LEADER == role_);
    removePeer(peer);
  };
  if (needLock) {
    std::lock_guard<std::mutex> guard(raftLock_);
    remove();
  } else {
    remove();
  }
}

folly::Future<nebula::cpp2::ErrorCode> RaftPart::appendAsync(ClusterID source, std::string log) {
  if (source < 0) {
    source = clusterId_;
  }
  return appendLogAsync(source, LogType::NORMAL, std::move(log));
}

folly::Future<nebula::cpp2::ErrorCode> RaftPart::atomicOpAsync(kvstore::MergeableAtomicOp op) {
  return appendLogAsync(clusterId_, LogType::ATOMIC_OP, "", std::move(op));
}

folly::Future<nebula::cpp2::ErrorCode> RaftPart::sendCommandAsync(std::string log) {
  return appendLogAsync(clusterId_, LogType::COMMAND, std::move(log));
}

folly::Future<nebula::cpp2::ErrorCode> RaftPart::appendLogAsync(ClusterID source,
                                                                LogType logType,
                                                                std::string log,
                                                                kvstore::MergeableAtomicOp op) {
  if (blocking_) {
    // No need to block heartbeats and empty log.
    if ((logType == LogType::NORMAL && !log.empty()) || logType == LogType::ATOMIC_OP) {
      return nebula::cpp2::ErrorCode::E_RAFT_WRITE_BLOCKED;
    }
  }

  LogCache swappedOutLogs;
  auto retFuture = folly::Future<nebula::cpp2::ErrorCode>::makeEmpty();

  if (bufferOverFlow_) {
    VLOG_EVERY_N(2, 1000)
        << idStr_ << "The appendLog buffer is full. Please slow down the log appending rate."
        << "replicatingLogs_ :" << std::boolalpha << replicatingLogs_;
    return nebula::cpp2::ErrorCode::E_RAFT_BUFFER_OVERFLOW;
  }
  {
    std::lock_guard<std::mutex> lck(logsLock_);

    VLOG(4) << idStr_ << "Checking whether buffer overflow";

    if (logs_.size() >= FLAGS_max_batch_size) {
      // Buffer is full
      VLOG(2) << idStr_ << "The appendLog buffer is full. Please slow down the log appending rate."
              << "replicatingLogs_ :" << std::boolalpha << replicatingLogs_;
      bufferOverFlow_ = true;
      return nebula::cpp2::ErrorCode::E_RAFT_BUFFER_OVERFLOW;
    }

    VLOG(4) << idStr_ << "Appending logs to the buffer";

    // Append new logs to the buffer
    DCHECK_GE(source, 0);
    folly::Promise<nebula::cpp2::ErrorCode> promise;
    retFuture = promise.getFuture();
    logs_.emplace_back(source, logType, std::move(log), std::move(op), std::move(promise));

    bool expected = false;
    if (replicatingLogs_.compare_exchange_strong(expected, true)) {
      // We need to send logs to all followers
      VLOG(4) << idStr_ << "Preparing to send AppendLog request";
      bufferOverFlow_ = false;
    } else {
      VLOG(4) << idStr_ << "Another AppendLogs request is ongoing, just return";
      return retFuture;
    }
  }

  LogID firstId = 0;
  TermID termId = 0;
  nebula::cpp2::ErrorCode res;
  {
    std::lock_guard<std::mutex> g(raftLock_);
    res = canAppendLogs();
    if (res == nebula::cpp2::ErrorCode::SUCCEEDED) {
      firstId = lastLogId_ + 1;
      termId = term_;
    }
  }

  if (!checkAppendLogResult(res)) {
    // Mosy likely failed because the partition is not leader
    VLOG_EVERY_N(2, 1000) << idStr_ << "Cannot append logs, clean the buffer";
    return nebula::cpp2::ErrorCode::E_LEADER_CHANGED;
  }
  // Replicate buffered logs to all followers
  // Replication will happen on a separate thread and will block
  // until majority accept the logs, the leadership changes, or
  // the partition stops
  {
    std::lock_guard<std::mutex> lck(logsLock_);
    AppendLogsIteratorFactory::make(logs_, sendingLogs_);
    bufferOverFlow_ = false;
    if (sendingLogs_.empty()) {
      replicatingLogs_ = false;
      return retFuture;
    }
  }
  AppendLogsIterator it(firstId, termId, std::move(sendingLogs_));
  appendLogsInternal(std::move(it), termId);

  return retFuture;
}

void RaftPart::appendLogsInternal(AppendLogsIterator iter, TermID termId) {
  TermID currTerm = 0;
  LogID prevLogId = 0;
  TermID prevLogTerm = 0;
  LogID committed = 0;
  LogID lastId = 0;
  nebula::cpp2::ErrorCode res = nebula::cpp2::ErrorCode::SUCCEEDED;
  do {
    std::lock_guard<std::mutex> g(raftLock_);
    res = canAppendLogs(termId);
    if (res != nebula::cpp2::ErrorCode::SUCCEEDED) {
      break;
    }
    currTerm = term_;
    prevLogId = lastLogId_;
    prevLogTerm = lastLogTerm_;
    committed = committedLogId_;
    // Step 1: Write WAL
    {
      SCOPED_TIMER(
          [](uint64_t execTime) { stats::StatsManager::addValue(kAppendWalLatencyUs, execTime); });
      if (!wal_->appendLogs(iter)) {
        VLOG_EVERY_N(2, 1000) << idStr_ << "Failed to write into WAL";
        res = nebula::cpp2::ErrorCode::E_RAFT_WAL_FAIL;
        lastLogId_ = wal_->lastLogId();
        lastLogTerm_ = wal_->lastLogTerm();
        break;
      }
    }
    lastId = wal_->lastLogId();
    VLOG(4) << idStr_ << "Succeeded writing logs [" << iter.firstLogId() << ", " << lastId
            << "] to WAL";
  } while (false);

  if (!checkAppendLogResult(res)) {
    iter.commit(res);
    return;
  }
  // Step 2: Replicate to followers
  auto* eb = ioThreadPool_->getEventBase();
  replicateLogs(eb, std::move(iter), currTerm, lastId, committed, prevLogTerm, prevLogId);
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
  nebula::cpp2::ErrorCode res = nebula::cpp2::ErrorCode::SUCCEEDED;
  do {
    std::lock_guard<std::mutex> g(raftLock_);
    res = canAppendLogs(currTerm);
    if (res != nebula::cpp2::ErrorCode::SUCCEEDED) {
      lastLogId_ = wal_->lastLogId();
      lastLogTerm_ = wal_->lastLogTerm();
      break;
    }
    hosts = hosts_;
  } while (false);

  if (!checkAppendLogResult(res)) {
    VLOG(3) << idStr_ << "replicateLogs failed because of not leader or term changed";
    iter.commit(res);
    return;
  }

  VLOG_IF(1, FLAGS_trace_raft) << idStr_ << "About to replicate logs in range ["
                               << iter.firstLogId() << ", " << lastLogId << "] to all peer hosts";

  lastMsgSentDur_.reset();
  auto beforeAppendLogUs = time::WallClock::fastNowInMicroSec();
  collectNSucceeded(gen::from(hosts) |
                        gen::map([self = shared_from_this(),
                                  eb,
                                  currTerm,
                                  lastLogId,
                                  prevLogId,
                                  prevLogTerm,
                                  committedId](std::shared_ptr<Host> hostPtr) {
                          VLOG(4) << self->idStr_ << "Appending logs to " << hostPtr->idStr();
                          return via(eb, [=]() -> Future<cpp2::AppendLogResponse> {
                            return hostPtr->appendLogs(
                                eb, currTerm, lastLogId, committedId, prevLogTerm, prevLogId);
                          });
                        }) |
                        gen::as<std::vector>(),
                    // Number of succeeded required
                    quorum_,
                    // Result evaluator
                    [hosts](size_t index, cpp2::AppendLogResponse& resp) {
                      return resp.get_error_code() == nebula::cpp2::ErrorCode::SUCCEEDED &&
                             !hosts[index]->isLearner();
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
             beforeAppendLogUs](folly::Try<AppendLogResponses>&& result) mutable {
        VLOG(4) << self->idStr_ << "Received enough response";
        CHECK(!result.hasException());
        stats::StatsManager::addValue(kReplicateLogLatencyUs,
                                      time::WallClock::fastNowInMicroSec() - beforeAppendLogUs);
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

void RaftPart::processAppendLogResponses(const AppendLogResponses& resps,
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
  TermID highestTerm = currTerm;
  for (auto& res : resps) {
    if (!hosts[res.first]->isLearner() &&
        res.second.get_error_code() == nebula::cpp2::ErrorCode::SUCCEEDED) {
      ++numSucceeded;
    }
    highestTerm = std::max(highestTerm, res.second.get_current_term());
  }

  nebula::cpp2::ErrorCode res = nebula::cpp2::ErrorCode::SUCCEEDED;
  {
    std::lock_guard<std::mutex> g(raftLock_);
    if (highestTerm > term_) {
      term_ = highestTerm;
      role_ = Role::FOLLOWER;
      leader_ = HostAddr("", 0);
      lastLogId_ = wal_->lastLogId();
      lastLogTerm_ = wal_->lastLogTerm();
      // It is nature to return E_RAFT_TERM_OUT_OF_DATE or LEADER_CHANGE
      // here, but this will make caller believe this append failed
      // however, this log is replicated to followers(written in WAL)
      // and those followers may become leader (use that WAL)
      // which means this log may actually commit succeeded.
      res = nebula::cpp2::ErrorCode::E_RAFT_UNKNOWN_APPEND_LOG;
    }
  }
  if (!checkAppendLogResult(res)) {
    iter.commit(res);
    return;
  }

  if (numSucceeded >= quorum_) {
    // Majority have succeeded
    VLOG(4) << idStr_ << numSucceeded << " hosts have accepted the logs";

    do {
      std::lock_guard<std::mutex> g(raftLock_);
      res = canAppendLogs(currTerm);
      if (res != nebula::cpp2::ErrorCode::SUCCEEDED) {
        lastLogId_ = wal_->lastLogId();
        lastLogTerm_ = wal_->lastLogTerm();
        break;
      }
      lastLogId_ = lastLogId;
      lastLogTerm_ = currTerm;
    } while (false);

    if (!checkAppendLogResult(res)) {
      VLOG(3) << idStr_
              << "processAppendLogResponses failed because of not leader "
                 "or term changed";
      iter.commit(res);
      return;
    }

    {
      auto walIt = wal_->iterator(committedId + 1, lastLogId);
      // Step 3: Commit the batch
      /*
      As for leader, we did't acquire raftLock because it would block heartbeat. Instead, we
      protect the partition by the logsLock_, there won't be another out-going logs. So the third
      parameters need to be true, we would grab the lock for some special operations. Besides,
      leader need to wait all logs applied to state machine, so the second parameters need to be
      true so the second parameters need to be true.
      */
      auto [code, lastCommitId, lastCommitTerm] = commitLogs(std::move(walIt), true, true);
      if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
        std::lock_guard<std::mutex> g(raftLock_);
        CHECK_EQ(lastLogId, lastCommitId);
        committedLogId_ = lastCommitId;
        committedLogTerm_ = lastCommitTerm;
        auto nowCostMs = lastMsgSentDur_.elapsedInMSec();
        auto nowTime = static_cast<uint64_t>(time::WallClock::fastNowInMilliSec());
        if (nowTime - nowCostMs >= lastMsgAcceptedTime_ - lastMsgAcceptedCostMs_) {
          lastMsgAcceptedCostMs_ = nowCostMs;
          lastMsgAcceptedTime_ = nowTime;
        }

        if (!commitInThisTerm_) {
          commitInThisTerm_ = true;
          bgWorkers_->addTask(
              [self = shared_from_this(), term = term_] { self->onLeaderReady(term); });
        }
      } else {
        LOG(FATAL) << idStr_ << "Failed to commit logs";
      }
      VLOG(4) << idStr_ << "Leader succeeded in committing the logs " << committedId + 1 << " to "
              << lastLogId;
    }

    // at this monment, we have confidence logs should be succeeded replicated
    LogID firstId = 0;
    {
      std::lock_guard<std::mutex> lck(logsLock_);
      CHECK(replicatingLogs_);
      iter.commit();
      if (logs_.empty()) {
        // no incoming during log replication
        replicatingLogs_ = false;
        VLOG(4) << idStr_ << "No more log to be replicated";
        return;
      } else {
        // we have some new coming logs during replication
        // need to send them also
        AppendLogsIteratorFactory::make(logs_, sendingLogs_);
        bufferOverFlow_ = false;
        if (sendingLogs_.empty()) {
          replicatingLogs_ = false;
          return;
        }
        firstId = lastLogId_ + 1;
      }
    }
    AppendLogsIterator it(firstId, currTerm, std::move(sendingLogs_));
    this->appendLogsInternal(std::move(it), currTerm);
    return;
  } else {
    // Not enough hosts accepted the log, re-try
    VLOG_EVERY_N(2, 1000) << idStr_ << "Only " << numSucceeded
                          << " hosts succeeded, Need to try again";
    usleep(1000);
    replicateLogs(eb, std::move(iter), currTerm, lastLogId, committedId, prevLogTerm, prevLogId);
  }
}

bool RaftPart::needToSendHeartbeat() {
  std::lock_guard<std::mutex> g(raftLock_);
  return status_ == Status::RUNNING && role_ == Role::LEADER;
}

bool RaftPart::needToStartElection() {
  std::lock_guard<std::mutex> g(raftLock_);
  if (status_ == Status::RUNNING && role_ == Role::FOLLOWER &&
      (lastMsgRecvDur_.elapsedInMSec() >= FLAGS_raft_heartbeat_interval_secs * 1000 ||
       isBlindFollower_)) {
    VLOG(1) << idStr_ << "Start leader election, reason: lastMsgDur "
            << lastMsgRecvDur_.elapsedInMSec() << ", term " << term_;
    role_ = Role::CANDIDATE;
    leader_ = HostAddr("", 0);
  }

  return role_ == Role::CANDIDATE;
}

bool RaftPart::prepareElectionRequest(cpp2::AskForVoteRequest& req,
                                      std::vector<std::shared_ptr<Host>>& hosts,
                                      bool isPreVote) {
  std::lock_guard<std::mutex> g(raftLock_);

  // Make sure the partition is running
  if (status_ != Status::RUNNING) {
    VLOG(3) << idStr_ << "The partition is not running";
    return false;
  }

  // Make sure the role is still CANDIDATE
  if (role_ != Role::CANDIDATE) {
    VLOG(3) << idStr_ << "A leader has been elected";
    return false;
  }

  req.space_ref() = spaceId_;
  req.part_ref() = partId_;
  req.candidate_addr_ref() = addr_.host;
  req.candidate_port_ref() = addr_.port;
  req.is_pre_vote_ref() = isPreVote;
  // Use term_ + 1 to check if peers would vote for me in prevote.
  // Only increase the term when prevote succeeeded.
  if (isPreVote) {
    req.term_ref() = term_ + 1;
  } else {
    req.term_ref() = ++term_;
    // vote for myself
    votedAddr_ = addr_;
    votedTerm_ = term_;
  }
  req.last_log_id_ref() = lastLogId_;
  req.last_log_term_ref() = lastLogTerm_;

  hosts = followers();

  return true;
}

void RaftPart::getState(cpp2::GetStateResponse& resp) {
  std::lock_guard<std::mutex> g(raftLock_);
  resp.term_ref() = term_;
  resp.role_ref() = role_;
  resp.is_leader_ref() = role_ == Role::LEADER;
  resp.error_code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
  resp.committed_log_id_ref() = committedLogId_;
  resp.last_log_id_ref() = lastLogId_;
  resp.last_log_term_ref() = lastLogTerm_;
  resp.status_ref() = status_;
  std::vector<std::string> peers;
  for (auto& h : hosts_) {
    std::string str = h->address().toString();
    if (h->isLearner()) {
      str += "_learner";
    }
    peers.emplace_back(str);
  }
  resp.peers_ref() = peers;
}

bool RaftPart::processElectionResponses(const RaftPart::ElectionResponses& results,
                                        std::vector<std::shared_ptr<Host>> hosts,
                                        TermID proposedTerm,
                                        bool isPreVote) {
  std::lock_guard<std::mutex> g(raftLock_);

  if (UNLIKELY(status_ == Status::STOPPED)) {
    VLOG(3) << idStr_ << "The part has been stopped, skip the request";
    return false;
  }

  if (UNLIKELY(status_ == Status::STARTING)) {
    VLOG(3) << idStr_ << "The partition is still starting";
    return false;
  }

  if (UNLIKELY(status_ == Status::WAITING_SNAPSHOT)) {
    VLOG(3) << idStr_ << "The partition is still waiting snapshot";
    return false;
  }

  if (role_ != Role::CANDIDATE) {
    VLOG(3) << idStr_ << "Partition's role has changed to " << roleStr(role_)
            << " during the election, so discard the results";
    return false;
  }

  CHECK(role_ == Role::CANDIDATE);

  // term changed during actual leader election
  if (!isPreVote && proposedTerm != term_) {
    VLOG(2) << idStr_ << "Partition's term has changed during election, "
            << "so just ignore the respsonses, "
            << "expected " << proposedTerm << ", actual " << term_;
    return false;
  }

  size_t numSucceeded = 0;
  TermID highestTerm = isPreVote ? proposedTerm - 1 : proposedTerm;
  for (auto& r : results) {
    if (r.second.get_error_code() == nebula::cpp2::ErrorCode::SUCCEEDED) {
      ++numSucceeded;
    } else {
      VLOG(2) << idStr_ << "Receive response about askForVote from " << hosts[r.first]->address()
              << ", error code is " << apache::thrift::util::enumNameSafe(r.second.get_error_code())
              << ", isPreVote = " << isPreVote;
    }
    highestTerm = std::max(highestTerm, r.second.get_current_term());
  }

  if (highestTerm > term_) {
    term_ = highestTerm;
    role_ = Role::FOLLOWER;
    leader_ = HostAddr("", 0);
    return false;
  }

  if (numSucceeded >= quorum_) {
    if (isPreVote) {
      VLOG(1) << idStr_ << "Partition win prevote of term " << proposedTerm;
    } else {
      VLOG(1) << idStr_ << "Partition is elected as the new leader for term " << proposedTerm;
      term_ = proposedTerm;
      role_ = Role::LEADER;
      leader_ = addr_;
      isBlindFollower_ = false;
    }
    return true;
  }

  VLOG(1) << idStr_ << "Did not get enough votes from election of term " << proposedTerm
          << ", isPreVote = " << isPreVote;
  return false;
}

folly::Future<bool> RaftPart::leaderElection(bool isPreVote) {
  VLOG(1) << idStr_ << "Start leader election...";
  using namespace folly;  // NOLINT since the fancy overload of | operator

  bool expected = false;

  if (!inElection_.compare_exchange_strong(expected, true)) {
    return false;
  }

  cpp2::AskForVoteRequest voteReq;
  decltype(hosts_) hosts;
  if (!prepareElectionRequest(voteReq, hosts, isPreVote)) {
    // Suppose we have three replicas A(leader), B, C, after A crashed,
    // B, C will begin the election. B win, and send hb, C has gap with B
    // and need the snapshot from B. Meanwhile C begin the election,
    // C will be Candidate, but because C is in WAITING_SNAPSHOT,
    // so prepareElectionRequest will return false and go on the election.
    // Because C is in Candidate, so it will reject the snapshot request from B.
    // Infinite loop begins.
    // So we need to go back to the follower state to avoid the case.
    std::lock_guard<std::mutex> g(raftLock_);
    role_ = Role::FOLLOWER;
    leader_ = HostAddr("", 0);
    inElection_ = false;
    return false;
  }

  // Send out the AskForVoteRequest
  VLOG(1) << idStr_ << "Sending out an election request "
          << "(space = " << voteReq.get_space() << ", part = " << voteReq.get_part()
          << ", term = " << voteReq.get_term() << ", lastLogId = " << voteReq.get_last_log_id()
          << ", lastLogTerm = " << voteReq.get_last_log_term()
          << ", candidateIP = " << voteReq.get_candidate_addr()
          << ", candidatePort = " << voteReq.get_candidate_port() << ")"
          << ", isPreVote = " << isPreVote;

  auto proposedTerm = voteReq.get_term();
  auto resps = ElectionResponses();
  stats::StatsManager::addValue(kNumStartElect);
  if (hosts.empty()) {
    auto ret = handleElectionResponses(resps, hosts, proposedTerm, isPreVote);
    inElection_ = false;
    return ret;
  } else {
    folly::Promise<bool> promise;
    auto future = promise.getFuture();
    auto eb = ioThreadPool_->getEventBase();
    collectNSucceeded(
        gen::from(hosts) |
            gen::map([eb, self = shared_from_this(), voteReq](std::shared_ptr<Host> host) {
              VLOG(4) << self->idStr_ << "Sending AskForVoteRequest to " << host->idStr();
              return via(eb, [voteReq, host, eb]() -> Future<cpp2::AskForVoteResponse> {
                return host->askForVote(voteReq, eb);
              });
            }) |
            gen::as<std::vector>(),
        // Number of succeeded required
        quorum_,
        // Result evaluator
        [hosts](size_t idx, cpp2::AskForVoteResponse& resp) {
          return resp.get_error_code() == nebula::cpp2::ErrorCode::SUCCEEDED &&
                 !hosts[idx]->isLearner();
        })
        .via(executor_.get())
        .then([self = shared_from_this(), pro = std::move(promise), hosts, proposedTerm, isPreVote](
                  auto&& t) mutable {
          VLOG(4) << self->idStr_
                  << "AskForVoteRequest has been sent to all peers, waiting for responses";
          CHECK(!t.hasException());
          pro.setValue(
              self->handleElectionResponses(t.value(), std::move(hosts), proposedTerm, isPreVote));
        });
    return future;
  }
}

bool RaftPart::handleElectionResponses(const ElectionResponses& resps,
                                       const std::vector<std::shared_ptr<Host>>& peers,
                                       TermID proposedTerm,
                                       bool isPreVote) {
  // Process the responses
  auto elected = processElectionResponses(resps, std::move(peers), proposedTerm, isPreVote);
  if (!isPreVote && elected) {
    std::vector<std::shared_ptr<Host>> hosts;
    {
      std::lock_guard<std::mutex> g(raftLock_);
      if (status_ == Status::RUNNING) {
        leader_ = addr_;
        hosts = hosts_;
        bgWorkers_->addTask(
            [self = shared_from_this(), proposedTerm] { self->onElected(proposedTerm); });
        lastMsgAcceptedTime_ = 0;
      }
      commitInThisTerm_ = false;
    }
    // reset host can't be executed with raftLock_, otherwise it may encounter deadlock
    for (auto& host : hosts) {
      host->reset();
      host->resume();
    }
    sendHeartbeat();
  }
  inElection_ = false;
  return elected;
}

void RaftPart::statusPolling(int64_t startTime) {
  {
    std::lock_guard<std::mutex> g(raftLock_);
    // If startTime is not same as the time when `statusPolling` is add to event
    // loop, it means the part has been restarted (it only happens in ut for
    // now), so don't add another `statusPolling`.
    if (startTime != startTimeMs_) {
      return;
    }
  }
  size_t delay = FLAGS_raft_heartbeat_interval_secs * 1000 / 3 + folly::Random::rand32(500);
  if (needToStartElection()) {
    if (leaderElection(true).get() && leaderElection(false).get()) {
      // elected as leader
    } else {
      // No leader has been elected, need to continue
      // (After sleeping a random period between [500ms, 2s])
      VLOG(4) << idStr_ << "Wait for a while and continue the leader election";
      delay = (folly::Random::rand32(1500) + 500);
    }
  } else if (needToSendHeartbeat()) {
    VLOG(4) << idStr_ << "Need to send heartbeat";
    sendHeartbeat();
  }
  if (needToCleanupSnapshot()) {
    cleanupSnapshot();
  }
  {
    std::lock_guard<std::mutex> g(raftLock_);
    if (status_ == Status::RUNNING || status_ == Status::WAITING_SNAPSHOT) {
      VLOG(4) << idStr_ << "Schedule new task";
      bgWorkers_->addDelayTask(
          delay, [self = shared_from_this(), startTime] { self->statusPolling(startTime); });
    }
  }
}

bool RaftPart::needToCleanupSnapshot() {
  std::lock_guard<std::mutex> g(raftLock_);
  return status_ == Status::WAITING_SNAPSHOT && role_ != Role::LEADER &&
         lastSnapshotRecvDur_.elapsedInSec() >= FLAGS_raft_snapshot_timeout;
}

void RaftPart::cleanupSnapshot() {
  VLOG(1) << idStr_
          << "Snapshot has not been received for a long time, convert to running so we can receive "
             "another snapshot";
  std::lock_guard<std::mutex> g(raftLock_);
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

void RaftPart::processAskForVoteRequest(const cpp2::AskForVoteRequest& req,
                                        cpp2::AskForVoteResponse& resp) {
  VLOG(1) << idStr_ << "Received a VOTING request"
          << ": space = " << req.get_space() << ", partition = " << req.get_part()
          << ", candidateAddr = " << req.get_candidate_addr() << ":" << req.get_candidate_port()
          << ", term = " << req.get_term() << ", lastLogId = " << req.get_last_log_id()
          << ", lastLogTerm = " << req.get_last_log_term()
          << ", isPreVote = " << req.get_is_pre_vote();

  std::lock_guard<std::mutex> g(raftLock_);
  resp.current_term_ref() = term_;

  // Make sure the partition is running
  if (UNLIKELY(status_ == Status::STOPPED)) {
    VLOG(3) << idStr_ << "The part has been stopped, skip the request";
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_STOPPED;
    return;
  }

  if (UNLIKELY(status_ == Status::STARTING)) {
    VLOG(3) << idStr_ << "The partition is still starting";
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_NOT_READY;
    return;
  }

  VLOG(1) << idStr_ << "The partition currently is a " << roleStr(role_) << ", lastLogId "
          << lastLogId_ << ", lastLogTerm " << lastLogTerm_ << ", committedLogId "
          << committedLogId_ << ", term " << term_;
  if (role_ == Role::LEARNER) {
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_BAD_ROLE;
    return;
  }

  auto candidate = HostAddr(req.get_candidate_addr(), req.get_candidate_port());
  auto code = checkPeer(candidate);
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    resp.error_code_ref() = code;
    return;
  }

  // Check term id
  if (req.get_term() < term_) {
    VLOG(1) << idStr_ << "The partition currently is on term " << term_
            << ", the term proposed by the candidate is " << req.get_term()
            << ", so it will be rejected";
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_TERM_OUT_OF_DATE;
    return;
  }

  auto oldRole = role_;
  auto oldTerm = term_;
  if (!req.get_is_pre_vote() && req.get_term() > term_) {
    // req.get_term() > term_ in formal election, update term and convert to follower
    term_ = req.get_term();
    role_ = Role::FOLLOWER;
    leader_ = HostAddr("", 0);
    resp.current_term_ref() = term_;
  } else if (req.get_is_pre_vote() && req.get_term() - 1 > term_) {
    // req.get_term() - 1 > term_ in prevote, update term and convert to follower.
    // we need to subtract 1 because the candidate's actual term is req.term() - 1
    term_ = req.get_term() - 1;
    role_ = Role::FOLLOWER;
    leader_ = HostAddr("", 0);
    resp.current_term_ref() = term_;
  }

  // Check the last term to receive a log
  if (req.get_last_log_term() < lastLogTerm_) {
    VLOG(1) << idStr_ << "The partition's last term to receive a log is " << lastLogTerm_
            << ", which is newer than the candidate's log " << req.get_last_log_term()
            << ". So the candidate will be rejected";
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_TERM_OUT_OF_DATE;
    return;
  }

  if (req.get_last_log_term() == lastLogTerm_) {
    // Check last log id
    if (req.get_last_log_id() < lastLogId_) {
      VLOG(1) << idStr_ << "The partition's last log id is " << lastLogId_
              << ". The candidate's last log id " << req.get_last_log_id()
              << " is smaller, so it will be rejected, candidate is " << candidate;
      resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_LOG_STALE;
      return;
    }
  }

  /*
  check if we have voted some one in the candidate's proposed term
  1. if this is a prevote:
    * not enough votes: the candidate will trigger another round election
    * majority votes: the candidate will start formal election (I'll reject the formal one as well)
  2. if this is a formal election:
    * not enough votes: the candidate will trigger another round election
    * majority votes: the candidate will be leader
  */
  if (votedTerm_ == req.get_term() && votedAddr_ != candidate) {
    VLOG(1) << idStr_ << "We have voted " << votedAddr_ << " on term " << votedTerm_
            << ", so we should reject the candidate " << candidate << " request on term "
            << req.get_term();
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_TERM_OUT_OF_DATE;
    return;
  }

  // Ok, no reason to refuse, we will vote for the candidate
  VLOG(1) << idStr_ << "The partition will vote for the candidate " << candidate
          << ", isPreVote = " << req.get_is_pre_vote();

  if (req.get_is_pre_vote()) {
    // return succeed if it is prevote, do not change any state
    resp.error_code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
    return;
  }

  // not a pre-vote, need to rollback wal if necessary
  // role_ and term_ has been set above
  if (oldRole == Role::LEADER) {
    if (wal_->lastLogId() > lastLogId_) {
      VLOG(2) << idStr_ << "There are some logs up to " << wal_->lastLogId()
              << " update lastLogId_ " << lastLogId_ << " to wal's";
      lastLogId_ = wal_->lastLogId();
      lastLogTerm_ = wal_->lastLogTerm();
    }
    for (auto& host : hosts_) {
      host->pause();
    }
  }
  if (oldRole == Role::LEADER) {
    bgWorkers_->addTask([self = shared_from_this(), oldTerm] { self->onLostLeadership(oldTerm); });
  }
  // not a pre-vote, req.term() >= term_, all check passed, convert to follower
  term_ = req.get_term();
  role_ = Role::FOLLOWER;
  leader_ = HostAddr("", 0);
  votedAddr_ = candidate;
  votedTerm_ = req.get_term();
  resp.error_code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
  resp.current_term_ref() = term_;

  // Reset the last message time
  lastMsgRecvDur_.reset();
  isBlindFollower_ = false;
  stats::StatsManager::addValue(kNumGrantVotes);
  return;
}

void RaftPart::processAppendLogRequest(const cpp2::AppendLogRequest& req,
                                       cpp2::AppendLogResponse& resp) {
  VLOG_IF(1, FLAGS_trace_raft) << idStr_ << "Received logAppend"
                               << ": GraphSpaceId = " << req.get_space()
                               << ", partition = " << req.get_part()
                               << ", leaderIp = " << req.get_leader_addr()
                               << ", leaderPort = " << req.get_leader_port()
                               << ", current_term = " << req.get_current_term()
                               << ", committedLogId = " << req.get_committed_log_id()
                               << ", lastLogIdSent = " << req.get_last_log_id_sent()
                               << ", lastLogTermSent = " << req.get_last_log_term_sent()
                               << ", num_logs = " << req.get_log_str_list().size()
                               << ", local lastLogId = " << lastLogId_
                               << ", local lastLogTerm = " << lastLogTerm_
                               << ", local committedLogId = " << committedLogId_
                               << ", local current term = " << term_
                               << ", wal lastLogId = " << wal_->lastLogId();
  std::lock_guard<std::mutex> g(raftLock_);

  resp.current_term_ref() = term_;
  resp.leader_addr_ref() = leader_.host;
  resp.leader_port_ref() = leader_.port;
  resp.committed_log_id_ref() = committedLogId_;
  // by default we ask leader send logs after committedLogId_
  resp.last_matched_log_id_ref() = committedLogId_;
  resp.last_matched_log_term_ref() = committedLogTerm_;

  // Check status
  if (UNLIKELY(status_ == Status::STOPPED)) {
    VLOG(3) << idStr_ << "The part has been stopped, skip the request";
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_STOPPED;
    return;
  }
  if (UNLIKELY(status_ == Status::STARTING)) {
    VLOG(3) << idStr_ << "The partition is still starting";
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_NOT_READY;
    return;
  }
  if (UNLIKELY(status_ == Status::WAITING_SNAPSHOT)) {
    VLOG(3) << idStr_ << "The partition is waiting for snapshot";
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_WAITING_SNAPSHOT;
    return;
  }
  // Check leadership
  nebula::cpp2::ErrorCode err = verifyLeader<cpp2::AppendLogRequest>(req);
  // Set term_ again because it may be modified in verifyLeader
  resp.current_term_ref() = term_;
  if (err != nebula::cpp2::ErrorCode::SUCCEEDED) {
    // Wrong leadership
    VLOG(3) << idStr_ << "Will not follow the leader";
    resp.error_code_ref() = err;
    return;
  }

  // Reset the timeout timer
  lastMsgRecvDur_.reset();

  // `lastMatchedLogId` is the last log id of which leader's and follower's log are matched
  // (which means log term of same log id are the same)
  // The relationships are as follows:
  // myself.committedLogId_ <= lastMatchedLogId <= lastLogId_
  LogID lastMatchedLogId = committedLogId_;
  do {
    size_t diffIndex = 0;
    size_t numLogs = req.get_log_str_list().size();
    LogID firstId = req.get_last_log_id_sent() + 1;
    LogID lastId = req.get_last_log_id_sent() + numLogs;
    if (req.get_last_log_id_sent() == lastLogId_ && req.get_last_log_term_sent() == lastLogTerm_) {
      // happy path: logs are matched, just append log
    } else {
      // We ask leader to send logs from committedLogId_ if one of the following occurs:
      // 1. Some of log entry in current request has been committed
      // 2. I don't have the log of req.last_log_id_sent
      // 3. My log term on req.last_log_id_sent is not same as req.last_log_term_sent
      // todo(doodle): One of the most common case when req.get_last_log_id_sent() < committedLogId_
      // is that leader timeout, and retry with same request, but follower has received it
      // previously in fact. There are two choices: ask leader to send logs after committedLogId_ or
      // just do nothing.
      if (req.get_last_log_id_sent() < committedLogId_ ||
          wal_->lastLogId() < req.get_last_log_id_sent()) {
        // case 1 and case 2
        resp.last_matched_log_id_ref() = committedLogId_;
        resp.last_matched_log_term() = committedLogTerm_;
        resp.error_code() = nebula::cpp2::ErrorCode::E_RAFT_LOG_GAP;
        return;
      }
      auto prevLogTerm = wal_->getLogTerm(req.get_last_log_id_sent());
      if (UNLIKELY(prevLogTerm == FileBasedWal::INVALID_TERM)) {
        /*
        At this point, the condition below established:
        committedLogId <= req.get_last_log_id_sent() <= wal_->lastLogId()

        When INVALID_TERM is returned, we failed to find the log of req.get_last_log_id_sent()
        in wal. This usually happens the node has received a snapshot recently.
        */
        if (req.get_last_log_id_sent() == committedLogId_ &&
            req.get_last_log_term_sent() == committedLogTerm_) {
          // remaining
          // there are any.
          // The first log of wal must be committedLogId_ + 1, it can't be 0 (no wal) as well
          // because it has been checked by case 2
          DCHECK(wal_->firstLogId() == committedLogId_ + 1);
        } else {
          // case 3: checked by committedLogId_ and committedLogTerm_
          // When log is not matched, we just return committedLogId_ and committedLogTerm_ instead
          resp.last_matched_log_id_ref() = committedLogId_;
          resp.last_matched_log_term() = committedLogTerm_;
          resp.error_code() = nebula::cpp2::ErrorCode::E_RAFT_LOG_GAP;
          return;
        }
      } else if (prevLogTerm != req.get_last_log_term_sent()) {
        // case 3
        resp.last_matched_log_id_ref() = committedLogId_;
        resp.last_matched_log_term() = committedLogTerm_;
        resp.error_code() = nebula::cpp2::ErrorCode::E_RAFT_LOG_GAP;
        return;
      }

      // wal_->logTerm(req.get_last_log_id_sent()) == req.get_last_log_term()
      // Try to find the diff point by comparing each log entry's term of same id between local wal
      // and log entry in request
      TermID lastTerm = (numLogs == 0) ? req.get_last_log_term_sent()
                                       : req.get_log_str_list().back().get_log_term();
      auto localWalIt = wal_->iterator(firstId, lastId);
      for (size_t i = 0; i < numLogs && localWalIt->valid(); ++i, ++(*localWalIt), ++diffIndex) {
        if (localWalIt->logTerm() != req.get_log_str_list()[i].get_log_term()) {
          break;
        }
      }
      if (diffIndex == numLogs) {
        // There are two cases here:
        // 1. numLogs != 0, all logs are the same in request and local wal, ask leader to send logs
        //    after lastId
        // 2. numLogs == 0, we won't append any logs below, ask leader to send logs after lastId
        lastMatchedLogId = lastId;
        resp.last_matched_log_id_ref() = lastId;
        resp.last_matched_log_term_ref() = lastTerm;
        break;
      }

      // Found a difference at log of (firstId + diffIndex), all logs from (firstId + diffIndex)
      // could be truncated
      wal_->rollbackToLog(firstId + diffIndex - 1);
      firstId = firstId + diffIndex;
      numLogs = numLogs - diffIndex;
    }

    // happy path or a difference is found: append remaining logs
    auto logEntries = std::vector<cpp2::RaftLogEntry>(
        std::make_move_iterator(req.get_log_str_list().begin() + diffIndex),
        std::make_move_iterator(req.get_log_str_list().end()));
    RaftLogIterator logIter(firstId, std::move(logEntries));
    bool hasLogsToAppend = logIter.valid();
    if (hasLogsToAppend) {
      bool result = false;
      {
        SCOPED_TIMER([](uint64_t execTime) {
          stats::StatsManager::addValue(kAppendWalLatencyUs, execTime);
        });
        result = wal_->appendLogs(logIter);
      }
      if (result) {
        CHECK_EQ(lastId, wal_->lastLogId());
        lastLogId_ = wal_->lastLogId();
        lastLogTerm_ = wal_->lastLogTerm();
        lastMatchedLogId = lastLogId_;
        resp.last_matched_log_id_ref() = lastLogId_;
        resp.last_matched_log_term_ref() = lastLogTerm_;
      } else {
        resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_WAL_FAIL;
        return;
      }
    }
  } while (false);

  // If follower found a point where log matches leader's log (lastMatchedLogId), if leader's
  // committed_log_id is greater than lastMatchedLogId, we can commit logs before lastMatchedLogId
  LogID lastLogIdCanCommit = std::min(lastMatchedLogId, req.get_committed_log_id());
  // When a node has received snapshot recently, it has no wal and its committedLogId_ may be same
  // as leader's. In this case, we skip the check of lastLogIdCanCommit
  if (wal_->lastLogId() != 0) {
    CHECK_LE(lastLogIdCanCommit, wal_->lastLogId());
  }
  if (lastLogIdCanCommit > committedLogId_) {
    auto walIt = wal_->iterator(committedLogId_ + 1, lastLogIdCanCommit);
    // follower do not wait all logs applied to state machine, so second parameter is false. And the
    // raftLock_ has been acquired, so the third parameter is false as well.
    auto [code, lastCommitId, lastCommitTerm] = commitLogs(std::move(walIt), false, false);
    if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
      VLOG(4) << idStr_ << "Follower succeeded committing log " << committedLogId_ + 1 << " to "
              << lastLogIdCanCommit;
      CHECK_EQ(lastLogIdCanCommit, lastCommitId);
      committedLogId_ = lastCommitId;
      committedLogTerm_ = lastCommitTerm;
      resp.committed_log_id_ref() = lastLogIdCanCommit;
      resp.error_code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
    } else if (code == nebula::cpp2::ErrorCode::E_WRITE_STALLED) {
      VLOG(4) << idStr_ << "Follower delay committing log " << committedLogId_ + 1 << " to "
              << lastLogIdCanCommit;
      // Even if log is not applied to state machine, still regard as succeeded:
      // 1. As a follower, upcoming request will try to commit them
      // 2. If it is elected as leader later, it will try to commit them as well
      resp.committed_log_id_ref() = committedLogId_;
      resp.error_code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
    } else {
      VLOG(3) << idStr_ << "Failed to commit log " << committedLogId_ + 1 << " to "
              << req.get_committed_log_id();
      resp.committed_log_id_ref() = committedLogId_;
      resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_WAL_FAIL;
    }
  } else {
    resp.error_code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  // Reset the timeout timer again in case wal and commit takes longer time than
  // expected
  lastMsgRecvDur_.reset();
}

template <typename REQ>
nebula::cpp2::ErrorCode RaftPart::verifyLeader(const REQ& req) {
  DCHECK(!raftLock_.try_lock());
  auto peer = HostAddr(req.get_leader_addr(), req.get_leader_port());
  auto code = checkPeer(peer);
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return code;
  }

  VLOG(4) << idStr_ << "The current role is " << roleStr(role_);
  // Make sure the remote term is greater than local's
  if (req.get_current_term() < term_) {
    VLOG(3) << idStr_ << "The current role is " << roleStr(role_) << ". The local term is " << term_
            << ". The remote term is not newer";
    return nebula::cpp2::ErrorCode::E_RAFT_TERM_OUT_OF_DATE;
  } else if (req.get_current_term() > term_) {
    // found new leader with higher term
  } else {
    // req.get_current_term() == term_
    if (UNLIKELY(leader_ == HostAddr("", 0))) {
      // I don't know who is the leader, will accept it as new leader
    } else {
      // I know who is leader
      if (LIKELY(leader_ == peer)) {
        // Same leader
        return nebula::cpp2::ErrorCode::SUCCEEDED;
      } else {
        LOG(ERROR) << idStr_ << "Split brain happens, will follow the new leader " << peer
                   << " on term " << req.get_current_term();
      }
    }
  }

  // Update my state.
  Role oldRole = role_;
  TermID oldTerm = term_;
  // Ok, no reason to refuse, just follow the leader
  VLOG(1) << idStr_ << "The current role is " << roleStr(role_) << ". Will follow the new leader "
          << peer << " on term " << req.get_current_term();

  if (role_ != Role::LEARNER) {
    role_ = Role::FOLLOWER;
  }
  leader_ = peer;
  term_ = req.get_current_term();
  isBlindFollower_ = false;
  // Before accept the logs from the new leader, check the logs locally.
  if (oldRole == Role::LEADER) {
    if (wal_->lastLogId() > lastLogId_) {
      VLOG(2) << idStr_ << "There are some logs up to " << wal_->lastLogId()
              << " update lastLogId_ " << lastLogId_ << " to wal's";
      lastLogId_ = wal_->lastLogId();
      lastLogTerm_ = wal_->lastLogTerm();
    }
    for (auto& host : hosts_) {
      host->pause();
    }
  }
  if (oldRole == Role::LEADER) {
    // Need to invoke onLostLeadership callback
    bgWorkers_->addTask([self = shared_from_this(), oldTerm] { self->onLostLeadership(oldTerm); });
  }
  bgWorkers_->addTask([self = shared_from_this()] { self->onDiscoverNewLeader(self->leader_); });
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

void RaftPart::processHeartbeatRequest(const cpp2::HeartbeatRequest& req,
                                       cpp2::HeartbeatResponse& resp) {
  VLOG_IF(1, FLAGS_trace_raft) << idStr_ << "Received heartbeat"
                               << ": GraphSpaceId = " << req.get_space()
                               << ", partition = " << req.get_part()
                               << ", leaderIp = " << req.get_leader_addr()
                               << ", leaderPort = " << req.get_leader_port()
                               << ", current_term = " << req.get_current_term()
                               << ", committedLogId = " << req.get_committed_log_id()
                               << ", lastLogIdSent = " << req.get_last_log_id_sent()
                               << ", lastLogTermSent = " << req.get_last_log_term_sent()
                               << ", local lastLogId = " << lastLogId_
                               << ", local lastLogTerm = " << lastLogTerm_
                               << ", local committedLogId = " << committedLogId_
                               << ", local current term = " << term_;
  std::lock_guard<std::mutex> g(raftLock_);

  // As for heartbeat, last_log_id and last_log_term is not checked by leader, follower only verify
  // whether leader is legal, just return lastLogId_ and lastLogTerm_ in resp. And we don't do any
  // log appending.
  // Leader will check the current_term in resp, if req.term < term_ (follower will reject req in
  // verifyLeader), leader will update term and step down as follower.
  resp.current_term_ref() = term_;
  resp.leader_addr_ref() = leader_.host;
  resp.leader_port_ref() = leader_.port;
  resp.committed_log_id_ref() = committedLogId_;
  resp.last_log_id_ref() = lastLogId_;
  resp.last_log_term_ref() = lastLogTerm_;

  // Check status
  if (UNLIKELY(status_ == Status::STOPPED)) {
    VLOG(3) << idStr_ << "The part has been stopped, skip the request";
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_STOPPED;
    return;
  }
  if (UNLIKELY(status_ == Status::STARTING)) {
    VLOG(3) << idStr_ << "The partition is still starting";
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_NOT_READY;
    return;
  }
  // Check leadership
  nebula::cpp2::ErrorCode err = verifyLeader<cpp2::HeartbeatRequest>(req);
  // Set term_ again because it may be modified in verifyLeader
  resp.current_term_ref() = term_;
  if (err != nebula::cpp2::ErrorCode::SUCCEEDED) {
    // Wrong leadership
    VLOG(3) << idStr_ << "Will not follow the leader";
    resp.error_code_ref() = err;
    return;
  }

  // Reset the timeout timer
  lastMsgRecvDur_.reset();

  // As for heartbeat, return ok after verifyLeader
  resp.error_code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
  return;
}

void RaftPart::processSendSnapshotRequest(const cpp2::SendSnapshotRequest& req,
                                          cpp2::SendSnapshotResponse& resp) {
  VLOG(2) << idStr_ << "Receive snapshot from " << req.get_leader_addr() << ":"
          << req.get_leader_port() << ", commit log id " << req.get_committed_log_id()
          << ", commit log term " << req.get_committed_log_term() << ", leader has sent "
          << req.get_total_count() << " logs of size " << req.get_total_size() << ", finished "
          << req.get_done();

  std::lock_guard<std::mutex> g(raftLock_);
  // Check status
  if (UNLIKELY(status_ == Status::STOPPED)) {
    VLOG(3) << idStr_ << "The part has been stopped, skip the request";
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_STOPPED;
    return;
  }
  if (UNLIKELY(status_ == Status::STARTING)) {
    VLOG(3) << idStr_ << "The partition is still starting";
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_NOT_READY;
    return;
  }
  resp.current_term_ref() = req.get_current_term();
  if (status_ != Status::WAITING_SNAPSHOT) {
    VLOG(2) << idStr_ << "Begin to receive the snapshot";
    // Check leadership
    nebula::cpp2::ErrorCode err = verifyLeader<cpp2::SendSnapshotRequest>(req);
    // Set term_ again because it may be modified in verifyLeader
    resp.current_term_ref() = term_;
    if (err != nebula::cpp2::ErrorCode::SUCCEEDED) {
      // Wrong leadership
      VLOG(3) << idStr_ << "Will not follow the leader";
      resp.error_code_ref() = err;
      return;
    }
    reset();
    status_ = Status::WAITING_SNAPSHOT;
    lastSnapshotCommitId_ = req.get_committed_log_id();
    lastSnapshotCommitTerm_ = req.get_committed_log_term();
    lastTotalCount_ = 0;
    lastTotalSize_ = 0;
  } else if (lastSnapshotCommitId_ != req.get_committed_log_id() ||
             lastSnapshotCommitTerm_ != req.get_committed_log_term()) {
    // Still waiting for snapshot from another peer, just return error. If the peer doesn't
    // send any logs during raft_snapshot_timeout, will convert to Status::RUNNING, so we can accept
    // snapshot again
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_WAITING_SNAPSHOT;
    return;
  }
  if (term_ > req.get_current_term()) {
    VLOG(2) << idStr_ << "leader changed, new term " << term_
            << " but continue to receive snapshot from old leader " << req.get_leader_addr()
            << " of term " << req.get_current_term();
  }
  lastSnapshotRecvDur_.reset();
  // TODO(heng): Maybe we should save them into one sst firstly?
  auto ret = commitSnapshot(
      req.get_rows(), req.get_committed_log_id(), req.get_committed_log_term(), req.get_done());
  if (std::get<0>(ret) != nebula::cpp2::ErrorCode::SUCCEEDED) {
    VLOG(2) << idStr_ << "Persist snapshot failed";
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_PERSIST_SNAPSHOT_FAILED;
    return;
  }
  lastTotalCount_ += std::get<1>(ret);
  lastTotalSize_ += std::get<2>(ret);
  if (lastTotalCount_ != req.get_total_count() || lastTotalSize_ != req.get_total_size()) {
    VLOG(2) << idStr_ << "Bad snapshot, total rows received " << lastTotalCount_
            << ", total rows sended " << req.get_total_count() << ", total size received "
            << lastTotalSize_ << ", total size sended " << req.get_total_size();
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_PERSIST_SNAPSHOT_FAILED;
    return;
  }
  if (req.get_done()) {
    committedLogId_ = req.get_committed_log_id();
    committedLogTerm_ = req.get_committed_log_term();
    lastLogId_ = committedLogId_;
    lastLogTerm_ = committedLogTerm_;
    // there should be no wal after state converts to WAITING_SNAPSHOT, the RaftPart has been reset
    DCHECK_EQ(wal_->firstLogId(), 0);
    DCHECK_EQ(wal_->lastLogId(), 0);
    status_ = Status::RUNNING;
    VLOG(1) << idStr_ << "Receive all snapshot, committedLogId_ " << committedLogId_
            << ", committedLogTerm_ " << committedLogTerm_ << ", lastLogId " << lastLogId_
            << ", lastLogTermId " << lastLogTerm_;
  }
  resp.error_code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
  return;
}

void RaftPart::sendHeartbeat() {
  // If leader has not commit any logs in this term, it must commit all logs in
  // previous term, so heartbeat is send by appending one empty log.
  if (!replicatingLogs_.load(std::memory_order_acquire)) {
    folly::via(executor_.get(), [this] {
      std::string log = "";
      appendLogAsync(clusterId_, LogType::NORMAL, std::move(log));
    });
  }

  using namespace folly;  // NOLINT since the fancy overload of | operator
  VLOG(2) << idStr_ << "Send heartbeat";
  TermID currTerm = 0;
  LogID commitLogId = 0;
  TermID prevLogTerm = 0;
  LogID prevLogId = 0;
  size_t replica = 0;
  decltype(hosts_) hosts;
  {
    std::lock_guard<std::mutex> g(raftLock_);
    nebula::cpp2::ErrorCode rc = canAppendLogs();
    if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return;
    }
    currTerm = term_;
    commitLogId = committedLogId_;
    prevLogTerm = lastLogTerm_;
    prevLogId = lastLogId_;
    replica = quorum_;
    hosts = hosts_;
  }
  auto eb = ioThreadPool_->getEventBase();
  auto startMs = time::WallClock::fastNowInMilliSec();
  collectNSucceeded(
      gen::from(hosts) |
          gen::map([self = shared_from_this(), eb, currTerm, commitLogId, prevLogId, prevLogTerm](
                       std::shared_ptr<Host> hostPtr) {
            VLOG(4) << self->idStr_ << "Send heartbeat to " << hostPtr->idStr();
            return via(eb, [=]() -> Future<cpp2::HeartbeatResponse> {
              return hostPtr->sendHeartbeat(eb, currTerm, commitLogId, prevLogTerm, prevLogId);
            });
          }) |
          gen::as<std::vector>(),
      // Number of succeeded required
      hosts.size(),
      // Result evaluator
      [hosts](size_t index, cpp2::HeartbeatResponse& resp) {
        return resp.get_error_code() == nebula::cpp2::ErrorCode::SUCCEEDED &&
               !hosts[index]->isLearner();
      })
      .then([replica, hosts = std::move(hosts), startMs, currTerm, this](
                folly::Try<HeartbeatResponses>&& resps) {
        CHECK(!resps.hasException());
        size_t numSucceeded = 0;
        TermID highestTerm = currTerm;
        for (auto& resp : *resps) {
          if (!hosts[resp.first]->isLearner() &&
              resp.second.get_error_code() == nebula::cpp2::ErrorCode::SUCCEEDED) {
            ++numSucceeded;
            // only metad 0 space 0 part need this state now.
            if (spaceId_ == kDefaultSpaceId) {
              hosts[resp.first]->setLastHeartbeatTime(time::WallClock::fastNowInMilliSec());
            }
          }
          highestTerm = std::max(highestTerm, resp.second.get_current_term());
        }
        {
          std::lock_guard<std::mutex> g(raftLock_);
          if (highestTerm > term_) {
            term_ = highestTerm;
            role_ = Role::FOLLOWER;
            leader_ = HostAddr("", 0);
            return;
          }
        }
        if (numSucceeded >= replica) {
          VLOG(4) << idStr_ << "Heartbeat is accepted by quorum";
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

bool RaftPart::checkAppendLogResult(nebula::cpp2::ErrorCode res) {
  if (res != nebula::cpp2::ErrorCode::SUCCEEDED) {
    {
      std::lock_guard<std::mutex> lck(logsLock_);
      auto setPromiseForLogs = [&](LogCache& logs) {
        for (auto& log : logs) {
          auto& promiseRef = std::get<4>(log);
          if (!promiseRef.isFulfilled()) {
            promiseRef.setValue(res);
          }
        }
      };
      setPromiseForLogs(logs_);
      setPromiseForLogs(sendingLogs_);
      logs_.clear();
      sendingLogs_.clear();
      bufferOverFlow_ = false;
      replicatingLogs_ = false;
    }
    return false;
  }
  return true;
}

void RaftPart::reset() {
  CHECK(!raftLock_.try_lock());
  wal_->reset();
  cleanup();
  lastLogId_ = committedLogId_ = 0;
  lastLogTerm_ = committedLogTerm_ = 0;
}

nebula::cpp2::ErrorCode RaftPart::isCaughtUp(const HostAddr& peer) {
  std::lock_guard<std::mutex> lck(raftLock_);
  VLOG(2) << idStr_ << "Check whether I catch up";
  if (role_ != Role::LEADER) {
    VLOG(2) << idStr_ << "I am not the leader";
    return nebula::cpp2::ErrorCode::E_LEADER_CHANGED;
  }
  if (peer == addr_) {
    VLOG(2) << idStr_ << "I am the leader";
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  for (auto& host : hosts_) {
    if (host->addr_ == peer) {
      if (host->followerCommittedLogId_ == 0 ||
          host->followerCommittedLogId_ < wal_->firstLogId()) {
        VLOG(2) << idStr_ << "The committed log id of peer is " << host->followerCommittedLogId_
                << ", which is invalid or less than my first wal log id";
        return nebula::cpp2::ErrorCode::E_RAFT_SENDING_SNAPSHOT;
      }
      return host->sendingSnapshot_ ? nebula::cpp2::ErrorCode::E_RAFT_SENDING_SNAPSHOT
                                    : nebula::cpp2::ErrorCode::SUCCEEDED;
    }
  }
  return nebula::cpp2::ErrorCode::E_RAFT_INVALID_PEER;
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
    VLOG(1) << idStr_ << "Check host " << h->addr_;
    auto it = std::find(peers.begin(), peers.end(), h->addr_);
    if (it == peers.end()) {
      VLOG(1) << idStr_ << "The peer " << h->addr_ << " should not exist in my peers";
      removePeer(h->addr_);
    }
  }
  for (auto& p : peers) {
    VLOG(1) << idStr_ << "Add peer " << p << " if not exist!";
    addPeer(p);
  }
}

void RaftPart::checkRemoteListeners(const std::set<HostAddr>& expected) {
  auto actual = listeners();
  for (const auto& host : actual) {
    auto it = std::find(expected.begin(), expected.end(), host);
    if (it == expected.end()) {
      VLOG(1) << idStr_ << "The listener " << host << " should not exist in my peers";
      removeListenerPeer(host);
    }
  }
  for (const auto& host : expected) {
    auto it = std::find(actual.begin(), actual.end(), host);
    if (it == actual.end()) {
      VLOG(1) << idStr_ << "Add listener " << host << " to my peers";
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
  // When majority has accepted a log, leader obtains a lease which last for
  // heartbeat. However, we need to take off the net io time. On the left side
  // of the inequality is the time duration since last time leader send a log
  // (the log has been accepted as well)
  return time::WallClock::fastNowInMilliSec() - lastMsgAcceptedTime_ <
         FLAGS_raft_heartbeat_interval_secs * 1000 - lastMsgAcceptedCostMs_;
}

}  // namespace raftex
}  // namespace nebula
