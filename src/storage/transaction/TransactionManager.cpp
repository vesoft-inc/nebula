/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/TransactionManager.h"

#include <folly/Format.h>                          // for sformat
#include <folly/Range.h>                           // for StringPiece
#include <folly/String.h>                          // for hexlify
#include <folly/Try.h>                             // for Try::~Try<T>
#include <folly/Unit.h>                            // for Unit
#include <folly/executors/IOThreadPoolExecutor.h>  // for IOThreadPoolE...
#include <folly/executors/ThreadPoolExecutor.h>    // for ThreadPoolExe...
#include <folly/futures/Future-pre.h>              // for valueCallable...
#include <folly/futures/Future.h>                  // for SemiFuture::~...
#include <folly/futures/Future.h>                  // for Future, Futur...
#include <folly/futures/Future.h>                  // for SemiFuture::~...
#include <folly/futures/Future.h>                  // for Future, Futur...
#include <folly/futures/Promise.h>                 // for Promise::Prom...
#include <folly/futures/Promise.h>                 // for PromiseExcept...
#include <folly/futures/Promise.h>                 // for Promise::Prom...
#include <folly/futures/Promise.h>                 // for PromiseExcept...
#include <folly/hash/Hash.h>                       // for hash
#include <gflags/gflags.h>                         // for DEFINE_int32
#include <thrift/lib/cpp/util/EnumUtils.h>         // for enumNameSafe

#include <chrono>       // for seconds
#include <functional>   // for _Bind, bind, _1
#include <ostream>      // for operator<<
#include <thread>       // for sleep_for
#include <type_traits>  // for remove_refere...

#include "clients/meta/MetaClient.h"                    // for MetaClient
#include "common/base/ErrorOr.h"                        // for error, ok, value
#include "common/base/Logging.h"                        // for LogMessage, LOG
#include "common/base/StatusOr.h"                       // for StatusOr
#include "kvstore/KVIterator.h"                         // for KVIterator
#include "kvstore/KVStore.h"                            // for KVStore
#include "kvstore/NebulaStore.h"                        // for NebulaStore
#include "storage/CommonUtils.h"                        // for StorageEnv
#include "storage/transaction/ChainBaseProcessor.h"     // for ChainBaseProc...
#include "storage/transaction/ChainProcessorFactory.h"  // for ChainProcesso...
#include "storage/transaction/ConsistUtil.h"            // for ConsistUtil

namespace folly {
class EventBase;

class EventBase;
}  // namespace folly

namespace nebula {
namespace storage {

DEFINE_int32(resume_interval_secs, 10, "Resume interval");
DEFINE_int32(toss_worker_num, 16, "Resume interval");

TransactionManager::TransactionManager(StorageEnv* env) : env_(env) {
  LOG(INFO) << "TransactionManager ctor()";
  exec_ = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_toss_worker_num);
}

bool TransactionManager::start() {
  std::vector<std::pair<GraphSpaceID, PartitionID>> existParts;
  auto fn = std::bind(&TransactionManager::onNewPartAdded, this, std::placeholders::_1);
  static_cast<::nebula::kvstore::NebulaStore*>(env_->kvstore_)
      ->registerOnNewPartAdded("TransactionManager", fn, existParts);
  for (auto&& [spaceId, partId] : existParts) {
    auto [termId, rc] = getTermFromKVStore(spaceId, partId);
    if (rc != Code::SUCCEEDED) {
      continue;
    }
    scanPrimes(spaceId, partId, termId);
  }
  return true;
}

void TransactionManager::monitorPoolStat(folly::ThreadPoolExecutor* pool, const std::string& msg) {
  monPoolStats_.emplace_back(std::make_pair(pool, msg));
}

void TransactionManager::bgPrintPoolStat() {
  while (!stop_) {
    for (auto&& [pool, msg] : monPoolStats_) {
      VLOG(1) << dumpPoolStat(pool, msg);
    }
    std::this_thread::sleep_for(std::chrono::seconds(20));
  }
}

std::string TransactionManager::dumpPoolStat(folly::ThreadPoolExecutor* exec,
                                             const std::string& msg) {
  auto stats = exec->getPoolStats();
  std::stringstream oss;
  oss << "\npoolStats: " << msg << "\n\t threadCount = " << stats.threadCount
      << "\n\t idleThreadCount = " << stats.idleThreadCount
      << "\n\t activeThreadCount = " << stats.activeThreadCount
      << "\n\t pendingTaskCount = " << stats.pendingTaskCount
      << "\n\t totalTaskCount = " << stats.totalTaskCount << "\n";
  return oss.str();
}

void TransactionManager::stop() {
  LOG(INFO) << "TransactionManager stop()";
  stop_ = true;
}

void TransactionManager::join() {
  LOG(INFO) << "TransactionManager join()";
  exec_->stop();
}

void TransactionManager::addChainTask(ChainBaseProcessor* proc) {
  if (stop_) {
    return;
  }
  folly::via(exec_.get())
      .thenValue([=](auto&&) { return proc->prepareLocal(); })
      .thenValue([=](auto&& code) { return proc->processRemote(code); })
      .thenValue([=](auto&& code) { return proc->processLocal(code); })
      .ensure([=]() { proc->finish(); });
}

TransactionManager::SPtrLock TransactionManager::getLockCore(GraphSpaceID spaceId,
                                                             GraphSpaceID partId,
                                                             TermID termId,
                                                             bool checkWhiteList) {
  if (checkWhiteList) {
    auto currTermKey = std::make_pair(spaceId, partId);
    auto it = currTerm_.find(currTermKey);
    if (it == currTerm_.end()) {
      return nullptr;
    }
    if (it->second != termId) {
      return nullptr;
    }
  }
  MemLockKey key = std::make_tuple(spaceId, partId, termId);
  auto it = memLocks_.find(key);
  if (it != memLocks_.end()) {
    return it->second;
  }

  auto item = memLocks_.insert(key, std::make_shared<LockCore>());
  return item.first->second;
}

std::pair<TermID, Code> TransactionManager::getTermFromKVStore(GraphSpaceID spaceId,
                                                               PartitionID partId) {
  TermID termId = -1;
  auto rc = Code::SUCCEEDED;
  auto part = env_->kvstore_->part(spaceId, partId);
  if (nebula::ok(part)) {
    termId = nebula::value(part)->termId();
  } else {
    rc = nebula::error(part);
  }
  return std::make_pair(termId, rc);
}

bool TransactionManager::checkTermFromCache(GraphSpaceID spaceId,
                                            PartitionID partId,
                                            TermID termId) {
  auto termFromMeta = env_->metaClient_->getTermFromCache(spaceId, partId);
  if (termFromMeta.ok()) {
    if (termId < termFromMeta.value()) {
      LOG(WARNING) << "checkTerm() failed: "
                   << "spaceId=" << spaceId << ", partId=" << partId
                   << ", in-coming term=" << termId
                   << ", term in meta cache=" << termFromMeta.value();
      return false;
    }
  }
  auto partUUID = std::make_pair(spaceId, partId);
  auto it = cachedTerms_.find(partUUID);
  if (it != cachedTerms_.cend()) {
    if (termId < it->second) {
      LOG(WARNING) << "term < it->second";
      return false;
    }
  }
  cachedTerms_.assign(partUUID, termId);
  return true;
}

void TransactionManager::addPrime(GraphSpaceID spaceId,
                                  PartitionID partId,
                                  TermID termId,
                                  const std::string& egKey,
                                  ResumeType type) {
  VLOG(2) << "addPrime() space=" << spaceId << ", hex=" << folly::hexlify(egKey)
          << ", ResumeType=" << static_cast<int>(type);
  auto* proc = ChainProcessorFactory::make(env_, spaceId, termId, egKey, type);
  if (proc == nullptr) {
    VLOG(1) << "delPrime() space=" << spaceId << ", hex=" << folly::hexlify(egKey);
    auto lk = getLockCore(spaceId, partId, termId, false);
    if (lk) {
      lk->unlock(egKey);
    }
    return;
  }
  auto fut = proc->getFinished();
  std::move(fut).thenValue([=](auto&& code) {
    if (code == Code::SUCCEEDED) {
      VLOG(2) << "delPrime() space=" << spaceId << ", hex=" << folly::hexlify(egKey);
      auto lk = getLockCore(spaceId, partId, termId, false);
      if (lk) {
        lk->unlock(egKey);
      }
    }
  });
  addChainTask(proc);
}

void TransactionManager::onNewPartAdded(std::shared_ptr<kvstore::Part>& part) {
  LOG(INFO) << folly::sformat("space={}, part={} added", part->spaceId(), part->partitionId());
  auto fnLeaderReady =
      std::bind(&TransactionManager::onLeaderElectedWrapper, this, std::placeholders::_1);
  auto fnLeaderLost =
      std::bind(&TransactionManager::onLeaderLostWrapper, this, std::placeholders::_1);
  part->registerOnLeaderReady(fnLeaderReady);
  part->registerOnLeaderLost(fnLeaderLost);
}

void TransactionManager::onLeaderLostWrapper(const ::nebula::kvstore::Part::CallbackOptions& opt) {
  LOG(INFO) << folly::sformat("leader lost, del space={}, part={}, term={} from white list",
                              opt.spaceId,
                              opt.partId,
                              opt.term);
  auto currTermKey = std::make_pair(opt.spaceId, opt.partId);
  auto currTermIter = currTerm_.find(currTermKey);
  if (currTermIter == currTerm_.end()) {
    return;
  }
  auto memLockKey = std::make_tuple(opt.spaceId, opt.partId, currTermIter->second);
  memLocks_.erase(memLockKey);
}

void TransactionManager::onLeaderElectedWrapper(
    const ::nebula::kvstore::Part::CallbackOptions& opt) {
  LOG(INFO) << folly::sformat(
      "leader get do scanPrimes space={}, part={}, term={}", opt.spaceId, opt.partId, opt.term);
  scanPrimes(opt.spaceId, opt.partId, opt.term);
}

void TransactionManager::scanPrimes(GraphSpaceID spaceId, PartitionID partId, TermID termId) {
  LOG(INFO) << folly::sformat(
      "{}(), space={}, part={}, term={}", __func__, spaceId, partId, termId);
  std::unique_ptr<kvstore::KVIterator> iter;
  auto prefix = ConsistUtil::primePrefix(partId);
  auto rc = env_->kvstore_->prefix(spaceId, partId, prefix, &iter);
  if (rc == nebula::cpp2::ErrorCode::SUCCEEDED) {
    for (; iter->valid(); iter->next()) {
      auto edgeKey = ConsistUtil::edgeKeyFromPrime(iter->key()).str();
      VLOG(1) << "scanned prime edge: " << folly::hexlify(edgeKey);
      auto lk = getLockCore(spaceId, partId, termId, false);
      auto succeed = lk->try_lock(edgeKey);
      if (!succeed) {
        LOG(ERROR) << "not supposed to lock fail: "
                   << ", spaceId " << spaceId << ", partId " << partId << ", termId " << termId
                   << folly::hexlify(edgeKey);
      }
      addPrime(spaceId, partId, termId, edgeKey, ResumeType::RESUME_CHAIN);
    }
  } else {
    VLOG(1) << "primePrefix() " << apache::thrift::util::enumNameSafe(rc);
  }

  prefix = ConsistUtil::doublePrimePrefix(partId);
  rc = env_->kvstore_->prefix(spaceId, partId, prefix, &iter);
  if (rc == nebula::cpp2::ErrorCode::SUCCEEDED) {
    for (; iter->valid(); iter->next()) {
      auto edgeKey = ConsistUtil::edgeKeyFromDoublePrime(iter->key()).str();
      VLOG(1) << "scanned double prime edge: " << folly::hexlify(edgeKey);
      auto lk = getLockCore(spaceId, partId, termId, false);
      auto succeed = lk->try_lock(edgeKey);
      if (!succeed) {
        LOG(ERROR) << "not supposed to lock fail: "
                   << ", space " << spaceId << ", partId " << partId << ", termId " << termId
                   << folly::hexlify(edgeKey);
      }
      addPrime(spaceId, partId, termId, edgeKey, ResumeType::RESUME_REMOTE);
    }
  } else {
    VLOG(1) << "doublePrimePrefix() " << apache::thrift::util::enumNameSafe(rc);
  }

  auto currTermKey = std::make_pair(spaceId, partId);
  currTerm_.insert_or_assign(currTermKey, termId);

  LOG(INFO) << "set curr term spaceId = " << spaceId << ", partId = " << partId
            << ", termId = " << termId;
}

folly::EventBase* TransactionManager::getEventBase() {
  return exec_->getEventBase();
}

}  // namespace storage
}  // namespace nebula
