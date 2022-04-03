/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/TransactionManager.h"

#include <folly/container/Enumerate.h>

#include "codec/RowWriterV2.h"
#include "common/utils/NebulaKeyUtils.h"
#include "kvstore/NebulaStore.h"
#include "storage/CommonUtils.h"
#include "storage/StorageFlags.h"
#include "storage/transaction/ChainProcessorFactory.h"

namespace nebula {
namespace storage {

DEFINE_int32(resume_interval_secs, 10, "Resume interval");
DEFINE_int32(toss_worker_num, 16, "Resume interval");

TransactionManager::TransactionManager(StorageEnv* env) : env_(env) {
  LOG(INFO) << "TransactionManager ctor()";
  worker_ = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_toss_worker_num);
  controller_ = std::make_shared<folly::IOThreadPoolExecutor>(1);
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
  worker_->stop();
  controller_->stop();
}

void TransactionManager::addChainTask(ChainBaseProcessor* proc) {
  if (stop_) {
    return;
  }
  folly::via(worker_.get())
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
  DLOG(INFO) << folly::sformat("space={}, part={} added", part->spaceId(), part->partitionId());
  auto fnLeaderReady =
      std::bind(&TransactionManager::onLeaderElectedWrapper, this, std::placeholders::_1);
  auto fnLeaderLost =
      std::bind(&TransactionManager::onLeaderLostWrapper, this, std::placeholders::_1);
  part->registerOnLeaderReady(fnLeaderReady);
  part->registerOnLeaderLost(fnLeaderLost);
}

void TransactionManager::onLeaderLostWrapper(const ::nebula::kvstore::Part::CallbackOptions& opt) {
  DLOG(INFO) << folly::sformat("leader lost, del space={}, part={}, term={} from white list",
                               opt.spaceId,
                               opt.partId,
                               opt.term);
  // clean some out-dated item in memory lock
  for (auto cit = memLocks_.cbegin(); cit != memLocks_.cend();) {
    auto& [spaceId, partId, termId] = cit->first;
    if (spaceId == opt.spaceId && partId == opt.partId && termId < opt.term) {
      auto sptrLockCore = cit->second;
      if (sptrLockCore->size() == 0) {
        cit = memLocks_.erase(cit);
        continue;
      }
    }
    ++cit;
  }
}

void TransactionManager::onLeaderElectedWrapper(
    const ::nebula::kvstore::Part::CallbackOptions& opt) {
  DLOG(INFO) << folly::sformat(
      "leader get do scanPrimes space={}, part={}, term={}", opt.spaceId, opt.partId, opt.term);
  scanPrimes(opt.spaceId, opt.partId, opt.term);
}

void TransactionManager::scanPrimes(GraphSpaceID spaceId, PartitionID partId, TermID termId) {
  DLOG(INFO) << folly::sformat(
      "{}(), space={}, part={}, term={}", __func__, spaceId, partId, termId);
  std::vector<std::string> prefixVec{ConsistUtil::primePrefix(partId),
                                     ConsistUtil::doublePrimePrefix(partId)};
  std::vector<ResumeType> resumeVec{ResumeType::RESUME_CHAIN, ResumeType::RESUME_REMOTE};
  auto termKey = std::make_pair(spaceId, partId);
  auto itCurrTerm = currTerm_.find(termKey);
  TermID prevTerm = itCurrTerm == currTerm_.end() ? -1 : itCurrTerm->second;

  std::unique_ptr<kvstore::KVIterator> iter;
  for (auto i = 0U; i != prefixVec.size(); ++i) {
    auto rc = env_->kvstore_->prefix(spaceId, partId, prefixVec[i], &iter);
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
        auto prevMemLockKey = std::make_tuple(spaceId, partId, prevTerm);
        auto prevMemLockIter = memLocks_.find(prevMemLockKey);
        auto spLock = prevMemLockIter == memLocks_.end() ? nullptr : prevMemLockIter->second;
        auto hasUnfinishedTask = [lk = spLock, key = edgeKey] {
          return (lk != nullptr) && lk->contains(key);
        };

        if (!hasUnfinishedTask()) {
          addPrime(spaceId, partId, termId, edgeKey, resumeVec[i]);
        } else {
          folly::Promise<folly::Unit> pro;
          auto fut = pro.getFuture();
          std::move(fut).thenValue(
              [=](auto&&) { addPrime(spaceId, partId, termId, edgeKey, resumeVec[i]); });
          waitUntil(std::move(hasUnfinishedTask), std::move(pro));
        }
      }
    } else {
      VLOG(1) << "primePrefix() " << apache::thrift::util::enumNameSafe(rc);
    }
  }

  currTerm_.insert_or_assign(termKey, termId);
  prevTerms_.insert_or_assign(termKey, prevTerm);

  DLOG(INFO) << "set curr term spaceId = " << spaceId << ", partId = " << partId
             << ", termId = " << termId;
}

folly::EventBase* TransactionManager::getEventBase() {
  return worker_->getEventBase();
}

void TransactionManager::waitUntil(std::function<bool()>&& cond, folly::Promise<folly::Unit>&& p) {
  controller_->add([cond = std::move(cond), p = std::move(p), this]() mutable {
    if (cond()) {
      p.setValue(folly::Unit());
    } else {
      std::this_thread::sleep_for(std::chrono::seconds(3));
      this->waitUntil(std::move(cond), std::move(p));
    }
  });
}

}  // namespace storage
}  // namespace nebula
