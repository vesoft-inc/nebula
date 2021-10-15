/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/transaction/TransactionManager.h"

#include <folly/container/Enumerate.h>

#include "codec/RowWriterV2.h"
#include "common/utils/NebulaKeyUtils.h"
#include "kvstore/NebulaStore.h"
#include "storage/CommonUtils.h"
#include "storage/StorageFlags.h"
#include "storage/transaction/ChainResumeProcessor.h"

namespace nebula {
namespace storage {

DEFINE_int32(resume_interval_secs, 10, "Resume interval");

ProcessorCounters kForwardTranxCounters;

TransactionManager::TransactionManager(StorageEnv* env) : env_(env) {
  LOG(INFO) << "TransactionManager ctor()";
  exec_ = std::make_shared<folly::IOThreadPoolExecutor>(10);
  iClient_ = env_->interClient_;
  resumeThread_ = std::make_unique<thread::GenericWorker>();
  std::vector<std::pair<GraphSpaceID, PartitionID>> existParts;
  auto fn = std::bind(&TransactionManager::onNewPartAdded, this, std::placeholders::_1);
  static_cast<::nebula::kvstore::NebulaStore*>(env_->kvstore_)
      ->registerOnNewPartAdded("TransactionManager", fn, existParts);
  for (auto& partOfSpace : existParts) {
    scanPrimes(partOfSpace.first, partOfSpace.second);
  }
}

TransactionManager::LockCore* TransactionManager::getLockCore(GraphSpaceID spaceId,
                                                              GraphSpaceID partId,
                                                              bool checkWhiteList) {
  if (checkWhiteList) {
    if (whiteListParts_.find(std::make_pair(spaceId, partId)) == whiteListParts_.end()) {
      return nullptr;
    }
  }
  auto it = memLocks_.find(spaceId);
  if (it != memLocks_.end()) {
    return it->second.get();
  }

  auto item = memLocks_.insert(spaceId, std::make_unique<LockCore>());
  return item.first->second.get();
}

StatusOr<TermID> TransactionManager::getTerm(GraphSpaceID spaceId, PartitionID partId) {
  return env_->metaClient_->getTermFromCache(spaceId, partId);
}

bool TransactionManager::checkTerm(GraphSpaceID spaceId, PartitionID partId, TermID term) {
  auto termOfMeta = env_->metaClient_->getTermFromCache(spaceId, partId);
  if (termOfMeta.ok()) {
    if (term < termOfMeta.value()) {
      LOG(WARNING) << "checkTerm() failed: "
                   << "spaceId=" << spaceId << ", partId=" << partId << ", in-coming term=" << term
                   << ", term in meta cache=" << termOfMeta.value();
      return false;
    }
  }
  auto partUUID = std::make_pair(spaceId, partId);
  auto it = cachedTerms_.find(partUUID);
  if (it != cachedTerms_.cend()) {
    if (term < it->second) {
      LOG(WARNING) << "term < it->second";
      return false;
    }
  }
  cachedTerms_.assign(partUUID, term);
  return true;
}

void TransactionManager::resumeThread() {
  SCOPE_EXIT {
    resumeThread_->addDelayTask(
        FLAGS_resume_interval_secs * 1000, &TransactionManager::resumeThread, this);
  };
  ChainResumeProcessor proc(env_);
  proc.process();
}

bool TransactionManager::start() {
  if (!resumeThread_->start()) {
    LOG(ERROR) << "resume thread start failed";
    return false;
  }
  resumeThread_->addDelayTask(
      FLAGS_resume_interval_secs * 1000, &TransactionManager::resumeThread, this);
  return true;
}

void TransactionManager::stop() {
  resumeThread_->stop();
  resumeThread_->wait();
}

std::string TransactionManager::makeLockKey(GraphSpaceID spaceId, const std::string& edge) {
  std::string lockKey;
  lockKey.append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID)).append(edge);
  return lockKey;
}

std::string TransactionManager::getEdgeKey(const std::string& lockKey) {
  std::string edgeKey(lockKey.c_str() + sizeof(GraphSpaceID));
  return edgeKey;
}

void TransactionManager::addPrime(GraphSpaceID spaceId, const std::string& edge, ResumeType type) {
  LOG_IF(INFO, FLAGS_trace_toss) << "addPrime() space=" << spaceId
                                 << ", hex=" << folly::hexlify(edge)
                                 << ", ResumeType=" << static_cast<int>(type);
  auto key = makeLockKey(spaceId, edge);
  reserveTable_.insert(std::make_pair(key, type));
}

void TransactionManager::delPrime(GraphSpaceID spaceId, const std::string& edge) {
  LOG_IF(INFO, FLAGS_trace_toss) << "delPrime() space=" << spaceId
                                 << ", hex=" << folly::hexlify(edge);
  auto key = makeLockKey(spaceId, edge);
  reserveTable_.erase(key);

  auto partId = NebulaKeyUtils::getPart(edge);
  auto* lk = getLockCore(spaceId, partId, false);
  auto lockKey = makeLockKey(spaceId, edge);
  lk->unlock(lockKey);
}

void TransactionManager::scanAll() {
  LOG(INFO) << "scanAll()";
  std::unordered_map<GraphSpaceID, std::vector<meta::cpp2::LeaderInfo>> leaders;
  if (env_->kvstore_->allLeader(leaders) == 0) {
    LOG(INFO) << "no leader found, skip any resume process";
    return;
  }
  for (auto& leader : leaders) {
    auto spaceId = leader.first;
    for (auto& partInfo : leader.second) {
      auto partId = partInfo.get_part_id();
      scanPrimes(spaceId, partId);
    }
  }
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
  whiteListParts_.erase(std::make_pair(opt.spaceId, opt.partId));
}

void TransactionManager::onLeaderElectedWrapper(
    const ::nebula::kvstore::Part::CallbackOptions& opt) {
  LOG(INFO) << folly::sformat(
      "leader get do scanPrimes space={}, part={}, term={}", opt.spaceId, opt.partId, opt.term);
  scanPrimes(opt.spaceId, opt.partId);
}

void TransactionManager::scanPrimes(GraphSpaceID spaceId, PartitionID partId) {
  LOG(INFO) << folly::sformat("{}(), spaceId={}, partId={}", __func__, spaceId, partId);
  std::unique_ptr<kvstore::KVIterator> iter;
  auto prefix = ConsistUtil::primePrefix(partId);
  auto rc = env_->kvstore_->prefix(spaceId, partId, prefix, &iter);
  if (rc == nebula::cpp2::ErrorCode::SUCCEEDED) {
    for (; iter->valid(); iter->next()) {
      auto edgeKey = ConsistUtil::edgeKeyFromPrime(iter->key());
      VLOG(1) << "scanned edgekey: " << folly::hexlify(edgeKey);
      auto lockKey = makeLockKey(spaceId, edgeKey.str());
      auto insSucceed = reserveTable_.insert(std::make_pair(lockKey, ResumeType::RESUME_CHAIN));
      if (!insSucceed.second) {
        LOG(ERROR) << "not supposed to insert fail: " << folly::hexlify(edgeKey);
      }
      auto* lk = getLockCore(spaceId, partId, false);
      auto succeed = lk->try_lock(edgeKey.str());
      if (!succeed) {
        LOG(ERROR) << "not supposed to lock fail: " << folly::hexlify(edgeKey);
      }
    }
  } else {
    VLOG(1) << "primePrefix() " << apache::thrift::util::enumNameSafe(rc);
    if (rc == nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
      return;
    }
  }
  prefix = ConsistUtil::doublePrimePrefix(partId);
  rc = env_->kvstore_->prefix(spaceId, partId, prefix, &iter);
  if (rc == nebula::cpp2::ErrorCode::SUCCEEDED) {
    for (; iter->valid(); iter->next()) {
      auto edgeKey = ConsistUtil::edgeKeyFromDoublePrime(iter->key());
      auto lockKey = makeLockKey(spaceId, edgeKey.str());
      auto insSucceed = reserveTable_.insert(std::make_pair(lockKey, ResumeType::RESUME_REMOTE));
      if (!insSucceed.second) {
        LOG(ERROR) << "not supposed to insert fail: " << folly::hexlify(edgeKey);
      }
      auto* lk = getLockCore(spaceId, partId, false);
      auto succeed = lk->try_lock(edgeKey.str());
      if (!succeed) {
        LOG(ERROR) << "not supposed to lock fail: " << folly::hexlify(edgeKey);
      }
    }
  } else {
    VLOG(1) << "doublePrimePrefix() " << apache::thrift::util::enumNameSafe(rc);
    if (rc == nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
      return;
    }
  }
  auto partOfSpace = std::make_pair(spaceId, partId);
  auto insRet = whiteListParts_.insert(std::make_pair(partOfSpace, 0));
  LOG(INFO) << "insert space=" << spaceId << ", part=" << partId
            << ", into white list suc=" << insRet.second;
}

folly::ConcurrentHashMap<std::string, ResumeType>* TransactionManager::getReserveTable() {
  return &reserveTable_;
}

}  // namespace storage
}  // namespace nebula
