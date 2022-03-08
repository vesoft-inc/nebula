/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ChainDeleteEdgesLocalProcessor.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/utils/DefaultValueContext.h"
#include "kvstore/Part.h"
#include "storage/StorageFlags.h"
#include "storage/mutate/DeleteEdgesProcessor.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

void ChainDeleteEdgesLocalProcessor::process(const cpp2::DeleteEdgesRequest& req) {
  auto rc = checkRequest(req);
  if (rc != Code::SUCCEEDED) {
    pushResultCode(rc, localPartId_);
    finish();
    return;
  }
  env_->txnMan_->addChainTask(this);
}

folly::SemiFuture<Code> ChainDeleteEdgesLocalProcessor::prepareLocal() {
  txnId_ = ConsistUtil::strUUID();

  if (!lockEdges(req_)) {
    rcPrepare_ = Code::E_WRITE_WRITE_CONFLICT;
    return rcPrepare_;
  }

  primes_ = makePrime(req_);

  std::vector<kvstore::KV> primes(primes_);

  auto [pro, fut] = folly::makePromiseContract<Code>();
  env_->kvstore_->asyncMultiPut(
      spaceId_, localPartId_, std::move(primes), [p = std::move(pro), this](auto rc) mutable {
        rcPrepare_ = rc;
        p.setValue(rc);
      });
  return std::move(fut);
}

folly::SemiFuture<Code> ChainDeleteEdgesLocalProcessor::processRemote(Code code) {
  VLOG(1) << txnId_ << " prepareLocal(), code = " << apache::thrift::util::enumNameSafe(code);
  if (rcPrepare_ != Code::SUCCEEDED) {
    return rcPrepare_;
  }
  DCHECK_EQ(req_.get_parts().size(), 1);
  auto reversedRequest = reverseRequest(req_);
  DCHECK_EQ(reversedRequest.get_parts().size(), 1);
  auto [pro, fut] = folly::makePromiseContract<Code>();
  doRpc(std::move(pro), std::move(reversedRequest));
  return std::move(fut);
}

folly::SemiFuture<Code> ChainDeleteEdgesLocalProcessor::processLocal(Code code) {
  VLOG(1) << txnId_ << " processRemote(), code = " << apache::thrift::util::enumNameSafe(code);
  if (rcPrepare_ != Code::SUCCEEDED) {
    return rcPrepare_;
  }

  auto [currTerm, suc] = env_->txnMan_->getTermFromKVStore(spaceId_, localPartId_);
  if (currTerm != term_) {
    LOG(WARNING) << "E_LEADER_CHANGED during prepare and commit local";
    rcCommit_ = Code::E_LEADER_CHANGED;
    return rcCommit_;
  }

  if (rcRemote_ == Code::E_RPC_FAILURE) {
    auto keyPrefix = ConsistUtil::doublePrimeTable(localPartId_);
    setDoublePrime_ = true;
    for (auto& kv : primes_) {
      auto key = keyPrefix + kv.first.substr(sizeof(PartitionID));
      doublePrimes_.emplace_back(key, kv.second);
    }
  }

  if (rcRemote_ == Code::SUCCEEDED || rcRemote_ == Code::E_RPC_FAILURE) {
    return commitLocal();
  } else {
    return abort();
  }

  // actually, should return either commit() or abort()
  return rcRemote_;
}

void ChainDeleteEdgesLocalProcessor::reportFailed(ResumeType type) {
  if (lk_ != nullptr) {
    lk_->setAutoUnlock(false);
  }
  for (auto& edgesOfPart : req_.get_parts()) {
    auto partId = edgesOfPart.first;
    for (auto& key : edgesOfPart.second) {
      auto strKey = ConsistUtil::edgeKey(spaceVidLen_, partId, key);
      env_->txnMan_->addPrime(spaceId_, localPartId_, term_, strKey, type);
    }
  }
}

std::vector<kvstore::KV> ChainDeleteEdgesLocalProcessor::makePrime(
    const cpp2::DeleteEdgesRequest& req) {
  std::vector<kvstore::KV> ret;
  std::vector<cpp2::DeleteEdgesRequest> requests;

  for (auto& partOfKeys : req.get_parts()) {
    auto partId = partOfKeys.first;
    for (auto& key : partOfKeys.second) {
      requests.emplace_back();
      requests.back().space_id_ref() = req_.get_space_id();
      std::unordered_map<PartitionID, std::vector<cpp2::EdgeKey>> parts;
      parts[partId].emplace_back(key);
      requests.back().parts_ref() = parts;
      requests.back().common_ref().copy_from(req_.common_ref());
    }
  }

  for (auto& singleReq : requests) {
    std::string val;
    apache::thrift::CompactSerializer::serialize(singleReq, &val);
    val += ConsistUtil::deleteIdentifier();
    auto partId = singleReq.get_parts().begin()->first;
    auto& edgeKey = singleReq.get_parts().begin()->second.back();
    auto key = ConsistUtil::primeTable(partId);
    key += ConsistUtil::edgeKey(spaceVidLen_, partId, edgeKey);
    ret.emplace_back(std::make_pair(key, val));
  }
  return ret;
}

Code ChainDeleteEdgesLocalProcessor::checkRequest(const cpp2::DeleteEdgesRequest& req) {
  CHECK_EQ(req.get_parts().size(), 1);
  req_ = req;
  DCHECK(!req_.get_parts().empty());
  spaceId_ = req_.get_space_id();
  localPartId_ = req.get_parts().begin()->first;

  auto rc = getSpaceVidLen(spaceId_);
  if (rc != Code::SUCCEEDED) {
    return rc;
  }

  auto part = env_->kvstore_->part(spaceId_, localPartId_);
  if (!nebula::ok(part)) {
    pushResultCode(nebula::error(part), localPartId_);
    return Code::E_SPACE_NOT_FOUND;
  }
  auto stPartNum = env_->metaClient_->partsNum(spaceId_);
  if (!stPartNum.ok()) {
    pushResultCode(nebula::error(part), localPartId_);
    return Code::E_PART_NOT_FOUND;
  }

  auto& oneEdgeKey = req.get_parts().begin()->second.front();
  auto& remoteVid = oneEdgeKey.get_dst().getStr();
  remotePartId_ = env_->metaClient_->partId(stPartNum.value(), remoteVid);

  term_ = (nebula::value(part))->termId();

  return Code::SUCCEEDED;
}

folly::SemiFuture<Code> ChainDeleteEdgesLocalProcessor::commitLocal() {
  auto* proc = DeleteEdgesProcessor::instance(env_, nullptr);
  auto fn = std::bind(&ChainDeleteEdgesLocalProcessor::hookFunc, this, std::placeholders::_1);
  proc->setHookFunc(fn);

  auto futProc = proc->getFuture();
  auto [pro, fut] = folly::makePromiseContract<Code>();
  std::move(futProc).thenValue([&, p = std::move(pro)](auto&& resp) mutable {
    auto rc = ConsistUtil::getErrorCode(resp);
    rcCommit_ = rc;
    p.setValue(rc);
  });
  proc->process(req_);
  return std::move(fut);
}

void ChainDeleteEdgesLocalProcessor::doRpc(folly::Promise<Code>&& promise,
                                           cpp2::DeleteEdgesRequest&& req,
                                           int retry) noexcept {
  if (retry > retryLimit_) {
    promise.setValue(Code::E_LEADER_CHANGED);
    return;
  }
  auto* iClient = env_->interClient_;
  folly::Promise<Code> p;
  auto f = p.getFuture();
  iClient->chainDeleteEdges(req, txnId_, term_, std::move(p));

  std::move(f).thenTry([=, p = std::move(promise)](auto&& t) mutable {
    rcRemote_ = t.hasValue() ? t.value() : Code::E_RPC_FAILURE;
    switch (rcRemote_) {
      case Code::E_LEADER_CHANGED:
        doRpc(std::move(p), std::move(req), ++retry);
        break;
      default:
        p.setValue(rcRemote_);
        break;
    }
    return rcRemote_;
  });
}

/**
 * @brief input para may be varies according to if the edge has index
 * if yes, DeleteEdgeProcessor will use batch,
 * else it will use a simple vector of keys
 * |-------------------|--------------------------|------------------------------------|
 * |                   | input keys               | input a batch                        |
 * |-------------------|--------------------------|------------------------------------|
 * | double prime (N)  | del edge, prime keys     | bat.remove(prime)               |
 * | double prime (Y)  | transform to batchHolder | bat.remove(prime) & put(double p.) |
 */
void ChainDeleteEdgesLocalProcessor::hookFunc(HookFuncPara& para) {
  std::string ret;

  if (setDoublePrime_) {
    if (para.keys) {
      kvstore::BatchHolder bat;
      for (auto& edgeKey : *para.keys.value()) {
        bat.remove(std::string(edgeKey));
      }
      for (auto& kv : primes_) {
        bat.remove(std::string(kv.first));
      }
      for (auto& kv : doublePrimes_) {
        bat.put(std::string(kv.first), std::string(kv.second));
      }
      para.result.emplace(kvstore::encodeBatchValue(bat.getBatch()));
    } else if (para.batch) {
      for (auto& kv : primes_) {
        para.batch.value()->remove(std::string(kv.first));
      }
      for (auto& kv : doublePrimes_) {
        para.batch.value()->put(std::string(kv.first), std::string(kv.second));
      }
    } else {
      LOG(ERROR) << "not supposed runs here";
    }
  } else {  // there is no double prime
    if (para.keys) {
      for (auto& kv : primes_) {
        para.keys.value()->emplace_back(kv.first);
      }
    } else if (para.batch) {
      for (auto& kv : primes_) {
        para.batch.value()->remove(std::string(kv.first));
      }
    } else {
      LOG(ERROR) << "not supposed runs here";
    }
  }
}

folly::SemiFuture<Code> ChainDeleteEdgesLocalProcessor::abort() {
  if (setPrime_) {
    return Code::SUCCEEDED;
  }

  std::vector<std::string> keyRemoved;
  for (auto& key : primes_) {
    keyRemoved.emplace_back(key.first);
  }

  auto [pro, fut] = folly::makePromiseContract<Code>();
  env_->kvstore_->asyncMultiRemove(
      req_.get_space_id(),
      localPartId_,
      std::move(keyRemoved),
      [p = std::move(pro), this](auto rc) mutable {
        VLOG(1) << txnId_ << " abort()=" << apache::thrift::util::enumNameSafe(rc);
        rcCommit_ = rc;
        p.setValue(rc);
      });
  return std::move(fut);
}

bool ChainDeleteEdgesLocalProcessor::lockEdges(const cpp2::DeleteEdgesRequest& req) {
  lkCore_ = env_->txnMan_->getLockCore(req.get_space_id(), localPartId_, term_);
  if (!lkCore_) {
    VLOG(1) << txnId_ << "get lock failed.";
    return false;
  }

  std::vector<std::string> keys;
  for (auto& key : req.get_parts().begin()->second) {
    auto eKey = ConsistUtil::edgeKey(spaceVidLen_, localPartId_, key);
    keys.emplace_back(std::move(eKey));
  }
  bool dedup = true;
  lk_ = std::make_unique<TransactionManager::LockGuard>(lkCore_.get(), keys, dedup);
  if (!lk_->isLocked()) {
    VLOG(1) << txnId_ << "term=" << term_ << ", conflict key = "
            << ConsistUtil::readableKey(spaceVidLen_, isIntId_, lk_->conflictKey());
  }
  return lk_->isLocked();
}

cpp2::DeleteEdgesRequest ChainDeleteEdgesLocalProcessor::reverseRequest(
    const cpp2::DeleteEdgesRequest& req) {
  cpp2::DeleteEdgesRequest reversedRequest;
  reversedRequest.space_id_ref() = req.get_space_id();
  reversedRequest.common_ref().copy_from(req.common_ref());
  for (auto& keysOfPart : *req.parts_ref()) {
    for (auto& edgeKey : keysOfPart.second) {
      auto rEdgeKey = ConsistUtil::reverseEdgeKey(edgeKey);
      (*reversedRequest.parts_ref())[remotePartId_].emplace_back(rEdgeKey);
    }
  }
  return reversedRequest;
}

void ChainDeleteEdgesLocalProcessor::finish() {
  VLOG(1) << txnId_ << " commitLocal() = " << apache::thrift::util::enumNameSafe(rcCommit_);
  TermID currTerm = 0;
  std::tie(currTerm, std::ignore) = env_->txnMan_->getTermFromKVStore(spaceId_, localPartId_);
  do {
    if (term_ != currTerm) {
      // transaction manager will do the clean.
      break;
    }

    if (rcPrepare_ != Code::SUCCEEDED) {
      break;  // nothing written, no need to recover.
    }

    if (rcCommit_ != Code::SUCCEEDED) {
      reportFailed(ResumeType::RESUME_CHAIN);
      break;
    }

    if (rcRemote_ == Code::E_RPC_FAILURE) {
      reportFailed(ResumeType::RESUME_REMOTE);
      break;
    }
  } while (0);

  auto rc = Code::SUCCEEDED;
  do {
    if (rcPrepare_ != Code::SUCCEEDED) {
      rc = rcPrepare_;
      break;
    }

    if (rcCommit_ != Code::SUCCEEDED) {
      rc = rcCommit_;
      break;
    }

    // rcCommit_ may be set SUCCEEDED in abort().
    // which we should return the error code or remote.
    if (rcRemote_ != Code::E_RPC_FAILURE) {
      rc = rcRemote_;
      break;
    }
  } while (0);
  pushResultCode(rc, localPartId_);
  finished_.setValue(rc);
  onFinished();
}

}  // namespace storage
}  // namespace nebula
