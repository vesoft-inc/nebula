/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/transaction/ChainUpdateEdgeProcessorLocal.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "storage/StorageFlags.h"
#include "storage/mutate/UpdateEdgeProcessor.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

void ChainUpdateEdgeProcessorLocal::process(const cpp2::UpdateEdgeRequest& req) {
  if (!prepareRequest(req)) {
    onFinished();
  }

  env_->txnMan_->addChainTask(this);
}

bool ChainUpdateEdgeProcessorLocal::prepareRequest(const cpp2::UpdateEdgeRequest& req) {
  req_ = req;
  spaceId_ = req.get_space_id();
  partId_ = req_.get_part_id();

  auto rc = getSpaceVidLen(spaceId_);
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    pushResultCode(rc, partId_);
    return false;
  }

  auto __term = env_->txnMan_->getTerm(req_.get_space_id(), partId_);
  if (__term.ok()) {
    termOfPrepare_ = __term.value();
  } else {
    pushResultCode(Code::E_PART_NOT_FOUND, partId_);
    return false;
  }
  return true;
}

/**
 * 1. set mem lock
 * 2. set edge prime
 * */
folly::SemiFuture<Code> ChainUpdateEdgeProcessorLocal::prepareLocal() {
  if (!setLock()) {
    LOG(INFO) << "set lock failed, return E_WRITE_WRITE_CONFLICT";
    return Code::E_WRITE_WRITE_CONFLICT;
  }

  auto key = ConsistUtil::primeKey(spaceVidLen_, partId_, req_.get_edge_key());

  std::string val;
  apache::thrift::CompactSerializer::serialize(req_, &val);
  val.append(ConsistUtil::updateIdentifier());

  std::vector<kvstore::KV> data{{key, val}};
  auto c = folly::makePromiseContract<Code>();
  env_->kvstore_->asyncMultiPut(
      spaceId_, partId_, std::move(data), [p = std::move(c.first), this](auto rc) mutable {
        if (rc == nebula::cpp2::ErrorCode::SUCCEEDED) {
          primeInserted_ = true;
        } else {
          VLOG(1) << "kvstore err: " << apache::thrift::util::enumNameSafe(rc);
        }
        p.setValue(rc);
      });
  return std::move(c.second);
}

folly::SemiFuture<Code> ChainUpdateEdgeProcessorLocal::processRemote(Code code) {
  LOG(INFO) << "prepareLocal()=" << apache::thrift::util::enumNameSafe(code);
  if (code != Code::SUCCEEDED) {
    return code;
  }
  auto [pro, fut] = folly::makePromiseContract<Code>();
  doRpc(std::move(pro));
  return std::move(fut);
}

folly::SemiFuture<Code> ChainUpdateEdgeProcessorLocal::processLocal(Code code) {
  LOG(INFO) << "processRemote(), code = " << apache::thrift::util::enumNameSafe(code);
  if (code != Code::SUCCEEDED && code_ == Code::SUCCEEDED) {
    code_ = code;
  }

  if (!checkTerm()) {
    LOG(WARNING) << "checkTerm() failed";
    return Code::E_OUTDATED_TERM;
  }

  if (code == Code::E_RPC_FAILURE) {
    appendDoublePrime();
    addUnfinishedEdge(ResumeType::RESUME_REMOTE);
  }

  if (code == Code::SUCCEEDED || code == Code::E_RPC_FAILURE) {
    erasePrime();
    forwardToDelegateProcessor();
  } else {
    if (primeInserted_) {
      abort();
    }
  }

  return code_;
}

void ChainUpdateEdgeProcessorLocal::doRpc(folly::Promise<Code>&& promise, int retry) noexcept {
  try {
    if (retry > retryLimit_) {
      promise.setValue(Code::E_LEADER_CHANGED);
      return;
    }
    auto* iClient = env_->txnMan_->getInternalClient();
    folly::Promise<Code> p;
    auto reversedReq = reverseRequest(req_);

    auto f = p.getFuture();
    iClient->chainUpdateEdge(reversedReq, termOfPrepare_, ver_, std::move(p));
    std::move(f)
        .thenTry([=, p = std::move(promise)](auto&& t) mutable {
          auto code = t.hasValue() ? t.value() : Code::E_RPC_FAILURE;
          VLOG(1) << "code = " << apache::thrift::util::enumNameSafe(code);
          switch (code) {
            case Code::E_LEADER_CHANGED:
              doRpc(std::move(p), ++retry);
              break;
            default:
              p.setValue(code);
              break;
          }
          return code;
        })
        .get();
  } catch (std::exception& ex) {
    LOG(WARNING) << "doRpc() ex: " << ex.what();
  }
}

void ChainUpdateEdgeProcessorLocal::erasePrime() {
  auto key = ConsistUtil::primeKey(spaceVidLen_, partId_, req_.get_edge_key());
  kvErased_.emplace_back(std::move(key));
}

void ChainUpdateEdgeProcessorLocal::appendDoublePrime() {
  auto key = ConsistUtil::doublePrime(spaceVidLen_, partId_, req_.get_edge_key());
  std::string val;
  apache::thrift::CompactSerializer::serialize(req_, &val);
  val += ConsistUtil::updateIdentifier();
  kvAppend_.emplace_back(std::make_pair(std::move(key), std::move(val)));
}

void ChainUpdateEdgeProcessorLocal::forwardToDelegateProcessor() {
  kUpdateEdgeCounters.init("update_edge");
  UpdateEdgeProcessor::ContextAdjuster fn = [=](EdgeContext& ctx) {
    ctx.kvAppend = std::move(kvAppend_);
    ctx.kvErased = std::move(kvErased_);
  };

  auto* proc = UpdateEdgeProcessor::instance(env_);
  proc->adjustContext(std::move(fn));
  auto f = proc->getFuture();
  proc->process(req_);
  auto resp = std::move(f).get();
  code_ = getErrorCode(resp);
  if (code_ != Code::SUCCEEDED) {
    addUnfinishedEdge(ResumeType::RESUME_CHAIN);
  }
  std::swap(resp_, resp);
}

Code ChainUpdateEdgeProcessorLocal::checkAndBuildContexts(const cpp2::UpdateEdgeRequest&) {
  return Code::SUCCEEDED;
}

std::string ChainUpdateEdgeProcessorLocal::sEdgeKey(const cpp2::UpdateEdgeRequest& req) {
  return ConsistUtil::edgeKey(spaceVidLen_, req.get_part_id(), req.get_edge_key());
}

void ChainUpdateEdgeProcessorLocal::finish() {
  LOG(INFO) << "ChainUpdateEdgeProcessorLocal::finish()";
  pushResultCode(code_, req_.get_part_id());
  onFinished();
}

bool ChainUpdateEdgeProcessorLocal::checkTerm() {
  return env_->txnMan_->checkTerm(req_.get_space_id(), req_.get_part_id(), termOfPrepare_);
}

bool ChainUpdateEdgeProcessorLocal::checkVersion() {
  if (!ver_) {
    return true;
  }
  auto [ver, rc] = ConsistUtil::versionOfUpdateReq(env_, req_);
  if (rc != Code::SUCCEEDED) {
    return false;
  }
  return *ver_ == ver;
}

void ChainUpdateEdgeProcessorLocal::abort() {
  auto key = ConsistUtil::primeKey(spaceVidLen_, partId_, req_.get_edge_key());
  kvErased_.emplace_back(std::move(key));

  folly::Baton<true, std::atomic> baton;
  env_->kvstore_->asyncMultiRemove(
      req_.get_space_id(), req_.get_part_id(), std::move(kvErased_), [&](auto rc) mutable {
        LOG(INFO) << " abort()=" << apache::thrift::util::enumNameSafe(rc);
        if (rc != Code::SUCCEEDED) {
          addUnfinishedEdge(ResumeType::RESUME_CHAIN);
        }
        baton.post();
      });
  baton.wait();
}

cpp2::UpdateEdgeRequest ChainUpdateEdgeProcessorLocal::reverseRequest(
    const cpp2::UpdateEdgeRequest& req) {
  cpp2::UpdateEdgeRequest reversedRequest(req);
  auto reversedEdgeKey = ConsistUtil::reverseEdgeKey(req.get_edge_key());
  reversedRequest.set_edge_key(reversedEdgeKey);

  auto partsNum = env_->metaClient_->partsNum(req.get_space_id());
  CHECK(partsNum.ok());
  auto srcVid = reversedRequest.get_edge_key().get_src().getStr();
  auto partId = env_->metaClient_->partId(partsNum.value(), srcVid);
  reversedRequest.set_part_id(partId);

  return reversedRequest;
}

bool ChainUpdateEdgeProcessorLocal::setLock() {
  auto spaceId = req_.get_space_id();
  auto* lockCore = env_->txnMan_->getLockCore(spaceId, req_.get_part_id());
  if (lockCore == nullptr) {
    return false;
  }
  auto key = ConsistUtil::edgeKey(spaceVidLen_, req_.get_part_id(), req_.get_edge_key());
  lk_ = std::make_unique<MemoryLockGuard<std::string>>(lockCore, key);
  return lk_->isLocked();
}

int64_t ChainUpdateEdgeProcessorLocal::getVersion(const cpp2::UpdateEdgeRequest& req) {
  int64_t invalidVer = -1;
  auto spaceId = req.get_space_id();
  auto vIdLen = env_->metaClient_->getSpaceVidLen(spaceId);
  if (!vIdLen.ok()) {
    LOG(WARNING) << vIdLen.status().toString();
    return invalidVer;
  }
  auto partId = req.get_part_id();
  auto key = ConsistUtil::edgeKey(vIdLen.value(), partId, req.get_edge_key());
  return ConsistUtil::getSingleEdgeVer(env_->kvstore_, spaceId, partId, key);
}

nebula::cpp2::ErrorCode ChainUpdateEdgeProcessorLocal::getErrorCode(
    const cpp2::UpdateResponse& resp) {
  auto& respCommon = resp.get_result();
  auto& parts = respCommon.get_failed_parts();
  if (parts.empty()) {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  return parts.front().get_code();
}

void ChainUpdateEdgeProcessorLocal::addUnfinishedEdge(ResumeType type) {
  LOG(INFO) << "addUnfinishedEdge()";
  if (lk_ != nullptr) {
    lk_->forceUnlock();
  }
  auto key = ConsistUtil::edgeKey(spaceVidLen_, req_.get_part_id(), req_.get_edge_key());
  env_->txnMan_->addPrime(spaceId_, key, type);
}

}  // namespace storage
}  // namespace nebula
