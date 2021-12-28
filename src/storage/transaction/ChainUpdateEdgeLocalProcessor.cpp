/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ChainUpdateEdgeLocalProcessor.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "storage/StorageFlags.h"
#include "storage/mutate/UpdateEdgeProcessor.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

void ChainUpdateEdgeLocalProcessor::process(const cpp2::UpdateEdgeRequest& req) {
  if (!prepareRequest(req)) {
    onFinished();
  }

  env_->txnMan_->addChainTask(this);
}

bool ChainUpdateEdgeLocalProcessor::prepareRequest(const cpp2::UpdateEdgeRequest& req) {
  req_ = req;
  spaceId_ = req.get_space_id();
  localPartId_ = req_.get_part_id();

  auto rc = getSpaceVidLen(spaceId_);
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    pushResultCode(rc, localPartId_);
    return false;
  }

  std::tie(term_, code_) = env_->txnMan_->getTerm(spaceId_, localPartId_);
  if (code_ != Code::SUCCEEDED) {
    return false;
  }
  return true;
}

/**
 * 1. set mem lock
 * 2. set edge prime
 * */
folly::SemiFuture<Code> ChainUpdateEdgeLocalProcessor::prepareLocal() {
  if (!setLock()) {
    LOG(INFO) << "set lock failed, return E_WRITE_WRITE_CONFLICT";
    return Code::E_WRITE_WRITE_CONFLICT;
  }

  auto key = ConsistUtil::primeKey(spaceVidLen_, localPartId_, req_.get_edge_key());

  std::string val;
  apache::thrift::CompactSerializer::serialize(req_, &val);
  val.append(ConsistUtil::updateIdentifier());

  std::vector<kvstore::KV> data{{key, val}};
  auto c = folly::makePromiseContract<Code>();
  env_->kvstore_->asyncMultiPut(
      spaceId_, localPartId_, std::move(data), [p = std::move(c.first), this](auto rc) mutable {
        if (rc == nebula::cpp2::ErrorCode::SUCCEEDED) {
          primeInserted_ = true;
        } else {
          VLOG(1) << "kvstore err: " << apache::thrift::util::enumNameSafe(rc);
        }
        p.setValue(rc);
      });
  return std::move(c.second);
}

folly::SemiFuture<Code> ChainUpdateEdgeLocalProcessor::processRemote(Code code) {
  LOG(INFO) << "prepareLocal()=" << apache::thrift::util::enumNameSafe(code);
  if (code != Code::SUCCEEDED) {
    return code;
  }
  auto [pro, fut] = folly::makePromiseContract<Code>();
  doRpc(std::move(pro));
  return std::move(fut);
}

folly::SemiFuture<Code> ChainUpdateEdgeLocalProcessor::processLocal(Code code) {
  LOG(INFO) << "processRemote(), code = " << apache::thrift::util::enumNameSafe(code);
  if (code != Code::SUCCEEDED && code_ == Code::SUCCEEDED) {
    code_ = code;
  }

  auto currTerm = env_->txnMan_->getTerm(spaceId_, localPartId_);
  if (currTerm.first != term_) {
    LOG(WARNING) << "E_LEADER_CHANGED during prepare and commit local";
    code_ = Code::E_LEADER_CHANGED;
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

void ChainUpdateEdgeLocalProcessor::doRpc(folly::Promise<Code>&& promise, int retry) noexcept {
  try {
    if (retry > retryLimit_) {
      promise.setValue(Code::E_LEADER_CHANGED);
      return;
    }
    auto* iClient = env_->txnMan_->getInternalClient();
    folly::Promise<Code> p;
    auto reversedReq = reverseRequest(req_);

    auto f = p.getFuture();
    iClient->chainUpdateEdge(reversedReq, term_, ver_, std::move(p));
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

void ChainUpdateEdgeLocalProcessor::erasePrime() {
  auto key = ConsistUtil::primeKey(spaceVidLen_, localPartId_, req_.get_edge_key());
  kvErased_.emplace_back(std::move(key));
}

void ChainUpdateEdgeLocalProcessor::appendDoublePrime() {
  auto key = ConsistUtil::doublePrime(spaceVidLen_, localPartId_, req_.get_edge_key());
  std::string val;
  apache::thrift::CompactSerializer::serialize(req_, &val);
  val += ConsistUtil::updateIdentifier();
  kvAppend_.emplace_back(std::make_pair(std::move(key), std::move(val)));
}

void ChainUpdateEdgeLocalProcessor::forwardToDelegateProcessor() {
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

Code ChainUpdateEdgeLocalProcessor::checkAndBuildContexts(const cpp2::UpdateEdgeRequest&) {
  return Code::SUCCEEDED;
}

std::string ChainUpdateEdgeLocalProcessor::sEdgeKey(const cpp2::UpdateEdgeRequest& req) {
  return ConsistUtil::edgeKey(spaceVidLen_, req.get_part_id(), req.get_edge_key());
}

void ChainUpdateEdgeLocalProcessor::finish() {
  LOG(INFO) << "ChainUpdateEdgeLocalProcessor::finish()";
  pushResultCode(code_, req_.get_part_id());
  onFinished();
}

void ChainUpdateEdgeLocalProcessor::abort() {
  auto key = ConsistUtil::primeKey(spaceVidLen_, localPartId_, req_.get_edge_key());
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

cpp2::UpdateEdgeRequest ChainUpdateEdgeLocalProcessor::reverseRequest(
    const cpp2::UpdateEdgeRequest& req) {
  cpp2::UpdateEdgeRequest reversedRequest(req);
  auto reversedEdgeKey = ConsistUtil::reverseEdgeKey(req.get_edge_key());
  reversedRequest.edge_key_ref() = reversedEdgeKey;

  auto partsNum = env_->metaClient_->partsNum(req.get_space_id());
  CHECK(partsNum.ok());
  auto srcVid = reversedRequest.get_edge_key().get_src().getStr();
  auto partId = env_->metaClient_->partId(partsNum.value(), srcVid);
  reversedRequest.part_id_ref() = partId;

  return reversedRequest;
}

bool ChainUpdateEdgeLocalProcessor::setLock() {
  auto spaceId = req_.get_space_id();
  auto* lockCore = env_->txnMan_->getLockCore(spaceId, req_.get_part_id());
  if (lockCore == nullptr) {
    return false;
  }
  auto key = ConsistUtil::edgeKey(spaceVidLen_, req_.get_part_id(), req_.get_edge_key());
  lk_ = std::make_unique<MemoryLockGuard<std::string>>(lockCore, key);
  return lk_->isLocked();
}

nebula::cpp2::ErrorCode ChainUpdateEdgeLocalProcessor::getErrorCode(
    const cpp2::UpdateResponse& resp) {
  auto& respCommon = resp.get_result();
  auto& parts = respCommon.get_failed_parts();
  if (parts.empty()) {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  return parts.front().get_code();
}

void ChainUpdateEdgeLocalProcessor::addUnfinishedEdge(ResumeType type) {
  LOG(INFO) << "addUnfinishedEdge()";
  if (lk_ != nullptr) {
    lk_->forceUnlock();
  }
  auto key = ConsistUtil::edgeKey(spaceVidLen_, req_.get_part_id(), req_.get_edge_key());
  env_->txnMan_->addPrime(spaceId_, key, type);
}

}  // namespace storage
}  // namespace nebula
