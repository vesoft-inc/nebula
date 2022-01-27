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
    pushResultCode(rcPrepare_, localPartId_);
    onFinished();
  }

  env_->txnMan_->addChainTask(this);
}

bool ChainUpdateEdgeLocalProcessor::prepareRequest(const cpp2::UpdateEdgeRequest& req) {
  req_ = req;
  spaceId_ = req.get_space_id();
  localPartId_ = req_.get_part_id();

  rcPrepare_ = getSpaceVidLen(spaceId_);
  if (rcPrepare_ != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return false;
  }

  std::tie(term_, rcPrepare_) = env_->txnMan_->getTermFromKVStore(spaceId_, localPartId_);
  if (rcPrepare_ != Code::SUCCEEDED) {
    return false;
  }
  return true;
}

folly::SemiFuture<Code> ChainUpdateEdgeLocalProcessor::prepareLocal() {
  if (!setLock()) {
    rcPrepare_ = Code::E_WRITE_WRITE_CONFLICT;
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
        rcPrepare_ = rc;
        p.setValue(rc);
      });
  return std::move(c.second);
}

folly::SemiFuture<Code> ChainUpdateEdgeLocalProcessor::processRemote(Code code) {
  VLOG(1) << " prepareLocal(): " << apache::thrift::util::enumNameSafe(code);
  if (code != Code::SUCCEEDED) {
    return code;
  }
  auto [pro, fut] = folly::makePromiseContract<Code>();
  doRpc(std::move(pro));
  return std::move(fut);
}

folly::SemiFuture<Code> ChainUpdateEdgeLocalProcessor::processLocal(Code code) {
  VLOG(1) << " processRemote(): " << apache::thrift::util::enumNameSafe(code);
  if (rcPrepare_ != Code::SUCCEEDED) {
    return rcPrepare_;
  }

  auto currTerm = env_->txnMan_->getTermFromKVStore(spaceId_, localPartId_);
  if (currTerm.first != term_) {
    rcCommit_ = Code::E_LEADER_CHANGED;
    return rcCommit_;
  }

  if (rcRemote_ == Code::E_RPC_FAILURE) {
    appendDoublePrime();
  }

  erasePrime();

  if (rcRemote_ != Code::SUCCEEDED && rcRemote_ != Code::E_RPC_FAILURE) {
    // prepare succeed and remote failed
    return abort();
  }

  return commit();
}

void ChainUpdateEdgeLocalProcessor::finish() {
  VLOG(1) << " commitLocal()=" << apache::thrift::util::enumNameSafe(rcCommit_);
  do {
    if (rcPrepare_ != Code::SUCCEEDED) {
      break;
    }
    if (isKVStoreError(rcCommit_)) {
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

    if (rcRemote_ != Code::E_RPC_FAILURE) {
      rc = rcRemote_;
      break;
    }
  } while (0);

  pushResultCode(rc, req_.get_part_id());
  finished_.setValue(rc);
  onFinished();
}

void ChainUpdateEdgeLocalProcessor::doRpc(folly::Promise<Code>&& promise, int retry) noexcept {
  try {
    if (retry > retryLimit_) {
      rcRemote_ = Code::E_LEADER_CHANGED;
      promise.setValue(rcRemote_);
      return;
    }
    auto* iClient = env_->interClient_;
    folly::Promise<Code> p;
    auto reversedReq = reverseRequest(req_);

    auto f = p.getFuture();
    iClient->chainUpdateEdge(reversedReq, term_, ver_, std::move(p));
    std::move(f)
        .thenTry([=, p = std::move(promise)](auto&& t) mutable {
          rcRemote_ = t.hasValue() ? t.value() : Code::E_RPC_FAILURE;
          switch (rcRemote_) {
            case Code::E_LEADER_CHANGED:
              doRpc(std::move(p), ++retry);
              break;
            default:
              p.setValue(rcRemote_);
              break;
          }
          return rcRemote_;
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

folly::SemiFuture<Code> ChainUpdateEdgeLocalProcessor::commit() {
  VLOG(1) << __func__ << "()";
  UpdateEdgeProcessor::ContextAdjuster fn = [=](EdgeContext& ctx) {
    ctx.kvAppend = std::move(kvAppend_);
    ctx.kvErased = std::move(kvErased_);
  };

  auto [pro, fut] = folly::makePromiseContract<Code>();
  auto* proc = UpdateEdgeProcessor::instance(env_);
  proc->adjustContext(std::move(fn));
  auto f = proc->getFuture();
  std::move(f).thenTry([&, p = std::move(pro)](auto&& t) mutable {
    if (t.hasValue()) {
      resp_ = std::move(t.value());
      rcCommit_ = getErrorCode(resp_);
    }
    p.setValue(rcCommit_);
  });

  proc->process(req_);
  return std::move(fut);
}

Code ChainUpdateEdgeLocalProcessor::checkAndBuildContexts(const cpp2::UpdateEdgeRequest&) {
  return Code::SUCCEEDED;
}

std::string ChainUpdateEdgeLocalProcessor::sEdgeKey(const cpp2::UpdateEdgeRequest& req) {
  return ConsistUtil::edgeKey(spaceVidLen_, req.get_part_id(), req.get_edge_key());
}

folly::SemiFuture<Code> ChainUpdateEdgeLocalProcessor::abort() {
  VLOG(1) << __func__ << "()";
  if (kvErased_.empty()) {
    return Code::SUCCEEDED;
  }

  auto [pro, fut] = folly::makePromiseContract<Code>();
  env_->kvstore_->asyncMultiRemove(req_.get_space_id(),
                                   req_.get_part_id(),
                                   std::move(kvErased_),
                                   [&, p = std::move(pro)](auto rc) mutable {
                                     rcCommit_ = rc;
                                     p.setValue(rc);
                                   });
  return std::move(fut);
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
  lkCore_ = env_->txnMan_->getLockCore(spaceId, req_.get_part_id(), term_);
  if (lkCore_ == nullptr) {
    return false;
  }
  auto key = ConsistUtil::edgeKey(spaceVidLen_, req_.get_part_id(), req_.get_edge_key());
  lk_ = std::make_unique<MemoryLockGuard<std::string>>(lkCore_.get(), key);
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

void ChainUpdateEdgeLocalProcessor::reportFailed(ResumeType type) {
  VLOG(1) << __func__ << "()";
  if (lk_ != nullptr) {
    lk_->setAutoUnlock(false);
  }
  auto key = ConsistUtil::edgeKey(spaceVidLen_, req_.get_part_id(), req_.get_edge_key());
  env_->txnMan_->addPrime(spaceId_, localPartId_, term_, key, type);
}

bool ChainUpdateEdgeLocalProcessor::isKVStoreError(nebula::cpp2::ErrorCode code) {
  auto iCode = static_cast<int>(code);
  auto kvStoreErrorCodeBegin = static_cast<int>(nebula::cpp2::ErrorCode::E_RAFT_UNKNOWN_PART);
  auto kvStoreErrorCodeEnd = static_cast<int>(nebula::cpp2::ErrorCode::E_RAFT_ATOMIC_OP_FAILED);
  return iCode >= kvStoreErrorCodeBegin && iCode <= kvStoreErrorCodeEnd;
}

}  // namespace storage
}  // namespace nebula
