/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ResumeAddEdgeRemoteProcessor.h"

namespace nebula {
namespace storage {

ResumeAddEdgeRemoteProcessor::ResumeAddEdgeRemoteProcessor(StorageEnv* env, const std::string& val)
    : ChainAddEdgesLocalProcessor(env) {
  req_ = ConsistUtil::parseAddRequest(val);
  ChainAddEdgesLocalProcessor::prepareRequest(req_);
}

folly::SemiFuture<nebula::cpp2::ErrorCode> ResumeAddEdgeRemoteProcessor::prepareLocal() {
  std::tie(term_, code_) = env_->txnMan_->getTerm(spaceId_, localPartId_);
  if (code_ != Code::SUCCEEDED) {
    return code_;
  }

  auto spaceId = req_.get_space_id();
  auto numOfPart = env_->metaClient_->partsNum(spaceId);
  if (!numOfPart.ok()) {
    return Code::E_SPACE_NOT_FOUND;
  }
  auto& parts = req_.get_parts();
  auto& dstId = parts.begin()->second.back().get_key().get_dst().getStr();
  remotePartId_ = env_->metaClient_->partId(numOfPart.value(), dstId);

  return Code::SUCCEEDED;
}

folly::SemiFuture<Code> ResumeAddEdgeRemoteProcessor::processRemote(Code code) {
  return ChainAddEdgesLocalProcessor::processRemote(code);
}

folly::SemiFuture<Code> ResumeAddEdgeRemoteProcessor::processLocal(Code code) {
  auto currTerm = env_->txnMan_->getTerm(spaceId_, localPartId_);
  if (currTerm.first != term_) {
    LOG(WARNING) << "E_LEADER_CHANGED during prepare and commit local";
    code_ = Code::E_LEADER_CHANGED;
  }

  if (code == Code::E_OUTDATED_TERM) {
    // E_OUTDATED_TERM indicate this host is no longer the leader of curr part
    // any following kv operation will fail
    // due to not allowed to write from follower
    return code;
  }

  if (code == Code::E_RPC_FAILURE) {
    // nothing to do, as we are already an rpc failure
  }

  if (code == Code::SUCCEEDED) {
    // if there are something wrong other than rpc failure
    // we need to keep the resume retry(by not remove those prime key)
    ChainAddEdgesLocalProcessor::eraseDoublePrime();
    code_ = forwardToDelegateProcessor().get();
    return code_;
  }

  return code;
}

}  // namespace storage
}  // namespace nebula
