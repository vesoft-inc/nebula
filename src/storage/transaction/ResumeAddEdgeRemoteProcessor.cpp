/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/transaction/ResumeAddEdgeRemoteProcessor.h"

namespace nebula {
namespace storage {

ResumeAddEdgeRemoteProcessor::ResumeAddEdgeRemoteProcessor(StorageEnv* env, const std::string& val)
    : ChainAddEdgesProcessorLocal(env) {
  req_ = ConsistUtil::parseAddRequest(val);
  LOG(WARNING) << ConsistUtil::dumpAddEdgeReq(req_);
  ChainAddEdgesProcessorLocal::prepareRequest(req_);
}

folly::SemiFuture<nebula::cpp2::ErrorCode> ResumeAddEdgeRemoteProcessor::prepareLocal() {
  if (!checkTerm(req_)) {
    LOG(WARNING) << this << "E_OUTDATED_TERM";
    return Code::E_OUTDATED_TERM;
  }

  if (!checkVersion(req_)) {
    LOG(WARNING) << this << "E_OUTDATED_EDGE";
    return Code::E_OUTDATED_EDGE;
  }

  auto spaceId = req_.get_space_id();
  auto numOfPart = env_->metaClient_->partsNum(spaceId);
  if (!numOfPart.ok()) {
    return Code::E_SPACE_NOT_FOUND;
  }
  auto& parts = req_.get_parts();
  auto& dstId = parts.begin()->second.back().get_key().get_dst().getStr();
  remotePartId_ = env_->metaClient_->partId(numOfPart.value(), dstId);
  std::vector<std::string> keys = sEdgeKey(req_);
  auto vers = ConsistUtil::getMultiEdgeVers(env_->kvstore_, spaceId, localPartId_, keys);
  edgeVer_ = vers.front();

  return Code::SUCCEEDED;
}

folly::SemiFuture<Code> ResumeAddEdgeRemoteProcessor::processRemote(Code code) {
  return ChainAddEdgesProcessorLocal::processRemote(code);
}

folly::SemiFuture<Code> ResumeAddEdgeRemoteProcessor::processLocal(Code code) {
  if (!checkTerm(req_)) {
    LOG(WARNING) << this << "E_OUTDATED_TERM";
    return Code::E_OUTDATED_TERM;
  }

  if (!checkVersion(req_)) {
    LOG(WARNING) << this << "E_OUTDATED_EDGE";
    return Code::E_OUTDATED_EDGE;
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
    ChainAddEdgesProcessorLocal::eraseDoublePrime();
    code_ = forwardToDelegateProcessor().get();
    return code_;
  }

  return code;
}

}  // namespace storage
}  // namespace nebula
