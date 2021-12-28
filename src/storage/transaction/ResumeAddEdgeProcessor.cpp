/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ResumeAddEdgeProcessor.h"

namespace nebula {
namespace storage {

ResumeAddEdgeProcessor::ResumeAddEdgeProcessor(StorageEnv* env, const std::string& val)
    : ChainAddEdgesLocalProcessor(env) {
  req_ = ConsistUtil::parseAddRequest(val);

  uuid_ = ConsistUtil::strUUID();
  readableEdgeDesc_ = makeReadableEdge(req_);
  VLOG(1) << uuid_ << " resume prime " << readableEdgeDesc_;
  ChainAddEdgesLocalProcessor::prepareRequest(req_);
}

folly::SemiFuture<nebula::cpp2::ErrorCode> ResumeAddEdgeProcessor::prepareLocal() {
  if (code_ != Code::SUCCEEDED) {
    return code_;
  }
  auto spaceId = req_.get_space_id();
  auto numOfPart = env_->metaClient_->partsNum(spaceId);
  if (!numOfPart.ok()) {
    return Code::E_SPACE_NOT_FOUND;
  }
  auto& parts = req_.get_parts();
  auto& srcId = parts.begin()->second.back().get_key().get_src().getStr();
  auto& dstId = parts.begin()->second.back().get_key().get_dst().getStr();
  localPartId_ = env_->metaClient_->partId(numOfPart.value(), srcId);
  remotePartId_ = env_->metaClient_->partId(numOfPart.value(), dstId);

  return code_;
}

folly::SemiFuture<Code> ResumeAddEdgeProcessor::processRemote(Code code) {
  VLOG(1) << uuid_ << " prepareLocal() " << apache::thrift::util::enumNameSafe(code);
  return ChainAddEdgesLocalProcessor::processRemote(code);
}

folly::SemiFuture<Code> ResumeAddEdgeProcessor::processLocal(Code code) {
  VLOG(1) << uuid_ << " processRemote() " << apache::thrift::util::enumNameSafe(code);
  setErrorCode(code);

  auto currTerm = env_->txnMan_->getTerm(spaceId_, localPartId_);
  if (currTerm.first != term_) {
    LOG(WARNING) << "E_LEADER_CHANGED during prepare and commit local";
    code_ = Code::E_LEADER_CHANGED;
  }

  if (code == Code::E_RPC_FAILURE) {
    kvAppend_ = ChainAddEdgesLocalProcessor::makeDoublePrime();
  }

  if (code == Code::E_RPC_FAILURE || code == Code::SUCCEEDED) {
    // if there are something wrong other than rpc failure
    // we need to keep the resume retry(by not remove those prime key)
    erasePrime();
    code_ = forwardToDelegateProcessor().get();
    return code_;
  }

  return code;
}

}  // namespace storage
}  // namespace nebula
