/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ResumeUpdateRemoteProcessor.h"

#include <storage/StorageFlags.h>

namespace nebula {
namespace storage {

ResumeUpdateRemoteProcessor::ResumeUpdateRemoteProcessor(StorageEnv* env, const std::string& val)
    : ChainUpdateEdgeLocalProcessor(env) {
  req_ = ConsistUtil::parseUpdateRequest(val);
  ChainUpdateEdgeLocalProcessor::prepareRequest(req_);
}

folly::SemiFuture<nebula::cpp2::ErrorCode> ResumeUpdateRemoteProcessor::prepareLocal() {
  std::tie(term_, code_) = env_->txnMan_->getTerm(spaceId_, localPartId_);
  return code_;
}

folly::SemiFuture<Code> ResumeUpdateRemoteProcessor::processRemote(Code code) {
  return ChainUpdateEdgeLocalProcessor::processRemote(code);
}

folly::SemiFuture<Code> ResumeUpdateRemoteProcessor::processLocal(Code code) {
  setErrorCode(code);

  auto currTerm = env_->txnMan_->getTerm(spaceId_, localPartId_);
  if (currTerm.first != term_) {
    LOG(WARNING) << "E_LEADER_CHANGED during prepare and commit local";
    code_ = Code::E_LEADER_CHANGED;
  }

  if (code == Code::SUCCEEDED) {
    // if there are something wrong other than rpc failure
    // we need to keep the resume retry(by not remove those prime key)
    auto key = ConsistUtil::doublePrime(spaceVidLen_, localPartId_, req_.get_edge_key());
    kvErased_.emplace_back(std::move(key));
    forwardToDelegateProcessor();
    return code;
  } else {
    // we can't decide if the double prime should be deleted.
    // so do nothing
  }

  return code;
}

void ResumeUpdateRemoteProcessor::finish() {
  if (FLAGS_trace_toss) {
    VLOG(1) << "commitLocal()=" << apache::thrift::util::enumNameSafe(code_);
  }
  finished_.setValue(code_);
  onFinished();
}

}  // namespace storage
}  // namespace nebula
