/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ResumeUpdateProcessor.h"

#include <storage/StorageFlags.h>

#include <boost/stacktrace.hpp>

namespace nebula {
namespace storage {

ResumeUpdateProcessor::ResumeUpdateProcessor(StorageEnv* env, const std::string& val)
    : ChainUpdateEdgeLocalProcessor(env) {
  req_ = ConsistUtil::parseUpdateRequest(val);
  ChainUpdateEdgeLocalProcessor::prepareRequest(req_);
}

folly::SemiFuture<nebula::cpp2::ErrorCode> ResumeUpdateProcessor::prepareLocal() {
  std::tie(term_, code_) = env_->txnMan_->getTerm(spaceId_, localPartId_);
  return code_;
}

folly::SemiFuture<Code> ResumeUpdateProcessor::processRemote(Code code) {
  VLOG(1) << "prepareLocal()=" << apache::thrift::util::enumNameSafe(code);
  return ChainUpdateEdgeLocalProcessor::processRemote(code);
}

folly::SemiFuture<Code> ResumeUpdateProcessor::processLocal(Code code) {
  VLOG(1) << "processRemote()=" << apache::thrift::util::enumNameSafe(code);
  setErrorCode(code);

  auto currTerm = env_->txnMan_->getTerm(spaceId_, localPartId_);
  if (currTerm.first != term_) {
    LOG(WARNING) << "E_LEADER_CHANGED during prepare and commit local";
    code_ = Code::E_LEADER_CHANGED;
  }

  if (code == Code::E_RPC_FAILURE) {
    appendDoublePrime();
  }

  if (code == Code::E_RPC_FAILURE || code == Code::SUCCEEDED) {
    // if there are something wrong other than rpc failure
    // we need to keep the resume retry(by not remove those prime key)
    auto key = ConsistUtil::primeKey(spaceVidLen_, localPartId_, req_.get_edge_key());
    kvErased_.emplace_back(std::move(key));
    forwardToDelegateProcessor();
    return code_;
  }

  return code;
}

void ResumeUpdateProcessor::finish() {
  VLOG(1) << "commitLocal()=" << apache::thrift::util::enumNameSafe(code_);
  finished_.setValue(code_);
  onFinished();
}

}  // namespace storage
}  // namespace nebula
