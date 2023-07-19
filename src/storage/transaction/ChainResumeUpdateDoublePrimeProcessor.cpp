/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ChainResumeUpdateDoublePrimeProcessor.h"

#include <storage/StorageFlags.h>

namespace nebula {
namespace storage {

ChainResumeUpdateDoublePrimeProcessor::ChainResumeUpdateDoublePrimeProcessor(StorageEnv* env,
                                                                             const std::string& val)
    : ChainUpdateEdgeLocalProcessor(env) {
  req_ = ConsistUtil::parseUpdateRequest(val);
  ChainUpdateEdgeLocalProcessor::prepareRequest(req_);
}

folly::SemiFuture<nebula::cpp2::ErrorCode> ChainResumeUpdateDoublePrimeProcessor::prepareLocal() {
  VLOG(1) << " prepareLocal()";
  std::tie(term_, rcPrepare_) = env_->txnMan_->getTermFromKVStore(spaceId_, localPartId_);
  return rcPrepare_;
}

folly::SemiFuture<Code> ChainResumeUpdateDoublePrimeProcessor::processRemote(Code code) {
  VLOG(1) << " prepareLocal(), code = " << apache::thrift::util::enumNameSafe(code);
  return ChainUpdateEdgeLocalProcessor::processRemote(code);
}

folly::SemiFuture<Code> ChainResumeUpdateDoublePrimeProcessor::processLocal(Code code) {
  VLOG(1) << " processRemote(), code = " << apache::thrift::util::enumNameSafe(code);

  if (code != Code::SUCCEEDED) {
    // if there are something wrong other than rpc failure
    // we need to keep the resume retry(by not remove those prime key)
    return code;
  }

  auto currTerm = env_->txnMan_->getTermFromKVStore(spaceId_, localPartId_);
  if (currTerm.first != term_) {
    rcCommit_ = Code::E_LEADER_CHANGED;
    return rcCommit_;
  }

  auto key = ConsistUtil::doublePrime(spaceVidLen_, localPartId_, req_.get_edge_key());
  kvErased_.emplace_back(std::move(key));
  return abort();
}

void ChainResumeUpdateDoublePrimeProcessor::finish() {
  VLOG(1) << " commitLocal()=" << apache::thrift::util::enumNameSafe(rcCommit_);
  if (isKVStoreError(rcCommit_) || rcRemote_ == Code::E_RPC_FAILURE) {
    reportFailed(ResumeType::RESUME_REMOTE);
  }

  pushResultCode(rcCommit_, req_.get_part_id());
  finished_.setValue(rcCommit_);
  onFinished();
}

}  // namespace storage
}  // namespace nebula
