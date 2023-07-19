/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ChainResumeAddDoublePrimeProcessor.h"

namespace nebula {
namespace storage {

ChainResumeAddDoublePrimeProcessor::ChainResumeAddDoublePrimeProcessor(StorageEnv* env,
                                                                       const std::string& val)
    : ChainAddEdgesLocalProcessor(env) {
  req_ = ConsistUtil::parseAddRequest(val);

  uuid_ = ConsistUtil::strUUID() + " ResumeDoublePrime, ";
}

folly::SemiFuture<nebula::cpp2::ErrorCode> ChainResumeAddDoublePrimeProcessor::prepareLocal() {
  ChainAddEdgesLocalProcessor::prepareRequest(req_);
  if (rcPrepare_ != Code::SUCCEEDED) {
    return rcPrepare_;
  }

  auto spaceId = req_.get_space_id();
  auto numOfPart = env_->metaClient_->partsNum(spaceId);
  if (!numOfPart.ok()) {
    rcPrepare_ = Code::E_SPACE_NOT_FOUND;
    return Code::E_SPACE_NOT_FOUND;
  }
  auto& parts = req_.get_parts();
  auto& dstId = parts.begin()->second.back().get_key().get_dst().getStr();
  remotePartId_ = env_->metaClient_->partId(numOfPart.value(), dstId);

  return Code::SUCCEEDED;
}

folly::SemiFuture<Code> ChainResumeAddDoublePrimeProcessor::processRemote(Code code) {
  return ChainAddEdgesLocalProcessor::processRemote(code);
}

folly::SemiFuture<Code> ChainResumeAddDoublePrimeProcessor::processLocal(Code code) {
  VLOG(2) << uuid_ << " commitLocal() = " << apache::thrift::util::enumNameSafe(code);
  auto currTerm = env_->txnMan_->getTermFromKVStore(spaceId_, localPartId_);
  if (currTerm.first != term_) {
    rcCommit_ = Code::E_LEADER_CHANGED;
    return rcCommit_;
  }

  if (code == Code::SUCCEEDED) {
    // if there are something wrong other than rpc failure
    // we need to keep the resume retry(by not remove those prime key)
    eraseDoublePrime();
    return abort();
  }

  return code;
}

void ChainResumeAddDoublePrimeProcessor::finish() {
  if (rcPrepare_ == Code::SUCCEEDED) {
    VLOG(1) << uuid_ << ", " << makeReadableEdge(req_)
            << ", rcPrepare_ = " << apache::thrift::util::enumNameSafe(rcPrepare_)
            << ", rcRemote_ = " << apache::thrift::util::enumNameSafe(rcRemote_)
            << ", rcCommit_ = " << apache::thrift::util::enumNameSafe(rcCommit_);
  }
  if (rcCommit_ != Code::SUCCEEDED || rcRemote_ != Code::SUCCEEDED) {
    reportFailed(ResumeType::RESUME_REMOTE);
  } else {
    // nothing todo
  }
  pushResultCode(rcCommit_, localPartId_);
  finished_.setValue(rcCommit_);
  onFinished();
}

}  // namespace storage
}  // namespace nebula
