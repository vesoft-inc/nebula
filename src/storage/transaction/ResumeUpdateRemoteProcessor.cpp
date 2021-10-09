/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/transaction/ResumeUpdateRemoteProcessor.h"

#include <storage/StorageFlags.h>

namespace nebula {
namespace storage {

ResumeUpdateRemoteProcessor::ResumeUpdateRemoteProcessor(StorageEnv* env, const std::string& val)
    : ChainUpdateEdgeProcessorLocal(env) {
  req_ = ConsistUtil::parseUpdateRequest(val);
  ChainUpdateEdgeProcessorLocal::prepareRequest(req_);
}

folly::SemiFuture<nebula::cpp2::ErrorCode> ResumeUpdateRemoteProcessor::prepareLocal() {
  return Code::SUCCEEDED;
}

folly::SemiFuture<Code> ResumeUpdateRemoteProcessor::processRemote(Code code) {
  return ChainUpdateEdgeProcessorLocal::processRemote(code);
}

folly::SemiFuture<Code> ResumeUpdateRemoteProcessor::processLocal(Code code) {
  setErrorCode(code);

  if (!checkTerm()) {
    LOG(WARNING) << "E_OUTDATED_TERM";
    return Code::E_OUTDATED_TERM;
  }

  if (!checkVersion()) {
    LOG(WARNING) << "E_OUTDATED_EDGE";
    return Code::E_OUTDATED_EDGE;
  }

  if (code == Code::SUCCEEDED) {
    // if there are something wrong other than rpc failure
    // we need to keep the resume retry(by not remove those prime key)
    auto key = ConsistUtil::doublePrime(spaceVidLen_, partId_, req_.get_edge_key());
    kvErased_.emplace_back(std::move(key));
    forwardToDelegateProcessor();
    return code;
  } else {
    // we can't decide if the double prime shoule be deleted.
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
